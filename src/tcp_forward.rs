//! Userspace L4 TCP forwarder: bind on a local address, bidirectionally proxy
//! every accepted connection to a fixed target address.
//!
//! Used by the runner to expose `[app_ula]:2222` on the host wireguard mesh
//! interface, forwarding to `{guest_ip}:2222` (the FC guest's sshd) so the
//! node can `ssh root@[app_ula]:2222` into a dev/devbox session.
//!
//! The bind address is IPv6 (the mesh ULA); the target is IPv4 (the /30 tap).
//! These are two entirely separate sockets — the kernel handles them independently.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::Semaphore;

/// TCP port the FC guest's sshd listens on (inside the VM, on its eth0). The
/// runner binds `[app_ula]:GUEST_SSH_PORT` on the host mesh interface and
/// forwards to `{guest_ip}:GUEST_SSH_PORT`. Shared by the serve-side bind and
/// the runtime's `guest_ssh_addr()` so the two never drift.
pub const GUEST_SSH_PORT: u16 = 2222;

/// TCP port the FC workspace BROKER serves its token-gated add-key control
/// endpoint on (§12 S6, T4 IDE-remote). The runner binds `[app_ula]:8732` and
/// forwards to `{guest_ip}:8732`; node POSTs the laptop pubkey here with its
/// bearer cap. NOT a frozen-contract value — an admin/control seam reconciled
/// with the broker (`tabbify-broker` `http_ctrl::BROKER_CTRL_PORT`) and node
/// (`ssh_tunnel::WORKSPACE_BROKER_CTRL_PORT`). Shared by the serve-side bind and
/// `guest_broker_ctrl_addr()` so the two never drift.
pub const GUEST_BROKER_CTRL_PORT: u16 = 8732;

/// Max concurrently-forwarded connections per forwarder. Acquiring a permit
/// before spawning a `forward_conn` task bounds the fds/tasks a stuck or
/// still-booting guest can leak: with the guest's sshd unreachable, each dial
/// would otherwise park a `TcpStream::connect` forever. 16 is ample for
/// interactive exec/devbox use (one or two live SSH sessions).
const MAX_INFLIGHT_CONNS: usize = 16;

/// Backoff after a fatal `accept()` error so an exhausted-fd condition
/// (EMFILE/ENFILE) does not busy-spin the accept loop at 100% CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Spawn a TCP L4 forwarder on `bind_addr` that proxies every accepted
/// connection to `target_addr` using [`tokio::io::copy_bidirectional`].
///
/// The listener is created with `SO_REUSEPORT` so that during a zero-downtime
/// deploy/swap (or a purge → respawn) the NEW runner can bind the SAME
/// `[app_ula]:2222` while the OLD runner still holds it: both sockets coexist
/// briefly (the old keeps serving existing connections and accepting; the new
/// also accepts), and the port is freed when the old forwarder drops. Without
/// `SO_REUSEPORT` the new bind would `EADDRINUSE` → WARN+None → SSH silently
/// dead until a later restart.
///
/// Bind errors (e.g. the mesh ULA not yet assigned) are returned immediately so
/// the caller can log and skip without crashing the runner.
///
/// Per-connection errors (connect-to-target failure, copy errors) are logged
/// at debug level and do NOT abort the forwarder.
///
/// # Errors
/// Returns an error only if creating, configuring, or binding the socket fails.
pub async fn spawn_forwarder(
    bind_addr: SocketAddr,
    target_addr: SocketAddr,
) -> Result<TcpForwarder> {
    let sock = match bind_addr {
        SocketAddr::V6(_) => TcpSocket::new_v6(),
        SocketAddr::V4(_) => TcpSocket::new_v4(),
    }
    .with_context(|| format!("L4 forwarder: create socket for {bind_addr}"))?;
    // SO_REUSEPORT: lets the old + new runner both bind app_ula:2222 across a
    // swap so the SSH path never has a bind-conflict gap (see fn docs).
    sock.set_reuseport(true)
        .with_context(|| format!("L4 forwarder: set SO_REUSEPORT on {bind_addr}"))?;
    sock.bind(bind_addr)
        .with_context(|| format!("L4 forwarder: bind {bind_addr}"))?;
    let listener = sock
        .listen(1024)
        .with_context(|| format!("L4 forwarder: listen on {bind_addr}"))?;

    let bound_addr = listener
        .local_addr()
        .with_context(|| "L4 forwarder: local_addr after bind")?;
    tracing::debug!(
        %bound_addr,
        %target_addr,
        "L4 forwarder: bound; forwarding connections"
    );

    let handle = tokio::spawn(accept_loop(listener, target_addr));
    Ok(TcpForwarder { _handle: handle })
}

/// Accept loop: accept connections on `listener` and, under a bounded
/// semaphore, spawn a bidirectional copy task for each one.
async fn accept_loop(listener: tokio::net::TcpListener, target: SocketAddr) {
    let permits = Arc::new(Semaphore::new(MAX_INFLIGHT_CONNS));
    loop {
        match listener.accept().await {
            Ok((inbound, peer)) => {
                // Bound in-flight conns: acquire a permit (own it, so it is
                // released when the conn task ends) before spawning. If all
                // permits are held the accept loop awaits here, applying
                // natural backpressure instead of leaking tasks/fds.
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    // The semaphore is never closed while the loop runs, so
                    // this is unreachable in practice; bail defensively.
                    break;
                };
                tracing::debug!(%peer, %target, "L4 forwarder: accepted connection");
                tokio::spawn(async move {
                    forward_conn(inbound, target).await;
                    drop(permit);
                });
            }
            Err(e) => {
                // A bind-level / resource error (TUN going away, EMFILE/ENFILE)
                // surfaces here. Transient errors (EINTR, ECONNABORTED) must not
                // abort the loop; back off briefly so an fd-exhaustion condition
                // does not busy-spin this task at 100% CPU.
                tracing::debug!(error = %e, %target, "L4 forwarder: accept error");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}

/// Connect to `target` and bidirectionally copy between `inbound` and it.
async fn forward_conn(mut inbound: TcpStream, target: SocketAddr) {
    match TcpStream::connect(target).await {
        Ok(mut outbound) => {
            match tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await {
                Ok((from_client, from_target)) => {
                    tracing::debug!(
                        %target,
                        bytes_from_client = from_client,
                        bytes_from_target = from_target,
                        "L4 forwarder: connection closed"
                    );
                }
                Err(e) => {
                    tracing::debug!(%target, error = %e, "L4 forwarder: copy error");
                }
            }
        }
        Err(e) => {
            tracing::debug!(%target, error = %e, "L4 forwarder: connect to target failed");
        }
    }
}

/// A running L4 TCP forwarder. Dropping this value aborts the accept loop and
/// stops forwarding new connections; in-flight copy tasks run to completion.
pub struct TcpForwarder {
    /// The accept-loop task handle. Held so the task is tied to this value's
    /// lifetime; [`Drop`] calls `abort()` on it.
    ///
    /// NOTE: simply dropping a `JoinHandle` DETACHES the task (it keeps
    /// running) — it does NOT cancel it, so the listener socket would stay
    /// bound and the port held. The `abort()` in [`Drop`] is what actually
    /// stops the task: abort → the accept loop's future is cancelled → the
    /// `TcpListener` it owns is dropped → the port is freed. Do not remove the
    /// `abort()` thinking the handle's drop suffices.
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for TcpForwarder {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Spawn an echo server on a fresh loopback port; returns its address. It
    /// accepts one connection, reads up to `read_len` bytes, echoes them back.
    async fn spawn_echo(read_len: usize) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; read_len];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });
        addr
    }

    /// Reserve a free loopback port by binding then immediately dropping the
    /// listener, so a forwarder can be addressed on a known port.
    async fn free_loopback_addr() -> SocketAddr {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        addr
    }

    /// Bytes sent through the forwarder must round-trip bidirectionally.
    #[tokio::test]
    async fn forwarder_round_trips_bytes() {
        let echo_addr = spawn_echo(5).await;
        let fwd_addr = free_loopback_addr().await;

        let _fwd = spawn_forwarder(fwd_addr, echo_addr)
            .await
            .expect("forwarder must bind");
        // Give the forwarder a moment to start its accept loop.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Connect through the forwarder, send 5 bytes, expect them back.
        let mut client = tokio::net::TcpStream::connect(fwd_addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut resp = [0u8; 5];
        client.read_exact(&mut resp).await.unwrap();
        assert_eq!(
            &resp, b"hello",
            "bytes must round-trip through the forwarder"
        );
    }

    /// TWO forwarders must be able to bind the SAME loopback address
    /// concurrently thanks to `SO_REUSEPORT` — this is the swap case: the new
    /// runner binds `[app_ula]:2222` while the old still holds it, so the SSH
    /// path has no bind-conflict gap. Both then forward bytes successfully.
    #[tokio::test]
    async fn two_forwarders_share_addr_with_reuseport() {
        let echo_addr = spawn_echo(5).await;
        let shared_addr = free_loopback_addr().await;

        // First forwarder (the "old" runner) binds the shared addr.
        let _fwd_old = spawn_forwarder(shared_addr, echo_addr)
            .await
            .expect("first forwarder must bind the shared addr");

        // Second forwarder (the "new" runner) MUST also bind the SAME addr
        // (SO_REUSEPORT) instead of EADDRINUSE — the regression this guards.
        let _fwd_new = spawn_forwarder(shared_addr, echo_addr)
            .await
            .expect("second forwarder must also bind the shared addr via SO_REUSEPORT");

        tokio::time::sleep(Duration::from_millis(10)).await;

        // A connection through the shared addr still round-trips (the kernel
        // load-balances accepts across both listeners; either may serve it).
        let mut client = tokio::net::TcpStream::connect(shared_addr).await.unwrap();
        client.write_all(b"world").await.unwrap();
        let mut resp = [0u8; 5];
        client.read_exact(&mut resp).await.unwrap();
        assert_eq!(
            &resp, b"world",
            "a connection must still round-trip with two coexisting forwarders"
        );
    }

    /// Dropping `TcpForwarder` aborts the accept loop — new connections to
    /// the (now-closed) listener are refused.
    #[tokio::test]
    async fn dropping_forwarder_stops_accepting() {
        // Stand up a dummy target so spawn_forwarder succeeds.
        let target_addr = free_loopback_addr().await;
        let fwd_addr = free_loopback_addr().await;

        let fwd = spawn_forwarder(fwd_addr, target_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Drop the forwarder — the listener task is aborted, freeing the port.
        drop(fwd);
        tokio::time::sleep(Duration::from_millis(5)).await;

        // A connection attempt to the now-closed forwarder must fail.
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            tokio::net::TcpStream::connect(fwd_addr),
        )
        .await;
        // Either the timeout fires or connect returns an error — either way the
        // forwarder is no longer serving new connections.
        if let Ok(Ok(_)) = result {
            panic!("connect to dropped forwarder must fail");
        }
    }
}
