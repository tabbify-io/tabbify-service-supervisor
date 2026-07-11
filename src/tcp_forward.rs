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
use arc_swap::ArcSwap;
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
/// connection to the CURRENT value of `target` using
/// [`tokio::io::copy_bidirectional`].
///
/// The upstream target is held in an [`ArcSwap`] and read PER CONNECTION at
/// dial time, so a caller can hot-swap it (`target.store(...)`) and every NEW
/// connection lands on the new upstream WITHOUT re-binding the listener — the
/// forge host-migration path (`POST /v1/forge-proxy/target`). In-flight copies
/// keep their original upstream. Static callers (ssh/code/ctrl runner
/// forwarders) simply wrap a fixed addr in an `ArcSwap` and never swap it.
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
    target: Arc<ArcSwap<SocketAddr>>,
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
        target = %target.load(),
        "L4 forwarder: bound; forwarding connections"
    );

    let handle = tokio::spawn(accept_loop(listener, target));
    Ok(TcpForwarder {
        _handle: handle,
        local_addr: bound_addr,
    })
}

/// Accept loop: accept connections on `listener` and, under a bounded
/// semaphore, spawn a bidirectional copy task for each one. The upstream
/// `target` is shared and read per connection (at dial time).
async fn accept_loop(listener: tokio::net::TcpListener, target: Arc<ArcSwap<SocketAddr>>) {
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
                tracing::debug!(%peer, "L4 forwarder: accepted connection");
                let target = Arc::clone(&target);
                tokio::spawn(async move {
                    forward_conn(inbound, &target).await;
                    drop(permit);
                });
            }
            Err(e) => {
                // A bind-level / resource error (TUN going away, EMFILE/ENFILE)
                // surfaces here. Transient errors (EINTR, ECONNABORTED) must not
                // abort the loop; back off briefly so an fd-exhaustion condition
                // does not busy-spin this task at 100% CPU.
                tracing::debug!(error = %e, "L4 forwarder: accept error");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}

/// Read the CURRENT upstream target from the swap, connect, and bidirectionally
/// copy between `inbound` and it. Reading at dial time is what makes a hot-swap
/// take effect for new connections.
async fn forward_conn(mut inbound: TcpStream, target: &ArcSwap<SocketAddr>) {
    let target = *target.load_full();
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
    /// The address the listener actually bound (resolves `port 0` to the
    /// OS-assigned port). Exposed via [`Self::local_addr`] for callers/tests.
    local_addr: SocketAddr,
}

impl TcpForwarder {
    /// The address the forwarder's listener is bound on (the OS-assigned port
    /// when `bind_addr` used port 0).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
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

    /// Spawn a server on a fresh loopback port that, on each accepted
    /// connection, writes `banner` then half-closes its write side so the
    /// client reads exactly the banner then EOF. Returns `(addr, banner)` so a
    /// test can assert WHICH upstream served a given connection.
    async fn spawn_banner_server(banner: &str) -> (SocketAddr, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let banner = banner.to_owned();
        let served = banner.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let msg = served.clone();
                tokio::spawn(async move {
                    let _ = stream.write_all(msg.as_bytes()).await;
                    // Half-close so the client's read_to_end sees EOF.
                    let _ = stream.shutdown().await;
                });
            }
        });
        (addr, banner)
    }

    /// Connect to `addr` (a forwarder) and read the upstream banner to EOF.
    async fn read_banner(addr: SocketAddr) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// The forwarder must read its upstream target from the shared `ArcSwap` at
    /// CONNECT time, so a hot-swap reroutes NEW connections WITHOUT re-binding
    /// the listener — the forge host-migration path (`POST /v1/forge-proxy/target`).
    #[tokio::test]
    async fn forwarder_reads_target_from_arcswap_at_connect_time() {
        // Two banner servers A, B. Point the swap at A, connect, expect A's
        // banner; swap to B, connect again, expect B's banner — no re-bind.
        let (a_addr, a_banner) = spawn_banner_server("A").await;
        let (b_addr, b_banner) = spawn_banner_server("B").await;
        let target = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(a_addr));
        let bind = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        let fwd = spawn_forwarder(bind, target.clone()).await.unwrap();
        assert_eq!(read_banner(fwd.local_addr()).await, a_banner);
        target.store(std::sync::Arc::new(b_addr));
        assert_eq!(read_banner(fwd.local_addr()).await, b_banner);
    }

    /// Bytes sent through the forwarder must round-trip bidirectionally.
    #[tokio::test]
    async fn forwarder_round_trips_bytes() {
        let echo_addr = spawn_echo(5).await;
        let fwd_addr = free_loopback_addr().await;

        let target = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(echo_addr));
        let _fwd = spawn_forwarder(fwd_addr, target)
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
        let target_old = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(echo_addr));
        let _fwd_old = spawn_forwarder(shared_addr, target_old)
            .await
            .expect("first forwarder must bind the shared addr");

        // Second forwarder (the "new" runner) MUST also bind the SAME addr
        // (SO_REUSEPORT) instead of EADDRINUSE — the regression this guards.
        let target_new = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(echo_addr));
        let _fwd_new = spawn_forwarder(shared_addr, target_new)
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

        let target = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(target_addr));
        let fwd = spawn_forwarder(fwd_addr, target).await.unwrap();
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
