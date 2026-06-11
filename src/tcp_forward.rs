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

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};

/// Spawn a TCP L4 forwarder on `bind_addr` that proxies every accepted
/// connection to `target_addr` using [`tokio::io::copy_bidirectional`].
///
/// The forwarder task lives until `bind_addr`'s listener is dropped (i.e.
/// until the returned `TcpForwarder` is dropped). Bind errors (e.g. the mesh
/// ULA not yet assigned) are returned immediately so the caller can log and
/// skip without crashing the runner.
///
/// Per-connection errors (connect-to-target failure, copy errors) are logged
/// at debug level and do NOT abort the forwarder.
///
/// # Errors
/// Returns an error only if binding `bind_addr` fails.
pub async fn spawn_forwarder(
    bind_addr: SocketAddr,
    target_addr: SocketAddr,
) -> Result<TcpForwarder> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("L4 forwarder: bind {bind_addr}"))?;
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

/// Accept loop: accept connections on `listener` and spawn a bidirectional
/// copy task for each one.
async fn accept_loop(listener: TcpListener, target: SocketAddr) {
    loop {
        match listener.accept().await {
            Ok((inbound, peer)) => {
                tracing::debug!(%peer, %target, "L4 forwarder: accepted connection");
                tokio::spawn(forward_conn(inbound, target));
            }
            Err(e) => {
                // A bind-level error (e.g. the TUN going away) would show here.
                // Log and continue — transient errors (EINTR, ECONNABORTED)
                // should not abort the loop.
                tracing::debug!(error = %e, %target, "L4 forwarder: accept error");
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
    /// The accept-loop task. Aborting it (via `JoinHandle::abort` on drop) is
    /// not needed — the listener socket is dropped when the `JoinHandle` is,
    /// which makes `accept()` return an error and the loop exit naturally.
    /// We hold the handle so the task is NOT detached: it is tied to the
    /// lifetime of `TcpForwarder`.
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Bind an echo server on loopback, then connect through the forwarder and
    /// assert that bytes round-trip bidirectionally.
    #[tokio::test]
    async fn forwarder_round_trips_bytes() {
        // Echo server: reads exactly N bytes and echoes them back.
        let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0u8; 128];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        // Bind the forwarder on a loopback ephemeral port, forwarding to echo.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let fwd = spawn_forwarder(bind_addr, echo_addr)
            .await
            .expect("forwarder must bind");
        // Retrieve the actual bound address by trying a connect — we don't expose
        // it directly, so just use the forwarder's listener port.
        // Re-bind to get the address: instead, accept the fact that `spawn_forwarder`
        // binds an ephemeral port and we need to find it. We'll use a known port.
        drop(fwd);

        // Re-run with a known port pair so we can address the forwarder directly.
        let fwd_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let fwd2 = spawn_forwarder(fwd_bind, echo_addr).await.unwrap();

        // We need the forwarder's port. Expose it via a second listener trick:
        // spawn a minimal probe by re-binding on the forwarder's addr.
        // Actually: since we used port 0, find the port via TcpListener + abort.
        // Simpler: bind the forwarder on a fixed port in a tempdir range.
        drop(fwd2);

        // Cleaner approach: use a fixed pair of ports.
        // Bind echo on :0, then forwarder on :0, then connect to forwarder via
        // a secondary bind just to get the port.
        let echo_listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr2 = echo_listener2.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = echo_listener2.accept().await.unwrap();
            let mut buf = vec![0u8; 5];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        // Bind the forwarder and get its address via a temp listener trick.
        let fwd_listener_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fwd_port = fwd_listener_probe.local_addr().unwrap().port();
        drop(fwd_listener_probe);

        let fwd_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), fwd_port);
        let _fwd = spawn_forwarder(fwd_addr, echo_addr2).await.unwrap();

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

    /// `spawn_forwarder` returns an error when the bind address is already in use.
    #[tokio::test]
    async fn forwarder_bind_failure_returns_error() {
        // Bind a listener to occupy the port.
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = occupied.local_addr().unwrap();

        // Attempting to bind the forwarder on the same port must fail.
        let result = spawn_forwarder(addr, addr).await;
        assert!(
            result.is_err(),
            "spawn_forwarder must return Err when the port is occupied"
        );
    }

    /// Dropping `TcpForwarder` aborts the accept loop — new connections to
    /// the (now-closed) listener are refused.
    #[tokio::test]
    async fn dropping_forwarder_stops_accepting() {
        // Stand up a dummy target so spawn_forwarder succeeds.
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();

        let fwd_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fwd_addr = fwd_probe.local_addr().unwrap();
        drop(fwd_probe);

        let fwd = spawn_forwarder(fwd_addr, target_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Drop the forwarder — the listener task is aborted.
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
