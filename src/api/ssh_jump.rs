//! Per-dev-session SSH TCP jump: a transparent byte forward that lets the NODE
//! reach a dev-FC's sshd WITHOUT entering the tenant network.
//!
//! # Why
//!
//! A dev-FC lives in a TENANT mesh network (app-ULA `fd5a:1f02:…`); the node is
//! on the `system` network (`fd5a:1f00:…`). Strict per-network mesh isolation
//! means the node has NO route to the dev-FC — a node→dev-FC:2222 dial fails
//! instantly with "no route to host". The SUPERVISOR, however, is co-located
//! with the dev-FC (it spawned the FC on its own tap) and CAN reach it on the
//! tap IPv4 `guest_ip:2222`.
//!
//! So per dev session the supervisor binds a fresh listener on its OWN mesh ULA
//! (`[my_ula]:<ephemeral>`, the SAME node-reachable ULA the control API binds)
//! and, per accepted connection, dials `guest_ip:2222` and copies bytes
//! bidirectionally. The node SSHes to the jump address; SSH auth stays
//! end-to-end node↔dev-FC (the node's key, which the dev-FC authorizes) — the
//! jump is a dumb TCP relay that never sees the SSH session's plaintext.
//!
//! # Lifecycle
//!
//! [`SshJump`] owns the accept-loop [`JoinHandle`] and the bound [`SocketAddr`].
//! Dropping it aborts the loop (and so tears the forward down), so storing the
//! jump in the in-memory [`crate::api::DevSession`] ties its lifetime to the
//! session: a session `remove`/overwrite drops the jump and frees the port.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// TCP port the dev-FC's `sshd` listens on (`0.0.0.0:2222` + `[::]:2222`), so it
/// is reachable from the supervisor host at `guest_ip:2222`. Mirrors the node's
/// `DEVBOX_SSH_PORT`; a non-22 port avoids colliding with any host sshd.
const DEV_FC_SSH_PORT: u16 = 2222;

/// A live per-session SSH TCP jump (see the module doc).
pub struct SshJump {
    /// The bound listen address (`[my_ula]:<port>`) the node dials.
    addr: SocketAddr,
    /// Accept-loop task; aborted on [`Drop`] to tear the forward down.
    task: JoinHandle<()>,
}

impl SshJump {
    /// Bind a forward on `[my_ula]:0` (ephemeral port) dialing the dev-FC sshd at
    /// `guest_ip:2222`. Used at session-create time.
    ///
    /// # Errors
    /// Propagates the [`TcpListener::bind`] / `local_addr` error (e.g. the ULA is
    /// not yet routable on this host) so the caller can fall back to the direct
    /// path and log it.
    pub async fn start(my_ula: IpAddr, guest_ip: Ipv4Addr) -> io::Result<Self> {
        Self::start_on_port(my_ula, guest_ip, 0).await
    }

    /// Like [`Self::start`] but binds an EXPLICIT `port` (0 ⇒ ephemeral). Used on
    /// supervisor restart to RE-BIND the persisted port so the node's cached jump
    /// address keeps working; the caller falls back to [`Self::start`] (a fresh
    /// port) if the persisted one is taken.
    ///
    /// # Errors
    /// Propagates the bind / `local_addr` error.
    pub async fn start_on_port(my_ula: IpAddr, guest_ip: Ipv4Addr, port: u16) -> io::Result<Self> {
        Self::start_to(
            SocketAddr::new(my_ula, port),
            SocketAddr::new(IpAddr::V4(guest_ip), DEV_FC_SSH_PORT),
        )
        .await
    }

    /// Inner seam: bind `bind_addr` and forward every accepted connection to
    /// `target`. Exposed (`pub(crate)`) so the unit test can point the forward at
    /// a loopback echo server instead of a real `guest_ip:2222`.
    ///
    /// # Errors
    /// Propagates the bind / `local_addr` error.
    pub(crate) async fn start_to(bind_addr: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((inbound, peer)) => {
                        tokio::spawn(async move {
                            if let Err(e) = forward(inbound, target).await {
                                // A reset/early-close is normal (ssh closes the
                                // channel); log at debug so it is not noisy.
                                tracing::debug!(%peer, %target, error = %e, "ssh-jump forward ended");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "ssh-jump accept failed; forward loop exiting");
                        break;
                    }
                }
            }
        });
        Ok(Self { addr, task })
    }

    /// The bound port (persisted in [`crate::api::DevSessionRecord`] so a restart
    /// can re-bind it).
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// The bound listen address (mostly for logging / tests).
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for SshJump {
    fn drop(&mut self) {
        // Tear the accept loop down: the bound port is freed and in-flight copies
        // end when their sockets close.
        self.task.abort();
    }
}

/// Dial the dev-FC sshd and copy bytes both ways until either side closes.
async fn forward(mut inbound: TcpStream, target: SocketAddr) -> io::Result<()> {
    let mut outbound = TcpStream::connect(target).await?;
    tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await?;
    Ok(())
}

// ── Tap /30 derivation + jump helpers ──────────────────────────────────────────

/// Derive the `(host_ip, guest_ip)` /30 tap pair for a dev-FC identified by
/// `app_uuid` + `image_ref` — the SAME pair the FC launch assigns.
///
/// The FC launch uses `vm_key = format!("{uuid}:{reff}")` where `reff` is the
/// OCI `image_ref`; we hash the SAME key to land on the same `/30` link_idx.
/// `host_ip` is the tap gateway the guest sees (used for the IPv4 `git_remote`);
/// `guest_ip` is `host_ip + 1` and is where the dev-FC sshd
/// ([`DEV_FC_SSH_PORT`]) is reachable from the host — the [`SshJump`] target.
///
/// Returns an error when the durable allocator cannot reserve a slot or the tap
/// subnet is invalid/exhausted. Callers fail closed instead of routing a dev
/// capability or SSH connection to another VM.
pub(crate) fn derive_dev_fc_link_ips(
    data_dir: &Path,
    app_uuid: &str,
    image_ref: &str,
    tap_subnet: &str,
) -> anyhow::Result<(Ipv4Addr, Ipv4Addr)> {
    // vm_key matches `launch_with_uuid` cold-start: `format!("{uuid}:{reff}")`.
    let vm_key = format!("{app_uuid}:{image_ref}");
    let allocation =
        crate::firecracker::link_allocator::LinkSlotAllocator::new(data_dir, tap_subnet)
            .reserve(&vm_key)?;
    crate::firecracker::link_allocator::link_ips(tap_subnet, allocation.slot)
}

/// Format a node-facing SSH-jump address (`"[<my_ula>]:<port>"`).
///
/// The node parses this back into the IPv6 ULA + port it SSHes to. `my_ula` is
/// the supervisor's mesh control ULA ([`crate::api::SupervisorState::ula`]); for
/// an IPv6 literal the brackets are mandatory in a `host:port` authority.
pub(crate) fn jump_addr_string(my_ula: &str, port: u16) -> String {
    format!("[{my_ula}]:{port}")
}

/// Start the per-session SSH jump for a dev-FC, returning the live [`SshJump`]
/// (or `None` to disable the jump → the node uses the direct app-ULA path).
///
/// Derives the dev-FC tap `guest_ip` ([`derive_dev_fc_link_ips`]) and binds a
/// forward on `[my_ula]:<port>`. `desired_port` is `None` at create (ephemeral)
/// and `Some(persisted)` on restart re-adoption (so the node's cached jump
/// address keeps working); a re-bind on a taken port falls back to a fresh one.
/// Every failure is logged + degraded to `None` (never fatal to a session).
pub(crate) async fn start_dev_ssh_jump(
    my_ula: IpAddr,
    data_dir: &Path,
    app_uuid: &str,
    image_ref: &str,
    tap_subnet: &str,
    desired_port: Option<u16>,
) -> Option<SshJump> {
    let (_, guest_ip) = match derive_dev_fc_link_ips(data_dir, app_uuid, image_ref, tap_subnet) {
        Ok(pair) => pair,
        Err(error) => {
            tracing::warn!(app_uuid, %error, "ssh-jump allocation lookup failed");
            return None;
        }
    };
    let started = match desired_port {
        Some(p) => match SshJump::start_on_port(my_ula, guest_ip, p).await {
            Ok(j) => Ok(j),
            Err(e) => {
                tracing::warn!(app_uuid, port = p, error = %e, "ssh-jump rebind on persisted port failed; trying a fresh port");
                SshJump::start(my_ula, guest_ip).await
            }
        },
        None => SshJump::start(my_ula, guest_ip).await,
    };
    match started {
        Ok(j) => Some(j),
        Err(e) => {
            tracing::warn!(app_uuid, error = %e, "ssh-jump start failed; node will use the direct app-ULA path");
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::Ipv6Addr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// Spawn a one-shot loopback echo server; return its bound address.
    async fn spawn_echo() -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if sock.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    /// The forward binds on the given ULA, accepts a connection, and copies bytes
    /// to/from the target (here a loopback echo server) transparently.
    #[tokio::test]
    async fn forward_relays_bytes_to_target() {
        let echo = spawn_echo().await;
        // Bind the jump on loopback IPv6 (stands in for `my_ula`).
        let jump = SshJump::start_to(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0), echo)
            .await
            .unwrap();
        assert_ne!(jump.port(), 0, "an ephemeral port must be assigned");

        let mut client = TcpStream::connect(jump.addr()).await.unwrap();
        client.write_all(b"hello jump").await.unwrap();
        let mut buf = [0u8; 10];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(
            &buf, b"hello jump",
            "bytes must round-trip through the forward"
        );
    }

    /// Dropping the jump tears the listener down: a later dial is refused.
    #[tokio::test]
    async fn drop_aborts_the_listener() {
        let echo = spawn_echo().await;
        let jump = SshJump::start_to(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0), echo)
            .await
            .unwrap();
        let addr = jump.addr();
        drop(jump);
        // Give the runtime a tick to action the abort + close the socket.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            TcpStream::connect(addr).await.is_err(),
            "the forward port must be closed once the jump is dropped"
        );
    }

    /// The jump's dial target is the dev-FC tap `guest_ip` = `host_ip + 1` (the
    /// SAME /30 the FC launch assigns), so it points at the guest's sshd — not
    /// the host gateway. Linux-only: the derivation source of truth lives in
    /// `firecracker::linux`.
    #[cfg(target_os = "linux")]
    #[test]
    fn link_ips_guest_is_host_plus_one() {
        use crate::config::DEFAULT_FC_TAP_SUBNET;
        use crate::firecracker::linux::{derive_link_ips, fc_identity_for_key};

        let app_uuid = "cc4bfba2-17a9-512d-b6f4-43f69114be65";
        let image_ref = "[fd5a::1]:5000/tabbify/devbox:latest";
        let (host_ip, guest_ip) = derive_dev_fc_link_ips(
            tempfile::tempdir().unwrap().path(),
            app_uuid,
            image_ref,
            DEFAULT_FC_TAP_SUBNET,
        )
        .unwrap();

        // Matches the FC launch derivation for vm_key = "uuid:image_ref".
        let (_, link_idx) = fc_identity_for_key(&format!("{app_uuid}:{image_ref}"));
        let (exp_host, exp_guest) = derive_link_ips(DEFAULT_FC_TAP_SUBNET, link_idx).unwrap();
        assert_eq!(host_ip, exp_host);
        assert_eq!(guest_ip, exp_guest);
        assert_eq!(
            u32::from(guest_ip),
            u32::from(host_ip) + 1,
            "the jump must dial guest_ip = host_ip + 1 (the guest's sshd side of the /30)"
        );
    }
}
