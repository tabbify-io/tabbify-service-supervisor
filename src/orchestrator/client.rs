//! Control-socket client: supervisor (orchestrator) side (Task 2.3).
//!
//! [`ControlClient`] sends a single [`Cmd`] over a Unix-domain socket and reads
//! back a single [`Reply`] — matching the newline-delimited JSON framing used by
//! [`crate::runner::control`] on the server side.
//!
//! # Framing
//! One JSON value per line (newline-terminated). Each *connection* carries
//! exactly one request/response pair; the server closes after writing the reply.
//!
//! # Timeouts
//! Both the connect and the read are bounded by [`TIMEOUT`]. A dead or absent
//! socket produces a clear [`anyhow::Error`] rather than hanging indefinitely.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::control_proto::{Cmd, Reply};

/// Connect + read timeout for the FAST control round-trips (ping / health /
/// stop / purge / shutdown). Sized to be generous enough for a runner that is
/// still starting up while being short enough that a crashed/dead runner fails
/// fast. Callers that need to *poll until ready* (e.g. `wait_health` in the
/// integration test) loop with their own outer deadline.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Read timeout for the `Deploy` round-trip specifically. Deploy is NOT a fast
/// command: the runner builds the new runtime SYNCHRONOUSLY before replying —
/// `build_runtime` (pull image + convert rootfs + boot the FC) then
/// `perform_swap` (health-gate + flip). A cold pull over the relay-only WAN
/// (~minutes for a multi-MB image) dwarfs the 5s [`TIMEOUT`], so reusing it made
/// every running-app warm-swap of an UNCACHED image fail with "deploy control
/// message failed" while only cache-warm swaps fit — the long-standing
/// "redeploy a running app 500s, an idle one works" bug (idle goes through the
/// generous 180s cold-spawn path instead). This deadline must cover a cold build
/// end-to-end; the connect still fails fast for a dead socket, the per-uuid
/// deploy lock + monitor-shield already tolerate a long-held deploy, and the
/// node waits on its side with no timeout.
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(300);

/// Thin client for the runner's Unix-domain control socket.
///
/// Constructing a [`ControlClient`] does **not** open a connection; each method
/// call opens a fresh connection, sends one [`Cmd`], reads one [`Reply`], and
/// closes. This keeps the client trivially cloneable and avoids long-lived
/// socket state.
#[derive(Debug, Clone)]
pub struct ControlClient {
    sock: PathBuf,
}

impl ControlClient {
    /// Create a client that connects to `sock`.
    ///
    /// No I/O happens here.
    pub fn new(sock: impl AsRef<Path>) -> Self {
        Self {
            sock: sock.as_ref().to_path_buf(),
        }
    }

    /// Send [`Cmd::Ping`], expect [`Reply::Pong`].
    pub async fn ping(&self) -> Result<Reply> {
        self.round_trip(Cmd::Ping).await
    }

    /// Send [`Cmd::Health`], expect [`Reply::Health { … }`].
    pub async fn health(&self) -> Result<Reply> {
        self.round_trip(Cmd::Health).await
    }

    /// Send [`Cmd::Stop`], expect [`Reply::Ok`].
    ///
    /// Stops the per-app listener; the runner process stays alive.
    pub async fn stop(&self) -> Result<Reply> {
        self.round_trip(Cmd::Stop).await
    }

    /// Send [`Cmd::Purge`], expect [`Reply::Ok`].
    ///
    /// Stops the per-app listener and clears the on-disk artifact cache.
    /// The runner process stays alive.
    pub async fn purge(&self) -> Result<Reply> {
        self.round_trip(Cmd::Purge).await
    }

    /// Send [`Cmd::Shutdown`], expect [`Reply::Ok`].
    ///
    /// Stops the per-app listener and signals the runner process to exit.
    /// The runner replies before exiting, so the caller reads `Ok` before the
    /// socket disappears.
    pub async fn shutdown(&self) -> Result<Reply> {
        self.round_trip(Cmd::Shutdown).await
    }

    /// Send [`Cmd::Deploy`] with the OCI image `reff`, expect [`Reply::Ok`].
    ///
    /// The runner builds a fresh runtime from `reff` and performs a
    /// zero-downtime swap. On success it replies [`Reply::Ok`]; if the new
    /// runtime never became healthy it replies [`Reply::Err`] and the OLD
    /// runtime stays in service (no downtime).
    pub async fn deploy(&self, reff: impl Into<String>) -> Result<Reply> {
        // Deploy builds the new runtime SYNCHRONOUSLY before replying (pull +
        // rootfs convert + boot + health-gated swap), which on a cold pull over
        // the relay-only WAN takes minutes — so it gets the build-length
        // DEPLOY_TIMEOUT, not the fast TIMEOUT the other commands use.
        self.round_trip_with_timeout(Cmd::Deploy { reff: reff.into() }, DEPLOY_TIMEOUT)
            .await
    }

    /// Open a fresh connection to `self.sock`, write `cmd` as a JSON line, read
    /// back one JSON-line [`Reply`], and close. Bounded by the fast [`TIMEOUT`].
    ///
    /// # Errors
    /// - The socket path does not exist or is not connectable.
    /// - The connect or read exceeds [`TIMEOUT`].
    /// - Serialization / deserialization fails.
    async fn round_trip(&self, cmd: Cmd) -> Result<Reply> {
        self.round_trip_with_timeout(cmd, TIMEOUT).await
    }

    /// [`round_trip`](Self::round_trip) with an explicit read deadline. Deploy
    /// uses the long [`DEPLOY_TIMEOUT`]; everything else uses [`TIMEOUT`]. The
    /// connect still fails fast for a dead socket regardless of `deadline`.
    ///
    /// # Errors
    /// As [`round_trip`](Self::round_trip), with the bound being `deadline`.
    async fn round_trip_with_timeout(&self, cmd: Cmd, deadline: Duration) -> Result<Reply> {
        let sock = &self.sock;

        let reply = timeout(deadline, async {
            let mut stream = UnixStream::connect(sock)
                .await
                .with_context(|| format!("connect to control socket {:?}", sock))?;

            let mut line = serde_json::to_string(&cmd).context("serialize Cmd")?;
            line.push('\n');

            stream
                .write_all(line.as_bytes())
                .await
                .context("write Cmd to control socket")?;

            // Flush is implicit on write for UnixStream, but be explicit so the
            // server sees the newline before we block on reading.
            stream.flush().await.context("flush control socket")?;

            let mut reader = BufReader::new(stream);
            let mut buf = String::new();
            reader
                .read_line(&mut buf)
                .await
                .context("read Reply from control socket")?;

            let reply: Reply = serde_json::from_str(buf.trim())
                .with_context(|| format!("deserialize Reply: {buf:?}"))?;

            Ok::<Reply, anyhow::Error>(reply)
        })
        .await
        .with_context(|| {
            format!(
                "control socket round-trip timed out after {deadline:?} for {:?}",
                sock
            )
        })??;

        Ok(reply)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Constructing a client with a non-existent socket does not panic or block.
    #[test]
    fn new_does_not_connect() {
        let client = ControlClient::new(PathBuf::from("/tmp/does-not-exist.sock"));
        assert_eq!(client.sock, PathBuf::from("/tmp/does-not-exist.sock"));
    }

    /// A dead (non-existent) socket returns an `Err` fast — not a hang.
    #[tokio::test]
    async fn dead_socket_returns_err_not_hang() {
        let dir = tempfile::tempdir().unwrap();
        let dead = dir.path().join("dead.sock");
        let client = ControlClient::new(&dead);
        let result = client.health().await;
        assert!(result.is_err(), "dead socket must return Err");
    }

    /// `round_trip_with_timeout` honors its `deadline` param: a deadline SHORTER
    /// than the server's reply delay times out, a generous one tolerates the slow
    /// reply. This is the mechanism `deploy` rides via the long `DEPLOY_TIMEOUT`
    /// (so a slow synchronous build no longer 500s) while the fast commands keep
    /// the short `TIMEOUT`.
    #[tokio::test]
    async fn round_trip_honors_its_deadline() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::UnixListener;

        use crate::control_proto::Cmd;

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("slow.sock");
        let sock_srv = sock.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_srv).unwrap();
            for _ in 0..4 {
                if let Ok((stream, _)) = listener.accept().await {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line).await;
                    // Reply ~200ms after the request — slower than a 50ms deadline,
                    // faster than a 5s one.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = ControlClient::new(&sock);
        let short = client
            .round_trip_with_timeout(Cmd::Health, Duration::from_millis(50))
            .await;
        assert!(
            short.is_err(),
            "a deadline shorter than the reply delay must time out"
        );
        let long = client
            .round_trip_with_timeout(Cmd::Health, Duration::from_secs(5))
            .await;
        assert!(
            long.is_ok(),
            "a generous deadline tolerates the slow reply: {long:?}"
        );
    }

    /// The Deploy round-trip deadline is build-length and far larger than the
    /// fast `TIMEOUT` — so a cold pull (~minutes) no longer trips the control
    /// timeout (the long-standing "redeploy a running app 500s" root).
    #[test]
    fn deploy_timeout_is_build_length() {
        assert!(
            DEPLOY_TIMEOUT > TIMEOUT,
            "deploy must use a longer deadline than the fast commands"
        );
        assert!(
            DEPLOY_TIMEOUT >= Duration::from_secs(120),
            "deploy deadline must cover a multi-minute cold build"
        );
    }
}
