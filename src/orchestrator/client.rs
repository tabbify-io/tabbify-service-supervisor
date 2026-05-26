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

/// Connect + read timeout applied to every control-socket round-trip.
///
/// Sized to be generous enough for a runner that is still starting up while
/// being short enough that a crashed/dead runner fails fast. Callers that need
/// to *poll until ready* (e.g. `wait_health` in the integration test) loop with
/// their own outer deadline.
const TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Open a fresh connection to `self.sock`, write `cmd` as a JSON line, read
    /// back one JSON-line [`Reply`], and close.
    ///
    /// The entire operation (connect + write + read) is bounded by [`TIMEOUT`].
    ///
    /// # Errors
    /// - The socket path does not exist or is not connectable.
    /// - The connect or read exceeds [`TIMEOUT`].
    /// - Serialization / deserialization fails.
    async fn round_trip(&self, cmd: Cmd) -> Result<Reply> {
        let sock = &self.sock;

        let reply = timeout(TIMEOUT, async {
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
                "control socket round-trip timed out after {TIMEOUT:?} for {:?}",
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
}
