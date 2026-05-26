//! Runner-side unix-socket control server (Task 1.4).
//!
//! [`serve`] accepts connections on a unix-domain socket, reads one [`Cmd`]
//! per line (newline-delimited JSON), dispatches it to a [`RunnerLifecycle`]
//! handle that is shared with the live [`super::serve::RunnerServe`], and
//! writes one [`Reply`] back before closing the connection.
//!
//! # Lifecycle sharing
//! [`RunnerLifecycle`] wraps an `Arc<Mutex<Option<HostedApp>>>` so the control
//! server and `RunnerServe` share ownership of the live listener handle.
//! Dropping the `Option<HostedApp>` (via `Stop`) aborts the listener task in
//! the `HostedApp::drop` impl — no extra teardown machinery needed.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Mutex, oneshot};

use crate::config::DockerConfig;
use crate::control_proto::{Cmd, Reply};
use crate::fetcher::S3Fetcher;
use crate::host::HostedApp;
use crate::runtime::{AppRuntime, RuntimeHealth};

/// Shared lifecycle state driven by the control server.
///
/// `RunnerServe` owns the primary `HostedApp` and hands a clone of this handle
/// to the control server. The `hosted` mutex guards the optional live listener:
/// `Some(…)` ↔ running, `None` ↔ stopped.
#[derive(Clone)]
pub struct RunnerLifecycle {
    /// The app's UUID (string form), for health replies and purge.
    pub(crate) uuid: String,
    /// The app's deterministic ULA (string form), for health replies.
    pub(crate) app_ula: String,
    /// Mutable ownership of the live per-app listener. Dropping the inner
    /// `HostedApp` (via `take`) aborts its tokio task.
    pub(crate) hosted: Arc<Mutex<Option<HostedApp>>>,
    /// S3 fetcher — used by `Purge` to clear the on-disk artifact cache.
    pub(crate) fetcher: S3Fetcher,
    /// Docker config — used by `Purge` to remove the built docker image.
    pub(crate) docker: DockerConfig,
    /// The app runtime, held so `Health` can call `AppRuntime::health()` to
    /// report the app's own liveness (not just whether the runner process is
    /// alive).
    pub(crate) runtime: Arc<dyn AppRuntime>,
    /// Optional sender that signals the main task to exit cleanly when
    /// `Shutdown` is dispatched. `None` when the control server was started
    /// without a shutdown notifier (legacy / test path).
    ///
    /// Wrapped in `Arc<Mutex<Option<…>>>` so the `Clone` impl doesn't need to
    /// duplicate the sender (only one `send` must fire; clones share the same
    /// slot and the first `take` wins).
    pub(crate) shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl RunnerLifecycle {
    /// Wire a shutdown notifier into this lifecycle. When `Shutdown` is
    /// dispatched the sender fires, signalling the main task's `select!`.
    pub async fn set_shutdown_tx(&self, tx: oneshot::Sender<()>) {
        *self.shutdown_tx.lock().await = Some(tx);
    }

    /// Is the app currently running (listener alive)?
    async fn is_running(&self) -> bool {
        self.hosted.lock().await.is_some()
    }

    /// Stop: drop the live `HostedApp` (aborts its listener task). Idempotent.
    async fn stop(&self) {
        let mut guard = self.hosted.lock().await;
        let _ = guard.take(); // Drop triggers HostedApp::drop → task.abort()
    }

    /// Purge: stop + remove the on-disk artifact cache + docker image.
    async fn purge(&self) {
        self.stop().await;

        // Best-effort docker image removal (docker apps only). A WASM runner
        // has no docker image; `purge_image` is a no-op when docker is absent.
        crate::docker::purge_image(&self.docker.docker_bin, &self.uuid).await;

        // Remove the on-disk cache.
        if let Err(e) = self.fetcher.purge_cache(&self.uuid).await {
            tracing::warn!(uuid = %self.uuid, error = %e, "purge_cache failed (continuing)");
        }
    }

    /// Build a [`Reply::Health`] snapshot from current state.
    ///
    /// Calls `AppRuntime::health()` to probe the app's own liveness so the
    /// reply reflects whether the app itself is serving, not just whether the
    /// runner process is up.
    async fn health(&self) -> Reply {
        let state = if self.is_running().await {
            "running"
        } else {
            "stopped"
        };
        let (app_health, app_health_reason) = match self.runtime.health().await {
            RuntimeHealth::Serving => ("serving".to_owned(), None),
            RuntimeHealth::Unavailable(reason) => ("unavailable".to_owned(), Some(reason)),
        };
        Reply::Health {
            state: state.to_owned(),
            app_ula: self.app_ula.clone(),
            app_uuid: self.uuid.clone(),
            pid: std::process::id(),
            app_health,
            app_health_reason,
        }
    }
}

/// Accept connections on `socket_path` forever; for each connection read one
/// [`Cmd`] (JSON line) and write one [`Reply`] (JSON line). The `lifecycle`
/// handle is cloned per-connection so concurrent clients are safe (Mutex
/// inside serialises `Stop`/`Purge`).
///
/// Removes any stale socket file at `socket_path` before binding so a crashed
/// runner doesn't leave a dead socket that blocks re-binding.
///
/// # Errors
/// Returns only if the unix listener itself fails to bind (e.g. the directory
/// does not exist). Per-connection errors are logged and discarded.
pub async fn serve(socket_path: impl AsRef<Path>, lifecycle: RunnerLifecycle) -> Result<()> {
    let socket_path = socket_path.as_ref();

    // Remove a stale socket from a previous run, if any.
    let _ = tokio::fs::remove_file(socket_path).await;

    let listener = UnixListener::bind(socket_path)
        .map_err(|e| anyhow::anyhow!("bind control socket {:?}: {e}", socket_path))?;

    tracing::info!(path = ?socket_path, "control socket listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let lc = lifecycle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, lc).await {
                        tracing::warn!(error = %e, "control connection error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "control accept error");
            }
        }
    }
}

/// Handle one control connection: read one JSON-line [`Cmd`], dispatch, write
/// one JSON-line [`Reply`].
async fn handle_connection(
    stream: tokio::net::UnixStream,
    lifecycle: RunnerLifecycle,
) -> Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    reader.read_line(&mut line).await?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let reply = match serde_json::from_str::<Cmd>(trimmed) {
        Ok(cmd) => dispatch(cmd, &lifecycle).await,
        Err(e) => Reply::Err {
            message: format!("bad command: {e}"),
        },
    };

    let mut out = serde_json::to_string(&reply)?;
    out.push('\n');
    write_half.write_all(out.as_bytes()).await?;
    write_half.flush().await?;
    Ok(())
}

/// Dispatch a [`Cmd`] to the lifecycle and produce a [`Reply`].
async fn dispatch(cmd: Cmd, lifecycle: &RunnerLifecycle) -> Reply {
    match cmd {
        Cmd::Ping => Reply::Pong,
        Cmd::Health => lifecycle.health().await,
        Cmd::Stop => {
            lifecycle.stop().await;
            Reply::Ok
        }
        Cmd::Purge => {
            lifecycle.purge().await;
            Reply::Ok
        }
        Cmd::Shutdown => {
            lifecycle.stop().await;
            // Signal the main task to exit cleanly, if a shutdown notifier is
            // wired. The main task calls `process::exit(0)` after the select
            // resolves so the reply can be flushed first.
            // Fallback: if no notifier is wired (legacy path), keep the old
            // behaviour of spawning a delayed exit directly.
            let tx = lifecycle.shutdown_tx.lock().await.take();
            if let Some(tx) = tx {
                let _ = tx.send(());
            } else {
                tokio::spawn(async {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    std::process::exit(0);
                });
            }
            Reply::Ok
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use http::{Request, Response};
    use tokio::sync::Mutex;

    use super::*;
    use crate::config::DockerConfig;
    use crate::control_proto::{Cmd, Reply};
    use crate::fetcher::S3Fetcher;
    use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, RuntimeHealth};

    // ---- Fake runtime -------------------------------------------------------

    /// A fake runtime whose health() returns a fixed value — no WASM or VM.
    struct FakeRuntime {
        health: RuntimeHealth,
    }

    impl AppRuntime for FakeRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }

        fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
            let h = self.health.clone();
            Box::pin(async move { h })
        }
    }

    fn fake_lifecycle(health: RuntimeHealth) -> RunnerLifecycle {
        RunnerLifecycle {
            uuid: "test-uuid".to_owned(),
            app_ula: "fd5a::1".to_owned(),
            hosted: Arc::new(Mutex::new(None)), // stopped
            fetcher: S3Fetcher::new("http://s3.invalid", std::path::Path::new("/tmp")),
            docker: DockerConfig::default(),
            runtime: Arc::new(FakeRuntime { health }),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    // ---- Health dispatch tests ----------------------------------------------

    /// Health reply carries app_health="serving" when the runtime is healthy.
    #[tokio::test]
    async fn health_reply_carries_app_health_serving() {
        let lc = fake_lifecycle(RuntimeHealth::Serving);
        let reply = dispatch(Cmd::Health, &lc).await;
        match reply {
            Reply::Health {
                app_health,
                app_health_reason,
                ..
            } => {
                assert_eq!(app_health, "serving");
                assert!(app_health_reason.is_none());
            }
            other => panic!("expected Health reply, got {other:?}"),
        }
    }

    /// Health reply carries app_health="unavailable" + a reason when the
    /// runtime reports Unavailable.
    #[tokio::test]
    async fn health_reply_carries_app_health_unavailable() {
        let lc = fake_lifecycle(RuntimeHealth::Unavailable("guest down".to_owned()));
        let reply = dispatch(Cmd::Health, &lc).await;
        match reply {
            Reply::Health {
                app_health,
                app_health_reason,
                ..
            } => {
                assert_eq!(app_health, "unavailable");
                assert_eq!(app_health_reason.as_deref(), Some("guest down"));
            }
            other => panic!("expected Health reply, got {other:?}"),
        }
    }
}
