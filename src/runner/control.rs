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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Mutex, oneshot};

use crate::build::{build_runtime, fetched_with_ref};
use crate::config::{DockerConfig, FcConfig};
use crate::control_proto::{Cmd, Reply};
use crate::fetcher::{FetchedApp, S3Fetcher};
use crate::host::HostedApp;
use crate::runner::active::{ActiveRuntime, perform_swap};
use crate::runtime::{AppRuntime, RuntimeHealth};

/// How long the in-flight (old) runtime keeps serving after a `Deploy` swap
/// before it is asked to shut down — the drain window for requests already
/// dispatched to the old runtime.
const DEPLOY_DRAIN: Duration = Duration::from_secs(10);

/// How long [`perform_swap`] waits for the NEW runtime to report
/// [`RuntimeHealth::Serving`] before aborting the deploy (the OLD runtime stays
/// in service, so an abort causes no downtime).
const DEPLOY_HEALTH_TIMEOUT: Duration = Duration::from_secs(60);

/// Shared lifecycle state driven by the control server.
///
/// `RunnerServe` owns the primary `HostedApp` and hands a clone of this handle
/// to the control server. The `hosted` mutex guards the optional live listener:
/// `Some(…)` ↔ running, `None` ↔ stopped.
#[derive(Clone)]
pub struct RunnerLifecycle {
    /// The app's UUID (string form), for health replies and purge.
    pub(crate) uuid: String,
    /// The app's version number, for versioned docker image tag on purge.
    pub(crate) version: u64,
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
    /// The swappable active-runtime cell `Deploy` performs its zero-downtime
    /// swap against. Shared with [`super::serve::RunnerServe`] and the binary's
    /// `run_until_exit` loop (which re-arms its crash-watch across swaps).
    pub(crate) active: Arc<ActiveRuntime>,
    /// The fetched app artifact (manifest + cached path + version). `Deploy`
    /// clones it, overrides the docker `registry_ref` with the deploy ref, and
    /// rebuilds the runtime from it via [`build_runtime`].
    pub(crate) fetched: FetchedApp,
    /// Firecracker runtime config — passed to [`build_runtime`] when `Deploy`
    /// rebuilds the runtime (the platform's single runtime).
    pub(crate) fc: FcConfig,
    /// Local data dir for the artifact / AOT cache — passed to
    /// [`build_runtime`] when `Deploy` rebuilds the runtime.
    pub(crate) data_dir: PathBuf,
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
        crate::docker::purge_image(&self.docker.docker_bin, &self.uuid, self.version).await;

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

    /// Deploy a new version by OCI image `reff`: build a fresh runtime from the
    /// app's manifest with `registry_ref = Some(reff)` applied, then perform a
    /// zero-downtime swap against the shared [`ActiveRuntime`] cell.
    ///
    /// The new docker container coexists with the old during the swap window:
    /// each launch gets a unique container name (`tbf-<uuid>-<seq>`, fresh
    /// monotonic `seq`) and a fresh ephemeral loopback host port, so there is no
    /// name/port collision with the still-serving old container.
    ///
    /// Returns:
    /// - [`Reply::Ok`] when the new runtime became healthy and the swap flipped
    ///   (the old runtime is draining + shutting down in the background);
    /// - [`Reply::Err`] when the build failed (e.g. `docker pull` failed / image
    ///   never came up) or [`perform_swap`] aborted because the new runtime was
    ///   unhealthy — in both cases the OLD runtime stays in service (no
    ///   downtime).
    async fn deploy(&self, reff: &str) -> Reply {
        // Build the new runtime from the app's manifest with the deploy ref
        // applied: the runtime is always Firecracker, which pulls `reff` from the
        // mesh registry, converts the OCI image to a rootfs.ext4, and boots it.
        let next_fetched = fetched_with_ref(&self.fetched, reff);
        let new_runtime = match build_runtime(
            &self.uuid,
            &next_fetched,
            &self.fc,
            &self.data_dir,
        )
        .await
        {
            Ok(rt) => rt,
            Err(e) => {
                tracing::warn!(uuid = %self.uuid, reff = %reff, error = %e, "deploy: build new runtime failed (keeping old)");
                return Reply::Err {
                    message: format!("deploy: build runtime for {reff}: {e}"),
                };
            }
        };

        // Zero-downtime swap: health-gate the new runtime, atomically flip, then
        // drain + shut down the old one. On a health-gate timeout the OLD
        // runtime stays active and the new one is torn down.
        match perform_swap(
            &self.active,
            new_runtime,
            DEPLOY_DRAIN,
            DEPLOY_HEALTH_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(uuid = %self.uuid, reff = %reff, "deploy: zero-downtime swap complete");
                Reply::Ok
            }
            Err(e) => {
                tracing::warn!(uuid = %self.uuid, reff = %reff, error = %e, "deploy: swap aborted (keeping old)");
                Reply::Err {
                    message: format!("deploy: swap aborted for {reff}: {e}"),
                }
            }
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
        Cmd::Deploy { reff } => lifecycle.deploy(&reff).await,
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

    use bytes::Bytes as BytesAlias;

    use super::*;
    use crate::config::DockerConfig;
    use crate::control_proto::{Cmd, Reply};
    use crate::fetcher::{FetchedApp, S3Fetcher};
    use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};
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

    /// A firecracker `FetchedApp` used only to populate
    /// `RunnerLifecycle::fetched`. The health-dispatch tests never build a
    /// runtime from it; the deploy build-failure test drives the FC build off it
    /// against an unreachable registry ref to force a deterministic failure.
    fn fc_fetched() -> FetchedApp {
        FetchedApp {
            version: 1,
            manifest: AppManifest {
                app: AppMeta {
                    id: None,
                    name: "hello".to_owned(),
                    version: String::new(),
                    kind: "headless".to_owned(),
                    description: String::new(),
                },
                lifecycle: Lifecycle {
                    mode: LifecycleMode::OnRequest,
                    idle_timeout_sec: 300,
                },
                runtime: Runtime {
                    r#type: "firecracker".to_owned(),
                    entry: "context.tar.gz".to_owned(),
                    fuel_per_request: 0,
                    memory_mb: 2048,
                    vcpus: Some(2),
                    kernel: None,
                    registry_ref: None,
                },
                routes: Routes::default(),
            },
            wasm: BytesAlias::new(),
            cached_path: std::path::PathBuf::from("/tmp/tabbify-deploy-test/context.tar.gz"),
        }
    }

    fn fake_lifecycle(health: RuntimeHealth) -> RunnerLifecycle {
        let runtime: Arc<dyn AppRuntime> = Arc::new(FakeRuntime { health });
        RunnerLifecycle {
            uuid: "test-uuid".to_owned(),
            version: 0,
            app_ula: "fd5a::1".to_owned(),
            hosted: Arc::new(Mutex::new(None)), // stopped
            fetcher: S3Fetcher::new("http://s3.invalid", std::path::Path::new("/tmp")),
            docker: DockerConfig::default(),
            runtime: runtime.clone(),
            active: Arc::new(ActiveRuntime::new(runtime)),
            fetched: fc_fetched(),
            fc: FcConfig::default(),
            data_dir: std::env::temp_dir().join("tabbify-deploy-test"),
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

    // ---- Deploy dispatch tests ----------------------------------------------

    // NOTE: the happy-path deploy/swap test was removed with the in-process WASM
    // runtime — it was the only runtime that could build a healthy app hermetically
    // (no docker daemon / no KVM). The build-failure path below still pins the
    // no-downtime invariant (a failed build must NOT swap the active runtime).

    /// When building the new runtime fails (the FC build pulls an UNREACHABLE
    /// registry ref — a non-routable mesh ULA — so the pull errors out), `Deploy`
    /// must reply `Err` and the active runtime must be UNCHANGED — the old
    /// runtime stays in service (no downtime).
    #[tokio::test]
    async fn deploy_build_failure_keeps_old_runtime_and_replies_err() {
        let lc = fake_lifecycle(RuntimeHealth::Serving);
        let before = lc.active.load();

        let reply = dispatch(
            Cmd::Deploy {
                // Unroutable mesh ULA: the FC build's `oras copy` pull fails,
                // which is the deterministic build failure this test pins.
                reff: "[fd5a::1]:5000/acme/app:sha".to_owned(),
            },
            &lc,
        )
        .await;

        match reply {
            Reply::Err { message } => assert!(
                message.contains("deploy"),
                "error must mention deploy, got: {message}"
            ),
            other => panic!("expected Err reply on build failure, got {other:?}"),
        }
        // The active runtime is unchanged — same allocation as before.
        assert!(
            Arc::ptr_eq(&before, &lc.active.load()),
            "a failed deploy must NOT swap the active runtime (no downtime)"
        );
    }
}
