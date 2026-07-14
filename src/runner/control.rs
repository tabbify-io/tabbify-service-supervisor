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
    /// Serializes the complete runner-side deploy transaction across cloned
    /// per-connection lifecycle handles. A transport retry may queue here, but
    /// can never overtake a still-building first request.
    pub(crate) deploy_lock: Arc<Mutex<()>>,
    /// Optional sender that signals the main task to exit cleanly when
    /// `Shutdown` is dispatched. `None` when the control server was started
    /// without a shutdown notifier (legacy / test path).
    ///
    /// Wrapped in `Arc<Mutex<Option<…>>>` so the `Clone` impl doesn't need to
    /// duplicate the sender (only one `send` must fire; clones share the same
    /// slot and the first `take` wins).
    pub(crate) shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    /// The resolved OCI manifest DIGEST (`sha256:…`) of the currently-active
    /// runtime's image. The deploy guard compares THIS, not the (possibly-
    /// floating) string ref in [`ActiveRuntime`]: for a connected-repo
    /// deploy the ref string can stay equal while the digest behind a moving tag
    /// changes, so a string-equal "no-op" would wrongly skip the new commit
    /// (TAB-10). `None` ⇒ unknown digest ⇒ the next deploy rebuilds (safe).
    ///
    /// Shared via `Arc<Mutex<…>>` for the same per-connection-clone reason as
    /// the active slot: the guard reads it and a successful swap writes it.
    pub(crate) current_digest: Arc<Mutex<Option<String>>>,
    /// Deploy-time extra `KEY=VALUE` env baked into the guest `/init`. Populated
    /// from the runner's `RUNNER_EXTRA_ENV` at startup and passed into
    /// [`build_runtime`] for both cold starts and zero-downtime swaps, so the
    /// guest always gets the same deploy-time env regardless of how the runtime
    /// is (re)built. `None` for a normal (non-devbox, non-dev-session) deploy.
    pub(crate) extra_env: Option<std::collections::HashMap<String, String>>,
    /// Egress allow-list (Track 7 network ACL). Populated from the runner's
    /// `RUNNER_EGRESS_ALLOW` at startup and passed into [`build_runtime`] on a
    /// zero-downtime swap so the new VM's cold rebuild re-applies the SAME
    /// host-side egress posture (deny-by-default + allowed hosts). `None` ⇒
    /// today's unrestricted egress.
    pub(crate) egress_allow: Option<Vec<String>>,
    /// TEST-ONLY override for the OCI digest-resolver runner used by the deploy
    /// guard. `None` in production ⇒ the guard spawns the real
    /// [`crate::runner::build::firecracker::production_fc_build_runner`] (real
    /// `oras resolve`). Tests set `Some(fake)` to exercise the digest
    /// short-circuit / fail-open logic hermetically without a real registry.
    pub(crate) digest_resolver: Option<crate::runner::build::firecracker::FcBuildRunner>,
    /// Runner-owned OCI auth config shared by pulls and every digest resolve.
    pub(crate) registry_config: Option<Arc<crate::runner::registry::RegistryConfig>>,
}

impl RunnerLifecycle {
    /// The OCI digest-resolver runner: the test override when set, else the real
    /// production `oras`-spawning runner. Centralised so the deploy guard, the
    /// post-swap re-resolve, and any future caller share one source.
    fn digest_runner(&self) -> crate::runner::build::firecracker::FcBuildRunner {
        self.digest_resolver
            .clone()
            .unwrap_or_else(crate::runner::build::firecracker::production_fc_build_runner)
    }

    async fn resolve_digest(&self, reff: &str) -> anyhow::Result<String> {
        crate::runner::registry::resolve_oci_digest(
            reff,
            &self.digest_runner(),
            self.registry_config.as_deref(),
        )
        .await
    }

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
        let (runtime, image_ref) = self.active.load_with_ref();
        let (app_health, app_health_reason) = match runtime.health().await {
            RuntimeHealth::Serving => ("serving".to_owned(), None),
            RuntimeHealth::Unavailable(reason) => ("unavailable".to_owned(), Some(reason)),
        };
        Reply::Health {
            state: state.to_owned(),
            app_ula: self.app_ula.clone(),
            app_uuid: self.uuid.clone(),
            pid: std::process::id(),
            image_ref,
            app_health,
            app_health_reason,
        }
    }

    /// Refresh the active runtime's warm snapshot IN-PLACE (the `Cmd::Snapshot`
    /// handler). Delegates to `AppRuntime::snapshot()` on the swappable active
    /// cell, so the live VM stays serving while its on-disk snapshot is
    /// rewritten. Returns [`Reply::Ok`] on success, [`Reply::Err`] (with the
    /// error text) if the snapshot create failed — the VM keeps running either
    /// way.
    async fn snapshot(&self) -> Reply {
        match self.active.snapshot().await {
            Ok(()) => {
                tracing::info!(uuid = %self.uuid, "Cmd::Snapshot: warm snapshot refreshed");
                Reply::Ok
            }
            Err(e) => {
                tracing::warn!(uuid = %self.uuid, error = %e, "Cmd::Snapshot: snapshot refresh failed (VM still serving)");
                Reply::Err {
                    message: format!("snapshot: {e}"),
                }
            }
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
        let _serialize = self.deploy_lock.lock().await;
        let mut runtime_ref = reff.to_owned();
        let mut resolved_digest = None;

        // Same-DIGEST re-deploy guard: if the requested ref resolves to the SAME
        // OCI manifest digest as the live runtime AND the active runtime is
        // healthy, a rebuild is a wasteful no-op — and worse, the new VM would
        // derive the SAME `uuid:reff` tap as the still-running old VM and collide
        // on it. Short-circuit with Ok ONLY when the DIGEST matches.
        //
        // Why digest, not the string ref (TAB-10): a connected-repo deploy can
        // carry a stable ref string while the digest behind a moving tag changes;
        // a string-equal compare would wrongly skip the new commit. We resolve
        // the requested ref's digest (~0.2s `oras resolve`, no blob pull) and
        // compare it to the recorded `current_digest`.
        //
        // FAIL-OPEN: if the digest cannot be resolved (registry unreachable,
        // transient flap), we DO NOT short-circuit — we fall through to a
        // rebuild. A rebuild is always safe; the tap-collision the guard avoids
        // only matters when the digest is genuinely identical, and we can only
        // know that on a successful resolve. (This is the opposite stance to the
        // build-side fail-CLOSED: there an unprovable commit must not ship; here
        // an unprovable digest must not BLOCK a deploy.)
        let serving = self.active.health().await == RuntimeHealth::Serving;
        if serving {
            // The FC host identity (tap / api-sock / /30 link) is
            // `blake3(uuid:reff)`, so a deploy whose ref STRING equals the live
            // ref derives the IDENTICAL identity as the still-running VM. A
            // coexist-swap onto that identity collides on the api-socket and
            // fails "firecracker API socket never appeared" (the new VM cannot
            // create a socket the live one already holds). A coexist-swap is
            // therefore safe ONLY when `reff != current_ref`; when the ref is
            // unchanged we must NOT rebuild unless we can PROVE the image moved.
            let same_ref = self.active.current_ref().as_deref() == Some(reff);
            match self.resolve_digest(reff).await {
                Ok(want) => {
                    let current = self.current_digest.lock().await.clone();
                    if Some(want.as_str()) == current.as_deref() {
                        tracing::info!(
                            uuid = %self.uuid,
                            reff = %reff,
                            digest = %want,
                            "deploy: requested digest already live and healthy — skipping rebuild (no-op)"
                        );
                        return Reply::Ok;
                    }
                    // Same ref string but no recorded digest to disprove sameness
                    // (e.g. post-respawn `current_digest = None`): a rebuild would
                    // collide on the identical identity and we have no evidence the
                    // image changed → no-op. A KNOWN-moved digest falls through
                    // and is pinned below so its coexist identity is distinct.
                    if same_ref && current.is_none() {
                        tracing::warn!(
                            uuid = %self.uuid,
                            reff = %reff,
                            "deploy: ref already live, digest unknown — no-op (avoids same-identity FC tap collision)"
                        );
                        return Reply::Ok;
                    }
                    if let Some(pinned) = moved_tag_runtime_ref(
                        reff,
                        self.active.current_ref().as_deref(),
                        current.as_deref(),
                        &want,
                    ) {
                        tracing::info!(
                            uuid = %self.uuid,
                            requested_ref = %reff,
                            active_ref = %pinned,
                            digest = %want,
                            "deploy: moving tag changed; using digest-pinned runtime identity"
                        );
                        runtime_ref = pinned;
                    }
                    resolved_digest = Some(want);
                }
                Err(e) => {
                    if same_ref {
                        // Cannot prove the image moved AND the ref is unchanged → a
                        // fail-open rebuild would collide on the identical
                        // `uuid:reff` identity ("socket never appeared"). The safe
                        // degradation is a no-op (keep serving the live VM); a new
                        // image must ship under a NEW ref (commit SHA / digest),
                        // which takes a distinct identity and swaps cleanly.
                        tracing::warn!(
                            uuid = %self.uuid,
                            reff = %reff,
                            error = %e,
                            "deploy: digest unresolved + ref already live — no-op (avoids same-identity FC tap collision)"
                        );
                        return Reply::Ok;
                    }
                    // Different ref → fail-open rebuild is safe (distinct identity).
                    tracing::warn!(
                        uuid = %self.uuid,
                        reff = %reff,
                        error = %e,
                        "deploy: digest resolve failed — proceeding with rebuild (fail-open)"
                    );
                }
            }
        }

        // Build the new runtime from the app's manifest with the effective ref
        // applied. A proven moving-tag change uses its digest-pinned ref here.
        let next_fetched = fetched_with_ref(&self.fetched, &runtime_ref);
        let registry_config_file = match self
            .registry_config
            .as_deref()
            .map(|config| config.file_for_ref(&runtime_ref))
            .transpose()
        {
            Ok(config) => config,
            Err(e) => {
                tracing::warn!(uuid = %self.uuid, reff = %reff, error = %e, "deploy: registry auth config rejected ref (keeping old)");
                return Reply::Err {
                    message: format!("deploy: registry auth for {reff}: {e}"),
                };
            }
        };
        // Deploy (`is_swap = true`): the OLD runtime keeps serving until
        // `perform_swap` flips; the new VM cold-boots `runtime_ref` on its own
        // `uuid:runtime_ref` tap so both coexist (no reconcile-kill of the old).
        // `extra_env` is the same deploy-time env the runner was launched with
        // (populated from `RUNNER_EXTRA_ENV`), so the new rootfs gets the same
        // vars as the initial build — no env drift across zero-downtime swaps.
        let new_runtime = match build_runtime(
            &self.uuid,
            &next_fetched,
            &self.fc,
            &self.data_dir,
            true,
            self.extra_env.as_ref(),
            self.egress_allow.as_deref(),
            registry_config_file,
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
            Some(runtime_ref.clone()),
            DEPLOY_DRAIN,
            DEPLOY_HEALTH_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                // Re-resolve the digest of the now-live ref so the next deploy's
                // digest guard has the correct baseline. On a resolve failure
                // leave `current_digest = None` (the next deploy then rebuilds —
                // safe) and warn; we never strand a stale digest.
                if let Some(digest) = resolved_digest {
                    *self.current_digest.lock().await = Some(digest);
                } else {
                    match self.resolve_digest(&runtime_ref).await {
                        Ok(d) => *self.current_digest.lock().await = Some(d),
                        Err(e) => {
                            *self.current_digest.lock().await = None;
                            tracing::warn!(
                                uuid = %self.uuid,
                                requested_ref = %reff,
                                active_ref = %runtime_ref,
                                error = %e,
                                "deploy: post-swap digest re-resolve failed — current_digest=None (next deploy rebuilds)"
                            );
                        }
                    }
                }
                tracing::info!(uuid = %self.uuid, requested_ref = %reff, active_ref = %runtime_ref, "deploy: zero-downtime swap complete");
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

fn digest_pinned_oci_ref(reff: &str, digest: &str) -> String {
    let without_digest = reff
        .split_once('@')
        .map_or(reff, |(repository, _)| repository);
    let last_slash = without_digest.rfind('/');
    let repository = match without_digest
        .rfind(':')
        .filter(|colon| last_slash.is_none_or(|slash| *colon > slash))
    {
        Some(tag_separator) => &without_digest[..tag_separator],
        None => without_digest,
    };
    format!("{repository}@{digest}")
}

fn moved_tag_runtime_ref(
    requested_ref: &str,
    active_ref: Option<&str>,
    current_digest: Option<&str>,
    resolved_digest: &str,
) -> Option<String> {
    (active_ref == Some(requested_ref)
        && current_digest.is_some_and(|current| current != resolved_digest))
    .then(|| digest_pinned_oci_ref(requested_ref, resolved_digest))
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
        Cmd::Snapshot => lifecycle.snapshot().await,
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
                    port: None,
                    kernel: None,
                    registry_ref: None,
                    stateful: false,
                    data_mount: None,
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
            active: Arc::new(ActiveRuntime::new(runtime)),
            fetched: fc_fetched(),
            fc: FcConfig::default(),
            data_dir: std::env::temp_dir().join("tabbify-deploy-test"),
            deploy_lock: Arc::new(Mutex::new(())),
            shutdown_tx: Arc::new(Mutex::new(None)),
            current_digest: Arc::new(Mutex::new(None)),
            extra_env: None,
            egress_allow: None,
            digest_resolver: None,
            registry_config: None,
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

    #[tokio::test]
    async fn health_reply_carries_ref_from_the_same_active_runtime_slot() {
        let lc = fake_lifecycle(RuntimeHealth::Serving);
        lc.active
            .swap_with_ref(lc.active.load(), Some("registry/app:current".to_owned()));

        let reply = dispatch(Cmd::Health, &lc).await;

        assert!(matches!(
            reply,
            Reply::Health {
                image_ref: Some(ref image_ref),
                ..
            } if image_ref == "registry/app:current"
        ));
    }

    /// `Cmd::Snapshot` dispatches to the lifecycle's snapshot path and replies
    /// `Ok` when the runtime's (default no-op) snapshot succeeds. The active
    /// runtime is unchanged (snapshot is an in-place refresh, never a swap).
    #[tokio::test]
    async fn snapshot_cmd_replies_ok_on_success() {
        let lc = fake_lifecycle(RuntimeHealth::Serving);
        let before = lc.active.load();
        let reply = dispatch(Cmd::Snapshot, &lc).await;
        assert!(
            matches!(reply, Reply::Ok),
            "Snapshot of a default-no-op runtime must reply Ok, got {reply:?}"
        );
        assert!(
            std::sync::Arc::ptr_eq(&before, &lc.active.load()),
            "Snapshot must NOT swap the active runtime"
        );
    }

    // ---- Digest-resolver fakes ----------------------------------------------

    use crate::runner::build::firecracker::FcBuildRunner;

    /// A fake [`FcBuildRunner`] that emulates `oras resolve` by printing a fixed
    /// `digest` on stdout (exit 0) for ANY argv. Lets the digest guard run
    /// hermetically without a real registry — the genuine short-circuit path.
    fn fake_digest_runner(digest: &'static str) -> FcBuildRunner {
        Arc::new(move |_argv: Vec<String>| {
            let fut: BoxFut<'static, (bool, Vec<u8>)> =
                Box::pin(async move { (true, format!("{digest}\n").into_bytes()) });
            fut
        })
    }

    /// A fake [`FcBuildRunner`] that emulates an `oras resolve` FAILURE (exit
    /// non-zero, empty stdout) — drives the fail-open branch of the guard.
    fn failing_digest_runner() -> FcBuildRunner {
        Arc::new(|_argv: Vec<String>| {
            let fut: BoxFut<'static, (bool, Vec<u8>)> = Box::pin(async { (false, Vec::new()) });
            fut
        })
    }

    // ---- Deploy dispatch tests ----------------------------------------------

    #[test]
    fn digest_pin_replaces_tag_without_mistaking_registry_port_for_tag() {
        assert_eq!(
            digest_pinned_oci_ref("registry.example:5000/acme/app:main", "sha256:abc"),
            "registry.example:5000/acme/app@sha256:abc"
        );
        assert_eq!(
            digest_pinned_oci_ref("[fd5a::1]:5000/acme/app:main", "sha256:def"),
            "[fd5a::1]:5000/acme/app@sha256:def"
        );
    }

    #[test]
    fn moved_same_ref_uses_digest_pinned_runtime_identity() {
        let requested = "registry.example:5000/acme/app:main";
        assert_eq!(
            moved_tag_runtime_ref(requested, Some(requested), Some("sha256:old"), "sha256:new")
                .as_deref(),
            Some("registry.example:5000/acme/app@sha256:new")
        );
        assert!(
            moved_tag_runtime_ref(requested, Some(requested), Some("sha256:new"), "sha256:new")
                .is_none()
        );
    }

    #[tokio::test]
    async fn deploy_commands_are_serialized_across_lifecycle_clones() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let active_resolves = Arc::new(AtomicUsize::new(0));
        let max_active_resolves = Arc::new(AtomicUsize::new(0));
        let active_for_runner = active_resolves.clone();
        let max_for_runner = max_active_resolves.clone();
        let resolver: FcBuildRunner = Arc::new(move |_argv| {
            let active = active_for_runner.clone();
            let max = max_for_runner.clone();
            Box::pin(async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                (true, format!("{DIGEST}\n").into_bytes())
            })
        });

        let mut lifecycle = fake_lifecycle(RuntimeHealth::Serving);
        lifecycle.digest_resolver = Some(resolver);
        *lifecycle.current_digest.lock().await = Some(DIGEST.to_owned());
        let reff = "registry.example:5000/acme/app:main";
        lifecycle
            .active
            .swap_with_ref(lifecycle.active.load(), Some(reff.to_owned()));

        let first = dispatch(
            Cmd::Deploy {
                reff: reff.to_owned(),
            },
            &lifecycle,
        );
        let second = dispatch(
            Cmd::Deploy {
                reff: reff.to_owned(),
            },
            &lifecycle,
        );
        let (first, second) = tokio::join!(first, second);

        assert!(matches!(first, Reply::Ok));
        assert!(matches!(second, Reply::Ok));
        assert_eq!(
            max_active_resolves.load(Ordering::SeqCst),
            1,
            "runner-side deploy transactions must never overlap"
        );
    }

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
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        // Make the digest guard deterministic + offline: the resolver fails →
        // fail-open → the guard falls through to the rebuild this test pins.
        lc.digest_resolver = Some(failing_digest_runner());
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

    /// A deploy whose ref resolves to the SAME DIGEST that is already live (and
    /// healthy) is a no-op: it returns `Ok` WITHOUT rebuilding, so the active
    /// runtime allocation is untouched (no wasteful build, no `uuid:reff` tap
    /// collision with the live VM). This is the digest-aware short-circuit — the
    /// critical tap-collision avoidance. A fake resolver returns a fixed digest
    /// and `current_digest` is seeded to MATCH it.
    #[tokio::test]
    async fn deploy_same_digest_when_healthy_is_noop() {
        const LIVE_DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        // The guard resolves ANY ref to LIVE_DIGEST via the fake resolver…
        lc.digest_resolver = Some(fake_digest_runner(LIVE_DIGEST));
        // …and the live runtime is already at LIVE_DIGEST → digests match.
        *lc.current_digest.lock().await = Some(LIVE_DIGEST.to_owned());
        // The ref STRING is irrelevant to the digest guard; set it for realism.
        lc.active.swap_with_ref(
            lc.active.load(),
            Some("[fd5a::1]:5000/acme/app:main".to_owned()),
        );
        let before = lc.active.load();

        let reply = dispatch(
            Cmd::Deploy {
                // A floating-tag ref — the string differs in spirit, but the
                // resolved digest is identical, which is what must drive the no-op.
                reff: "[fd5a::1]:5000/acme/app:main".to_owned(),
            },
            &lc,
        )
        .await;

        assert!(
            matches!(reply, Reply::Ok),
            "same-digest deploy of a healthy runtime must reply Ok (no-op), got {reply:?}"
        );
        // No rebuild/swap happened — the active runtime is the SAME allocation.
        assert!(
            Arc::ptr_eq(&before, &lc.active.load()),
            "same-digest deploy must NOT rebuild/swap the active runtime"
        );
    }

    #[tokio::test]
    async fn deploy_digest_resolve_uses_runner_registry_auth_config() {
        const LIVE_DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        const REFF: &str = "registry.example:5000/acme/app:main";
        const TOKEN: &str = "runner-secret-token";
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_for_runner = calls.clone();
        let resolver: FcBuildRunner = Arc::new(move |argv| {
            calls_for_runner.lock().unwrap().push(argv);
            Box::pin(async { (true, format!("{LIVE_DIGEST}\n").into_bytes()) })
        });
        let registry = Arc::new(crate::runner::registry::RegistryConfig::new(TOKEN, REFF).unwrap());
        let config_file = registry.file_for_ref(REFF).unwrap().to_owned();
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        lc.digest_resolver = Some(resolver);
        lc.registry_config = Some(registry);
        *lc.current_digest.lock().await = Some(LIVE_DIGEST.to_owned());
        lc.active
            .swap_with_ref(lc.active.load(), Some(REFF.to_owned()));

        let reply = dispatch(
            Cmd::Deploy {
                reff: REFF.to_owned(),
            },
            &lc,
        )
        .await;

        assert!(matches!(reply, Reply::Ok));
        let argv = &calls.lock().unwrap()[0];
        assert!(
            argv.windows(2)
                .any(|args| args[0] == "--registry-config" && args[1] == config_file)
        );
        assert!(!argv.iter().any(|arg| arg.contains(TOKEN)));
    }

    /// A deploy whose ref resolves to a DIFFERENT digest than the live runtime
    /// must NOT short-circuit — even though the ref STRING is unchanged (the
    /// TAB-10 bug: a floating tag whose digest moved). The guard must fall
    /// through to a rebuild (here the FC build fails on the unroutable ref,
    /// proving the short-circuit was bypassed and a real build was attempted).
    #[tokio::test]
    async fn deploy_moved_digest_when_healthy_rebuilds() {
        const LIVE_DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        const NEW_DIGEST: &str =
            "sha256:2222222222222222222222222222222222222222222222222222222222222222";
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        // The requested ref now resolves to NEW_DIGEST…
        lc.digest_resolver = Some(fake_digest_runner(NEW_DIGEST));
        // …but the live runtime is still at the OLD digest → MUST rebuild.
        *lc.current_digest.lock().await = Some(LIVE_DIGEST.to_owned());
        // Same ref string as before — the string-compare bug would wrongly no-op.
        let live_ref = "[fd5a::1]:5000/acme/app:main";
        lc.active
            .swap_with_ref(lc.active.load(), Some(live_ref.to_owned()));

        let reply = dispatch(
            Cmd::Deploy {
                reff: live_ref.to_owned(),
            },
            &lc,
        )
        .await;

        // The guard did NOT short-circuit (digest moved), so the FC build ran and
        // failed on the unreachable ref — an Err, NOT the no-op Ok.
        match reply {
            Reply::Err { message } => assert!(
                message.contains("deploy"),
                "moved-digest deploy must attempt a rebuild (Err), got: {message}"
            ),
            other => panic!("moved-digest deploy must rebuild (not no-op); got {other:?}"),
        }
    }

    /// FAIL-OPEN (DIFFERENT ref): when the digest cannot be resolved (registry
    /// flap) AND the requested ref differs from the live ref, the guard must NOT
    /// short-circuit — a rebuild onto a DISTINCT `uuid:reff` identity is safe (no
    /// tap collision), so it falls through to a rebuild. Here the resolver fails
    /// and the FC build then fails on the unroutable ref → Err.
    #[tokio::test]
    async fn deploy_resolve_failure_different_ref_fails_open_and_rebuilds() {
        const LIVE_DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        lc.digest_resolver = Some(failing_digest_runner());
        *lc.current_digest.lock().await = Some(LIVE_DIGEST.to_owned());
        lc.active.swap_with_ref(
            lc.active.load(),
            Some("[fd5a::1]:5000/acme/app:oldsha".to_owned()),
        );

        let reply = dispatch(
            Cmd::Deploy {
                // DIFFERENT ref than the live one → distinct FC identity → a
                // rebuild cannot collide, so fail-open rebuild is correct.
                reff: "[fd5a::1]:5000/acme/app:newsha".to_owned(),
            },
            &lc,
        )
        .await;

        // Resolve failed + different ref → fail-open → rebuild attempted → FC
        // build fails on the unreachable ref → Err (NOT a no-op Ok).
        match reply {
            Reply::Err { message } => assert!(
                message.contains("deploy"),
                "fail-open resolve (different ref) must attempt a rebuild (Err), got: {message}"
            ),
            other => panic!(
                "fail-open (resolve error, different ref) must rebuild, never no-op; got {other:?}"
            ),
        }
    }

    /// COLLISION AVOIDANCE: when the digest cannot be resolved (registry flap
    /// over the relay) AND the requested ref equals the live ref, a fail-open
    /// rebuild would derive the IDENTICAL `uuid:reff` firecracker identity as the
    /// still-running VM and collide on its api-socket ("socket never appeared").
    /// Since we cannot prove the image moved and the ref is unchanged, the guard
    /// must NO-OP (keep serving the live VM) — NOT fail open to a colliding
    /// rebuild. This is the regression for the "Redeploy an unchanged commit"
    /// deploy failure (the resolve fails over the flaky relay, the old fail-open
    /// rebuilt onto the same identity, and the swap died "socket never appeared").
    #[tokio::test]
    async fn deploy_resolve_failure_same_ref_is_noop() {
        const LIVE_DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        lc.digest_resolver = Some(failing_digest_runner());
        *lc.current_digest.lock().await = Some(LIVE_DIGEST.to_owned());
        let live_ref = "[fd5a::1]:5000/acme/app:6457e5c";
        lc.active
            .swap_with_ref(lc.active.load(), Some(live_ref.to_owned()));
        let before = lc.active.load();

        let reply = dispatch(
            Cmd::Deploy {
                reff: live_ref.to_owned(), // SAME ref as live → identity would collide
            },
            &lc,
        )
        .await;

        assert!(
            matches!(reply, Reply::Ok),
            "resolve-failure on the SAME live ref must no-op (avoid tap collision), got {reply:?}"
        );
        assert!(
            Arc::ptr_eq(&before, &lc.active.load()),
            "same-ref no-op must NOT rebuild/swap the active runtime"
        );
    }

    /// Same-ref + UNKNOWN current digest (`None`, e.g. post-respawn): even when
    /// the resolve SUCCEEDS, there is no recorded digest to disprove sameness, so
    /// a rebuild onto the identical identity must be avoided → no-op.
    #[tokio::test]
    async fn deploy_same_ref_unknown_current_digest_is_noop() {
        let mut lc = fake_lifecycle(RuntimeHealth::Serving);
        lc.digest_resolver = Some(fake_digest_runner(
            "sha256:3333333333333333333333333333333333333333333333333333333333333333",
        ));
        // No recorded digest (post-respawn): cannot disprove sameness.
        *lc.current_digest.lock().await = None;
        let live_ref = "[fd5a::1]:5000/acme/app:6457e5c";
        lc.active
            .swap_with_ref(lc.active.load(), Some(live_ref.to_owned()));
        let before = lc.active.load();

        let reply = dispatch(
            Cmd::Deploy {
                reff: live_ref.to_owned(),
            },
            &lc,
        )
        .await;

        assert!(
            matches!(reply, Reply::Ok),
            "same-ref deploy with unknown current digest must no-op (avoid collision), got {reply:?}"
        );
        assert!(
            Arc::ptr_eq(&before, &lc.active.load()),
            "same-ref/unknown-digest no-op must NOT rebuild/swap the active runtime"
        );
    }

    /// The same-ref guard does NOT short-circuit when the active runtime is
    /// unhealthy: even if the requested ref matches the live ref, an unhealthy
    /// runtime must still attempt a rebuild (here it fails the FC build against
    /// an unreachable ref, proving the guard was bypassed and the build ran).
    #[tokio::test]
    async fn deploy_same_ref_when_unhealthy_does_not_short_circuit() {
        let lc = fake_lifecycle(RuntimeHealth::Unavailable("guest down".to_owned()));
        let live_ref = "[fd5a::1]:5000/acme/app:sha";
        lc.active
            .swap_with_ref(lc.active.load(), Some(live_ref.to_owned()));

        let reply = dispatch(
            Cmd::Deploy {
                reff: live_ref.to_owned(),
            },
            &lc,
        )
        .await;

        // The guard was bypassed (unhealthy), so the FC build ran and failed on
        // the unreachable ref — an Err, NOT the no-op Ok.
        match reply {
            Reply::Err { message } => assert!(
                message.contains("deploy"),
                "error must mention deploy, got: {message}"
            ),
            other => panic!(
                "expected Err (build attempted) for unhealthy same-ref deploy, got {other:?}"
            ),
        }
    }
}
