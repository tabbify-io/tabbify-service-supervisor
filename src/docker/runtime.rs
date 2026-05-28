//! [`DockerRuntime`] struct + [`crate::runtime::AppRuntime`] impl + the
//! production runner factories used by [`super`] and re-exported for the build
//! backend.
//!
//! The container-launch orchestration (W3 tar-load → registry pull → W2 cache
//! → build → run → ready-probe) lives in [`super::DockerRuntime::launch_with_id`]
//! and pulls its lower-level helpers from [`super::build`] and [`super::push`].

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http::Request;
use tokio::net::TcpListener;
use tokio::process::Command;

use super::DockerConfig;
use super::build::{build_image, precheck, run_docker, run_docker_check};
use super::docker_available;
use super::protocol::{
    ImageCacheDecision, PullDecision, container_name, image_cache_decision, inspect_args,
    load_args, proxy_request, pull_decision, rm_args, run_args, stop_args, versioned_image_tag,
};
use super::push::pull_and_tag;
use crate::manifest::Runtime;
use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

/// How long to wait for the container app's HTTP server to come up.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for the container app.
const READY_POLL: Duration = Duration::from_millis(250);

/// Monotonic per-process counter → a unique container name per launch, so
/// repeated launches of the same app (stop→start, re-host) don't collide.
static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

/// Probe type: given a `host:port` string returns `true` iff a TCP connect
/// succeeds. In production this calls [`tcp_connect_probe`]; in tests an
/// injected closure fakes the result so no real container is needed.
pub(super) type TcpProbe = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Exit-watch signal type: a shared future that resolves to an
/// [`ExitReason`] when the container exits. Production uses
/// [`container_exit_watcher`]; tests inject a pre-resolved future via a
/// [`tokio::sync::watch`] channel or a one-shot sender so no real Docker
/// daemon is needed.
///
/// Wrapped in `Arc` so [`DockerRuntime`] is `Clone`-compatible and the
/// future can be polled from multiple locations (both `watch_for_exit` calls
/// on a cloned `Arc<dyn AppRuntime>` share the same underlying signal).
pub(super) type ExitWatcher = Arc<dyn Fn() -> BoxFut<'static, ExitReason> + Send + Sync>;

/// Command-runner seam for [`DockerRuntime::shutdown`] and the push/pull
/// paths: given a list of `docker` sub-command arguments (e.g.
/// `["stop", "tbf-abc-0"]`), run the command and return whether it succeeded.
///
/// Production: the real `docker` binary via [`tokio::process::Command`].
/// Tests: an injected closure that records which commands were issued without
/// invoking a real Docker daemon — the same injection pattern used by
/// [`TcpProbe`] and [`ExitWatcher`].
///
/// Re-exported at the module level so [`crate::build_backend`] can reuse the
/// same seam for the host-docker build backend.
pub(crate) type CommandRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

/// Image-inspect runner seam for the W2 build-cache check: given a list of
/// `docker image inspect` arguments, run the command and return `true` iff
/// the image is present (exit 0). Production: the real `docker` CLI.
/// Tests: an injected closure so no real daemon is needed.
pub(super) type InspectRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

/// Tar-load runner seam for the W3 warm-start path: given a list of
/// `docker load` arguments (e.g. `["load", "-i", "/path/to/image.tar.gz"]`),
/// run the command and return `true` iff it exits 0 (load succeeded).
/// Production: the real `docker` CLI via [`production_command_runner`].
/// Tests: an injected closure that records calls without a real daemon.
pub(super) type TarLoadRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

/// Build the production [`CommandRunner`]: spawns `<docker_bin> <args>` and
/// returns `true` iff the process exits 0. Best-effort: a spawn failure or
/// non-zero exit both yield `false` (the shutdown path logs and continues).
///
/// Re-exported at the module level so [`crate::build_backend`] can construct
/// a production runner for the host-docker build backend.
pub(crate) fn production_command_runner(docker_bin: String) -> CommandRunner {
    Arc::new(move |args: Vec<String>| {
        let docker_bin = docker_bin.clone();
        let fut: BoxFut<'static, bool> = Box::pin(async move {
            match Command::new(&docker_bin)
                .args(&args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
            {
                Ok(s) => s.success(),
                Err(_) => false,
            }
        });
        fut
    })
}

/// Build a production exit watcher for `container`: runs `docker wait
/// <container>` asynchronously and resolves to `ExitReason::Died(detail)`
/// when the container exits (any exit code). The Docker daemon blocks on
/// `docker wait` until the named container stops, so this future stays
/// pending while the container is alive.
fn container_exit_watcher(docker_bin: String, container: String) -> ExitWatcher {
    Arc::new(move || {
        let docker_bin = docker_bin.clone();
        let container = container.clone();
        let fut: BoxFut<'static, ExitReason> = Box::pin(async move {
            match Command::new(&docker_bin)
                .args(["wait", &container])
                .stdin(Stdio::null())
                .output()
                .await
            {
                Ok(out) => {
                    let exit_code = String::from_utf8_lossy(&out.stdout).trim().to_owned();
                    ExitReason::Died(format!(
                        "container {container} exited with code {exit_code}"
                    ))
                }
                Err(e) => {
                    ExitReason::Died(format!("container {container}: docker wait failed: {e}"))
                }
            }
        });
        fut
    })
}

/// Production TCP probe: try to connect to `addr` with a short timeout.
/// Returns `true` iff the connection succeeds.
fn tcp_connect_probe(addr: &str) -> bool {
    use std::net::TcpStream;
    use std::time::Duration;
    TcpStream::connect_timeout(
        &addr
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()),
        Duration::from_millis(300),
    )
    .is_ok()
}

/// A running app container. Owns the container (by name) + the loopback host
/// port its app port is published on; [`Drop`] force-removes the container.
pub struct DockerRuntime {
    /// `tbf-<id>-<seq>` — the `--name` of the `docker run` container.
    container: String,
    /// The `docker` binary (from [`DockerConfig`]) used for teardown.
    docker_bin: String,
    /// `http://127.0.0.1:<host_port>` — the base the proxy targets.
    container_base: String,
    client: reqwest::Client,
    /// TCP-connect probe, injectable for tests. Production uses
    /// [`tcp_connect_probe`]; tests substitute a closure.
    probe: TcpProbe,
    /// Exit-watcher factory, injectable for tests. Production uses
    /// [`container_exit_watcher`]; tests inject a factory that returns a
    /// pre-resolved future so no real Docker daemon is needed.
    exit_watcher: ExitWatcher,
    /// Command-runner seam for graceful shutdown. Production uses
    /// [`production_command_runner`]; tests inject a recording closure so
    /// no real Docker daemon is needed.
    shutdown_runner: CommandRunner,
    /// Content-stable image tag `tbf-img-<uuid>-v<N>` for this app version.
    /// Used by the W2 build-cache check and by `purge_image`.
    versioned_image_tag: String,
    /// Image-inspect runner seam for the W2 build-cache check. Production
    /// uses the real `docker image inspect`; tests inject a recording closure.
    inspect_runner: InspectRunner,
    /// Tar-load runner seam for the W3 warm-start path. Production uses the
    /// real `docker load -i <path>`; tests inject a recording closure so no
    /// real daemon or tar file is needed.
    tar_load_runner: TarLoadRunner,
}

impl DockerRuntime {
    /// Build the image from the `.tar.gz` build context at `context` and run
    /// it, then wait for the app's HTTP server.
    ///
    /// Steps: docker guard → reserve an ephemeral loopback host port →
    /// `docker build -t <tag> -` (tarball piped on stdin) → `docker run -d
    /// --name <name> -p 127.0.0.1:<host>:<app> <tag>` → poll until ready.
    /// The `id` (app uuid or content hash) makes the image tag + container
    /// name deterministic.
    ///
    /// # Errors
    /// `!docker_available()`, a missing context tarball, a `docker build`
    /// failure (or timeout), a `docker run` failure, or the app not
    /// answering within [`READY_TIMEOUT`].
    pub async fn launch(context: &Path, rt: &Runtime, cfg: &DockerConfig) -> Result<Self> {
        // version = 0 for the non-registry path (derive_id gives a stem-based id).
        Self::launch_with_id(context, rt, cfg, &super::build::derive_id(context), 0, None).await
    }

    /// [`Self::launch`] with an explicit `id` and `version` for the image tag /
    /// container name (the registry passes the app uuid + version; tests can pin both).
    /// `version` makes the image tag content-stable: `tbf-img-<id>-v<version>`.
    ///
    /// `image_ref` is an optional OCI ref (e.g. `[fd5a::1]:5000/acme/app:sha`)
    /// to pull from the mesh registry. When set it is tried BEFORE the W2 cache
    /// check. Pass `None` to keep the existing W3 → W2 → build behaviour.
    ///
    /// Source priority (first hit wins):
    /// 1. W3  — prebuilt `image.tar.gz` in the app dir
    /// 2. Registry pull — `image_ref` set → `docker pull <ref>` + `docker tag`
    /// 3. W2  — image already in the daemon (`docker image inspect` exits 0)
    /// 4. Build from source (`docker build -`)
    ///
    /// If the image already exists in the Docker daemon (`docker image inspect` exits 0),
    /// the build step is SKIPPED and the cached image is reused directly.
    pub async fn launch_with_id(
        context: &Path,
        rt: &Runtime,
        cfg: &DockerConfig,
        id: &str,
        version: u64,
        image_ref: Option<&str>,
    ) -> Result<Self> {
        precheck(docker_available(), context)?;

        let vtag = versioned_image_tag(id, version);

        // W3 warm-start: if the app dir contains a prebuilt image tar, load
        // it before the registry-pull and W2 cache check. After a successful
        // load the image `tbf-img-<uuid>-v<N>` exists, so all later steps
        // short-circuit via the W2 inspect.
        let tar_load_runner =
            production_command_runner(cfg.docker_bin.clone()) as TarLoadRunner;
        let app_dir = context.parent().unwrap_or(context);
        let tar_path = app_dir.join("image.tar.gz");
        if tar_path.is_file() {
            let tar_str = tar_path.to_string_lossy().into_owned();
            let loaded = (tar_load_runner)(load_args(&tar_str)).await;
            if loaded {
                tracing::info!(tag = %vtag, path = %tar_str, "docker image loaded from prebuilt tar (warm start)");
            } else {
                tracing::warn!(tag = %vtag, path = %tar_str, "docker load failed — falling through to registry pull / W2 build path");
            }
        }

        // Registry pull: if an OCI ref is set, try to pull + tag it into the
        // local daemon BEFORE the W2 cache check. On success the image is now
        // present so W2's inspect returns Skip. On failure we warn and fall
        // through to the W2 / build path unchanged.
        let pull_runner = production_command_runner(cfg.docker_bin.clone()) as CommandRunner;
        if let PullDecision::Pull(ref reff) = pull_decision(image_ref) {
            let pulled = pull_and_tag(&cfg.docker_bin, reff, &vtag, &pull_runner).await;
            if pulled {
                tracing::info!(
                    tag = %vtag,
                    registry_ref = %reff,
                    "docker image pulled from mesh registry (registry source)"
                );
            } else {
                tracing::warn!(
                    tag = %vtag,
                    registry_ref = %reff,
                    "docker pull from mesh registry failed — falling through to W2 / build"
                );
            }
        }

        // W2 build-cache: skip `docker build` if the image is already present
        // (either loaded from the tar above, pulled from the registry, or left
        // over from a prior run).
        let image_exists = run_docker_check(&cfg.docker_bin, &inspect_args(&vtag)).await;
        match image_cache_decision(image_exists) {
            ImageCacheDecision::Build => {
                tracing::debug!(tag = %vtag, "docker image not cached — building from source");
                build_image(&cfg.docker_bin, &vtag, context, cfg.build_timeout_secs).await?;
            }
            ImageCacheDecision::Skip => {
                tracing::debug!(tag = %vtag, "docker image present — skipping build");
            }
        }

        // Reserve an ephemeral loopback host port by binding :0 then dropping
        // the listener; docker re-binds it for the published port. (A small
        // TOCTOU window, acceptable for this dev/RnD runtime.)
        let host_port = reserve_loopback_port().await?;
        let app_port = rt_app_port(rt, cfg);
        let seq = RUN_SEQ.fetch_add(1, Ordering::SeqCst);
        let container = container_name(id, seq);

        // Best-effort remove any stale container of the same name first.
        let _ = run_docker(&cfg.docker_bin, &rm_args(&container)).await;
        run_docker(
            &cfg.docker_bin,
            &run_args(&container, host_port, app_port, &vtag),
        )
        .await
        .context("docker run")?;

        let inspect_runner = production_command_runner(cfg.docker_bin.clone());
        let me = Self {
            container: container.clone(),
            docker_bin: cfg.docker_bin.clone(),
            container_base: format!("http://127.0.0.1:{host_port}"),
            client: reqwest::Client::new(),
            probe: Arc::new(tcp_connect_probe),
            exit_watcher: container_exit_watcher(cfg.docker_bin.clone(), container),
            shutdown_runner: production_command_runner(cfg.docker_bin.clone()),
            versioned_image_tag: vtag,
            inspect_runner,
            tar_load_runner,
        };

        // On any readiness failure `me` drops → container force-removed.
        me.wait_until_ready().await?;
        Ok(me)
    }

    /// Build a `DockerRuntime` with an injectable probe for unit tests.
    /// `container_base` is `http://127.0.0.1:<port>` (the proxy target base);
    /// `container` is the `tbf-…` container name; `probe` is the
    /// TCP-connect check that `health()` will call.
    ///
    /// This constructor is `#[cfg(test)]`-only so it never surfaces in
    /// production code.
    #[cfg(test)]
    pub fn with_probe_for_test(container_base: &str, container: &str, probe: TcpProbe) -> Self {
        // Default exit watcher: never resolves (pending), so existing health
        // tests that only care about the probe are unaffected.
        let exit_watcher: ExitWatcher = Arc::new(|| Box::pin(std::future::pending()));
        // Default shutdown runner: no-op (records nothing, returns true).
        let shutdown_runner: CommandRunner = Arc::new(|_args| Box::pin(async { true }));
        // Default inspect runner: no-op (image absent by default, won't be called in health tests).
        let inspect_runner: InspectRunner = Arc::new(|_args| Box::pin(async { false }));
        // Default tar-load runner: no-op (no tar in health tests).
        let tar_load_runner: TarLoadRunner = Arc::new(|_args| Box::pin(async { false }));
        Self {
            container: container.to_owned(),
            docker_bin: "docker".to_owned(),
            container_base: container_base.to_owned(),
            client: reqwest::Client::new(),
            probe,
            exit_watcher,
            shutdown_runner,
            versioned_image_tag: "tbf-img-test-v0".to_owned(),
            inspect_runner,
            tar_load_runner,
        }
    }

    /// Build a `DockerRuntime` with both injectable probe AND exit watcher
    /// for unit tests. `exit_watcher` is a factory that returns a
    /// `BoxFut<'static, ExitReason>` — each call to `watch_for_exit` invokes
    /// the factory once and polls the returned future.
    #[cfg(test)]
    pub fn with_watcher_for_test(
        container_base: &str,
        container: &str,
        probe: TcpProbe,
        exit_watcher: ExitWatcher,
    ) -> Self {
        let shutdown_runner: CommandRunner = Arc::new(|_args| Box::pin(async { true }));
        let inspect_runner: InspectRunner = Arc::new(|_args| Box::pin(async { false }));
        let tar_load_runner: TarLoadRunner = Arc::new(|_args| Box::pin(async { false }));
        Self {
            container: container.to_owned(),
            docker_bin: "docker".to_owned(),
            container_base: container_base.to_owned(),
            client: reqwest::Client::new(),
            probe,
            exit_watcher,
            shutdown_runner,
            versioned_image_tag: "tbf-img-test-v0".to_owned(),
            inspect_runner,
            tar_load_runner,
        }
    }

    /// Build a `DockerRuntime` with an injectable command runner for testing
    /// [`AppRuntime::shutdown`]. The `shutdown_runner` closure is called with
    /// the argument list of each `docker` sub-command issued during shutdown
    /// (first `["stop", <name>]`, then `["rm", "-f", <name>]`), so tests can
    /// record which commands were issued without invoking a real Docker daemon.
    ///
    /// This constructor is `#[cfg(test)]`-only.
    #[cfg(test)]
    pub fn with_shutdown_for_test(
        container_base: &str,
        container: &str,
        shutdown_runner: CommandRunner,
    ) -> Self {
        let probe: TcpProbe = Arc::new(|_addr: &str| true);
        let exit_watcher: ExitWatcher = Arc::new(|| Box::pin(std::future::pending()));
        let inspect_runner: InspectRunner = Arc::new(|_args| Box::pin(async { false }));
        let tar_load_runner: TarLoadRunner = Arc::new(|_args| Box::pin(async { false }));
        Self {
            container: container.to_owned(),
            docker_bin: "docker".to_owned(),
            container_base: container_base.to_owned(),
            client: reqwest::Client::new(),
            probe,
            exit_watcher,
            shutdown_runner,
            versioned_image_tag: "tbf-img-test-v0".to_owned(),
            inspect_runner,
            tar_load_runner,
        }
    }

    /// Build a `DockerRuntime` with an injectable inspect runner for testing
    /// the W2 build-cache skip decision. The `inspect_runner` is called with
    /// `["image", "inspect", "<versioned-tag>"]` and returns `true` if the
    /// image is present. `uuid` + `version` determine the versioned tag that
    /// the runner will be called with.
    ///
    /// This constructor is `#[cfg(test)]`-only.
    #[cfg(test)]
    pub fn with_inspect_for_test(
        container_base: &str,
        container: &str,
        uuid: &str,
        version: u64,
        inspect_runner: InspectRunner,
    ) -> Self {
        let probe: TcpProbe = Arc::new(|_addr: &str| true);
        let exit_watcher: ExitWatcher = Arc::new(|| Box::pin(std::future::pending()));
        let shutdown_runner: CommandRunner = Arc::new(|_args| Box::pin(async { true }));
        let tar_load_runner: TarLoadRunner = Arc::new(|_args| Box::pin(async { false }));
        Self {
            container: container.to_owned(),
            docker_bin: "docker".to_owned(),
            container_base: container_base.to_owned(),
            client: reqwest::Client::new(),
            probe,
            exit_watcher,
            shutdown_runner,
            versioned_image_tag: versioned_image_tag(uuid, version),
            inspect_runner,
            tar_load_runner,
        }
    }

    /// Build a `DockerRuntime` with an injectable tar-load runner for testing
    /// the W3 warm-start path. `tar_load_runner` is called with
    /// `["load", "-i", "<tar_path>"]` when `load_image_tar` is invoked.
    /// `uuid` + `version` set the versioned image tag (used in log messages).
    ///
    /// This constructor is `#[cfg(test)]`-only.
    #[cfg(test)]
    pub fn with_tar_load_for_test(
        container_base: &str,
        container: &str,
        uuid: &str,
        version: u64,
        tar_load_runner: TarLoadRunner,
    ) -> Self {
        let probe: TcpProbe = Arc::new(|_addr: &str| true);
        let exit_watcher: ExitWatcher = Arc::new(|| Box::pin(std::future::pending()));
        let shutdown_runner: CommandRunner = Arc::new(|_args| Box::pin(async { true }));
        let inspect_runner: InspectRunner = Arc::new(|_args| Box::pin(async { false }));
        Self {
            container: container.to_owned(),
            docker_bin: "docker".to_owned(),
            container_base: container_base.to_owned(),
            client: reqwest::Client::new(),
            probe,
            exit_watcher,
            shutdown_runner,
            versioned_image_tag: versioned_image_tag(uuid, version),
            inspect_runner,
            tar_load_runner,
        }
    }

    /// Ask the injected inspect runner whether the versioned image exists.
    /// Returns `true` if the image should be reused (skip build), `false`
    /// if the build step must run.
    ///
    /// In production this is called inline inside `launch_with_id`; it is
    /// exposed as a method so tests can drive it via `with_inspect_for_test`
    /// without needing a real Docker daemon or build context.
    pub async fn should_skip_build(&self) -> bool {
        let exists = (self.inspect_runner)(inspect_args(&self.versioned_image_tag)).await;
        matches!(image_cache_decision(exists), ImageCacheDecision::Skip)
    }

    /// W3 warm-start: if `<app_dir>/image.tar.gz` exists, call `docker load
    /// -i <path>` via the injected tar-load runner.
    ///
    /// Returns `true` if the tar was present AND the load succeeded (the
    /// image is now in the daemon store so W2's inspect will return Skip).
    /// Returns `false` if the tar is absent (source-only app) OR the load
    /// failed (falls through to the W2 build/cache path).
    ///
    /// Exposed as a method so tests can drive it via
    /// [`Self::with_tar_load_for_test`] without a real Docker daemon or tar.
    pub async fn load_image_tar(&self, app_dir: &std::path::Path) -> bool {
        let tar_path = app_dir.join("image.tar.gz");
        if !tar_path.is_file() {
            return false;
        }
        let tar_str = tar_path.to_string_lossy().into_owned();
        (self.tar_load_runner)(load_args(&tar_str)).await
    }

    /// Extract the `host:port` from `container_base` (e.g.
    /// `http://127.0.0.1:49231` → `127.0.0.1:49231`). Used by `health()`.
    fn host_port(&self) -> Option<String> {
        let url = self.container_base.trim_start_matches("http://");
        if url.is_empty() {
            None
        } else {
            Some(url.to_owned())
        }
    }

    /// Poll the container app's HTTP server until it answers (any status) or
    /// [`READY_TIMEOUT`] elapses.
    async fn wait_until_ready(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            match self
                .client
                .get(&self.container_base)
                .timeout(READY_POLL)
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(READY_POLL).await;
                }
                Err(e) => bail!(
                    "container app at {} never became ready: {e}",
                    self.container_base
                ),
            }
        }
    }
}

impl AppRuntime for DockerRuntime {
    fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a> {
        // Delegate to the container-independent proxy core (wiremock-tested).
        Box::pin(proxy_request(&self.client, &self.container_base, request))
    }

    /// Probe whether the container's published port is reachable (TCP connect).
    /// Uses the injected `probe` closure so unit tests need no real Docker daemon.
    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        let addr = self.host_port();
        let probe = self.probe.clone();
        Box::pin(async move {
            match addr {
                Some(hp) if (probe)(&hp) => RuntimeHealth::Serving,
                Some(hp) => RuntimeHealth::Unavailable(format!(
                    "TCP connect to container port {hp} refused"
                )),
                None => RuntimeHealth::Unavailable("container base URL is malformed".to_owned()),
            }
        })
    }

    /// Watch for the container to exit unexpectedly. Calls the injected
    /// `exit_watcher` factory (production: `docker wait <container>`; tests:
    /// a pre-resolved future). Resolves to `ExitReason::Died(detail)` when
    /// the container stops.
    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
        // Invoke the factory to get a fresh future for this watch session.
        (self.exit_watcher)()
    }

    /// Graceful container teardown: `docker stop <container>` (SIGTERM →
    /// SIGKILL after grace period) then `docker rm <container>` (remove the
    /// stopped record). Both steps are best-effort and idempotent — a "no
    /// such container" error from either is silently ignored, so a second
    /// call (or a call after the container already stopped) is harmless.
    ///
    /// Uses the injected [`CommandRunner`] so unit tests can verify the
    /// exact commands issued without a real Docker daemon.
    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
        let container = self.container.clone();
        let runner = self.shutdown_runner.clone();
        Box::pin(async move {
            // Step 1: graceful stop (SIGTERM → SIGKILL). Best-effort.
            let stop_ok = (runner)(stop_args(&container)).await;
            if !stop_ok {
                tracing::warn!(
                    container = %container,
                    "docker stop returned non-zero or failed (container may already be gone)"
                );
            }
            // Step 2: remove the container record. Best-effort.
            let rm_ok = (runner)(rm_args(&container)).await;
            if !rm_ok {
                tracing::warn!(
                    container = %container,
                    "docker rm failed during shutdown (container may already be removed)"
                );
            }
        })
    }
}

impl Drop for DockerRuntime {
    fn drop(&mut self) {
        // Force-remove the container (kills it if running). Synchronous,
        // best-effort — we may be in an async drop context, and a quick
        // `docker rm -f` spawn is acceptable on teardown.
        match std::process::Command::new(&self.docker_bin)
            .args(["rm", "-f", &self.container])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            Ok(s) if s.success() => {}
            Ok(s) => {
                tracing::warn!(container = %self.container, code = ?s.code(), "docker rm -f nonzero exit");
            }
            Err(e) => {
                tracing::warn!(container = %self.container, error = %e, "docker rm -f failed");
            }
        }
    }
}

/// The container app port: `runtime.memory_mb`-style override isn't a thing
/// here, so it's simply the configured [`DockerConfig::app_port`]. Kept as a
/// function so a future per-app override has one place to live.
pub(super) const fn rt_app_port(_rt: &Runtime, cfg: &DockerConfig) -> u16 {
    cfg.app_port
}

/// Reserve an ephemeral loopback host port: bind `127.0.0.1:0`, read the
/// assigned port, drop the listener so docker can re-bind it.
pub(super) async fn reserve_loopback_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("reserve ephemeral loopback port")?;
    let port = listener.local_addr().context("local_addr")?.port();
    drop(listener);
    Ok(port)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::DockerConfig;
    use crate::manifest::Runtime;

    #[test]
    fn rt_app_port_uses_config() {
        let rt = Runtime {
            r#type: "docker".to_owned(),
            entry: "context.tar.gz".to_owned(),
            fuel_per_request: 0,
            memory_mb: 0,
            kernel: None,
            registry_ref: None,
        };
        let cfg = DockerConfig {
            app_port: 9000,
            ..DockerConfig::default()
        };
        assert_eq!(rt_app_port(&rt, &cfg), 9000);
    }

    #[tokio::test]
    async fn reserve_loopback_port_returns_a_nonzero_port() {
        let p = reserve_loopback_port().await.unwrap();
        assert_ne!(p, 0);
    }
}
