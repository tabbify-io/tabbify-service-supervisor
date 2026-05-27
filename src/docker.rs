//! Docker container runtime (third [`crate::runtime::AppRuntime`]).
//!
//! Hosts an app by BUILDING a container image from the app's source on the
//! supervisor (`docker build`) and RUNNING it (`docker run`), then proxying HTTP
//! to the app's server inside the container. The fetched artifact is a `.tar.gz`
//! of the app directory INCLUDING a `Dockerfile`; that gzipped tar IS a Docker
//! build context, so it is piped straight to `docker build -` on stdin.
//!
//! Unlike [`crate::firecracker`], Docker is **cross-platform**: Docker Desktop /
//! the engine runs on macOS + Linux alike, and this runtime only ever shells out
//! to the `docker` CLI. So there is NO `cfg(target_os)` split — one impl serves
//! every host. A host without a reachable Docker daemon simply can't host docker
//! apps: [`docker_available`] gates that (it also drives the `docker` mesh tag),
//! and [`DockerRuntime::launch`] returns a clear `Err` when the daemon is absent.
//!
//! ## How an app runs
//! 1. `docker build -t tabbify-app-<hash> -` ← the build-context tarball on stdin.
//! 2. `docker run -d --name tbf-<hash-seq> -p 127.0.0.1:<host_port>:<app_port>`
//!    publishes the container's app port onto an ephemeral LOOPBACK host port.
//! 3. Poll `http://127.0.0.1:<host_port>` until the app answers (bounded).
//! 4. `handle(req)` proxies the whole path to that loopback base via `reqwest`.
//! 5. [`Drop`] removes the container (`docker rm -f`), best-effort.

use crate::config::DockerConfig;

/// Is this host able to run Docker containers? True iff the Docker daemon is
/// reachable (`docker info` succeeds). A host where Docker isn't installed or
/// the daemon isn't running returns `false` and the supervisor degrades to
/// WASM-only (+ firecracker on KVM), refusing docker apps loudly.
#[must_use]
pub fn docker_available() -> bool {
    protocol::docker_available_with(|| default_docker_check(crate::config::DEFAULT_DOCKER_BIN))
}

/// Default Docker probe used by [`docker_available`]: run `<docker_bin> info`
/// and succeed iff it exits 0 (the daemon answered). `docker info` talks to the
/// daemon (unlike `docker version`, whose client half succeeds even with no
/// daemon), so a zero exit means we can actually build + run.
fn default_docker_check(docker_bin: &str) -> bool {
    std::process::Command::new(docker_bin)
        .arg("info")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Best-effort FULL image teardown for app `id` at `version` — the disk-reclaiming
/// half of a purge. `stop` / [`DockerRuntime`]'s `Drop` remove only the *container*,
/// leaving the built image on disk so a restart is fast; purge removes it.
///
/// Removes any containers created from the app's image FIRST (so the image is
/// not "in use" — independent of when the runtime's `Drop` removes its own
/// container), then force-removes the image. Best-effort: it logs but never
/// errors, so a purge still forgets the app + clears the cache even if Docker is
/// unreachable or already clean.
pub async fn purge_image(docker_bin: &str, id: &str, version: u64) {
    use tokio::process::Command;
    let tag = protocol::versioned_image_tag(id, version);

    // Remove containers built from this image (running or stopped) so `rmi`
    // isn't blocked by an in-use image.
    if let Ok(out) = Command::new(docker_bin)
        .args(["ps", "-aq", "--filter", &format!("ancestor={tag}")])
        .stdin(std::process::Stdio::null())
        .output()
        .await
    {
        let ids: Vec<&str> = std::str::from_utf8(&out.stdout)
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        if !ids.is_empty() {
            let mut rm = vec!["rm", "-f"];
            rm.extend(ids);
            let _ = Command::new(docker_bin)
                .args(rm)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
        }
    }

    // Force-remove the image itself.
    if let Err(e) = Command::new(docker_bin)
        .args(protocol::rmi_args(&tag))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
    {
        tracing::warn!(image = %tag, error = %e, "docker rmi -f failed during purge (continuing)");
    }
}

/// Cross-platform docker helpers: the `docker` CLI argument builders, the
/// availability-probe seam, the deterministic image tag, and the hop-by-hop
/// header filter + proxy core. Pure functions so they're unit-testable without
/// invoking a real `docker`.
mod protocol {
    /// Hop-by-hop headers (RFC 7230 §6.1) that MUST NOT be forwarded when
    /// proxying between the inbound request and the container, nor copied back
    /// from the container response. Lower-cased for case-insensitive match.
    const HOP_BY_HOP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
    ];

    /// [`super::docker_available`] with an injectable probe — lets tests assert
    /// the gate logic without a real Docker daemon.
    pub fn docker_available_with(check: impl Fn() -> bool) -> bool {
        check()
    }

    /// Deterministic container name for an app instance: the sanitized `id`
    /// plus a per-launch `seq` so repeated launches of the same app (e.g. after
    /// a stop/start) don't collide on the container name.
    pub fn container_name(id: &str, seq: u64) -> String {
        let sanitized: String = id
            .chars()
            .map(|c| {
                let c = c.to_ascii_lowercase();
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        format!("tbf-{sanitized}-{seq}")
    }

    /// `docker build` argv (sans the leading binary): tag the image `tag` and
    /// read the gzipped-tar build context from stdin (`-`). The supervisor pipes
    /// the fetched `.tar.gz` to the child's stdin.
    pub fn build_args(tag: &str) -> Vec<String> {
        vec![
            "build".to_owned(),
            "-t".to_owned(),
            tag.to_owned(),
            "-".to_owned(),
        ]
    }

    /// `docker run` argv (sans the leading binary): start detached (`-d`), name
    /// the container `name`, and publish the container's `app_port` onto an
    /// ephemeral LOOPBACK host port (`127.0.0.1:<host_port>:<app_port>`) so the
    /// app is reachable only from this host, then the `image` to run.
    pub fn run_args(name: &str, host_port: u16, app_port: u16, image: &str) -> Vec<String> {
        vec![
            "run".to_owned(),
            "-d".to_owned(),
            "--name".to_owned(),
            name.to_owned(),
            "-p".to_owned(),
            format!("127.0.0.1:{host_port}:{app_port}"),
            image.to_owned(),
        ]
    }

    /// `docker stop <name>` argv (sans the leading binary): ask the container to
    /// stop gracefully (SIGTERM, then SIGKILL after the default 10-second grace
    /// period). Used as the first step of the graceful [`super::DockerRuntime::shutdown`]
    /// before `docker rm` removes the container record.
    pub fn stop_args(name: &str) -> Vec<String> {
        vec!["stop".to_owned(), name.to_owned()]
    }

    /// `docker rm -f <name>` argv (sans the leading binary): force-remove the
    /// container (kills it if running). Used on teardown ([`super::DockerRuntime`]'s
    /// `Drop`).
    pub fn rm_args(name: &str) -> Vec<String> {
        vec!["rm".to_owned(), "-f".to_owned(), name.to_owned()]
    }

    /// `docker rmi -f <tag>` argv (sans the leading binary): force-remove the
    /// built image. Used on PURGE — `Drop` removes only the container, leaving
    /// the image on disk for a fast restart; purge reclaims it.
    pub fn rmi_args(tag: &str) -> Vec<String> {
        vec!["rmi".to_owned(), "-f".to_owned(), tag.to_owned()]
    }

    /// `docker load -i <tar_path>` argv (sans the leading binary): load a
    /// pre-built image tar into the local daemon. Docker auto-detects gzip so
    /// both plain and gzipped tars work. Used for the W3 warm-start path.
    pub fn load_args(tar_path: &str) -> Vec<String> {
        vec!["load".to_owned(), "-i".to_owned(), tar_path.to_owned()]
    }

    /// Content-stable image tag keyed by uuid + version: `tbf-img-<uuid>-v<N>`.
    /// DISTINCT from the per-run container name `tbf-<uuid>-<seq>`. The tag
    /// changes when the app version changes, so a new push never reuses a stale
    /// image, yet the same version on restart reuses the cached one.
    pub fn versioned_image_tag(uuid: &str, version: u64) -> String {
        let sanitized: String = uuid
            .chars()
            .map(|c| {
                let c = c.to_ascii_lowercase();
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        format!("tbf-img-{sanitized}-v{version}")
    }

    /// `docker image inspect <tag>` argv (sans the leading binary): exits 0 iff
    /// the image is present in the local daemon's store. Used to decide whether to
    /// skip the (slow) `docker build` on restart.
    pub fn inspect_args(tag: &str) -> Vec<String> {
        vec!["image".to_owned(), "inspect".to_owned(), tag.to_owned()]
    }

    /// `docker pull <ref>` argv (sans the leading binary): pull the image from
    /// the mesh OCI registry by its full ref (host:port/name:tag). The daemon
    /// must list the registry under `insecure-registries` for IPv6 or plain-HTTP
    /// registries.
    pub fn pull_args(reff: &str) -> Vec<String> {
        vec!["pull".to_owned(), reff.to_owned()]
    }

    /// `docker tag <ref> <vtag>` argv (sans the leading binary): alias the
    /// pulled image under the supervisor's versioned local tag `tbf-img-<uuid>-v<N>`
    /// so the W2 cache check and downstream run_args can use the stable tag.
    pub fn tag_args(reff: &str, vtag: &str) -> Vec<String> {
        vec!["tag".to_owned(), reff.to_owned(), vtag.to_owned()]
    }

    /// Decision from the registry-ref check: should we pull the image from the
    /// mesh OCI registry, or skip the pull (no ref configured)?
    #[derive(Debug, PartialEq, Eq)]
    pub enum PullDecision {
        /// Pull and tag the image using this OCI ref.
        Pull(String),
        /// No registry ref — skip the pull entirely.
        Skip,
    }

    /// Map an optional OCI image ref to a [`PullDecision`].
    /// When `image_ref` is `Some(r)`, we pull; when `None`, we skip.
    pub fn pull_decision(image_ref: Option<&str>) -> PullDecision {
        match image_ref {
            Some(r) => PullDecision::Pull(r.to_owned()),
            None => PullDecision::Skip,
        }
    }

    /// Decision from the image-cache check: did the inspect runner report that the
    /// image already exists (skip the build) or not (must build)?
    #[derive(Debug, PartialEq, Eq)]
    pub enum ImageCacheDecision {
        /// The image is present in the daemon's store — skip `docker build`.
        Skip,
        /// The image is absent — run `docker build` and tag it.
        Build,
    }

    /// Map an image-inspect result to a [`ImageCacheDecision`].
    /// `image_exists` is `true` if `docker image inspect <tag>` exited 0.
    pub fn image_cache_decision(image_exists: bool) -> ImageCacheDecision {
        if image_exists {
            ImageCacheDecision::Skip
        } else {
            ImageCacheDecision::Build
        }
    }

    /// Is `name` a hop-by-hop header (case-insensitive)?
    pub fn is_hop_by_hop(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        HOP_BY_HOP.iter().any(|h| *h == lower)
    }

    /// Copy `src` headers into `dst`, dropping hop-by-hop headers. Used both
    /// when forwarding the inbound request to the container and when relaying
    /// the container's response back out.
    pub fn copy_filtered_headers(src: &http::HeaderMap, dst: &mut http::HeaderMap) {
        for (name, value) in src {
            if !is_hop_by_hop(name.as_str()) {
                dst.append(name.clone(), value.clone());
            }
        }
    }

    /// Proxy one inbound request to `container_base` (e.g.
    /// `http://127.0.0.1:49231`) and buffer the container's response.
    ///
    /// The path+query is forwarded verbatim, method + non-hop-by-hop headers +
    /// body are sent on, and the container's status + filtered headers + body
    /// are relayed back. This is the container-independent core of the docker
    /// runtime's `handle`, so it can be exercised against a wiremock "fake
    /// container" on any platform.
    ///
    /// # Errors
    /// A transport failure talking to the container, or a malformed response.
    pub async fn proxy_request(
        client: &reqwest::Client,
        container_base: &str,
        request: http::Request<bytes::Bytes>,
    ) -> anyhow::Result<http::Response<bytes::Bytes>> {
        use anyhow::Context as _;

        let (parts, body) = request.into_parts();
        let path_and_query = parts
            .uri
            .path_and_query()
            .map_or_else(|| "/".to_owned(), |pq| pq.as_str().to_owned());
        let url = format!("{container_base}{path_and_query}");

        let mut out_headers = http::HeaderMap::new();
        copy_filtered_headers(&parts.headers, &mut out_headers);
        let upstream = client
            .request(parts.method, &url)
            .headers(out_headers)
            .body(body.to_vec())
            .send()
            .await
            .with_context(|| format!("proxy to container {url}"))?;

        let status = upstream.status();
        let mut resp = http::Response::builder().status(status);
        if let Some(h) = resp.headers_mut() {
            copy_filtered_headers(upstream.headers(), h);
        }
        let bytes = upstream
            .bytes()
            .await
            .context("collect container response body")?;
        resp.body(bytes).context("build proxied response")
    }
}

mod runtime_impl {
    use std::path::Path;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use anyhow::{Context, Result, bail};
    use bytes::Bytes;
    use http::Request;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::process::Command;

    use super::protocol::{
        ImageCacheDecision, PullDecision, build_args, container_name, image_cache_decision,
        inspect_args, load_args, proxy_request, pull_args, pull_decision, rm_args, run_args,
        stop_args, tag_args, versioned_image_tag,
    };
    use super::{DockerConfig, docker_available};
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
    type TcpProbe = Arc<dyn Fn(&str) -> bool + Send + Sync>;

    /// Exit-watch signal type: a shared future that resolves to an
    /// [`ExitReason`] when the container exits. Production uses
    /// [`container_exit_watcher`]; tests inject a pre-resolved future via a
    /// [`tokio::sync::watch`] channel or a one-shot sender so no real Docker
    /// daemon is needed.
    ///
    /// Wrapped in `Arc` so [`DockerRuntime`] is `Clone`-compatible and the
    /// future can be polled from multiple locations (both `watch_for_exit` calls
    /// on a cloned `Arc<dyn AppRuntime>` share the same underlying signal).
    type ExitWatcher = Arc<dyn Fn() -> BoxFut<'static, ExitReason> + Send + Sync>;

    /// Command-runner seam for [`DockerRuntime::shutdown`]: given a list of
    /// `docker` sub-command arguments (e.g. `["stop", "tbf-abc-0"]`), run the
    /// command and return whether it succeeded.
    ///
    /// Production: the real `docker` binary via [`tokio::process::Command`].
    /// Tests: an injected closure that records which commands were issued without
    /// invoking a real Docker daemon — the same injection pattern used by
    /// [`TcpProbe`] and [`ExitWatcher`].
    type CommandRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

    /// Image-inspect runner seam for the W2 build-cache check: given a list of
    /// `docker image inspect` arguments, run the command and return `true` iff
    /// the image is present (exit 0). Production: the real `docker` CLI.
    /// Tests: an injected closure so no real daemon is needed.
    type InspectRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

    /// Tar-load runner seam for the W3 warm-start path: given a list of
    /// `docker load` arguments (e.g. `["load", "-i", "/path/to/image.tar.gz"]`),
    /// run the command and return `true` iff it exits 0 (load succeeded).
    /// Production: the real `docker` CLI via [`production_command_runner`].
    /// Tests: an injected closure that records calls without a real daemon.
    type TarLoadRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

    /// Build the production [`CommandRunner`]: spawns `<docker_bin> <args>` and
    /// returns `true` iff the process exits 0. Best-effort: a spawn failure or
    /// non-zero exit both yield `false` (the shutdown path logs and continues).
    fn production_command_runner(docker_bin: String) -> CommandRunner {
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
            Self::launch_with_id(context, rt, cfg, &derive_id(context), 0, None).await
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
        ///
        /// # Errors
        /// See [`Self::launch`].
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
                    self::build_image(&cfg.docker_bin, &vtag, context, cfg.build_timeout_secs)
                        .await?;
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
                    None => {
                        RuntimeHealth::Unavailable("container base URL is malformed".to_owned())
                    }
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
    const fn rt_app_port(_rt: &Runtime, cfg: &DockerConfig) -> u16 {
        cfg.app_port
    }

    /// Pre-launch guards: the Docker daemon must be reachable AND the build
    /// context tarball must exist on disk. Pure (takes `available` + the path)
    /// so the clear-error messages are unit-testable without a real daemon — the
    /// `no-docker → clear Err` case the runtime-selection branch relies on.
    ///
    /// # Errors
    /// `available == false` (clear "requires a reachable Docker daemon"), or a
    /// missing context file.
    fn precheck(available: bool, context: &Path) -> Result<()> {
        if !available {
            bail!("docker runtime requires a reachable Docker daemon (`docker info` failed)");
        }
        if !context.is_file() {
            bail!("docker build context not found at {}", context.display());
        }
        Ok(())
    }

    /// Pull `reff` from the mesh OCI registry and immediately tag it as `vtag`
    /// (the supervisor's versioned local tag `tbf-img-<uuid>-v<N>`).
    ///
    /// Returns `true` only if BOTH `docker pull <reff>` AND `docker tag <reff>
    /// <vtag>` succeed; `false` on any failure (the caller falls through to the
    /// W2 / build path).
    ///
    /// Uses the injectable [`CommandRunner`] so tests can record the issued
    /// commands without a real Docker daemon.
    pub(crate) async fn pull_and_tag(
        _docker_bin: &str,
        reff: &str,
        vtag: &str,
        runner: &CommandRunner,
    ) -> bool {
        let pull_ok = (runner)(pull_args(reff)).await;
        if !pull_ok {
            return false;
        }
        (runner)(tag_args(reff, vtag)).await
    }

    /// `docker build -t <tag> -` with the build-context tarball at `context`
    /// streamed to the child's stdin, bounded by `timeout_secs`.
    async fn build_image(
        docker_bin: &str,
        tag: &str,
        context: &Path,
        timeout_secs: u64,
    ) -> Result<()> {
        let tarball = tokio::fs::read(context)
            .await
            .with_context(|| format!("read build context {}", context.display()))?;

        let mut child = Command::new(docker_bin)
            .args(build_args(tag))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn `{docker_bin} build`"))?;

        // Stream the gzipped-tar build context to docker's stdin, then close it
        // so docker sees EOF and starts the build.
        let mut stdin = child
            .stdin
            .take()
            .context("docker build child has no stdin")?;
        stdin
            .write_all(&tarball)
            .await
            .context("write build context to docker stdin")?;
        stdin.flush().await.context("flush docker stdin")?;
        drop(stdin);

        let output =
            tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
                .await
                .map_err(|_| anyhow::anyhow!("docker build timed out after {timeout_secs}s"))?
                .context("wait for docker build")?;

        if !output.status.success() {
            bail!(
                "docker build failed (exit {:?}): {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    /// Run a `docker <args>` command to completion, erroring on a non-zero exit
    /// with the captured stderr.
    async fn run_docker(docker_bin: &str, args: &[String]) -> Result<()> {
        let out = Command::new(docker_bin)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("spawn `{docker_bin} {}`", args.join(" ")))?;
        if !out.status.success() {
            bail!(
                "`docker {}` failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    /// Run a `docker <args>` command and return `true` iff it exits 0. Never
    /// errors: spawn failures or non-zero exits both yield `false`. Used for the
    /// W2 build-cache check (`docker image inspect`) where absence is normal.
    async fn run_docker_check(docker_bin: &str, args: &[String]) -> bool {
        match Command::new(docker_bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
        {
            Ok(s) => s.success(),
            Err(_) => false,
        }
    }

    /// Reserve an ephemeral loopback host port: bind `127.0.0.1:0`, read the
    /// assigned port, drop the listener so docker can re-bind it.
    async fn reserve_loopback_port() -> Result<u16> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("reserve ephemeral loopback port")?;
        let port = listener.local_addr().context("local_addr")?.port();
        drop(listener);
        Ok(port)
    }

    /// Derive a stable id from the build-context path (its parent dir name, which
    /// in the cache layout is `v<N>` under `apps/<uuid>/`, plus the file stem).
    /// Used only by [`DockerRuntime::launch`]; the registry calls
    /// [`DockerRuntime::launch_with_id`] with the real uuid.
    fn derive_id(context: &Path) -> String {
        context
            .file_stem()
            .and_then(|s| s.to_str())
            .map_or_else(|| "context".to_owned(), str::to_owned)
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    mod tests {
        use super::*;

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

        #[test]
        fn derive_id_uses_file_stem() {
            assert_eq!(
                derive_id(Path::new("/cache/apps/u/v1/context.tar.gz")),
                "context.tar"
            );
        }

        #[tokio::test]
        async fn reserve_loopback_port_returns_a_nonzero_port() {
            let p = reserve_loopback_port().await.unwrap();
            assert_ne!(p, 0);
        }

        #[test]
        fn precheck_without_docker_errors_clearly() {
            // available = false → the clear "no docker daemon" error, regardless
            // of the context path. This is the `no-docker → clear Err` arm.
            let err = precheck(false, Path::new("/whatever/context.tar.gz")).unwrap_err();
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("docker") && msg.contains("daemon"),
                "got: {err}"
            );
        }

        #[test]
        fn precheck_with_docker_but_missing_context_errors() {
            let err = precheck(true, Path::new("/does/not/exist.tar.gz")).unwrap_err();
            assert!(
                err.to_string().contains("build context not found"),
                "got: {err}"
            );
        }

        #[test]
        fn precheck_passes_when_available_and_context_present() {
            let f = tempfile::NamedTempFile::new().unwrap();
            assert!(precheck(true, f.path()).is_ok());
        }
    }
}

pub use runtime_impl::DockerRuntime;
#[cfg(test)]
pub(crate) use runtime_impl::pull_and_tag as pull_and_tag_for_test;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::protocol::{
        ImageCacheDecision, PullDecision, build_args, container_name, copy_filtered_headers,
        docker_available_with, image_cache_decision, inspect_args, is_hop_by_hop, proxy_request,
        pull_args, pull_decision, rm_args, rmi_args, run_args, stop_args, tag_args,
        versioned_image_tag,
    };

    #[test]
    fn docker_gate_reflects_the_injected_probe() {
        assert!(docker_available_with(|| true));
        assert!(!docker_available_with(|| false));
    }

    // ---- versioned image tag (W2 image cache) --------------------------------

    /// Content-stable image tag keyed by uuid + version: `tbf-img-<uuid>-v<N>`.
    /// Two builds of the same uuid at different versions must yield different tags.
    #[test]
    fn versioned_image_tag_encodes_uuid_and_version() {
        let uuid = "0191e7c2-1111-7222-8333-444455556666";
        assert_eq!(
            versioned_image_tag(uuid, 3),
            "tbf-img-0191e7c2-1111-7222-8333-444455556666-v3"
        );
    }

    #[test]
    fn versioned_image_tag_differs_across_versions() {
        let uuid = "abc123";
        assert_ne!(versioned_image_tag(uuid, 1), versioned_image_tag(uuid, 2));
    }

    #[test]
    fn versioned_image_tag_sanitizes_uuid() {
        // Upper-case and slashes in the id must be lower-cased / replaced with '-'.
        assert_eq!(versioned_image_tag("My/App", 1), "tbf-img-my-app-v1");
    }

    // ---- inspect_args (W2 image inspect argv) --------------------------------

    /// `docker image inspect <tag>` argv builder.
    #[test]
    fn inspect_args_returns_correct_argv() {
        assert_eq!(
            inspect_args("tbf-img-abc-v1"),
            vec!["image", "inspect", "tbf-img-abc-v1"]
        );
    }

    // ---- ImageCacheDecision (W2 skip-build seam) ----------------------------

    /// When the injected inspect runner reports the image EXISTS (exit 0),
    /// the decision must be `Skip`.
    #[test]
    fn image_cache_decision_skip_when_image_exists() {
        let decision = image_cache_decision(true);
        assert!(
            matches!(decision, ImageCacheDecision::Skip),
            "image present → must skip build; got {decision:?}"
        );
    }

    /// When the injected inspect runner reports the image is ABSENT (non-zero),
    /// the decision must be `Build`.
    #[test]
    fn image_cache_decision_build_when_image_absent() {
        let decision = image_cache_decision(false);
        assert!(
            matches!(decision, ImageCacheDecision::Build),
            "image absent → must build; got {decision:?}"
        );
    }

    // ---- purge targets versioned image tag -----------------------------------

    /// `purge_image` must target the `tbf-img-<uuid>-v<N>` versioned tag (not
    /// the old generic tag). Verified by composing `rmi_args(versioned_image_tag)`
    /// and confirming the resulting argv matches the purge contract.
    #[test]
    fn purge_rmi_targets_versioned_image_tag() {
        let uuid = "0191e7c2-1111-7222-8333-444455556666";
        let version = 7_u64;
        let tag = versioned_image_tag(uuid, version);
        // purge_image calls rmi_args(&tag) — verify the args are correct.
        assert_eq!(
            rmi_args(&tag),
            vec![
                "rmi".to_owned(),
                "-f".to_owned(),
                "tbf-img-0191e7c2-1111-7222-8333-444455556666-v7".to_owned(),
            ],
            "purge must pass the versioned image tag to docker rmi"
        );
    }

    #[test]
    fn container_name_is_prefixed_with_seq() {
        assert_eq!(container_name("abc", 0), "tbf-abc-0");
        assert_eq!(container_name("My/App", 7), "tbf-my-app-7");
    }

    #[test]
    fn build_args_tag_the_image_and_read_context_from_stdin() {
        assert_eq!(
            build_args("tabbify-app-x"),
            vec!["build", "-t", "tabbify-app-x", "-"]
        );
    }

    #[test]
    fn run_args_publish_app_port_on_loopback_host_port() {
        let args = run_args("tbf-x-0", 49231, 8080, "tabbify-app-x");
        assert_eq!(
            args,
            vec![
                "run",
                "-d",
                "--name",
                "tbf-x-0",
                "-p",
                "127.0.0.1:49231:8080",
                "tabbify-app-x",
            ]
        );
    }

    #[test]
    fn stop_args_graceful_stop_by_name() {
        assert_eq!(stop_args("tbf-x-0"), vec!["stop", "tbf-x-0"]);
    }

    #[test]
    fn rm_args_force_remove_by_name() {
        assert_eq!(rm_args("tbf-x-0"), vec!["rm", "-f", "tbf-x-0"]);
    }

    #[test]
    fn rmi_args_force_remove_image_by_tag() {
        assert_eq!(
            rmi_args("tabbify-app-x"),
            vec!["rmi", "-f", "tabbify-app-x"]
        );
    }

    #[test]
    fn hop_by_hop_detection_is_case_insensitive() {
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("transfer-encoding"));
        assert!(is_hop_by_hop("HOST"));
        assert!(!is_hop_by_hop("content-type"));
        assert!(!is_hop_by_hop("x-app-header"));
    }

    #[test]
    fn copy_filtered_headers_drops_hop_by_hop_keeps_the_rest() {
        let mut src = http::HeaderMap::new();
        src.insert("content-type", "application/json".parse().unwrap());
        src.insert("connection", "keep-alive".parse().unwrap());
        src.insert("host", "container.local".parse().unwrap());
        src.insert("x-custom", "abc".parse().unwrap());

        let mut dst = http::HeaderMap::new();
        copy_filtered_headers(&src, &mut dst);

        assert_eq!(dst.get("content-type").unwrap(), "application/json");
        assert_eq!(dst.get("x-custom").unwrap(), "abc");
        assert!(dst.get("connection").is_none());
        assert!(dst.get("host").is_none());
    }

    // The proxy core is tested against a wiremock "fake container" HTTP server —
    // the same path the docker `handle` uses, exercised on any platform.
    use bytes::Bytes;
    use wiremock::matchers::{header, header_exists, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn proxy_forwards_path_and_returns_container_body() {
        let container = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/data"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("x-app", "yes")
                    .set_body_string("hello from docker"),
            )
            .mount(&container)
            .await;

        let req = http::Request::builder()
            .method("GET")
            .uri("http://app-ula/api/data?q=1")
            .body(Bytes::new())
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &container.uri(), req)
            .await
            .expect("proxy");

        assert_eq!(resp.status(), 201);
        assert_eq!(resp.headers().get("x-app").unwrap(), "yes");
        assert_eq!(String::from_utf8_lossy(resp.body()), "hello from docker");
    }

    #[tokio::test]
    async fn proxy_strips_hop_by_hop_request_headers_before_forwarding() {
        let container = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(header("x-keep", "1"))
            .and(header_exists("content-type"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&container)
            .await;

        let req = http::Request::builder()
            .method("POST")
            .uri("http://app-ula/submit")
            .header("connection", "keep-alive")
            .header("x-keep", "1")
            .header("content-type", "text/plain")
            .body(Bytes::from_static(b"payload"))
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &container.uri(), req)
            .await
            .expect("proxy");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "ok");
    }

    // ---- health() contract for DockerRuntime --------------------------------

    /// A DockerRuntime whose probe is faked to return "reachable" must report
    /// RuntimeHealth::Serving.
    #[tokio::test]
    async fn docker_health_serving_when_probe_reachable() {
        use crate::runtime::{AppRuntime, RuntimeHealth};
        use std::sync::Arc;
        let rt = super::runtime_impl::DockerRuntime::with_probe_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-0",
            Arc::new(|_addr: &str| true),
        );
        assert_eq!(rt.health().await, RuntimeHealth::Serving);
    }

    /// A DockerRuntime whose probe is faked to return "unreachable" must report
    /// RuntimeHealth::Unavailable.
    #[tokio::test]
    async fn docker_health_unavailable_when_probe_unreachable() {
        use crate::runtime::{AppRuntime, RuntimeHealth};
        use std::sync::Arc;
        let rt = super::runtime_impl::DockerRuntime::with_probe_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-0",
            Arc::new(|_addr: &str| false),
        );
        assert!(
            matches!(rt.health().await, RuntimeHealth::Unavailable(_)),
            "must be Unavailable when probe returns false"
        );
    }

    // ---- watch_for_exit() contract for DockerRuntime -------------------------

    /// A DockerRuntime with an injected exit watcher that resolves immediately
    /// must return ExitReason::Died when watch_for_exit is awaited.
    #[tokio::test]
    async fn docker_watch_for_exit_resolves_died_with_injected_watcher() {
        use crate::runtime::{AppRuntime, BoxFut, ExitReason};
        use std::sync::Arc;

        let exit_watcher: Arc<dyn Fn() -> BoxFut<'static, ExitReason> + Send + Sync> =
            Arc::new(|| {
                let fut: BoxFut<'static, ExitReason> = Box::pin(async {
                    ExitReason::Died("container tbf-test-1 exited with code 1".to_owned())
                });
                fut
            });

        let rt = super::runtime_impl::DockerRuntime::with_watcher_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-1",
            Arc::new(|_addr: &str| true),
            exit_watcher,
        );

        let reason = rt.watch_for_exit().await;
        assert_eq!(
            reason,
            ExitReason::Died("container tbf-test-1 exited with code 1".to_owned()),
            "watch_for_exit must resolve to Died when the injected watcher fires"
        );
    }

    /// A DockerRuntime with the default pending exit watcher must NOT resolve
    /// within a short timeout — confirming the default (from with_probe_for_test)
    /// is indeed pending.
    #[tokio::test]
    async fn docker_watch_for_exit_default_is_pending() {
        use crate::runtime::AppRuntime;
        use std::sync::Arc;
        let rt = super::runtime_impl::DockerRuntime::with_probe_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-2",
            Arc::new(|_addr: &str| true),
        );
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rt.watch_for_exit()).await;
        assert!(
            result.is_err(),
            "default exit watcher for tests must be pending"
        );
    }

    // ---- shutdown() contract for DockerRuntime --------------------------------

    /// DockerRuntime::shutdown must issue `docker stop <container>` followed by
    /// `docker rm <container>` — in that order — via the injected command runner.
    /// No real Docker daemon required.
    #[tokio::test]
    async fn docker_shutdown_issues_stop_then_rm() {
        use crate::runtime::{AppRuntime, BoxFut};
        use std::sync::{Arc, Mutex};

        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let shutdown_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |args: Vec<String>| {
                issued2.lock().unwrap().push(args);
                let fut: BoxFut<'static, bool> = Box::pin(async { true });
                fut
            });

        let rt = super::runtime_impl::DockerRuntime::with_shutdown_for_test(
            "http://127.0.0.1:49999",
            "tbf-shutdown-0",
            shutdown_runner,
        );

        rt.shutdown().await;

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 2, "must issue exactly 2 commands (stop + rm)");
        assert_eq!(
            cmds[0],
            vec!["stop".to_owned(), "tbf-shutdown-0".to_owned()],
            "first command must be docker stop <container>"
        );
        assert_eq!(
            cmds[1],
            vec![
                "rm".to_owned(),
                "-f".to_owned(),
                "tbf-shutdown-0".to_owned()
            ],
            "second command must be docker rm -f <container>"
        );
    }

    /// Calling shutdown twice is idempotent: the second call still issues
    /// stop + rm without panicking (the container may already be gone; the
    /// runner records both calls as best-effort).
    #[tokio::test]
    async fn docker_shutdown_is_idempotent() {
        use crate::runtime::{AppRuntime, BoxFut};
        use std::sync::{Arc, Mutex};

        let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let cc = call_count.clone();

        let shutdown_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |_args: Vec<String>| {
                *cc.lock().unwrap() += 1;
                // Simulate "no such container" on the second pass by returning false.
                let fut: BoxFut<'static, bool> = Box::pin(async { false });
                fut
            });

        let rt = super::runtime_impl::DockerRuntime::with_shutdown_for_test(
            "http://127.0.0.1:49999",
            "tbf-shutdown-1",
            shutdown_runner,
        );

        // First call.
        rt.shutdown().await;
        // Second call — must not panic even if commands return false.
        rt.shutdown().await;

        // 2 commands per call × 2 calls = 4 total.
        assert_eq!(
            *call_count.lock().unwrap(),
            4,
            "two shutdown calls must each issue 2 commands (stop + rm)"
        );
    }

    /// DockerRuntime::shutdown via the AppRuntime trait object also issues the
    /// two commands (confirms the override is dispatched through dyn dispatch).
    #[tokio::test]
    async fn docker_shutdown_via_trait_object_issues_stop_then_rm() {
        use crate::runtime::{AppRuntime, BoxFut};
        use std::sync::{Arc, Mutex};

        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let shutdown_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |args: Vec<String>| {
                issued2.lock().unwrap().push(args);
                let fut: BoxFut<'static, bool> = Box::pin(async { true });
                fut
            });

        let rt: Arc<dyn AppRuntime> =
            Arc::new(super::runtime_impl::DockerRuntime::with_shutdown_for_test(
                "http://127.0.0.1:49999",
                "tbf-shutdown-2",
                shutdown_runner,
            ));

        rt.shutdown().await;

        let cmds = issued.lock().unwrap();
        assert_eq!(
            cmds.len(),
            2,
            "must issue exactly 2 commands via trait object"
        );
        assert_eq!(cmds[0][0], "stop", "first command must be stop");
        assert_eq!(cmds[1][0], "rm", "second command must be rm");
    }

    // ---- W2 image-cache seam (with_inspect_for_test) -------------------------

    /// DockerRuntime::with_inspect_for_test wires a recording inspect runner so
    /// tests can observe that `docker image inspect <tag>` was called with the
    /// expected versioned image tag.
    ///
    /// When the injected runner returns `true` (image exists), the runtime must
    /// record one inspect call with the correct versioned tag and no build call.
    #[tokio::test]
    async fn docker_inspect_runner_receives_versioned_tag_when_image_exists() {
        use crate::runtime::BoxFut;
        use std::sync::{Arc, Mutex};

        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        // Inject inspect runner: records args + reports image exists (true).
        let inspect_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |args: Vec<String>| {
                issued2.lock().unwrap().push(args);
                Box::pin(async { true })
            });

        let rt = super::runtime_impl::DockerRuntime::with_inspect_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-inspect-0",
            "abc123",
            5,
            inspect_runner,
        );

        // Call should_skip_build — verifies the seam fires and inspect args are correct.
        let skip = rt.should_skip_build().await;
        assert!(skip, "image present → must skip build");

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 1, "must issue exactly one inspect call");
        assert_eq!(
            cmds[0],
            vec![
                "image".to_owned(),
                "inspect".to_owned(),
                "tbf-img-abc123-v5".to_owned(),
            ],
            "inspect must use versioned image tag tbf-img-<uuid>-v<N>"
        );
    }

    /// When the injected inspect runner returns `false` (image absent), the
    /// runtime must report that the build is NOT skipped.
    #[tokio::test]
    async fn docker_inspect_runner_build_required_when_image_absent() {
        use crate::runtime::BoxFut;
        use std::sync::Arc;

        let inspect_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(|_args: Vec<String>| Box::pin(async { false }));

        let rt = super::runtime_impl::DockerRuntime::with_inspect_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-inspect-1",
            "abc123",
            5,
            inspect_runner,
        );

        let skip = rt.should_skip_build().await;
        assert!(!skip, "image absent → must NOT skip build");
    }

    // ---- load_args (W3 tar-load argv) ----------------------------------------

    #[test]
    fn load_args_returns_correct_argv() {
        assert_eq!(
            super::protocol::load_args("/cache/apps/abc/v3/image.tar.gz"),
            vec!["load", "-i", "/cache/apps/abc/v3/image.tar.gz"]
        );
    }

    // ---- W3 warm-start: load_image_tar seam ----------------------------------

    /// When `image.tar.gz` is present in the app dir, `load_image_tar` must call
    /// the tar-load runner with `["load", "-i", "<path>"]` and return `true` on
    /// a successful load.
    #[tokio::test]
    async fn docker_load_image_tar_invokes_runner_with_tar_path() {
        use crate::runtime::BoxFut;
        use std::sync::{Arc, Mutex};

        let tmp = tempfile::TempDir::new().unwrap();
        // Create a (dummy) image.tar.gz in the temp dir.
        let tar_path = tmp.path().join("image.tar.gz");
        std::fs::write(&tar_path, b"fake-tar").unwrap();

        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let tar_load_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |args: Vec<String>| {
                issued2.lock().unwrap().push(args);
                Box::pin(async { true }) // simulate successful load
            });

        let rt = super::runtime_impl::DockerRuntime::with_tar_load_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-tarload-0",
            "uuid-warm",
            3,
            tar_load_runner,
        );

        let loaded = rt.load_image_tar(tmp.path()).await;
        assert!(loaded, "tar present + load succeeded → must return true");

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 1, "must issue exactly one docker load call");
        assert_eq!(cmds[0][0], "load", "first arg must be 'load'");
        assert_eq!(cmds[0][1], "-i", "second arg must be '-i'");
        assert!(
            cmds[0][2].ends_with("image.tar.gz"),
            "third arg must be the tar path; got {:?}",
            cmds[0][2]
        );
    }

    /// When the app dir does NOT contain `image.tar.gz` (source-only app),
    /// `load_image_tar` must return `false` WITHOUT calling the tar-load runner.
    #[tokio::test]
    async fn docker_load_image_tar_no_tar_returns_false_without_invoking_runner() {
        use crate::runtime::BoxFut;
        use std::sync::{Arc, Mutex};

        let tmp = tempfile::TempDir::new().unwrap();
        // No image.tar.gz in the dir.

        let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let cc = call_count.clone();

        let tar_load_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |_args: Vec<String>| {
                *cc.lock().unwrap() += 1;
                Box::pin(async { false })
            });

        let rt = super::runtime_impl::DockerRuntime::with_tar_load_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-tarload-1",
            "uuid-source",
            1,
            tar_load_runner,
        );

        let loaded = rt.load_image_tar(tmp.path()).await;
        assert!(!loaded, "no tar → must return false");
        assert_eq!(
            *call_count.lock().unwrap(),
            0,
            "runner must NOT be called when no tar is present"
        );
    }

    /// When `image.tar.gz` is present but `docker load` fails (runner returns
    /// false), `load_image_tar` must return `false` so the caller falls through
    /// to the W2 build/cache path.
    #[tokio::test]
    async fn docker_load_image_tar_failed_load_returns_false() {
        use crate::runtime::BoxFut;
        use std::sync::Arc;

        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("image.tar.gz"), b"bad-tar").unwrap();

        let tar_load_runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(|_args| Box::pin(async { false })); // simulate load failure

        let rt = super::runtime_impl::DockerRuntime::with_tar_load_for_test(
            "http://127.0.0.1:49999",
            "tbf-test-tarload-2",
            "uuid-fail",
            2,
            tar_load_runner,
        );

        let loaded = rt.load_image_tar(tmp.path()).await;
        assert!(
            !loaded,
            "failed load → must return false (fall through to W2)"
        );
    }

    // ---- pull_args / tag_args (registry pull argv) ---------------------------

    #[test]
    fn pull_args_returns_correct_argv() {
        assert_eq!(
            pull_args("[fd5a::1]:5000/acme/app:abc"),
            vec!["pull", "[fd5a::1]:5000/acme/app:abc"]
        );
    }

    #[test]
    fn tag_args_returns_correct_argv() {
        assert_eq!(
            tag_args("[fd5a::1]:5000/acme/app:abc", "tbf-img-uuid-v3"),
            vec!["tag", "[fd5a::1]:5000/acme/app:abc", "tbf-img-uuid-v3"]
        );
    }

    // ---- PullDecision (registry-pull seam) -----------------------------------

    /// When `image_ref` is `Some`, `pull_decision` must return `Pull(ref)`.
    /// When `None`, it must return `Skip`.
    #[test]
    fn pull_decision_uses_ref_when_present_else_skips() {
        assert_eq!(
            pull_decision(Some("[fd5a::1]:5000/acme/app:abc")),
            PullDecision::Pull("[fd5a::1]:5000/acme/app:abc".to_owned())
        );
        assert_eq!(pull_decision(None), PullDecision::Skip);
    }

    // ---- pull_and_tag seam (injected runner) ---------------------------------

    /// When the runner succeeds for both pull and tag, `pull_and_tag` must
    /// issue `["pull", <ref>]` then `["tag", <ref>, <vtag>]` in order and
    /// return `true`.
    #[tokio::test]
    async fn pull_and_tag_issues_pull_then_tag_on_success() {
        use crate::runtime::BoxFut;
        use std::sync::{Arc, Mutex};

        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |args: Vec<String>| {
                issued2.lock().unwrap().push(args);
                Box::pin(async { true })
            });

        let ok = super::pull_and_tag_for_test(
            "docker",
            "[fd5a::1]:5000/acme/app:sha",
            "tbf-img-uuid-v1",
            &runner,
        )
        .await;

        assert!(ok, "both pull + tag succeed → must return true");

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 2, "must issue exactly 2 commands (pull + tag)");
        assert_eq!(
            cmds[0],
            vec!["pull".to_owned(), "[fd5a::1]:5000/acme/app:sha".to_owned()],
            "first command must be docker pull <ref>"
        );
        assert_eq!(
            cmds[1],
            vec![
                "tag".to_owned(),
                "[fd5a::1]:5000/acme/app:sha".to_owned(),
                "tbf-img-uuid-v1".to_owned(),
            ],
            "second command must be docker tag <ref> <vtag>"
        );
    }

    /// When the runner fails on pull, `pull_and_tag` must return `false` and
    /// NOT issue the tag command.
    #[tokio::test]
    async fn pull_and_tag_returns_false_and_skips_tag_on_pull_failure() {
        use crate::runtime::BoxFut;
        use std::sync::{Arc, Mutex};

        let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let cc = call_count.clone();

        let runner: Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync> =
            Arc::new(move |_args: Vec<String>| {
                *cc.lock().unwrap() += 1;
                Box::pin(async { false }) // pull fails
            });

        let ok = super::pull_and_tag_for_test(
            "docker",
            "[fd5a::1]:5000/acme/app:sha",
            "tbf-img-uuid-v2",
            &runner,
        )
        .await;

        assert!(!ok, "pull fails → must return false");
        assert_eq!(
            *call_count.lock().unwrap(),
            1,
            "must issue only the pull command (tag must NOT be called on pull failure)"
        );
    }
}
