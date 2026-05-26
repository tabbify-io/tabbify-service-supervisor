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

/// Best-effort FULL image teardown for app `id` — the disk-reclaiming half of a
/// purge. `stop` / [`DockerRuntime`]'s `Drop` remove only the *container*,
/// leaving the built image on disk so a restart is fast; purge removes it.
///
/// Removes any containers created from the app's image FIRST (so the image is
/// not "in use" — independent of when the runtime's `Drop` removes its own
/// container), then force-removes the image. Best-effort: it logs but never
/// errors, so a purge still forgets the app + clears the cache even if Docker is
/// unreachable or already clean.
pub async fn purge_image(docker_bin: &str, id: &str) {
    use tokio::process::Command;
    let tag = protocol::image_tag(id);

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

    /// Deterministic image tag for an app build context identified by `id`
    /// (the app uuid, or a content hash). Lower-cased + sanitized to the
    /// `[a-z0-9_.-]` Docker repository-name charset so an arbitrary id can't
    /// produce an invalid tag.
    pub fn image_tag(id: &str) -> String {
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
        format!("tabbify-app-{sanitized}")
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use anyhow::{Context, Result, bail};
    use bytes::Bytes;
    use http::Request;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::process::Command;

    use super::protocol::{
        build_args, container_name, image_tag, proxy_request, rm_args, run_args,
    };
    use super::{DockerConfig, docker_available};
    use crate::manifest::Runtime;
    use crate::runtime::{AppRuntime, BoxRespFut};

    /// How long to wait for the container app's HTTP server to come up.
    const READY_TIMEOUT: Duration = Duration::from_secs(30);
    /// Poll interval while waiting for the container app.
    const READY_POLL: Duration = Duration::from_millis(250);

    /// Monotonic per-process counter → a unique container name per launch, so
    /// repeated launches of the same app (stop→start, re-host) don't collide.
    static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

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
            Self::launch_with_id(context, rt, cfg, &derive_id(context)).await
        }

        /// [`Self::launch`] with an explicit `id` for the image tag / container
        /// name (the registry passes the app uuid; tests can pin one).
        ///
        /// # Errors
        /// See [`Self::launch`].
        pub async fn launch_with_id(
            context: &Path,
            rt: &Runtime,
            cfg: &DockerConfig,
            id: &str,
        ) -> Result<Self> {
            precheck(docker_available(), context)?;

            let tag = image_tag(id);
            self::build_image(&cfg.docker_bin, &tag, context, cfg.build_timeout_secs).await?;

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
                &run_args(&container, host_port, app_port, &tag),
            )
            .await
            .context("docker run")?;

            let me = Self {
                container,
                docker_bin: cfg.docker_bin.clone(),
                container_base: format!("http://127.0.0.1:{host_port}"),
                client: reqwest::Client::new(),
            };

            // On any readiness failure `me` drops → container force-removed.
            me.wait_until_ready().await?;
            Ok(me)
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::protocol::{
        build_args, container_name, copy_filtered_headers, docker_available_with, image_tag,
        is_hop_by_hop, proxy_request, rm_args, rmi_args, run_args,
    };

    #[test]
    fn docker_gate_reflects_the_injected_probe() {
        assert!(docker_available_with(|| true));
        assert!(!docker_available_with(|| false));
    }

    #[test]
    fn image_tag_is_prefixed_and_sanitized() {
        assert_eq!(
            image_tag("0191e7c2-1111-7222-8333-444455556666"),
            "tabbify-app-0191e7c2-1111-7222-8333-444455556666"
        );
        // Uppercase + illegal chars are lower-cased / replaced with '-'.
        assert_eq!(image_tag("My/App:v2"), "tabbify-app-my-app-v2");
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
}
