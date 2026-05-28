//! Pure-helper layer for [`super`]: `docker` CLI argument builders, the
//! availability-probe seam, deterministic image/container naming, decision
//! enums, and the hop-by-hop header filter + proxy core.
//!
//! Everything in this file is a pure function (or a small data enum) so it can
//! be unit-tested without invoking a real `docker` binary or daemon.

use anyhow::Context as _;

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

/// `docker push <ref>` argv (sans the leading binary): push the locally-tagged
/// image to the mesh OCI registry by its full ref (host:port/name:tag).
/// Used by the build-runner after `docker build` to publish the image into
/// the mesh registry so supervisors on other nodes can pull it.
///
/// Called via [`super::push_image`] → [`crate::build_backend::push_to_registry`]
/// and directly by the P3.4 `run_build` orchestration.
pub fn push_args(reff: &str) -> Vec<String> {
    vec!["push".to_owned(), reff.to_owned()]
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
