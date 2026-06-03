//! Tests for [`super`] — top-level docker module + protocol helpers + DockerRuntime traits.
#![allow(clippy::unwrap_used)]

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
    let rt = super::DockerRuntime::with_probe_for_test(
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
    let rt = super::DockerRuntime::with_probe_for_test(
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

    let rt = super::DockerRuntime::with_watcher_for_test(
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
    let rt = super::DockerRuntime::with_probe_for_test(
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

    let shutdown_runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        issued2.lock().unwrap().push(args);
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async { Ok(()) });
        fut
    });

    let rt = super::DockerRuntime::with_shutdown_for_test(
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

    let shutdown_runner: super::CommandRunner = Arc::new(move |_args: Vec<String>| {
        *cc.lock().unwrap() += 1;
        // Simulate "no such container" by returning Err (best-effort shutdown).
        let fut: BoxFut<'static, Result<(), String>> =
            Box::pin(async { Err("no such container".to_owned()) });
        fut
    });

    let rt = super::DockerRuntime::with_shutdown_for_test(
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

    let shutdown_runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        issued2.lock().unwrap().push(args);
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async { Ok(()) });
        fut
    });

    let rt: Arc<dyn AppRuntime> = Arc::new(super::DockerRuntime::with_shutdown_for_test(
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

    let rt = super::DockerRuntime::with_inspect_for_test(
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

    let rt = super::DockerRuntime::with_inspect_for_test(
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

    let rt = super::DockerRuntime::with_tar_load_for_test(
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

    let rt = super::DockerRuntime::with_tar_load_for_test(
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

    let rt = super::DockerRuntime::with_tar_load_for_test(
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

// ---- pull_args / push_args / tag_args (registry argv) -------------------

#[test]
fn pull_args_returns_correct_argv() {
    assert_eq!(
        pull_args("[fd5a::1]:5000/acme/app:abc"),
        vec!["pull", "[fd5a::1]:5000/acme/app:abc"]
    );
}

/// `push_args` must produce `["push", <ref>]` — the mirror of `pull_args`.
#[test]
fn push_args_returns_correct_argv() {
    assert_eq!(
        super::protocol::push_args("[fd5a::1]:5000/acme/app:sha"),
        vec!["push", "[fd5a::1]:5000/acme/app:sha"]
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
    use std::sync::{Arc, Mutex};

    let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let issued2 = issued.clone();

    let runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        issued2.lock().unwrap().push(args);
        Box::pin(async { Ok(()) })
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
    use std::sync::{Arc, Mutex};

    let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let cc = call_count.clone();

    let runner: super::CommandRunner = Arc::new(move |_args: Vec<String>| {
        *cc.lock().unwrap() += 1;
        Box::pin(async { Err("pull failed".to_owned()) }) // pull fails
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

// ---- push_image seam (injected runner) -----------------------------------

/// When the runner succeeds for both tag and push, `push_image` must issue
/// `["tag", <local_tag>, <ref>]` then `["push", <ref>]` in that order and
/// return `true`.
#[tokio::test]
async fn push_image_issues_tag_then_push_on_success() {
    use std::sync::{Arc, Mutex};

    let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let issued2 = issued.clone();

    let runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        issued2.lock().unwrap().push(args);
        Box::pin(async { Ok(()) })
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v1",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    assert!(result.is_ok(), "both tag + push succeed → must return Ok");

    let cmds = issued.lock().unwrap();
    assert_eq!(cmds.len(), 2, "must issue exactly 2 commands (tag + push)");
    assert_eq!(
        cmds[0],
        vec![
            "tag".to_owned(),
            "tbf-img-uuid-v1".to_owned(),
            "[fd5a::1]:5000/acme/app:sha".to_owned(),
        ],
        "first command must be docker tag <local_tag> <ref>"
    );
    assert_eq!(
        cmds[1],
        vec!["push".to_owned(), "[fd5a::1]:5000/acme/app:sha".to_owned()],
        "second command must be docker push <ref>"
    );
}

/// When the runner fails on the tag step, `push_image` must return `Err`
/// and NOT issue the push command.
#[tokio::test]
async fn push_image_returns_err_and_skips_push_on_tag_failure() {
    use std::sync::{Arc, Mutex};

    let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let cc = call_count.clone();

    let runner: super::CommandRunner = Arc::new(move |_args: Vec<String>| {
        *cc.lock().unwrap() += 1;
        Box::pin(async { Err("tag failed".to_owned()) }) // tag fails
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v2",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    assert!(result.is_err(), "tag fails → must return Err");
    assert_eq!(
        *call_count.lock().unwrap(),
        1,
        "must issue only the tag command (push must NOT be called on tag failure)"
    );
}

/// When the push step fails, `push_image` must surface the runner's exact
/// stderr text in the returned `Err` (so the build runner can bail with the
/// real registry diagnostic instead of just the image ref). The tag step
/// succeeds; the push step returns `Err("unauthorized: authentication
/// required")`, and that exact text must appear in the propagated error.
#[tokio::test]
async fn push_image_surfaces_push_stderr_in_err() {
    use crate::runtime::BoxFut;
    use std::sync::Arc;

    let runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        // tag succeeds; push fails with a registry auth error.
        let fut: BoxFut<'static, Result<(), String>> =
            if args.first().map(String::as_str) == Some("push") {
                Box::pin(async { Err("unauthorized: authentication required".to_owned()) })
            } else {
                Box::pin(async { Ok(()) })
            };
        fut
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v9",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    let err = result.expect_err("push failure → push_image must return Err");
    assert!(
        err.contains("unauthorized: authentication required"),
        "push_image Err must surface the runner's exact stderr; got: {err}"
    );
}

/// The build runner's docker path must bail with the registry stderr (not just
/// the image ref) when the supervisor-side `skopeo` push fails — confirming the
/// diagnostic survives all the way to the `run_docker_build` caller.
#[tokio::test]
async fn run_docker_build_bails_with_push_stderr() {
    use crate::build_backend::HostDockerBackend;
    use crate::git::GitRun;
    use crate::runner::build::{BuildJob, BuildKind, run_build};
    use crate::runtime::BoxFut;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();

    // Clone creates the src dir + a Dockerfile so the docker path proceeds.
    let git: GitRun = Arc::new(move |args: Vec<String>, _env| {
        let dest = args.last().cloned().unwrap_or_default();
        Box::pin(async move {
            std::fs::create_dir_all(&dest).ok();
            std::fs::write(format!("{dest}/Dockerfile"), "FROM scratch\n").unwrap();
            Ok(())
        })
    });

    // Build backend succeeds (image built locally).
    let build_runner: super::CommandRunner = Arc::new(|_args| Box::pin(async { Ok(()) }));
    let backend = HostDockerBackend::with_runner("docker".to_owned(), build_runner);

    // Skopeo push runner: the `skopeo copy` fails with the auth stderr.
    let skopeo_runner: super::CommandRunner = Arc::new(|args: Vec<String>| {
        let fut: BoxFut<'static, Result<(), String>> =
            if args.first().map(String::as_str) == Some("copy") {
                Box::pin(async { Err("unauthorized: authentication required".to_owned()) })
            } else {
                Box::pin(async { Ok(()) })
            };
        fut
    });

    let job = BuildJob {
        repo_url: "https://github.com/acme/app".into(),
        git_ref: "abc123".into(),
        tenant: "acme".into(),
        app_uuid: "u".into(),
        registry_ula: "[fd5a::1]:5000".into(),
        clone_token: None,
        push_token: None,
        build_kind: BuildKind::Docker,
        build_cmd: None,
        artifact_path: None,
    };

    let err = run_build(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .expect_err("push failure → run_build must bail")
        .to_string();

    assert!(
        err.contains("unauthorized: authentication required"),
        "run_build must bail with the registry stderr, not just the ref; got: {err}"
    );
}
