//! Integration tests (contract §5, §6, §10): the S3 fetcher against a wiremock
//! stand-in for S3, the axum CONTROL API driven via `tower::ServiceExt::oneshot`,
//! and — the core of Component 3 — per-app-ULA HOSTING: registering / starting
//! an app binds a dedicated listener (loopback in tests, since no TUN) whose
//! WHOLE path serves the WASM fixture; stop / idle-reap tears it down.
//!
//! These use real behavior with minimal mocks: the only mock is the HTTP object
//! store (wiremock); the registry, lifecycle, WASM runtime, and the per-app
//! listeners are exercised for real against `tests/fixtures/hello.wasm` over an
//! actual loopback TCP socket.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tabbify_supervisor::api::{SupervisorState, router};
use tabbify_supervisor::fetcher::{FetchError, S3Fetcher};
use tabbify_supervisor::host::AppHost;
use tabbify_supervisor::registry::{AppRegistry, AppState};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

/// The committed pure-proxy fixture (compiled wasi:http/proxy component).
const HELLO_WASM: &[u8] = include_bytes!("fixtures/hello.wasm");

/// `on_request` manifest for the fixture.
const ON_REQUEST_MANIFEST: &str = r#"
[app]
name        = "hello-tabbify"
kind        = "headless"
description = "fixture"

[lifecycle]
mode             = "on_request"
idle_timeout_sec = 300

[runtime]
type             = "wasm-http"
entry            = "app.wasm"
fuel_per_request = 1000000000
memory_mb        = 64

[routes]
dynamic_prefixes = ["/"]
"#;

/// `always_on` manifest variant.
const ALWAYS_ON_MANIFEST: &str = r#"
[app]
name = "hello-always"

[lifecycle]
mode = "always_on"

[runtime]
"#;

/// `on_request` manifest with a near-zero idle timeout so the reaper fires
/// without waiting in real time.
const FAST_REAP_MANIFEST: &str = r#"
[app]
name = "hello-fastreap"

[lifecycle]
mode             = "on_request"
idle_timeout_sec = 0

[runtime]
"#;

/// A `firecracker`-runtime manifest. Its entry is a rootfs image (not wasm).
const FIRECRACKER_MANIFEST: &str = r#"
[app]
name = "vm-app"

[lifecycle]
mode = "always_on"

[runtime]
type      = "firecracker"
entry     = "rootfs.ext4"
memory_mb = 256
"#;

/// A manifest naming an unknown runtime type — must be rejected.
const UNKNOWN_RUNTIME_MANIFEST: &str = r#"
[app]
name = "weird"

[lifecycle]
mode = "always_on"

[runtime]
type  = "quantum-vm"
entry = "app.bin"
"#;

/// Stand up a wiremock S3 that serves `latest`, `manifest.toml`, `app.wasm`
/// for `APP_UUID` at version 1, with the given manifest body.
async fn mock_s3(manifest: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/app.wasm")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(HELLO_WASM.to_vec()))
        .mount(&server)
        .await;
    server
}

/// Like [`mock_s3`] but serves an arbitrary entry filename with arbitrary
/// bytes (used to stand up a `firecracker`/unknown manifest whose entry is a
/// rootfs image rather than `app.wasm`).
async fn mock_s3_entry(manifest: &str, entry: &str, bytes: &[u8]) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/{entry}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes.to_vec()))
        .mount(&server)
        .await;
    server
}

/// Loopback-hosted registry: no mesh/TUN, so per-app listeners bind `[::1]:0`
/// and tests dial the address the registry reports in its summary.
fn temp_registry(base_url: &str) -> (AppRegistry, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fetcher = S3Fetcher::new(base_url, dir.path());
    (AppRegistry::new(fetcher, AppHost::loopback()), dir)
}

fn test_state(registry: AppRegistry) -> SupervisorState {
    SupervisorState::new(registry, "test-supervisor".into(), "fd5a:1f00:1::1".into())
}

/// GET the hosted app's per-app listener directly (the address the registry
/// bound it on) and return (status, body string).
async fn dial_app(registry: &AppRegistry, uuid: &str, path: &str) -> (StatusCode, String) {
    let addr = registry
        .get(uuid)
        .expect("app known")
        .bound_addr
        .expect("app hosted (bound_addr present)");
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("dial per-app listener");
    let status = resp.status();
    let body = resp.text().await.expect("body");
    (StatusCode::from_u16(status.as_u16()).unwrap(), body)
}

// ---------------------------------------------------------------------------
// Fetcher (wiremock standing in for S3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetcher_reads_latest_then_manifest_and_wasm() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let dir = tempfile::tempdir().unwrap();
    let fetcher = S3Fetcher::new(server.uri(), dir.path());

    let fetched = fetcher.fetch(APP_UUID).await.expect("fetch");
    assert_eq!(fetched.version, 1);
    assert_eq!(fetched.manifest.app.name, "hello-tabbify");
    assert_eq!(fetched.wasm.len(), HELLO_WASM.len());

    let cache = fetcher.cache_dir(APP_UUID, 1);
    assert!(cache.join("manifest.toml").is_file());
    assert!(cache.join("app.wasm").is_file());
}

#[tokio::test]
async fn fetcher_missing_app_is_not_found() {
    let server = MockServer::start().await; // empty → 404
    let dir = tempfile::tempdir().unwrap();
    let fetcher = S3Fetcher::new(server.uri(), dir.path());

    let err = fetcher.fetch(APP_UUID).await.unwrap_err();
    assert!(matches!(err, FetchError::NotFound(_)), "got {err:?}");
}

#[tokio::test]
async fn fetcher_bad_latest_body_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-a-number"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let fetcher = S3Fetcher::new(server.uri(), dir.path());

    let err = fetcher.latest_version(APP_UUID).await.unwrap_err();
    assert!(matches!(err, FetchError::BadLatest { .. }), "got {err:?}");
}

// ---------------------------------------------------------------------------
// CONTROL API: health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_reports_ok_with_identity() {
    let (registry, _dir) = temp_registry("http://unused.invalid");
    let app = router(test_state(registry));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = json_body(resp).await;
    assert_eq!(body["status"], "ok");
    assert_eq!(body["supervisor_id"], "test-supervisor");
    assert_eq!(body["ula"], "fd5a:1f00:1::1");
}

// ---------------------------------------------------------------------------
// CONTROL API: GET /v1/apps/:uuid  (present / absent against the mocked S3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_app_present_when_fetchable_from_s3() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    let app = router(test_state(registry));

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/apps/{APP_UUID}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = json_body(resp).await;
    assert_eq!(body["present"], true);
    assert_eq!(body["version"], 1);
    // app_ula is the deterministic per-app ULA (golden prefix).
    assert_eq!(body["app_ula"], "fd5a:1f02:44a5:240b:121a::1");
    // on_request + not started → not hosted yet.
    assert!(body["bound_addr"].is_null());
}

#[tokio::test]
async fn get_app_absent_returns_not_present() {
    let server = MockServer::start().await; // empty → 404
    let (registry, _dir) = temp_registry(&server.uri());
    let app = router(test_state(registry));

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/apps/{APP_UUID}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: Value = json_body(resp).await;
    assert_eq!(body["present"], false);
}

// ---------------------------------------------------------------------------
// CONTROL API: GET /v1/apps listing reflects hosted state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_apps_reflects_registered_app_with_app_ula() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    let state = registry.register(APP_UUID).await.expect("register");
    assert_eq!(state, AppState::Available); // on_request → available, not hosted

    let app = router(test_state(registry));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/apps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = json_body(resp).await;
    let apps = body["apps"].as_array().unwrap();
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0]["uuid"], APP_UUID);
    assert_eq!(apps[0]["name"], "hello-tabbify");
    assert_eq!(apps[0]["lifecycle"], "on_request");
    assert_eq!(apps[0]["state"], "available");
    assert_eq!(apps[0]["app_ula"], "fd5a:1f02:44a5:240b:121a::1");
    assert!(apps[0]["bound_addr"].is_null(), "not hosted yet");
}

// ---------------------------------------------------------------------------
// HOSTING: a hosted app's per-app listener serves the WASM fixture end-to-end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_request_lazy_hosts_and_per_app_listener_serves_wasm() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    // First reference (pin=false, the on-request lazy-host path) hosts the app.
    let state = registry
        .ensure_running(APP_UUID, false)
        .await
        .expect("host");
    assert_eq!(state, AppState::Running);

    // The per-app listener serves the WHOLE path through the WASM (no prefix).
    let (status, body) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Hello, Tabbify!");

    // A deep subpath also reaches the wasm (the ULA is the identity, the path
    // is passed verbatim).
    let (status, body) = dial_app(&registry, APP_UUID, "/some/deep/path?q=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Hello, Tabbify!");
}

#[tokio::test]
async fn always_on_app_hosts_on_register() {
    let server = mock_s3(ALWAYS_ON_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    let state = registry.register(APP_UUID).await.expect("register");
    assert_eq!(state, AppState::Running, "always_on must host on register");
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);

    // Hosted at registration → its per-app listener is already serving.
    let (status, body) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Hello, Tabbify!");

    // always_on is never reaped, even past its idle timeout.
    std::thread::sleep(Duration::from_millis(2));
    let reaped = registry.reap_idle().await;
    assert!(reaped.is_empty(), "always_on app was reaped: {reaped:?}");
    // Still hosted.
    let (status, _) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// CONTROL API start/stop: hosts on start (+ pin), unhosts + tears down on stop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_hosts_and_pins_then_reaper_skips_it() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    let app = router(test_state(registry.clone()));
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/apps/{APP_UUID}/start"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = json_body(resp).await;
    assert_eq!(body["state"], "running");
    assert_eq!(body["app_ula"], "fd5a:1f02:44a5:240b:121a::1");
    assert!(
        body["bound_addr"].as_str().is_some(),
        "start must report the bound per-app addr"
    );

    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);

    // Hosted + serving.
    let (status, was) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(was, "Hello, Tabbify!");

    // Even with idle_timeout 300s irrelevant — pinned overrides; a reap pass
    // right now must NOT unhost it.
    let reaped = registry.reap_idle().await;
    assert!(reaped.is_empty(), "pinned app was reaped: {reaped:?}");
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);
    let (status, _) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn stop_unhosts_and_tears_down_the_listener() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    registry
        .ensure_running(APP_UUID, true)
        .await
        .expect("start");
    let addr = registry.get(APP_UUID).unwrap().bound_addr.expect("hosted");

    // Serving before stop.
    let (status, _) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);

    // Stop via the API.
    let app = router(test_state(registry.clone()));
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/apps/{APP_UUID}/stop"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = json_body(resp).await;
    assert_eq!(body["state"], "stopped");

    let s = registry.get(APP_UUID).unwrap();
    assert_eq!(s.state, AppState::Stopped);
    assert!(s.bound_addr.is_none(), "listener handle must be cleared");

    // The listener is gone: a fresh connection must fail.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let dial = reqwest::Client::new()
        .get(format!("http://{addr}/"))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(dial.is_err(), "listener still answered after stop");
}

// ---------------------------------------------------------------------------
// idle-reap: unhosts an idle, unpinned on_request app + tears down its listener
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idle_reap_unhosts_unpinned_on_request_app() {
    let server = mock_s3(FAST_REAP_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    // Lazy-host (unpinned). idle_timeout_sec = 0 → immediately reapable.
    registry
        .ensure_running(APP_UUID, false)
        .await
        .expect("host");
    let addr = registry.get(APP_UUID).unwrap().bound_addr.expect("hosted");
    let (status, _) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);

    // A reap pass must unhost it (unpinned + idle >= 0).
    let reaped = registry.reap_idle().await;
    assert_eq!(reaped, vec![APP_UUID.to_string()]);

    let s = registry.get(APP_UUID).unwrap();
    assert_eq!(s.state, AppState::Stopped);
    assert!(s.bound_addr.is_none(), "listener must be torn down on reap");

    // Listener gone.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let dial = reqwest::Client::new()
        .get(format!("http://{addr}/"))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(dial.is_err(), "listener still answered after reap");
}

// ---------------------------------------------------------------------------
// Runtime SELECTION branch (manifest.runtime.type → which AppRuntime)
// ---------------------------------------------------------------------------

/// A `wasm-http` manifest selects the WASM runtime and serves the fixture (the
/// happy-path arm of the selection branch).
#[tokio::test]
async fn runtime_selection_wasm_http_hosts_and_serves() {
    let server = mock_s3(ALWAYS_ON_MANIFEST).await; // type defaults to wasm-http
    let (registry, _dir) = temp_registry(&server.uri());

    let state = registry
        .register(APP_UUID)
        .await
        .expect("register wasm app");
    assert_eq!(state, AppState::Running);
    let (status, body) = dial_app(&registry, APP_UUID, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Hello, Tabbify!");
}

/// A `firecracker` manifest selects the firecracker runtime; on this (macOS /
/// no-KVM) host the launch must fail with a CLEAR Linux/KVM error rather than
/// silently falling back. The cached rootfs path still resolves (fetcher fix).
#[tokio::test]
async fn runtime_selection_firecracker_without_kvm_errors_clearly() {
    let server = mock_s3_entry(FIRECRACKER_MANIFEST, "rootfs.ext4", b"fake-rootfs-bytes").await;
    let (registry, _dir) = temp_registry(&server.uri());

    let err = registry
        .register(APP_UUID)
        .await
        .expect_err("firecracker must fail without KVM / on non-Linux");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("firecracker") && (msg.contains("kvm") || msg.contains("linux")),
        "error should clearly mention firecracker + KVM/Linux, got: {err}"
    );
}

/// An unknown `runtime.type` is a hard error (no silent default).
#[tokio::test]
async fn runtime_selection_unknown_type_is_rejected() {
    let server = mock_s3_entry(UNKNOWN_RUNTIME_MANIFEST, "app.bin", b"bytes").await;
    let (registry, _dir) = temp_registry(&server.uri());

    let err = registry
        .register(APP_UUID)
        .await
        .expect_err("unknown runtime type must be rejected");
    assert!(
        err.to_string().contains("unknown runtime type"),
        "got: {err}"
    );
}

/// The fetcher caches the entry file under its manifest name (`rootfs.ext4`),
/// NOT a hardcoded `app.wasm`, and reports that path as `cached_path`.
#[tokio::test]
async fn fetcher_caches_entry_under_manifest_name() {
    let server = mock_s3_entry(FIRECRACKER_MANIFEST, "rootfs.ext4", b"rootfs-data").await;
    let dir = tempfile::tempdir().unwrap();
    let fetcher = S3Fetcher::new(server.uri(), dir.path());

    let fetched = fetcher.fetch(APP_UUID).await.expect("fetch");
    let cache = fetcher.cache_dir(APP_UUID, 1);
    assert!(
        cache.join("rootfs.ext4").is_file(),
        "entry must be cached under its manifest name"
    );
    assert!(
        !cache.join("app.wasm").exists(),
        "must NOT hardcode app.wasm"
    );
    assert_eq!(fetched.cached_path, cache.join("rootfs.ext4"));
    // firecracker entry is NOT loaded into memory.
    assert!(fetched.wasm.is_empty(), "rootfs must not be read into RAM");
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}
