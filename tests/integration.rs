//! Integration tests (contract §6, §10): the S3 fetcher against a wiremock
//! stand-in for S3, and the axum API driven via `tower::ServiceExt::oneshot`
//! end-to-end — including serving `/apps/<uuid>/` through the WASM fixture.
//!
//! These use real behavior with minimal mocks: the only mock is the HTTP object
//! store (wiremock); the registry, lifecycle, and WASM runtime are exercised
//! for real against the committed `tests/fixtures/hello.wasm`.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tabbify_supervisor::api::{SupervisorState, router};
use tabbify_supervisor::fetcher::{FetchError, S3Fetcher};
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

fn temp_registry(base_url: &str) -> (AppRegistry, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fetcher = S3Fetcher::new(base_url, dir.path());
    (AppRegistry::new(fetcher), dir)
}

fn test_state(registry: AppRegistry) -> SupervisorState {
    SupervisorState::new(registry, "test-supervisor".into(), "fd5a:1f00:1::1".into())
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

    // Artifacts must be cached on disk under <data>/apps/<uuid>/v1/.
    let cache = fetcher.cache_dir(APP_UUID, 1);
    assert!(cache.join("manifest.toml").is_file());
    assert!(cache.join("app.wasm").is_file());
}

#[tokio::test]
async fn fetcher_missing_app_is_not_found() {
    let server = MockServer::start().await;
    // No mounts → wiremock returns 404 for everything.
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
// API: health
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
// API: GET /v1/apps/:uuid  (present / absent against the mocked S3)
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
    // app_ula is reported (forward-compat) and matches the golden prefix.
    assert!(body["app_ula"].as_str().unwrap().starts_with("fd5a:1f02:"));
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
// API: GET /v1/apps listing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_apps_reflects_registered_app() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    // Register one app up front.
    let state = registry.register(APP_UUID).await.expect("register");
    assert_eq!(state, AppState::Available); // on_request → available, not running

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
}

// ---------------------------------------------------------------------------
// API: end-to-end serve through the WASM fixture
// ---------------------------------------------------------------------------

#[tokio::test]
async fn serve_app_runs_fixture_end_to_end() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    let app = router(test_state(registry));

    // Hit the bare app root: lazy-spawn (on_request) then run the wasm.
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/apps/{APP_UUID}/"))
                .header("host", "supervisor.local")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"Hello, Tabbify!");
}

#[tokio::test]
async fn serve_app_with_subpath_runs_fixture() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    let app = router(test_state(registry));

    // A deeper subpath must also reach the wasm (prefix stripped).
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/apps/{APP_UUID}/some/deep/path?q=1"))
                .header("host", "supervisor.local")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"Hello, Tabbify!");
}

// ---------------------------------------------------------------------------
// API: start pins (sticky over the idle reaper)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_pins_app_so_reaper_skips_it() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    // Start via the API → running + pinned.
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

    // The app is running.
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);

    // A reap pass right now must NOT stop it (pinned).
    let reaped = registry.reap_idle();
    assert!(reaped.is_empty(), "pinned app was reaped: {reaped:?}");
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);
}

#[tokio::test]
async fn stop_unpins_and_marks_stopped() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());
    registry
        .ensure_running(APP_UUID, true)
        .await
        .expect("start");
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);

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
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Stopped);
}

// ---------------------------------------------------------------------------
// Registry lifecycle: always_on spawns immediately
// ---------------------------------------------------------------------------

#[tokio::test]
async fn always_on_app_spawns_on_register() {
    let server = mock_s3(ALWAYS_ON_MANIFEST).await;
    let (registry, _dir) = temp_registry(&server.uri());

    let state = registry.register(APP_UUID).await.expect("register");
    assert_eq!(state, AppState::Running, "always_on must spawn on register");
    assert_eq!(registry.get(APP_UUID).unwrap().state, AppState::Running);

    // always_on is never reaped, even when idle past its timeout (default 300s
    // here is irrelevant — the policy short-circuits on the mode).
    std::thread::sleep(Duration::from_millis(1));
    let reaped = registry.reap_idle();
    assert!(reaped.is_empty(), "always_on app was reaped: {reaped:?}");
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}
