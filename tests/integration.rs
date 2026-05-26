//! Control-API integration tests (contract §5, §6, §10): the S3 fetcher against
//! a wiremock stand-in for S3, and the axum CONTROL API driven via
//! `tower::ServiceExt::oneshot` — now backed by the runner ORCHESTRATOR.
//!
//! Since Task 2.6 the control API no longer hosts apps in-process; it drives the
//! orchestrator, which spawns a DETACHED `tabbify-runner` process per app. So
//! these tests spawn real runner subprocesses (no-mesh / loopback) and assert:
//! - `POST /v1/apps/:uuid/start` spawns a runner that becomes healthy + reports
//!   `state: running` and the app's deterministic ULA;
//! - `POST /v1/apps/:uuid/stop` shuts the runner down + forgets it;
//! - `POST /v1/apps/:uuid/purge` purges + shuts down + forgets it;
//! - `GET /v1/apps` and `GET /v1/apps/:uuid` reflect the live runner fleet.
//!
//! The only mock is the HTTP object store (wiremock); the orchestrator, the
//! spawned runners, the WASM runtime, and the per-app listeners are all real.
//! Each test force-kills any runner it spawned on teardown so no detached
//! process leaks.

use std::path::Path;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tabbify_supervisor::api::{SupervisorState, router};
use tabbify_supervisor::fetcher::{FetchError, S3Fetcher};
use tabbify_supervisor::orchestrator::handle::RunnerHandle;
use tabbify_supervisor::orchestrator::{Orchestrator, SharedRunnerConfig};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

/// The deterministic per-app ULA for `APP_UUID` (golden value, see `app_ula`).
const APP_ULA: &str = "fd5a:1f02:44a5:240b:121a::1";

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

// ---------------------------------------------------------------------------
// Orchestrator-backed test harness
// ---------------------------------------------------------------------------

/// A control-API test harness: a wiremock S3, a real [`Orchestrator`] over temp
/// dirs (pointed at the cargo-built `tabbify-runner`), and the temp dirs kept
/// alive for the test's duration.
struct Harness {
    orchestrator: Orchestrator,
    fetcher: S3Fetcher,
    runner_dir: tempfile::TempDir,
    _data_dir: tempfile::TempDir,
}

impl Harness {
    /// Build a harness whose orchestrator spawns real no-mesh runners that fetch
    /// from `s3_uri`.
    fn new(s3_uri: &str) -> Self {
        let data_dir = tempfile::tempdir().expect("data dir");
        let runner_dir = tempfile::tempdir().expect("runner dir");
        let shared = SharedRunnerConfig {
            runner_bin: env!("CARGO_BIN_EXE_tabbify-runner").into(),
            s3_base_url: s3_uri.to_owned(),
            data_dir: data_dir.path().to_path_buf(),
            parent: None,
            no_mesh: true,
        };
        let orchestrator = Orchestrator::new(shared, runner_dir.path().to_path_buf());
        let fetcher = S3Fetcher::new(s3_uri, data_dir.path());
        Self {
            orchestrator,
            fetcher,
            runner_dir,
            _data_dir: data_dir,
        }
    }

    /// Build the control-API router over this harness's orchestrator + fetcher.
    fn router(&self) -> axum::Router {
        let state = SupervisorState::new(
            self.orchestrator.clone(),
            self.fetcher.clone(),
            "test-supervisor".into(),
            "fd5a:1f00:1::1".into(),
        );
        router(state)
    }

    /// Force-kill every runner this harness's orchestrator has a record for
    /// (best-effort teardown so a panicking test never leaks a detached runner).
    fn kill_all_runners(&self) {
        if let Ok(records) = RunnerHandle::list(self.runner_dir.path()) {
            for rec in records {
                force_kill(rec.pid);
            }
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.kill_all_runners();
    }
}

/// Force-kill `pid` (best-effort, no-op if already gone).
fn force_kill(pid: u32) {
    // SAFETY: `kill(2)` is a standard POSIX syscall; SIGKILL to a (possibly
    // already-dead) pid is harmless in this short test window.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

/// Drive one HTTP request through `router` and return (status, JSON body).
async fn call(router: axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(req).await.expect("router oneshot");
    let status = resp.status();
    let body = json_body(resp).await;
    (status, body)
}

/// `POST /v1/apps/:uuid/start` helper.
fn start_req(uuid: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{uuid}/start"))
        .body(Body::empty())
        .unwrap()
}

/// `POST /v1/apps/:uuid/<verb>` helper.
fn post_req(uuid: &str, verb: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{uuid}/{verb}"))
        .body(Body::empty())
        .unwrap()
}

/// `GET <uri>` helper.
fn get_req(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

/// Wait until the runner record for `uuid` is gone (the orchestrator forgot it
/// after stop/purge). Returns `true` if it disappeared within `timeout`.
async fn wait_record_gone(runner_dir: &Path, uuid: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match RunnerHandle::load(runner_dir, uuid) {
            Ok(None) => return true,
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    false
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
    let harness = Harness::new("http://unused.invalid");

    let (status, body) = call(harness.router(), get_req("/health")).await;
    assert_eq!(status, StatusCode::OK);
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
    let harness = Harness::new(&server.uri());

    let (status, body) = call(harness.router(), get_req(&format!("/v1/apps/{APP_UUID}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["present"], true);
    // app_ula is the deterministic per-app ULA (golden prefix).
    assert_eq!(body["app_ula"], APP_ULA);
    // Not started → no live runner → stopped.
    assert_eq!(body["state"], "stopped");
}

#[tokio::test]
async fn get_app_absent_returns_not_present() {
    let server = MockServer::start().await; // empty → 404
    let harness = Harness::new(&server.uri());

    let (status, body) = call(harness.router(), get_req(&format!("/v1/apps/{APP_UUID}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["present"], false);
}

// ---------------------------------------------------------------------------
// CONTROL API start: spawns a runner that becomes healthy + serves the fixture
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_spawns_runner_and_reports_running() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    let (status, body) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK, "start should succeed, body: {body}");
    assert_eq!(body["state"], "running");
    // In the orchestrator model the app's address IS its app-ULA.
    assert_eq!(body["app_ula"], APP_ULA);
    assert_eq!(
        body["bound_addr"], APP_ULA,
        "bound_addr is the app-ULA (the runner serves on its own ULA)"
    );

    // A record was persisted for the spawned runner.
    let rec = RunnerHandle::load(harness.runner_dir.path(), APP_UUID)
        .expect("load record")
        .expect("record present after start");
    assert_eq!(rec.uuid, APP_UUID);
    assert_eq!(rec.app_ula, APP_ULA);

    // The orchestrator can reach the spawned runner's control socket.
    let state = harness
        .orchestrator
        .app_state(APP_UUID)
        .await
        .expect("app_state");
    assert_eq!(state.as_str(), "running");
}

/// Starting an already-running app is idempotent: it returns the SAME running
/// runner (same pid, same record) rather than spawning a second one.
#[tokio::test]
async fn start_is_idempotent_for_a_running_app() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    let (status, _) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);
    let pid1 = RunnerHandle::load(harness.runner_dir.path(), APP_UUID)
        .unwrap()
        .unwrap()
        .pid;

    // Second start must not spawn a new process.
    let (status, body) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "running");
    let pid2 = RunnerHandle::load(harness.runner_dir.path(), APP_UUID)
        .unwrap()
        .unwrap()
        .pid;

    assert_eq!(pid1, pid2, "idempotent start must reuse the running runner");
}

// ---------------------------------------------------------------------------
// CONTROL API stop: shuts the runner down + forgets it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stop_shuts_down_runner_and_forgets_it() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    // Start → a runner exists + is reachable.
    let (status, _) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        RunnerHandle::load(harness.runner_dir.path(), APP_UUID)
            .unwrap()
            .is_some()
    );

    // Stop via the API.
    let (status, body) = call(harness.router(), post_req(APP_UUID, "stop")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "stopped");

    // The orchestrator forgot the runner record.
    assert!(
        wait_record_gone(harness.runner_dir.path(), APP_UUID, Duration::from_secs(5)).await,
        "stop must remove the runner record"
    );

    // The on-disk artifact cache is KEPT (stop frees memory, not disk — fast
    // restart).
    assert!(
        harness
            .fetcher
            .cache_dir(APP_UUID, 1)
            .join("app.wasm")
            .is_file(),
        "stop must keep the on-disk artifact cache for a fast restart"
    );
}

// ---------------------------------------------------------------------------
// CONTROL API purge: purge + shut the runner down + forget it + clear cache
// ---------------------------------------------------------------------------

#[tokio::test]
async fn purge_endpoint_purges_shuts_down_and_forgets() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    // Start → runner caches the artifact on disk.
    let (status, _) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);
    let cache = harness.fetcher.cache_dir(APP_UUID, 1);
    assert!(cache.join("app.wasm").is_file(), "artifact cached on start");

    // Purge via the API.
    let (status, body) = call(harness.router(), post_req(APP_UUID, "purge")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "purged");

    // The orchestrator forgot the runner record.
    assert!(
        wait_record_gone(harness.runner_dir.path(), APP_UUID, Duration::from_secs(5)).await,
        "purge must remove the runner record"
    );

    // The on-disk artifact cache is reclaimed.
    assert!(
        !cache.exists(),
        "purge must remove the on-disk artifact cache, but it lingers"
    );
}

// ---------------------------------------------------------------------------
// CONTROL API GET /v1/apps listing reflects the live runner fleet
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_apps_reflects_running_runner() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    // Empty fleet first.
    let (status, body) = call(harness.router(), get_req("/v1/apps")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["apps"].as_array().unwrap().is_empty(),
        "no runners spawned yet"
    );

    // Start one.
    let (status, _) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);

    // The listing now shows the running runner with its app-ULA.
    let (status, body) = call(harness.router(), get_req("/v1/apps")).await;
    assert_eq!(status, StatusCode::OK);
    let apps = body["apps"].as_array().unwrap();
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0]["uuid"], APP_UUID);
    assert_eq!(apps[0]["app_ula"], APP_ULA);
    assert_eq!(apps[0]["state"], "running");
}

/// `GET /v1/apps/:uuid` reports a started app as running with its app-ULA.
#[tokio::test]
async fn get_app_reports_running_after_start() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let harness = Harness::new(&server.uri());

    let (status, _) = call(harness.router(), start_req(APP_UUID)).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = call(harness.router(), get_req(&format!("/v1/apps/{APP_UUID}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["present"], true);
    assert_eq!(body["state"], "running");
    assert_eq!(body["app_ula"], APP_ULA);
}

// NOTE: deterministic control-sock path + app-ULA derivation are covered by the
// `orchestrator::api` unit tests (`control_sock_is_uuid_dot_sock_under_runner_dir`,
// `app_ula_matches_derive`), so they are not re-tested here.

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}
