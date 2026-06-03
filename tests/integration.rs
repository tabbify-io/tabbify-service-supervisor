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
//! The in-process WASM runtime was removed, so the runner-SPAWNING control-API
//! tests (start / stop / purge / list / get-running) — which needed a hermetic
//! runtime to bring the spawned runner healthy — were removed with it. What
//! remains is the S3 fetcher coverage and the runner-LESS control-API handlers
//! (health / about / get-app present-or-absent), none of which build a runtime.

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

/// Opaque artifact bytes the mock S3 serves. The fetcher treats the app
/// artifact as opaque (it does NOT compile it), so any non-empty payload
/// exercises the fetch + cache path identically.
const ARTIFACT_BYTES: &[u8] = b"\0asm\x01\0\0\0opaque-artifact";

/// `on_request` manifest used by the fetcher + runner-less control-API tests.
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
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES.to_vec()))
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
            relay_url: None,
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

/// `GET <uri>` helper.
fn get_req(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
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
    assert_eq!(fetched.wasm.len(), ARTIFACT_BYTES.len());

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

#[tokio::test]
async fn about_reports_version_and_mesh_status() {
    // The harness binds a real IPv6 ULA, so `about` must derive `mesh_status:
    // "joined"` from it (the only new branching logic in this handler).
    let harness = Harness::new("http://unused.invalid");

    let (status, body) = call(harness.router(), get_req("/v1/about")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["peer_id"], "test-supervisor");
    assert_eq!(body["mesh_status"], "joined");
    assert!(body["uptime_secs"].is_u64(), "got: {body}");
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
