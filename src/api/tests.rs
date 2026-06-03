//! Tests for [`super`] — supervisor control API router + handlers.
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use axum::body::Body;
use http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tempfile::TempDir;
use tower::ServiceExt as _;

use super::*;
use crate::fetcher::S3Fetcher;
use crate::orchestrator::handle::RunnerHandle;
use crate::orchestrator::restart::RestartState;
use crate::orchestrator::{Orchestrator, SharedRunnerConfig};

const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";
const APP_ULA: &str = "fd5a:1f02:44a5:240b:121a::1";

fn make_state(runner_dir: PathBuf) -> SupervisorState {
    let orchestrator = Orchestrator::new(
        SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: None,
            no_mesh: true,
            relay_url: None,
        },
        runner_dir.clone(),
    );
    let fetcher = S3Fetcher::new("http://s3.invalid", PathBuf::from("/var/lib/tabbify/data"));
    SupervisorState::new(
        orchestrator,
        fetcher,
        "test-supervisor".to_owned(),
        "::1".to_owned(),
    )
}

fn crashed_record(runner_dir: &std::path::Path) -> RunnerHandle {
    RunnerHandle {
        uuid: APP_UUID.to_owned(),
        pid: 99_999_999, // non-existent
        control_sock: runner_dir.join(format!("{APP_UUID}.sock")),
        app_ula: APP_ULA.to_owned(),
        parent: None,
        spawned_at: 0,
        restart: RestartState {
            consecutive_failures: 5,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_001_000,
            last_healthy_at: 0,
        },
        image_ref: None,
        requested_runtime: None,
    }
}

async fn body_bytes(resp: axum::response::Response) -> bytes::Bytes {
    resp.into_body()
        .collect()
        .await
        .expect("body collection failed")
        .to_bytes()
}

// ── POST /v1/apps/:uuid/reset — 200 for known uuid ────────────────────────

/// Posting reset for a uuid that has an on-disk runner record returns 200
/// with a JSON body containing restart fields.
#[tokio::test]
async fn reset_known_uuid_returns_200() {
    let dir = TempDir::new().unwrap();
    let rec = crashed_record(dir.path());
    rec.save(dir.path()).unwrap();

    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/reset"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // The response mirrors running_json: state, app_ula, bound_addr.
    assert!(json.get("state").is_some(), "response must contain 'state'");
    assert!(
        json.get("app_ula").is_some(),
        "response must contain 'app_ula'"
    );
}

// ── POST /v1/apps/:uuid/reset — 404 for unknown uuid ─────────────────────

/// Posting reset for a uuid that has NO on-disk runner record returns 404.
#[tokio::test]
async fn reset_unknown_uuid_returns_404() {
    let dir = TempDir::new().unwrap();
    // No record written → uuid is unknown.
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/reset"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── anyhow_to_not_found_or_error ─────────────────────────────────────────

/// An error whose message contains "no runner record found" maps to 404.
#[test]
fn not_found_error_maps_to_404() {
    let e = anyhow::anyhow!("no runner record found for abc-123");
    let resp = super::handlers::anyhow_to_not_found_or_error(&e);
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Any other error maps to 500.
#[test]
fn generic_error_maps_to_500() {
    let e = anyhow::anyhow!("something went wrong");
    let resp = super::handlers::anyhow_to_not_found_or_error(&e);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ── DeployBody deserialization ────────────────────────────────────────────

/// `{"ref":"x"}` parses to `reff == "x"` — pins the `#[serde(rename="ref")]`.
#[test]
fn deploy_body_ref_field_deserializes() {
    let body: DeployBody = serde_json::from_str(r#"{"ref":"x"}"#).unwrap();
    assert_eq!(body.reff, "x");
}

/// A realistic OCI image ref round-trips through DeployBody.
#[test]
fn deploy_body_oci_ref_round_trips() {
    let json = r#"{"ref":"[fd5a::1]:5000/a/b:sha256abc"}"#;
    let body: DeployBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.reff, "[fd5a::1]:5000/a/b:sha256abc");
}

// ── POST /v1/apps/:uuid/deploy — 404 for unknown uuid (no record) ─────────

/// Posting deploy for a uuid that has no on-disk runner record and no live
/// runner returns a non-200 response (spawn failure or 404).
#[tokio::test]
async fn deploy_unknown_uuid_returns_error() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/deploy"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"ref":"reg:5000/a/b:sha"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // No runner binary available → spawn fails → should not be 200.
    assert_ne!(
        resp.status(),
        StatusCode::OK,
        "deploy of unknown uuid with no runner binary must not return 200"
    );
}

// ── POST /v1/apps/:uuid/deploy — 200 for known uuid with a live runner ────

/// Posting deploy for a uuid that has a live (fake) runner returns 200 with
/// state/app_ula/bound_addr, and the persisted record has image_ref updated.
#[tokio::test]
async fn deploy_known_uuid_with_live_runner_returns_200() {
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join(format!("{APP_UUID}.sock"));

    // Fake control server: replies Ok to every command (health + deploy).
    let sock_path_srv = sock_path.clone();
    tokio::spawn(async move {
        let listener = UnixListener::bind(&sock_path_srv).unwrap();
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_secs(2), listener.accept()).await {
                Ok(Ok((stream, _))) => {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line).await;
                    let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
                }
                _ => break,
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Persist a runner record pointing at the fake socket.
    let rec = RunnerHandle {
        uuid: APP_UUID.to_owned(),
        pid: 12345,
        control_sock: sock_path.clone(),
        app_ula: APP_ULA.to_owned(),
        parent: None,
        spawned_at: 0,
        restart: RestartState::default(),
        image_ref: None,
        requested_runtime: None,
    };
    rec.save(dir.path()).unwrap();

    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/deploy"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"ref":"[fd5a::1]:5000/acme/app:sha256abc"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("state").is_some(), "response must contain 'state'");
    assert!(
        json.get("app_ula").is_some(),
        "response must contain 'app_ula'"
    );

    // Persisted record must have image_ref updated.
    let updated = RunnerHandle::load(dir.path(), APP_UUID)
        .unwrap()
        .expect("record must exist after deploy");
    assert_eq!(
        updated.image_ref.as_deref(),
        Some("[fd5a::1]:5000/acme/app:sha256abc"),
        "image_ref must be persisted after HTTP deploy"
    );
}

// ── BuildBody deserialization ─────────────────────────────────────────────

/// `{"ref":"abc","repo_url":..., ...}` parses `git_ref == "abc"` — pins the
/// `#[serde(rename="ref")]` on [`BuildBody`].
#[test]
fn build_body_ref_field_deserializes() {
    let json = r#"{
        "repo_url":"https://github.com/acme/app",
        "ref":"abc123",
        "tenant":"acme",
        "app_uuid":"u",
        "registry_ula":"[fd5a::1]:5000"
    }"#;
    let body: BuildBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.git_ref, "abc123", "git_ref must come from 'ref' key");
}

/// Optional token fields default to `None` when omitted.
#[test]
fn build_body_optional_tokens_default_to_none() {
    let json = r#"{
        "repo_url":"r","ref":"v1","tenant":"t","app_uuid":"u","registry_ula":"[::1]:5000"
    }"#;
    let body: BuildBody = serde_json::from_str(json).unwrap();
    assert!(body.clone_token.is_none() && body.push_token.is_none());
}

/// A body WITHOUT the wasm fields defaults to `BuildKind::Docker` (the
/// pre-WC behaviour: every existing docker `/v1/build` request is unchanged).
#[test]
fn build_body_defaults_to_docker_build_kind() {
    use crate::runner::build::BuildKind;
    let json = r#"{
        "repo_url":"r","ref":"v1","tenant":"t","app_uuid":"u","registry_ula":"[::1]:5000"
    }"#;
    let body: BuildBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.build_kind, BuildKind::Docker);
    assert!(body.build_cmd.is_none());
    assert!(body.artifact_path.is_none());
}

// ── POST /v1/build — route exists and delegates to the orchestrator ────────

/// `POST /v1/build` with a valid body reaches the handler and returns 500
/// (the runner binary is not present in tests) — not 404, 405, or 422.
/// This pins the route registration.
#[tokio::test]
async fn post_v1_build_route_is_registered() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let body = r#"{
        "repo_url":"https://github.com/acme/app",
        "ref":"abc123",
        "tenant":"acme",
        "app_uuid":"u",
        "registry_ula":"[fd5a::1]:5000"
    }"#;

    let req = Request::builder()
        .method("POST")
        .uri("/v1/build")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Runner binary missing → the orchestrator returns an error → 500.
    // What matters here is that the route IS registered (not 404 / 405) and
    // the body is correctly decoded (not 422).
    assert_ne!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "POST /v1/build must be registered (got 404)"
    );
    assert_ne!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "POST /v1/build must accept POST (got 405)"
    );
    assert_ne!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "build body must parse correctly (got 422)"
    );
}

/// A malformed JSON body to `POST /v1/build` returns a 4xx error (axum body
/// rejection — 400 or 422 depending on the axum version), confirming that the
/// route parses the body with the expected shape.
#[tokio::test]
async fn post_v1_build_rejects_bad_body() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/build")
        .header("content-type", "application/json")
        .body(Body::from("not json at all"))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // axum returns 400 Bad Request for syntactically invalid JSON and
    // 422 Unprocessable Entity for valid JSON that fails schema validation.
    // Either is acceptable here — what matters is that the route IS wired and
    // the body extractor runs (not a 404 or 405).
    let status = resp.status().as_u16();
    assert!(
        status == 400 || status == 422,
        "malformed body must return 400 or 422, got {status}"
    );
}
