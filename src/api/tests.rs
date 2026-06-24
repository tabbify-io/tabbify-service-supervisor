//! Tests for [`super`] — supervisor control API router + handlers.
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use axum::body::Body;
use http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tempfile::TempDir;
use tower::ServiceExt as _;

use super::*;
use crate::{
    fetcher::S3Fetcher,
    orchestrator::{Orchestrator, SharedRunnerConfig, handle::RunnerHandle, restart::RestartState},
};

const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";
const APP_ULA: &str = "fd5a:1f02:44a5:240b:121a::1";

fn make_state(runner_dir: PathBuf) -> SupervisorState {
    make_state_with_data_dir(runner_dir.clone(), PathBuf::from("/var/lib/tabbify/data"))
}

/// Like [`make_state`] but accepts an explicit `data_dir` (the directory where
/// `<data_dir>/runners/<uuid>.log` lands). Used by tests that pre-populate a
/// runner log fixture and want the orchestrator to find it.
fn make_state_with_data_dir(runner_dir: PathBuf, data_dir: PathBuf) -> SupervisorState {
    let orchestrator = Orchestrator::new(
        SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: data_dir.clone(),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        },
        runner_dir,
    );
    let fetcher = S3Fetcher::new("http://s3.invalid", data_dir);
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
        network: None,
        runner_join_token: None,
        manifest_toml: None,
        extra_env: None,
        egress_allow: None,
        crash_looped: false,
        stopped: false,
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

/// Phase-2: DeployBody accepts `network` + `runner_join_token` from the node.
#[test]
fn deploy_body_parses_phase2_network_and_token() {
    let json = r#"{
        "ref": "[fd5a::1]:5000/a/b:sha",
        "network": "n_jpegxik72nng",
        "runner_join_token": "scoped-runner-jwt"
    }"#;
    let body: DeployBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.network.as_deref(), Some("n_jpegxik72nng"));
    assert_eq!(body.runner_join_token.as_deref(), Some("scoped-runner-jwt"));
}

/// Phase-2 fields are OPTIONAL: a deploy body without them parses to `None`
/// (backward-compatible — today's node/CLI still works).
#[test]
fn deploy_body_phase2_fields_default_to_none() {
    let body: DeployBody = serde_json::from_str(r#"{"ref":"x"}"#).unwrap();
    assert!(body.network.is_none());
    assert!(body.runner_join_token.is_none());
}

/// A deploy body with an `env` map deserializes the map into `DeployBody.env`.
/// This pins the serde path that the task spec requires.
#[test]
fn deploy_body_env_map_deserializes() {
    let json = r#"{
        "ref": "[fd5a::1]:5000/a/b:sha",
        "env": {"TABBIFY_DEVBOX_AUTHORIZED_KEY": "ssh-ed25519 AAAA", "PORT": "9000"}
    }"#;
    let body: DeployBody = serde_json::from_str(json).unwrap();
    let env = body.env.as_ref().expect("env field must be present");
    assert_eq!(
        env.get("TABBIFY_DEVBOX_AUTHORIZED_KEY").map(String::as_str),
        Some("ssh-ed25519 AAAA"),
        "TABBIFY_DEVBOX_AUTHORIZED_KEY must deserialize from body.env"
    );
    assert_eq!(
        env.get("PORT").map(String::as_str),
        Some("9000"),
        "PORT must deserialize from body.env"
    );
}

/// A deploy body WITHOUT `env` deserializes with `env = None` (normal deploy,
/// backward-compatible — no env means the OCI image's own vars are used as-is).
#[test]
fn deploy_body_env_defaults_to_none() {
    let body: DeployBody = serde_json::from_str(r#"{"ref":"x"}"#).unwrap();
    assert!(
        body.env.is_none(),
        "env must be None when omitted from body"
    );
}

/// Track 7: a deploy body carrying `egress_allow` (the network ACL allow-list)
/// deserializes the array into `DeployBody.egress_allow`. Pins the wire shape
/// the node populates from `GET /v1/egress/resolve`.
#[test]
fn deploy_body_accepts_egress_allow() {
    let body: DeployBody = serde_json::from_str(
        r#"{"ref":"x","egress_allow":["api.telegram.org","10.0.0.0/24"]}"#,
    )
    .unwrap();
    assert_eq!(
        body.egress_allow.as_deref().unwrap(),
        ["api.telegram.org", "10.0.0.0/24"]
    );
}

/// Track 7: a deploy body WITHOUT `egress_allow` deserializes to `None` — a
/// pre-ACL node / today's deploys keep the supervisor's unrestricted egress.
#[test]
fn deploy_body_egress_allow_defaults_none() {
    let body: DeployBody = serde_json::from_str(r#"{"ref":"x"}"#).unwrap();
    assert!(body.egress_allow.is_none());
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

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

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
        network: None,
        runner_join_token: None,
        manifest_toml: None,
        extra_env: None,
        egress_allow: None,
        crash_looped: false,
        stopped: false,
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

// ── POST /v1/apps/:uuid/deploy — 500 includes runner_log_tail ────────────

/// A deploy that fails (no runner binary) AND has a pre-populated runner log
/// must return 500 with `runner_log_tail` containing the last log line.
///
/// This pins the feature: spawn failures include the runner log tail in the
/// 500 JSON body so remote diagnosis does not require SSH access to the worker.
#[tokio::test]
async fn deploy_500_includes_runner_log_tail() {
    let dir = TempDir::new().unwrap();

    // Pre-populate the runner log at <data_dir>/runners/<uuid>.log.
    // The orchestrator's data_dir is the same temp dir for simplicity.
    let runners_dir = dir.path().join("runners");
    std::fs::create_dir_all(&runners_dir).unwrap();
    std::fs::write(
        runners_dir.join(format!("{APP_UUID}.log")),
        "boot line 1\nboot line 2\nFATAL: tap device failed\n",
    )
    .unwrap();

    // Use a state whose data_dir points at the temp dir so runner_log_tail
    // can find the fixture file.
    let state = make_state_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/deploy"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"ref":"reg:5000/a/b:sha"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Spawn must fail (no runner binary in test env) → 500.
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "deploy with no runner binary must return 500"
    );

    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tail = json
        .get("runner_log_tail")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        tail.contains("FATAL: tap device failed"),
        "500 body must include runner log tail; got body: {json}"
    );
}

// ── POST /v1/apps/:uuid/deploy — 500 WITHOUT a log omits the tail key ─────

/// A deploy that fails with NO pre-existing runner log must return a plain
/// 500 whose JSON body has NO `runner_log_tail` key (the tail is best-effort:
/// absent, not an empty string / null). Note the spawn machinery pre-creates
/// an EMPTY log before exec'ing the runner, so this also pins "empty file →
/// key omitted", not just "missing file → key omitted".
#[tokio::test]
async fn deploy_500_without_log_omits_tail_key() {
    let dir = TempDir::new().unwrap();
    // NO <data_dir>/runners/<uuid>.log fixture written.
    let state = make_state_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/apps/{APP_UUID}/deploy"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"ref":"reg:5000/a/b:sha"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "deploy with no runner binary must return 500"
    );

    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json.get("error").is_some(),
        "500 body must still carry 'error'; got: {json}"
    );
    assert!(
        json.get("runner_log_tail").is_none(),
        "500 body must OMIT 'runner_log_tail' when no log exists; got: {json}"
    );
}

// ── POST /v1/dev-sessions ─────────────────────────────────────────────────────

/// `POST /v1/dev-sessions` with a valid body:
/// - when the underlying `deploy_app` fails (no runner binary in test env) the
///   response must be 500 carrying `runner_log_tail` (the fixture log exists),
///   the git cap must be revoked (no cap left registered → the proxy 403s every
///   `/git/<cap>` request), and the dev-session registry must be empty.
#[tokio::test]
async fn create_dev_session_deploy_failure_cleans_up() {
    let dir = TempDir::new().unwrap();

    // Pre-populate the runner log at <data_dir>/runners/<uuid>.log so the 500
    // body carries the tail (same fixture as deploy_500_includes_runner_log_tail).
    let runners_dir = dir.path().join("runners");
    std::fs::create_dir_all(&runners_dir).unwrap();
    std::fs::write(
        runners_dir.join(format!("{APP_UUID}.log")),
        "boot line 1\nFATAL: dev-FC spawn failed\n",
    )
    .unwrap();

    let state = make_state_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
    let app = router(state.clone());

    let body = serde_json::json!({
        "app_uuid": APP_UUID,
        "image_ref": "[fd5a::1]:5000/tabbify/devbox:latest",
        "repo_url": "https://github.com/acme/app.git",
        "branch": "main",
        "git_token": "ghs_test_token",
        "git_token_ttl_secs": 3600,
        "authorized_key": "ssh-ed25519 AAAA test"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/dev-sessions")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Create is now ASYNC (202): it returns immediately and provisions in the
    // background, so the node never blocks on a multi-minute cold pull (which
    // previously blew the node's HTTP timeout and produced a false "create
    // failed" + an orphan VM). Here the background deploy fails (no runner /
    // KVM) → it must revoke the git cap + drop the session.
    assert_eq!(
        resp.status(),
        StatusCode::ACCEPTED,
        "create_dev_session now returns 202 (async provision)"
    );

    // Poll for the background-failure cleanup: the session is registered up front
    // (so the node can track it), then dropped + its cap revoked once the deploy
    // fails. The failure is fast here (no KVM on the test host → bail).
    let cleaned = {
        let mut ok = false;
        for _ in 0..100 {
            if state.dev_sessions.is_empty() && state.git_sessions.registered_caps().is_empty() {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        ok
    };
    assert!(
        cleaned,
        "background deploy failure must drop the session + revoke the git cap"
    );
}

/// `POST /v1/dev-sessions` route is registered (not 404 / 405) and the body
/// shape is accepted (not 422). The deploy fails (no runner binary) → 500.
#[tokio::test]
async fn post_v1_dev_sessions_route_is_registered() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let body = serde_json::json!({
        "app_uuid": APP_UUID,
        "image_ref": "[fd5a::1]:5000/devbox:latest",
        "repo_url": "https://github.com/acme/app.git",
        "branch": "main",
        "git_token": "tok",
        "git_token_ttl_secs": 3600,
        "authorized_key": "ssh-ed25519 AAAA test"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/dev-sessions")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert_ne!(status, 404, "POST /v1/dev-sessions must be registered");
    assert_ne!(status, 405, "POST /v1/dev-sessions must accept POST");
    assert_ne!(status, 422, "body must parse correctly");
}

// ── POST /v1/dev-sessions/:id/git-token ───────────────────────────────────────

/// `POST /v1/dev-sessions/:id/git-token` for an unknown session id returns 404.
#[tokio::test]
async fn refresh_git_token_unknown_session_returns_404() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let body = serde_json::json!({
        "git_token": "new-token",
        "git_token_ttl_secs": 3600
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/dev-sessions/no-such-id/git-token")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `POST /v1/dev-sessions/:id/git-token` for a KNOWN session returns 200.
#[tokio::test]
async fn refresh_git_token_known_session_returns_200() {
    use crate::api::dev_sessions::DevSession;
    use std::time::{Duration, Instant};

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());

    // Pre-register a session and a git cap directly.
    let cap = "a".repeat(64);
    state.git_sessions.register(
        cap.clone(),
        GitSessionEntry {
            upstream_url: "https://github.com/acme/app.git".to_owned(),
            token: "old-token".to_owned(),
            expires_at: Instant::now() + Duration::from_secs(3600),
        },
    );
    let now = Instant::now();
    state.dev_sessions.insert(DevSession {
        session_id: "test-sess-1".to_owned(),
        app_uuid: APP_UUID.to_owned(),
        cap: cap.clone(),
        created_at: now,
        last_activity: now,
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
    });

    let app = router(state.clone());

    let body = serde_json::json!({
        "git_token": "new-token",
        "git_token_ttl_secs": 7200
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/dev-sessions/test-sess-1/git-token")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        json.get("refreshed").and_then(|v| v.as_bool()),
        Some(true),
        "response must have refreshed: true; got: {json}"
    );
}

// ── DELETE /v1/dev-sessions/:id ───────────────────────────────────────────────

/// `DELETE /v1/dev-sessions/:id` for an unknown id returns 404.
#[tokio::test]
async fn delete_dev_session_unknown_returns_404() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("DELETE")
        .uri("/v1/dev-sessions/no-such-id")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `DELETE /v1/dev-sessions/:id` for a known session:
/// - returns 200 with `{ purged: true }`.
/// - the session is removed from the registry.
/// - the git cap is revoked (git proxy returns 403 for the cap).
/// - a second DELETE returns 404.
#[tokio::test]
async fn delete_dev_session_known_returns_200_and_cleans_up() {
    use crate::api::dev_sessions::DevSession;
    use std::time::{Duration, Instant};

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());

    let cap = "b".repeat(64);
    state.git_sessions.register(
        cap.clone(),
        GitSessionEntry {
            upstream_url: "https://github.com/acme/app.git".to_owned(),
            token: "tok".to_owned(),
            expires_at: Instant::now() + Duration::from_secs(3600),
        },
    );
    let now = Instant::now();
    state.dev_sessions.insert(DevSession {
        session_id: "del-sess".to_owned(),
        app_uuid: APP_UUID.to_owned(),
        cap: cap.clone(),
        created_at: now,
        last_activity: now,
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
    });

    // First DELETE: should return 200.
    {
        let app = router(state.clone());
        let req = Request::builder()
            .method("DELETE")
            .uri("/v1/dev-sessions/del-sess")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json.get("purged").and_then(|v| v.as_bool()),
            Some(true),
            "DELETE response must have purged: true; got: {json}"
        );
    }

    // Session must be gone from the registry.
    assert!(
        state.dev_sessions.lookup("del-sess").is_none(),
        "session must be removed after DELETE"
    );

    // The revoked cap must 403 at the git proxy (the lookup runs BEFORE any
    // upstream contact, so this never touches the network).
    {
        let app = router(state.clone());
        let req = Request::builder()
            .method("GET")
            .uri(format!("/git/{cap}/info/refs?service=git-upload-pack"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "deleted session's cap must be revoked at the proxy"
        );
    }

    // Second DELETE: should return 404.
    {
        let app = router(state.clone());
        let req = Request::builder()
            .method("DELETE")
            .uri("/v1/dev-sessions/del-sess")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

// ── GET /v1/dev-sessions ──────────────────────────────────────────────────────

/// `GET /v1/dev-sessions` returns 200 with a JSON `{ sessions: [...] }`.
/// With an empty registry, `sessions` is an empty array.
#[tokio::test]
async fn get_dev_sessions_empty_returns_200() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/dev-sessions")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sessions = json.get("sessions").and_then(|v| v.as_array()).unwrap();
    assert!(
        sessions.is_empty(),
        "empty registry must return empty sessions array"
    );
}

/// `GET /v1/dev-sessions` with one session returns a single row with the
/// expected fields.
#[tokio::test]
async fn get_dev_sessions_with_one_session_returns_row() {
    use crate::api::dev_sessions::DevSession;
    use std::time::Instant;

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());

    let now = Instant::now();
    state.dev_sessions.insert(DevSession {
        session_id: "list-sess".to_owned(),
        app_uuid: APP_UUID.to_owned(),
        cap: "c".repeat(64),
        created_at: now,
        last_activity: now,
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
    });

    let app = router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/dev-sessions")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sessions = json.get("sessions").and_then(|v| v.as_array()).unwrap();
    assert_eq!(
        sessions.len(),
        1,
        "one session in registry must yield one row"
    );
    let row = &sessions[0];
    assert_eq!(
        row.get("session_id").and_then(|v| v.as_str()),
        Some("list-sess")
    );
    assert_eq!(row.get("app_uuid").and_then(|v| v.as_str()), Some(APP_UUID));
    assert!(
        row.get("created_age_secs").is_some(),
        "row must have created_age_secs"
    );
    assert!(row.get("idle_secs").is_some(), "row must have idle_secs");
}

// ── git_remote URL shape ──────────────────────────────────────────────────────

/// Verify that `generate_cap` output is compatible with the git-proxy route
/// (64 hex chars, no special characters that would break the URL path).
#[test]
fn dev_session_cap_is_url_safe() {
    use crate::api::dev_sessions::generate_cap;
    // `generate_cap` is pub(crate) — only visible within the crate.
    let cap = generate_cap("sess-url-test", "app-url-test");
    // Hex chars are always URL-safe path segments.
    assert!(
        cap.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')),
        "cap must be lowercase hex (URL-safe path segment); got: {cap}"
    );
}

// ── SupervisorState has dev_sessions ─────────────────────────────────────────

/// Confirm that `SupervisorState::new` initialises an empty `dev_sessions`
/// registry (smoke test for the field addition).
#[test]
fn supervisor_state_has_empty_dev_sessions_on_new() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    assert_eq!(
        state.dev_sessions.len(),
        0,
        "dev_sessions must be empty on a fresh SupervisorState"
    );
}

// ── POST /v1/workspaces/:uuid/snapshot — 404 for unknown workspace ──────────

/// Snapshotting a workspace this supervisor does not host returns 404 BEFORE any
/// control dispatch — the endpoint is gated on the registry (Seam B). Proves the
/// route is wired and the not-found gate fires.
#[tokio::test]
async fn snapshot_unknown_workspace_returns_404() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/snapshot"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// A KNOWN workspace whose runner socket is dead returns 500 (the snapshot
/// dispatch fails) — NOT 404. This proves the registry gate passes through to
/// the orchestrator dispatch for a hosted workspace (the VM-still-serving error
/// path; no live runner is wired in the unit test).
#[tokio::test]
async fn snapshot_known_workspace_dispatches_and_500s_without_runner() {
    use crate::api::Workspace;
    use std::time::Instant;

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    // Register the workspace so the registry gate passes; no runner socket
    // exists, so the Cmd::Snapshot round-trip fails → 500 (not 404).
    state.workspaces.insert(Workspace {
        workspace_uuid: APP_UUID.to_owned(),
        user_id: "acct_a".to_owned(),
        caps: vec!["cap".to_owned()],
        created_at: Instant::now(),
        last_activity: Instant::now(),
    });
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/snapshot"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ── POST /v1/workspaces/:uuid/repos — 404 for unknown workspace ─────────────

/// Adding a repo to a workspace this supervisor does not host returns 404 BEFORE
/// any cap registration / respawn — gated on the registry (mirror snapshot).
#[tokio::test]
async fn add_repo_unknown_workspace_returns_404() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let body = serde_json::json!({
        "repo_url": "https://github.com/acme/extra.git",
        "branch": "main",
        "git_token": "ghs_xxx",
        "git_token_ttl_secs": 3300_u64,
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/repos"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// A KNOWN workspace: add_repo registers a NEW git-proxy cap (host-side), appends
/// the durable record, appends the in-mem registry caps, and merges the new
/// `<repo>.url` into the persisted `CAP_FILES_ENV` so the background respawn
/// re-bakes the FULL cap-file set. Returns 202 (async respawn, mirror create).
#[tokio::test]
async fn add_repo_known_workspace_registers_cap_and_appends_record() {
    use crate::api::{
        CAP_FILES_ENV, WORKSPACE_MARKER_ENV, Workspace, WorkspaceCap, WorkspaceRecord,
        cap_repo_basename,
    };
    use crate::orchestrator::handle::RunnerHandle;
    use std::collections::HashMap;
    use std::time::Instant;

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());

    // Seed the workspace: registry entry (caps gate), a durable WorkspaceRecord
    // with one existing repo, and a RunnerHandle carrying the FULL extra_env
    // (CAP_FILES_ENV with the first repo) the respawn merges into.
    let existing_cap = "cap-existing";
    state.workspaces.insert(Workspace {
        workspace_uuid: APP_UUID.to_owned(),
        user_id: "acct_a".to_owned(),
        caps: vec![existing_cap.to_owned()],
        created_at: Instant::now(),
        last_activity: Instant::now(),
    });
    WorkspaceRecord {
        workspace_uuid: APP_UUID.to_owned(),
        user_id: "acct_a".to_owned(),
        caps: vec![WorkspaceCap {
            cap: existing_cap.to_owned(),
            repo_url: "https://github.com/acme/app.git".to_owned(),
        }],
        branches: vec!["main".to_owned()],
        created_at_unix: 1_700_000_000,
        last_activity_unix: 1_700_000_000,
    }
    .save(state.orchestrator.runner_dir())
    .unwrap();
    let mut extra_env = HashMap::new();
    extra_env.insert(WORKSPACE_MARKER_ENV.to_owned(), APP_UUID.to_owned());
    extra_env.insert(
        CAP_FILES_ENV.to_owned(),
        serde_json::json!({ "app.url": "http://h:8788/git/cap-existing" }).to_string(),
    );
    let runner = RunnerHandle {
        uuid: APP_UUID.to_owned(),
        pid: 0,
        control_sock: dir.path().join(format!("{APP_UUID}.sock")),
        app_ula: APP_ULA.to_owned(),
        parent: None,
        spawned_at: 0,
        restart: Default::default(),
        image_ref: Some("reg/ws:latest".to_owned()),
        requested_runtime: None,
        network: None,
        runner_join_token: None,
        manifest_toml: None,
        extra_env: Some(extra_env),
        egress_allow: None,
        crash_looped: false,
        stopped: false,
    };
    runner.save(state.orchestrator.runner_dir()).unwrap();

    let app = router(state.clone());
    let body = serde_json::json!({
        "repo_url": "https://github.com/acme/extra.git",
        "branch": "dev",
        "git_token": "ghs_xxx",
        "git_token_ttl_secs": 3300_u64,
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/repos"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let json: serde_json::Value =
        serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["workspace_uuid"], APP_UUID);
    assert_eq!(json["repo"], cap_repo_basename("https://github.com/acme/extra.git"));
    let git_remote = json["git_remote"].as_str().unwrap();
    assert!(git_remote.contains("/git/"), "git_remote must be a proxy URL");

    // A NEW cap was registered host-side (2 total now).
    assert_eq!(
        state.workspaces.caps_of(APP_UUID).unwrap().len(),
        2,
        "the new repo cap must be appended to the in-mem registry"
    );
    // The durable record gained the repo (2 caps, 2 branches).
    let rec = WorkspaceRecord::load(state.orchestrator.runner_dir(), APP_UUID)
        .unwrap()
        .unwrap();
    assert_eq!(rec.caps.len(), 2, "durable record must append the new cap");
    assert_eq!(rec.caps[1].repo_url, "https://github.com/acme/extra.git");
    assert_eq!(rec.branches, vec!["main".to_owned(), "dev".to_owned()]);
    // The registered git-session cap actually resolves (token injectable).
    let new_cap = &rec.caps[1].cap;
    assert!(
        state.git_sessions.registered_caps().contains(new_cap),
        "the new git-proxy cap must be live in the session registry"
    );
}

// ── POST /v1/workspaces/:uuid/stop — 404 for unknown workspace ──────────────

/// Stopping a workspace this supervisor does not host returns 404 (registry gate,
/// mirror snapshot/delete) — no stray stop dispatch.
#[tokio::test]
async fn stop_unknown_workspace_returns_404() {
    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    let app = router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/stop"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// A KNOWN workspace stop returns 200 `{stopped:true}` and drives
/// `orchestrator.stop_app` (which PRESERVES the record's image_ref/extra_env for
/// a later warm restore). No live runner is wired — stop_app tolerates that and
/// marks the record stopped regardless.
#[tokio::test]
async fn stop_known_workspace_returns_200_and_marks_stopped() {
    use crate::api::Workspace;
    use crate::orchestrator::handle::RunnerHandle;
    use std::time::Instant;

    let dir = TempDir::new().unwrap();
    let state = make_state(dir.path().to_path_buf());
    state.workspaces.insert(Workspace {
        workspace_uuid: APP_UUID.to_owned(),
        user_id: "acct_a".to_owned(),
        caps: vec!["cap".to_owned()],
        created_at: Instant::now(),
        last_activity: Instant::now(),
    });
    // A runner record so stop_app has something to mark stopped + its
    // image_ref/extra_env are preserved (the warm-restore invariant).
    let runner = RunnerHandle {
        uuid: APP_UUID.to_owned(),
        pid: 4242,
        control_sock: dir.path().join(format!("{APP_UUID}.sock")),
        app_ula: APP_ULA.to_owned(),
        parent: None,
        spawned_at: 0,
        restart: Default::default(),
        image_ref: Some("reg/ws:latest".to_owned()),
        requested_runtime: None,
        network: None,
        runner_join_token: None,
        manifest_toml: None,
        extra_env: None,
        egress_allow: None,
        crash_looped: false,
        stopped: false,
    };
    runner.save(state.orchestrator.runner_dir()).unwrap();

    let app = router(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/workspaces/{APP_UUID}/stop"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["stopped"], true);

    // The record is preserved + marked stopped, with image_ref intact (warm
    // restore can respawn from it).
    let rec = RunnerHandle::load(state.orchestrator.runner_dir(), APP_UUID)
        .unwrap()
        .unwrap();
    assert!(rec.stopped, "stop_app must mark the record stopped");
    assert_eq!(
        rec.image_ref.as_deref(),
        Some("reg/ws:latest"),
        "stop must PRESERVE image_ref for warm restore"
    );
}
