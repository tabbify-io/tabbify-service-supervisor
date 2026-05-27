//! Supervisor CONTROL HTTP API (axum, contract §5). Bound on `[peer_ula]:8730`
//! in prod; tests bind a loopback addr and drive the router directly via
//! `tower::ServiceExt::oneshot`.
//!
//! Since the per-app-runner refactor (Task 2.6) the control plane drives the
//! runner [`Orchestrator`] instead of hosting apps in-process: `start` spawns a
//! detached `tabbify-runner` and waits until it is healthy; `stop` / `purge`
//! shut the runner down (and `purge` reclaims its cache) and forget it; `list` /
//! `get` read the live runner fleet (records + a quick control-socket health
//! probe). App traffic is served by the per-app runners on their own app-ULAs,
//! NOT here — there is no `/apps/<uuid>/*` route on the control port; an app's
//! ULA IS its address.
//!
//! Endpoints:
//! - `GET  /health`
//! - `GET  /v1/apps`
//! - `GET  /v1/apps/:uuid`
//! - `POST /v1/apps/:uuid/start`  (spawn a runner + wait healthy)
//! - `POST /v1/apps/:uuid/stop`   (shutdown the runner + forget)
//! - `POST /v1/apps/:uuid/purge`  (purge + shutdown the runner + forget + clear cache)
//! - `POST /v1/apps/:uuid/reset`  (clear crash-loop/backoff state + retry immediately)
//! - `POST /v1/apps/:uuid/deploy` (zero-downtime swap or cold spawn pinned to `ref`)

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http::StatusCode;
use serde_json::json;

use crate::fetcher::{FetchError, S3Fetcher};
use crate::orchestrator::{AppState, AppSummary, Orchestrator};

/// Shared handler state.
#[derive(Clone)]
pub struct SupervisorState {
    /// Runner orchestrator — spawns / monitors / re-adopts per-app runners.
    pub orchestrator: Orchestrator,
    /// S3 fetcher, used ONLY by the discovery path (`GET /v1/apps/:uuid` for an
    /// app the orchestrator has no runner for): probe whether the artifact is
    /// fetchable so the endpoint can report `present: true/false`.
    pub fetcher: S3Fetcher,
    /// Stable-ish supervisor id (peer id, or a local placeholder w/o mesh).
    pub supervisor_id: String,
    /// Our control ULA (peer-ULA), or the bind addr's host when running w/o mesh.
    pub ula: String,
    /// Whether this host can run Firecracker microVMs (/dev/kvm present). Surfaced
    /// on `/health` so an operator can see at a glance what this supervisor can run.
    pub firecracker: bool,
    /// Whether this host can run Docker containers (daemon reachable). Surfaced
    /// on `/health` alongside `firecracker`.
    pub docker: bool,
}

impl SupervisorState {
    /// Construct shared state over the runner `orchestrator` + a `fetcher` for
    /// the discovery path. Firecracker + docker capabilities default off; set
    /// them with [`Self::with_firecracker`] / [`Self::with_docker`].
    #[must_use]
    pub fn new(
        orchestrator: Orchestrator,
        fetcher: S3Fetcher,
        supervisor_id: String,
        ula: String,
    ) -> Self {
        Self {
            orchestrator,
            fetcher,
            supervisor_id,
            ula,
            firecracker: false,
            docker: false,
        }
    }

    /// Set the Firecracker (KVM) capability reported on `/health`.
    #[must_use]
    pub fn with_firecracker(mut self, firecracker: bool) -> Self {
        self.firecracker = firecracker;
        self
    }

    /// Set the Docker capability reported on `/health`.
    #[must_use]
    pub fn with_docker(mut self, docker: bool) -> Self {
        self.docker = docker;
        self
    }
}

/// Build the axum [`Router`] with the supervisor CONTROL endpoints (no app
/// serving — that lives on the per-app runners' own ULAs).
pub fn router(state: SupervisorState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/:uuid", get(get_app))
        .route("/v1/apps/:uuid/start", post(start_app))
        .route("/v1/apps/:uuid/stop", post(stop_app))
        .route("/v1/apps/:uuid/purge", post(purge_app))
        .route("/v1/apps/:uuid/reset", post(reset_app))
        .route("/v1/apps/:uuid/deploy", post(deploy_app))
        .route("/v1/build", post(build_app))
        .with_state(Arc::new(state))
}

type SharedState = Arc<SupervisorState>;

async fn health(State(state): State<SharedState>) -> Response {
    axum::Json(json!({
        "status": "ok",
        "supervisor_id": state.supervisor_id,
        "ula": state.ula,
        "firecracker": state.firecracker,
        "docker": state.docker,
    }))
    .into_response()
}

async fn list_apps(State(state): State<SharedState>) -> Response {
    match state.orchestrator.app_summaries().await {
        Ok(apps) => {
            let apps: Vec<_> = apps.iter().map(summary_json).collect();
            axum::Json(json!({ "apps": apps })).into_response()
        }
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn get_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    // Does the orchestrator have a runner record for it? Report its live state.
    match state.orchestrator.app_summary(&uuid).await {
        Ok(Some(s)) => return present_json(&s).into_response(),
        Ok(None) => {}
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    }
    // No runner: probe S3 (discovery — present iff fetchable) so a client can
    // learn the app exists before starting it. We do NOT spawn here.
    match state.fetcher.latest_version(&uuid).await {
        Ok(_) => present_discovered(&state, &uuid),
        Err(FetchError::NotFound(_)) => not_present(&uuid),
        Err(e) => fetch_error_response(&e),
    }
}

/// `POST /v1/apps/:uuid/start` — spawn a detached runner (if none live) + wait
/// until it is healthy. Idempotent: an already-running app returns its current
/// state. Returns `{state, app_ula, bound_addr}` where `bound_addr` is the
/// app-ULA (the runner serves on its own ULA — there is no in-supervisor
/// listener address any more).
async fn start_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.start_app(&uuid).await {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => anyhow_to_response(&e),
    }
}

/// `POST /v1/apps/:uuid/stop` — shut the runner down (it exits, KEEPING its
/// on-disk artifacts + docker image for a fast restart) + forget its record.
async fn stop_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.stop_app(&uuid).await {
        Ok(()) => axum::Json(json!({ "state": "stopped" })).into_response(),
        Err(e) => anyhow_to_response(&e),
    }
}

/// `POST /v1/apps/:uuid/purge` — full teardown: purge the runner (it clears its
/// cache + removes its docker image) then shut it down, forget its record, and
/// reclaim the on-disk cache. The disk-reclaiming counterpart to `stop`.
async fn purge_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.purge_app(&uuid).await {
        Ok(()) => axum::Json(json!({ "state": "purged", "uuid": uuid })).into_response(),
        Err(e) => anyhow_to_response(&e),
    }
}

/// `POST /v1/apps/:uuid/reset` — clear the crash-loop / backoff state and retry
/// immediately. This is the `systemctl reset-failed` analog: it zeroes the
/// consecutive-failure counter so a dead runner is eligible for an immediate
/// respawn. Unlike `purge` it does NOT delete the artifact cache.
///
/// Returns the app's current status JSON (same shape as `start`). Returns `404`
/// when no runner record exists (the app was never started).
async fn reset_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.reset_app(&uuid).await {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => anyhow_to_not_found_or_error(&e),
    }
}

/// Request body for `POST /v1/apps/:uuid/deploy`.
#[derive(serde::Deserialize)]
struct DeployBody {
    /// OCI image ref to deploy (e.g. `[fd5a::1]:5000/acme/app:sha256abc`).
    ///
    /// Renamed from `reff` because `ref` is a Rust keyword.
    #[serde(rename = "ref")]
    reff: String,
}

/// `POST /v1/apps/:uuid/deploy` — zero-downtime swap if a runner is live, or
/// cold spawn pinned to `ref` if not. Persists the deployed ref so a future
/// supervisor restart respawns the runner on the same version.
///
/// Returns the app's current status JSON (same shape as `start` / `reset`).
/// Returns `404` when no runner record exists AND the cold spawn fails because
/// the uuid is unknown; otherwise mirrors `reset_app` error semantics.
async fn deploy_app(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    Json(body): Json<DeployBody>,
) -> Response {
    match state.orchestrator.deploy_app(&uuid, &body.reff).await {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => anyhow_to_not_found_or_error(&e),
    }
}

/// Request body for `POST /v1/build`.
#[derive(serde::Deserialize)]
struct BuildBody {
    /// HTTPS URL of the Git repository to clone.
    repo_url: String,
    /// Git ref (branch, tag, or full SHA) to check out.
    ///
    /// Serialized as `"ref"` (a JSON key) because `ref` is a Rust keyword.
    #[serde(rename = "ref")]
    git_ref: String,
    /// Tenant namespace used as the registry path prefix.
    tenant: String,
    /// UUID of the app; used in the image tag as `<tenant>/<app_uuid>:<git_ref>`.
    app_uuid: String,
    /// Mesh ULA + port of the registry to push to.
    registry_ula: String,
    /// Short-lived clone token (`None` = public repo).
    #[serde(default)]
    clone_token: Option<String>,
    /// Token for pushing to the registry (`None` = anonymous).
    #[serde(default)]
    push_token: Option<String>,
    /// Which build pipeline to run. Absent ⇒ [`BuildKind::Docker`] (the original
    /// behaviour); `"wasm"` selects the wasm-component path.
    #[serde(default)]
    build_kind: crate::runner::build::BuildKind,
    /// (Wasm only) shell command that produces the `.wasm`, run with the cloned
    /// source dir as cwd. Ignored by the docker path.
    #[serde(default)]
    build_cmd: Option<String>,
    /// (Wasm only) path to the produced `.wasm`, relative to the repo root.
    /// Ignored by the docker path.
    #[serde(default)]
    artifact_path: Option<String>,
}

/// `POST /v1/build` — dispatch a one-shot build: clone `repo_url`@`ref`, build
/// an OCI image, push it to `registry_ula`, and return the [`ArtifactRef`] as
/// JSON.
///
/// The full multi-target control-plane (build-then-deploy across a fleet) is
/// Phase 4; this is the minimal invoker.
async fn build_app(State(state): State<SharedState>, Json(body): Json<BuildBody>) -> Response {
    use crate::runner::build::BuildJob;
    let job = BuildJob {
        repo_url: body.repo_url,
        git_ref: body.git_ref,
        tenant: body.tenant,
        app_uuid: body.app_uuid,
        registry_ula: body.registry_ula,
        clone_token: body.clone_token,
        push_token: body.push_token,
        // Threaded from the request: a caller may select the wasm build path
        // (`build_kind: "wasm"` + `build_cmd` + `artifact_path`); all three
        // default to the docker pipeline when omitted (unchanged behaviour).
        build_kind: body.build_kind,
        build_cmd: body.build_cmd,
        artifact_path: body.artifact_path,
    };
    match state.orchestrator.spawn_build(&job).await {
        Ok(art) => axum::Json(art).into_response(),
        Err(e) => anyhow_to_response(&e),
    }
}

/// JSON row for `GET /v1/apps` (the live runner fleet).
fn summary_json(s: &AppSummary) -> serde_json::Value {
    json!({
        "uuid": s.uuid,
        "app_ula": s.app_ula,
        "state": s.state.as_str(),
        // The app's address IS its app-ULA in the orchestrator model.
        "bound_addr": s.app_ula,
        "restart_status": s.restart_status,
        "restart_count": s.restart_count,
        "next_retry_at": s.next_retry_at,
    })
}

/// JSON for `POST /v1/apps/:uuid/start`.
fn running_json(s: &AppSummary) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "state": s.state.as_str(),
        "app_ula": s.app_ula,
        // The runner serves on its OWN ULA, so the bound address is the app-ULA.
        "bound_addr": s.app_ula,
    }))
}

/// JSON for `GET /v1/apps/:uuid` when the orchestrator has a runner record.
fn present_json(s: &AppSummary) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "uuid": s.uuid,
        "present": true,
        "state": s.state.as_str(),
        "app_ula": s.app_ula,
        "bound_addr": s.app_ula,
        "restart_status": s.restart_status,
        "restart_count": s.restart_count,
        "next_retry_at": s.next_retry_at,
    }))
}

/// JSON for `GET /v1/apps/:uuid` when there is no runner but the artifact is
/// fetchable from S3 (discovery). State is `stopped` (no live runner); the
/// app-ULA is still deterministic from the uuid.
fn present_discovered(state: &SupervisorState, uuid: &str) -> Response {
    let app_ula = match state.orchestrator.app_ula_for(uuid) {
        Ok(u) => u.to_string(),
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    axum::Json(json!({
        "uuid": uuid,
        "present": true,
        "state": AppState::Stopped.as_str(),
        "app_ula": app_ula,
        "bound_addr": null,
    }))
    .into_response()
}

fn not_present(uuid: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({ "uuid": uuid, "present": false })),
    )
        .into_response()
}

fn fetch_error_response(e: &FetchError) -> Response {
    match e {
        FetchError::NotFound(uuid) => not_present(uuid),
        other => error_json(StatusCode::BAD_GATEWAY, &other.to_string()),
    }
}

/// Map an `anyhow::Error` from the orchestrator to an HTTP response. A
/// [`FetchError`] underneath maps via [`fetch_error_response`]; everything else
/// is 500.
fn anyhow_to_response(e: &anyhow::Error) -> Response {
    if let Some(fe) = e.downcast_ref::<FetchError>() {
        return fetch_error_response(fe);
    }
    error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
}

/// Like [`anyhow_to_response`] but also maps "no runner record found" messages
/// to 404. Used by [`reset_app`] so a reset of an unknown uuid returns 404
/// rather than 500.
fn anyhow_to_not_found_or_error(e: &anyhow::Error) -> Response {
    let msg = e.to_string();
    if msg.contains("no runner record found") {
        return (StatusCode::NOT_FOUND, axum::Json(json!({ "error": msg }))).into_response();
    }
    anyhow_to_response(e)
}

fn error_json(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use axum::body::Body;
    use http::Request;
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
        let resp = anyhow_to_not_found_or_error(&e);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Any other error maps to 500.
    #[test]
    fn generic_error_maps_to_500() {
        let e = anyhow::anyhow!("something went wrong");
        let resp = anyhow_to_not_found_or_error(&e);
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

    /// A body WITH `build_kind: "wasm"` (+ `build_cmd` / `artifact_path`) parses
    /// to `BuildKind::Wasm` and carries the wasm build coordinates through.
    #[test]
    fn build_body_parses_wasm_build_kind() {
        use crate::runner::build::BuildKind;
        let json = r#"{
            "repo_url":"https://github.com/acme/app",
            "ref":"abc123",
            "tenant":"acme",
            "app_uuid":"u",
            "registry_ula":"[fd5a::1]:5000",
            "build_kind":"wasm",
            "build_cmd":"cargo build --release --target wasm32-wasip2",
            "artifact_path":"target/wasm32-wasip2/release/app.wasm"
        }"#;
        let body: BuildBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.build_kind, BuildKind::Wasm);
        assert_eq!(
            body.build_cmd.as_deref(),
            Some("cargo build --release --target wasm32-wasip2")
        );
        assert_eq!(
            body.artifact_path.as_deref(),
            Some("target/wasm32-wasip2/release/app.wasm")
        );
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
}
