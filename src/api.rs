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

use std::sync::Arc;

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

/// JSON row for `GET /v1/apps` (the live runner fleet).
fn summary_json(s: &AppSummary) -> serde_json::Value {
    json!({
        "uuid": s.uuid,
        "app_ula": s.app_ula,
        "state": s.state.as_str(),
        // The app's address IS its app-ULA in the orchestrator model.
        "bound_addr": s.app_ula,
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

fn error_json(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}
