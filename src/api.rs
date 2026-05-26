//! Supervisor CONTROL HTTP API (axum, contract §5). Bound on `[peer_ula]:8730`
//! in prod; tests bind a loopback addr and drive the router directly via
//! `tower::ServiceExt::oneshot`.
//!
//! This is the CONTROL plane only — app traffic is served by the per-app-ULA
//! listeners ([`crate::host`]), NOT here. There is no `/apps/<uuid>/*` route on
//! the control port any more; an app's ULA IS its address.
//!
//! Endpoints:
//! - `GET  /health`
//! - `GET  /v1/apps`
//! - `GET  /v1/apps/:uuid`
//! - `POST /v1/apps/:uuid/start`  (fetch + host on the app-ULA + PIN)
//! - `POST /v1/apps/:uuid/stop`   (unhost + unpin)

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http::StatusCode;
use serde_json::json;

use crate::fetcher::FetchError;
use crate::registry::{AppRegistry, AppSummary};

/// Shared handler state.
#[derive(Clone)]
pub struct SupervisorState {
    /// App registry + lifecycle + per-app-ULA hosting.
    pub registry: AppRegistry,
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
    /// Construct shared state. Firecracker + docker capabilities default off;
    /// set them with [`Self::with_firecracker`] / [`Self::with_docker`].
    #[must_use]
    pub fn new(registry: AppRegistry, supervisor_id: String, ula: String) -> Self {
        Self {
            registry,
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
/// serving — that lives on the per-app-ULA listeners).
pub fn router(state: SupervisorState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/:uuid", get(get_app))
        .route("/v1/apps/:uuid/start", post(start_app))
        .route("/v1/apps/:uuid/stop", post(stop_app))
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
    let apps: Vec<_> = state
        .registry
        .list()
        .into_iter()
        .map(summary_json)
        .collect();
    axum::Json(json!({ "apps": apps })).into_response()
}

async fn get_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    // Known already? Report its live state + app_ula.
    if let Some(s) = state.registry.get(&uuid) {
        return present_json(&s).into_response();
    }
    // Not known: probe S3 (discovery — present iff fetchable). We do NOT host
    // here, just confirm the artifact exists + learn metadata.
    match state.registry.is_fetchable(&uuid).await {
        Ok(true) => match state.registry.ensure_known(&uuid).await {
            Ok(s) => present_json(&s).into_response(),
            Err(_) => not_present(&uuid),
        },
        Ok(false) => not_present(&uuid),
        Err(e) => fetch_error_response(&e),
    }
}

/// `POST /v1/apps/:uuid/start` — fetch + host on the app-ULA + PIN (sticky).
async fn start_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.registry.ensure_running(&uuid, /* pin */ true).await {
        Ok(_) => match state.registry.get(&uuid) {
            Some(s) => axum::Json(json!({
                "state": "running",
                "app_ula": s.app_ula.to_string(),
                "bound_addr": s.bound_addr.map(|a| a.to_string()),
            }))
            .into_response(),
            None => error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "app vanished after start",
            ),
        },
        Err(e) => anyhow_to_response(&e),
    }
}

/// `POST /v1/apps/:uuid/stop` — unhost the app-ULA (abort listener + joiner
/// unhost) + unpin.
async fn stop_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    let _ = state.registry.stop(&uuid).await;
    axum::Json(json!({ "state": "stopped" })).into_response()
}

fn summary_json(s: AppSummary) -> serde_json::Value {
    json!({
        "uuid": s.uuid,
        "app_ula": s.app_ula.to_string(),
        "version": s.version,
        "name": s.name,
        "lifecycle": lifecycle_str(s.lifecycle),
        "state": s.state.as_str(),
        "bound_addr": s.bound_addr.map(|a| a.to_string()),
    })
}

fn present_json(s: &AppSummary) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "uuid": s.uuid,
        "present": true,
        "version": s.version,
        "state": s.state.as_str(),
        "app_ula": s.app_ula.to_string(),
        "bound_addr": s.bound_addr.map(|a| a.to_string()),
    }))
}

fn not_present(uuid: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({ "uuid": uuid, "present": false })),
    )
        .into_response()
}

fn lifecycle_str(mode: crate::manifest::LifecycleMode) -> &'static str {
    match mode {
        crate::manifest::LifecycleMode::AlwaysOn => "always_on",
        crate::manifest::LifecycleMode::OnRequest => "on_request",
    }
}

fn fetch_error_response(e: &FetchError) -> Response {
    match e {
        FetchError::NotFound(uuid) => not_present(uuid),
        other => error_json(StatusCode::BAD_GATEWAY, &other.to_string()),
    }
}

/// Map an `anyhow::Error` from the registry to an HTTP response. A NotFound
/// underneath becomes 404; everything else is 502 (upstream/S3 problem) or 500.
fn anyhow_to_response(e: &anyhow::Error) -> Response {
    if let Some(fe) = e.downcast_ref::<FetchError>() {
        return fetch_error_response(fe);
    }
    error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
}

fn error_json(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}
