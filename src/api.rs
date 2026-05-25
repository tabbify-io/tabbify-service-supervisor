//! Supervisor HTTP API (axum, contract §5). Bound on `[my_ula]:8730` in prod;
//! tests bind a loopback addr and drive the router directly via
//! `tower::ServiceExt::oneshot`.
//!
//! Endpoints:
//! - `GET  /health`
//! - `GET  /v1/apps`
//! - `GET  /v1/apps/:uuid`
//! - `POST /v1/apps/:uuid/start`  (fetch + spawn + PIN)
//! - `POST /v1/apps/:uuid/stop`   (stop + unpin)
//! - `ANY  /apps/:uuid/*rest`     (strip prefix, lazy-spawn, run wasm, return)

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::json;

use crate::fetcher::FetchError;
use crate::registry::{AppRegistry, AppSummary};

/// Shared handler state.
#[derive(Clone)]
pub struct SupervisorState {
    /// App registry + lifecycle.
    pub registry: AppRegistry,
    /// Stable-ish supervisor id (peer id, or a local placeholder w/o mesh).
    pub supervisor_id: String,
    /// Our serving ULA (or the bind addr's host when running w/o mesh).
    pub ula: String,
}

impl SupervisorState {
    /// Construct shared state.
    #[must_use]
    pub fn new(registry: AppRegistry, supervisor_id: String, ula: String) -> Self {
        Self {
            registry,
            supervisor_id,
            ula,
        }
    }
}

/// Build the axum [`Router`] with all supervisor endpoints.
pub fn router(state: SupervisorState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/:uuid", get(get_app))
        .route("/v1/apps/:uuid/start", post(start_app))
        .route("/v1/apps/:uuid/stop", post(stop_app))
        .route("/apps/:uuid/*rest", any(serve_app))
        // Also serve the bare app root (`/apps/<uuid>` and `/apps/<uuid>/`).
        .route("/apps/:uuid", any(serve_app_root))
        .route("/apps/:uuid/", any(serve_app_root))
        .with_state(Arc::new(state))
}

type SharedState = Arc<SupervisorState>;

async fn health(State(state): State<SharedState>) -> Response {
    axum::Json(json!({
        "status": "ok",
        "supervisor_id": state.supervisor_id,
        "ula": state.ula,
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
        return present_json(&uuid, &s).into_response();
    }
    // Not known: probe S3 (discovery — present iff fetchable). We do NOT spawn
    // here, just confirm the artifact exists.
    match state.registry.is_fetchable(&uuid).await {
        Ok(true) => {
            // Learn metadata so the version/state are accurate.
            match state.registry.ensure_known(&uuid).await {
                Ok(s) => present_json(&uuid, &s).into_response(),
                Err(_) => not_present(&uuid),
            }
        }
        Ok(false) => not_present(&uuid),
        Err(e) => fetch_error_response(&e),
    }
}

async fn start_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.registry.ensure_running(&uuid, /* pin */ true).await {
        Ok(_) => {
            let app_ula = app_ula_for(&uuid);
            axum::Json(json!({ "state": "running", "app_ula": app_ula })).into_response()
        }
        Err(e) => anyhow_to_response(&e),
    }
}

async fn stop_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    let _ = state.registry.stop(&uuid);
    axum::Json(json!({ "state": "stopped" })).into_response()
}

/// `ANY /apps/:uuid/*rest` — strip the `/apps/<uuid>` prefix, lazy-spawn per
/// lifecycle, run the wasm, return its response.
async fn serve_app(
    State(state): State<SharedState>,
    Path((uuid, rest)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    serve_inner(&state, &uuid, &format!("/{rest}"), req).await
}

/// `ANY /apps/:uuid` and `/apps/:uuid/` — same as [`serve_app`] with `/` path.
async fn serve_app_root(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    req: Request<Body>,
) -> Response {
    serve_inner(&state, &uuid, "/", req).await
}

async fn serve_inner(
    state: &SupervisorState,
    uuid: &str,
    stripped_path: &str,
    req: Request<Body>,
) -> Response {
    // Lazy-spawn per lifecycle if not already running (no pin — request traffic
    // doesn't pin; only API start does).
    if state.registry.take_runtime_for_request(uuid).is_none() {
        if let Err(e) = state.registry.ensure_running(uuid, /* pin */ false).await {
            return anyhow_to_response(&e);
        }
    }
    let Some(runtime) = state.registry.take_runtime_for_request(uuid) else {
        return error_json(StatusCode::NOT_FOUND, "app not running");
    };

    // Rebuild the request with the prefix stripped, preserving method, headers,
    // query string, and the (buffered) body.
    let rewritten = match rewrite_request(req, stripped_path).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    match runtime.handle(rewritten).await {
        Ok(resp) => wasm_response_to_axum(resp),
        Err(e) => error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("wasm execution failed: {e}"),
        ),
    }
}

/// Translate an inbound axum request into the `http::Request<Bytes>` the wasm
/// runtime expects, replacing the path with `stripped_path` but keeping the
/// query string. Buffers the body fully (Phase-1).
async fn rewrite_request(
    req: Request<Body>,
    stripped_path: &str,
) -> Result<Request<Bytes>, Response> {
    let (mut parts, body) = req.into_parts();

    let query = parts
        .uri
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let new_path_and_query = format!("{stripped_path}{query}");
    parts.uri = match Uri::builder().path_and_query(new_path_and_query).build() {
        Ok(u) => u,
        Err(e) => {
            return Err(error_json(
                StatusCode::BAD_REQUEST,
                &format!("bad rewritten uri: {e}"),
            ));
        }
    };

    let collected = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            return Err(error_json(
                StatusCode::BAD_REQUEST,
                &format!("read request body: {e}"),
            ));
        }
    };

    Ok(Request::from_parts(parts, collected))
}

/// Translate the wasm `http::Response<Bytes>` back into an axum response,
/// preserving status + headers.
fn wasm_response_to_axum(resp: http::Response<Bytes>) -> Response {
    let (parts, body) = resp.into_parts();
    let mut builder = Response::builder().status(parts.status);
    if let Some(headers) = builder.headers_mut() {
        *headers = parts.headers;
    }
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| error_json(StatusCode::INTERNAL_SERVER_ERROR, "build response"))
}

fn summary_json(s: AppSummary) -> serde_json::Value {
    json!({
        "uuid": s.uuid,
        "version": s.version,
        "name": s.name,
        "lifecycle": lifecycle_str(s.lifecycle),
        "state": s.state.as_str(),
    })
}

fn present_json(uuid: &str, s: &AppSummary) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "uuid": uuid,
        "present": true,
        "version": s.version,
        "state": s.state.as_str(),
        "app_ula": app_ula_for(uuid),
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

/// Compute the deterministic app-ULA string for an app uuid, or empty when the
/// uuid is malformed (Phase-1 reports it for forward-compat; not used to bind).
fn app_ula_for(uuid: &str) -> String {
    uuid::Uuid::parse_str(uuid)
        .map(|u| crate::app_ula::derive_app_ula(u).to_string())
        .unwrap_or_default()
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
