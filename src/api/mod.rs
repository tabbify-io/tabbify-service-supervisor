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
//! - `POST /v1/build`             (clone → build → push to mesh registry)
//!
//! ## Module layout
//! - [`handlers`] — every HTTP handler + the request DTOs (`DeployBody`, `BuildBody`)
//!   + small response/error helpers.
//! - [`dto`]      — response DTOs for `#[utoipa::path]` annotations.

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use crate::fetcher::S3Fetcher;
use crate::orchestrator::Orchestrator;

mod dev_sessions;
mod dto;
mod git_proxy;
mod handlers;

pub use dev_sessions::{
    CreateDevSessionBody, DEV_SESSION_IDLE_TTL, DEV_SESSION_MAX_TTL, DevSessionCreated,
    DevSessionPurged, DevSessionRegistry, DevSessionRow, GitTokenRefreshed, RefreshGitTokenBody,
    sweep_expired,
};
pub use git_proxy::{GitSessionEntry, GitSessions};

// ── Public re-exports — must remain stable for `crate::openapi` + tests. ─────

pub use dto::{
    AboutResponse, AppActionResponse, AppListResponse, AppPresence, AppPurgeResponse,
    AppStopResponse, ErrorResponse, HealthResponse,
};
pub use handlers::{
    BuildBody, DeployBody, about, build_app, deploy_app, get_app, health, list_apps, purge_app,
    reset_app, start_app, stop_app,
};

// utoipa's `#[utoipa::path]` macro generates `__path_<fn>` types in the SAME
// module as the handler. The aggregator `#[derive(OpenApi)] paths(crate::api::<fn>)`
// looks for them under `crate::api`, so we re-export each one here.
#[doc(hidden)]
pub use handlers::{
    __path_about, __path_build_app, __path_deploy_app, __path_get_app, __path_health,
    __path_list_apps, __path_purge_app, __path_reset_app, __path_start_app, __path_stop_app,
};
#[doc(hidden)]
pub use dev_sessions::{
    __path_create_dev_session, __path_delete_dev_session, __path_list_dev_sessions,
    __path_refresh_git_token,
};

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
    /// Running binary's release version (`build.rs`-embedded), surfaced on
    /// `/health` + `/v1/about`. Empty until set via [`Self::with_version`].
    pub version: String,
    /// When this process started serving — drives `/v1/about` uptime.
    pub started_at: std::time::Instant,
    /// Dev-session git proxy registry: capability → upstream URL + token.
    /// Credentials are injected here (outside VMs) and never forwarded to
    /// sandboxes. See [`git_proxy`].
    pub git_sessions: std::sync::Arc<GitSessions>,
    /// Dev-session lifecycle registry: session_id → DevSession (app uuid + cap).
    pub dev_sessions: std::sync::Arc<DevSessionRegistry>,
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
            version: String::new(),
            started_at: std::time::Instant::now(),
            git_sessions: std::sync::Arc::new(GitSessions::default()),
            dev_sessions: std::sync::Arc::new(DevSessionRegistry::default()),
        }
    }

    /// Set the running binary's release version reported on `/health` +
    /// `/v1/about`.
    #[must_use]
    pub fn with_version(mut self, version: String) -> Self {
        self.version = version;
        self
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
/// serving — that lives on the per-app runners' own ULAs). Also mounts
/// `/openapi.json` + `/swagger-ui` for the OpenAPI 3 doc.
///
/// The `/git/:cap/*tail` routes are the git smart-HTTP proxy for tokenless
/// in-VM remotes (dev sessions). They are NOT included in the OpenAPI spec —
/// they speak the git wire protocol, not REST.
pub fn router(state: SupervisorState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/about", get(about))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/:uuid", get(get_app))
        .route("/v1/apps/:uuid/start", post(start_app))
        .route("/v1/apps/:uuid/stop", post(stop_app))
        .route("/v1/apps/:uuid/purge", post(purge_app))
        .route("/v1/apps/:uuid/reset", post(reset_app))
        .route("/v1/apps/:uuid/deploy", post(deploy_app))
        .route("/v1/build", post(build_app))
        // Dev-session lifecycle endpoints.
        .route(
            "/v1/dev-sessions",
            post(dev_sessions::create_dev_session).get(dev_sessions::list_dev_sessions),
        )
        .route(
            "/v1/dev-sessions/:id/git-token",
            post(dev_sessions::refresh_git_token),
        )
        .route(
            "/v1/dev-sessions/:id",
            axum::routing::delete(dev_sessions::delete_dev_session),
        )
        // Git smart-HTTP proxy — tokenless in-VM remote (dev sessions).
        // Not in OpenAPI (wire protocol, not REST).
        .route(
            "/git/:cap/*tail",
            get(git_proxy::git_proxy).post(git_proxy::git_proxy),
        )
        .merge(crate::openapi::swagger_routes())
        .with_state(Arc::new(state))
}

/// Shared handler state behind an `Arc` — the axum `State<...>` extractor type.
pub(crate) type SharedState = Arc<SupervisorState>;

#[cfg(test)]
mod tests;
