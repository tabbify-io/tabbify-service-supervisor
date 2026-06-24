//! utoipa `OpenAPI` document + Swagger UI mount.
//!
//! Every REST handler in [`crate::api`] carries `#[utoipa::path]`; this module
//! aggregates them into one `OpenAPI` 3 document served at `GET /openapi.json`,
//! with Swagger UI at `/swagger-ui`. Both are unauthenticated — the supervisor
//! CONTROL API is internal (mesh-side, peer-ULA bound) so there is no Bearer
//! security scheme.

use std::sync::Arc;

use axum::Router;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::api::{
    AboutResponse, AddRepoBody, AddRepoResult, AppActionResponse, AppListResponse, AppPresence,
    AppPurgeResponse, AppStopResponse, BuildBody, CreateDevSessionBody, CreateWorkspaceBody,
    DeployBody, DevSessionCreated, DevSessionPurged, DevSessionRow, ErrorResponse,
    GitTokenRefreshed, HealthResponse, RefreshGitTokenBody, RepoSpec, SupervisorState,
    WorkspaceCreated,
};
use crate::runner::build::{ArtifactRef, BuildJob, BuildKind};

/// Aggregated `OpenAPI` 3 document for the supervisor CONTROL API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "tabbify-supervisor",
        version = "0.1.0",
        description = "Per-host orchestrator: builds app images, spawns per-app \
                       runners as mesh peers, and drives their lifecycle. \
                       Internal mesh-side API (peer-ULA bound, no public auth)."
    ),
    paths(
        crate::api::health,
        crate::api::about,
        crate::api::list_apps,
        crate::api::get_app,
        crate::api::start_app,
        crate::api::stop_app,
        crate::api::purge_app,
        crate::api::reset_app,
        crate::api::deploy_app,
        crate::api::build_app,
        crate::api::create_dev_session,
        crate::api::refresh_git_token,
        crate::api::delete_dev_session,
        crate::api::list_dev_sessions,
        crate::api::create_workspace,
        crate::api::list_workspaces,
        crate::api::delete_workspace,
        crate::api::snapshot_workspace,
        crate::api::add_workspace_repo,
        crate::api::stop_workspace,
    ),
    components(schemas(
        HealthResponse,
        AboutResponse,
        AppPresence,
        AppListResponse,
        AppActionResponse,
        AppStopResponse,
        AppPurgeResponse,
        ErrorResponse,
        DeployBody,
        BuildBody,
        BuildJob,
        BuildKind,
        ArtifactRef,
        CreateDevSessionBody,
        DevSessionCreated,
        RefreshGitTokenBody,
        GitTokenRefreshed,
        DevSessionPurged,
        DevSessionRow,
        CreateWorkspaceBody,
        RepoSpec,
        WorkspaceCreated,
        AddRepoBody,
        AddRepoResult,
    ))
)]
pub struct ApiDoc;

/// Router serving the Swagger UI (`/swagger-ui`) + the raw spec
/// (`/openapi.json`).
///
/// Returns a `Router<Arc<SupervisorState>>` so it merges into the main
/// supervisor router before its `.with_state(Arc::new(state))` is applied.
pub fn swagger_routes() -> Router<Arc<SupervisorState>> {
    SwaggerUi::new("/swagger-ui")
        .url("/openapi.json", ApiDoc::openapi())
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OpenAPI doc builds and enumerates every supervisor route.
    #[test]
    fn openapi_doc_enumerates_routes() {
        let doc = ApiDoc::openapi();
        let paths = &doc.paths.paths;
        for expected in [
            "/health",
            "/v1/about",
            "/v1/apps",
            "/v1/apps/{uuid}",
            "/v1/apps/{uuid}/start",
            "/v1/apps/{uuid}/stop",
            "/v1/apps/{uuid}/purge",
            "/v1/apps/{uuid}/reset",
            "/v1/apps/{uuid}/deploy",
            "/v1/build",
            "/v1/dev-sessions",
            "/v1/dev-sessions/{id}/git-token",
            "/v1/dev-sessions/{id}",
        ] {
            assert!(paths.contains_key(expected), "missing path {expected}");
        }
    }

    /// The doc serializes cleanly to JSON and carries the service title.
    #[test]
    fn openapi_serializes_to_json() {
        let json = ApiDoc::openapi().to_json().expect("serialize openapi");
        assert!(json.contains("tabbify-supervisor"));
        assert!(json.contains("/v1/build"));
        assert!(json.contains("/v1/apps/{uuid}/deploy"));
    }
}
