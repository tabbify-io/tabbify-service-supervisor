//! HTTP request handlers + request DTOs + small response helpers.
//!
//! Each handler carries a `#[utoipa::path]` annotation so [`crate::openapi`]
//! can enumerate it into the aggregated OpenAPI 3 document.

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;

use super::{
    SharedState,
    dto::{
        AboutResponse, AppActionResponse, AppListResponse, AppPresence, AppPurgeResponse,
        AppStopResponse, ErrorResponse, HealthResponse,
    },
};
use crate::{
    fetcher::FetchError,
    orchestrator::{AppState, AppSummary},
};

/// Liveness probe + capability report.
///
/// Surfaces this host's Firecracker / Docker availability so an operator can
/// see at a glance what this supervisor can run.
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Supervisor is alive", body = HealthResponse),
    ),
)]
#[tracing::instrument(skip_all)]
pub async fn health(State(state): State<SharedState>) -> Response {
    axum::Json(json!({
        "status": "ok",
        "supervisor_id": state.supervisor_id,
        "ula": state.ula,
        "version": state.version,
        "firecracker": state.firecracker,
        "docker": state.docker,
    }))
    .into_response()
}

/// Self-identification for the self-update control plane: running version,
/// peer id, mesh status, uptime.
#[utoipa::path(
    get,
    path = "/v1/about",
    responses((status = 200, description = "Supervisor self-identification", body = AboutResponse)),
)]
#[tracing::instrument(skip_all)]
pub async fn about(State(state): State<SharedState>) -> Response {
    let mesh_status = if state.ula.contains(':') && !state.ula.starts_with("0.0.0.0") {
        "joined"
    } else {
        "no_mesh"
    };
    axum::Json(json!({
        "version": state.version,
        "peer_id": state.supervisor_id,
        "mesh_status": mesh_status,
        "uptime_secs": state.started_at.elapsed().as_secs(),
    }))
    .into_response()
}

/// List the live runner fleet on this supervisor.
///
/// Each row reflects the result of one control-socket health probe per runner
/// record on disk, plus the persisted restart / backoff state.
#[utoipa::path(
    get,
    path = "/v1/apps",
    responses(
        (
            status = 200,
            description = "Snapshot of the runner fleet on this supervisor",
            body = AppListResponse,
            example = json!({
                "apps": [
                    {
                        "uuid": "0191e7c2-0000-7000-8000-000000000001",
                        "app_ula": "fd5a:1f02:abcdef::1",
                        "state": "running",
                        "bound_addr": "fd5a:1f02:abcdef::1",
                        "restart_status": "running",
                        "restart_count": 0,
                        "next_retry_at": 0
                    }
                ]
            })
        ),
        (status = 500, description = "Failed to list runner records", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip_all)]
pub async fn list_apps(State(state): State<SharedState>) -> Response {
    match state.orchestrator.app_summaries().await {
        Ok(apps) => {
            let apps: Vec<_> = apps.iter().map(summary_json).collect();
            axum::Json(json!({ "apps": apps })).into_response()
        }
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Look up a single app.
///
/// If the orchestrator holds a runner record for the uuid, returns its live
/// state. Otherwise probes S3: if the artifact is fetchable, returns a
/// discovery row (`state: "stopped"`, `bound_addr: null`); otherwise 404.
#[utoipa::path(
    get,
    path = "/v1/apps/{uuid}",
    params(("uuid" = String, Path, description = "App UUID v7")),
    responses(
        (status = 200, description = "App found", body = AppPresence),
        (status = 404, description = "App not present", body = ErrorResponse),
        (status = 400, description = "Malformed uuid", body = ErrorResponse),
        (status = 502, description = "S3 probe failed", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(uuid = %uuid))]
pub async fn get_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
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
#[utoipa::path(
    post,
    path = "/v1/apps/{uuid}/start",
    params(("uuid" = String, Path, description = "App UUID v7")),
    responses(
        (status = 200, description = "Runner is healthy", body = AppActionResponse),
        (status = 404, description = "Artifact not found in S3", body = ErrorResponse),
        (status = 500, description = "Spawn or health-probe failed", body = ErrorResponse),
        (status = 502, description = "S3 fetch failed", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(uuid = %uuid))]
pub async fn start_app(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    body: Option<Json<StartBody>>,
) -> Response {
    // The optional `{"runtime": ...}` override (D4 wire string) is forwarded
    // through the orchestrator into `build_runtime`. `None`/no-body keeps the
    // historical manifest-default behaviour (D10).
    let runtime_override = body
        .and_then(|Json(b)| b.runtime)
        .map(|r| r.as_wire().to_owned());
    match state
        .orchestrator
        .start_app(&uuid, runtime_override.as_deref())
        .await
    {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => {
            let tail = state.orchestrator.runner_log_tail(&uuid, 20).await;
            anyhow_to_response_with_tail(&e, tail.as_deref())
        }
    }
}

/// `POST /v1/apps/:uuid/stop` — shut the runner down (it exits, KEEPING its
/// on-disk artifacts + docker image for a fast restart) + forget its record.
#[utoipa::path(
    post,
    path = "/v1/apps/{uuid}/stop",
    params(("uuid" = String, Path, description = "App UUID v7")),
    responses(
        (status = 200, description = "Runner shut down + record forgotten", body = AppStopResponse),
        (status = 400, description = "Malformed uuid", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(uuid = %uuid))]
pub async fn stop_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.stop_app(&uuid).await {
        Ok(()) => axum::Json(json!({ "state": "stopped" })).into_response(),
        Err(e) => anyhow_to_response(&e),
    }
}

/// `POST /v1/apps/:uuid/purge` — full teardown: purge the runner (it clears its
/// cache + removes its docker image) then shut it down, forget its record, and
/// reclaim the on-disk cache. The disk-reclaiming counterpart to `stop`.
#[utoipa::path(
    post,
    path = "/v1/apps/{uuid}/purge",
    params(("uuid" = String, Path, description = "App UUID v7")),
    responses(
        (status = 200, description = "Runner purged + cache reclaimed", body = AppPurgeResponse),
        (status = 400, description = "Malformed uuid", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(uuid = %uuid))]
pub async fn purge_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
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
#[utoipa::path(
    post,
    path = "/v1/apps/{uuid}/reset",
    params(("uuid" = String, Path, description = "App UUID v7")),
    responses(
        (status = 200, description = "Crash-loop / backoff cleared + reconcile triggered", body = AppActionResponse),
        (status = 404, description = "No runner record exists for this uuid", body = ErrorResponse),
        (status = 400, description = "Malformed uuid", body = ErrorResponse),
        (status = 500, description = "Reconcile failure", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(uuid = %uuid))]
pub async fn reset_app(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.reset_app(&uuid).await {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => anyhow_to_not_found_or_error(&e),
    }
}

/// Request body for `POST /v1/apps/:uuid/deploy`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct DeployBody {
    /// OCI image ref to deploy (e.g. `[fd5a::1]:5000/acme/app:sha256abc`).
    ///
    /// Renamed from `reff` because `ref` is a Rust keyword.
    #[serde(rename = "ref")]
    #[schema(
        example = "[fd5a:1f00:0:3::1]:5000/tabbify/0191e7c2-0000-7000-8000-000000000001:sha256abc"
    )]
    pub(super) reff: String,
    /// Runtime override (D4 wire string); `None` ⇒ manifest default (D10).
    /// Travels in the body only, never persisted to the manifest.
    #[serde(default)]
    pub(super) runtime: Option<crate::runtime::Runtime>,
    /// Tenant network slug (Phase-2 contract). When `Some`, the spawned runner
    /// joins the mesh scoped to this network — it is passed to the runner as
    /// `--network <slug>` so the coordinator stamps it `network=<slug>`,
    /// `tags=["tag:net-<slug>"]`. `None` (the default) keeps today's behavior
    /// (the runner joins unscoped).
    #[serde(default)]
    pub(super) network: Option<String>,
    /// Scoped node-join JWT the node minted for THIS app's runner (Phase-2
    /// contract). Carries `network=<slug>`, `tags=["tag:net-<slug>"]`,
    /// `subject=<app-uuid>`. Threaded to the runner as the
    /// `TABBIFY_RUNNER_JOIN_TOKEN` env so a validating coordinator authenticates
    /// the runner's register. `None` keeps the current tokenless behavior.
    #[serde(default)]
    pub(super) runner_join_token: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (raw TOML) for a connect-repo deploy.
    /// On a cold spawn its `[runtime]`/`[routes]` drive the synthesized manifest
    /// for the BUILD-pipeline app (no S3 manifest). `None` keeps the hardcoded
    /// FC defaults. Travels in the body only, never persisted.
    #[serde(default)]
    pub(super) manifest_toml: Option<String>,
    /// Extra `KEY=VALUE` environment variables baked into the guest `/init` at
    /// deploy time. Appended AFTER the OCI image's own `config.Env` so deploy-time
    /// entries win on key collision (last `export` wins in a POSIX shell).
    ///
    /// Typical uses: `TABBIFY_DEVBOX_AUTHORIZED_KEY` for devbox SSH access,
    /// `TABBIFY_GIT_REMOTE`/`TABBIFY_GIT_BRANCH` for dev-session git seeding.
    ///
    /// Persisted on the runner record so a respawn after a crash re-bakes the
    /// same env (the guest is rebuilt from the same image+env). Omitted on normal
    /// (non-devbox, non-dev-session) deploys.
    #[serde(default)]
    pub(super) env: Option<std::collections::HashMap<String, String>>,
    /// Egress allow-list (Track 7 network ACL): the hosts/CIDRs/IPs the spawned
    /// FC may reach outbound. When `Some(non-empty)`, the runner installs
    /// host-side egress-filter iptables rules (deny-by-default + these hosts,
    /// plus the always-allowed mesh uplink + git-proxy). `None`/empty ⇒ today's
    /// unrestricted egress. Persisted on the runner record so a crash-respawn
    /// re-applies the same posture. A pre-ACL supervisor ignores the key.
    #[serde(default)]
    pub(super) egress_allow: Option<Vec<String>>,
}

/// Request body for `POST /v1/apps/:uuid/start`. All fields optional so a
/// bodyless start (the historical behaviour) still works.
#[derive(Debug, Default, Deserialize, ToSchema)]
pub struct StartBody {
    /// Runtime override (D4 wire string); `None` ⇒ manifest default (D10).
    #[serde(default)]
    pub(super) runtime: Option<crate::runtime::Runtime>,
}

/// `POST /v1/apps/:uuid/deploy` — zero-downtime swap if a runner is live, or
/// cold spawn pinned to `ref` if not. Persists the deployed ref so a future
/// supervisor restart respawns the runner on the same version.
///
/// Returns the app's current status JSON (same shape as `start` / `reset`).
/// Returns `404` when no runner record exists AND the cold spawn fails because
/// the uuid is unknown; otherwise mirrors `reset_app` error semantics.
#[utoipa::path(
    post,
    path = "/v1/apps/{uuid}/deploy",
    params(("uuid" = String, Path, description = "App UUID v7")),
    request_body(
        content = DeployBody,
        description = "New image ref to deploy",
        content_type = "application/json",
        example = json!({"ref": "[fd5a:1f00:0:3::1]:5000/tabbify/0191e7c2-0000-7000-8000-000000000001:sha256abc"})
    ),
    responses(
        (
            status = 200,
            description = "Deploy applied (zero-downtime swap or cold spawn)",
            body = AppActionResponse,
            example = json!({
                "state": "running",
                "app_ula": "fd5a:1f02:abcdef::1",
                "bound_addr": "fd5a:1f02:abcdef::1"
            })
        ),
        (status = 400, description = "Malformed uuid or body", body = ErrorResponse),
        (status = 404, description = "No runner record + uuid is unknown", body = ErrorResponse),
        (status = 500, description = "Control-socket or spawn failure", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(uuid = %uuid))]
pub async fn deploy_app(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    Json(body): Json<DeployBody>,
) -> Response {
    // The optional `runtime` override (D4 wire string) is forwarded through the
    // orchestrator and into `Cmd::Deploy.runtime`. `None` keeps the
    // manifest-default behaviour (D10).
    let runtime_override = body.runtime.map(|r| r.as_wire().to_owned());
    // Phase-2 binding: the network slug + node-minted scoped runner token. On a
    // COLD spawn both are applied to the new runner AND persisted on its
    // record; on a live zero-downtime swap the running runner keeps its mesh
    // identity, but `Some` values are still persisted onto the record
    // (Some-replaces/None-keeps) so a future crash-respawn re-joins with a
    // valid token instead of 401ing. `None` keeps the previously-persisted
    // value; explicit clearing = purge + fresh deploy.
    let net = crate::orchestrator::api::DeployNetwork {
        network: body.network,
        runner_join_token: body.runner_join_token,
    };
    match state
        .orchestrator
        .deploy_app(
            &uuid,
            &body.reff,
            runtime_override.as_deref(),
            body.manifest_toml.as_deref(),
            net,
            body.env.as_ref(),
            body.egress_allow.as_deref(),
        )
        .await
    {
        Ok(s) => running_json(&s).into_response(),
        Err(e) => {
            let tail = state.orchestrator.runner_log_tail(&uuid, 20).await;
            // Cold-spawn crash-loop verdict (option B): a DISTINCT 503 + a
            // machine-readable `restart_status:"crashloop"` body marker so the
            // node classifies it as AppCrashLoop and flips its async deploy_status
            // off eternal "pending" (the node treats any 2xx as success, so the
            // verdict MUST be non-2xx). All other errors keep today's 404/500
            // mapping. The monitor still self-heals the runner in the background.
            // A deploy refused because its manifest would strip the app's
            // persistent data disk is the CALLER's config, not a platform
            // failure: 409 (conflicts with the app's durable state) so the node
            // reports a fixable config error instead of a platform incident.
            if let Some(reg) =
                e.downcast_ref::<crate::orchestrator::manifest_retention::StatefulRegression>()
            {
                return (
                    StatusCode::CONFLICT,
                    axum::Json(json!({
                        "error": reg.to_string(),
                        "user_fault": true,
                        "previous_data_mount": reg.previous_mount,
                    })),
                )
                    .into_response();
            }
            if let Some(cs) = e.downcast_ref::<crate::orchestrator::api::ColdStartUnhealthy>() {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(json!({
                        "error": cs.to_string(),
                        "restart_status": "crashloop",
                        "runner_log_tail": tail,
                    })),
                )
                    .into_response();
            }
            anyhow_to_not_found_or_error_with_tail(&e, tail.as_deref())
        }
    }
}

/// Request body for `POST /v1/build`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct BuildBody {
    /// HTTPS URL of the Git repository to clone.
    #[schema(example = "https://github.com/acme/hello-tabbify")]
    pub(super) repo_url: String,
    /// Git ref (branch, tag, or full SHA) to check out.
    ///
    /// Serialized as `"ref"` (a JSON key) because `ref` is a Rust keyword.
    #[serde(rename = "ref")]
    #[schema(example = "deadbeefcafe1234567890abcdef1234567890ab")]
    pub(super) git_ref: String,
    /// Tenant namespace used as the registry path prefix.
    #[schema(example = "acme")]
    pub(super) tenant: String,
    /// UUID of the app; used in the image tag as `<tenant>/<app_uuid>:<git_ref>`.
    #[schema(example = "0191e7c2-0000-7000-8000-000000000001")]
    pub(super) app_uuid: String,
    /// Mesh ULA + port of the registry to push to.
    #[schema(example = "[fd5a:1f00:0:3::1]:5000")]
    pub(super) registry_ula: String,
    /// Short-lived clone token (`None` = public repo).
    #[serde(default)]
    pub(super) clone_token: Option<String>,
    /// Token for pushing to the registry (`None` = anonymous).
    #[serde(default)]
    pub(super) push_token: Option<String>,
    /// Which build pipeline to run. Absent ⇒ [`crate::runner::build::BuildKind::Docker`]
    /// (the only kind today).
    #[serde(default)]
    pub(super) build_kind: crate::runner::build::BuildKind,
    /// Inert wire field (formerly the wasm `build_cmd`); no build path consumes it.
    #[serde(default)]
    pub(super) build_cmd: Option<String>,
    /// Inert wire field (formerly the wasm `artifact_path`); no build path consumes it.
    #[serde(default)]
    pub(super) artifact_path: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (raw TOML) for a connect-repo deploy.
    /// Injected into the clone ONLY when the repo ships none (repo-wins), then
    /// parsed to drive `[build]`/`[runtime]`/`[routes]`. `None` = no managed config.
    #[serde(default)]
    pub(super) manifest_toml: Option<String>,
}

/// `POST /v1/build` — dispatch a one-shot build: clone `repo_url`@`ref`, build
/// an OCI image, push it to `registry_ula`, and return the
/// [`crate::runner::build::ArtifactRef`] as JSON.
///
/// The full multi-target control-plane (build-then-deploy across a fleet) is
/// Phase 4; this is the minimal invoker.
#[utoipa::path(
    post,
    path = "/v1/build",
    request_body(
        content = BuildBody,
        description = "Build job: clone source, build artifact, push to mesh registry",
        content_type = "application/json",
        example = json!({
            "repo_url": "https://github.com/acme/hello-tabbify",
            "ref": "deadbeefcafe1234567890abcdef1234567890ab",
            "tenant": "acme",
            "app_uuid": "0191e7c2-0000-7000-8000-000000000001",
            "registry_ula": "[fd5a:1f00:0:3::1]:5000"
        })
    ),
    responses(
        (
            status = 200,
            description = "Build succeeded; pushed image ref returned",
            body = crate::runner::build::ArtifactRef,
            example = json!({
                "reff": "[fd5a:1f00:0:3::1]:5000/tabbify/0191e7c2-0000-7000-8000-000000000001:deadbeefcafe1234567890abcdef1234567890ab",
                "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            })
        ),
        (status = 400, description = "Malformed body", body = ErrorResponse),
        (status = 500, description = "Build pipeline failure (clone / build / push)", body = ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(app_uuid = %body.app_uuid))]
pub async fn build_app(State(state): State<SharedState>, Json(body): Json<BuildBody>) -> Response {
    use crate::runner::build::BuildJob;
    let job = BuildJob {
        repo_url: body.repo_url,
        git_ref: body.git_ref,
        // OCI repo names must be lowercase; normalize the GitHub owner at the
        // boundary so the stored/echoed tenant is consistent (the push + pull
        // ref builders also lowercase defensively).
        tenant: body.tenant.to_lowercase(),
        app_uuid: body.app_uuid,
        registry_ula: body.registry_ula,
        clone_token: body.clone_token,
        push_token: body.push_token,
        // build_kind defaults to docker (the only pipeline); build_cmd /
        // artifact_path are inert wire fields preserved for spec compatibility.
        build_kind: body.build_kind,
        build_cmd: body.build_cmd,
        artifact_path: body.artifact_path,
        // Managed `tabbify.toml`: injected into the clone if the repo ships none
        // (repo-wins), then parsed to drive build + runtime in `run_build`.
        manifest_toml: body.manifest_toml,
    };
    match state.orchestrator.spawn_build(&job).await {
        Ok(art) => axum::Json(art).into_response(),
        Err(e) => {
            // Failing-STAGE attribution (deploy observability): a staged build
            // failure renders the structured body the node's classifier reads —
            // {error, stage, user_fault, log_tail} — so the deploy owner learns
            // WHICH step failed and (for user-fault failures) WHY, instead of a
            // bare "build failed". 422 for a user-fault failure (their repo /
            // config), 500 for a platform fault; a legacy node treats both as
            // "ran and failed" (any non-2xx), so the split is wire-compatible.
            if let Some(staged) =
                e.downcast_ref::<crate::runner::build::stage::StagedBuildError>()
            {
                let status = if staged.user_fault {
                    StatusCode::UNPROCESSABLE_ENTITY
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                tracing::warn!(
                    stage = %staged.stage,
                    user_fault = staged.user_fault,
                    status = status.as_u16(),
                    "build_app: staged build failure → structured error body"
                );
                return (
                    status,
                    axum::Json(json!({
                        "error": staged.to_string(),
                        "stage": staged.stage,
                        "user_fault": staged.user_fault,
                        "log_tail": staged.log_tail,
                    })),
                )
                    .into_response();
            }
            anyhow_to_response(&e)
        }
    }
}

/// `GET /v1/build/{uuid}/progress` — current build progress (P1-3).
///
/// Returns the derived build `stage` (`starting`/`pulling`/`building`/
/// `converting`/`booting`) + a tail of the LIVE build log + the log's byte
/// length, so the node can poll
/// this WHILE its (blocking) `POST /v1/build` request is in flight and surface
/// forward progress to the agent (distinguishing a slow build from a hung one).
/// 404 when no build log exists yet for `uuid`.
///
/// Not in OpenAPI: a mesh-internal node↔supervisor polling endpoint (like the
/// git smart-HTTP proxy), so it is registered as a plain route.
pub async fn build_progress(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    match state.orchestrator.build_progress(&uuid, 40).await {
        Some(p) => (StatusCode::OK, axum::Json(p)).into_response(),
        None => error_json(
            StatusCode::NOT_FOUND,
            &format!("no build in progress for {uuid}"),
        ),
    }
}

// ── Response helpers ─────────────────────────────────────────────────────────

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
        "last_exit_at": s.last_exit_at,
    })
}

/// JSON for `POST /v1/apps/:uuid/start`.
fn running_json(s: &AppSummary) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "state": s.state.as_str(),
        "app_ula": s.app_ula,
        // The runner serves on its OWN ULA, so the bound address is the app-ULA.
        "bound_addr": s.app_ula,
        // Echo the requested override back (omitted by serde_json when null).
        "requested_runtime": s.requested_runtime,
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
        "last_exit_at": s.last_exit_at,
    }))
}

/// JSON for `GET /v1/apps/:uuid` when there is no runner but the artifact is
/// fetchable from S3 (discovery). State is `stopped` (no live runner); the
/// app-ULA is still deterministic from the uuid.
fn present_discovered(state: &super::SupervisorState, uuid: &str) -> Response {
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
    anyhow_to_response_with_tail(e, None)
}

/// Like [`anyhow_to_response`] but appends `runner_log_tail` to the JSON body
/// on 500 responses when `tail` is `Some`. Preserves the exact [`FetchError`]
/// downcast behaviour: a `FetchError` still returns its own status code WITHOUT
/// the tail (fetch errors are not spawn errors; their log is irrelevant).
pub(super) fn anyhow_to_response_with_tail(e: &anyhow::Error, tail: Option<&str>) -> Response {
    if let Some(fe) = e.downcast_ref::<FetchError>() {
        return fetch_error_response(fe);
    }
    let msg = e.to_string();
    match tail {
        Some(t) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({ "error": msg, "runner_log_tail": t })),
        )
            .into_response(),
        None => error_json(StatusCode::INTERNAL_SERVER_ERROR, &msg),
    }
}

/// Like [`anyhow_to_response`] but also maps "no runner record found" messages
/// to 404. Used by [`reset_app`] so a reset of an unknown uuid returns 404
/// rather than 500.
pub(super) fn anyhow_to_not_found_or_error(e: &anyhow::Error) -> Response {
    anyhow_to_not_found_or_error_with_tail(e, None)
}

/// Like [`anyhow_to_not_found_or_error`] but appends `runner_log_tail` to 500
/// bodies when `tail` is `Some`. The 404 contract is unchanged: a "no runner
/// record found" error returns 404 WITHOUT a tail (there is no runner whose
/// log could explain anything).
pub(super) fn anyhow_to_not_found_or_error_with_tail(
    e: &anyhow::Error,
    tail: Option<&str>,
) -> Response {
    let msg = e.to_string();
    if msg.contains("no runner record found") {
        return (StatusCode::NOT_FOUND, axum::Json(json!({ "error": msg }))).into_response();
    }
    anyhow_to_response_with_tail(e, tail)
}

fn error_json(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::runtime::Runtime;

    /// `AboutResponse` serializes the self-identification fields the self-update
    /// control plane reads: version / peer_id / mesh_status / uptime_secs.
    #[test]
    fn about_response_serializes_all_fields() {
        let resp = AboutResponse {
            version: "1.4.0".to_owned(),
            peer_id: "0191e7c2-1111-7222-8333-444455556666".to_owned(),
            mesh_status: "joined".to_owned(),
            uptime_secs: 42,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"version\":\"1.4.0\""), "got: {json}");
        assert!(json.contains("\"peer_id\":"), "got: {json}");
        assert!(json.contains("\"mesh_status\":\"joined\""), "got: {json}");
        assert!(json.contains("\"uptime_secs\":42"), "got: {json}");
    }

    #[test]
    fn start_body_parses_explicit_runtime() {
        // Single-runtime model: a legacy `"docker"` runtime still parses and
        // coerces to the only runtime (Firecracker).
        let b: StartBody = serde_json::from_str(r#"{"runtime":"docker"}"#).unwrap();
        assert_eq!(b.runtime, Some(Runtime::Firecracker));
    }

    #[test]
    fn start_body_defaults_runtime_to_none_on_empty_object() {
        let b: StartBody = serde_json::from_str("{}").unwrap();
        assert!(b.runtime.is_none());
    }

    #[test]
    fn deploy_body_parses_runtime_override() {
        // Single-runtime model: a legacy `"docker"` runtime override still parses
        // and coerces to the only runtime (Firecracker).
        let b: DeployBody =
            serde_json::from_str(r#"{"ref":"reg/acme/app:sha","runtime":"docker"}"#).unwrap();
        assert_eq!(b.runtime, Some(Runtime::Firecracker));
    }

    #[test]
    fn deploy_body_runtime_defaults_to_none() {
        let b: DeployBody = serde_json::from_str(r#"{"ref":"reg/acme/app:sha"}"#).unwrap();
        assert!(b.runtime.is_none());
    }
}
