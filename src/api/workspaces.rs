//! Per-USER workspace lifecycle: one always-on Firecracker workspace VM per user
//! on a STABLE `workspace_uuid`, holding N repos (each with its own git-proxy
//! cap) under `~/projects`. The evolution of dev-sessions (spec §3 re-key
//! #1–#4): stable identity, multi-cap register-before-spawn, persistent (~∞ TTL).
//!
//! ## Snapshot timing (§12) + cap channel (§12 S1)
//! The workspace marker (`TABBIFY_WORKSPACE_UUID` in `extra_env`) makes the
//! RUNNER process (Task 9 `run_firecracker_build`) SUPPRESS the cold-boot
//! snapshot — `cold_boot`'s readiness probe answers BEFORE rust-analyzer is
//! indexed, so a boot snapshot would freeze a COLD index. The warm snapshot is
//! taken ONLY by `Cmd::Snapshot` after the code-service signals `indexed && idle`.
//! Per-repo cap-URLs ride in a SINGLE reserved `extra_env` key,
//! [`CAP_FILES_ENV`] (a JSON map), which Task 9's runner-process writer EXTRACTS
//! and REMOVES before baking env — writing each to `/run/tabbify/caps/<repo>.url`
//! (0600, broker-uid) — so the cap content is NEVER `export`ed into agent env nor
//! frozen into a snapshot (§4/§12 S1).
//!
//! ## Endpoints
//! - `POST   /v1/workspaces`      — ensure the per-user workspace (idempotent on
//!   `workspace_uuid`): register N caps + spawn the VM (async).
//! - `GET    /v1/workspaces`      — list (ops).
//! - `DELETE /v1/workspaces/:uuid` — purge the VM + revoke every cap (alarmed).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tabbify_workspace_contract::workspace_uuid;
use utoipa::ToSchema;

use crate::api::dev_sessions::{derive_dev_fc_host_ip, generate_cap};
use crate::api::{
    GIT_PROXY_IPV4_PORT, GitSessionEntry, SharedState, WorkspaceCap, WorkspaceRecord, now_unix,
};
use crate::orchestrator::api::DeployNetwork;

/// Hard TTL ceiling for a workspace. dev-sessions reclaim at 7d; a WORKSPACE is
/// permanent (spec §3 re-key #3: "поднимаем MAX_TTL 7d → ~∞"). Set far beyond any
/// real lifetime so the safety reaper never fires for a workspace — a workspace
/// lives until an explicit (alarmed) DELETE.
pub const WORKSPACE_MAX_TTL: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60); // ~100y ≈ ∞

/// The SINGLE reserved `extra_env` key whose value is a JSON map
/// `{ "<repo>.url": "<cap-url>", "forge-admin.token": "<token>" }` of files the
/// runner-process cap-file writer (Task 9 `run_firecracker_build`) must write to
/// `/run/tabbify/caps/<file>` (0600, broker-uid) and then REMOVE from the env so
/// the value is NEVER `export`ed into agent env nor frozen into a Full snapshot
/// (§4 / §12 S1). This is the ONLY env carrier for cap-URLs; the broker reads the
/// files, the agent never sees them. Kept here (the producer) and read in Task 9
/// (the consumer) — one constant, one source.
pub const CAP_FILES_ENV: &str = "TABBIFY_CAP_FILES";

/// Sanitized last path segment of a clone URL, used as the cap-file STEM under
/// `/run/tabbify/caps/<stem>.url`. Keeps only `[A-Za-z0-9._-]` (others → `_`),
/// strips a trailing `.git`, and never returns empty (`"repo"` fallback) — so the
/// derived file name can never traverse out of the caps dir or collide with the
/// reserved `forge-admin.token`.
#[must_use]
pub fn cap_repo_basename(repo_url: &str) -> String {
    let last = repo_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("repo");
    let last = last.strip_suffix(".git").unwrap_or(last);
    let cleaned: String = last
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' })
        .collect();
    if cleaned.is_empty() { "repo".to_owned() } else { cleaned }
}

/// One repo inside the workspace request: clone URL (no creds) + branch +
/// short-lived provider token the git proxy injects.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RepoSpec {
    /// Provider clone URL WITHOUT credentials.
    #[schema(example = "https://github.com/acme/app.git")]
    pub repo_url: String,
    /// Branch to make available.
    #[schema(example = "main")]
    pub branch: String,
    /// Short-lived provider token (proxy injects it; the VM never sees it).
    pub git_token: String,
    /// Token TTL in seconds.
    #[schema(example = "3600")]
    pub git_token_ttl_secs: u64,
}

/// `POST /v1/workspaces` request body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateWorkspaceBody {
    /// Canonical internal account id. `workspace_uuid` is derived from THIS
    /// (frozen contract) so the same user always re-keys to the same workspace.
    #[schema(example = "acct_01H...")]
    pub user_id: String,
    /// OCI image ref of the FIXED workspace image (never a repo-as-rootfs).
    #[schema(example = "[fd5a::1]:5000/tabbify/workspace:latest")]
    pub image_ref: String,
    /// Repos to make available under `~/projects` (each gets its own cap).
    pub repos: Vec<RepoSpec>,
    /// SSH public key to authorize inside the workspace (`authorized_keys`).
    /// NO `#[serde(default)]` — T2 (the only caller) MUST send it (§12 S5); a
    /// request omitting it 400s on deserialize.
    pub authorized_key: String,
    /// Optional forge-admin token (§12 S1/S2): written to
    /// `/run/tabbify/caps/forge-admin.token` (0600, broker-uid) inside the FC so
    /// T1's broker can mediate forge ops. `None` when the workspace has no
    /// in-mesh forge org yet (the common MVP case). NEVER in agent env.
    #[serde(default)]
    pub forge_admin_token: Option<String>,
    /// Tenant network slug (optional).
    #[serde(default)]
    pub network: Option<String>,
    /// Scoped node-minted runner-join token (optional).
    #[serde(default)]
    pub runner_join_token: Option<String>,
}

/// `POST /v1/workspaces` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct WorkspaceCreated {
    /// Stable per-user workspace uuid (== the FC app uuid).
    pub workspace_uuid: String,
    /// Tokenless git remote per repo (index-parallel to the request `repos`).
    pub git_remotes: Vec<String>,
}

/// One live workspace in the in-memory registry.
pub struct Workspace {
    /// Stable per-user workspace uuid (string form).
    pub workspace_uuid: String,
    /// Canonical account id.
    pub user_id: String,
    /// All git-proxy caps (one per repo).
    pub caps: Vec<String>,
    /// When the workspace was created (monotonic; drives `created_age_secs`).
    pub created_at: Instant,
    /// Last activity (monotonic; drives `idle_secs`). Reserved for a future
    /// idle-tracking touch; a workspace is never idle-reaped (MAX_TTL ≈ ∞).
    pub last_activity: Instant,
}

/// `workspace_uuid` → `Workspace`.
#[derive(Default)]
pub struct WorkspaceRegistry(Mutex<HashMap<String, Workspace>>);

impl WorkspaceRegistry {
    /// Insert / overwrite the workspace for its uuid.
    pub fn insert(&self, ws: Workspace) {
        self.0
            .lock()
            .expect("workspace lock")
            .insert(ws.workspace_uuid.clone(), ws);
    }

    /// Remove by uuid; returns the removed workspace (or `None`).
    pub fn remove(&self, workspace_uuid: &str) -> Option<Workspace> {
        self.0.lock().expect("workspace lock").remove(workspace_uuid)
    }

    /// Caps for a workspace (cheap clone), or `None` if absent.
    pub fn caps_of(&self, workspace_uuid: &str) -> Option<Vec<String>> {
        self.0
            .lock()
            .expect("workspace lock")
            .get(workspace_uuid)
            .map(|w| w.caps.clone())
    }

    /// `(workspace_uuid, user_id, cap_count, created_at, last_activity)` per row.
    #[allow(clippy::type_complexity)]
    pub fn snapshot(&self) -> Vec<(String, String, usize, Instant, Instant)> {
        self.0
            .lock()
            .expect("workspace lock")
            .values()
            .map(|w| {
                (
                    w.workspace_uuid.clone(),
                    w.user_id.clone(),
                    w.caps.len(),
                    w.created_at,
                    w.last_activity,
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.lock().expect("workspace lock").len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.0.lock().expect("workspace lock").is_empty()
    }
}

/// `POST /v1/workspaces` — ensure the per-user workspace. Re-keys to the stable
/// `workspace_uuid`, registers N git-proxy caps BEFORE spawning, persists a
/// durable record, and provisions the VM in the BACKGROUND (202). Sets the
/// `TABBIFY_WORKSPACE_UUID` marker so the RUNNER process (Task 9) SUPPRESSES the
/// cold-boot snapshot (the readiness probe answers PRE-INDEX) — the warm snapshot
/// is taken only by `Cmd::Snapshot` post-index. Per-repo cap-URLs ride in the
/// single `CAP_FILES_ENV` key (§12 S1), written to 0600 files + removed from env
/// by the runner — never `export`ed, never snapshot-frozen.
#[utoipa::path(
    post,
    path = "/v1/workspaces",
    request_body(content = CreateWorkspaceBody, content_type = "application/json"),
    responses(
        (status = 202, description = "Workspace accepted; VM provisions asynchronously (async deploy errors revoke caps + drop the workspace; the env-safety guard rejects a forbidden-key spawn in the runner)", body = WorkspaceCreated),
    ),
)]
#[tracing::instrument(skip(state, body), fields(user_id = %body.user_id))]
pub async fn create_workspace(
    State(state): State<SharedState>,
    Json(body): Json<CreateWorkspaceBody>,
) -> Response {
    // RE-KEY #1: stable identity. The FC app uuid IS the workspace uuid, derived
    // purely from user_id (frozen contract) — same user → same VM/ULA/snapshot.
    let ws_uuid = workspace_uuid(&body.user_id).to_string();

    // RE-KEY #2: multi-repo. One git-proxy cap PER repo, registered in the
    // SHARED GitSessions HashMap BEFORE the spawn so the VM can reach every
    // remote from first boot. N caps are free (it is a HashMap).
    let host_ip = derive_dev_fc_host_ip(&ws_uuid, &body.image_ref, &state.tap_subnet);
    let mut caps: Vec<String> = Vec::with_capacity(body.repos.len());
    let mut git_remotes: Vec<String> = Vec::with_capacity(body.repos.len());
    let mut record_caps: Vec<WorkspaceCap> = Vec::with_capacity(body.repos.len());
    let mut branches: Vec<String> = Vec::with_capacity(body.repos.len());
    // §12 S1 cap-file payloads: filename → file content. Written by the runner to
    // `/run/tabbify/caps/<filename>` (0600, broker-uid) and REMOVED from env.
    let mut cap_files: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for repo in &body.repos {
        let cap = generate_cap(&ws_uuid, &repo.repo_url);
        let git_remote = format!("http://{host_ip}:{GIT_PROXY_IPV4_PORT}/git/{cap}");
        let expires_at = Instant::now() + Duration::from_secs(repo.git_token_ttl_secs);
        state.git_sessions.register(
            cap.clone(),
            GitSessionEntry {
                upstream_url: repo.repo_url.clone(),
                token: repo.git_token.clone(),
                expires_at,
            },
        );
        // The broker reads `/run/tabbify/caps/<repo>.url` (off-env). The file name
        // is derived from the repo's last path segment (sanitized); the value is
        // the TOKENLESS git-proxy remote (no secret — the token stays host-side in
        // GitSessions and is injected by the proxy, never in the VM).
        let repo_file = format!("{}.url", cap_repo_basename(&repo.repo_url));
        cap_files.insert(repo_file, serde_json::Value::String(git_remote.clone()));
        record_caps.push(WorkspaceCap {
            cap: cap.clone(),
            repo_url: repo.repo_url.clone(),
        });
        branches.push(repo.branch.clone());
        git_remotes.push(git_remote);
        caps.push(cap);
    }
    // The forge-admin token (when present, §12 S1/S2) is a credential — it rides
    // the SAME off-env cap-file channel, never an env var.
    if let Some(tok) = &body.forge_admin_token {
        cap_files.insert(
            "forge-admin.token".to_owned(),
            serde_json::Value::String(tok.clone()),
        );
    }

    // RE-KEY #3 (persistence): the workspace marker env + a durable record. The
    // marker (a) makes a respawn re-bake the same identity, (b) drives the runner
    // to SUPPRESS the cold-boot snapshot (Task 9), and (c) lets readopt re-register
    // all caps after a supervisor restart. The cap-file payloads ride the single
    // reserved `CAP_FILES_ENV` key (a JSON map) — the runner WRITES + REMOVES it
    // (Task 9) so no cap content is ever `export`ed nor snapshot-frozen.
    //
    // SECURITY (spec §4 / §12 snapshot-timing): the env-safety guard runs in the
    // RUNNER process (Task 9 `run_firecracker_build`), where `RUNNER_EXTRA_ENV` is
    // actually re-baked into the rootfs `/init` — that is the only place a leak
    // can be frozen. It is NOT asserted here (the API process never re-bakes env).
    let mut extra_env: HashMap<String, String> = HashMap::new();
    extra_env.insert(crate::api::WORKSPACE_MARKER_ENV.to_owned(), ws_uuid.clone());
    extra_env.insert("TABBIFY_USER_ID".to_owned(), body.user_id.clone());
    extra_env.insert(
        "TABBIFY_DEVBOX_AUTHORIZED_KEY".to_owned(),
        body.authorized_key.clone(),
    );
    if !cap_files.is_empty() {
        // serde_json::to_string of a Map never fails (all values are strings).
        extra_env.insert(
            CAP_FILES_ENV.to_owned(),
            serde_json::Value::Object(cap_files).to_string(),
        );
    }

    let now = Instant::now();
    state.workspaces.insert(Workspace {
        workspace_uuid: ws_uuid.clone(),
        user_id: body.user_id.clone(),
        caps: caps.clone(),
        created_at: now,
        last_activity: now,
    });

    let now_u = now_unix();
    let record = WorkspaceRecord {
        workspace_uuid: ws_uuid.clone(),
        user_id: body.user_id.clone(),
        caps: record_caps,
        branches,
        created_at_unix: now_u,
        last_activity_unix: now_u,
    };
    if let Err(e) = record.save(state.orchestrator.runner_dir()) {
        tracing::warn!(workspace_uuid = %ws_uuid, error = %e, "failed to persist workspace record (live in-memory only)");
    }

    let net = DeployNetwork {
        network: body.network.clone(),
        runner_join_token: body.runner_join_token.clone(),
    };

    // ASYNC spawn (202): register-before-spawn already done; the VM provisions in
    // the background (a cold image pull can take minutes). On failure, revoke
    // every cap + drop the workspace + remove the record.
    let bg = state.clone();
    let app_uuid = ws_uuid.clone();
    let image_ref = body.image_ref.clone();
    let caps_for_bg = caps.clone();
    tokio::spawn(async move {
        match bg
            .orchestrator
            .deploy_app(&app_uuid, &image_ref, None, None, net, Some(&extra_env))
            .await
        {
            Ok(_) => tracing::info!(workspace_uuid = %app_uuid, "workspace provisioned (async)"),
            Err(e) => {
                tracing::warn!(workspace_uuid = %app_uuid, error = %e, "workspace deploy failed (async); revoking caps + dropping workspace");
                for c in &caps_for_bg {
                    bg.git_sessions.revoke(c);
                }
                bg.workspaces.remove(&app_uuid);
                let _ = WorkspaceRecord::remove(bg.orchestrator.runner_dir(), &app_uuid);
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(WorkspaceCreated {
            workspace_uuid: ws_uuid,
            git_remotes,
        }),
    )
        .into_response()
}

/// `GET /v1/workspaces` — list live workspaces (ops).
#[utoipa::path(get, path = "/v1/workspaces", responses((status = 200, description = "List of live workspaces")))]
#[tracing::instrument(skip(state))]
pub async fn list_workspaces(State(state): State<SharedState>) -> Response {
    let now = Instant::now();
    let rows: Vec<_> = state
        .workspaces
        .snapshot()
        .into_iter()
        .map(|(workspace_uuid, user_id, cap_count, created_at, last_activity)| {
            json!({
                "workspace_uuid": workspace_uuid,
                "user_id": user_id,
                "repo_count": cap_count,
                "created_age_secs": now.duration_since(created_at).as_secs(),
                "idle_secs": now.duration_since(last_activity).as_secs(),
            })
        })
        .collect();
    Json(json!({ "workspaces": rows })).into_response()
}

/// `DELETE /v1/workspaces/:uuid` — purge the VM, revoke EVERY cap, drop the
/// record. The alarmed/confirm gate (spec §3 re-key #3) lives node-side; the
/// supervisor performs the teardown when asked.
#[utoipa::path(
    delete,
    path = "/v1/workspaces/{uuid}",
    params(("uuid" = String, Path, description = "Workspace uuid")),
    responses(
        (status = 200, description = "Workspace purged"),
        (status = 404, description = "Workspace not found", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(workspace_uuid = %uuid))]
pub async fn delete_workspace(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
) -> Response {
    let Some(ws) = state.workspaces.remove(&uuid) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    };
    for c in &ws.caps {
        state.git_sessions.revoke(c);
    }
    if let Err(e) = WorkspaceRecord::remove(state.orchestrator.runner_dir(), &uuid) {
        tracing::warn!(workspace_uuid = %uuid, error = %e, "failed to remove workspace record on delete");
    }
    if let Err(e) = state.orchestrator.purge_app(&uuid).await {
        tracing::warn!(workspace_uuid = %uuid, error = %e, "workspace purge_app failed (continuing)");
    }
    Json(json!({ "purged": true })).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The stable workspace_uuid is a pure fn of user_id (frozen contract): same
    /// user → same uuid, distinct users → distinct. This is the re-key keystone.
    #[test]
    fn workspace_uuid_is_stable_per_user() {
        assert_eq!(
            workspace_uuid("acct_A").to_string(),
            workspace_uuid("acct_A").to_string(),
            "same user must re-key to the same workspace"
        );
        assert_ne!(
            workspace_uuid("acct_A").to_string(),
            workspace_uuid("acct_B").to_string(),
        );
    }

    /// The registry stores N caps for one workspace and returns them all.
    #[test]
    fn registry_holds_n_caps() {
        let reg = WorkspaceRegistry::default();
        reg.insert(Workspace {
            workspace_uuid: "ws-1".to_owned(),
            user_id: "u".to_owned(),
            caps: vec!["capA".to_owned(), "capB".to_owned()],
            created_at: Instant::now(),
            last_activity: Instant::now(),
        });
        let caps = reg.caps_of("ws-1").unwrap();
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&"capB".to_owned()));
        // remove returns the workspace; a second remove is None.
        assert!(reg.remove("ws-1").is_some());
        assert!(reg.remove("ws-1").is_none());
        assert_eq!(reg.len(), 0);
    }

    /// WORKSPACE_MAX_TTL is effectively infinite (spec §3 re-key #3): far beyond
    /// the dev-session 7d, so the safety reaper never reclaims a workspace.
    #[test]
    fn workspace_max_ttl_is_effectively_infinite() {
        assert!(
            WORKSPACE_MAX_TTL > Duration::from_secs(7 * 24 * 60 * 60),
            "workspace TTL must exceed the dev-session 7d ceiling"
        );
        assert!(
            WORKSPACE_MAX_TTL >= Duration::from_secs(10 * 365 * 24 * 60 * 60),
            "workspace TTL must be effectively infinite (≥10y)"
        );
    }

    /// The cap-file stem is sanitized + traversal-safe: it strips `.git`, keeps
    /// only safe chars, and never escapes the caps dir or empties (§12 S1).
    #[test]
    fn cap_repo_basename_is_safe_and_strips_git() {
        assert_eq!(cap_repo_basename("https://github.com/acme/app.git"), "app");
        assert_eq!(cap_repo_basename("https://github.com/acme/My-Repo"), "My-Repo");
        // A traversal attempt can never produce a path separator.
        assert!(!cap_repo_basename("../../etc/passwd").contains('/'));
        // Trailing slash + empty segment still yields a usable, non-empty stem.
        assert!(!cap_repo_basename("https://x/").is_empty());
    }
}
