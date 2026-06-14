//! Dev-session lifecycle: always-on dev-FC + tokenless git-proxy capability.
//!
//! A dev session pairs:
//! - An always-on Firecracker VM spawned via `deploy_app` whose guest `/init`
//!   receives `TABBIFY_GIT_REMOTE`, `TABBIFY_GIT_BRANCH`, and
//!   `TABBIFY_DEVBOX_AUTHORIZED_KEY` at boot so it can clone the repo and run
//!   sshd for exec.
//! - A git-proxy capability (64 hex chars) registered in [`crate::api::GitSessions`]
//!   so the in-VM git client can clone over plain HTTP — credentials never enter
//!   the sandbox.
//!
//! ## Endpoints
//! - `POST /v1/dev-sessions` — spawn + register.
//! - `POST /v1/dev-sessions/:id/git-token` — refresh the proxy token.
//! - `DELETE /v1/dev-sessions/:id` — purge the VM + revoke the cap.
//! - `GET /v1/dev-sessions` — list (ops).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::{GIT_PROXY_IPV4_PORT, GitSessionEntry, SharedState};
use crate::orchestrator::api::DeployNetwork;

// ── TTL constants ─────────────────────────────────────────────────────────────

/// Idle timeout. Dev sessions are PERSISTENT + resumable by design: a user who
/// reconnects (e.g. a new MCP connection) must land back in the SAME existing
/// container/project, not a fresh one. So idle NO LONGER reaps — this is set far
/// beyond any real session so the idle branch never fires; a session lives until
/// an explicit `DELETE` or the hard `DEV_SESSION_MAX_TTL` safety ceiling.
/// (The node still refreshes the git-proxy token periodically so `git push`
/// keeps working — that is independent of session lifetime.)
pub const DEV_SESSION_IDLE_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60); // ~never

/// Hard ceiling: a truly-forgotten session (no explicit close) is reclaimed after
/// this regardless of activity — a safety net against leaked VMs. Kept generous
/// so persistence/resume works across long gaps.
pub const DEV_SESSION_MAX_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 7 d

// ── Registry ─────────────────────────────────────────────────────────────────

/// One live dev session: an always-on dev-FC + a git-proxy capability.
pub struct DevSession {
    /// Session identifier (UUID v7, string form).
    pub session_id: String,
    /// The FC app uuid (fresh per session — purged on DELETE/reap).
    pub app_uuid: String,
    /// Git-proxy capability token (64 hex chars, unguessable blake3 hash).
    pub cap: String,
    /// When this session was created.
    pub created_at: Instant,
    /// Last activity timestamp; bumped by `POST .../git-token`. With persistent
    /// sessions this no longer drives reaping (see [`DEV_SESSION_IDLE_TTL`]); it
    /// is still surfaced as `idle_secs` so a client can pick the freshest session.
    pub last_activity: Instant,
    /// The repo this session was created for
    /// (`https://github.com/owner/repo.git`). Surfaced in the list so a client
    /// can find + REUSE the session for a given repo instead of duplicating it.
    pub repo_url: String,
    /// The branch checked out at `/workspace`.
    pub branch: String,
}

/// Dev-session registry: `session_id` → `DevSession`.
#[derive(Default)]
pub struct DevSessionRegistry(Mutex<HashMap<String, DevSession>>);

impl DevSessionRegistry {
    /// Insert a new session. Overwrites any existing entry with the same id.
    pub fn insert(&self, session: DevSession) {
        self.0
            .lock()
            .expect("dev session lock")
            .insert(session.session_id.clone(), session);
    }

    /// Remove a session by id. Returns the removed session (or `None` if absent).
    pub fn remove(&self, session_id: &str) -> Option<DevSession> {
        self.0.lock().expect("dev session lock").remove(session_id)
    }

    /// Look up a session by id without removing it; returns `(app_uuid, cap)`
    /// (cheaper than cloning the whole struct).
    pub fn lookup(&self, session_id: &str) -> Option<(String, String)> {
        let guard = self.0.lock().expect("dev session lock");
        guard
            .get(session_id)
            .map(|s| (s.app_uuid.clone(), s.cap.clone()))
    }

    /// Bump `last_activity` for a session. Returns `false` if not found.
    pub fn bump_activity(&self, session_id: &str) -> bool {
        let mut guard = self.0.lock().expect("dev session lock");
        if let Some(s) = guard.get_mut(session_id) {
            s.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    /// Returns a snapshot of
    /// `(session_id, app_uuid, cap, created_at, last_activity, repo_url, branch)`
    /// for every session. Used by the list endpoint and the max-age reaper.
    #[allow(clippy::type_complexity)]
    pub fn snapshot(&self) -> Vec<(String, String, String, Instant, Instant, String, String)> {
        let guard = self.0.lock().expect("dev session lock");
        guard
            .values()
            .map(|s| {
                (
                    s.session_id.clone(),
                    s.app_uuid.clone(),
                    s.cap.clone(),
                    s.created_at,
                    s.last_activity,
                    s.repo_url.clone(),
                    s.branch.clone(),
                )
            })
            .collect()
    }

    /// Number of live sessions (for tests + ops).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.lock().expect("dev session lock").len()
    }

    /// Returns `true` when no sessions are registered.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.0.lock().expect("dev session lock").is_empty()
    }
}

// ── IPv4 git_remote derivation ────────────────────────────────────────────────

/// Derive the `host_ip` for a dev-FC identified by `app_uuid` + `image_ref`.
///
/// The FC launch uses `vm_key = format!("{uuid}:{reff}")` where `reff` is the
/// OCI image ref (`runtime.registry_ref`). For a dev session, `reff` is the
/// caller-supplied `image_ref` (e.g. `"[fd5a::1]:5000/tabbify/devbox:latest"`).
/// We must hash the SAME key to get the same `/30` link_idx and thus the same
/// `host_ip` as the FC launch.
///
/// The derivation is Linux-only in production (FC requires `/dev/kvm`), but
/// the math is platform-independent. On non-Linux builds we fall back to
/// `"127.0.0.1"` (functionally harmless — non-Linux hosts can't boot FC VMs;
/// tests that need the real value must run on Linux).
pub(crate) fn derive_dev_fc_host_ip(app_uuid: &str, image_ref: &str, tap_subnet: &str) -> String {
    // vm_key matches `launch_with_uuid` cold-start: `format!("{uuid}:{reff}")`.
    let vm_key = format!("{app_uuid}:{image_ref}");
    #[cfg(target_os = "linux")]
    {
        let (_, link_idx) = crate::firecracker::linux::fc_identity_for_key(&vm_key);
        match crate::firecracker::linux::derive_link_ips(tap_subnet, link_idx) {
            Ok((host_ip, _)) => host_ip.to_string(),
            Err(e) => {
                tracing::warn!(
                    app_uuid,
                    error = %e,
                    "dev-session: failed to derive host_ip; falling back to 127.0.0.1"
                );
                "127.0.0.1".to_owned()
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux: FC VMs cannot run; return a sentinel. Dev-session create
        // will succeed on Linux hosts in production.
        let _ = (vm_key, tap_subnet); // suppress unused warnings
        "127.0.0.1".to_owned()
    }
}

// ── Capability generation ─────────────────────────────────────────────────────

/// Generate a 64-hex-char capability token that is unguessable.
///
/// `blake3::hash` over `(session_id, app_uuid, salt_a, salt_b)` where each salt
/// is a `Uuid::new_v4()` — 122 random bits straight from the OS CSPRNG
/// (getrandom). 244 bits of fresh randomness expanded through blake3 into a
/// 256-bit token; the ids only bind the cap to its session.
pub(crate) fn generate_cap(session_id: &str, app_uuid: &str) -> String {
    let salt_a = Uuid::new_v4().to_string();
    let salt_b = Uuid::new_v4().to_string();
    let input = format!("{session_id}:{app_uuid}:{salt_a}:{salt_b}");
    let hash = blake3::hash(input.as_bytes());
    hex::encode(hash.as_bytes())
}

// ── Request / response DTOs ───────────────────────────────────────────────────

/// `POST /v1/dev-sessions` request body.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateDevSessionBody {
    /// Fresh app UUID to use for the dev-FC (caller-supplied so the node controls
    /// the UUID namespace).
    #[schema(example = "0191e7c2-0000-7000-8000-000000000001")]
    pub app_uuid: String,
    /// OCI image ref to deploy as the dev-FC guest.
    #[schema(example = "[fd5a::1]:5000/tabbify/devbox:latest")]
    pub image_ref: String,
    /// Provider clone URL WITHOUT credentials (the proxy injects the token).
    #[schema(example = "https://github.com/acme/app.git")]
    pub repo_url: String,
    /// Git branch to clone.
    #[schema(example = "main")]
    pub branch: String,
    /// Short-lived provider token (e.g. GitHub App installation token).
    pub git_token: String,
    /// Token TTL in seconds; the proxy rejects requests after expiry.
    #[schema(example = "3600")]
    pub git_token_ttl_secs: u64,
    /// SSH public key to authorize inside the dev-FC (`authorized_keys`).
    #[schema(example = "ssh-ed25519 AAAA...")]
    pub authorized_key: String,
    /// Tenant network slug (optional).
    #[serde(default)]
    pub network: Option<String>,
    /// Scoped node-minted runner-join token (optional).
    #[serde(default)]
    pub runner_join_token: Option<String>,
}

/// `POST /v1/dev-sessions` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct DevSessionCreated {
    /// Opaque session identifier used for refresh + delete calls.
    pub session_id: String,
    /// The FC app uuid spawned for this session.
    pub app_uuid: String,
    /// Tokenless git remote URL — pass this to the in-VM git clone.
    pub git_remote: String,
}

/// `POST /v1/dev-sessions/:id/git-token` request body.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RefreshGitTokenBody {
    /// New provider token to inject into the proxy.
    pub git_token: String,
    /// New token TTL in seconds.
    pub git_token_ttl_secs: u64,
}

/// `POST /v1/dev-sessions/:id/git-token` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct GitTokenRefreshed {
    /// Always `true` on 200.
    pub refreshed: bool,
}

/// `DELETE /v1/dev-sessions/:id` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct DevSessionPurged {
    /// Always `true` on 200.
    pub purged: bool,
}

/// One row in `GET /v1/dev-sessions` response.
#[derive(Debug, Serialize, ToSchema)]
pub struct DevSessionRow {
    /// Session identifier.
    pub session_id: String,
    /// The FC app uuid.
    pub app_uuid: String,
    /// Seconds since session was created.
    pub created_age_secs: u64,
    /// Seconds since the last `POST .../git-token` heartbeat (no longer drives
    /// reaping — see [`DEV_SESSION_IDLE_TTL`]; freshness hint only).
    pub idle_secs: u64,
    /// The repo this session is for (`https://github.com/owner/repo.git`) — lets
    /// a client find + reuse the session for a given repo.
    pub repo_url: String,
    /// The branch checked out at `/workspace`.
    pub branch: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `POST /v1/dev-sessions` — spawn an always-on dev-FC + register a git-proxy
/// capability. Returns `200` with `{ session_id, app_uuid, git_remote }`.
///
/// The deploy is SYNCHRONOUS (mirrors `deploy_app`, which also answers 200 only
/// once the VM is healthy); the node tolerates long mesh-internal calls.
#[utoipa::path(
    post,
    path = "/v1/dev-sessions",
    request_body(
        content = CreateDevSessionBody,
        description = "Dev-session creation: image ref, repo, branch, token, SSH key",
        content_type = "application/json"
    ),
    responses(
        (status = 200, description = "Session created, VM healthy", body = DevSessionCreated),
        (status = 500, description = "Deploy failure", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(app_uuid = %body.app_uuid))]
pub async fn create_dev_session(
    State(state): State<SharedState>,
    Json(body): Json<CreateDevSessionBody>,
) -> Response {
    // TODO(T8): validate non-empty app_uuid/repo_url/git_token/authorized_key
    // before node integration.
    let session_id = Uuid::now_v7().to_string();
    let cap = generate_cap(&session_id, &body.app_uuid);

    // Build the tokenless git remote URL on the IPv4 host_ip the guest sees as
    // its default gateway. The guest is an IPv4-only FC VM on a /30 tap — it
    // has no IPv6 or mesh access, so the old `http://[ula]:8730` was
    // unreachable from inside. The IPv4 git-proxy listener (`GIT_PROXY_IPV4_PORT`)
    // is bound on `0.0.0.0` and reachable via the tap's host_ip.
    //
    // vm_key used by FC launch = `format!("{uuid}:{reff}")` where reff =
    // registry_ref = image_ref — `derive_dev_fc_host_ip` takes both to match it.
    let host_ip = derive_dev_fc_host_ip(&body.app_uuid, &body.image_ref, &state.tap_subnet);
    let git_remote = format!("http://{host_ip}:{GIT_PROXY_IPV4_PORT}/git/{cap}");

    // Register the git proxy capability BEFORE spawning (so the VM can reach it
    // from first boot). Revoked below on deploy failure.
    let expires_at = Instant::now() + Duration::from_secs(body.git_token_ttl_secs);
    state.git_sessions.register(
        cap.clone(),
        GitSessionEntry {
            upstream_url: body.repo_url.clone(),
            token: body.git_token.clone(),
            expires_at,
        },
    );

    // Build the extra env map for the dev-FC guest.
    let mut extra_env: HashMap<String, String> = HashMap::new();
    extra_env.insert("TABBIFY_GIT_REMOTE".to_owned(), git_remote.clone());
    extra_env.insert("TABBIFY_GIT_BRANCH".to_owned(), body.branch.clone());
    extra_env.insert(
        "TABBIFY_DEVBOX_AUTHORIZED_KEY".to_owned(),
        body.authorized_key.clone(),
    );

    let net = DeployNetwork {
        network: body.network,
        runner_join_token: body.runner_join_token,
    };

    // Spawn the dev-FC (synchronous — returns when healthy or fails).
    // On failure: revoke the cap and return the orchestrator error.
    let result = state
        .orchestrator
        .deploy_app(
            &body.app_uuid,
            &body.image_ref,
            None,
            None,
            net,
            Some(&extra_env),
        )
        .await;

    match result {
        Err(e) => {
            // Revoke the cap so the git proxy rejects future requests.
            state.git_sessions.revoke(&cap);
            tracing::warn!(app_uuid = %body.app_uuid, error = %e, "dev-session deploy failed");
            let tail = state.orchestrator.runner_log_tail(&body.app_uuid, 20).await;
            crate::api::handlers::anyhow_to_response_with_tail(&e, tail.as_deref())
        }
        Ok(_summary) => {
            // Register the session ONLY after a successful deploy.
            let now = Instant::now();
            let session = DevSession {
                session_id: session_id.clone(),
                app_uuid: body.app_uuid.clone(),
                cap,
                created_at: now,
                last_activity: now,
                repo_url: body.repo_url,
                branch: body.branch,
            };
            state.dev_sessions.insert(session);

            // 200, not 202: the deploy is synchronous — the VM is healthy here.
            Json(DevSessionCreated {
                session_id,
                app_uuid: body.app_uuid,
                git_remote,
            })
            .into_response()
        }
    }
}

/// `POST /v1/dev-sessions/:id/git-token` — refresh the git-proxy token for an
/// existing session. Bumps `last_activity`.
#[utoipa::path(
    post,
    path = "/v1/dev-sessions/{id}/git-token",
    params(("id" = String, Path, description = "Dev session id")),
    request_body(content = RefreshGitTokenBody, content_type = "application/json"),
    responses(
        (status = 200, description = "Token refreshed", body = GitTokenRefreshed),
        (status = 404, description = "Session not found", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(session_id = %id))]
pub async fn refresh_git_token(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(body): Json<RefreshGitTokenBody>,
) -> Response {
    let Some((_, cap)) = state.dev_sessions.lookup(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("dev session {id} not found") })),
        )
            .into_response();
    };

    let expires_at = Instant::now() + Duration::from_secs(body.git_token_ttl_secs);
    state
        .git_sessions
        .refresh_token(&cap, body.git_token, expires_at);
    state.dev_sessions.bump_activity(&id);

    Json(GitTokenRefreshed { refreshed: true }).into_response()
}

/// `DELETE /v1/dev-sessions/:id` — tear down the dev-FC, revoke the git-proxy
/// capability, and remove the session from the registry. Idempotent-ish: a
/// second DELETE returns 404.
#[utoipa::path(
    delete,
    path = "/v1/dev-sessions/{id}",
    params(("id" = String, Path, description = "Dev session id")),
    responses(
        (status = 200, description = "Session purged", body = DevSessionPurged),
        (status = 404, description = "Session not found", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(session_id = %id))]
pub async fn delete_dev_session(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.dev_sessions.remove(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("dev session {id} not found") })),
        )
            .into_response();
    };

    // Revoke the git-proxy capability immediately.
    state.git_sessions.revoke(&session.cap);

    // Purge the VM (purge NOT stop — the monitor must never respawn it).
    if let Err(e) = state.orchestrator.purge_app(&session.app_uuid).await {
        tracing::warn!(
            session_id = %id,
            app_uuid = %session.app_uuid,
            error = %e,
            "dev-session purge_app failed (continuing)"
        );
    }

    Json(DevSessionPurged { purged: true }).into_response()
}

/// `GET /v1/dev-sessions` — list all live sessions (cheap, for ops).
#[utoipa::path(
    get,
    path = "/v1/dev-sessions",
    responses(
        (status = 200, description = "List of live dev sessions"),
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn list_dev_sessions(State(state): State<SharedState>) -> Response {
    let now = Instant::now();
    let rows: Vec<DevSessionRow> = state
        .dev_sessions
        .snapshot()
        .into_iter()
        .map(
            |(session_id, app_uuid, _, created_at, last_activity, repo_url, branch)| DevSessionRow {
                session_id,
                app_uuid,
                created_age_secs: now.duration_since(created_at).as_secs(),
                idle_secs: now.duration_since(last_activity).as_secs(),
                repo_url,
                branch,
            },
        )
        .collect();
    Json(json!({ "sessions": rows })).into_response()
}

// ── Idle reaper ───────────────────────────────────────────────────────────────

/// Scan the dev-session registry for sessions that have exceeded the idle TTL
/// or the hard maximum TTL, and tear them down.
///
/// Returns the session IDs that were purged. Designed to be called from a
/// dedicated tokio interval task in `main.rs` every 60 s.
///
/// The TTL parameters are injected so tests can use short durations without
/// sleeping; production code passes the module-level constants.
pub async fn sweep_expired(
    state: &Arc<crate::api::SupervisorState>,
    idle_ttl: Duration,
    max_ttl: Duration,
) -> Vec<String> {
    let now = Instant::now();
    let expired: Vec<(String, String, String)> = state
        .dev_sessions
        .snapshot()
        .into_iter()
        .filter_map(|(session_id, app_uuid, cap, created_at, last_activity, _, _)| {
            let idle = now.duration_since(last_activity);
            let age = now.duration_since(created_at);
            if idle > idle_ttl || age > max_ttl {
                Some((session_id, app_uuid, cap))
            } else {
                None
            }
        })
        .collect();

    let mut purged = Vec::new();
    for (session_id, app_uuid, cap) in expired {
        // Remove from registry first so a concurrent request gets 404.
        state.dev_sessions.remove(&session_id);
        // Revoke the git-proxy capability.
        state.git_sessions.revoke(&cap);
        // Purge the VM.
        if let Err(e) = state.orchestrator.purge_app(&app_uuid).await {
            tracing::warn!(
                session_id,
                app_uuid,
                error = %e,
                "dev-session idle-reap purge_app failed (continuing)"
            );
        }
        tracing::info!(
            session_id,
            app_uuid,
            "dev-session reaped (idle/max-ttl exceeded)"
        );
        purged.push(session_id);
    }
    purged
}

// Tests live out-of-line (<500-line file rule); the file IS this module body.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "dev_sessions_tests.rs"]
mod tests;
