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
use std::net::IpAddr;
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

use super::ssh_jump::{jump_addr_string, start_dev_ssh_jump};
use crate::api::{
    DevSessionRecord, GIT_PROXY_IPV4_PORT, GitSessionEntry, SharedState, SshJump, now_unix,
};
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
    /// The per-session SSH TCP jump: a transparent forward on `[my_ula]:<port>`
    /// that relays node SSH to the dev-FC's tap `guest_ip:2222` (the node has no
    /// route into the tenant network — see [`crate::api::SshJump`]). `None` when
    /// the forward could not be started (non-Linux / un-derivable guest_ip /
    /// bind failure); the node then falls back to the direct app-ULA path.
    /// Owning it here ties the forward's lifetime to the session: a `remove`
    /// drops it and frees the port.
    pub ssh_jump: Option<SshJump>,
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

    /// Returns a snapshot of `(session_id, app_uuid, cap, created_at,
    /// last_activity, repo_url, branch, ssh_jump_port)` for every session. Used
    /// by the list endpoint (which turns `ssh_jump_port` into `ssh_jump_addr`)
    /// and the max-age reaper.
    #[allow(clippy::type_complexity)]
    pub fn snapshot(
        &self,
    ) -> Vec<(
        String,
        String,
        String,
        Instant,
        Instant,
        String,
        String,
        Option<u16>,
    )> {
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
                    s.ssh_jump.as_ref().map(SshJump::port),
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
    super::ssh_jump::derive_dev_fc_link_ips(app_uuid, image_ref, tap_subnet)
        .map(|(host_ip, _)| host_ip.to_string())
        // Non-Linux / un-derivable: FC VMs cannot run here, so a sentinel is
        // harmless (dev-session create only really runs on a Linux KVM host).
        .unwrap_or_else(|| "127.0.0.1".to_owned())
}

// The `(host_ip, guest_ip)` /30 derivation + the SSH-jump start/address helpers
// live in `super::ssh_jump` (cohesive with the forward itself); see that module.

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
    /// Node-facing SSH-jump address (`"[<my_ula>]:<port>"`): the supervisor mesh
    /// ULA + ephemeral port the node SSHes to instead of the (unrouted) dev-FC
    /// app-ULA. `None` when no jump was started (the node falls back to the
    /// direct path). Omitted from the wire when absent so an older node ignores
    /// it cleanly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_jump_addr: Option<String>,
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
    /// Node-facing SSH-jump address (`"[<my_ula>]:<port>"`) — see
    /// [`DevSessionCreated::ssh_jump_addr`]. Lets a node that lost its in-memory
    /// map (restart) re-learn the current jump address from the list. Omitted
    /// when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_jump_addr: Option<String>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `POST /v1/dev-sessions` — spawn an always-on dev-FC + register a git-proxy
/// capability. Returns `202 Accepted` immediately with
/// `{ session_id, app_uuid, git_remote }`.
///
/// The deploy is ASYNCHRONOUS: the session + git cap are registered up front and
/// the VM provisions in a background task (a cold image pull can take minutes —
/// see the inline rationale below). The node observes readiness via
/// exec/list/status rather than blocking on the create call.
#[utoipa::path(
    post,
    path = "/v1/dev-sessions",
    request_body(
        content = CreateDevSessionBody,
        description = "Dev-session creation: image ref, repo, branch, token, SSH key",
        content_type = "application/json"
    ),
    responses(
        (status = 202, description = "Session accepted; VM provisions asynchronously", body = DevSessionCreated),
        (status = 500, description = "Spawn setup failure", body = crate::api::ErrorResponse),
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

    // Start the per-session SSH TCP jump: the node has NO route into the tenant
    // network where this dev-FC lives, so it SSHes to a transparent forward on
    // the supervisor's OWN mesh ULA (`[my_ula]:<ephemeral>`) which relays to the
    // dev-FC tap `guest_ip:2222`. SSH auth stays end-to-end node↔dev-FC. The
    // listener binds NOW (before the FC boots); the node's exec retry absorbs the
    // window where the dial to `guest_ip:2222` is refused until sshd is up.
    // `None` (non-Linux / un-derivable / bind failure) ⇒ no `ssh_jump_addr` →
    // the node falls back to the direct app-ULA path.
    let ssh_jump = match state.ula.parse::<IpAddr>() {
        Ok(my_ula) => {
            start_dev_ssh_jump(my_ula, &body.app_uuid, &body.image_ref, &state.tap_subnet, None)
                .await
        }
        Err(e) => {
            tracing::warn!(ula = %state.ula, error = %e, "control ULA not an IP; ssh-jump disabled");
            None
        }
    };
    let ssh_jump_port = ssh_jump.as_ref().map(SshJump::port);
    let ssh_jump_addr = ssh_jump_port.map(|p| jump_addr_string(&state.ula, p));

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

    // ASYNC spawn (202). Previously this `await`ed `deploy_app` SYNCHRONOUSLY
    // until the VM was HEALTHY — but a cold image pull takes minutes, exceeding
    // the node's 300 s HTTP timeout, so the node reported "create failed" while
    // the DETACHED runner kept provisioning (an orphan VM, plus a duplicate when
    // the agent retried). Instead: register the session NOW so the node can
    // list/exec/refresh it immediately, then provision in the BACKGROUND. On
    // failure, revoke the git cap + drop the session so a later exec/list sees it
    // gone. The runner is detached, so provisioning survives this handler return.
    let now = Instant::now();
    state.dev_sessions.insert(DevSession {
        session_id: session_id.clone(),
        app_uuid: body.app_uuid.clone(),
        cap: cap.clone(),
        created_at: now,
        last_activity: now,
        repo_url: body.repo_url.clone(),
        branch: body.branch.clone(),
        // The registry OWNS the forward: dropping this session (delete / reap /
        // async-deploy-failure) aborts the listener and frees the port.
        ssh_jump,
    });

    // Persist a durable sidecar so this session survives a supervisor restart/OTA:
    // the dev-VM runner survives (KillMode=process) but the in-memory registries
    // do not, so without this the VM is orphaned on restart (see
    // [`crate::api::readopt_dev_sessions`]). Best-effort — a write failure only
    // loses restart-survival; the in-memory session still works. The async-failure
    // path below removes it again.
    let now_u = now_unix();
    let record = DevSessionRecord {
        session_id: session_id.clone(),
        app_uuid: body.app_uuid.clone(),
        cap: cap.clone(),
        repo_url: body.repo_url.clone(),
        branch: body.branch.clone(),
        created_at_unix: now_u,
        last_activity_unix: now_u,
        // Persist the jump port so a supervisor restart can re-bind the SAME port
        // (keeping the node's cached jump address valid). `readopt_dev_sessions`
        // reads it back.
        ssh_jump_port,
    };
    if let Err(e) = record.save(state.orchestrator.runner_dir()) {
        tracing::warn!(app_uuid = %body.app_uuid, error = %e, "failed to persist dev-session record (session live in-memory only)");
    }

    let bg = state.clone();
    let app_uuid = body.app_uuid.clone();
    let image_ref = body.image_ref.clone();
    let sid = session_id.clone();
    tokio::spawn(async move {
        match bg
            .orchestrator
            // Dev-sessions carry no egress allow-list (no network ACL surface on
            // the dev-VM path) → `None` keeps unrestricted egress for the dev box.
            .deploy_app(&app_uuid, &image_ref, None, None, net, Some(&extra_env), None)
            .await
        {
            Ok(_) => {
                tracing::info!(app_uuid = %app_uuid, session_id = %sid, "dev-session provisioned (async)");
            }
            Err(e) => {
                tracing::warn!(
                    app_uuid = %app_uuid, session_id = %sid, error = %e,
                    "dev-session deploy failed (async); purging partial runtime before dropping session"
                );
                match bg.orchestrator.purge_app(&app_uuid).await {
                    Ok(()) => {
                        bg.git_sessions.revoke(&cap);
                        bg.dev_sessions.remove(&sid);
                        let _ = DevSessionRecord::remove(bg.orchestrator.runner_dir(), &app_uuid);
                    }
                    Err(purge_error) => tracing::error!(
                        app_uuid = %app_uuid,
                        session_id = %sid,
                        error = %purge_error,
                        "partial dev-session purge failed; retaining session retry handles"
                    ),
                }
            }
        }
    });

    // 202 Accepted: session is registered + provisioning in the background. The
    // node observes readiness via exec/list/status — never blocks on the pull.
    (
        axum::http::StatusCode::ACCEPTED,
        Json(DevSessionCreated {
            session_id,
            app_uuid: body.app_uuid,
            git_remote,
            ssh_jump_addr,
        }),
    )
        .into_response()
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
    let Some((app_uuid, cap)) = state.dev_sessions.lookup(&id) else {
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

    // Persist the activity bump so `idle_secs` survives a restart (best-effort).
    // A `None` record is a pre-persistence session (created before this fix) —
    // nothing to update.
    let runner_dir = state.orchestrator.runner_dir();
    match DevSessionRecord::load(runner_dir, &app_uuid) {
        Ok(Some(mut rec)) => {
            rec.last_activity_unix = now_unix();
            if let Err(e) = rec.save(runner_dir) {
                tracing::warn!(session_id = %id, error = %e, "failed to persist dev-session activity bump");
            }
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(session_id = %id, error = %e, "failed to load dev-session record for activity bump");
        }
    }

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
    // F2.2b (audit #93): REAP THE FC BEFORE forgetting the session. Look the
    // session up WITHOUT removing it (still the 404 gate) so every handle that
    // lets us find + reap the VM survives the purge; only AFTER `purge_app` has
    // killed the FC do we drop the registry entry / cap / durable sidecar. The
    // old order removed the registry entry + sidecar FIRST and purged LAST — a
    // crash in between stranded the FC with no record + no pidfile.
    let Some((app_uuid, cap)) = state.dev_sessions.lookup(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("dev session {id} not found") })),
        )
            .into_response();
    };

    // Purge the VM FIRST (purge NOT stop — the monitor must never respawn it).
    if let Err(e) = state.orchestrator.purge_app(&app_uuid).await {
        tracing::error!(
            session_id = %id,
            app_uuid = %app_uuid,
            error = %e,
            "dev-session purge_app failed; retaining session for retry"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to purge dev session {id}: {e}") })),
        )
            .into_response();
    }

    // FC reaped — now forget the session everywhere.
    state.dev_sessions.remove(&id);
    // Revoke the git-proxy capability.
    state.git_sessions.revoke(&cap);
    // Remove the durable sidecar so a later restart cannot resurrect a phantom
    // session for the purged VM (best-effort).
    if let Err(e) = DevSessionRecord::remove(state.orchestrator.runner_dir(), &app_uuid) {
        tracing::warn!(
            session_id = %id,
            app_uuid = %app_uuid,
            error = %e,
            "failed to remove dev-session record on delete"
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
            |(session_id, app_uuid, _, created_at, last_activity, repo_url, branch, ssh_jump_port)| {
                DevSessionRow {
                    session_id,
                    app_uuid,
                    created_age_secs: now.duration_since(created_at).as_secs(),
                    idle_secs: now.duration_since(last_activity).as_secs(),
                    repo_url,
                    branch,
                    // Re-derive the node-facing jump address from our control ULA
                    // + the bound port so a restarted node can re-learn it.
                    ssh_jump_addr: ssh_jump_port.map(|p| jump_addr_string(&state.ula, p)),
                }
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
        .filter_map(|(session_id, app_uuid, cap, created_at, last_activity, _, _, _)| {
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
        // F2.2b (audit #93): REAP THE FC BEFORE forgetting the session. The old
        // order removed the registry entry + durable sidecar FIRST and purged the
        // VM LAST — a crash between the two stranded the FC with NO record + NO
        // pidfile, invisible to the per-uuid reaper (an orphan only the new
        // record-less sweep could later catch). Purge first so the FC is gone
        // before we drop every handle that lets us find it again.
        if let Err(e) = state.orchestrator.purge_app(&app_uuid).await {
            tracing::warn!(
                session_id,
                app_uuid,
                error = %e,
                "dev-session idle-reap purge_app failed; retaining retry handles"
            );
            continue;
        }
        // Now the FC is reaped — safe to forget the session everywhere.
        // Remove from registry so a concurrent request gets 404.
        state.dev_sessions.remove(&session_id);
        // Revoke the git-proxy capability.
        state.git_sessions.revoke(&cap);
        // Remove the durable sidecar (third teardown path, alongside delete +
        // async-deploy-failure) so a restart cannot resurrect a reaped session.
        if let Err(e) = DevSessionRecord::remove(state.orchestrator.runner_dir(), &app_uuid) {
            tracing::warn!(session_id, app_uuid, error = %e, "dev-session reap: failed to remove record");
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
