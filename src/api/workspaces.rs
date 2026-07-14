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
//! - `POST   /v1/workspaces/:uuid/snapshot` — refresh the warm-LSP snapshot.
//! - `POST   /v1/workspaces/:uuid/repos` — ADD one repo (additive cap +
//!   respawn so the runner re-bakes the full cap-file set), async.
//! - `POST   /v1/workspaces/:uuid/stop` — pause the VM, PRESERVING the record
//!   (image_ref/extra_env) for a warm restore.

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
use crate::orchestrator::handle::RunnerHandle;

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

/// MERGE one cap-file entry (a repo's `<repo>.url`, or the reserved
/// `forge-admin.token` on a backfilling add_repo) into the persisted
/// [`CAP_FILES_ENV`] JSON map in an existing workspace's `extra_env`, PRESERVING
/// every other cap-file entry already baked in (the other repos' URLs, the
/// forge-admin token, `authkeys.cap`). This is the additive `add_repo` respawn
/// channel: the
/// supervisor never persists the authorized-key / forge-admin token in the
/// durable [`WorkspaceRecord`], but they DO live in the runner's persisted
/// `extra_env` (the create-time `CAP_FILES_ENV` map) — so merging into that map
/// (instead of rebuilding from the record) is what lets a respawn re-bake the
/// FULL cap-file set without losing the authkeys/forge secrets. A record with no
/// prior `CAP_FILES_ENV` starts a fresh single-entry map. All non-cap env keys
/// (the workspace marker, user-id, authorized-key) are untouched.
///
/// `cap_file` is the cap-file NAME (a repo's `"<stem>.url"` from
/// [`cap_repo_basename`], or the reserved `"forge-admin.token"` on a backfilling
/// add_repo); `value` is that file's content (a TOKENLESS git-proxy URL — no
/// secret, the token stays host-side in `GitSessions` — or the forge-admin creds
/// JSON). A malformed prior value (not a JSON object) is replaced by a fresh
/// single-entry map rather than propagating corruption.
fn merge_cap_into_env(
    extra_env: &mut HashMap<String, String>,
    cap_file: &str,
    value: &str,
) {
    let mut cap_files: serde_json::Map<String, serde_json::Value> = extra_env
        .get(CAP_FILES_ENV)
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    cap_files.insert(
        cap_file.to_owned(),
        serde_json::Value::String(value.to_owned()),
    );
    extra_env.insert(
        CAP_FILES_ENV.to_owned(),
        serde_json::Value::Object(cap_files).to_string(),
    );
}

/// PRESERVE the durable `<repo>.url` cap-file entries from a workspace's PRIOR
/// persisted env across a re-provision. [`create_workspace`] rebuilds its
/// `cap_files` map FROM SCRATCH on every call (fresh authkeys/forge caps + the
/// request `repos`), so a repo that was added LATER via [`add_workspace_repo`] —
/// whose `<repo>.url` cap lives ONLY in the prior [`CAP_FILES_ENV`] map, never in
/// the create request — would be WIPED, and the runner's cold re-bake would then
/// drop that repo's clone (an in-house forge repo never lands in `~/projects`).
///
/// `prior_extra_env` is the previously-persisted runner env (from
/// [`crate::orchestrator::handle::RunnerHandle::extra_env`], the SAME map
/// `add_workspace_repo` merges into). Every prior `*.url` entry (the tokenless
/// git-remote URLs) is carried into the freshly-built `cap_files` WITHOUT
/// overwriting a same-stem entry the request already produced (the request is the
/// current source of truth for its own repos, with freshly-minted caps).
///
/// ONLY `*.url` keys are carried here: `forge-admin.token` is request-supplied
/// on every create (auth is its source of truth — preserving a prior one could
/// resurrect a revoked credential), and `authkeys.cap` has its own explicit
/// reuse-or-mint step in [`create_workspace`] (see
/// `crate::api::workspace_cap_reuse` — reused for value stability, minted when
/// absent). No prior record / no `CAP_FILES_ENV` / a malformed prior map ⇒ a
/// no-op (create behaves exactly as before).
fn preserve_prior_repo_caps(
    cap_files: &mut serde_json::Map<String, serde_json::Value>,
    prior_extra_env: Option<&HashMap<String, String>>,
) {
    let Some(prior_json) = prior_extra_env.and_then(|e| e.get(CAP_FILES_ENV)) else {
        return; // no prior record / no CAP_FILES_ENV → behavior unchanged
    };
    let Some(prior_map) = serde_json::from_str::<serde_json::Value>(prior_json)
        .ok()
        .and_then(|v| v.as_object().cloned())
    else {
        return; // malformed prior map → carry nothing (never propagate corruption)
    };
    for (name, value) in prior_map {
        // Carry ONLY durable repo cap-URLs (`<stem>.url`): skip the re-minted
        // secrets (authkeys.cap / forge-admin.token) and never clobber a fresh
        // same-stem entry the create request already produced above.
        if name.ends_with(".url") && value.is_string() && !cap_files.contains_key(&name) {
            cap_files.insert(name, value);
        }
    }
}

/// PRESERVE the durable add_repo `WorkspaceCap` + branch rows from a workspace's
/// PRIOR record across a re-provision — the DURABLE-RECORD twin of
/// [`preserve_prior_repo_caps`]. [`create_workspace`] rebuilds `record_caps` /
/// `branches` FROM SCRATCH from the request `repos`, so a repo added later via
/// [`add_workspace_repo`] (its cap row lives ONLY in the prior durable record,
/// not in THIS request) would be DROPPED from the persisted record. On a COLD
/// supervisor restart, readopt re-registers the git-proxy caps FROM this record —
/// so dropping the add_repo cap would ORPHAN the `<repo>.url` cap-URL that
/// [`preserve_prior_repo_caps`] just kept in `/init` (its cap would no longer be
/// registered → git-proxy 403). Carrying the prior cap+branch forward keeps the
/// two preservations SYMMETRIC so the preserved URL always resolves after a cold
/// boot, not just warm.
///
/// A prior cap whose `repo_url` the request ALSO carries is skipped (the request
/// is the current source of truth for its own repos, with a freshly-minted cap).
/// The branch rides parallel-by-index in the prior record; a malformed record
/// with a shorter `branches` vec defaults the carried branch to empty. No prior
/// record ⇒ a no-op (create behaves exactly as before).
fn preserve_prior_record_caps(
    record_caps: &mut Vec<WorkspaceCap>,
    branches: &mut Vec<String>,
    prior_record: Option<&WorkspaceRecord>,
) {
    let Some(prior) = prior_record else {
        return; // no prior record → behavior unchanged
    };
    // Owned (not borrowed from `record_caps`) so we can push to it in the loop.
    let request_urls: std::collections::HashSet<String> =
        record_caps.iter().map(|c| c.repo_url.clone()).collect();
    for (i, cap) in prior.caps.iter().enumerate() {
        // The request re-provisions its own repos with fresh caps — skip those.
        if request_urls.contains(&cap.repo_url) {
            continue;
        }
        record_caps.push(cap.clone());
        branches.push(prior.branches.get(i).cloned().unwrap_or_default());
    }
}

/// Insert the broker's forge ENDPOINT config into the workspace boot env:
/// `TABBIFY_FORGE_URL` + `TABBIFY_FORGE_ORG` (§12 S1/S2). These two keys are
/// exactly what the in-FC broker's `ForgeCfg::from_env` requires; without them
/// every forge op fails "forge not configured" even when the creds cap-file is
/// present. Both are NON-secret (a mesh address + the tenant slug — the CREDS
/// ride the 0600 cap-file channel instead), so the env channel is safe: neither
/// key is in
/// [`crate::firecracker::snapshot_decision::snapshot_forbidden_env_keys`].
/// `None` ⇒ the key is omitted (no forge / an older node) — forge ops then
/// honestly report unconfigured rather than dialing a bogus endpoint.
///
/// FORGE-PROXY REWRITE (mandatory): the node passes `forge_url` as the forge's
/// RAW v6 mesh ULA (`http://[fd5a:…]:8730`). A workspace FC guest is IPv4-only on
/// its /30 tap and cannot route v6, so the guest-facing value is ALWAYS the
/// host-side proxy gateway URL (`http://{host_ip}:FORGE_PROXY_IPV4_PORT`, from
/// [`crate::api::forge_proxy_gateway_url`]) — the L4 forward relays it to the
/// forge over the mesh. Baking the raw v6 ULA into an IPv4-only FC is the exact
/// #107 bug, so when a `forge_url` is configured but no `forge_proxy_gateway` is
/// supplied we return [`ForgeEnvError::MissingGateway`] rather than silently
/// falling back to the raw ULA. The ORG slug is NEVER rewritten.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ForgeEnvError {
    /// A forge is configured but the mandatory host-side proxy gateway was not
    /// supplied — baking a raw v6 ULA into an IPv4-only FC is the exact bug this
    /// guards against, so we refuse rather than silently fall back.
    #[error("forge configured but no forge-proxy gateway available")]
    MissingGateway,
}

fn insert_forge_env(
    extra_env: &mut HashMap<String, String>,
    forge_url: &Option<String>,
    forge_org: &Option<String>,
    forge_proxy_gateway: Option<&str>,
) -> Result<(), ForgeEnvError> {
    // The URL is ALWAYS the tap-gateway proxy (the raw v6 ULA is unreachable
    // from the IPv4-only FC). Only injected when the node supplied a forge_url
    // (no forge → no key → honest "unconfigured"); a configured forge WITHOUT a
    // gateway is a hard error — we never bake the raw ULA.
    if forge_url.is_some() {
        let gw = forge_proxy_gateway.ok_or(ForgeEnvError::MissingGateway)?;
        extra_env.insert("TABBIFY_FORGE_URL".to_owned(), gw.to_owned());
    }
    // The ORG slug rides the env channel untouched (never rewritten).
    if let Some(org) = forge_org {
        extra_env.insert("TABBIFY_FORGE_ORG".to_owned(), org.clone());
    }
    Ok(())
}

/// The reserved cap-file name for the §12-S6 authorized-keys cap (the `:8732`
/// add-key bearer token). Written by the runner as a 0600 broker-uid file under
/// `/run/tabbify/caps/` (same off-env channel as the git caps / forge-admin
/// token), so the AGENT uid can never read it. One constant, one source.
pub const AUTHKEYS_CAP_FILE: &str = "authkeys.cap";

/// Generate the §12-S6 authorized-keys cap for `ws_uuid` and insert it into the
/// `cap_files` map under [`AUTHKEYS_CAP_FILE`], returning the token so the
/// handler can ALSO return it to node. The token is an unguessable
/// blake3-over-CSPRNG-salts value (the same generator the git caps use); the
/// broker validates incoming `:8732` add-key requests against this exact value,
/// and the runner writes it 0600 broker-uid so the agent cannot read it.
fn insert_authkeys_cap(
    ws_uuid: &str,
    cap_files: &mut serde_json::Map<String, serde_json::Value>,
) -> String {
    let token = generate_cap(ws_uuid, "authkeys");
    cap_files.insert(
        AUTHKEYS_CAP_FILE.to_owned(),
        serde_json::Value::String(token.clone()),
    );
    token
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
    /// Mesh-internal Forgejo base URL — injected into the FC env as
    /// `TABBIFY_FORGE_URL` so the broker's `ForgeCfg::from_env` resolves.
    /// NON-secret (a mesh address), so the env channel is safe (not in
    /// [`crate::firecracker::snapshot_decision::snapshot_forbidden_env_keys`]).
    /// `None` (an older node / no forge configured) ⇒ no env key — forge ops
    /// honestly report unconfigured.
    #[schema(example = "http://[fd5a:1f02::1]:8730")]
    #[serde(default)]
    pub forge_url: Option<String>,
    /// The account's forge org slug — injected as `TABBIFY_FORGE_ORG` (the org
    /// the broker provisions/lists repos under; never agent-supplied). NON-secret
    /// (the tenant slug).
    #[schema(example = "t_acme")]
    #[serde(default)]
    pub forge_org: Option<String>,
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
    /// The authorized-keys cap (§12 S6): the bearer token node MUST present on
    /// its `POST [ula]:8732/v1/authorized-keys` add-key calls (T4 IDE-remote
    /// dynamic add-key). The SAME token is written into the FC as the off-env
    /// cap-file `/run/tabbify/caps/authkeys.cap` (0600, broker-uid), which the
    /// broker reads to validate. The AGENT uid cannot read that cap-file, so only
    /// node (which receives this token over the trusted node→supervisor channel)
    /// can authorize an add-key. NOT a git credential — but it IS an authz
    /// secret, so node MUST hold it off agent reach and not log it.
    pub authkeys_cap: String,
}

/// `POST /v1/workspaces/{uuid}/repos` request body — ADD one repo to an existing
/// workspace. Mirrors [`RepoSpec`] field-for-field (the node mints the token +
/// resolves the repo exactly like create, then sends this single-repo body), plus
/// the optional forge fields the create body carries (§12 S1/S2). When the node
/// threads them, the handler MERGES `forge-admin.token` into the persisted
/// `CAP_FILES_ENV` map + sets `TABBIFY_FORGE_URL`/`TABBIFY_FORGE_ORG`, so the cold
/// respawn re-bakes the forge cap-file — backfilling a workspace whose forge org
/// did not exist at provision time. All three are `#[serde(default)]`: a pre-fix
/// node omits them and the body deserializes exactly as before.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AddRepoBody {
    /// Provider clone URL WITHOUT credentials.
    #[schema(example = "https://github.com/acme/app.git")]
    pub repo_url: String,
    /// Branch to make available.
    #[schema(example = "main")]
    pub branch: String,
    /// Short-lived provider token (the git proxy injects it; the VM never sees it).
    pub git_token: String,
    /// Token TTL in seconds.
    #[schema(example = "3600")]
    pub git_token_ttl_secs: u64,
    /// Optional forge-admin creds JSON (§12 S1/S2): merged into the persisted
    /// `CAP_FILES_ENV` as `forge-admin.token` so the cold respawn writes it to
    /// `/run/tabbify/caps/forge-admin.token` (0600, broker-uid). `None` when the
    /// account has no in-mesh forge org (or a pre-fix node). NEVER in agent env.
    #[serde(default)]
    pub forge_admin_token: Option<String>,
    /// Mesh-internal Forgejo base URL — injected into the FC env as
    /// `TABBIFY_FORGE_URL` (rewritten to the tap-gateway proxy when the host-side
    /// forge-proxy is enabled) so the broker's `ForgeCfg::from_env` resolves.
    /// NON-secret. `None` ⇒ the env key is left untouched.
    #[schema(example = "http://[fd5a:1f02::1]:8730")]
    #[serde(default)]
    pub forge_url: Option<String>,
    /// The account's forge org slug — injected as `TABBIFY_FORGE_ORG`. NON-secret
    /// (the tenant slug). `None` ⇒ the env key is left untouched.
    #[schema(example = "t_acme")]
    #[serde(default)]
    pub forge_org: Option<String>,
}

/// `POST /v1/workspaces/{uuid}/repos` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct AddRepoResult {
    /// The workspace the repo was added to (== the FC app uuid).
    pub workspace_uuid: String,
    /// The cap-file stem the repo was registered under (`<stem>.url`), derived
    /// from the clone URL's last path segment (sanitized). The node clones into
    /// `~/projects/<stem>` after the respawn.
    pub repo: String,
    /// The TOKENLESS git-proxy remote the in-VM broker uses for this repo.
    pub git_remote: String,
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

    /// Append one repo's git-proxy `cap` to a live workspace's cap list
    /// (additive — keeps the existing caps so delete/snapshot 404-gating stays
    /// correct after an `add_repo`). Returns `true` if the workspace existed and
    /// the cap was appended, `false` (a no-op) for an unknown workspace.
    pub fn append_cap(&self, workspace_uuid: &str, cap: String) -> bool {
        match self
            .0
            .lock()
            .expect("workspace lock")
            .get_mut(workspace_uuid)
        {
            Some(ws) => {
                ws.caps.push(cap);
                true
            }
            None => false,
        }
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
    let operation_lock = state.workspace_operation_lock(&ws_uuid);
    let operation_guard = operation_lock.lock_owned().await;

    // PRIOR GENERATION (loaded up front — both preservation blocks below AND the
    // cap-REUSE in the repo loop need it): the previously-persisted runner env
    // (the CAP_FILES_ENV map) + the durable record (the (cap, repo_url) rows).
    let prior_extra_env = RunnerHandle::load(state.orchestrator.runner_dir(), &ws_uuid)
        .ok()
        .flatten()
        .and_then(|h| h.extra_env);
    let prior_record = WorkspaceRecord::load(state.orchestrator.runner_dir(), &ws_uuid)
        .ok()
        .flatten();

    // RE-KEY #2: multi-repo. One git-proxy cap PER repo, registered in the
    // SHARED GitSessions HashMap BEFORE the spawn so the VM can reach every
    // remote from first boot. N caps are free (it is a HashMap).
    //
    // CAP-VALUE STABILITY (stale-caps invariant, cost half): the rootfs cache
    // key fingerprints cap VALUES, so an ensure must only change a value when it
    // GENUINELY rotates. Reuse the prior generation's cap token for the same
    // repo_url (re-registered below with the request's FRESH provider token/TTL)
    // instead of re-minting per ensure — re-minting would force a full ~2.3 GB
    // rootfs rebuild on EVERY ensure. See `crate::api::workspace_cap_reuse`.
    let host_ip = derive_dev_fc_host_ip(&ws_uuid, &body.image_ref, &state.tap_subnet);
    let mut caps: Vec<String> = Vec::with_capacity(body.repos.len());
    let mut git_remotes: Vec<String> = Vec::with_capacity(body.repos.len());
    let mut record_caps: Vec<WorkspaceCap> = Vec::with_capacity(body.repos.len());
    let mut branches: Vec<String> = Vec::with_capacity(body.repos.len());
    // §12 S1 cap-file payloads: filename → file content. Written by the runner to
    // `/run/tabbify/caps/<filename>` (0600, broker-uid) and REMOVED from env.
    let mut cap_files: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for repo in &body.repos {
        let (cap, cap_provenance) = match crate::api::workspace_cap_reuse::prior_repo_cap(
            prior_record.as_ref(),
            &repo.repo_url,
        ) {
            Some(prior) => (prior, "reused prior token (value-stable ensure)"),
            None => (
                generate_cap(&ws_uuid, &repo.repo_url),
                "minted fresh (no prior record for this repo_url)",
            ),
        };
        // Cache-decision trace (token value NEVER logged): whether this ensure
        // keeps the baked `<stem>.url` byte-stable (→ rootfs cache hit) or
        // introduces a new value (→ fingerprint change → re-bake).
        tracing::info!(
            workspace_uuid = %ws_uuid,
            repo_stem = %cap_repo_basename(&repo.repo_url),
            provenance = cap_provenance,
            "workspace create: git cap provenance (value never logged)"
        );
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

    // PRESERVE add_repo cap-URLs across a re-provision. `cap_files` was just
    // rebuilt FROM SCRATCH (request repos + forge/authkeys caps), so a repo
    // added later via `add_workspace_repo` — whose `<repo>.url` lives ONLY in the
    // prior persisted runner env, not in THIS request — would be clobbered and the
    // cold re-bake would drop its clone (the in-house forge repo never lands in
    // `~/projects`). Carry every prior `*.url` entry forward (the prior runner
    // env — the SAME CAP_FILES_ENV map add_workspace_repo merges into — was
    // loaded above). Best-effort: a load error / no prior record leaves create
    // unchanged. The warm git-session cap stays registered (add_repo never
    // revoked it); a cold supervisor re-registers it from the durable record on
    // readopt, so the preserved URL still resolves.
    preserve_prior_repo_caps(&mut cap_files, prior_extra_env.as_ref());

    // SYMMETRIC cold-safety: also carry the prior DURABLE record's add_repo
    // cap/branch rows forward. readopt re-registers git-proxy caps from THIS
    // record after a supervisor restart, so dropping the add_repo cap would
    // orphan the `<repo>.url` just preserved above (its /init cap-file would then
    // reference an UNregistered cap → git-proxy 403 on a cold boot). Fresh request
    // repos win on repo_url collision. Best-effort load (no prior record → no-op).
    preserve_prior_record_caps(&mut record_caps, &mut branches, prior_record.as_ref());

    // §12 S6: the authorized-keys cap, written into the FC as the off-env
    // cap-file `authkeys.cap` (the runner writes it 0600, broker-uid — so the
    // AGENT uid cannot read it) and ALSO returned to node so node can authorize
    // its `[ula]:8732/v1/authorized-keys` add-key POSTs. The broker validates
    // incoming :8732 requests against this exact token; an unauthenticated
    // (agent) request 401s. Cap-file channel ONLY — never an env var, never the
    // agent's reach.
    //
    // CAP-VALUE STABILITY: REUSED from the prior generation when present (same
    // workspace, same trust domain, same lifetime — a per-ensure rotation added
    // no security and churned the rootfs fingerprint into a ~2.3 GB rebuild per
    // ensure); minted fresh only on the first ensure / after a purge.
    let authkeys_cap =
        match crate::api::workspace_cap_reuse::prior_authkeys_cap(prior_extra_env.as_ref()) {
            Some(prior) => {
                tracing::info!(
                    workspace_uuid = %ws_uuid,
                    provenance = "reused prior token (value-stable ensure)",
                    "workspace create: authkeys.cap provenance (value never logged)"
                );
                cap_files.insert(
                    AUTHKEYS_CAP_FILE.to_owned(),
                    serde_json::Value::String(prior.clone()),
                );
                prior
            }
            None => {
                tracing::info!(
                    workspace_uuid = %ws_uuid,
                    provenance = "minted fresh (no prior persisted cap)",
                    "workspace create: authkeys.cap provenance (value never logged)"
                );
                insert_authkeys_cap(&ws_uuid, &mut cap_files)
            }
        };

    // DIAGNOSTIC (keys/presence ONLY — a cap-file's VALUE is a git-proxy URL or a
    // secret token and is NEVER logged): exactly which cap-files this create is
    // threading into `/run/tabbify/caps/` (the repo `<stem>.url`s + the reserved
    // `forge-admin.token`/`authkeys.cap`) and whether the forge ENDPOINT env is
    // set or absent. A workspace that later reports "forge not provisioned" is
    // explained by `forge_admin_token=false` HERE; a repo that never clones is
    // explained by its `<stem>.url` missing from `cap_files`.
    tracing::info!(
        workspace_uuid = %ws_uuid,
        repo_count = body.repos.len(),
        cap_files = ?cap_files.keys().collect::<Vec<_>>(),
        forge_admin_token = body.forge_admin_token.is_some(),
        forge_url_set = body.forge_url.is_some(),
        forge_org_set = body.forge_org.is_some(),
        forge_proxy_enabled = state.forge_proxy_enabled,
        "workspace create: threaded cap-files + forge endpoint env (keys/presence only; values never logged)"
    );

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
    // The broker's forge ENDPOINT config (§12 S1/S2): URL + org slug ride the
    // env channel (non-secret; the CREDS ride the cap-file above). When the
    // host-side forge-proxy is enabled, REWRITE the guest-facing URL to the FC's
    // own tap gateway (`http://{host_ip}:FORGE_PROXY_IPV4_PORT`) — the IPv4-only
    // guest cannot route the raw v6 mesh ULA the node supplies. `host_ip` here is
    // the SAME tap gateway the git remotes already point at (derived above).
    let forge_gateway = state
        .forge_proxy_enabled
        .then(|| crate::api::forge_proxy_gateway_url(&host_ip));
    if let Err(e) = insert_forge_env(
        &mut extra_env,
        &body.forge_url,
        &body.forge_org,
        forge_gateway.as_deref(),
    ) {
        tracing::error!(workspace_uuid = %ws_uuid, error = %e, "refusing to bake a raw forge ULA: forge-proxy gateway unavailable");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("forge env: {e}") })),
        )
            .into_response();
    }
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
        let _operation_guard = operation_guard;
        match bg
            .orchestrator
            // Workspace-scope egress ACL is a LABELED v2 follow-up (Track 7
            // self-review): the supervisor enforcement primitive supports the
            // `workspace` scope, but resolving + threading a workspace-scoped
            // allow-list at this spawn site lands with the Track-3 lifecycle. Until
            // then `None` keeps the workspace's egress unrestricted (no regression).
            .deploy_app(&app_uuid, &image_ref, None, None, net, Some(&extra_env), None)
            .await
        {
            Ok(_) => tracing::info!(workspace_uuid = %app_uuid, "workspace provisioned (async)"),
            Err(e) => {
                tracing::warn!(workspace_uuid = %app_uuid, error = %e, "workspace deploy failed (async); purging partial runtime before dropping workspace");
                match bg.orchestrator.purge_app(&app_uuid).await {
                    Ok(()) => {
                        for cap in &caps_for_bg {
                            bg.git_sessions.revoke(cap);
                        }
                        bg.workspaces.remove(&app_uuid);
                        let _ = WorkspaceRecord::remove(bg.orchestrator.runner_dir(), &app_uuid);
                    }
                    Err(purge_error) => tracing::error!(
                        workspace_uuid = %app_uuid,
                        error = %purge_error,
                        "partial workspace purge failed; retaining workspace retry handles"
                    ),
                }
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(WorkspaceCreated {
            workspace_uuid: ws_uuid,
            git_remotes,
            authkeys_cap,
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
    let operation_lock = state.workspace_operation_lock(&uuid);
    let _operation_guard = operation_lock.lock_owned().await;
    let Some(_) = state.workspaces.caps_of(&uuid) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    };
    if let Err(e) = state.orchestrator.purge_app(&uuid).await {
        tracing::error!(workspace_uuid = %uuid, error = %e, "workspace purge_app failed; retaining workspace for retry");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to purge workspace {uuid}: {e}") })),
        )
            .into_response();
    }
    if let Some(workspace) = state.workspaces.remove(&uuid) {
        for cap in &workspace.caps {
            state.git_sessions.revoke(cap);
        }
    }
    if let Err(e) = WorkspaceRecord::remove(state.orchestrator.runner_dir(), &uuid) {
        tracing::warn!(workspace_uuid = %uuid, error = %e, "failed to remove workspace record on delete");
    }
    Json(json!({ "purged": true })).into_response()
}

/// `POST /v1/workspaces/{uuid}/snapshot` — refresh the workspace VM's warm-LSP
/// snapshot IN-PLACE (§12 post-index, Seam B).
///
/// The node calls this AFTER the code-service reports the dogfood repo's
/// `index_status == Ready`, so the snapshot captures a WARM LSP index (the
/// cold-boot snapshot is suppressed for a workspace — see the module docs). The
/// handler dispatches `Cmd::Snapshot` to the runner via the orchestrator; the VM
/// is left RUNNING on any failure (best-effort refresh). A snapshot of a
/// workspace this supervisor does not know returns `404`.
#[utoipa::path(
    post,
    path = "/v1/workspaces/{uuid}/snapshot",
    params(("uuid" = String, Path, description = "Workspace uuid")),
    responses(
        (status = 200, description = "Warm snapshot refreshed"),
        (status = 404, description = "Workspace not found", body = crate::api::ErrorResponse),
        (status = 500, description = "Snapshot create failed (VM still serving)", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(workspace_uuid = %uuid))]
pub async fn snapshot_workspace(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
) -> Response {
    // Only snapshot a workspace this supervisor actually hosts — a stray uuid
    // (or one whose VM was already torn down) is a 404, never a blind dispatch.
    if state.workspaces.caps_of(&uuid).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    }
    match state.orchestrator.snapshot_app(&uuid).await {
        Ok(()) => Json(json!({ "snapshotted": true })).into_response(),
        Err(e) => {
            // The VM keeps serving (the runner always resumes); report the
            // failure so the node can retry the post-index snapshot.
            tracing::warn!(workspace_uuid = %uuid, error = %e, "workspace snapshot failed (VM still serving)");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("snapshot failed: {e}") })),
            )
                .into_response()
        }
    }
}

/// `POST /v1/workspaces/{uuid}/repos` — ADD one repo to an existing workspace
/// (additive; NOT a hardcoded repo). Registers a NEW git-proxy cap host-side,
/// appends the durable record + the in-mem registry, MERGES the new `<repo>.url`
/// into the runner's persisted `CAP_FILES_ENV` (preserving the existing repos +
/// the authkeys/forge-admin cap-files), and RESPAWNS the workspace VM so the
/// runner re-bakes the FULL cap-file set at COLD boot (a warm zero-downtime swap
/// re-bakes the OLD spawn-time env, so the new cap-file would not land — hence a
/// stop + cold deploy). The node then drives a `clone` over `code_call` after the
/// respawn so the repo lands in `~/projects` + rust-analyzer indexes it.
///
/// Async (202, mirror create): the respawn runs in the background (a stop + cold
/// re-deploy takes seconds, image cached). A workspace this supervisor does not
/// host (or one with no deployable artifact) is a 404 BEFORE any cap mutation.
#[utoipa::path(
    post,
    path = "/v1/workspaces/{uuid}/repos",
    params(("uuid" = String, Path, description = "Workspace uuid")),
    request_body(content = AddRepoBody, content_type = "application/json"),
    responses(
        (status = 202, description = "Repo accepted; the workspace respawns asynchronously to re-bake the full cap-file set", body = AddRepoResult),
        (status = 404, description = "Workspace not found (or no deployable artifact)", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state, body), fields(workspace_uuid = %uuid, repo_url = %body.repo_url))]
pub async fn add_workspace_repo(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    Json(body): Json<AddRepoBody>,
) -> Response {
    // Registry gate: only a workspace this supervisor hosts can gain a repo
    // (mirror snapshot/delete) — a stray uuid is a 404, never a blind mutation.
    if state.workspaces.caps_of(&uuid).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    }

    // Load the durable RunnerHandle for (a) the workspace's `image_ref` (needed
    // to derive the SAME tap host_ip the VM boots on AND to pin the respawn) and
    // (b) the persisted `extra_env` (the create-time CAP_FILES_ENV map we MERGE
    // the new repo into — that map also holds the authkeys/forge-admin caps the
    // durable WorkspaceRecord does NOT persist, so merging the runner env is what
    // keeps those secrets across the respawn). No artifact → cannot respawn → 404.
    let runner = match RunnerHandle::load(state.orchestrator.runner_dir(), &uuid) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("workspace {uuid} has no runner record") })),
            )
                .into_response();
        }
        Err(e) => {
            tracing::warn!(workspace_uuid = %uuid, error = %e, "add_repo: could not load runner record");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("could not load workspace {uuid}: {e}") })),
            )
                .into_response();
        }
    };
    let Some(image_ref) = runner.image_ref.clone() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} has no image_ref to respawn") })),
        )
            .into_response();
    };

    // Register the repo's git-proxy cap host-side (the SAME shared GitSessions
    // the create path uses), so the respawned VM can reach the remote from boot.
    //
    // CAP-VALUE STABILITY (stale-caps invariant, cost half): a RE-ADD of a repo
    // the workspace already has (the agent-retry pattern) reuses the prior cap
    // token from the durable record — re-minting would rotate the baked
    // `<stem>.url` value, change the rootfs fingerprint, and pay a full ~2.3 GB
    // re-bake for a no-op add. The reused cap is re-registered with the FRESH
    // provider token/TTL, so reuse never extends a dead upstream credential.
    let prior_record = WorkspaceRecord::load(state.orchestrator.runner_dir(), &uuid)
        .ok()
        .flatten();
    let (cap, reused_prior_cap) = match crate::api::workspace_cap_reuse::prior_repo_cap(
        prior_record.as_ref(),
        &body.repo_url,
    ) {
        Some(prior) => (prior, true),
        None => (generate_cap(&uuid, &body.repo_url), false),
    };
    tracing::info!(
        workspace_uuid = %uuid,
        provenance = if reused_prior_cap {
            "reused prior token (re-add of a known repo; value-stable)"
        } else {
            "minted fresh (first add of this repo_url)"
        },
        "add_repo: git cap provenance (value never logged)"
    );
    let host_ip = derive_dev_fc_host_ip(&uuid, &image_ref, &state.tap_subnet);
    let git_remote = format!("http://{host_ip}:{GIT_PROXY_IPV4_PORT}/git/{cap}");
    let expires_at = Instant::now() + Duration::from_secs(body.git_token_ttl_secs);
    state.git_sessions.register(
        cap.clone(),
        GitSessionEntry {
            upstream_url: body.repo_url.clone(),
            token: body.git_token.clone(),
            expires_at,
        },
    );

    // Append to the in-mem registry (keep delete/snapshot 404-gating correct).
    // A REUSED cap is already registered there (create/readopt/prior add_repo
    // put it in) — appending again would only duplicate the row.
    if !reused_prior_cap {
        state.workspaces.append_cap(&uuid, cap.clone());
    }

    // Append to the durable WorkspaceRecord (cap + branch) so a supervisor
    // restart re-registers this repo too. Best-effort persist (live in-mem still
    // holds it). Load-modify-save the existing record. A REUSED cap's row is
    // already in the record (that is where it was reused FROM) — skip the append
    // so a re-add never duplicates rows.
    let repo_stem = cap_repo_basename(&body.repo_url);
    let repo_file = format!("{repo_stem}.url");
    if !reused_prior_cap {
        match WorkspaceRecord::load(state.orchestrator.runner_dir(), &uuid) {
            Ok(Some(mut rec)) => {
                rec.caps.push(WorkspaceCap {
                    cap: cap.clone(),
                    repo_url: body.repo_url.clone(),
                });
                rec.branches.push(body.branch.clone());
                rec.last_activity_unix = now_unix();
                if let Err(e) = rec.save(state.orchestrator.runner_dir()) {
                    tracing::warn!(workspace_uuid = %uuid, error = %e, "add_repo: failed to persist appended workspace record");
                }
            }
            Ok(None) => {
                tracing::warn!(workspace_uuid = %uuid, "add_repo: no durable workspace record to append (in-mem only)");
            }
            Err(e) => {
                tracing::warn!(workspace_uuid = %uuid, error = %e, "add_repo: could not load workspace record to append");
            }
        }
    }

    // MERGE the new cap-file into the runner's persisted env (preserving the
    // existing repos + authkeys/forge-admin caps) — this is the env the COLD
    // respawn re-bakes into `/run/tabbify/caps/`.
    let mut merged_env = runner.extra_env.clone().unwrap_or_default();
    merge_cap_into_env(&mut merged_env, &repo_file, &git_remote);

    // BACKFILL the forge-admin creds + endpoint env (§12 S1/S2). A workspace
    // provisioned BEFORE its account's forge org existed has NO `forge-admin.token`
    // in its persisted `CAP_FILES_ENV`, so every forge op fails "forge not
    // provisioned" forever (there is no other re-thread path). When the node
    // threads the resolved creds on THIS add_repo, MERGE them into the cap-file map
    // (the SAME off-env channel the create path uses) so the cold respawn below
    // re-bakes the 0600 `forge-admin.token` cap-file, and set/refresh the forge
    // endpoint env (`TABBIFY_FORGE_URL`/`TABBIFY_FORGE_ORG`) the broker's
    // `ForgeCfg::from_env` also needs — proxy-rewriting the URL to the tap gateway
    // exactly as create does (the IPv4-only guest cannot route the raw v6 ULA).
    // Absent (an account with no forge org / a pre-fix node) ⇒ untouched (unchanged
    // behavior). The creds are a credential and are NEVER logged.
    if let Some(tok) = &body.forge_admin_token {
        merge_cap_into_env(&mut merged_env, "forge-admin.token", tok);
        // Fires ONLY on the backfill path (a workspace whose forge org did not
        // exist at provision time). The token itself is a credential — NEVER
        // logged; presence + destination workspace only.
        tracing::info!(
            workspace_uuid = %uuid,
            "add_repo: backfilling forge-admin.token into workspace cap-files (value never logged)"
        );
    }
    let forge_gateway = state
        .forge_proxy_enabled
        .then(|| crate::api::forge_proxy_gateway_url(&host_ip));
    if let Err(e) = insert_forge_env(
        &mut merged_env,
        &body.forge_url,
        &body.forge_org,
        forge_gateway.as_deref(),
    ) {
        tracing::error!(workspace_uuid = %uuid, error = %e, "refusing to bake a raw forge ULA: forge-proxy gateway unavailable");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("forge env: {e}") })),
        )
            .into_response();
    }

    // DIAGNOSTIC (keys/presence ONLY — cap-file VALUES are URLs/secrets, NEVER
    // logged): the new repo cap-file being merged + the FULL post-merge cap-file
    // key set (so a lost authkeys/forge cap after a respawn is visible), plus
    // whether the forge endpoint env was set. This is the add_repo twin of the
    // create-path diagnostic.
    let merged_cap_keys: Vec<String> = merged_env
        .get(CAP_FILES_ENV)
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.as_object().map(|o| o.keys().cloned().collect()))
        .unwrap_or_default();
    tracing::info!(
        workspace_uuid = %uuid,
        repo_file = %repo_file,
        cap_files = ?merged_cap_keys,
        forge_admin_token = body.forge_admin_token.is_some(),
        forge_url_set = body.forge_url.is_some(),
        forge_org_set = body.forge_org.is_some(),
        forge_proxy_enabled = state.forge_proxy_enabled,
        "add_repo: merged repo cap into workspace env + set forge endpoint (keys/presence only; values never logged)"
    );

    // ASYNC respawn (202): a workspace add_repo is a rare, human-initiated op.
    // STOP the live runner then COLD re-deploy with the merged env so the runner
    // re-bakes the FULL cap-file set (a warm swap re-bakes the OLD spawn-time
    // env). `stop_app` preserves the record; `deploy_app`'s cold path reads the
    // passed `extra_env` and writes a fresh `stopped:false` record.
    let net = DeployNetwork {
        network: runner.network.clone(),
        runner_join_token: runner.runner_join_token.clone(),
    };
    let bg = state.clone();
    let app_uuid = uuid.clone();
    let cap_for_bg = cap.clone();
    tokio::spawn(async move {
        if let Err(e) = bg.orchestrator.stop_app(&app_uuid).await {
            tracing::warn!(workspace_uuid = %app_uuid, error = %e, "add_repo: stop before respawn failed (continuing)");
        }
        match bg
            .orchestrator
            .deploy_app(&app_uuid, &image_ref, None, None, net, Some(&merged_env), None)
            .await
        {
            Ok(_) => tracing::info!(workspace_uuid = %app_uuid, "add_repo: workspace respawned with new repo cap (async)"),
            Err(e) => {
                // The new cap is durably recorded; a later respawn/ensure still
                // picks it up. Revoke the freshly-minted git-session cap so a
                // failed respawn does not leak a live token (the record keeps the
                // cap row; the node's token sweep re-registers on the next adopt).
                tracing::warn!(workspace_uuid = %app_uuid, error = %e, "add_repo: respawn failed (cap recorded; will converge on next ensure)");
                bg.git_sessions.revoke(&cap_for_bg);
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(AddRepoResult {
            workspace_uuid: uuid,
            repo: repo_stem,
            git_remote,
        }),
    )
        .into_response()
}

/// `POST /v1/workspaces/{uuid}/forge-creds` request body (P1-4 forge auto-heal).
///
/// Backfills the forge-admin credential + endpoint env into an EXISTING
/// workspace WITHOUT adding a repo. A workspace provisioned BEFORE its account's
/// forge org existed has no `/run/tabbify/caps/forge-admin.token`, so its broker
/// fails every forge op with "forge not provisioned" — and the ONLY existing
/// re-thread path is `add_repo`, forcing an agent into a confusing add-a-dummy-
/// repo dance. This creds-only channel lets the node (which resolved the org's
/// admin creds) heal the workspace directly. Fields mirror the forge fields of
/// [`AddRepoBody`]; `forge_admin_token` is REQUIRED (a creds-only heal without
/// it is a no-op).
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct ForgeCredsBody {
    /// Forge-admin token (§12 S1/S2): merged into the persisted `CAP_FILES_ENV`
    /// as `forge-admin.token`, re-baked to `/run/tabbify/caps/forge-admin.token`
    /// (0600, broker-uid) on the cold respawn. A credential — NEVER logged.
    pub forge_admin_token: String,
    /// Mesh-internal Forgejo base URL — injected as `TABBIFY_FORGE_URL` (rewritten
    /// to the tap-gateway proxy when the host-side forge-proxy is enabled).
    /// NON-secret. `None` ⇒ the env key is left untouched.
    #[serde(default)]
    pub forge_url: Option<String>,
    /// The account's forge org slug — injected as `TABBIFY_FORGE_ORG`. NON-secret.
    /// `None` ⇒ the env key is left untouched.
    #[serde(default)]
    pub forge_org: Option<String>,
}

/// `POST /v1/workspaces/{uuid}/forge-creds` — backfill forge-admin creds into an
/// existing workspace + cold-respawn so the broker's next op finds them (P1-4).
///
/// The creds-only twin of [`add_workspace_repo`]'s forge-backfill block: NO repo
/// is registered — the only effect is re-baking the `forge-admin.token` cap-file
/// (via [`merge_cap_into_env`]) + setting the forge endpoint env (via
/// [`insert_forge_env`]), then a cold respawn. Returns 202 (respawn is async);
/// 404 when the workspace / its artifact is unknown. Not in OpenAPI (a
/// mesh-internal node↔supervisor heal channel).
#[tracing::instrument(skip(state, body), fields(workspace_uuid = %uuid))]
pub async fn forge_creds_backfill(
    State(state): State<SharedState>,
    Path(uuid): Path<String>,
    Json(body): Json<ForgeCredsBody>,
) -> Response {
    // Registry gate: only a workspace this supervisor hosts can be healed
    // (mirror add_repo/snapshot/delete) — a stray uuid is a 404.
    if state.workspaces.caps_of(&uuid).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    }

    // Need the durable RunnerHandle for the `image_ref` (to derive the SAME tap
    // host_ip AND pin the respawn) and the persisted `extra_env` (the create-time
    // CAP_FILES_ENV map that also holds the authkeys/repo caps we must preserve).
    let runner = match RunnerHandle::load(state.orchestrator.runner_dir(), &uuid) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("workspace {uuid} has no runner record") })),
            )
                .into_response();
        }
        Err(e) => {
            tracing::warn!(workspace_uuid = %uuid, error = %e, "forge-creds: could not load runner record");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("could not load workspace {uuid}: {e}") })),
            )
                .into_response();
        }
    };
    let Some(image_ref) = runner.image_ref.clone() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} has no image_ref to respawn") })),
        )
            .into_response();
    };

    // MERGE the forge-admin cred into the persisted cap-file map + set the forge
    // endpoint env — exactly like add_repo, minus any repo registration. The
    // token is a credential and is NEVER logged (presence/destination only). The
    // URL is proxy-rewritten to the tap gateway exactly as create/add_repo do
    // (the IPv4-only guest cannot route the raw v6 mesh ULA the node supplies).
    let host_ip = derive_dev_fc_host_ip(&uuid, &image_ref, &state.tap_subnet);
    let mut merged_env = runner.extra_env.clone().unwrap_or_default();
    merge_cap_into_env(&mut merged_env, "forge-admin.token", &body.forge_admin_token);
    let forge_gateway = state
        .forge_proxy_enabled
        .then(|| crate::api::forge_proxy_gateway_url(&host_ip));
    if let Err(e) = insert_forge_env(
        &mut merged_env,
        &body.forge_url,
        &body.forge_org,
        forge_gateway.as_deref(),
    ) {
        tracing::error!(workspace_uuid = %uuid, error = %e, "refusing to bake a raw forge ULA: forge-proxy gateway unavailable");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("forge env: {e}") })),
        )
            .into_response();
    }

    // DIAGNOSTIC (keys/presence ONLY — values are secrets/URLs, NEVER logged):
    // the full post-merge cap-file key set (so a lost authkeys/repo cap is
    // visible) + whether the forge endpoint env was set.
    let merged_cap_keys: Vec<String> = merged_env
        .get(CAP_FILES_ENV)
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.as_object().map(|o| o.keys().cloned().collect()))
        .unwrap_or_default();
    tracing::info!(
        workspace_uuid = %uuid,
        cap_files = ?merged_cap_keys,
        forge_url_set = body.forge_url.is_some(),
        forge_org_set = body.forge_org.is_some(),
        forge_proxy_enabled = state.forge_proxy_enabled,
        "forge-creds: backfilling forge-admin.token into workspace cap-files + forge endpoint env (values never logged)"
    );

    // ASYNC cold respawn (202): STOP the live runner then COLD re-deploy with the
    // merged env so the runner re-bakes the FULL cap-file set (a warm swap would
    // keep the OLD spawn-time env, missing the cred). `stop_app` preserves the
    // record; `deploy_app`'s cold path reads the passed `extra_env`.
    let net = DeployNetwork {
        network: runner.network.clone(),
        runner_join_token: runner.runner_join_token.clone(),
    };
    let bg = state.clone();
    let app_uuid = uuid.clone();
    tokio::spawn(async move {
        if let Err(e) = bg.orchestrator.stop_app(&app_uuid).await {
            tracing::warn!(workspace_uuid = %app_uuid, error = %e, "forge-creds: stop before respawn failed (continuing)");
        }
        match bg
            .orchestrator
            .deploy_app(&app_uuid, &image_ref, None, None, net, Some(&merged_env), None)
            .await
        {
            Ok(_) => tracing::info!(workspace_uuid = %app_uuid, "forge-creds: workspace respawned with forge cred (async)"),
            Err(e) => tracing::warn!(workspace_uuid = %app_uuid, error = %e, "forge-creds: respawn failed (cred recorded in env; will converge on next ensure)"),
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(json!({ "workspace_uuid": uuid, "healed": "forge-admin.token" })),
    )
        .into_response()
}

/// `POST /v1/workspaces/{uuid}/stop` — PAUSE a workspace VM (the warm-restore
/// twin of `delete`). Shuts the runner down while PRESERVING its durable record
/// (`image_ref`/`extra_env`/`manifest_toml`/`runner_join_token`) so a later
/// `create_workspace` / deploy warm-restores it. NOT a purge — caps stay
/// registered, the record stays on disk; only the live FC is reaped.
///
/// The node calls this AFTER taking a warm snapshot (`snapshot_when_indexed`) so
/// the paused VM can be restored with a warm LSP index. A workspace this
/// supervisor does not host is a 404.
#[utoipa::path(
    post,
    path = "/v1/workspaces/{uuid}/stop",
    params(("uuid" = String, Path, description = "Workspace uuid")),
    responses(
        (status = 200, description = "Workspace stopped (record preserved for warm restore)"),
        (status = 404, description = "Workspace not found", body = crate::api::ErrorResponse),
        (status = 500, description = "Stop failed", body = crate::api::ErrorResponse),
    ),
)]
#[tracing::instrument(skip(state), fields(workspace_uuid = %uuid))]
pub async fn stop_workspace(State(state): State<SharedState>, Path(uuid): Path<String>) -> Response {
    // Registry gate (mirror snapshot/delete): only stop a workspace this
    // supervisor hosts.
    if state.workspaces.caps_of(&uuid).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("workspace {uuid} not found") })),
        )
            .into_response();
    }
    // `stop_app` marks the record stopped + reaps the FC, PRESERVING the deploy
    // artifact (image_ref/extra_env) for a warm restore. It tolerates a missing
    // live runner (already gone) and only errors on a malformed uuid.
    match state.orchestrator.stop_app(&uuid).await {
        Ok(()) => Json(json!({ "stopped": true })).into_response(),
        Err(e) => {
            tracing::warn!(workspace_uuid = %uuid, error = %e, "workspace stop failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("stop failed: {e}") })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
#[path = "workspaces_tests.rs"]
mod tests;
