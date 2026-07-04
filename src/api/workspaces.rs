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

/// MERGE a new `<repo>.url` cap-file entry into the persisted [`CAP_FILES_ENV`]
/// JSON map in an existing workspace's `extra_env`, PRESERVING every other
/// cap-file entry already baked in (the other repos' URLs, `forge-admin.token`,
/// `authkeys.cap`). This is the additive `add_repo` respawn channel: the
/// supervisor never persists the authorized-key / forge-admin token in the
/// durable [`WorkspaceRecord`], but they DO live in the runner's persisted
/// `extra_env` (the create-time `CAP_FILES_ENV` map) — so merging into that map
/// (instead of rebuilding from the record) is what lets a respawn re-bake the
/// FULL cap-file set without losing the authkeys/forge secrets. A record with no
/// prior `CAP_FILES_ENV` starts a fresh single-entry map. All non-cap env keys
/// (the workspace marker, user-id, authorized-key) are untouched.
///
/// `repo_file` is the cap-file name (`"<stem>.url"`, from [`cap_repo_basename`]);
/// `git_remote` is the TOKENLESS git-proxy URL (no secret — the token stays
/// host-side in `GitSessions`). A malformed prior value (not a JSON object) is
/// replaced by a fresh single-entry map rather than propagating corruption.
fn merge_cap_into_env(
    extra_env: &mut HashMap<String, String>,
    repo_file: &str,
    git_remote: &str,
) {
    let mut cap_files: serde_json::Map<String, serde_json::Value> = extra_env
        .get(CAP_FILES_ENV)
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    cap_files.insert(
        repo_file.to_owned(),
        serde_json::Value::String(git_remote.to_owned()),
    );
    extra_env.insert(
        CAP_FILES_ENV.to_owned(),
        serde_json::Value::Object(cap_files).to_string(),
    );
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
/// FORGE-PROXY REWRITE: the node passes `forge_url` as the forge's RAW v6 mesh
/// ULA (`http://[fd5a:…]:8730`). A workspace FC guest is IPv4-only on its /30 tap
/// and cannot route v6, so when the host-side forge-proxy is enabled the caller
/// passes `forge_proxy_gateway` = the guest's OWN tap-gateway proxy URL
/// (`http://{host_ip}:FORGE_PROXY_IPV4_PORT`, from
/// [`crate::api::forge_proxy_gateway_url`]) and we inject THAT instead — the L4
/// forward relays it to the forge over the mesh. With no proxy configured
/// (`forge_proxy_gateway == None`) the node value is passed through unchanged
/// (today's behavior). The ORG slug is NEVER rewritten — only the URL host:port.
fn insert_forge_env(
    extra_env: &mut HashMap<String, String>,
    forge_url: &Option<String>,
    forge_org: &Option<String>,
    forge_proxy_gateway: Option<&str>,
) {
    // The URL: when the host-side forge-proxy is enabled, the guest-facing value
    // is the tap-gateway proxy (the raw v6 ULA is unreachable from the IPv4-only
    // FC); otherwise the node value passes through unchanged. Only injected when
    // the node supplied a forge_url (no forge → no key → honest "unconfigured").
    if forge_url.is_some() {
        let url = match forge_proxy_gateway {
            Some(gw) => gw.to_owned(),
            // Safe: guarded by `forge_url.is_some()` above.
            None => forge_url.clone().unwrap_or_default(),
        };
        extra_env.insert("TABBIFY_FORGE_URL".to_owned(), url);
    }
    // The ORG slug rides the env channel untouched (never rewritten).
    if let Some(org) = forge_org {
        extra_env.insert("TABBIFY_FORGE_ORG".to_owned(), org.clone());
    }
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
/// resolves the repo exactly like create, then sends this single-repo body).
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

    // §12 S6: the authorized-keys cap. Generate a fresh unguessable token, write
    // it into the FC as the off-env cap-file `authkeys.cap` (the runner writes it
    // 0600, broker-uid — so the AGENT uid cannot read it), and ALSO return it to
    // node so node can authorize its `[ula]:8732/v1/authorized-keys` add-key
    // POSTs. The broker validates incoming :8732 requests against this exact
    // token; an unauthenticated (agent) request 401s. Cap-file channel ONLY —
    // never an env var, never the agent's reach.
    let authkeys_cap = insert_authkeys_cap(&ws_uuid, &mut cap_files);

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
    insert_forge_env(
        &mut extra_env,
        &body.forge_url,
        &body.forge_org,
        forge_gateway.as_deref(),
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

    // Register the NEW repo's git-proxy cap host-side (the SAME shared GitSessions
    // the create path uses), so the respawned VM can reach the remote from boot.
    let host_ip = derive_dev_fc_host_ip(&uuid, &image_ref, &state.tap_subnet);
    let cap = generate_cap(&uuid, &body.repo_url);
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
    state.workspaces.append_cap(&uuid, cap.clone());

    // Append to the durable WorkspaceRecord (cap + branch) so a supervisor
    // restart re-registers this repo too. Best-effort persist (live in-mem still
    // holds it). Load-modify-save the existing record.
    let repo_stem = cap_repo_basename(&body.repo_url);
    let repo_file = format!("{repo_stem}.url");
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

    // MERGE the new cap-file into the runner's persisted env (preserving the
    // existing repos + authkeys/forge-admin caps) — this is the env the COLD
    // respawn re-bakes into `/run/tabbify/caps/`.
    let mut merged_env = runner.extra_env.clone().unwrap_or_default();
    merge_cap_into_env(&mut merged_env, &repo_file, &git_remote);

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
