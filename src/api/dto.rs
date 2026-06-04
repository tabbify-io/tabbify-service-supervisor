//! Response DTOs for the supervisor control API.
//!
//! These types describe the JSON shapes the handlers in [`super::handlers`]
//! emit so they can be referenced from `#[utoipa::path]` annotations. The
//! handlers themselves still return ad-hoc `serde_json::Value` — the DTOs are
//! doc-only and MUST stay in sync with the actual JSON keys produced by
//! `summary_json` / `running_json` / `present_json` / `health` / `stop_app` /
//! `purge_app`.

use serde::Serialize;
use utoipa::ToSchema;

/// `GET /health` body.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthResponse {
    /// Always `"ok"` when the supervisor is serving.
    #[schema(example = "ok")]
    pub status: String,
    /// Stable-ish supervisor id (peer id, or a local placeholder w/o mesh).
    #[schema(example = "0191e7c2-1111-7222-8333-444455556666")]
    pub supervisor_id: String,
    /// This supervisor's control ULA (peer-ULA).
    #[schema(example = "fd5a:1f00:0:3::1")]
    pub ula: String,
    /// Running binary's release version (`build.rs`-embedded) — readiness.
    #[schema(example = "1.4.0")]
    pub version: String,
    /// Whether this host can run Firecracker microVMs (`/dev/kvm` present).
    pub firecracker: bool,
    /// Whether this host can run Docker containers (daemon reachable).
    pub docker: bool,
}

/// `GET /v1/about` body — self-identification for the self-update control plane.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AboutResponse {
    /// Running binary's release version (`build.rs`-embedded).
    #[schema(example = "1.4.0")]
    pub version: String,
    /// Coordinator-assigned peer id (or a local placeholder w/o mesh).
    #[schema(example = "0191e7c2-1111-7222-8333-444455556666")]
    pub peer_id: String,
    /// `"joined"` when bound on a mesh ULA, `"no_mesh"` otherwise.
    #[schema(example = "joined")]
    pub mesh_status: String,
    /// Seconds since this process started serving.
    pub uptime_secs: u64,
}

/// One row of `GET /v1/apps` — a snapshot of one app in the live runner fleet.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppPresence {
    /// App UUID (v7, string form).
    #[schema(example = "0191e7c2-0000-7000-8000-000000000001")]
    pub uuid: String,
    /// The app's deterministic mesh ULA.
    #[schema(example = "fd5a:1f02:abcdef::1")]
    pub app_ula: String,
    /// Lifecycle state: `"running"` if the per-app runner answers its socket,
    /// otherwise `"stopped"`.
    #[schema(example = "running")]
    pub state: String,
    /// The runner serves on its OWN ULA, so the bound address is the app-ULA.
    /// `null` when the app is discovered via S3 only (no runner record).
    #[schema(example = "fd5a:1f02:abcdef::1", nullable = true)]
    pub bound_addr: Option<String>,
    /// Coarse restart lifecycle status (`"running"` / `"backoff"` / `"crashloop"`).
    #[schema(example = "running")]
    pub restart_status: String,
    /// Consecutive failure count without a stable window in between.
    pub restart_count: u32,
    /// Earliest Unix timestamp (seconds) at which a respawn is eligible.
    pub next_retry_at: u64,
}

/// Body of `GET /v1/apps`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppListResponse {
    /// Snapshots of every app the orchestrator has a runner record for.
    pub apps: Vec<AppPresence>,
}

/// Body of `POST /v1/apps/{uuid}/start|reset|deploy` — "what happened?".
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppActionResponse {
    /// Resulting state (`"running"` / `"stopped"`).
    #[schema(example = "running")]
    pub state: String,
    /// The app's deterministic mesh ULA.
    #[schema(example = "fd5a:1f02:abcdef::1")]
    pub app_ula: String,
    /// The bound address (the runner serves on its OWN ULA — the same value).
    #[schema(example = "fd5a:1f02:abcdef::1")]
    pub bound_addr: String,
    /// The runtime the caller requested as an override (D4 wire string), echoed
    /// back so a client can confirm the supervisor honored it. `None` (omitted)
    /// ⇒ the manifest default was used (D10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(example = "docker")]
    pub requested_runtime: Option<String>,
}

/// Body of `POST /v1/apps/{uuid}/stop`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppStopResponse {
    /// Always `"stopped"`.
    #[schema(example = "stopped")]
    pub state: String,
}

/// Body of `POST /v1/apps/{uuid}/purge`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppPurgeResponse {
    /// Always `"purged"`.
    #[schema(example = "purged")]
    pub state: String,
    /// The uuid that was purged.
    #[schema(example = "0191e7c2-0000-7000-8000-000000000001")]
    pub uuid: String,
}

/// Body of any error response (4xx/5xx).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn app_action_response_serializes_requested_runtime() {
        let resp = AppActionResponse {
            state: "running".to_owned(),
            app_ula: "fd5a:1f02:abcdef::1".to_owned(),
            bound_addr: "fd5a:1f02:abcdef::1".to_owned(),
            requested_runtime: Some("docker".to_owned()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(
            json.contains("\"requested_runtime\":\"docker\""),
            "got: {json}"
        );
    }

    #[test]
    fn app_action_response_omits_requested_runtime_when_none() {
        let resp = AppActionResponse {
            state: "running".to_owned(),
            app_ula: "fd5a:1f02:abcdef::1".to_owned(),
            bound_addr: "fd5a:1f02:abcdef::1".to_owned(),
            requested_runtime: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("requested_runtime"), "got: {json}");
    }
}
