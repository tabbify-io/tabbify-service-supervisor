//! Per-app runner configuration via env + command-line arguments (clap).
//!
//! `tabbify-runner` hosts exactly ONE app (identified by `--uuid`). Every field
//! is `#[arg(long, env = "VAR")]`, matching the `supervisord` Config style
//! (contract §0). Defaults reuse the same prod consts as [`crate::config`].

use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use uuid::Uuid;

use crate::config::{DockerConfig, FcConfig, DEFAULT_COORDINATOR_URL, DEFAULT_S3_BASE_URL};

/// Environment variable the runner reads its SCOPED node-join token from
/// (Phase-2 contract). The node mints this per-deploy and the supervisor sets it
/// on the runner's process env (never on the arg list — it is a credential).
/// Empty / unset = no token (current, backward-compatible behavior).
pub const RUNNER_JOIN_TOKEN_ENV: &str = "TABBIFY_RUNNER_JOIN_TOKEN";

/// `tabbify-runner` configuration — one runner per app instance.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "tabbify-runner",
    about = "Tabbify per-app runner (hosts exactly one app instance)",
    version
)]
pub struct RunnerConfig {
    /// UUID of the app to host (required).
    #[arg(long, env = "RUNNER_UUID")]
    pub uuid: Uuid,

    /// ULA of the parent supervisor that spawned this runner (IPv6 address
    /// string). When set the runner reports health back to the supervisor.
    #[arg(long, env = "RUNNER_PARENT")]
    pub parent: Option<String>,

    /// Unix-domain socket path for the parent control channel.
    #[arg(
        long,
        env = "RUNNER_CONTROL_SOCK",
        default_value = "/run/tabbify/runners/runner.sock"
    )]
    pub control_sock: PathBuf,

    /// Skip mesh join; bind plain loopback/`--bind`. Used for local
    /// runs/tests without root + TUN.
    #[arg(long, env = "RUNNER_NO_MESH", default_value_t = false)]
    pub no_mesh: bool,

    /// HTTP bind address override. When unset the runner derives its bind
    /// address from the mesh ULA.
    #[arg(long, env = "RUNNER_BIND")]
    pub bind: Option<SocketAddr>,

    /// Mesh coordinator control-plane URL.
    #[arg(long, env = "TABBIFY_MESH_COORDINATOR", default_value = DEFAULT_COORDINATOR_URL)]
    pub coordinator_url: String,

    /// Explicit mesh relay endpoint (DERP-style). When set, the runner connects
    /// its relay over THIS url (e.g. `wss://relay.tabbify.io/v1/mesh/relay`)
    /// instead of deriving `ws://` from the coordinator URL — required to reach
    /// the relay through corporate proxies/firewalls. The supervisor passes this
    /// via `--mesh-relay-url`; the `env=` fallback also picks up an inherited
    /// `TABBIFY_MESH_RELAY_URL`. None ⇒ derive from coordinator_url.
    #[arg(long = "mesh-relay-url", env = "TABBIFY_MESH_RELAY_URL")]
    pub relay_url: Option<String>,

    /// Declare this runner **relay-only**: it has NO reachable direct endpoint
    /// (it shares the host's NAT/firewall with the supervisor). When `true` the
    /// coordinator never synthesizes a reflexive direct endpoint for the runner
    /// and never emits a hole-punch directive for any pair involving it, so the
    /// runner's WG handshake completes single-sided over the relay. The supervisor
    /// forwards this as the bare `--mesh-relay-only` flag; the `env=` fallback also
    /// picks up an inherited `TABBIFY_MESH_RELAY_ONLY`. `false` (the default)
    /// keeps direct + hole-punch traversal.
    #[arg(
        long = "mesh-relay-only",
        env = "TABBIFY_MESH_RELAY_ONLY",
        default_value_t = false
    )]
    pub relay_only: bool,

    /// S3 base URL for anonymous artifact fetch (overridable for tests).
    #[arg(long, env = "RUNNER_S3_BASE_URL", default_value = DEFAULT_S3_BASE_URL)]
    pub s3_base_url: String,

    /// Local data dir for cached app artifacts.
    #[arg(long, env = "RUNNER_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// OCI image ref of a previously-deployed version. When set the runner
    /// applies it to the manifest's docker `registry_ref` before building the
    /// initial runtime, so a supervisor-driven respawn comes up on the deployed
    /// version (a `docker pull <ref>` instead of a source build). `None` =
    /// build from the S3 manifest as usual.
    #[arg(long, env = "RUNNER_IMAGE_REF")]
    pub image_ref: Option<String>,

    /// The Tabbify-MANAGED `tabbify.toml` (raw TOML) for a connect-repo deploy,
    /// forwarded by the supervisor via the `RUNNER_MANIFEST_TOML` environment
    /// variable (an env, not an arg: the toml is multi-line and would clutter
    /// `ps`). When set and the app has NO S3 manifest (the BUILD-pipeline path),
    /// its `[runtime]`/`[routes]` drive the synthesized manifest. `None` keeps
    /// the hardcoded FC defaults.
    #[arg(long, env = "RUNNER_MANIFEST_TOML")]
    pub manifest_toml: Option<String>,

    /// Path to a JSON build-spec file. When set the runner operates in
    /// one-shot builder mode: it reads the [`BuildJob`], runs the build
    /// pipeline, prints the [`ArtifactRef`] JSON to stdout, and exits.
    /// The mesh join and serve-forever path are skipped entirely.
    ///
    /// [`BuildJob`]: crate::runner::build::BuildJob
    /// [`ArtifactRef`]: crate::runner::build::ArtifactRef
    #[arg(long, env = "RUNNER_BUILD_SPEC")]
    pub build_spec: Option<PathBuf>,

    /// Listener port used when binding the mesh ULA.
    #[arg(long, env = "RUNNER_PORT", default_value_t = 8730)]
    pub port: u16,

    /// Tenant network slug (Phase-2 contract). When set, the runner joins the
    /// mesh scoped to this network — it advertises `tag:net-<slug>` and the
    /// coordinator (when validating the scoped join token) stamps it
    /// `network=<slug>`, `tags=["tag:net-<slug>"]`. The supervisor passes this
    /// via `--network`. `None` ⇒ today's unscoped join.
    #[arg(long, env = "RUNNER_NETWORK")]
    pub network: Option<String>,

    /// Scoped node-join JWT for THIS app's runner (Phase-2 contract). Read ONLY
    /// from the `TABBIFY_RUNNER_JOIN_TOKEN` environment variable (no CLI flag —
    /// it is a credential and must not appear in `ps`/process args); the
    /// supervisor sets it on the runner's process env. Sent to the coordinator
    /// as `Authorization: Bearer <token>` on register. `None` ⇒ tokenless join
    /// (current behavior, valid against a coordinator without `AUTH_URL`).
    #[arg(skip)]
    pub runner_join_token: Option<String>,

    /// Deploy-time extra `KEY=VALUE` environment variables baked into the guest
    /// `/init`. Set by the supervisor via the `RUNNER_EXTRA_ENV` environment
    /// variable as a JSON-encoded object (`{"KEY":"VALUE"}`); never a CLI flag
    /// (the values may be credentials). Decoded once at startup and merged AFTER
    /// the OCI image's `config.Env` so deploy-time entries win on key collision.
    /// `None` ⇒ no extra env (guest gets exactly the OCI image's vars).
    #[arg(long, env = "RUNNER_EXTRA_ENV")]
    pub extra_env_json: Option<String>,

    /// Firecracker microVM runtime configuration.
    #[command(flatten)]
    pub firecracker: FcConfig,

    /// Docker container runtime configuration.
    #[command(flatten)]
    pub docker: DockerConfig,
}

/// Read the scoped runner-join token from the [`RUNNER_JOIN_TOKEN_ENV`]
/// environment variable, treating a blank value as absent (no blank bearer).
///
/// Kept out of clap's `env=` derivation so the credential is never reflected in
/// `--help` / usage and never becomes a CLI flag. The binary calls this after
/// `parse()` to populate [`RunnerConfig::runner_join_token`]; a unit test can
/// drive it directly.
#[must_use]
pub fn runner_join_token_from_env() -> Option<String> {
    std::env::var(RUNNER_JOIN_TOKEN_ENV)
        .ok()
        .filter(|t| !t.trim().is_empty())
}

/// Decode the `RUNNER_EXTRA_ENV` JSON string into a `HashMap<String, String>`.
/// Returns `None` when the string is absent or blank; logs a warning and returns
/// `None` on a parse failure (a broken JSON must never wedge the runner).
#[must_use]
pub fn parse_extra_env(json: Option<&str>) -> Option<std::collections::HashMap<String, String>> {
    let s = json?.trim();
    if s.is_empty() {
        return None;
    }
    match serde_json::from_str::<std::collections::HashMap<String, String>>(s) {
        Ok(map) => Some(map),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "RUNNER_EXTRA_ENV is not valid JSON (ignoring); guest will use only the OCI image env"
            );
            None
        }
    }
}

impl RunnerConfig {
    /// Parse from argv + env, then resolve the scoped runner-join token from
    /// [`RUNNER_JOIN_TOKEN_ENV`] (it is intentionally NOT a clap field). This is
    /// the binary's entry point so the token is wired exactly once.
    #[must_use]
    pub fn parse_with_env() -> Self {
        let mut cfg = <Self as Parser>::parse();
        cfg.runner_join_token = runner_join_token_from_env();
        cfg
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        RunnerConfig::command().debug_assert();
    }

    #[test]
    fn parses_required_and_optional_fields() {
        let c = RunnerConfig::try_parse_from([
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
            "--parent",
            "fd5a:1f00:0:3::1",
            "--control-sock",
            "/run/tabbify/runners/x.sock",
            "--no-mesh",
        ])
        .unwrap();
        assert_eq!(c.uuid.to_string(), "0191e7c2-1111-7222-8333-444455556666");
        assert_eq!(c.parent.as_deref(), Some("fd5a:1f00:0:3::1"));
        assert_eq!(c.control_sock, PathBuf::from("/run/tabbify/runners/x.sock"));
        assert!(c.no_mesh);
    }

    #[test]
    fn defaults_apply() {
        let c = RunnerConfig::try_parse_from([
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
        ])
        .unwrap();
        assert_eq!(c.coordinator_url, DEFAULT_COORDINATOR_URL);
        assert_eq!(c.s3_base_url, DEFAULT_S3_BASE_URL);
        assert_eq!(c.port, 8730);
        assert!(!c.no_mesh);
        assert!(c.bind.is_none());
        assert!(c.parent.is_none());
        assert_eq!(c.data_dir, PathBuf::from("./data"));
        // Phase-2 fields default to unscoped/tokenless.
        assert!(c.network.is_none());
        assert!(c.runner_join_token.is_none());
        // Relay-only defaults off (direct + hole-punch traversal).
        assert!(!c.relay_only);
    }

    #[test]
    fn relay_only_flag_parses() {
        // The bare `--mesh-relay-only` flag (forwarded by the supervisor) sets
        // the runner's relay_only bool true.
        let c = RunnerConfig::try_parse_from([
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
            "--mesh-relay-only",
        ])
        .unwrap();
        assert!(c.relay_only);
    }

    #[test]
    fn parses_network_flag() {
        let c = RunnerConfig::try_parse_from([
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
            "--network",
            "n_jpegxik72nng",
        ])
        .unwrap();
        assert_eq!(c.network.as_deref(), Some("n_jpegxik72nng"));
    }

    /// The scoped runner-join token is NOT a clap flag (it is a credential):
    /// attempting to pass it as `--runner-join-token` is a usage error.
    #[test]
    fn runner_join_token_is_not_a_cli_flag() {
        let result = RunnerConfig::try_parse_from([
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
            "--runner-join-token",
            "secret",
        ]);
        assert!(
            result.is_err(),
            "--runner-join-token must not be a parseable flag (credential rides the env)"
        );
    }
}
