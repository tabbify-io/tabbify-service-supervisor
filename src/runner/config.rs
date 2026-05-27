//! Per-app runner configuration via env + command-line arguments (clap).
//!
//! `tabbify-runner` hosts exactly ONE app (identified by `--uuid`). Every field
//! is `#[arg(long, env = "VAR")]`, matching the `supervisord` Config style
//! (contract §0). Defaults reuse the same prod consts as [`crate::config`].

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use uuid::Uuid;

use crate::config::{DEFAULT_COORDINATOR_URL, DEFAULT_S3_BASE_URL, DockerConfig, FcConfig};

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

    /// Firecracker microVM runtime configuration.
    #[command(flatten)]
    pub firecracker: FcConfig,

    /// Docker container runtime configuration.
    #[command(flatten)]
    pub docker: DockerConfig,
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
    }
}
