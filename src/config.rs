//! Service configuration via env + command-line arguments (clap).
//!
//! Every field is `#[arg(long, env = "VAR")]` (contract §0). Defaults bake the
//! prod coordinator EIP and S3 bucket so the supervisor is zero-config on
//! Kamatera; tests override the bind addr / S3 base url to hit loopback mocks.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use uuid::Uuid;

/// Prod coordinator control-plane URL (baked default, contract §5).
pub const DEFAULT_COORDINATOR_URL: &str = "http://3.124.69.92:8888";

/// Anonymous public-read base for app artifacts (contract §2).
pub const DEFAULT_S3_BASE_URL: &str = "https://tabbify-apps.s3.eu-central-1.amazonaws.com";

/// Default control/serve bind addr. `[::]:8730` so loopback tests work without
/// a live mesh; in production the binary rebinds to `[my_ula]:8730` once the
/// joiner reports its ULA (unless `--bind` is set explicitly).
pub const DEFAULT_BIND_ADDR: &str = "[::]:8730";

/// `supervisord` configuration.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "supervisord",
    about = "Tabbify app-layer supervisor (mesh + WASM)",
    version
)]
pub struct Config {
    /// HTTP control/serve bind address. When unset, the binary binds
    /// `[my_ula]:8730` from the mesh join. Set explicitly to pin a loopback
    /// addr for local testing (e.g. `127.0.0.1:8730`).
    #[arg(long, env = "SUPERVISOR_BIND")]
    pub bind: Option<SocketAddr>,

    /// Listener port used when binding the mesh ULA (contract §5 = 8730).
    #[arg(long, env = "SUPERVISOR_PORT", default_value_t = 8730)]
    pub port: u16,

    /// Mesh coordinator control-plane URL.
    #[arg(long, env = "TABBIFY_MESH_COORDINATOR", default_value = DEFAULT_COORDINATOR_URL)]
    pub coordinator_url: String,

    /// Skip mesh join entirely and bind a plain loopback/`--bind` addr. Used
    /// for local runs/tests without root + TUN. Defaults off (join the mesh).
    #[arg(long, env = "SUPERVISOR_NO_MESH", default_value_t = false)]
    pub no_mesh: bool,

    /// Display name advertised to the coordinator.
    #[arg(long, env = "SUPERVISOR_NAME", default_value = "tabbify-supervisor")]
    pub display_name: String,

    /// Local data dir for cached app artifacts (`<data_dir>/apps/<uuid>/v<N>/`).
    #[arg(long, env = "SUPERVISOR_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// S3 base URL for anonymous artifact fetch (overridable for tests).
    #[arg(long, env = "SUPERVISOR_S3_BASE_URL", default_value = DEFAULT_S3_BASE_URL)]
    pub s3_base_url: String,

    /// Pre-register an app by UUID at boot (repeatable). `always_on` apps spawn
    /// immediately; `on_request` apps are fetched lazily on first request.
    #[arg(long = "app", value_name = "UUID")]
    pub apps: Vec<Uuid>,
}

impl Config {
    /// Parse from the environment + argv.
    #[must_use]
    pub fn from_env() -> Self {
        Config::parse()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Config::command().debug_assert();
    }

    #[test]
    fn defaults_apply() {
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert_eq!(cfg.coordinator_url, DEFAULT_COORDINATOR_URL);
        assert_eq!(cfg.s3_base_url, DEFAULT_S3_BASE_URL);
        assert_eq!(cfg.port, 8730);
        assert!(!cfg.no_mesh);
        assert!(cfg.bind.is_none());
        assert!(cfg.apps.is_empty());
        assert_eq!(cfg.data_dir, PathBuf::from("./data"));
    }

    #[test]
    fn parses_repeatable_apps_and_overrides() {
        let cfg = Config::try_parse_from([
            "supervisord",
            "--bind",
            "127.0.0.1:9999",
            "--coordinator-url",
            "http://10.0.0.1:8888",
            "--s3-base-url",
            "http://localhost:1234",
            "--no-mesh",
            "--app",
            "0191e7c2-1111-7222-8333-444455556666",
            "--app",
            "0191e7c2-2222-7222-8333-444455556666",
        ])
        .unwrap();
        assert_eq!(cfg.bind.unwrap().to_string(), "127.0.0.1:9999");
        assert_eq!(cfg.coordinator_url, "http://10.0.0.1:8888");
        assert_eq!(cfg.s3_base_url, "http://localhost:1234");
        assert!(cfg.no_mesh);
        assert_eq!(cfg.apps.len(), 2);
        assert_eq!(
            cfg.apps[0],
            Uuid::parse_str("0191e7c2-1111-7222-8333-444455556666").unwrap()
        );
    }
}
