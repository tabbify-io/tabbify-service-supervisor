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

    /// Firecracker microVM runtime configuration (only consulted when hosting a
    /// `firecracker` app on a KVM-capable Linux host).
    #[command(flatten)]
    pub firecracker: FcConfig,
}

/// Default firecracker binary (looked up on `$PATH`).
pub const DEFAULT_FC_BIN: &str = "firecracker";

/// Default guest kernel image (operator-provisioned on the host).
pub const DEFAULT_FC_KERNEL: &str = "/opt/tabbify/vmlinux";

/// Default per-VM tap subnet; each VM is carved a /30 out of this range.
pub const DEFAULT_FC_TAP_SUBNET: &str = "172.31.0.0/16";

/// Firecracker microVM runtime configuration. Only consulted when the supervisor
/// is asked to host an app whose `runtime.type == "firecracker"` on a host with
/// `/dev/kvm`; ignored everywhere else (so a WASM-only supervisor never needs
/// any of these).
#[derive(Debug, Clone, Parser)]
pub struct FcConfig {
    /// Path to the `firecracker` binary.
    #[arg(long = "firecracker-bin", env = "SUPERVISOR_FC_BIN", default_value = DEFAULT_FC_BIN)]
    pub bin: String,

    /// Guest kernel image used when a manifest omits `runtime.kernel`.
    #[arg(long = "firecracker-kernel", env = "SUPERVISOR_FC_KERNEL", default_value = DEFAULT_FC_KERNEL)]
    pub kernel: String,

    /// vCPU count for each microVM.
    #[arg(
        long = "firecracker-vcpus",
        env = "SUPERVISOR_FC_VCPUS",
        default_value_t = 1
    )]
    pub vcpus: u32,

    /// Subnet from which per-VM /30 tap links are allocated (CIDR).
    #[arg(long = "firecracker-tap-subnet", env = "SUPERVISOR_FC_TAP_SUBNET", default_value = DEFAULT_FC_TAP_SUBNET)]
    pub tap_subnet: String,

    /// Port the app's HTTP server listens on inside the guest.
    #[arg(
        long = "firecracker-app-port",
        env = "SUPERVISOR_FC_APP_PORT",
        default_value_t = 8080
    )]
    pub app_port: u16,
}

impl Default for FcConfig {
    /// The same defaults clap bakes — handy for tests + for an
    /// [`crate::registry::AppRegistry`] that has no firecracker apps.
    fn default() -> Self {
        Self {
            bin: DEFAULT_FC_BIN.to_owned(),
            kernel: DEFAULT_FC_KERNEL.to_owned(),
            vcpus: 1,
            tap_subnet: DEFAULT_FC_TAP_SUBNET.to_owned(),
            app_port: 8080,
        }
    }
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
    use clap::CommandFactory;

    use super::*;

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
    fn firecracker_defaults_apply() {
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert_eq!(cfg.firecracker.bin, DEFAULT_FC_BIN);
        assert_eq!(cfg.firecracker.kernel, DEFAULT_FC_KERNEL);
        assert_eq!(cfg.firecracker.vcpus, 1);
        assert_eq!(cfg.firecracker.tap_subnet, DEFAULT_FC_TAP_SUBNET);
        assert_eq!(cfg.firecracker.app_port, 8080);
    }

    #[test]
    fn firecracker_default_impl_matches_clap_defaults() {
        let parsed = Config::try_parse_from(["supervisord"]).unwrap().firecracker;
        let dflt = FcConfig::default();
        assert_eq!(parsed.bin, dflt.bin);
        assert_eq!(parsed.kernel, dflt.kernel);
        assert_eq!(parsed.vcpus, dflt.vcpus);
        assert_eq!(parsed.tap_subnet, dflt.tap_subnet);
        assert_eq!(parsed.app_port, dflt.app_port);
    }

    #[test]
    fn firecracker_overrides_parse() {
        let cfg = Config::try_parse_from([
            "supervisord",
            "--firecracker-bin",
            "/usr/local/bin/firecracker",
            "--firecracker-kernel",
            "/srv/vmlinux-6.1",
            "--firecracker-vcpus",
            "4",
            "--firecracker-tap-subnet",
            "10.200.0.0/16",
            "--firecracker-app-port",
            "3000",
        ])
        .unwrap();
        assert_eq!(cfg.firecracker.bin, "/usr/local/bin/firecracker");
        assert_eq!(cfg.firecracker.kernel, "/srv/vmlinux-6.1");
        assert_eq!(cfg.firecracker.vcpus, 4);
        assert_eq!(cfg.firecracker.tap_subnet, "10.200.0.0/16");
        assert_eq!(cfg.firecracker.app_port, 3000);
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
