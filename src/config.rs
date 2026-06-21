//! Service configuration via env + command-line arguments (clap).
//!
//! Every field is `#[arg(long, env = "VAR")]` (contract §0). Defaults bake the
//! prod coordinator EIP and S3 bucket so the supervisor is zero-config on
//! Kamatera; tests override the bind addr / S3 base url to hit loopback mocks.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uuid::Uuid;

/// Prod coordinator control-plane URL (baked default, contract §5).
pub const DEFAULT_COORDINATOR_URL: &str = "http://3.124.69.92:8888";

/// Prod DERP-style relay endpoint over TLS/443. Baked default applied
/// ONLY when the coordinator is the default (production) one — see
/// [`Config::effective_relay_url`]. TLS matters: corporate firewalls
/// mangle/kill plaintext `ws://:8888`, and the relay is the
/// connectivity floor, so a zero-config node must land on `wss://` out
/// of the box (previously this lived in a `/run` systemd drop-in on the
/// ThinkPad — lost on every reboot).
pub const DEFAULT_RELAY_URL: &str = "wss://relay.tabbify.io/v1/mesh/relay";

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

    /// Explicit mesh relay endpoint (DERP-style). When set, the joiner connects
    /// its relay over THIS url (e.g. `wss://relay.tabbify.io/v1/mesh/relay`)
    /// instead of deriving `ws://` from the coordinator URL — required to reach
    /// the relay through corporate proxies/firewalls that mangle plaintext ws.
    /// None ⇒ derive from coordinator_url (the default for AWS-side peers).
    #[arg(long = "mesh-relay-url", env = "TABBIFY_MESH_RELAY_URL")]
    pub relay_url: Option<String>,

    /// Declare this supervisor (and every runner it spawns) **relay-only**: the
    /// peer has NO reachable direct endpoint (it runs behind a NAT/firewall with
    /// the WireGuard UDP port dropped, reachable ONLY over its outbound DERP
    /// relay). When `true` the coordinator (a) never synthesizes a reflexive
    /// direct endpoint for this peer and (b) never emits a hole-punch directive
    /// for any pair involving it, so the WG handshake becomes single-sided and
    /// completes cleanly over the relay — eliminating the simultaneous-init
    /// thrash that otherwise stalls a relay-only ↔ NAT'd handshake. `false` (the
    /// default) keeps direct + hole-punch traversal. A plain pass-through bool
    /// (no `effective_*` baking, unlike `relay_url`).
    #[arg(
        long = "mesh-relay-only",
        env = "TABBIFY_MESH_RELAY_ONLY",
        default_value_t = false
    )]
    pub relay_only: bool,

    /// Explicit endpoint this peer **advertises to the coordinator** instead of
    /// the reflexive (public) one. Useful on LAN-local nodes that share a NAT:
    /// e.g. `10.17.21.133:51820` lets two peers on the same subnet hole-punch
    /// each other directly without going through the relay. When unset (the
    /// default) the coordinator uses the reflexive endpoint it observes on the
    /// incoming UDP register — unchanged behavior for cloud/public peers.
    #[arg(
        long = "mesh-advertise-endpoint",
        env = "TABBIFY_MESH_ADVERTISE_ENDPOINT"
    )]
    pub advertise_endpoint: Option<String>,

    /// Skip mesh join entirely and bind a plain loopback/`--bind` addr. Used
    /// for local runs/tests without root + TUN. Defaults off (join the mesh).
    #[arg(long, env = "SUPERVISOR_NO_MESH", default_value_t = false)]
    pub no_mesh: bool,

    /// Designate this supervisor a BUILD host: advertise the `builder` mesh
    /// tag so the node routes `/v1/build` jobs here. An explicit operator
    /// decision (fleet composition), never auto-detected — a build host
    /// additionally needs a reachable docker daemon + `skopeo` + `git`.
    /// Defaults off (run-only node).
    #[arg(long, env = "SUPERVISOR_BUILDER", default_value_t = false)]
    pub builder: bool,

    /// Display name advertised to the coordinator.
    #[arg(long, env = "SUPERVISOR_NAME", default_value = "tabbify-supervisor")]
    pub display_name: String,

    /// Local data dir for cached app artifacts (`<data_dir>/apps/<uuid>/v<N>/`)
    /// AND the sticky mesh identity (`<data_dir>/mesh-identity.json`). Defaults to
    /// a STABLE absolute path so a host that forgets `SUPERVISOR_DATA_DIR`
    /// (ThinkPad/NixOS/node-in-FC) still persists its identity across restarts
    /// instead of churning its pubkey. Containers/systemd already set this env.
    #[arg(long, env = "SUPERVISOR_DATA_DIR", default_value = "/var/lib/tabbify")]
    pub data_dir: PathBuf,

    /// S3 base URL for anonymous artifact fetch (overridable for tests).
    #[arg(long, env = "SUPERVISOR_S3_BASE_URL", default_value = DEFAULT_S3_BASE_URL)]
    pub s3_base_url: String,

    /// Probe entrypoint: when set, this process is an out-of-band self-update
    /// CANDIDATE. It joins the mesh with a TRANSIENT identity
    /// (`--candidate-identity-path`), runs the 3-part health gate against
    /// itself, and exits 0 (gate passed) or 1 (gate failed) — it never claims
    /// the sticky ULA and never serves traffic.
    #[arg(long = "check", env = "SUPERVISOR_CHECK", default_value_t = false)]
    pub check_mode: bool,

    /// Transient identity file the candidate (`--check`) joins with — NEVER the
    /// sticky `mesh-identity.json`. Ignored unless `--check` is set.
    #[arg(long, env = "SUPERVISOR_CANDIDATE_IDENTITY")]
    pub candidate_identity_path: Option<PathBuf>,

    /// Pre-register an app by UUID at boot (repeatable). `always_on` apps spawn
    /// immediately; `on_request` apps are fetched lazily on first request.
    #[arg(long = "app", value_name = "UUID")]
    pub apps: Vec<Uuid>,

    /// Firecracker microVM runtime configuration (only consulted when hosting a
    /// `firecracker` app on a KVM-capable Linux host).
    #[command(flatten)]
    pub firecracker: FcConfig,

    /// Docker container runtime configuration (only consulted when hosting a
    /// `docker` app on a host with a reachable Docker daemon).
    #[command(flatten)]
    pub docker: DockerConfig,

    /// Optional subcommand. With NONE the binary runs as the daemon (the default
    /// `supervisord` boot). With [`Command::SelfUpdate`] it runs the one-shot
    /// production self-update flow and exits. Optional so the existing flat
    /// invocation (`supervisord [--flags]`) and the `--check` candidate both
    /// keep working unchanged.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Top-level subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Run the health-gated production self-update flow to `--to <version>` and
    /// exit: fetch + sha256-verify the release, probe the candidate out-of-band
    /// behind the 3-part gate, and on PASS swap the symlinks + restart (the next
    /// boot's self-watchdog confirms or reverts). Replaces the legacy bash
    /// fetch/probe/swap reimplementation in the NixOS `tabbify-update` unit.
    SelfUpdate {
        /// The target release version to update to, e.g. `v1.4.0`.
        #[arg(long = "to", value_name = "VERSION")]
        to: String,
    },
    /// Roll the binary symlinks back to the newest previous-good release and exit
    /// (the crash-at-startup catch-net's remediation, spec §3.2). Reuses the
    /// audited `selfupdate::watchdog::revert_to_previous` (symlink re-point +
    /// VERSION rewrite ONLY — systemd owns the restart), stamps the reverted-from
    /// version into the VERSION `quarantine` list so the OTA poller can't re-swap
    /// the known-bad release, and is invoked by the `OnFailure=tabbify-boot-revert`
    /// unit. When there is no complete previous release (a genuine first boot):
    /// with `--reboot-on-exhausted` it consults the `RebootGuard` and reboots as a
    /// last resort, then parks; without it, it exits non-zero so the OnFailure
    /// script can distinguish "no previous (bail)" from "revert failed".
    RevertToPrevious {
        /// On an exhausted revert (no complete previous-good release), reboot the
        /// host as a last resort (guarded ≤3/hr) instead of just exiting non-zero.
        #[arg(long = "reboot-on-exhausted")]
        reboot_on_exhausted: bool,
    },
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
    /// The same defaults clap bakes — handy for tests + for a runner that has
    /// no firecracker app.
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

/// Default `docker` binary (looked up on `$PATH`).
pub const DEFAULT_DOCKER_BIN: &str = "docker";

/// Default `oras` binary (looked up on `$PATH`). Used to pull WASM OCI
/// artifacts from the mesh registry (`oras pull --plain-http <ref>`).
pub const DEFAULT_ORAS_BIN: &str = "oras";

/// Default `skopeo` binary (looked up on `$PATH`). Used by the docker build path
/// to push the built image from the local docker daemon to the mesh registry
/// (`skopeo copy docker-daemon:<tag>:latest docker://<ref>`), run supervisor-side
/// so the docker daemon never needs a mesh route.
pub const DEFAULT_SKOPEO_BIN: &str = "skopeo";

/// Docker BUILD-tool configuration: the external-CLI paths the supervisor shells
/// out to when BUILDING + PUSHING OCI images (`docker build` then `skopeo copy`
/// to the mesh registry). Docker no longer RUNS apps (an OCI image is converted
/// to ext4 and booted as a Firecracker microVM), so this holds only build-side
/// binary paths. Unlike firecracker, the docker build path is cross-platform —
/// it shells out to the `docker` CLI, which runs on macOS + Linux alike.
///
/// Also holds the `oras_bin` path, co-located here with the other external-tool
/// paths.
#[derive(Debug, Clone, Parser)]
pub struct DockerConfig {
    /// Path to the `docker` binary.
    #[arg(long = "docker-bin", env = "SUPERVISOR_DOCKER_BIN", default_value = DEFAULT_DOCKER_BIN)]
    pub docker_bin: String,

    /// Path to the `oras` binary. The registry is plain HTTP over the WireGuard
    /// overlay so the `oras` source flag is `--from-plain-http` for every
    /// `[ula]:5000` ref.
    #[arg(long = "oras-bin", env = "SUPERVISOR_ORAS_BIN", default_value = DEFAULT_ORAS_BIN)]
    pub oras_bin: String,

    /// Path to the `skopeo` binary used by the docker build path to push the
    /// built image from the local docker daemon to the mesh registry
    /// (`skopeo copy docker-daemon:<tag>:latest docker://<ref>`). Run
    /// supervisor-side (on the mesh) so the docker daemon — which has no mesh
    /// route — never talks to the registry; `--dest-tls-verify=false` is used
    /// because the mesh registry is plain HTTP over the WireGuard overlay.
    #[arg(long = "skopeo-bin", env = "SUPERVISOR_SKOPEO_BIN", default_value = DEFAULT_SKOPEO_BIN)]
    pub skopeo_bin: String,
}

impl Default for DockerConfig {
    /// The same defaults clap bakes — handy for tests + for a runner that has
    /// no docker app.
    fn default() -> Self {
        Self {
            docker_bin: DEFAULT_DOCKER_BIN.to_owned(),
            oras_bin: DEFAULT_ORAS_BIN.to_owned(),
            skopeo_bin: DEFAULT_SKOPEO_BIN.to_owned(),
        }
    }
}

impl Config {
    /// Parse from the environment + argv.
    #[must_use]
    pub fn from_env() -> Self {
        Config::parse()
    }

    /// Path to the persistent mesh identity file (`{private_key, ula}`), placed
    /// under the data dir so it survives container/process restarts when
    /// `data_dir` is a mounted volume — giving the supervisor a STABLE ULA
    /// across restarts (sticky identity) instead of the joiner's `$HOME`
    /// fallback (which is ephemeral inside a container).
    #[must_use]
    pub fn mesh_identity_path(&self) -> PathBuf {
        self.data_dir.join("mesh-identity.json")
    }

    /// The relay URL to hand to the mesh joiner (and forward to runners).
    ///
    /// Resolution order:
    /// 1. explicit `--mesh-relay-url` / `TABBIFY_MESH_RELAY_URL` — verbatim;
    /// 2. default (production) coordinator → [`DEFAULT_RELAY_URL`]
    ///    (`wss://` on 443: zero-config nodes must traverse corporate
    ///    firewalls that kill plaintext ws);
    /// 3. custom coordinator → `None` (the joiner derives
    ///    `ws(s)://{coordinator-host}/v1/mesh/relay`, the right answer for
    ///    local/dev meshes where no TLS relay exists).
    #[must_use]
    pub fn effective_relay_url(&self) -> Option<String> {
        self.relay_url.clone().or_else(|| {
            (self.coordinator_url == DEFAULT_COORDINATOR_URL).then(|| DEFAULT_RELAY_URL.to_owned())
        })
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
        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/tabbify"));
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
    fn docker_defaults_apply() {
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert_eq!(cfg.docker.docker_bin, DEFAULT_DOCKER_BIN);
        assert_eq!(cfg.docker.oras_bin, DEFAULT_ORAS_BIN);
        assert_eq!(cfg.docker.skopeo_bin, DEFAULT_SKOPEO_BIN);
    }

    #[test]
    fn docker_default_impl_matches_clap_defaults() {
        let parsed = Config::try_parse_from(["supervisord"]).unwrap().docker;
        let dflt = DockerConfig::default();
        assert_eq!(parsed.docker_bin, dflt.docker_bin);
        assert_eq!(parsed.oras_bin, dflt.oras_bin);
        assert_eq!(parsed.skopeo_bin, dflt.skopeo_bin);
    }

    #[test]
    fn docker_overrides_parse() {
        let cfg = Config::try_parse_from([
            "supervisord",
            "--docker-bin",
            "/usr/local/bin/docker",
            "--oras-bin",
            "/usr/local/bin/oras",
            "--skopeo-bin",
            "/usr/local/bin/skopeo",
        ])
        .unwrap();
        assert_eq!(cfg.docker.docker_bin, "/usr/local/bin/docker");
        assert_eq!(cfg.docker.oras_bin, "/usr/local/bin/oras");
        assert_eq!(cfg.docker.skopeo_bin, "/usr/local/bin/skopeo");
    }

    #[test]
    fn release_base_url_flag_is_not_exposed() {
        // SU-3 had promoted a `--release-base-url` flag, but nothing in this
        // crate reads `Config::release_base_url` (the self-update fetch engine
        // builds its own `SelfUpdateConfig::release_base_url`). A flag with no
        // reader is dead wiring; it stays dropped until a live consumer exists.
        assert!(
            Config::try_parse_from(["supervisord", "--release-base-url", "http://localhost:9"])
                .is_err(),
            "--release-base-url must not be a parseable flag (no reader)"
        );
        assert!(
            Config::try_parse_from(["supervisord", "--check"]).is_ok(),
            "candidate flags must still parse"
        );
    }

    #[test]
    fn mesh_identity_path_is_under_data_dir() {
        let cfg =
            Config::try_parse_from(["supervisord", "--data-dir", "/var/lib/tabbify"]).unwrap();
        assert_eq!(
            cfg.mesh_identity_path(),
            PathBuf::from("/var/lib/tabbify/mesh-identity.json")
        );
    }

    #[test]
    fn no_subcommand_runs_as_daemon() {
        // The bare invocation (and `--check`) must leave `command` as None so the
        // daemon / candidate paths are unchanged by adding the subcommand.
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert!(cfg.command.is_none());
        let chk = Config::try_parse_from(["supervisord", "--check"]).unwrap();
        assert!(chk.command.is_none());
        assert!(chk.check_mode);
    }

    #[test]
    fn parses_self_update_subcommand_with_to_version() {
        let cfg = Config::try_parse_from(["supervisord", "self-update", "--to", "v1.4.0"]).unwrap();
        match cfg.command {
            Some(Command::SelfUpdate { to }) => assert_eq!(to, "v1.4.0"),
            other => panic!("expected SelfUpdate, got {other:?}"),
        }
    }

    #[test]
    fn self_update_requires_to_version() {
        // `self-update` with no `--to` is a usage error, not a silent no-op.
        assert!(Config::try_parse_from(["supervisord", "self-update"]).is_err());
    }

    #[test]
    fn parses_revert_to_previous_subcommand() {
        // The bare subcommand: reboot-on-exhausted defaults to false (the
        // OnFailure script only opts into the reboot escalation explicitly).
        let cfg = Config::try_parse_from(["supervisord", "revert-to-previous"]).unwrap();
        match cfg.command {
            Some(Command::RevertToPrevious {
                reboot_on_exhausted,
            }) => assert!(
                !reboot_on_exhausted,
                "reboot-on-exhausted must default to false"
            ),
            other => panic!("expected RevertToPrevious, got {other:?}"),
        }
    }

    #[test]
    fn parses_revert_to_previous_with_reboot_on_exhausted() {
        let cfg =
            Config::try_parse_from(["supervisord", "revert-to-previous", "--reboot-on-exhausted"])
                .unwrap();
        match cfg.command {
            Some(Command::RevertToPrevious {
                reboot_on_exhausted,
            }) => assert!(
                reboot_on_exhausted,
                "--reboot-on-exhausted must set the flag"
            ),
            other => panic!("expected RevertToPrevious, got {other:?}"),
        }
    }

    #[test]
    fn relay_url_defaults_to_none() {
        // Absent `--mesh-relay-url` / `TABBIFY_MESH_RELAY_URL` ⇒ the RAW field
        // is None; the EFFECTIVE relay for the default (prod) coordinator is
        // the baked wss:// endpoint — see `effective_relay_url_*` below.
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert!(cfg.relay_url.is_none());
    }

    #[test]
    fn effective_relay_url_bakes_wss_for_default_coordinator() {
        // Zero-config prod node: default coordinator ⇒ TLS relay on 443, so
        // a fresh install traverses corporate firewalls with NO env/drop-in
        // (the old `/run` systemd drop-in died on every reboot).
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert_eq!(
            cfg.effective_relay_url().as_deref(),
            Some(DEFAULT_RELAY_URL)
        );
    }

    #[test]
    fn effective_relay_url_derives_for_custom_coordinator() {
        // A custom (local/dev) coordinator has no TLS relay; None lets the
        // joiner keep deriving ws(s)://{coordinator-host}/v1/mesh/relay.
        let cfg =
            Config::try_parse_from(["supervisord", "--coordinator-url", "http://127.0.0.1:8888"])
                .unwrap();
        assert!(cfg.effective_relay_url().is_none());
    }

    #[test]
    fn effective_relay_url_explicit_overrides_everything() {
        // An operator-pinned relay wins over both defaults.
        let cfg = Config::try_parse_from([
            "supervisord",
            "--mesh-relay-url",
            "wss://relay.example.com/v1/mesh/relay",
        ])
        .unwrap();
        assert_eq!(
            cfg.effective_relay_url().as_deref(),
            Some("wss://relay.example.com/v1/mesh/relay")
        );
    }

    #[test]
    fn relay_url_flag_parses() {
        // The explicit flag (how the supervisor forwards it to runners, and how
        // an operator pins it) parses into `Some(..)` verbatim.
        let cfg = Config::try_parse_from([
            "supervisord",
            "--mesh-relay-url",
            "wss://relay.tabbify.io/v1/mesh/relay",
        ])
        .unwrap();
        assert_eq!(
            cfg.relay_url.as_deref(),
            Some("wss://relay.tabbify.io/v1/mesh/relay")
        );
    }

    #[test]
    fn relay_only_defaults_to_false() {
        // Absent `--mesh-relay-only` / `TABBIFY_MESH_RELAY_ONLY` ⇒ the peer
        // participates in direct + hole-punch traversal as usual (a plain
        // pass-through bool, no baking).
        let cfg = Config::try_parse_from(["supervisord"]).unwrap();
        assert!(!cfg.relay_only);
    }

    #[test]
    fn relay_only_flag_parses() {
        // The bare `--mesh-relay-only` flag (no value) sets the bool true: how
        // an operator declares a NAT/firewalled peer with no reachable direct
        // endpoint, and how the supervisor forwards it to runners.
        let cfg = Config::try_parse_from(["supervisord", "--mesh-relay-only"]).unwrap();
        assert!(cfg.relay_only);
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
