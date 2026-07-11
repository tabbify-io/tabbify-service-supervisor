//! Firecracker microVM runtime (contract: firecracker-runtime-design).
//!
//! A second [`crate::runtime::AppRuntime`] implementation that boots an app's
//! rootfs as a Firecracker microVM and proxies HTTP to the app's server inside
//! the guest. Unlike the in-process WASM runtime, this needs real hardware
//! virtualization (`/dev/kvm`) and Linux host networking (`iproute2` taps), so:
//!
//! - the REAL implementation is `#[cfg(target_os = "linux")]`;
//! - a `#[cfg(not(target_os = "linux"))]` STUB lets the supervisor still BUILD
//!   and run (serving WASM apps) on a macOS dev host — its `launch` returns a
//!   clear `Err`.
//!
//! KVM-gated: [`kvm_available`] decides at startup whether this host can host
//! firecracker apps at all (it also drives the `firecracker` mesh tag). A
//! no-KVM host serves WASM and refuses firecracker apps loudly.
//!
//! ## Firecracker API client
//! Firecracker is configured over a per-VM UNIX-socket HTTP/1.1 REST API with
//! tiny JSON bodies (`PUT /machine-config`, `/boot-source`, `/drives/...`,
//! `/network-interfaces/...`, `/actions`). Rather than pull a heavy crate, the
//! request BODIES are built by pure functions in the [`protocol`] submodule
//! (unit-tested without a socket) and a ~60-line hand-rolled HTTP/1.1-over-
//! `tokio::net::UnixStream` client (Linux-only, in the [`linux`] submodule)
//! sends them. This keeps the dependency set unchanged and compiles cleanly
//! on musl.
//!
//! ## Module layout
//! - [`protocol`] — request-body builders + hop-by-hop filter + proxy core
//!   + raw HTTP response head parser (no I/O, cross-platform).
//! - [`snapshot`] — `snap.vmstate` + `snap.mem` path/presence helpers.
//! - [`pidfile`]  — pidfile read/write + stale-VM reconciliation.
//! - [`linux`]    — Linux microVM runtime; `cfg(target_os = "linux")`.
//! - [`stub`]     — Non-Linux stub so the crate builds on macOS dev hosts.

// The real machinery and the stub share these cross-platform pieces (config
// type, request-body builders, hop-by-hop header set, the KVM probe).
use crate::config::FcConfig;

#[cfg(target_os = "linux")]
pub(crate) mod build_vm;
// `pub(crate)` so crate-internal tests (e.g. `fc_sandbox`'s build-VM identity
// disjointness check) can reach helpers like `derive_link_ips`; the public
// surface is still only the `FirecrackerRuntime` re-export below.
#[cfg(target_os = "linux")]
pub(crate) mod linux;
pub(crate) mod pidfile;
// Pure, host-agnostic per-FC CPU-scope argv + scope-name builder (F1, audit
// #93) — NO cfg gate so the argv/fallback LOGIC is unit-testable on macOS (the
// spawn + `systemctl stop` reaping that consume it are behind the Linux gate).
pub(crate) mod cpu_scope;
mod protocol;
// Pure, host-agnostic readiness-port PLANNING + multi-port first-answering PROBE
// — NO cfg gate so the plan LOGIC + the probe are unit-testable on macOS (the
// probe dials `host:port` over plain TCP against real localhost listeners).
pub(crate) mod port_plan;
pub(crate) mod snapshot;
// Pure, host-agnostic snapshot DECISION logic — NO cfg gate so it is unit-
// testable on macOS (the rest of the snapshot path is behind the Linux gate).
pub mod snapshot_decision;
// Pure, host-agnostic egress allow-list rule builder + DNS-pin resolver (Track 7
// network ACL) — NO cfg gate so the rule LOGIC is unit-testable on macOS (the
// `setup_guest_nat` enforcement that consumes it is behind the Linux gate).
pub mod egress_filter;
// Pure, host-agnostic iptables arg builder for the IPv4 tap-gateway proxy
// firewalls (git-proxy :8788 + forge-proxy :8789) — NO cfg gate so the rule
// LOGIC is unit-testable on macOS (the `iptables` shell-out that consumes it,
// `setup_proxy_ipv4_firewall`, is behind the Linux gate in `linux.rs`).
pub mod proxy_firewall;
#[cfg(not(target_os = "linux"))]
mod stub;

// The concrete `FirecrackerRuntime` differs by platform. The real Linux impl
// owns a child process + tap and proxies over the WG/tap network; the non-Linux
// stub exists only so the crate builds on macOS dev hosts.
#[cfg(target_os = "linux")]
pub use linux::FirecrackerRuntime;
#[cfg(not(target_os = "linux"))]
pub use stub::FirecrackerRuntime;

// Re-exported `pub` (the `linux` module itself is only `pub(crate)`) so the
// `supervisord` binary can install the git-proxy + forge-proxy IPv4 firewalls at
// startup.
#[cfg(target_os = "linux")]
pub use linux::{setup_forge_proxy_firewall, setup_git_proxy_firewall};

/// The fixed build-VM tap name (`fc-bld0`). Cross-platform const so the F2.2
/// orphan sweep can compute the build api-socket WITHOUT pulling in the
/// Linux-only `build_vm` module (keeps the sweep's pure fns macOS-testable). The
/// Linux `build_vm` module re-exports THIS as `BUILD_TAP` (single source).
//
// Only the Linux build_vm + the Linux sweep (+ cross-platform tests) consume
// these; off Linux they have no non-test caller (allow-dead-code there only).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const BUILD_TAP_NAME: &str = "fc-bld0";

/// The fixed build-VM scope/pidfile identity (`build0`). Cross-platform for the
/// same reason as [`BUILD_TAP_NAME`]; `build_vm::BUILD_SCOPE_ID` re-exports it.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const BUILD_SCOPE_ID_NAME: &str = "build0";

/// The deterministic FC host TAP name for identity `key` (`uuid` on cold start,
/// `uuid:reff` on a deploy). `fc-<48-bit blake3 hex>` — 15 chars (IFNAMSIZ).
///
/// CROSS-PLATFORM single source of truth: the Linux runtime's
/// `linux::fc_identity_for_key` delegates here for the tap name, and the
/// record-less FC orphan sweep (F2.2, audit #93) reconstructs the EXPECTED
/// api-socket of every LIVE runner record by hashing its key the SAME way — so
/// the sweep's "this FC belongs to a live runner" correlation can never drift
/// from the spawn path. Pure, no I/O, unit-testable on macOS.
#[must_use]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn fc_tap_name_for_key(key: &str) -> String {
    let digest = blake3::hash(key.as_bytes());
    let b = digest.as_bytes();
    let hash48: u64 = (u64::from(b[0]) << 40)
        | (u64::from(b[1]) << 32)
        | (u64::from(b[2]) << 24)
        | (u64::from(b[3]) << 16)
        | (u64::from(b[4]) << 8)
        | u64::from(b[5]);
    format!("fc-{hash48:012x}")
}

/// The deterministic FC api-socket path for identity `key`:
/// `/tmp/firecracker-<tap>.sock` (matches the spawn sites in `linux.rs` /
/// `build_vm.rs`). Used by the orphan sweep to build the live-socket set.
#[must_use]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn fc_api_sock_for_key(key: &str) -> String {
    format!("/tmp/firecracker-{}.sock", fc_tap_name_for_key(key))
}

/// Path to the KVM device node; presence + R/W openability gates firecracker.
const DEV_KVM: &str = "/dev/kvm";

/// Is this host able to run Firecracker microVMs? True iff `/dev/kvm` exists
/// AND can be opened read-write (KVM requires R/W). EC2 `t3.micro` (no nested
/// virt) has no `/dev/kvm`, so this returns `false` there and the supervisor
/// degrades to WASM-only.
#[must_use]
pub fn kvm_available() -> bool {
    protocol::kvm_available_with(default_kvm_check)
}

/// Default KVM probe used by [`kvm_available`]: try to open `/dev/kvm` R/W.
fn default_kvm_check() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(DEV_KVM)
        .is_ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::protocol::{
        aux_drive_body, boot_source_body, copy_filtered_headers, instance_start_body, is_hop_by_hop,
        kvm_available_with, machine_config_body, network_iface_body, parse_status_line, pause_body,
        read_http_status, resolve_port, resolve_vcpus, resume_body, rootfs_drive_body,
        snapshot_create_body, snapshot_load_body, workspace_or_resolved_port,
    };
    use crate::config::FcConfig;
    use crate::manifest::Runtime;

    /// A firecracker [`Runtime`] fixture with the given optional `vcpus` override.
    fn fc_runtime(vcpus: Option<u32>) -> Runtime {
        Runtime {
            r#type: "firecracker".to_owned(),
            entry: "rootfs.ext4".to_owned(),
            fuel_per_request: 0,
            memory_mb: 1536,
            vcpus,
            port: None,
            kernel: None,
            registry_ref: None,
        }
    }

    #[test]
    fn kvm_gate_reflects_the_injected_probe() {
        assert!(kvm_available_with(|| true));
        assert!(!kvm_available_with(|| false));
    }

    #[test]
    fn machine_config_body_carries_vcpus_and_mem() {
        let b = machine_config_body(2, 512);
        assert_eq!(b["vcpu_count"], 2);
        assert_eq!(b["mem_size_mib"], 512);
        assert_eq!(b["smt"], false);
    }

    /// The managed `[runtime].vcpus` override drives the FC machine-config:
    /// `Some(2)` must win over the supervisor's configured default (1), exactly
    /// as `memory_mb` already does. (Regression for the bug where a connect-repo
    /// app with `vcpus = 2` cold-spawned with `vcpu_count = 1`.)
    #[test]
    fn resolve_vcpus_prefers_manifest_override_over_config_default() {
        let cfg = FcConfig::default(); // cfg.vcpus == 1
        let rt = fc_runtime(Some(2));
        assert_eq!(resolve_vcpus(&rt, &cfg), 2);
    }

    /// When the manifest omits `vcpus`, the supervisor's configured default is
    /// used (the documented `None` → `FcConfig::vcpus` contract).
    #[test]
    fn resolve_vcpus_falls_back_to_config_default_when_absent() {
        let cfg = FcConfig {
            vcpus: 4,
            ..FcConfig::default()
        };
        let rt = fc_runtime(None);
        assert_eq!(resolve_vcpus(&rt, &cfg), 4);
    }

    /// End-to-end: the resolved vcpu count flows into the machine-config body.
    #[test]
    fn machine_config_uses_resolved_manifest_vcpus() {
        let cfg = FcConfig::default(); // default 1
        let rt = fc_runtime(Some(2));
        let b = machine_config_body(resolve_vcpus(&rt, &cfg), rt.memory_mb);
        assert_eq!(b["vcpu_count"], 2);
        assert_eq!(b["mem_size_mib"], 1536);
    }

    /// TIER 1 — the managed `[runtime].port` override drives the readiness-probe /
    /// proxy target: `Some(8788)` (e.g. www-backend) must win over BOTH the image's
    /// `ExposedPorts` AND the configured default (8080), so an explicit user
    /// override is always honoured.
    #[test]
    fn resolve_port_prefers_manifest_override_over_all() {
        let cfg = FcConfig::default(); // cfg.app_port == 8080
        let mut rt = fc_runtime(None);
        rt.port = Some(8788);
        // Even with an image ExposedPort of 80, the manifest override wins.
        assert_eq!(resolve_port(&rt, Some(80), &cfg), 8788);
    }

    /// TIER 2 — with no manifest `port`, the image's OWN declared port (the lowest
    /// `ExposedPorts` TCP entry, resolved by the build path) is used: this is what
    /// makes a static site `FROM nginx` (`EXPOSE 80`) work with zero user action
    /// instead of being force-probed on 8080.
    #[test]
    fn resolve_port_uses_image_exposed_port_when_no_manifest_override() {
        let cfg = FcConfig::default(); // 8080
        let rt = fc_runtime(None); // port: None
        assert_eq!(resolve_port(&rt, Some(80), &cfg), 80);
    }

    /// TIER 3 — when the manifest omits `port` AND the image declares no
    /// `ExposedPorts` (`image_port == None`), the supervisor's configured default
    /// (`FcConfig::app_port`) is used — preserves the 8080 status quo unchanged.
    #[test]
    fn resolve_port_falls_back_to_config_default_when_absent() {
        let cfg = FcConfig::default(); // 8080
        let rt = fc_runtime(None); // port: None
        assert_eq!(resolve_port(&rt, None, &cfg), 8080);
    }

    /// WORKSPACE — the fixed workspace-init port (`FcConfig::app_port`, 8080) is
    /// FORCED regardless of the image's `ExposedPorts` (the workspace base image
    /// declares `EXPOSE 2222` for devbox SSH, which is NOT its readiness port) AND
    /// regardless of any manifest `[runtime].port`. Regression for 9bb169a, where
    /// the image-derived port made the readiness probe target `:2222` and the
    /// workspace hung in `provisioning` forever.
    #[test]
    fn workspace_or_resolved_port_forces_fixed_app_port_for_workspace() {
        let cfg = FcConfig::default(); // cfg.app_port == 8080
        let mut rt = fc_runtime(None);
        rt.port = Some(9999); // even an explicit manifest override is ignored
        // image_exposed_port = Some(2222) (the base image's EXPOSE 2222) is ignored.
        assert_eq!(
            workspace_or_resolved_port(true, &rt, Some(2222), &cfg),
            8080
        );
    }

    /// APP — a non-workspace launch preserves the full [`resolve_port`] precedence:
    /// manifest override wins, else the image's lowest `ExposedPorts` TCP, else the
    /// 8080 default. Proves the fix does NOT alter app port resolution.
    #[test]
    fn workspace_or_resolved_port_preserves_app_precedence() {
        let cfg = FcConfig::default(); // 8080
        // Manifest override wins.
        let mut rt = fc_runtime(None);
        rt.port = Some(8788);
        assert_eq!(workspace_or_resolved_port(false, &rt, Some(80), &cfg), 8788);
        // No manifest override → image ExposedPort wins.
        let rt = fc_runtime(None); // port: None
        assert_eq!(workspace_or_resolved_port(false, &rt, Some(80), &cfg), 80);
        // Neither manifest nor image port → 8080 default.
        assert_eq!(workspace_or_resolved_port(false, &rt, None, &cfg), 8080);
    }

    #[test]
    fn boot_source_body_has_kernel_and_ip_boot_arg() {
        let b = boot_source_body("/opt/tabbify/vmlinux", "172.31.0.2", "172.31.0.1");
        assert_eq!(b["kernel_image_path"], "/opt/tabbify/vmlinux");
        let args = b["boot_args"].as_str().unwrap();
        assert!(args.contains("ip=172.31.0.2::172.31.0.1:255.255.255.252::eth0:off"));
        assert!(args.contains("console=ttyS0"));
        // root + init are emitted EXPLICITLY so the boot args do not depend on a
        // kernel's built-in CONFIG_CMDLINE — lets the stock FC CI kernel (ACPI
        // on, no idle core-spin) boot the generic-FC rootfs. Regression-loud.
        assert!(args.contains("root=/dev/vda rw"));
        assert!(args.contains("init=/init"));
    }

    /// F4 (audit #93) — ANTI-REGRESSION GUARD: the single boot-args builder must
    /// NEVER emit `acpi=off`. The legacy docker-derived guest kernel baked
    /// `acpi=off`, which disabled the LAPIC → the guest fell back to the PIT and
    /// busy-spun a core forever instead of HLT-idling (task #23, the MSI "furnace"
    /// amplifier). The stock Firecracker CI kernel (ACPI on) is correct today and
    /// this builder is kernel-agnostic; this test FAILS LOUDLY the moment anyone
    /// reintroduces `acpi=off` into `boot_source_body` (e.g. copy-pasting a legacy
    /// docker cmdline). Asserted across several kernel/IP inputs so no code path
    /// can sneak it in. The kernel SOURCE is pinned in `nixos/tabbify-node.nix`
    /// (`kernelUrl`) so the digest can't silently regress either.
    #[test]
    fn boot_source_body_never_disables_acpi() {
        for (kernel, guest, host) in [
            ("/opt/tabbify/vmlinux", "172.31.0.2", "172.31.0.1"),
            ("/opt/tabbify/vmlinux-6.1.128", "10.0.0.2", "10.0.0.1"),
            ("", "192.168.255.2", "192.168.255.1"),
        ] {
            let b = boot_source_body(kernel, guest, host);
            let args = b["boot_args"].as_str().unwrap();
            assert!(
                !args.contains("acpi=off"),
                "boot_source_body must NEVER emit acpi=off (busy-spin kernel regression, audit #93): got {args:?}"
            );
            // Positive companion: pci=off + the working serial console stay, so a
            // blanket "strip everything" edit doesn't pass this test vacuously.
            assert!(args.contains("pci=off"), "pci=off expected: {args:?}");
            assert!(args.contains("console=ttyS0"), "console=ttyS0 expected: {args:?}");
        }
    }

    #[test]
    fn rootfs_drive_body_is_root_and_writable() {
        let b = rootfs_drive_body("/var/lib/tabbify/rootfs.ext4", false);
        assert_eq!(b["drive_id"], "rootfs");
        assert_eq!(b["path_on_host"], "/var/lib/tabbify/rootfs.ext4");
        assert_eq!(b["is_root_device"], true);
        assert_eq!(b["is_read_only"], false);
    }

    #[test]
    fn aux_drive_body_for_data_disk_is_nonroot_writable() {
        // A stateful FC app's persistent disk is attached via this exact
        // `/drives/data` body: a NON-root, WRITABLE auxiliary block device the
        // guest sees as /dev/vdb (attached right after the rootfs /dev/vda). This
        // is the contract the boot path issues when a `data_disk` is requested,
        // and the one Task 6 relies on — lock it.
        let b = aux_drive_body("data", "/x/data.ext4", false);
        assert_eq!(b["drive_id"], "data");
        assert_eq!(b["path_on_host"], "/x/data.ext4");
        assert_eq!(b["is_root_device"], false);
        assert_eq!(b["is_read_only"], false);
    }

    #[test]
    fn network_iface_body_binds_tap_to_eth0() {
        let b = network_iface_body("fc-tap0", "02:FC:00:00:00:00");
        assert_eq!(b["iface_id"], "eth0");
        assert_eq!(b["host_dev_name"], "fc-tap0");
        assert_eq!(b["guest_mac"], "02:FC:00:00:00:00");
    }

    #[test]
    fn instance_start_body_is_instance_start() {
        assert_eq!(instance_start_body()["action_type"], "InstanceStart");
    }

    #[test]
    fn pause_body_has_paused_state() {
        let b = pause_body();
        assert_eq!(b["state"], "Paused");
    }

    #[test]
    fn resume_body_has_resumed_state() {
        let b = resume_body();
        assert_eq!(b["state"], "Resumed");
    }

    #[test]
    fn snapshot_create_body_has_full_type_and_paths() {
        let b = snapshot_create_body(
            "/data/apps/abc/cache/snap.vmstate",
            "/data/apps/abc/cache/snap.mem",
        );
        assert_eq!(b["snapshot_type"], "Full");
        assert_eq!(b["snapshot_path"], "/data/apps/abc/cache/snap.vmstate");
        assert_eq!(b["mem_file_path"], "/data/apps/abc/cache/snap.mem");
        // snapshot_type must be exactly "Full" — Firecracker rejects other values.
        assert_eq!(b["snapshot_type"].as_str().unwrap(), "Full");
    }

    #[test]
    fn snapshot_load_body_resume_true_sets_resume_vm() {
        let b = snapshot_load_body(
            "/data/apps/abc/cache/snap.vmstate",
            "/data/apps/abc/cache/snap.mem",
            true,
        );
        assert_eq!(b["snapshot_path"], "/data/apps/abc/cache/snap.vmstate");
        assert_eq!(
            b["mem_backend"]["backend_path"],
            "/data/apps/abc/cache/snap.mem"
        );
        assert_eq!(b["mem_backend"]["backend_type"], "File");
        assert_eq!(b["resume_vm"], true);
    }

    #[test]
    fn snapshot_load_body_resume_false_does_not_resume() {
        let b = snapshot_load_body("/snap.vmstate", "/snap.mem", false);
        assert_eq!(b["resume_vm"], false);
        // mem_backend structure must be present regardless of resume flag.
        assert_eq!(b["mem_backend"]["backend_type"], "File");
    }

    #[test]
    fn hop_by_hop_detection_is_case_insensitive() {
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("transfer-encoding"));
        assert!(is_hop_by_hop("HOST"));
        assert!(!is_hop_by_hop("content-type"));
        assert!(!is_hop_by_hop("x-app-header"));
    }

    #[test]
    fn copy_filtered_headers_drops_hop_by_hop_keeps_the_rest() {
        let mut src = http::HeaderMap::new();
        src.insert("content-type", "application/json".parse().unwrap());
        src.insert("connection", "keep-alive".parse().unwrap());
        src.insert("host", "guest.local".parse().unwrap());
        src.insert("x-custom", "abc".parse().unwrap());

        let mut dst = http::HeaderMap::new();
        copy_filtered_headers(&src, &mut dst);

        assert_eq!(dst.get("content-type").unwrap(), "application/json");
        assert_eq!(dst.get("x-custom").unwrap(), "abc");
        assert!(dst.get("connection").is_none());
        assert!(dst.get("host").is_none());
    }

    // The proxy core is tested against a wiremock "fake VM" HTTP server — the
    // same path the Linux firecracker `handle` uses, exercised on any platform.
    use bytes::Bytes;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::protocol::proxy_request;

    #[tokio::test]
    async fn proxy_forwards_path_and_returns_guest_body() {
        let vm = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/data"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("x-guest", "yes")
                    .set_body_string("guest-says-hi"),
            )
            .mount(&vm)
            .await;

        let req = http::Request::builder()
            .method("GET")
            .uri("http://app-ula/api/data?q=1")
            .body(Bytes::new())
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &vm.uri(), req)
            .await
            .expect("proxy");

        assert_eq!(resp.status(), 201);
        assert_eq!(resp.headers().get("x-guest").unwrap(), "yes");
        assert_eq!(String::from_utf8_lossy(resp.body()), "guest-says-hi");
    }

    #[tokio::test]
    async fn proxy_strips_hop_by_hop_request_headers_before_forwarding() {
        let vm = MockServer::start().await;
        // The guest must NOT see `connection`/`host` forwarded, but MUST see the
        // custom header. wiremock asserts header presence/absence on match.
        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(wiremock::matchers::header("x-keep", "1"))
            .and(wiremock::matchers::header_exists("content-type"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&vm)
            .await;

        let req = http::Request::builder()
            .method("POST")
            .uri("http://app-ula/submit")
            .header("connection", "keep-alive")
            .header("x-keep", "1")
            .header("content-type", "text/plain")
            .body(Bytes::from_static(b"payload"))
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &vm.uri(), req)
            .await
            .expect("proxy");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "ok");
    }

    #[test]
    fn parses_firecracker_status_line() {
        assert_eq!(
            parse_status_line(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap(),
            204
        );
        assert_eq!(
            parse_status_line(b"HTTP/1.1 400 Bad Request\r\n\r\n{\"x\":1}").unwrap(),
            400
        );
    }

    /// Firecracker replies then KEEPS THE SOCKET OPEN (keep-alive, ignoring our
    /// `Connection: close`). `read_http_status` must return at the header
    /// terminator and not block waiting for an EOF that never comes.
    #[tokio::test]
    async fn read_http_status_returns_on_keepalive_without_eof() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, mut server) = tokio::io::duplex(1024);
        server
            .write_all(
                b"HTTP/1.1 204 \r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
        // Deliberately keep `server` alive (no EOF) across the read.
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_status(&mut client),
        )
        .await
        .expect("must not block waiting for EOF on a kept-alive socket")
        .expect("status parsed");
        assert_eq!(status, 204);
        drop(server);
    }

    /// A non-2xx reply carries a JSON body; only the status from the head is
    /// needed, and the trailing body must not cause a hang either.
    #[tokio::test]
    async fn read_http_status_reads_head_then_ignores_body() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, mut server) = tokio::io::duplex(1024);
        server
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 9\r\n\r\n{\"err\":1}")
            .await
            .unwrap();
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_status(&mut client),
        )
        .await
        .expect("no hang")
        .expect("status");
        assert_eq!(status, 400);
        drop(server);
    }

    // ---- health() contract for FirecrackerRuntime ---------------------------

    /// On a non-Linux host the stub's health() always returns Unavailable with
    /// a message indicating the host doesn't support firecracker.
    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn fc_stub_health_is_unavailable_on_non_linux() {
        use crate::runtime::{AppRuntime, RuntimeHealth};
        // The stub FirecrackerRuntime is zero-size; construct directly.
        let rt = super::stub::FirecrackerRuntime;
        let h = rt.health().await;
        assert!(
            matches!(h, RuntimeHealth::Unavailable(_)),
            "stub must be Unavailable, got {:?}",
            h
        );
    }

    /// On a Linux host, a FirecrackerRuntime with a probe faked to return
    /// "reachable" must report Serving.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn fc_linux_health_serving_when_probe_reachable() {
        use crate::runtime::{AppRuntime, RuntimeHealth};
        use std::sync::Arc;
        let rt = super::linux::FirecrackerRuntime::with_probe_for_test(
            "http://172.31.0.2:8080",
            Arc::new(|_addr: &str| true),
        );
        assert_eq!(rt.health().await, RuntimeHealth::Serving);
    }

    /// On a Linux host, a FirecrackerRuntime with a probe faked to return
    /// "unreachable" must report Unavailable.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn fc_linux_health_unavailable_when_probe_unreachable() {
        use crate::runtime::{AppRuntime, RuntimeHealth};
        use std::sync::Arc;
        let rt = super::linux::FirecrackerRuntime::with_probe_for_test(
            "http://172.31.0.2:8080",
            Arc::new(|_addr: &str| false),
        );
        assert!(
            matches!(rt.health().await, RuntimeHealth::Unavailable(_)),
            "must be Unavailable when probe returns false"
        );
    }
}
