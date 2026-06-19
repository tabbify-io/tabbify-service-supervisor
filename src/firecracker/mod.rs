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
mod protocol;
pub(crate) mod snapshot;
// Pure, host-agnostic snapshot DECISION logic — NO cfg gate so it is unit-
// testable on macOS (the rest of the snapshot path is behind the Linux gate).
pub mod snapshot_decision;
// Pure, host-agnostic egress allow-list rule builder + DNS-pin resolver (Track 7
// network ACL) — NO cfg gate so the rule LOGIC is unit-testable on macOS (the
// `setup_guest_nat` enforcement that consumes it is behind the Linux gate).
pub mod egress_filter;
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
// `supervisord` binary can install the git-proxy IPv4 firewall at startup.
#[cfg(target_os = "linux")]
pub use linux::setup_git_proxy_firewall;

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
        boot_source_body, copy_filtered_headers, instance_start_body, is_hop_by_hop,
        kvm_available_with, machine_config_body, network_iface_body, parse_status_line, pause_body,
        read_http_status, resolve_port, resolve_vcpus, resume_body, rootfs_drive_body,
        snapshot_create_body, snapshot_load_body,
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

    /// The managed `[runtime].port` override drives the readiness-probe / proxy
    /// target: `Some(8788)` (e.g. www-backend) must win over the configured
    /// default (8080), so a non-8080 image runs as an FC app unchanged.
    #[test]
    fn resolve_port_prefers_manifest_override_over_config_default() {
        let cfg = FcConfig::default(); // cfg.app_port == 8080
        let mut rt = fc_runtime(None);
        rt.port = Some(8788);
        assert_eq!(resolve_port(&rt, &cfg), 8788);
    }

    /// When the manifest omits `port`, the supervisor's configured default
    /// (`FcConfig::app_port`) is used — preserves the 8080 status quo.
    #[test]
    fn resolve_port_falls_back_to_config_default_when_absent() {
        let cfg = FcConfig::default(); // 8080
        let rt = fc_runtime(None); // port: None
        assert_eq!(resolve_port(&rt, &cfg), 8080);
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

    #[test]
    fn rootfs_drive_body_is_root_and_writable() {
        let b = rootfs_drive_body("/var/lib/tabbify/rootfs.ext4", false);
        assert_eq!(b["drive_id"], "rootfs");
        assert_eq!(b["path_on_host"], "/var/lib/tabbify/rootfs.ext4");
        assert_eq!(b["is_root_device"], true);
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
