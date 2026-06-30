//! Linux-only firecracker runtime tests.
#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn link_ips_carve_sequential_slash30s() {
    let (h0, g0) = derive_link_ips("172.31.0.0/16", 0).unwrap();
    assert_eq!(h0, Ipv4Addr::new(172, 31, 0, 1));
    assert_eq!(g0, Ipv4Addr::new(172, 31, 0, 2));
    let (h1, g1) = derive_link_ips("172.31.0.0/16", 1).unwrap();
    assert_eq!(h1, Ipv4Addr::new(172, 31, 0, 5));
    assert_eq!(g1, Ipv4Addr::new(172, 31, 0, 6));
}

#[test]
fn guest_mac_is_locally_administered_and_deterministic() {
    assert_eq!(derive_guest_mac(0), "02:FC:00:00:00:00");
    assert_eq!(derive_guest_mac(1), "02:FC:01:00:00:00");
}

#[test]
fn fc_identity_is_stable_per_uuid() {
    // Same uuid → same tap name + link index every call (so cold boot, a
    // respawn, and a snapshot restore for one app all share host plumbing).
    let a = fc_identity_for_key("cc4bfba2-17a9-512d-b6f4-43f69114be65");
    let b = fc_identity_for_key("cc4bfba2-17a9-512d-b6f4-43f69114be65");
    assert_eq!(a, b);
}

#[test]
fn fc_identity_distinct_uuids_distinct_taps() {
    // The bug this fixes: two different apps must NOT land on the same tap
    // device / api-socket (the old VM_SEQ put every app on fc-tap0).
    let (tap_a, _) = fc_identity_for_key("cc4bfba2-17a9-512d-b6f4-43f69114be65");
    let (tap_b, _) = fc_identity_for_key("78a254d8-77ab-5e0b-ac55-c95e0ce7f0c3");
    assert_ne!(tap_a, tap_b);
}

#[test]
fn fc_identity_swap_old_and_new_reff_get_distinct_taps() {
    // Zero-downtime swap: the SAME app deploying a NEW reff must get a DIFFERENT
    // tap than the old reff it replaces, so the two microVMs coexist while
    // `perform_swap` health-gates the new one — yet both stay unique per app
    // (the uuid is in the key).
    let uuid = "cc4bfba2-17a9-512d-b6f4-43f69114be65";
    let (tap_old, _) = fc_identity_for_key(&format!("{uuid}:registry/x:sha-OLD"));
    let (tap_new, _) = fc_identity_for_key(&format!("{uuid}:registry/x:sha-NEW"));
    assert_ne!(
        tap_old, tap_new,
        "old/new reff must not share a tap during swap"
    );
    // A different app at the SAME reff is still distinct (cross-app uniqueness).
    let (tap_other, _) =
        fc_identity_for_key("78a254d8-77ab-5e0b-ac55-c95e0ce7f0c3:registry/x:sha-NEW");
    assert_ne!(tap_new, tap_other);
}

#[test]
fn fc_identity_tap_name_fits_ifnamsiz_and_is_prefixed() {
    // Linux interface names are capped at 15 chars (IFNAMSIZ - 1). `fc-` + 12
    // hex = exactly 15.
    for uuid in [
        "cc4bfba2-17a9-512d-b6f4-43f69114be65",
        "78a254d8-77ab-5e0b-ac55-c95e0ce7f0c3",
        "fc-launch-default",
        "0191e7c2-1111-7222-8333-444455556666",
    ] {
        let (tap, _) = fc_identity_for_key(uuid);
        assert!(tap.starts_with("fc-"), "tap {tap} must start with fc-");
        assert!(
            tap.len() <= 15,
            "tap {tap} exceeds IFNAMSIZ (len {})",
            tap.len()
        );
    }
}

#[test]
fn fc_identity_link_idx_never_hits_build_slot() {
    // link_idx must stay below the build VM's reserved /30 (BUILD_SEQ = 16382),
    // so a serving VM and the build VM never share a /30.
    for uuid in [
        "cc4bfba2-17a9-512d-b6f4-43f69114be65",
        "78a254d8-77ab-5e0b-ac55-c95e0ce7f0c3",
        "0191e7c2-2222-7222-8333-444455556666",
    ] {
        let (_, idx) = fc_identity_for_key(uuid);
        assert!(
            idx < SERVING_LINK_SLOTS,
            "idx {idx} must be < {SERVING_LINK_SLOTS}"
        );
        assert!(
            idx < crate::firecracker::build_vm::BUILD_SEQ,
            "idx {idx} collides build slot"
        );
    }
}

/// REAL microVM boot — Linux + `/dev/kvm` + a provisioned kernel + a
/// rootfs only. `#[ignore]`d so CI / the macOS dev host never runs it;
/// run it by hand on a KVM box (e.g. Leo's Lima Ubuntu):
///
/// ```text
/// # On the Lima guest, as root (needs /dev/kvm + iproute2 + firecracker):
/// #   - put a vmlinux at /opt/tabbify/vmlinux
/// #   - put a rootfs whose app serves HTTP on :8080 at /tmp/rootfs.ext4
/// sudo -E cargo test -p tabbify-service-supervisor \
///     firecracker::linux::tests::real_vm_boots_and_serves -- --ignored --nocapture
/// ```
///
/// Asserts the VM boots, the guest app answers, and `Drop` tears the VM
/// + tap down.
#[tokio::test]
#[ignore = "requires Linux + /dev/kvm + a provisioned kernel/rootfs (run on Lima)"]
async fn real_vm_boots_and_serves() {
    use crate::runtime::AppRuntime;

    let rootfs = std::path::PathBuf::from("/tmp/rootfs.ext4");
    let rt = crate::manifest::Runtime {
        r#type: "firecracker".to_owned(),
        entry: "rootfs.ext4".to_owned(),
        fuel_per_request: 0,
        memory_mb: 256,
        vcpus: None,
        port: None,
        kernel: None,
        registry_ref: None,
    };
    let cfg = FcConfig::default();

    let vm = FirecrackerRuntime::launch(&rootfs, &rt, &cfg)
        .await
        .expect("boot microVM");
    let req = Request::builder()
        .method("GET")
        .uri("http://app/")
        .body(Bytes::new())
        .unwrap();
    let resp = vm.handle(req).await.expect("proxy to guest");
    assert!(resp.status().is_success());
    drop(vm); // child killed + tap deleted
}

/// FIX (a): the warm-restore path must REAP a stale FC from a prior restore of
/// the same uuid BEFORE spawning a new one (FCs were piling up — 3 FCs for 1
/// runner). `launch_from_snapshot` now runs the SAME `pidfile::take` +
/// `kill_stale_if_alive` reap the cold path uses. This test exercises that exact
/// sequence against a real child (a stand-in for the stale FC) without needing
/// KVM: it spawns a `sleep`, records its pid in the per-uuid pidfile, runs the
/// reap sequence, and asserts the child is killed AND the pidfile is consumed.
#[test]
fn restore_reaps_prior_fc_via_pidfile() {
    use super::pidfile;

    let dir = tempfile::tempdir().unwrap();
    let uuid = "0191e7c2-cafe-7222-8333-444455556666";

    // A real "prior FC" we can safely kill.
    let mut prior_fc = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn stale-fc stand-in");
    let prior_pid = prior_fc.id();
    pidfile::write(dir.path(), uuid, prior_pid);
    assert!(pidfile::path(dir.path(), uuid).exists());

    // The exact reap sequence launch_from_snapshot runs before its FC spawn.
    if let Some(stale_pid) = pidfile::take(dir.path(), uuid) {
        assert_eq!(stale_pid, prior_pid);
        pidfile::kill_stale_if_alive(stale_pid, pidfile::process_is_alive);
    } else {
        panic!("pidfile must yield the prior FC pid");
    }

    // The pidfile must be consumed (so only one respawn runs at a time).
    assert!(
        !pidfile::path(dir.path(), uuid).exists(),
        "reap must consume the pidfile"
    );

    // The prior FC must be dead. SIGKILL is near-instant; poll waitpid(WNOHANG)
    // (reaps the zombie AND detects exit) for up to ~100ms.
    let mut alive_after = true;
    for _ in 0..10 {
        let r =
            unsafe { libc::waitpid(prior_pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) };
        if r != 0 {
            alive_after = false;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let _ = prior_fc.wait();
    assert!(
        !alive_after,
        "the prior FC (pid {prior_pid}) must be reaped before the new restore"
    );
}

#[test]
fn parse_default_dev_extracts_iface() {
    assert_eq!(
        parse_default_dev("default via 10.0.0.1 dev eth0 proto dhcp src 10.0.0.5 metric 100"),
        Some("eth0".to_owned())
    );
    assert_eq!(
        parse_default_dev("default via 192.168.1.1 dev wlan0\n"),
        Some("wlan0".to_owned())
    );
}

#[test]
fn parse_default_dev_none_when_no_route_or_dev() {
    assert_eq!(parse_default_dev(""), None);
    assert_eq!(parse_default_dev("blackhole 10.0.0.0/8"), None);
    // "dev" with no following token must not panic.
    assert_eq!(parse_default_dev("default via 10.0.0.1 dev"), None);
}

/// Track 7: `parse_default_gateway` extracts the `via <gw>` IP — the always-allow
/// the in-VM supervisor needs to keep its mesh uplink under a strict allow-list.
#[test]
fn parses_default_gateway_via() {
    assert_eq!(
        parse_default_gateway("default via 10.0.0.1 dev eth0 proto dhcp"),
        Some("10.0.0.1".to_owned())
    );
    assert_eq!(
        parse_default_gateway("default via 192.168.1.1 dev wlan0\n"),
        Some("192.168.1.1".to_owned())
    );
}

/// A default route with no `via` (link-scope, e.g. a WG/point-to-point default)
/// yields `None` — there is no gateway IP to always-allow. Must not panic.
#[test]
fn parse_default_gateway_none_when_no_via() {
    assert_eq!(parse_default_gateway("default dev wg0 scope link"), None);
    assert_eq!(parse_default_gateway(""), None);
    // "via" with no following token must not panic.
    assert_eq!(parse_default_gateway("default via"), None);
}

/// `FirecrackerRuntime::guest_ssh_addr()` must point at the guest's sshd:
/// the runtime's own `guest_ip` on TCP :2222 (an IPv4 socket). This is what
/// the runner's L4 forwarder targets so `[app_ula]:2222 → guest_ip:2222` works.
#[test]
fn guest_ssh_addr_targets_guest_ip_port_2222() {
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Arc;

    use crate::runtime::AppRuntime;

    // `with_probe_for_test` bakes in guest_ip = 169.254.0.2 (see linux.rs).
    let vm = FirecrackerRuntime::with_probe_for_test("http://169.254.0.2:8080", Arc::new(|_| true));
    let expected_ip = Ipv4Addr::new(169, 254, 0, 2);

    let ssh = vm
        .guest_ssh_addr()
        .expect("a Firecracker runtime must expose a guest SSH target");
    assert_eq!(ssh, SocketAddr::new(IpAddr::V4(expected_ip), 2222));
    assert!(ssh.is_ipv4(), "guest SSH target must be IPv4 (the /30 tap)");
    assert_eq!(ssh.port(), 2222, "guest sshd listens on :2222");
}

/// `snapshot()` is a no-op `Ok(())` when the runtime has no cache dir (the bare
/// `with_probe_for_test` constructor sets `snapshot_cache_dir: None`) — proving
/// the early-return guard so a test/build VM never tries to snapshot.
#[tokio::test]
async fn snapshot_noop_without_cache_dir() {
    use std::sync::Arc;

    use crate::runtime::AppRuntime;

    let rt = FirecrackerRuntime::with_probe_for_test("http://169.254.0.2:8080", Arc::new(|_| true));
    assert!(
        rt.snapshot().await.is_ok(),
        "snapshot() with no cache dir must be a no-op Ok(())"
    );
}

/// A throwaway HTTP/1.1 server that answers the FIRST connection with `status`,
/// then returns its bound base URL (`http://127.0.0.1:<port>`). Used to drive the
/// GAP#4 `pre_snapshot_scrub` fail-closed semantics without a real microVM.
async fn stub_scrub_server(status: u16) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let body = "ok";
            let resp = format!(
                "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        }
    });
    base
}

/// GAP#4: a NON-workspace runtime never scrubs — `pre_snapshot_scrub` is an
/// immediate `Ok(())` even with NO server reachable (no broker, no creds).
#[tokio::test]
async fn pre_snapshot_scrub_skips_for_non_workspace() {
    // Point at a dead port; a non-workspace must NOT dial it.
    let rt = FirecrackerRuntime::with_scrub_target_for_test("http://127.0.0.1:1", false);
    assert!(
        rt.pre_snapshot_scrub().await.is_ok(),
        "non-workspace scrub must be a no-op Ok(()) (no broker to scrub)"
    );
}

/// GAP#4: a WORKSPACE whose broker scrub returns 200 → `Ok(())` (proceed to snapshot).
#[tokio::test]
async fn pre_snapshot_scrub_ok_for_workspace_on_200() {
    let base = stub_scrub_server(200).await;
    let rt = FirecrackerRuntime::with_scrub_target_for_test(&base, true);
    assert!(
        rt.pre_snapshot_scrub().await.is_ok(),
        "a 200 scrub must let the snapshot proceed"
    );
}

/// GAP#4 FAIL-CLOSED: a WORKSPACE whose broker scrub returns 500 → `Err` so the
/// caller ABORTS the snapshot (never freeze a held secret).
#[tokio::test]
async fn pre_snapshot_scrub_aborts_for_workspace_on_500() {
    let base = stub_scrub_server(500).await;
    let rt = FirecrackerRuntime::with_scrub_target_for_test(&base, true);
    let err = rt
        .pre_snapshot_scrub()
        .await
        .expect_err("a non-2xx scrub must abort the snapshot");
    assert!(err.to_string().contains("500"), "error names the status: {err}");
}

/// GAP#4 FAIL-CLOSED: a WORKSPACE whose broker is UNREACHABLE → `Err` (abort).
/// A workspace's broker must be live to scrub; a connect refusal means it died /
/// never came up, so we must NOT snapshot creds.
#[tokio::test]
async fn pre_snapshot_scrub_aborts_for_workspace_when_unreachable() {
    // Port 1 on loopback refuses immediately.
    let rt = FirecrackerRuntime::with_scrub_target_for_test("http://127.0.0.1:1", true);
    assert!(
        rt.pre_snapshot_scrub().await.is_err(),
        "an unreachable workspace broker must abort the snapshot (fail-closed)"
    );
}

#[tokio::test]
async fn wait_for_socket_returns_immediately_when_present() {
    // A path that already exists (stand-in for the firecracker API socket)
    // resolves on the first poll — the happy path is not slowed by the
    // generous ceiling.
    let f = tempfile::NamedTempFile::new().unwrap();
    wait_for_socket_within(f.path(), Duration::from_secs(30))
        .await
        .expect("present socket must resolve immediately");
}

#[tokio::test]
async fn wait_for_socket_times_out_with_actionable_error_when_absent() {
    // An absent socket past the deadline yields the actionable "never appeared"
    // error (a tiny timeout keeps the test fast; the real ceiling is 30s).
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("firecracker-fc-deadbeef.sock");
    let err = wait_for_socket_within(&missing, Duration::from_millis(60))
        .await
        .expect_err("an absent socket past the deadline must error");
    assert!(
        err.to_string().contains("never appeared"),
        "error should name the missing socket: {err}"
    );
}
