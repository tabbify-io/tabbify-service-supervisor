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
    assert_ne!(tap_old, tap_new, "old/new reff must not share a tap during swap");
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
        assert!(tap.len() <= 15, "tap {tap} exceeds IFNAMSIZ (len {})", tap.len());
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
        assert!(idx < SERVING_LINK_SLOTS, "idx {idx} must be < {SERVING_LINK_SLOTS}");
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
