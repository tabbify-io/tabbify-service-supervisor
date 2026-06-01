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

/// node-firecracker `health()` reports liveness from the shared `exited` flag
/// (not an HTTP probe): `Serving` until the flag is set, `Unavailable` after.
#[tokio::test]
async fn node_fc_health_serving_until_exit_flag_set() {
    use crate::runtime::{AppRuntime, RuntimeHealth};
    let rt = FirecrackerRuntime::test_node_fc();
    assert!(matches!(rt.health().await, RuntimeHealth::Serving));
    rt.exited.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(rt.health().await, RuntimeHealth::Unavailable(_)));
}

/// node-firecracker `handle()` returns 502: the VM is reached over the mesh by
/// its own ULA, never proxied over the tap.
#[tokio::test]
async fn node_fc_handle_returns_502() {
    use crate::runtime::AppRuntime;
    let rt = FirecrackerRuntime::test_node_fc();
    let req = http::Request::builder().body(bytes::Bytes::new()).unwrap();
    assert_eq!(
        rt.handle(req).await.unwrap().status(),
        http::StatusCode::BAD_GATEWAY
    );
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
