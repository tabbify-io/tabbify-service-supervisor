//! KVM-gated end-to-end test for the generic Firecracker runtime.
//!
//! Converts a real OCI image into a rootfs.ext4 (docker-less: `oras` pulls the
//! image as an OCI layout, the layers are untarred and `mkfs.ext4`'d), boots it
//! as a microVM via the production path, and asserts the guest app answers HTTP
//! 200. Requires:
//!   - `/dev/kvm` (R/W)  — absent on macOS dev hosts and non-nested CI;
//!   - the `firecracker` binary on `$PATH`;
//!   - a guest kernel at `/opt/tabbify/vmlinux`.
//!
//! It is `#[ignore]`d so `cargo test` on CI/macOS never runs it. Run it on the
//! `thinkpad` mesh-node (tags `[supervisor, firecracker]`):
//!   `cargo test -p tabbify-service-supervisor --test fc_integration -- --ignored`

#![cfg(target_os = "linux")]

use tabbify_supervisor::firecracker::kvm_available;

/// OCI image (exec-form ENTRYPOINT serving on :8080) → rootfs.ext4 → boot →
/// HTTP 200. Skips itself (passes trivially) when KVM is unavailable so a
/// `--ignored` run on a non-KVM box doesn't hard-fail.
#[tokio::test]
#[ignore = "requires /dev/kvm + firecracker + /opt/tabbify/vmlinux (run on thinkpad)"]
async fn oci_image_boots_as_firecracker_and_serves_200() {
    if !kvm_available() {
        eprintln!("SKIP: /dev/kvm unavailable on this host (expected on macOS/CI)");
        return;
    }

    use tabbify_supervisor::config::FcConfig;
    use tabbify_supervisor::runner::build::firecracker::{
        production_fc_build_runner, run_firecracker_build,
    };

    let tmp = tempfile::tempdir().expect("tmp data dir");
    let uuid = "fc-e2e-0000-0000-0000-000000000000";

    // The test image must be PUSHED to the mesh registry under the ref encoded
    // in the manifest's registry_ref — `run_firecracker_build` pulls it itself
    // first as an OCI layout via `oras` (docker-less; no local docker tag), then
    // converts it. The thinkpad runbook builds `tabbify/hello-http:exec` and
    // pushes it. Build a FetchedApp manifest pointing at it by digest.
    let digest = std::env::var("FC_TEST_IMAGE_DIGEST")
        .expect("set FC_TEST_IMAGE_DIGEST=sha256:... to the pushed test image");
    let fetched = make_fc_fetched(uuid, &digest);

    let runner = production_fc_build_runner();
    let rt = run_firecracker_build(uuid, &fetched, &FcConfig::default(), tmp.path(), &runner, false)
        .await
        .expect("convert + boot firecracker microVM");

    // Drive one request through the runtime's proxy into the guest app. The
    // `AppRuntime::handle` method resolves through the `dyn AppRuntime` type, so
    // the trait need not be imported explicitly.
    use bytes::Bytes;
    use http::Request;
    let req = Request::builder()
        .method("GET")
        .uri("http://app/")
        .body(Bytes::new())
        .unwrap();
    let resp = rt.handle(req).await.expect("guest responds");
    assert_eq!(resp.status(), 200, "guest app must serve 200");
}

/// Build a firecracker `FetchedApp` whose `registry_ref` points the mesh
/// registry image at `[fd5a::1]:5000/test/hello@<digest>`, with `memory_mb=256`
/// so `mkfs.ext4` has room for the unpacked image. `run_firecracker_build`
/// reads the digest off the `@<digest>` suffix to key the rootfs cache and pulls
/// the image by this ref before converting it.
fn make_fc_fetched(uuid: &str, digest: &str) -> tabbify_supervisor::fetcher::FetchedApp {
    use bytes::Bytes;
    use tabbify_supervisor::fetcher::FetchedApp;
    use tabbify_supervisor::manifest::{
        AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime,
    };

    FetchedApp {
        version: 1,
        manifest: AppManifest {
            app: AppMeta {
                id: None,
                name: "fc-e2e".to_owned(),
                version: String::new(),
                kind: "headless".to_owned(),
                description: String::new(),
            },
            lifecycle: Lifecycle {
                mode: LifecycleMode::AlwaysOn,
                idle_timeout_sec: 300,
            },
            runtime: Runtime {
                r#type: "firecracker".to_owned(),
                entry: "rootfs.ext4".to_owned(),
                fuel_per_request: 0,
                memory_mb: 256,
                vcpus: Some(1),
                kernel: None,
                registry_ref: Some(format!("[fd5a::1]:5000/test/hello@{digest}")),
            },
            routes: Routes::default(),
        },
        wasm: Bytes::new(),
        cached_path: std::path::PathBuf::from(format!("/cache/apps/{uuid}/v1/rootfs.ext4")),
    }
}
