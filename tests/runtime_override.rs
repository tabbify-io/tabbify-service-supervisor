//! Integration: `runtime_override` changes the runtime branch `build_runtime`
//! takes, independent of the manifest's declared `runtime.type` (contract D5/D10).
//!
//! Uses the PUBLIC [`build_runtime`] entry point (not the crate-private
//! `build_runtime_with_oras`, whose `CommandRunner` seam is `pub(crate)`). With
//! no `registry_ref` set, `build_runtime` never invokes `oras`, so the two
//! branches exercised here behave identically to the unit-test seam: the
//! `wasm-http` override loads the in-memory S3 bytes, and the docker default
//! branch is attempted (and fails without a daemon).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tabbify_supervisor::build::build_runtime;
use tabbify_supervisor::config::{DockerConfig, FcConfig};
use tabbify_supervisor::fetcher::FetchedApp;
use tabbify_supervisor::manifest::{
    AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime,
};

/// Real wasm component fixture (committed alongside the unit tests).
const HELLO_WASM: &[u8] = include_bytes!("fixtures/hello.wasm");

/// A docker-runtime manifest whose S3 wasm bytes are the hello fixture, so a
/// `wasm-http` override can build a real wasm runtime from it.
fn docker_manifest_with_wasm_bytes() -> FetchedApp {
    FetchedApp {
        version: 1,
        manifest: AppManifest {
            app: AppMeta {
                id: None,
                name: "override-app".to_owned(),
                version: String::new(),
                kind: "headless".to_owned(),
                description: String::new(),
            },
            lifecycle: Lifecycle {
                mode: LifecycleMode::OnRequest,
                idle_timeout_sec: 300,
            },
            runtime: Runtime {
                r#type: "docker".to_owned(),
                entry: "context.tar.gz".to_owned(),
                fuel_per_request: 0,
                memory_mb: 64,
                vcpus: None,
                kernel: None,
                registry_ref: None,
            },
            routes: Routes::default(),
        },
        wasm: Bytes::from_static(HELLO_WASM),
        cached_path: PathBuf::from("/cache/apps/override/v1/context.tar.gz"),
    }
}

/// With `runtime_override = Some("wasm-http")` the wasm branch is taken even
/// though the manifest declares docker — and it builds a real runtime.
#[tokio::test]
async fn override_to_wasm_builds_from_docker_manifest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fetched = docker_manifest_with_wasm_bytes();

    let rt = build_runtime(
        Some("wasm-http"),
        "uuid-itest-override",
        &fetched,
        &FcConfig::default(),
        &DockerConfig::default(),
        tmp.path(),
    )
    .await
    .expect("override to wasm-http must build a wasm runtime from a docker manifest");
    assert!(Arc::strong_count(&rt) > 0);
}

/// With `runtime_override = None` the manifest default (docker) branch is taken;
/// without a docker daemon it errors — proving the branch differs from the
/// `wasm-http` override above.
#[tokio::test]
async fn none_override_takes_manifest_docker_branch() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fetched = docker_manifest_with_wasm_bytes();

    let result = build_runtime(
        None,
        "uuid-itest-none",
        &fetched,
        &FcConfig::default(),
        &DockerConfig::default(),
        tmp.path(),
    )
    .await;
    // No docker daemon in CI → the docker branch fails. The KEY assertion is
    // that it did NOT build a wasm runtime (it took a different branch than the
    // override case above).
    assert!(
        result.is_err(),
        "manifest-default docker branch must be attempted (and fail without a daemon), not the wasm branch"
    );
}
