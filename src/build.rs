//! Shared runtime construction — the `build_runtime` free function used by the
//! per-app runner serve core ([`crate::runner::serve`]).
//!
//! Keeping the `wasm-http` / `firecracker` / `docker` runtime-selection match in
//! one place keeps it DRY and unit-testable independent of the serve wiring.

use std::sync::Arc;

use crate::config::{DockerConfig, FcConfig};
use crate::docker::{CommandRunner, DockerRuntime};
use crate::fetcher::FetchedApp;
use crate::firecracker::FirecrackerRuntime;
use crate::oras::{find_wasm, oras_pull, production_oras_runner};
use crate::runtime::{AppRuntime, WasmRuntime};

/// Build the [`AppRuntime`] for a fetched app from the EFFECTIVE runtime, which
/// is `runtime_override` (the request-body override, contract D10) when present,
/// otherwise `manifest.runtime.type`:
/// - `wasm-http`   → in-process [`WasmRuntime`]; when `manifest.runtime.registry_ref`
///   is set, the WASM bytes are pulled from the mesh OCI registry via `oras pull`
///   (using `docker.oras_bin`). Falls back to S3 bytes if the pull fails or no
///   ref is set.
/// - `firecracker` → KVM-gated [`FirecrackerRuntime`] microVM (errors clearly on
///   non-Linux / no `/dev/kvm`)
/// - `docker`      → [`DockerRuntime`] container built from the cached context
///   tarball (errors if no Docker daemon)
/// - anything else → hard error (no silent fallback)
///
/// `runtime_override` is the D4 wire string (`docker` | `firecracker` |
/// `wasm-http`); `None` ⇒ the manifest default is used (D10).
/// `uuid` makes the docker image tag + container name deterministic, and
/// drives the firecracker pidfile path for stale-VM reconciliation.
/// `data_dir` is the local cache root used to write / read the fc pidfile.
///
/// # Errors
/// A wasm compile failure, a firecracker launch failure (no KVM / non-Linux /
/// boot failure), a docker launch failure (no daemon / build / run failure),
/// or an unknown runtime type.
pub async fn build_runtime(
    runtime_override: Option<&str>,
    uuid: &str,
    fetched: &FetchedApp,
    fc: &FcConfig,
    docker: &DockerConfig,
    data_dir: &std::path::Path,
) -> anyhow::Result<Arc<dyn AppRuntime>> {
    build_runtime_with_oras(
        runtime_override,
        uuid,
        fetched,
        fc,
        docker,
        data_dir,
        &production_oras_runner(docker.oras_bin.clone()),
    )
    .await
}

/// Like [`build_runtime`] but accepts an injectable [`CommandRunner`] for the
/// `oras pull` step. Used by unit tests to avoid invoking a real `oras` binary.
#[allow(clippy::too_many_arguments)]
pub async fn build_runtime_with_oras(
    runtime_override: Option<&str>,
    uuid: &str,
    fetched: &FetchedApp,
    fc: &FcConfig,
    docker: &DockerConfig,
    data_dir: &std::path::Path,
    oras_runner: &CommandRunner,
) -> anyhow::Result<Arc<dyn AppRuntime>> {
    let rt = &fetched.manifest.runtime;
    // Override wins over the manifest default (D10). The manifest's runtime
    // type is the fallback when no override travels in the request body.
    let effective = runtime_override.unwrap_or(rt.r#type.as_str());
    match effective {
        "wasm-http" => {
            // AOT cache: <data_dir>/apps/<uuid>/v<N>/app.cwasm
            // The parent directory is created by `load_cached_or_compile` if
            // it doesn't exist yet.  A missing/corrupt/version-mismatched cache
            // falls back to Cranelift recompile automatically.
            let cache_dir = data_dir.join("apps").join(uuid).join("cache");
            let cache_path = cache_dir.join("app.cwasm");

            // Registry-pull: if a ref is set, pull the WASM OCI artifact from
            // the mesh registry via `oras pull --plain-http`. On success, read
            // the pulled `.wasm` bytes. On failure (or no ref), fall back to the
            // S3-cached bytes already in `fetched.wasm`.
            let wasm_bytes: std::borrow::Cow<[u8]> = if let Some(reff) = rt.registry_ref.as_deref()
            {
                let pull_dir = data_dir.join("apps").join(uuid).join("pulled");
                if let Err(e) = tokio::fs::create_dir_all(&pull_dir).await {
                    tracing::warn!(
                        uuid,
                        error = %e,
                        "failed to create oras pull dir — falling back to S3 bytes"
                    );
                    std::borrow::Cow::Borrowed(&fetched.wasm)
                } else {
                    let pulled = oras_pull(&docker.oras_bin, reff, &pull_dir, oras_runner).await;
                    if pulled {
                        match find_wasm(&pull_dir) {
                            Some(wasm_path) => match tokio::fs::read(&wasm_path).await {
                                Ok(bytes) => {
                                    tracing::info!(
                                        uuid,
                                        registry_ref = %reff,
                                        path = %wasm_path.display(),
                                        "wasm pulled from mesh registry"
                                    );
                                    std::borrow::Cow::Owned(bytes)
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        uuid,
                                        error = %e,
                                        path = %wasm_path.display(),
                                        "failed to read pulled wasm — falling back to S3 bytes"
                                    );
                                    std::borrow::Cow::Borrowed(&fetched.wasm)
                                }
                            },
                            None => {
                                tracing::warn!(
                                    uuid,
                                    registry_ref = %reff,
                                    "oras pull succeeded but no .wasm found in output dir — falling back to S3 bytes"
                                );
                                std::borrow::Cow::Borrowed(&fetched.wasm)
                            }
                        }
                    } else {
                        tracing::warn!(
                            uuid,
                            registry_ref = %reff,
                            "oras pull from mesh registry failed — falling back to S3 bytes"
                        );
                        std::borrow::Cow::Borrowed(&fetched.wasm)
                    }
                }
            } else {
                std::borrow::Cow::Borrowed(&fetched.wasm)
            };

            let wasm =
                WasmRuntime::load_cached_or_compile(&wasm_bytes, &cache_path, rt.fuel_per_request)?;
            Ok(Arc::new(wasm))
        }
        "firecracker" => {
            let vm =
                FirecrackerRuntime::launch_with_uuid(&fetched.cached_path, rt, fc, uuid, data_dir)
                    .await?;
            Ok(Arc::new(vm))
        }
        "docker" => {
            // Registry-pull is docker-only; wasm uses oras (above).
            let container = DockerRuntime::launch_with_id(
                &fetched.cached_path,
                rt,
                docker,
                uuid,
                fetched.version,
                rt.registry_ref.as_deref(),
            )
            .await?;
            Ok(Arc::new(container))
        }
        other => anyhow::bail!("unknown runtime type: {other}"),
    }
}

/// Return a clone of `fetched` with its docker `registry_ref` overridden to
/// `reff`, so a subsequent [`build_runtime`] call pulls THAT image instead of
/// building from source (P2.3 zero-downtime deploy by ref).
///
/// Only the manifest's `runtime.registry_ref` is changed; every other field
/// (version, wasm bytes, cached path, runtime type/entry/fuel) is preserved so
/// the rebuilt runtime serves the same app on the same version, just from a
/// freshly-pulled image. For a non-docker runtime the override is harmless:
/// `build_runtime` ignores `registry_ref` for wasm/firecracker, so a `Deploy`
/// to such an app simply rebuilds from the existing (S3) manifest.
#[must_use]
pub fn fetched_with_ref(fetched: &FetchedApp, reff: &str) -> FetchedApp {
    let mut next = fetched.clone();
    next.manifest.runtime.registry_ref = Some(reff.to_owned());
    next
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;

    use super::*;
    use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};

    /// Real WASM component used to verify the `wasm-http` arm of `build_runtime`.
    const HELLO_WASM: &[u8] = include_bytes!("../tests/fixtures/hello.wasm");

    /// Build a wasm-http [`FetchedApp`] with the given `registry_ref` and WASM bytes.
    fn wasm_fetched(registry_ref: Option<String>, wasm: Bytes) -> FetchedApp {
        FetchedApp {
            version: 1,
            manifest: AppManifest {
                app: AppMeta {
                    id: None,
                    name: "hello".to_owned(),
                    version: "0.1.0".to_owned(),
                    kind: "service".to_owned(),
                    description: String::new(),
                },
                lifecycle: Lifecycle {
                    mode: LifecycleMode::AlwaysOn,
                    idle_timeout_sec: 300,
                },
                runtime: Runtime {
                    r#type: "wasm-http".to_owned(),
                    entry: "app.wasm".to_owned(),
                    fuel_per_request: 0,
                    memory_mb: 0,
                    kernel: None,
                    registry_ref,
                },
                routes: Routes::default(),
            },
            wasm,
            cached_path: PathBuf::from("/cache/apps/hello/v1/app.wasm"),
        }
    }

    /// When `registry_ref` is `None`, the wasm arm uses the S3 bytes unchanged.
    #[tokio::test]
    async fn wasm_arm_no_ref_uses_s3_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let fetched = wasm_fetched(None, Bytes::from_static(HELLO_WASM));
        let cfg = DockerConfig::default();

        // A runner that must NOT be called (no ref → no oras pull).
        let called = Arc::new(Mutex::new(false));
        let called2 = called.clone();
        let runner: CommandRunner = Arc::new(move |_| {
            *called2.lock().unwrap() = true;
            Box::pin(async { false })
        });

        let rt = build_runtime_with_oras(
            None,
            "uuid-no-ref",
            &fetched,
            &FcConfig::default(),
            &cfg,
            tmp.path(),
            &runner,
        )
        .await
        .unwrap();
        // Should have loaded successfully from S3 bytes.
        assert!(Arc::strong_count(&rt) > 0);
        assert!(
            !*called.lock().unwrap(),
            "oras runner must NOT be called when registry_ref is None"
        );
    }

    /// When `registry_ref` is set and the runner writes a `.wasm` file into
    /// the pull dir (simulating a successful `oras pull`), the wasm arm loads
    /// from the pulled bytes — not from the (intentionally empty) S3 bytes.
    #[tokio::test]
    async fn wasm_arm_with_ref_loads_from_oras_pull() {
        let tmp = tempfile::tempdir().unwrap();
        let reff = "[fd5a:1f02::1]:5000/acme/hello:sha256abc";

        // S3 bytes are intentionally empty — the pull provides the real wasm.
        let fetched = wasm_fetched(Some(reff.to_owned()), Bytes::new());
        let cfg = DockerConfig::default();

        // The pull dir will be <tmp>/apps/<uuid>/pulled.
        let pull_dir = tmp.path().join("apps").join("uuid-oras-ref").join("pulled");
        let pull_dir_clone = pull_dir.clone();

        let captured_args: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let captured2 = captured_args.clone();

        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            *captured2.lock().unwrap() = args.clone();
            // Simulate a successful pull: write the hello.wasm fixture into the pull dir.
            let dir = pull_dir_clone.clone();
            Box::pin(async move {
                std::fs::create_dir_all(&dir).ok();
                std::fs::write(dir.join("app.wasm"), HELLO_WASM).unwrap();
                true
            })
        });

        let rt = build_runtime_with_oras(
            None,
            "uuid-oras-ref",
            &fetched,
            &FcConfig::default(),
            &cfg,
            tmp.path(),
            &runner,
        )
        .await
        .unwrap();
        assert!(
            Arc::strong_count(&rt) > 0,
            "runtime should be built from pulled wasm"
        );

        let argv = captured_args.lock().unwrap().clone();
        assert!(
            argv.contains(&"--plain-http".to_owned()),
            "oras pull must include --plain-http; got {argv:?}"
        );
        assert!(
            argv.contains(&reff.to_owned()),
            "oras pull must include the ref; got {argv:?}"
        );
    }

    /// When `registry_ref` is set but the runner returns `false` (pull failed),
    /// the wasm arm falls back to the S3 bytes.
    #[tokio::test]
    async fn wasm_arm_fallback_to_s3_on_pull_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let reff = "[fd5a:1f02::1]:5000/acme/hello:sha256abc";
        // S3 bytes hold the real WASM so the fallback path compiles successfully.
        let fetched = wasm_fetched(Some(reff.to_owned()), Bytes::from_static(HELLO_WASM));
        let cfg = DockerConfig::default();

        // Runner always fails.
        let runner: CommandRunner = Arc::new(|_| Box::pin(async { false }));

        let rt = build_runtime_with_oras(
            None,
            "uuid-fallback",
            &fetched,
            &FcConfig::default(),
            &cfg,
            tmp.path(),
            &runner,
        )
        .await
        .unwrap();
        assert!(
            Arc::strong_count(&rt) > 0,
            "should fall back to S3 bytes and still compile"
        );
    }

    fn docker_fetched(registry_ref: Option<String>) -> FetchedApp {
        FetchedApp {
            version: 7,
            manifest: AppManifest {
                app: AppMeta {
                    id: None,
                    name: "container-app".to_owned(),
                    version: String::new(),
                    kind: "headless".to_owned(),
                    description: String::new(),
                },
                lifecycle: Lifecycle {
                    mode: LifecycleMode::AlwaysOn,
                    idle_timeout_sec: 300,
                },
                runtime: Runtime {
                    r#type: "docker".to_owned(),
                    entry: "context.tar.gz".to_owned(),
                    fuel_per_request: 0,
                    memory_mb: 64,
                    kernel: None,
                    registry_ref,
                },
                routes: Routes::default(),
            },
            wasm: Bytes::new(),
            cached_path: PathBuf::from("/cache/apps/u/v7/context.tar.gz"),
        }
    }

    /// `runtime_override = Some("wasm-http")` over a docker manifest builds a
    /// wasm runtime (the override wins over `manifest.runtime.type`, D10).
    #[tokio::test]
    async fn override_wins_over_manifest_runtime_type() {
        let tmp = tempfile::tempdir().unwrap();
        // Docker manifest, but the wasm bytes live in the S3 field — overriding
        // to wasm-http forces the wasm arm.
        let mut fetched = docker_fetched(None);
        fetched.wasm = Bytes::from_static(HELLO_WASM);
        let cfg = DockerConfig::default();
        let runner: CommandRunner = Arc::new(|_| Box::pin(async { false }));

        let rt = build_runtime_with_oras(
            Some("wasm-http"),
            "uuid-override-wasm",
            &fetched,
            &FcConfig::default(),
            &cfg,
            tmp.path(),
            &runner,
        )
        .await
        .expect("override to wasm-http must build a wasm runtime");
        assert!(Arc::strong_count(&rt) > 0);
    }

    /// `runtime_override = None` builds from the manifest default (wasm-http here).
    #[tokio::test]
    async fn none_override_uses_manifest_runtime_type() {
        let tmp = tempfile::tempdir().unwrap();
        let fetched = wasm_fetched(None, Bytes::from_static(HELLO_WASM));
        let cfg = DockerConfig::default();
        let runner: CommandRunner = Arc::new(|_| Box::pin(async { false }));

        let rt = build_runtime_with_oras(
            None,
            "uuid-none",
            &fetched,
            &FcConfig::default(),
            &cfg,
            tmp.path(),
            &runner,
        )
        .await
        .expect("None override must use manifest default (wasm-http)");
        assert!(Arc::strong_count(&rt) > 0);
    }

    /// `fetched_with_ref` sets the docker `registry_ref` to the deploy ref so a
    /// subsequent `build_runtime` pulls that image instead of building.
    #[test]
    fn fetched_with_ref_sets_registry_ref() {
        let base = docker_fetched(None);
        let reff = "[fd5a:1f02::1]:5000/acme/app:sha256abc";
        let next = fetched_with_ref(&base, reff);
        assert_eq!(next.manifest.runtime.registry_ref.as_deref(), Some(reff));
    }

    /// Overriding the ref preserves everything else: version, runtime type +
    /// entry, fuel, and the cached path are untouched (same app, same version —
    /// only the image source changes).
    #[test]
    fn fetched_with_ref_preserves_other_fields() {
        let base = docker_fetched(Some("old/ref:v1".to_owned()));
        let next = fetched_with_ref(&base, "new/ref:v2");
        assert_eq!(next.version, base.version);
        assert_eq!(next.manifest.runtime.r#type, "docker");
        assert_eq!(next.manifest.runtime.entry, base.manifest.runtime.entry);
        assert_eq!(next.cached_path, base.cached_path);
        // The original is not mutated (we operate on a clone).
        assert_eq!(
            base.manifest.runtime.registry_ref.as_deref(),
            Some("old/ref:v1")
        );
    }
}
