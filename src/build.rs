//! Shared runtime construction — the `build_runtime` free function used by the
//! per-app runner serve core ([`crate::runner::serve`]).
//!
//! Keeping the `wasm-http` / `firecracker` / `docker` runtime-selection match in
//! one place keeps it DRY and unit-testable independent of the serve wiring.

use std::sync::Arc;

use crate::config::{DockerConfig, FcConfig};
use crate::docker::{CommandRunner, DockerRuntime};
use crate::fetcher::FetchedApp;
use crate::oras::{find_wasm, oras_pull, production_oras_runner};
use crate::runtime::{AppRuntime, WasmRuntime};

/// Build the [`AppRuntime`] for a fetched app from the EFFECTIVE runtime, which
/// is `runtime_override` (the request-body override, contract D10) when present,
/// otherwise `manifest.runtime.type`:
/// - `wasm-http`   → in-process [`WasmRuntime`]; when `manifest.runtime.registry_ref`
///   is set, the WASM bytes are pulled from the mesh OCI registry via `oras pull`
///   (using `docker.oras_bin`). Falls back to S3 bytes if the pull fails or no
///   ref is set.
/// - `firecracker` → KVM-gated [`FirecrackerRuntime`](crate::firecracker::FirecrackerRuntime)
///   microVM (errors clearly on non-Linux / no `/dev/kvm`)
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
            // Generic Firecracker (D11): convert the deployed OCI image into a
            // rootfs.ext4 (cached by digest) + a PID-1 init, then boot it via
            // the existing FirecrackerRuntime contract. The conversion shells
            // out to docker/tar/mkfs.ext4 via the production runner.
            let runner = crate::runner::build::firecracker::production_fc_build_runner();
            crate::runner::build::firecracker::run_firecracker_build(
                uuid, fetched, fc, data_dir, &runner,
            )
            .await
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

/// Synthesize a minimal docker [`FetchedApp`] from a deployed OCI image `reff`,
/// for an app that was deployed via the BUILD pipeline (`POST /v1/deploy` with a
/// `repo_url`): its image lives in the mesh OCI registry but there is NO app
/// artifact/manifest in S3 (only `tcli push` uploads an S3 manifest). The runner
/// therefore runs the deployed image directly via `docker pull <reff>` instead of
/// erroring on the (absent) S3 fetch.
///
/// The runtime is `docker` with `registry_ref = Some(reff)`, so `build_runtime`
/// pulls + tags the image from the registry rather than building from source. The
/// container port the runner proxies to is taken from [`DockerConfig::app_port`]
/// (NOT the manifest — see [`crate::docker::runtime::rt_app_port`]), so no port
/// needs to be carried here. `cached_path` is set relative (`apps/<uuid>/deployed/
/// context.tar.gz`); [`resolve_fetched`] rebases it under the real data dir AND
/// materializes a placeholder file there, because the docker launch precheck
/// requires the build-context file to *exist* on disk even when the image is
/// pulled by ref (the placeholder is never read — a successful pull makes the W2
/// inspect skip `docker build`).
#[must_use]
pub fn fetched_from_ref(uuid: &str, reff: &str) -> FetchedApp {
    use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};

    FetchedApp {
        // No S3 `latest` exists for a build-pipeline app; the deployed image ref
        // is the authoritative version. 0 keeps the cache layout deterministic.
        version: 0,
        manifest: AppManifest {
            app: AppMeta {
                id: None,
                name: uuid.to_owned(),
                version: String::new(),
                kind: "headless".to_owned(),
                description: String::new(),
            },
            lifecycle: Lifecycle {
                // A deployed app should come up immediately, not lazily.
                mode: LifecycleMode::AlwaysOn,
                idle_timeout_sec: 300,
            },
            runtime: Runtime {
                r#type: "docker".to_owned(),
                entry: "context.tar.gz".to_owned(),
                fuel_per_request: 0,
                // Advisory for docker; a sane non-zero cap.
                memory_mb: 64,
                vcpus: None,
                kernel: None,
                registry_ref: Some(reff.to_owned()),
            },
            routes: Routes::default(),
        },
        wasm: bytes::Bytes::new(),
        cached_path: std::path::Path::new("apps")
            .join(uuid)
            .join("deployed")
            .join("context.tar.gz"),
    }
}

/// Decide the [`FetchedApp`] the runner should build its INITIAL runtime from,
/// given the result of the S3 fetch, the app `uuid`, an optional deployed
/// `image_ref` (the orchestrator's `--image-ref`), and the runner's `data_dir`.
///
/// - S3 fetch **succeeds** → behaves byte-identically to today: when `image_ref`
///   is `Some`, apply it via [`fetched_with_ref`] (the override the runner has
///   always done for tcli-push docker / wasm / firecracker apps); when `None`,
///   return the fetched app unchanged.
/// - S3 fetch **fails** with `image_ref` `Some` → synthesize a docker
///   [`FetchedApp`] from the ref via [`fetched_from_ref`] (the BUILD-pipeline
///   app: image in the registry, no S3 manifest). The synthesized `cached_path`
///   is rebased under `data_dir` and a placeholder build-context file is created
///   there so the docker launch precheck (which requires the context file to
///   exist) passes — the file is never read because a successful registry pull
///   makes the W2 cache check skip `docker build`.
/// - S3 fetch **fails** with `image_ref` `None` → propagate the error (a genuine
///   missing app, unchanged behavior).
///
/// Extracted as a pure-ish decision function (the only side effect is creating
/// the placeholder file on the fallback path) so the fallback logic is directly
/// unit-testable without binding a listener or building a real runtime.
///
/// # Errors
/// The original [`crate::fetcher::FetchError`] (as an [`anyhow::Error`]) when the
/// S3 fetch failed and no `image_ref` was supplied; or a filesystem error if the
/// placeholder build-context file cannot be created on the fallback path.
pub fn resolve_fetched(
    fetch_result: Result<FetchedApp, crate::fetcher::FetchError>,
    uuid: &str,
    image_ref: Option<&str>,
    data_dir: &std::path::Path,
) -> anyhow::Result<FetchedApp> {
    use anyhow::Context as _;

    match fetch_result {
        Ok(fetched) => Ok(match image_ref {
            Some(reff) => fetched_with_ref(&fetched, reff),
            None => fetched,
        }),
        Err(e) => match image_ref {
            Some(reff) => {
                // BUILD-pipeline app: image in the mesh registry, no S3 manifest.
                let mut fetched = fetched_from_ref(uuid, reff);
                // Rebase the relative cached_path under the real data dir and
                // materialize a placeholder build context so the docker precheck
                // (context.is_file()) passes — never read (pull skips the build).
                let abs = data_dir.join(&fetched.cached_path);
                if let Some(parent) = abs.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("create deployed build-context dir {}", parent.display())
                    })?;
                }
                if !abs.is_file() {
                    std::fs::write(&abs, b"").with_context(|| {
                        format!("write placeholder build context {}", abs.display())
                    })?;
                }
                fetched.cached_path = abs;
                Ok(fetched)
            }
            None => Err(anyhow::Error::new(e)),
        },
    }
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
                    vcpus: None,
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
            Box::pin(async { Err("oras pull failed".to_owned()) })
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
                Ok(())
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
        let runner: CommandRunner =
            Arc::new(|_| Box::pin(async { Err("oras pull failed".to_owned()) }));

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
                    vcpus: None,
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
        let runner: CommandRunner =
            Arc::new(|_| Box::pin(async { Err("oras pull failed".to_owned()) }));

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
        let runner: CommandRunner =
            Arc::new(|_| Box::pin(async { Err("oras pull failed".to_owned()) }));

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

    // ---- fetched_from_ref: synthesize a docker FetchedApp from an image ref ----

    /// `fetched_from_ref` synthesizes a docker `FetchedApp` carrying the given
    /// `registry_ref` — the manifest the runner uses when an app was deployed
    /// via the BUILD pipeline (image in the mesh registry, NO S3 manifest).
    #[test]
    fn fetched_from_ref_builds_docker_manifest_with_ref() {
        let reff = "[fd5a:1f00:0:3::1]:5000/tabbify/abc:main";
        let f = fetched_from_ref("abc-uuid", reff);
        assert_eq!(
            f.manifest.runtime.r#type, "docker",
            "synthesized runtime must be docker"
        );
        assert_eq!(
            f.manifest.runtime.registry_ref.as_deref(),
            Some(reff),
            "registry_ref must carry the deployed image ref"
        );
        // The docker runtime's container port comes from DockerConfig::app_port
        // (NOT the manifest), so the entry need only be the conventional docker
        // build-context filename — and there must be a sane memory cap.
        assert_eq!(f.manifest.runtime.entry, "context.tar.gz");
        assert!(f.manifest.runtime.memory_mb > 0, "memory cap must be sane");
        // No wasm bytes for a docker app.
        assert!(f.wasm.is_empty());
    }

    /// The synthesized manifest is `always_on` (a deployed app should come up
    /// immediately, not lazily) and is named from the uuid for diagnostics.
    #[test]
    fn fetched_from_ref_is_always_on() {
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main");
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::AlwaysOn);
    }

    // ---- resolve_fetched: fallback decision when S3 fetch fails ---------------

    /// On a SUCCESSFUL S3 fetch, `resolve_fetched` returns the fetched app with
    /// the image_ref override applied (byte-identical to today's behavior) —
    /// the tcli-push / wasm / firecracker path is untouched.
    #[test]
    fn resolve_fetched_s3_ok_applies_override() {
        let tmp = tempfile::tempdir().unwrap();
        let base = docker_fetched(None);
        let reff = "[fd5a::1]:5000/acme/app:sha";
        let resolved =
            resolve_fetched(Ok(base.clone()), "u", Some(reff), tmp.path()).unwrap();
        // Override applied; every other field preserved (== fetched_with_ref).
        assert_eq!(resolved.manifest.runtime.registry_ref.as_deref(), Some(reff));
        assert_eq!(resolved.version, base.version);
        assert_eq!(resolved.cached_path, base.cached_path);
    }

    /// On a SUCCESSFUL S3 fetch with NO image_ref, the fetched app is returned
    /// unchanged (no override) — the plain tcli-push path.
    #[test]
    fn resolve_fetched_s3_ok_no_ref_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let base = docker_fetched(Some("kept/ref:v1".to_owned()));
        let resolved = resolve_fetched(Ok(base.clone()), "u", None, tmp.path()).unwrap();
        assert_eq!(
            resolved.manifest.runtime.registry_ref.as_deref(),
            Some("kept/ref:v1")
        );
        assert_eq!(resolved.cached_path, base.cached_path);
    }

    /// On a FAILED S3 fetch WITH an image_ref, `resolve_fetched` synthesizes a
    /// docker FetchedApp from the ref (no error) — this is the BUILD-pipeline
    /// app whose image lives in the registry and has no S3 manifest. The
    /// synthesized cached_path must be a REAL file on disk so the docker
    /// runtime's precheck (which requires the build-context file to exist)
    /// passes even though the image is pulled by ref, not built.
    #[test]
    fn resolve_fetched_s3_err_with_ref_synthesizes() {
        let tmp = tempfile::tempdir().unwrap();
        let reff = "[fd5a:1f00:0:3::1]:5000/tabbify/abc:main";
        let resolved = resolve_fetched(
            Err(crate::fetcher::FetchError::NotFound("abc".to_owned())),
            "abc",
            Some(reff),
            tmp.path(),
        )
        .expect("S3 fetch failure + image_ref present must NOT error");
        assert_eq!(resolved.manifest.runtime.r#type, "docker");
        assert_eq!(resolved.manifest.runtime.registry_ref.as_deref(), Some(reff));
        assert!(
            resolved.cached_path.is_file(),
            "cached_path must be a real file so docker precheck passes; got {}",
            resolved.cached_path.display()
        );
    }

    /// On a FAILED S3 fetch with NO image_ref, the error propagates (unchanged
    /// behavior — a non-deployed app that genuinely is missing from S3).
    #[test]
    fn resolve_fetched_s3_err_no_ref_propagates() {
        let tmp = tempfile::tempdir().unwrap();
        let err = resolve_fetched(
            Err(crate::fetcher::FetchError::NotFound("abc".to_owned())),
            "abc",
            None,
            tmp.path(),
        )
        .expect_err("S3 failure with no image_ref must propagate the error");
        assert!(err.to_string().contains("abc"));
    }
}
