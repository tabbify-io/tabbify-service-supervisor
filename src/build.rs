//! Shared runtime construction — the `build_runtime` free function used by the
//! per-app runner serve core ([`crate::runner::serve`]).
//!
//! Keeping the `wasm-http` / `firecracker` / `docker` runtime-selection match in
//! one place keeps it DRY and unit-testable independent of the serve wiring.

use std::sync::Arc;

use crate::config::{DockerConfig, FcConfig};
use crate::docker::DockerRuntime;
use crate::fetcher::FetchedApp;
use crate::firecracker::FirecrackerRuntime;
use crate::runtime::{AppRuntime, WasmRuntime};

/// Build the [`AppRuntime`] for a fetched app from `manifest.runtime.type`:
/// - `wasm-http`   → in-process [`WasmRuntime`]
/// - `firecracker` → KVM-gated [`FirecrackerRuntime`] microVM (errors clearly on
///   non-Linux / no `/dev/kvm`)
/// - `docker`      → [`DockerRuntime`] container built from the cached context
///   tarball (errors if no Docker daemon)
/// - anything else → hard error (no silent fallback)
///
/// `uuid` makes the docker image tag + container name deterministic, and
/// drives the firecracker pidfile path for stale-VM reconciliation.
/// `data_dir` is the local cache root used to write / read the fc pidfile.
///
/// # Errors
/// A wasm compile failure, a firecracker launch failure (no KVM / non-Linux /
/// boot failure), a docker launch failure (no daemon / build / run failure),
/// or an unknown runtime type.
pub async fn build_runtime(
    uuid: &str,
    fetched: &FetchedApp,
    fc: &FcConfig,
    docker: &DockerConfig,
    data_dir: &std::path::Path,
) -> anyhow::Result<Arc<dyn AppRuntime>> {
    let rt = &fetched.manifest.runtime;
    match rt.r#type.as_str() {
        "wasm-http" => {
            // AOT cache: <data_dir>/apps/<uuid>/v<N>/app.cwasm
            // The parent directory is created by `load_cached_or_compile` if
            // it doesn't exist yet.  A missing/corrupt/version-mismatched cache
            // falls back to Cranelift recompile automatically.
            let cache_dir = data_dir.join("apps").join(uuid).join("cache");
            let cache_path = cache_dir.join("app.cwasm");
            let wasm = WasmRuntime::load_cached_or_compile(
                &fetched.wasm,
                &cache_path,
                rt.fuel_per_request,
            )?;
            Ok(Arc::new(wasm))
        }
        "firecracker" => {
            let vm =
                FirecrackerRuntime::launch_with_uuid(&fetched.cached_path, rt, fc, uuid, data_dir)
                    .await?;
            Ok(Arc::new(vm))
        }
        "docker" => {
            // Registry-pull is docker-only; wasm/firecracker ignore registry_ref.
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

    use bytes::Bytes;

    use super::*;
    use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};

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
