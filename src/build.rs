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
            let container =
                DockerRuntime::launch_with_id(&fetched.cached_path, rt, docker, uuid).await?;
            Ok(Arc::new(container))
        }
        other => anyhow::bail!("unknown runtime type: {other}"),
    }
}
