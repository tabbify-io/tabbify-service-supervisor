//! Shared runtime construction — the `build_runtime` free function used by the
//! per-app runner serve core ([`crate::runner::serve`]).
//!
//! There is exactly ONE executable runtime: generic Firecracker. Keeping the
//! single construction path in one free function keeps it DRY and unit-testable
//! independent of the serve wiring.

use std::sync::Arc;

use crate::config::FcConfig;
use crate::fetcher::FetchedApp;
use crate::runtime::AppRuntime;

/// FC-sane default microVM RAM (MiB) for a connect-repo app with no managed
/// `[runtime].memory_mb`. 64 MiB starves a microVM (ACPI init fails, virtio-mmio
/// devices aren't discovered, the guest panics on "Cannot open root device
/// vda"); 2 GiB boots reliably AND runs dind.
const FC_DEFAULT_MEMORY_MB: u32 = 2048;
/// FC-sane default vCPU count for a connect-repo app with no managed `[runtime].vcpus`.
const FC_DEFAULT_VCPUS: u32 = 2;
/// Default idle-stop timeout (seconds) when none is supplied.
const FC_DEFAULT_IDLE_SEC: u64 = 300;

/// Build the [`AppRuntime`] for a fetched app — always a KVM-gated
/// [`FirecrackerRuntime`](crate::firecracker::FirecrackerRuntime) microVM
/// (errors clearly on non-Linux / no `/dev/kvm`).
///
/// The platform serves a SINGLE FC-from-image runtime: the deployed OCI image
/// is converted to a rootfs.ext4 and booted as a microVM. The
/// `manifest.runtime.type` field is no longer consulted to select a runtime —
/// the in-process WASM and the `docker run` EXECUTION runtimes were both
/// removed, and the runtime-string match (with its `docker`/`wasm-http` →
/// hard-error arm) has been collapsed to this unconditional firecracker path.
/// A deploy whose manifest still says `docker` / `wasm-http` therefore now
/// builds as Firecracker instead of bailing — the intended end-state, ahead of
/// the cross-repo step that drops those wire strings (and the lenient-deser
/// step). Docker survives only as the BUILD backend (`docker build` + skopeo
/// push), not as a way to RUN apps.
///
/// `uuid` makes the docker image tag + container name deterministic, and
/// drives the firecracker pidfile path for stale-VM reconciliation.
/// `data_dir` is the local cache root used to write / read the fc pidfile.
///
/// # Errors
/// A firecracker launch failure (no KVM / non-Linux / boot failure).
pub async fn build_runtime(
    uuid: &str,
    fetched: &FetchedApp,
    fc: &FcConfig,
    data_dir: &std::path::Path,
    is_swap: bool,
    extra_env: Option<&std::collections::HashMap<String, String>>,
) -> anyhow::Result<Arc<dyn AppRuntime>> {
    // Generic Firecracker (D11): convert the deployed OCI image into a
    // rootfs.ext4 (cached by digest) + a PID-1 init, then boot it via the
    // existing FirecrackerRuntime contract. The conversion shells out to
    // docker/tar/mkfs.ext4 via the production runner.
    //
    // `is_swap` distinguishes a DEPLOY (true: the old VM keeps serving, so the
    // launch must not reconcile-kill it, and a fresh `uuid:reff` tap lets old +
    // new coexist) from a COLD START (false: reconcile + warm-restore).
    let runner = crate::runner::build::firecracker::production_fc_build_runner();
    crate::runner::build::firecracker::run_firecracker_build(
        uuid, fetched, fc, data_dir, &runner, is_swap, extra_env,
    )
    .await
}

/// Return a clone of `fetched` with its docker `registry_ref` overridden to
/// `reff`, so a subsequent [`build_runtime`] call pulls THAT image instead of
/// building from source (P2.3 zero-downtime deploy by ref).
///
/// Only the manifest's `runtime.registry_ref` is changed; every other field
/// (version, cached path, runtime type/entry) is preserved so the rebuilt
/// runtime serves the same app on the same version, just from a freshly-pulled
/// image.
#[must_use]
pub fn fetched_with_ref(fetched: &FetchedApp, reff: &str) -> FetchedApp {
    let mut next = fetched.clone();
    next.manifest.runtime.registry_ref = Some(reff.to_owned());
    next
}

/// Synthesize a minimal [`FetchedApp`] from a deployed OCI image `reff`, for an
/// app that was deployed via the BUILD pipeline (`POST /v1/deploy` with a
/// `repo_url`): its image lives in the mesh OCI registry but there is NO app
/// artifact/manifest in S3 (only `tcli push` uploads an S3 manifest). The runner
/// therefore runs the deployed image directly by pulling `reff` instead of
/// erroring on the (absent) S3 fetch.
///
/// The synthesized runtime type is `firecracker` with `registry_ref =
/// Some(reff)`: a by-ref deploy is the FC pull source — the image is converted
/// to a rootfs.ext4 and booted as a microVM (the platform's single runtime).
/// `cached_path` is set relative (`apps/<uuid>/deployed/context.tar.gz`);
/// [`resolve_fetched`] rebases it under the real data dir AND materializes a
/// placeholder file there.
///
/// `manifest_toml` carries the Tabbify-MANAGED `tabbify.toml` (the same string
/// used by the connect-repo deploy): when `Some` and it parses, its `[runtime]`
/// (`memory_mb`, `vcpus`, `lifecycle`, `idle_timeout_sec`) and `[routes]`
/// (`dynamic_prefixes`) drive the synthesized manifest. `None` (or a toml that
/// fails to parse) falls back to the hardcoded FC-sane defaults — synthesis is
/// infallible so a broken toml never wedges a deploy here (build-time validation
/// is the gate).
#[must_use]
pub fn fetched_from_ref(uuid: &str, reff: &str, manifest_toml: Option<&str>) -> FetchedApp {
    use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};

    // Parse the managed toml when present; ignore a malformed one (fall back to
    // the FC-sane defaults). Synthesis is infallible so a broken toml never
    // wedges a deploy here (build-time validation is the gate).
    let managed = manifest_toml
        .and_then(|t| toml::from_str::<crate::unified_manifest::UnifiedManifest>(t).ok());

    let (mode, idle_timeout_sec, memory_mb, vcpus, dynamic_prefixes) = match &managed {
        Some(m) => {
            let mode = crate::unified_manifest::lifecycle_mode_from_str(&m.runtime.lifecycle);
            (
                mode,
                m.runtime.idle_timeout_sec,
                m.runtime.memory_mb,
                m.runtime.vcpus,
                m.routes.dynamic_prefixes.clone(),
            )
        }
        // FC-sane defaults (today's behaviour) when no managed toml.
        None => (
            LifecycleMode::AlwaysOn,
            FC_DEFAULT_IDLE_SEC,
            FC_DEFAULT_MEMORY_MB,
            FC_DEFAULT_VCPUS,
            Vec::new(),
        ),
    };

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
                mode,
                idle_timeout_sec,
            },
            runtime: Runtime {
                r#type: "firecracker".to_owned(),
                entry: "context.tar.gz".to_owned(),
                fuel_per_request: 0,
                memory_mb,
                vcpus: Some(vcpus),
                kernel: None,
                registry_ref: Some(reff.to_owned()),
            },
            routes: Routes { dynamic_prefixes },
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
/// - S3 fetch **fails** with `image_ref` `Some` → synthesize a firecracker
///   [`FetchedApp`] from the ref via [`fetched_from_ref`] (the BUILD-pipeline
///   app: image in the registry, no S3 manifest). The managed `manifest_toml`
///   (when threaded from the deploy) drives the synthesized `[runtime]`/`[routes]`.
///   The synthesized `cached_path`
///   is rebased under `data_dir` and a placeholder build-context file is created
///   there so the launch precheck (which requires the context file to exist)
///   passes — the file is never read because a successful registry pull makes
///   the W2 cache check skip the source build.
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
    manifest_toml: Option<&str>,
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
                // The managed `tabbify.toml` (when threaded from the deploy)
                // drives the synthesized `[runtime]`/`[routes]`.
                let mut fetched = fetched_from_ref(uuid, reff, manifest_toml);
                // Rebase the relative cached_path under the real data dir and
                // materialize a placeholder build context so the launch precheck
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

    /// `fetched_from_ref` synthesizes a firecracker `FetchedApp` carrying the
    /// given `registry_ref` — the manifest the runner uses when an app was
    /// deployed via the BUILD pipeline (image in the mesh registry, NO S3
    /// manifest). A by-ref deploy is the FC pull source.
    #[test]
    fn fetched_from_ref_builds_firecracker_manifest_with_ref() {
        let reff = "[fd5a:1f00:0:3::1]:5000/tabbify/abc:main";
        let f = fetched_from_ref("abc-uuid", reff, None);
        assert_eq!(
            f.manifest.runtime.r#type, "firecracker",
            "synthesized runtime must be firecracker"
        );
        assert_eq!(
            f.manifest.runtime.registry_ref.as_deref(),
            Some(reff),
            "registry_ref must carry the deployed image ref"
        );
        // The entry is the conventional build-context filename, and the memory
        // cap is the microVM's RAM (must be a sane non-zero value).
        assert_eq!(f.manifest.runtime.entry, "context.tar.gz");
        assert!(f.manifest.runtime.memory_mb > 0, "memory cap must be sane");
        // No wasm bytes.
        assert!(f.wasm.is_empty());
    }

    /// The synthesized manifest is `always_on` (a deployed app should come up
    /// immediately, not lazily) and is named from the uuid for diagnostics.
    #[test]
    fn fetched_from_ref_is_always_on() {
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main", None);
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::AlwaysOn);
    }

    /// When a managed `tabbify.toml` is provided, `fetched_from_ref` applies its
    /// `[runtime]` (`memory_mb`, `vcpus`, `lifecycle`) + `[routes]` to the
    /// synthesized manifest instead of the hardcoded defaults.
    #[test]
    fn fetched_from_ref_applies_managed_runtime_and_routes() {
        let toml = r#"
[app]
name = "sized"

[build]
kind = "docker"

[runtime]
memory_mb = 1024
vcpus = 2
lifecycle = "always_on"

[routes]
dynamic_prefixes = ["/api", "/health"]
"#;
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main", Some(toml));
        assert_eq!(f.manifest.runtime.memory_mb, 1024);
        assert_eq!(f.manifest.runtime.vcpus, Some(2));
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::AlwaysOn);
        assert_eq!(
            f.manifest.routes.dynamic_prefixes,
            vec!["/api".to_owned(), "/health".to_owned()]
        );
        // The deployed image ref is still carried.
        assert_eq!(
            f.manifest.runtime.registry_ref.as_deref(),
            Some("reg:5000/x:main")
        );
    }

    /// `on_request` lifecycle from the managed toml maps to `OnRequest` and
    /// carries the idle timeout.
    #[test]
    fn fetched_from_ref_managed_on_request_lifecycle() {
        let toml = r#"
[app]
name = "lazy"
[build]
kind = "docker"
[runtime]
lifecycle = "on_request"
idle_timeout_sec = 120
"#;
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main", Some(toml));
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::OnRequest);
        assert_eq!(f.manifest.lifecycle.idle_timeout_sec, 120);
    }

    /// A malformed managed toml falls back to the hardcoded defaults rather than
    /// erroring (the synthesis is infallible; a broken toml should never wedge a
    /// deploy here — build-time validation is the gate).
    #[test]
    fn fetched_from_ref_malformed_toml_falls_back_to_defaults() {
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main", Some("not = = toml"));
        assert_eq!(f.manifest.runtime.memory_mb, FC_DEFAULT_MEMORY_MB);
        assert_eq!(f.manifest.runtime.vcpus, Some(FC_DEFAULT_VCPUS));
        assert_eq!(f.manifest.lifecycle.idle_timeout_sec, FC_DEFAULT_IDLE_SEC);
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::AlwaysOn);
    }

    /// CONVENTION: an UNKNOWN `[runtime].lifecycle` value maps to `AlwaysOn`
    /// (the FC live-path default), matching `derive_app_manifest` — an unknown
    /// lifecycle must never silently flip a deployed app to lazy-start.
    #[test]
    fn fetched_from_ref_unknown_lifecycle_maps_to_always_on() {
        let toml =
            "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\n[runtime]\nlifecycle = \"weird\"\n";
        let f = fetched_from_ref("abc-uuid", "reg:5000/x:main", Some(toml));
        assert_eq!(f.manifest.lifecycle.mode, LifecycleMode::AlwaysOn);
    }

    // ---- resolve_fetched: fallback decision when S3 fetch fails ---------------

    /// On a SUCCESSFUL S3 fetch, `resolve_fetched` returns the fetched app with
    /// the image_ref override applied (byte-identical to today's behavior) —
    /// the tcli-push / firecracker path is untouched.
    #[test]
    fn resolve_fetched_s3_ok_applies_override() {
        let tmp = tempfile::tempdir().unwrap();
        let base = docker_fetched(None);
        let reff = "[fd5a::1]:5000/acme/app:sha";
        let resolved =
            resolve_fetched(Ok(base.clone()), "u", Some(reff), None, tmp.path()).unwrap();
        // Override applied; every other field preserved (== fetched_with_ref).
        assert_eq!(
            resolved.manifest.runtime.registry_ref.as_deref(),
            Some(reff)
        );
        assert_eq!(resolved.version, base.version);
        assert_eq!(resolved.cached_path, base.cached_path);
    }

    /// On a SUCCESSFUL S3 fetch with NO image_ref, the fetched app is returned
    /// unchanged (no override) — the plain tcli-push path.
    #[test]
    fn resolve_fetched_s3_ok_no_ref_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let base = docker_fetched(Some("kept/ref:v1".to_owned()));
        let resolved = resolve_fetched(Ok(base.clone()), "u", None, None, tmp.path()).unwrap();
        assert_eq!(
            resolved.manifest.runtime.registry_ref.as_deref(),
            Some("kept/ref:v1")
        );
        assert_eq!(resolved.cached_path, base.cached_path);
    }

    /// On a FAILED S3 fetch WITH an image_ref, `resolve_fetched` synthesizes a
    /// firecracker FetchedApp from the ref (no error) — this is the
    /// BUILD-pipeline app whose image lives in the registry and has no S3
    /// manifest. The synthesized cached_path must be a REAL file on disk so the
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
            None,
            tmp.path(),
        )
        .expect("S3 fetch failure + image_ref present must NOT error");
        assert_eq!(resolved.manifest.runtime.r#type, "firecracker");
        assert_eq!(
            resolved.manifest.runtime.registry_ref.as_deref(),
            Some(reff)
        );
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
            None,
            tmp.path(),
        )
        .expect_err("S3 failure with no image_ref must propagate the error");
        assert!(err.to_string().contains("abc"));
    }
}
