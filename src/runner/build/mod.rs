//! One-shot builder mode for `tabbify-runner`.
//!
//! When launched with `--build-spec <file>`, the runner reads a [`BuildJob`] from
//! the JSON file, runs the build pipeline end-to-end, prints the resulting
//! [`ArtifactRef`] as JSON to stdout, and exits — it never joins the mesh or
//! starts a serve loop.
//!
//! The orchestration pipeline ([`run_build`]) is fully injection-seamed so tests
//! can drive clone/build/push without any real git or Docker daemon.  The
//! production wiring (real `git`, `docker`) lives only in [`run_one_shot_build`].
//!
//! ## Module layout
//! - [`docker`] — the docker sub-pipeline (`Dockerfile` → build → docker push).
//! - [`firecracker`] — the generic Firecracker RUNTIME-build (OCI image →
//!   bootable `rootfs.ext4` + PID-1 init); `pub` so the KVM-gated integration
//!   test can drive [`firecracker::run_firecracker_build`] end-to-end.
//! - Everything else (the [`BuildJob`] type, the dispatcher, the production
//!   wiring, and the tests) lives in this file.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

mod docker;
pub(crate) mod fc_sandbox;
pub mod firecracker;

/// Which build pipeline a [`BuildJob`] drives.
///
/// Absent in the JSON spec ⇒ [`BuildKind::Docker`] (the original behaviour), so
/// every pre-existing docker job + test is unchanged. The in-process WASM
/// runtime was removed, so the only build pipeline is the docker one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum BuildKind {
    /// Clone → require `Dockerfile` → `docker build` → `docker push`.
    #[default]
    Docker,
}

/// A one-shot build job: clone `repo_url`@`git_ref`, build an artifact, push it
/// to the mesh registry at `registry_ula` as `<tenant>/<app_uuid>:<sha>`.
///
/// `build_kind` selects the pipeline (docker — the only kind today). The
/// `build_cmd` / `artifact_path` fields are retained as inert wire surface
/// (the WASM build path that consumed them was removed).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct BuildJob {
    /// HTTPS URL of the Git repository to clone.
    pub repo_url: String,
    /// Git ref (branch, tag, or full SHA) to check out.
    ///
    /// MVP: this value is used verbatim as the image tag component.  The
    /// control-plane should pass an immutable SHA; resolving HEAD via
    /// `git rev-parse` (requires a stdout-capturing runner) is a follow-up.
    pub git_ref: String,
    /// Tenant namespace used as the registry path prefix.
    pub tenant: String,
    /// UUID of the app; used in the image tag as `<tenant>/<app_uuid>:<sha>`.
    pub app_uuid: String,
    /// Mesh ULA + port of the registry to push to, e.g. `"[fd5a:1f02:aa::1]:5000"`.
    pub registry_ula: String,
    /// Short-lived GitHub token for the clone (`None` = public repo).
    #[serde(default)]
    pub clone_token: Option<String>,
    /// Token for pushing to the registry (`None` = anonymous registry).
    #[serde(default)]
    pub push_token: Option<String>,
    /// Which build pipeline to run. Absent ⇒ [`BuildKind::Docker`].
    #[serde(default)]
    pub build_kind: BuildKind,
    /// Inert wire field (formerly the wasm `build_cmd`). Retained so existing
    /// build specs still parse; no build path consumes it.
    #[serde(default)]
    pub build_cmd: Option<String>,
    /// Inert wire field (formerly the wasm `artifact_path`). Retained so existing
    /// build specs still parse; no build path consumes it.
    #[serde(default)]
    pub artifact_path: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (a raw TOML string) for a connect-repo
    /// deploy. Injected into the clone ONLY when the repo ships none (repo-wins);
    /// then parsed to drive `[build]` (dockerfile/context) + `[runtime]`/`[routes]`.
    /// `None` (the default) = no managed config (a `tcli`/local build, or a repo
    /// expected to carry its own toml).
    #[serde(default)]
    pub manifest_toml: Option<String>,
}

/// The result of a build: the immutable image ref and (optionally) its digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ArtifactRef {
    /// Fully-qualified OCI image reference, e.g. `"[fd5a::1]:5000/acme/myapp:abc1234"`.
    pub reff: String,
    /// OCI content-digest of the pushed manifest, e.g. `"sha256:deadbeef..."`.
    /// `None` if the registry did not return a digest.
    #[serde(default)]
    pub digest: Option<String>,
}

/// Run a build job end-to-end with injected dependencies.
///
/// Dispatches on [`BuildJob::build_kind`]:
/// - [`BuildKind::Docker`] (the only kind): clone → require `Dockerfile` →
///   `backend.build` → `docker tag`+`push` to the mesh registry.
///
/// Computes the ref scheme `job.registry_ula/<tenant>/<app_uuid>:<git_ref>` (the
/// `git_ref` is used verbatim as the tag component; the control-plane must
/// supply an immutable SHA) and returns an [`ArtifactRef`].
///
/// All dependencies are injected so the function is fully unit-testable without
/// a real git binary, Docker daemon, or `skopeo` binary. The `skopeo_runner` +
/// `skopeo_bin` drive the registry PUSH (the image is built by `backend`, then
/// `skopeo` copies it from the local docker daemon to the mesh registry — the
/// daemon never needs a mesh route).
///
/// # Errors
/// Clone failure; missing `Dockerfile`, build error, or push failure.
pub async fn run_build(
    job: &BuildJob,
    backend: &dyn crate::build_backend::BuildBackend,
    git: &crate::git::GitRun,
    tool_runner: &crate::docker::CommandRunner,
    skopeo_bin: &str,
    oras_bin: &str,
    workdir: &Path,
) -> anyhow::Result<ArtifactRef> {
    // 1. Clone into <workdir>/src.
    let src = workdir.join("src");
    crate::git::clone(
        &job.repo_url,
        &job.git_ref,
        job.clone_token.as_deref(),
        &src,
        git,
    )
    .await
    .context("clone")?;

    // Inject the Tabbify-MANAGED `tabbify.toml` ONLY when the repo ships none
    // (repo-wins): a repo that carries its own `tabbify.toml` keeps using it,
    // while a repo with none gets the managed default written at the clone root.
    let toml_path = src.join("tabbify.toml");
    if !toml_path.exists() {
        if let Some(t) = &job.manifest_toml {
            std::fs::write(&toml_path, t)
                .with_context(|| format!("write managed tabbify.toml to {}", toml_path.display()))?;
        }
    }

    // Resolve `[build]` (dockerfile/context) from the toml now present at the
    // clone root (repo's own or the injected managed one). Absent ⇒ today's
    // defaults: context = `<src>` and Docker's default `<src>/Dockerfile`.
    let build_spec = resolve_build_spec(&src, &toml_path)?;

    // Image ref: <registry_ula>/<tenant>/<app_uuid>:<git_ref>.
    let reff = format!(
        "{}/{}/{}:{}",
        job.registry_ula, job.tenant, job.app_uuid, job.git_ref
    );

    match job.build_kind {
        BuildKind::Docker => {
            // Phase-2 sandbox: build inside an ephemeral Firecracker VM
            // (explicit opt-in + KVM). The host clones (above), the VM
            // builds, the host pushes — no docker daemon anywhere.
            if fc_sandbox::enabled() {
                let fc_runner = firecracker::production_fc_build_runner();
                // The one-shot build child resolves data_dir from
                // SUPERVISOR_DATA_DIR (the spawner injects the supervisor's
                // resolved value); fall back to the install default.
                let data_dir = std::env::var("SUPERVISOR_DATA_DIR")
                    .unwrap_or_else(|_| "/var/lib/tabbify".to_owned());
                let layout = fc_sandbox::run_sandboxed_build(
                    &job.app_uuid,
                    &src,
                    &job.registry_ula,
                    std::path::Path::new(&data_dir),
                    workdir,
                    &fc_runner,
                )
                .await
                .context("sandboxed (firecracker) build")?;
                if let Err(e) = (tool_runner)(crate::skopeo::oras_push_args(
                    oras_bin,
                    &layout.to_string_lossy(),
                    &reff,
                ))
                .await
                {
                    anyhow::bail!("push to registry failed: {reff}: {e}");
                }
                return Ok(ArtifactRef { reff, digest: None });
            }
            docker::run_docker_build(
                job,
                backend,
                tool_runner,
                skopeo_bin,
                oras_bin,
                &build_spec,
                reff,
            )
            .await
        }
    }
}

/// The resolved `[build]` directives for a docker build: the clone root, the
/// context directory the image is built from, and the optional Dockerfile path.
/// All are absolute paths under the clone root.
#[derive(Debug, Clone)]
pub(crate) struct BuildSpec {
    /// The clone root (`<workdir>/src`); used to place the `oci-out` push layout
    /// stably at `<workdir>/oci-out` regardless of a non-root build context.
    pub clone_root: std::path::PathBuf,
    /// The build context dir (`<src>/<[build].context>`, default `<src>`).
    pub context_dir: std::path::PathBuf,
    /// The Dockerfile path (`<src>/<[build].dockerfile>`). `None` ⇒ Docker's
    /// default (`<context_dir>/Dockerfile`).
    pub dockerfile: Option<std::path::PathBuf>,
}

/// Resolve the [`BuildSpec`] from the `tabbify.toml` at `toml_path` (if present).
///
/// - No toml at the clone root ⇒ today's defaults: `context_dir = src`,
///   `dockerfile = None` (Docker's default `<src>/Dockerfile`).
/// - Toml present ⇒ parse it with the vendored [`crate::unified_manifest::UnifiedManifest`]
///   and resolve `[build].context` (default `.`) + `[build].dockerfile`
///   (default `Dockerfile`) relative to `src`.
///
/// # Errors
/// A malformed `tabbify.toml` (parse error) is surfaced so the build fails with
/// a clear diagnostic rather than silently ignoring a broken managed config.
fn resolve_build_spec(src: &Path, toml_path: &Path) -> anyhow::Result<BuildSpec> {
    if !toml_path.exists() {
        return Ok(BuildSpec {
            clone_root: src.to_path_buf(),
            context_dir: src.to_path_buf(),
            dockerfile: None,
        });
    }
    let text = std::fs::read_to_string(toml_path)
        .with_context(|| format!("read tabbify.toml at {}", toml_path.display()))?;
    let manifest: crate::unified_manifest::UnifiedManifest = toml::from_str(&text)
        .with_context(|| format!("parse tabbify.toml at {}", toml_path.display()))?;
    Ok(BuildSpec {
        clone_root: src.to_path_buf(),
        context_dir: src.join(manifest.context()),
        dockerfile: Some(src.join(manifest.dockerfile())),
    })
}

/// Read + parse a [`BuildJob`] from `spec_path` and run it with production
/// backends (real `git`, `docker`).
///
/// This is the one-shot builder-mode entry point invoked by `--build-spec`.
/// Returns the [`ArtifactRef`] on success or a descriptive error on failure.
pub async fn run_one_shot_build(spec_path: &Path) -> anyhow::Result<ArtifactRef> {
    let text = std::fs::read_to_string(spec_path)
        .map_err(|e| anyhow::anyhow!("read build spec {}: {e}", spec_path.display()))?;
    let job: BuildJob =
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("parse build spec: {e}"))?;

    // Production backends.
    // Allow overriding the docker binary via env (follows the supervisor pattern).
    let docker_bin = std::env::var("RUNNER_DOCKER_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_DOCKER_BIN.to_owned());
    let git_bin = std::env::var("RUNNER_GIT_BIN").unwrap_or_else(|_| "git".to_owned());

    // The docker build path pushes via supervisor-side `skopeo` (copies the
    // built image from the local docker daemon to the mesh registry), so the
    // docker daemon — which has no mesh route — never talks to the registry.
    let skopeo_bin = std::env::var("SUPERVISOR_SKOPEO_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_SKOPEO_BIN.to_owned());
    // oras does the registry leg of the push (bracketed-IPv6-capable refs);
    // same env override pattern as the run-side pull.
    let oras_bin = std::env::var("SUPERVISOR_ORAS_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_ORAS_BIN.to_owned());

    let backend = crate::build_backend::HostDockerBackend::new(docker_bin.clone());
    let git = crate::git::real_git_run(git_bin);
    let tool_runner = crate::skopeo::production_tool_runner();

    // Work directory: a fresh sub-dir under a tempdir for this build.
    // Using tempdir keeps build artefacts off any persistent volume without
    // requiring a configured data dir in build-only mode.
    let workdir = tempfile::Builder::new()
        .prefix(&format!("tbf-build-{}-", job.app_uuid))
        .tempdir()
        .context("create build workdir")?;

    run_build(
        &job,
        &backend,
        &git,
        &tool_runner,
        &skopeo_bin,
        &oras_bin,
        workdir.path(),
    )
    .await
}

#[cfg(test)]
mod tests;
