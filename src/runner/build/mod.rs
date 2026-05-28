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
//! - [`wasm`]   — the wasm sub-pipeline (`build_cmd` → verify `.wasm` → oras push).
//! - Everything else (the [`BuildJob`] type, the dispatcher, the production
//!   wiring, and the tests) lives in this file.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::runtime::BoxFut;

mod docker;
mod wasm;

/// Which build pipeline a [`BuildJob`] drives.
///
/// Absent in the JSON spec ⇒ [`BuildKind::Docker`] (the original behaviour), so
/// every pre-existing docker job + test is unchanged. [`BuildKind::Wasm`] selects
/// the additive wasm-component build path (`build_cmd` → `oras push`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    utoipa::ToSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum BuildKind {
    /// Clone → require `Dockerfile` → `docker build` → `docker push`.
    #[default]
    Docker,
    /// Clone → run `build_cmd` → verify `artifact_path` → `oras push` the `.wasm`.
    Wasm,
}

/// The build-command executor seam for the wasm path.
///
/// Receives the shell command string and the working directory (the cloned
/// source dir) and returns `true` iff the command exited successfully. The seam
/// lets tests simulate a build (e.g. write the expected `.wasm`) without running
/// a real toolchain; production uses [`production_build_cmd_runner`].
pub type BuildCmdRunner =
    std::sync::Arc<dyn Fn(String, PathBuf) -> BoxFut<'static, bool> + Send + Sync>;

/// Build the production [`BuildCmdRunner`]: runs `sh -c <cmd>` with the working
/// directory set to `cwd` and returns `true` iff the process exits 0.
///
/// The command runs untrusted-ish source on the host — the same trust model as
/// the docker build path (trusted source / RnD). The `cmd` originates from the
/// [`BuildJob`] (set by the deployer), never blindly from the cloned repo.
/// fc-sandbox hardening for untrusted source is a separate follow-up.
#[must_use]
pub fn production_build_cmd_runner() -> BuildCmdRunner {
    use std::sync::Arc;
    use tokio::process::Command;

    Arc::new(move |cmd: String, cwd: PathBuf| {
        let fut: BoxFut<'static, bool> = Box::pin(async move {
            match Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .current_dir(&cwd)
                .stdin(std::process::Stdio::null())
                .status()
                .await
            {
                Ok(s) => s.success(),
                Err(_) => false,
            }
        });
        fut
    })
}

/// A one-shot build job: clone `repo_url`@`git_ref`, build an artifact, push it
/// to the mesh registry at `registry_ula` as `<tenant>/<app_uuid>:<sha>`.
///
/// `build_kind` selects the pipeline (docker — the default — or wasm). The
/// `build_cmd` / `artifact_path` fields are only consulted for the wasm path.
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
    /// (Wasm only) shell command that produces the `.wasm`, run with the cloned
    /// source dir as cwd, e.g. `"cargo build --release --target wasm32-wasip2"`.
    #[serde(default)]
    pub build_cmd: Option<String>,
    /// (Wasm only) path to the produced `.wasm`, relative to the repo root,
    /// e.g. `"target/wasm32-wasip2/release/app.wasm"`.
    #[serde(default)]
    pub artifact_path: Option<String>,
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
/// - [`BuildKind::Docker`] (default): clone → require `Dockerfile` →
///   `backend.build` → `docker tag`+`push` to the mesh registry.
/// - [`BuildKind::Wasm`]: clone → run `build_cmd` (cwd = cloned src) → verify
///   `artifact_path` exists → `oras push` the `.wasm` to the mesh registry.
///
/// Both paths compute the same ref scheme
/// `job.registry_ula/<tenant>/<app_uuid>:<git_ref>` (the `git_ref` is used
/// verbatim as the tag component; the control-plane must supply an immutable
/// SHA) and return an [`ArtifactRef`].
///
/// All dependencies are injected so the function is fully unit-testable without
/// a real git binary, Docker daemon, build toolchain, or `oras` binary. The
/// `push_runner` + `docker_bin` drive the docker path; the `oras_runner` +
/// `build_cmd_runner` + `oras_bin` drive the wasm path (each path ignores the
/// other's runners).
///
/// # Errors
/// Clone failure; (docker) missing `Dockerfile`, build error, or push failure;
/// (wasm) missing `build_cmd`/`artifact_path`, build-command failure, a missing
/// produced artifact, or `oras push` failure.
#[allow(clippy::too_many_arguments)]
pub async fn run_build(
    job: &BuildJob,
    backend: &dyn crate::build_backend::BuildBackend,
    git: &crate::git::GitRun,
    push_runner: &crate::docker::CommandRunner,
    docker_bin: &str,
    oras_runner: &crate::docker::CommandRunner,
    build_cmd_runner: &BuildCmdRunner,
    oras_bin: &str,
    workdir: &Path,
) -> anyhow::Result<ArtifactRef> {
    // 1. Clone into <workdir>/src (shared by both build kinds).
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

    // Image/artifact ref: <registry_ula>/<tenant>/<app_uuid>:<git_ref> (shared).
    let reff = format!(
        "{}/{}/{}:{}",
        job.registry_ula, job.tenant, job.app_uuid, job.git_ref
    );

    match job.build_kind {
        BuildKind::Docker => {
            docker::run_docker_build(job, backend, push_runner, docker_bin, &src, reff).await
        }
        BuildKind::Wasm => {
            wasm::run_wasm_build(job, build_cmd_runner, oras_runner, oras_bin, &src, reff).await
        }
    }
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

    let oras_bin = std::env::var("SUPERVISOR_ORAS_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_ORAS_BIN.to_owned());

    let backend = crate::build_backend::HostDockerBackend::new(docker_bin.clone());
    let git = crate::git::real_git_run(git_bin);
    let push_runner = crate::docker::production_command_runner(docker_bin.clone());
    let oras_runner = crate::oras::production_oras_runner(oras_bin.clone());
    let build_cmd_runner = production_build_cmd_runner();

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
        &push_runner,
        &docker_bin,
        &oras_runner,
        &build_cmd_runner,
        &oras_bin,
        workdir.path(),
    )
    .await
}


#[cfg(test)]
mod tests;
