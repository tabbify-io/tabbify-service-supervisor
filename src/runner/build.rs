//! One-shot builder mode for `tabbify-runner`.
//!
//! When launched with `--build-spec <file>`, the runner reads a [`BuildJob`] from
//! the JSON file, runs the build pipeline end-to-end, prints the resulting
//! [`ArtifactRef`] as JSON to stdout, and exits — it never joins the mesh or
//! starts a serve loop.
//!
//! The orchestration pipeline (`run_build`) is fully injection-seamed so tests
//! can drive clone/build/push without any real git or Docker daemon.  The
//! production wiring (real `git`, `docker`) lives only in `run_one_shot_build`.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::runtime::BoxFut;

/// Which build pipeline a [`BuildJob`] drives.
///
/// Absent in the JSON spec ⇒ [`BuildKind::Docker`] (the original behaviour), so
/// every pre-existing docker job + test is unchanged. [`BuildKind::Wasm`] selects
/// the additive wasm-component build path (`build_cmd` → `oras push`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
            run_docker_build(job, backend, push_runner, docker_bin, &src, reff).await
        }
        BuildKind::Wasm => {
            run_wasm_build(job, build_cmd_runner, oras_runner, oras_bin, &src, reff).await
        }
    }
}

/// The DOCKER build path (unchanged behaviour): require a `Dockerfile`, build
/// the local image via `backend`, then tag + push to the mesh registry.
///
/// # Errors
/// Missing `Dockerfile`, build error, or push failure.
async fn run_docker_build(
    job: &BuildJob,
    backend: &dyn crate::build_backend::BuildBackend,
    push_runner: &crate::docker::CommandRunner,
    docker_bin: &str,
    src: &Path,
    reff: String,
) -> anyhow::Result<ArtifactRef> {
    // Require a Dockerfile.
    if !src.join("Dockerfile").is_file() {
        anyhow::bail!(
            "no Dockerfile in {} (set build_kind=wasm for a wasm-component build)",
            src.display()
        );
    }

    // Build the local image. Local tag is scoped to this build so concurrent
    // builds don't collide.
    let local_tag = format!("tbf-build-{}", job.app_uuid);
    backend
        .build(src, &local_tag)
        .await
        .context("build image")?;

    // Tag + push to the mesh registry.
    let pushed = crate::docker::push_image(docker_bin, &local_tag, &reff, push_runner).await;
    if !pushed {
        anyhow::bail!("push to registry failed: {reff}");
    }

    Ok(ArtifactRef { reff, digest: None })
}

/// The WASM build path: run `job.build_cmd` in the cloned `src` dir, verify the
/// produced `.wasm` at `job.artifact_path`, then `oras push` it to the mesh
/// registry as a wasm OCI artifact.
///
/// # Errors
/// Missing `build_cmd`/`artifact_path`, a failing build command, a missing
/// produced artifact, or an `oras push` failure.
async fn run_wasm_build(
    job: &BuildJob,
    build_cmd_runner: &BuildCmdRunner,
    oras_runner: &crate::docker::CommandRunner,
    oras_bin: &str,
    src: &Path,
    reff: String,
) -> anyhow::Result<ArtifactRef> {
    // Require build_cmd + artifact_path for a wasm job.
    let build_cmd = job.build_cmd.as_deref().ok_or_else(|| {
        anyhow::anyhow!("wasm build job requires `build_cmd` (the command that produces the .wasm)")
    })?;
    let artifact_path = job.artifact_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "wasm build job requires `artifact_path` (path to the produced .wasm, relative to repo root)"
        )
    })?;

    // Run the build command in the cloned source dir.
    let built = (build_cmd_runner)(build_cmd.to_owned(), src.to_path_buf()).await;
    if !built {
        anyhow::bail!("wasm build command failed: {build_cmd}");
    }

    // Verify the produced artifact exists at <src>/<artifact_path>.
    let artifact_abs = src.join(artifact_path);
    if !artifact_abs.is_file() {
        anyhow::bail!(
            "wasm build produced no artifact at {} (expected from build_cmd `{build_cmd}`)",
            artifact_abs.display()
        );
    }

    // oras push the wasm artifact to the mesh registry.
    let artifact_abs_str = artifact_abs.to_string_lossy().into_owned();
    let pushed = crate::oras::oras_push(oras_bin, &reff, &artifact_abs_str, oras_runner).await;
    if !pushed {
        anyhow::bail!("oras push to registry failed: {reff}");
    }

    Ok(ArtifactRef { reff, digest: None })
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
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::build_backend::BuildBackend;
    use crate::docker::CommandRunner;
    use crate::git::GitRun;
    use crate::runtime::BoxFut;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Minimal fake [`BuildBackend`]: records `(context_dir, tag)` on each
    /// `build` call and returns `Ok(())`.
    struct FakeBackend {
        built: Arc<Mutex<Option<(PathBuf, String)>>>,
    }

    impl BuildBackend for FakeBackend {
        fn build<'a>(
            &'a self,
            context_dir: &'a Path,
            tag: &'a str,
        ) -> BoxFut<'a, anyhow::Result<()>> {
            let slot = self.built.clone();
            let dir = context_dir.to_path_buf();
            let tag = tag.to_owned();
            Box::pin(async move {
                *slot.lock().unwrap() = Some((dir, tag));
                Ok(())
            })
        }
    }

    /// Fake [`BuildBackend`] that always fails.
    struct FailBackend;
    impl BuildBackend for FailBackend {
        fn build<'a>(&'a self, _ctx: &'a Path, _tag: &'a str) -> BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async { anyhow::bail!("fake build failure") })
        }
    }

    /// A [`CommandRunner`] that records every argv list it receives and returns
    /// `true` (success) for all calls.
    fn record_runner(store: Arc<Mutex<Vec<Vec<String>>>>) -> CommandRunner {
        Arc::new(move |args: Vec<String>| {
            store.lock().unwrap().push(args);
            let fut: BoxFut<'static, bool> = Box::pin(async { true });
            fut
        })
    }

    /// A [`CommandRunner`] that always returns `false` (failure).
    fn fail_runner() -> CommandRunner {
        Arc::new(|_args: Vec<String>| Box::pin(async { false }) as BoxFut<'static, bool>)
    }

    /// A no-op [`CommandRunner`] (returns `true`, records nothing). Used for the
    /// runner the active build path ignores (e.g. the oras runner on a docker
    /// build, or the docker push runner on a wasm build).
    fn noop_runner() -> CommandRunner {
        Arc::new(|_args: Vec<String>| Box::pin(async { true }) as BoxFut<'static, bool>)
    }

    /// A no-op [`BuildCmdRunner`] (returns `true`, runs nothing). Used as the
    /// build-cmd runner on the DOCKER path, which never invokes it.
    fn noop_build_cmd_runner() -> BuildCmdRunner {
        Arc::new(|_cmd: String, _cwd: PathBuf| Box::pin(async { true }) as BoxFut<'static, bool>)
    }

    /// A [`BuildCmdRunner`] that simulates a successful wasm build: it writes the
    /// expected `.wasm` at `<cwd>/<artifact_rel>` and records `(cmd, cwd)`.
    /// `artifact_rel` is the repo-relative artifact path the job declares.
    fn build_cmd_runner_writing_artifact(
        artifact_rel: &str,
        recorded: Arc<Mutex<Vec<(String, PathBuf)>>>,
    ) -> BuildCmdRunner {
        let artifact_rel = artifact_rel.to_owned();
        Arc::new(move |cmd: String, cwd: PathBuf| {
            let artifact_rel = artifact_rel.clone();
            let recorded = recorded.clone();
            Box::pin(async move {
                let out = cwd.join(&artifact_rel);
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent).ok();
                }
                std::fs::write(&out, b"\0asm\x01\0\0\0").unwrap();
                recorded.lock().unwrap().push((cmd, cwd));
                true
            }) as BoxFut<'static, bool>
        })
    }

    /// A [`BuildCmdRunner`] that simulates a FAILED build: records the call and
    /// returns `false` WITHOUT writing any artifact.
    fn failing_build_cmd_runner(recorded: Arc<Mutex<Vec<(String, PathBuf)>>>) -> BuildCmdRunner {
        Arc::new(move |cmd: String, cwd: PathBuf| {
            let recorded = recorded.clone();
            Box::pin(async move {
                recorded.lock().unwrap().push((cmd, cwd));
                false
            }) as BoxFut<'static, bool>
        })
    }

    /// A [`GitRun`] that creates a `Dockerfile` inside `dest` to simulate a
    /// successful clone, and records the destination path.
    fn git_with_dockerfile(cloned_to: Arc<Mutex<Option<String>>>) -> GitRun {
        Arc::new(move |args: Vec<String>, _env| {
            let dest = args.last().cloned().unwrap_or_default();
            let slot = cloned_to.clone();
            Box::pin(async move {
                std::fs::create_dir_all(&dest).ok();
                std::fs::write(format!("{dest}/Dockerfile"), "FROM scratch\n").unwrap();
                *slot.lock().unwrap() = Some(dest);
                Ok(())
            })
        })
    }

    /// A [`GitRun`] that creates the destination directory but does NOT write a
    /// `Dockerfile` (simulates a non-Docker repo).
    fn git_without_dockerfile() -> GitRun {
        Arc::new(|args: Vec<String>, _env| {
            let dest = args.last().cloned().unwrap_or_default();
            Box::pin(async move {
                std::fs::create_dir_all(&dest).ok();
                Ok(())
            })
        })
    }

    /// Canonical minimal DOCKER [`BuildJob`] for tests (build_kind defaults to
    /// Docker so the docker path is exercised).
    fn test_job() -> BuildJob {
        BuildJob {
            repo_url: "https://github.com/acme/app".into(),
            git_ref: "abc123".into(),
            tenant: "acme".into(),
            app_uuid: "u".into(),
            registry_ula: "[fd5a::1]:5000".into(),
            clone_token: None,
            push_token: None,
            build_kind: BuildKind::Docker,
            build_cmd: None,
            artifact_path: None,
        }
    }

    /// A WASM [`BuildJob`] with the given build command + artifact path.
    fn wasm_job(build_cmd: Option<&str>, artifact_path: Option<&str>) -> BuildJob {
        BuildJob {
            repo_url: "https://github.com/acme/app".into(),
            git_ref: "abc123".into(),
            tenant: "acme".into(),
            app_uuid: "u".into(),
            registry_ula: "[fd5a::1]:5000".into(),
            clone_token: None,
            push_token: None,
            build_kind: BuildKind::Wasm,
            build_cmd: build_cmd.map(str::to_owned),
            artifact_path: artifact_path.map(str::to_owned),
        }
    }

    /// Convenience wrapper around [`run_build`] for DOCKER tests: passes no-op
    /// runners for the wasm-path seams (which the docker path ignores).
    async fn run_docker_test(
        job: &BuildJob,
        backend: &dyn BuildBackend,
        git: &GitRun,
        push_runner: &CommandRunner,
        docker_bin: &str,
        workdir: &Path,
    ) -> anyhow::Result<ArtifactRef> {
        run_build(
            job,
            backend,
            git,
            push_runner,
            docker_bin,
            &noop_runner(),
            &noop_build_cmd_runner(),
            "oras",
            workdir,
        )
        .await
    }

    // ── serde round-trips ─────────────────────────────────────────────────────

    #[test]
    fn build_job_round_trips_json() {
        let job = BuildJob {
            repo_url: "https://github.com/acme/app".into(),
            git_ref: "main".into(),
            tenant: "acme".into(),
            app_uuid: "11111111-1111-1111-1111-111111111111".into(),
            registry_ula: "[fd5a:1f02:aa::1]:5000".into(),
            clone_token: Some("ght_xxx".into()),
            push_token: None,
            build_kind: BuildKind::Docker,
            build_cmd: None,
            artifact_path: None,
        };
        let s = serde_json::to_string(&job).unwrap();
        assert_eq!(serde_json::from_str::<BuildJob>(&s).unwrap(), job);
    }

    #[test]
    fn optional_tokens_default_to_none() {
        let json = r#"{"repo_url":"r","git_ref":"v1","tenant":"t","app_uuid":"u","registry_ula":"[::1]:5000"}"#;
        let job: BuildJob = serde_json::from_str(json).unwrap();
        assert!(job.clone_token.is_none() && job.push_token.is_none());
    }

    // ── run_build (injected) ──────────────────────────────────────────────────

    /// Happy path: clone creates Dockerfile → build → push → correct ArtifactRef.
    #[tokio::test]
    async fn run_build_clones_builds_pushes_and_returns_ref() {
        let dir = tempfile::tempdir().unwrap();
        let cloned = Arc::new(Mutex::new(None));
        let git = git_with_dockerfile(cloned.clone());

        let built = Arc::new(Mutex::new(None));
        let backend = FakeBackend {
            built: built.clone(),
        };

        let pushed = Arc::new(Mutex::new(Vec::new()));
        let push_runner = record_runner(pushed.clone());

        let job = test_job();
        let art = run_docker_test(&job, &backend, &git, &push_runner, "docker", dir.path())
            .await
            .unwrap();

        // ArtifactRef must encode the full registry path.
        assert_eq!(art.reff, "[fd5a::1]:5000/acme/u:abc123");
        assert!(art.digest.is_none());

        // backend.build must have been called with (<workdir>/src, local-tag).
        let b = built.lock().unwrap();
        assert!(b.is_some(), "backend.build must be called");
        let (ctx, tag) = b.as_ref().unwrap();
        assert!(
            ctx.ends_with("src"),
            "build context must be <workdir>/src, got {ctx:?}"
        );
        assert_eq!(tag, "tbf-build-u");

        // push runner must have been called with push argv containing the ref.
        let cmds = pushed.lock().unwrap();
        assert!(
            cmds.iter().any(|c| c.contains(&"push".to_string())),
            "push command must be issued; got {cmds:?}"
        );
        assert!(
            cmds.iter()
                .any(|c| c.iter().any(|a| a.contains("[fd5a::1]:5000/acme/u:abc123"))),
            "push argv must contain the registry ref; got {cmds:?}"
        );
    }

    /// When the cloned source has no Dockerfile, `run_build` must fail with a
    /// clear "no Dockerfile" error.
    #[tokio::test]
    async fn run_build_errors_when_no_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FakeBackend {
            built: Arc::new(Mutex::new(None)),
        };
        let push_runner = fail_runner();

        let job = test_job();
        let err = run_docker_test(&job, &backend, &git, &push_runner, "docker", dir.path())
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.to_lowercase().contains("dockerfile"),
            "error must mention Dockerfile; got: {err}"
        );
    }

    /// When the push step fails, `run_build` must return an error referencing
    /// the registry ref.
    #[tokio::test]
    async fn run_build_errors_when_push_fails() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
        let backend = FakeBackend {
            built: Arc::new(Mutex::new(None)),
        };
        let push_runner = fail_runner();

        let job = test_job();
        let err = run_docker_test(&job, &backend, &git, &push_runner, "docker", dir.path())
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("push to registry failed"),
            "error must mention push failure; got: {err}"
        );
        assert!(
            err.contains("[fd5a::1]:5000/acme/u:abc123"),
            "error must contain the registry ref; got: {err}"
        );
    }

    /// When the build step fails, `run_build` must propagate the error.
    #[tokio::test]
    async fn run_build_errors_when_build_fails() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
        let backend = FailBackend;
        let push_runner = fail_runner();

        let err = run_docker_test(
            &test_job(),
            &backend,
            &git,
            &push_runner,
            "docker",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("build image") || err.contains("build"),
            "error must reference build step; got: {err}"
        );
    }

    /// The computed registry ref must follow `<ula>/<tenant>/<uuid>:<git_ref>`.
    #[tokio::test]
    async fn run_build_ref_format_is_correct() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
        let backend = FakeBackend {
            built: Arc::new(Mutex::new(None)),
        };
        let pushed = Arc::new(Mutex::new(Vec::new()));
        let push_runner = record_runner(pushed);

        let job = BuildJob {
            repo_url: "https://github.com/example/repo".into(),
            git_ref: "deadbeef".into(),
            tenant: "myteam".into(),
            app_uuid: "my-app-uuid".into(),
            registry_ula: "[fd5a:1f02:cc::1]:5000".into(),
            clone_token: None,
            push_token: None,
            build_kind: BuildKind::Docker,
            build_cmd: None,
            artifact_path: None,
        };
        let art = run_docker_test(&job, &backend, &git, &push_runner, "docker", dir.path())
            .await
            .unwrap();

        assert_eq!(
            art.reff,
            "[fd5a:1f02:cc::1]:5000/myteam/my-app-uuid:deadbeef"
        );
    }

    // ── run_build wasm path (injected) ────────────────────────────────────────

    /// Happy path: clone → build cmd writes the `.wasm` → oras push → correct
    /// ArtifactRef. Asserts the build cmd ran (with the cloned src as cwd) and
    /// that oras pushed the produced artifact.
    #[tokio::test]
    async fn run_build_wasm_runs_build_cmd_and_oras_pushes() {
        let dir = tempfile::tempdir().unwrap();
        // Clone just creates the src dir (no Dockerfile needed for wasm).
        let git = git_without_dockerfile();
        // Docker backend must NOT be used on the wasm path.
        let backend = FailBackend;
        // Docker push runner must NOT be used on the wasm path.
        let push_runner = fail_runner();

        let artifact_rel = "target/wasm32-wasip2/release/app.wasm";
        let cmd_calls = Arc::new(Mutex::new(Vec::new()));
        let build_cmd_runner = build_cmd_runner_writing_artifact(artifact_rel, cmd_calls.clone());

        let oras_calls = Arc::new(Mutex::new(Vec::new()));
        let oras_runner = record_runner(oras_calls.clone());

        let job = wasm_job(
            Some("cargo build --release --target wasm32-wasip2"),
            Some(artifact_rel),
        );
        let art = run_build(
            &job,
            &backend,
            &git,
            &push_runner,
            "docker",
            &oras_runner,
            &build_cmd_runner,
            "oras",
            dir.path(),
        )
        .await
        .unwrap();

        // Correct ref (same scheme as docker) + no digest.
        assert_eq!(art.reff, "[fd5a::1]:5000/acme/u:abc123");
        assert!(art.digest.is_none());

        // The build command ran exactly once with cwd = <workdir>/src.
        let calls = cmd_calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "build cmd must run exactly once");
        let (cmd, cwd) = &calls[0];
        assert_eq!(cmd, "cargo build --release --target wasm32-wasip2");
        assert!(
            cwd.ends_with("src"),
            "build cmd cwd must be <workdir>/src; got {cwd:?}"
        );

        // oras push ran with a push argv carrying the ref and the wasm media type
        // on the ABSOLUTE artifact path (<src>/<artifact_rel>).
        let pushes = oras_calls.lock().unwrap();
        assert_eq!(pushes.len(), 1, "oras push must run exactly once");
        let argv = &pushes[0];
        assert_eq!(argv[0], "push", "must be an oras push; got {argv:?}");
        assert!(
            argv.iter()
                .any(|a| a.contains("[fd5a::1]:5000/acme/u:abc123")),
            "push argv must contain the registry ref; got {argv:?}"
        );
        let expected_abs = dir.path().join("src").join(artifact_rel);
        let expected_file_arg = format!("{}:application/wasm", expected_abs.to_string_lossy());
        assert!(
            argv.contains(&expected_file_arg),
            "push argv must carry <abs-artifact>:application/wasm; want {expected_file_arg}, got {argv:?}"
        );
    }

    /// A wasm job missing `build_cmd` must error before running anything.
    #[tokio::test]
    async fn run_build_wasm_errors_when_build_cmd_missing() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FailBackend;
        let oras_calls = Arc::new(Mutex::new(Vec::new()));
        let oras_runner = record_runner(oras_calls.clone());

        // artifact_path present but build_cmd absent.
        let job = wasm_job(None, Some("target/app.wasm"));
        let err = run_build(
            &job,
            &backend,
            &git,
            &fail_runner(),
            "docker",
            &oras_runner,
            &noop_build_cmd_runner(),
            "oras",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("build_cmd"),
            "error must mention build_cmd; got: {err}"
        );
        assert!(
            oras_calls.lock().unwrap().is_empty(),
            "oras push must NOT run when build_cmd is missing"
        );
    }

    /// A wasm job missing `artifact_path` must error.
    #[tokio::test]
    async fn run_build_wasm_errors_when_artifact_path_missing() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FailBackend;
        let oras_calls = Arc::new(Mutex::new(Vec::new()));
        let oras_runner = record_runner(oras_calls.clone());

        // build_cmd present but artifact_path absent.
        let job = wasm_job(Some("make wasm"), None);
        let err = run_build(
            &job,
            &backend,
            &git,
            &fail_runner(),
            "docker",
            &oras_runner,
            &noop_build_cmd_runner(),
            "oras",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("artifact_path"),
            "error must mention artifact_path; got: {err}"
        );
        assert!(
            oras_calls.lock().unwrap().is_empty(),
            "oras push must NOT run when artifact_path is missing"
        );
    }

    /// When the build command fails, `run_build` must error and NOT oras-push.
    #[tokio::test]
    async fn run_build_wasm_errors_when_build_cmd_fails_and_does_not_push() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FailBackend;

        let cmd_calls = Arc::new(Mutex::new(Vec::new()));
        let build_cmd_runner = failing_build_cmd_runner(cmd_calls.clone());

        let oras_calls = Arc::new(Mutex::new(Vec::new()));
        let oras_runner = record_runner(oras_calls.clone());

        let job = wasm_job(Some("cargo build --target wasm32-wasip2"), Some("app.wasm"));
        let err = run_build(
            &job,
            &backend,
            &git,
            &fail_runner(),
            "docker",
            &oras_runner,
            &build_cmd_runner,
            "oras",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("wasm build command failed"),
            "error must mention the build command failure; got: {err}"
        );
        // Build cmd was attempted...
        assert_eq!(
            cmd_calls.lock().unwrap().len(),
            1,
            "build cmd must be attempted"
        );
        // ...but no push happened.
        assert!(
            oras_calls.lock().unwrap().is_empty(),
            "oras push must NOT run when the build command fails"
        );
    }

    /// When the build command "succeeds" but produces no artifact at the
    /// declared path, `run_build` must error and NOT oras-push.
    #[tokio::test]
    async fn run_build_wasm_errors_when_artifact_absent_after_build() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FailBackend;

        // Build cmd returns true but writes nothing.
        let build_cmd_runner: BuildCmdRunner = noop_build_cmd_runner();

        let oras_calls = Arc::new(Mutex::new(Vec::new()));
        let oras_runner = record_runner(oras_calls.clone());

        let job = wasm_job(Some("true"), Some("target/app.wasm"));
        let err = run_build(
            &job,
            &backend,
            &git,
            &fail_runner(),
            "docker",
            &oras_runner,
            &build_cmd_runner,
            "oras",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("produced no artifact"),
            "error must mention the missing artifact; got: {err}"
        );
        assert!(
            oras_calls.lock().unwrap().is_empty(),
            "oras push must NOT run when the artifact is absent"
        );
    }

    /// When the build + artifact succeed but `oras push` fails, `run_build` must
    /// error referencing the registry ref.
    #[tokio::test]
    async fn run_build_wasm_errors_when_oras_push_fails() {
        let dir = tempfile::tempdir().unwrap();
        let git = git_without_dockerfile();
        let backend = FailBackend;

        let artifact_rel = "out/app.wasm";
        let cmd_calls = Arc::new(Mutex::new(Vec::new()));
        let build_cmd_runner = build_cmd_runner_writing_artifact(artifact_rel, cmd_calls.clone());

        // oras push fails.
        let oras_runner = fail_runner();

        let job = wasm_job(Some("make"), Some(artifact_rel));
        let err = run_build(
            &job,
            &backend,
            &git,
            &fail_runner(),
            "docker",
            &oras_runner,
            &build_cmd_runner,
            "oras",
            dir.path(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("oras push to registry failed"),
            "error must mention oras push failure; got: {err}"
        );
        assert!(
            err.contains("[fd5a::1]:5000/acme/u:abc123"),
            "error must contain the registry ref; got: {err}"
        );
    }

    /// `build_kind` defaults to Docker when absent from the JSON spec, so a spec
    /// with no `build_kind` parses as a docker job (existing jobs unchanged).
    #[test]
    fn build_kind_defaults_to_docker_when_absent() {
        let json = r#"{"repo_url":"r","git_ref":"v1","tenant":"t","app_uuid":"u","registry_ula":"[::1]:5000"}"#;
        let job: BuildJob = serde_json::from_str(json).unwrap();
        assert_eq!(job.build_kind, BuildKind::Docker);
        assert!(job.build_cmd.is_none() && job.artifact_path.is_none());
    }

    /// A wasm spec round-trips through JSON with lowercase `"wasm"` and its
    /// build fields preserved.
    #[test]
    fn wasm_build_job_round_trips_json() {
        let job = wasm_job(
            Some("cargo build --release --target wasm32-wasip2"),
            Some("target/wasm32-wasip2/release/app.wasm"),
        );
        let s = serde_json::to_string(&job).unwrap();
        assert!(
            s.contains("\"build_kind\":\"wasm\""),
            "build_kind must serialize lowercase; got {s}"
        );
        assert_eq!(serde_json::from_str::<BuildJob>(&s).unwrap(), job);
    }

    // ── run_one_shot_build (I/O + parse) ──────────────────────────────────────

    /// A missing spec file must return a read error (not a panic).
    #[tokio::test]
    async fn run_one_shot_build_rejects_missing_file() {
        let err = run_one_shot_build(Path::new("/does/not/exist.json"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("read build spec"),
            "expected read error, got: {err}"
        );
    }

    #[tokio::test]
    async fn run_one_shot_build_rejects_bad_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not json").unwrap();
        let err = run_one_shot_build(&path).await.unwrap_err().to_string();
        assert!(
            err.contains("parse build spec"),
            "expected parse error, got: {err}"
        );
    }
}
