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

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

/// A one-shot build job: clone `repo_url`@`git_ref`, build an OCI image, push it
/// to the mesh registry at `registry_ula` as `<tenant>/<app_uuid>:<sha>`.
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
/// Steps:
/// 1. Clone `job.repo_url`@`job.git_ref` into `<workdir>/src` via `git`.
/// 2. Detect build kind: require a `Dockerfile` in the cloned source tree.
///    WASM-component detection is a follow-up.
/// 3. Build the OCI image locally via `backend.build`.
/// 4. Tag + push to `job.registry_ula/<tenant>/<app_uuid>:<git_ref>` via
///    `push_runner`.  The `git_ref` is used verbatim as the tag component; the
///    control-plane must supply an immutable SHA.
///
/// All dependencies are injected so the function is fully unit-testable without
/// a real git binary or Docker daemon.
///
/// # Errors
/// Clone failure, missing `Dockerfile`, build error, or push failure.
pub async fn run_build(
    job: &BuildJob,
    backend: &dyn crate::build_backend::BuildBackend,
    git: &crate::git::GitRun,
    push_runner: &crate::docker::CommandRunner,
    docker_bin: &str,
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

    // 2. Require a Dockerfile (MVP; wasm-component detection is a follow-up).
    if !src.join("Dockerfile").is_file() {
        anyhow::bail!(
            "no Dockerfile in {} (wasm-component builds are a follow-up)",
            src.display()
        );
    }

    // 3. Build the local image.
    //    Local tag is scoped to this build so concurrent builds don't collide.
    let local_tag = format!("tbf-build-{}", job.app_uuid);
    backend
        .build(&src, &local_tag)
        .await
        .context("build image")?;

    // 4. Tag + push to the mesh registry.
    //    Image ref: <registry_ula>/<tenant>/<app_uuid>:<git_ref>
    let reff = format!(
        "{}/{}/{}:{}",
        job.registry_ula, job.tenant, job.app_uuid, job.git_ref
    );
    let pushed = crate::docker::push_image(docker_bin, &local_tag, &reff, push_runner).await;
    if !pushed {
        anyhow::bail!("push to registry failed: {reff}");
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

    let backend = crate::build_backend::HostDockerBackend::new(docker_bin.clone());
    let git = crate::git::real_git_run(git_bin);
    let push_runner = crate::docker::production_command_runner(docker_bin.clone());

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

    /// Canonical minimal [`BuildJob`] for tests.
    fn test_job() -> BuildJob {
        BuildJob {
            repo_url: "https://github.com/acme/app".into(),
            git_ref: "abc123".into(),
            tenant: "acme".into(),
            app_uuid: "u".into(),
            registry_ula: "[fd5a::1]:5000".into(),
            clone_token: None,
            push_token: None,
        }
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
        let art = run_build(&job, &backend, &git, &push_runner, "docker", dir.path())
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
        let err = run_build(&job, &backend, &git, &push_runner, "docker", dir.path())
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
        let err = run_build(&job, &backend, &git, &push_runner, "docker", dir.path())
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

        let err = run_build(
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
        };
        let art = run_build(&job, &backend, &git, &push_runner, "docker", dir.path())
            .await
            .unwrap();

        assert_eq!(
            art.reff,
            "[fd5a:1f02:cc::1]:5000/myteam/my-app-uuid:deadbeef"
        );
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
