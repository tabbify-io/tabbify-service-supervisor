//! Tests for [`super`] — the runner build pipeline.
#![allow(clippy::unwrap_used)]

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
    fn build<'a>(&'a self, context_dir: &'a Path, tag: &'a str) -> BoxFut<'a, anyhow::Result<()>> {
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
/// `Ok(())` (success) for all calls.
fn record_runner(store: Arc<Mutex<Vec<Vec<String>>>>) -> CommandRunner {
    Arc::new(move |args: Vec<String>| {
        store.lock().unwrap().push(args);
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async { Ok(()) });
        fut
    })
}

/// A [`CommandRunner`] that always returns `Err` (failure), carrying a
/// stderr-like diagnostic.
fn fail_runner() -> CommandRunner {
    Arc::new(|_args: Vec<String>| {
        Box::pin(async { Err("command failed".to_owned()) }) as BoxFut<'static, Result<(), String>>
    })
}

/// Extract the clone destination from ONE step of the real
/// [`crate::git::clone`] argv sequence. Only `git init -q <dest>`
/// carries the dest as its last argument; the later steps end with
/// the repo URL (`remote add`), the git ref (`fetch`), or the literal
/// `FETCH_HEAD` (`checkout`). A fake that blindly used `args.last()`
/// for every step used to create those tokens as RELATIVE DIRS in the
/// crate root (`FETCH_HEAD/`, `abc123/`, `https:/…`) on every test
/// run.
fn init_dest(args: &[String]) -> Option<String> {
    (args.first().map(String::as_str) == Some("init"))
        .then(|| args.last().cloned())
        .flatten()
}

/// A [`GitRun`] that creates a `Dockerfile` inside `dest` to simulate a
/// successful clone, and records the destination path. Only the
/// `init` step materializes anything; the other steps are no-op Ok.
fn git_with_dockerfile(cloned_to: Arc<Mutex<Option<String>>>) -> GitRun {
    Arc::new(move |args: Vec<String>, _env| {
        let dest = init_dest(&args);
        let slot = cloned_to.clone();
        Box::pin(async move {
            if let Some(dest) = dest {
                std::fs::create_dir_all(&dest).ok();
                std::fs::write(format!("{dest}/Dockerfile"), "FROM scratch\n").unwrap();
                *slot.lock().unwrap() = Some(dest);
            }
            Ok(())
        })
    })
}

/// A [`GitRun`] that creates the destination directory but does NOT write a
/// `Dockerfile` (simulates a non-Docker repo). Only the `init` step
/// creates the dir.
fn git_without_dockerfile() -> GitRun {
    Arc::new(|args: Vec<String>, _env| {
        let dest = init_dest(&args);
        Box::pin(async move {
            if let Some(dest) = dest {
                std::fs::create_dir_all(&dest).ok();
            }
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

/// Convenience wrapper around [`run_build`] for DOCKER tests.
///
/// The docker path pushes via the supervisor-side `skopeo`, so `skopeo_runner`
/// is the runner that records the registry-push argv.
async fn run_docker_test(
    job: &BuildJob,
    backend: &dyn BuildBackend,
    git: &GitRun,
    skopeo_runner: &CommandRunner,
    skopeo_bin: &str,
    workdir: &Path,
) -> anyhow::Result<ArtifactRef> {
    run_build(job, backend, git, skopeo_runner, skopeo_bin, workdir).await
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

/// Happy path: clone creates Dockerfile → build → skopeo push → correct
/// ArtifactRef. The registry push is a `skopeo copy docker-daemon:<tag>:latest
/// docker://<reff>` issued by the supervisor-side skopeo runner.
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
    let skopeo_runner = record_runner(pushed.clone());

    let job = test_job();
    let art = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
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

    // skopeo runner must have been called with a `copy` argv that reads from
    // the local docker daemon and writes the registry ref via docker://.
    let cmds = pushed.lock().unwrap();
    assert!(
        cmds.iter().any(|c| c.first() == Some(&"copy".to_string())),
        "skopeo copy command must be issued; got {cmds:?}"
    );
    assert!(
        cmds.iter()
            .any(|c| c.contains(&"docker-daemon:tbf-build-u:latest".to_string())),
        "skopeo argv must read the built image from the local docker daemon; got {cmds:?}"
    );
    assert!(
        cmds.iter()
            .any(|c| c.contains(&"docker://[fd5a::1]:5000/acme/u:abc123".to_string())),
        "skopeo argv must push to docker://<reff>; got {cmds:?}"
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
    let skopeo_runner = fail_runner();

    let job = test_job();
    let err = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
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
    let skopeo_runner = fail_runner();

    let job = test_job();
    let err = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
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
    let skopeo_runner = fail_runner();

    let err = run_docker_test(
        &test_job(),
        &backend,
        &git,
        &skopeo_runner,
        "skopeo",
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
    let skopeo_runner = record_runner(pushed);

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
    let art = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    assert_eq!(
        art.reff,
        "[fd5a:1f02:cc::1]:5000/myteam/my-app-uuid:deadbeef"
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
