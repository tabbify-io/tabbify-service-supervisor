//! Tests for [`super`] — the runner build pipeline.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::*;
use crate::build_backend::BuildBackend;
use crate::docker::CommandRunner;
use crate::git::{GitCapture, GitRun};
use crate::runtime::BoxFut;

// ── helpers ──────────────────────────────────────────────────────────────

/// The fixed 40-hex commit SHA the test [`GitCapture`] resolves the clone HEAD
/// to. The builder tags the image with THIS (not `job.git_ref`), so the
/// expected `reff` tag component is this value, not the input ref.
const TEST_HEAD_SHA: &str = "c64f621abcdef0123456789abcdef0123456789a";

/// A [`GitCapture`] that resolves the clone HEAD to [`TEST_HEAD_SHA`] — the
/// production seam runs `git rev-parse HEAD`; the fake just returns a valid
/// 40-hex SHA so the SHA-tag path is exercised hermetically.
fn git_capture_head_sha() -> GitCapture {
    Arc::new(|_args: Vec<String>| Box::pin(async { Ok(format!("{TEST_HEAD_SHA}\n")) }))
}

/// The `(context_dir, tag, dockerfile)` recorded by [`FakeBackend`] on a build.
type BuiltCall = (PathBuf, String, Option<PathBuf>);

/// Minimal fake [`BuildBackend`]: records `(context_dir, tag, dockerfile)` on
/// each `build` call and returns `Ok(())`.
struct FakeBackend {
    built: Arc<Mutex<Option<BuiltCall>>>,
}

impl BuildBackend for FakeBackend {
    fn build<'a>(
        &'a self,
        context_dir: &'a Path,
        tag: &'a str,
        dockerfile: Option<&'a Path>,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        let slot = self.built.clone();
        let dir = context_dir.to_path_buf();
        let tag = tag.to_owned();
        let df = dockerfile.map(Path::to_path_buf);
        Box::pin(async move {
            *slot.lock().unwrap() = Some((dir, tag, df));
            Ok(())
        })
    }
}

/// Fake [`BuildBackend`] that always fails.
struct FailBackend;
impl BuildBackend for FailBackend {
    fn build<'a>(
        &'a self,
        _ctx: &'a Path,
        _tag: &'a str,
        _dockerfile: Option<&'a Path>,
    ) -> BoxFut<'a, anyhow::Result<()>> {
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

/// A [`GitRun`] that simulates a clone of a repo that ships its OWN
/// `tabbify.toml` (plus a `Dockerfile`). Used by the repo-wins test: the inject
/// step must NOT overwrite the repo's own toml.
fn git_with_dockerfile_and_own_toml(own_toml: &'static str) -> GitRun {
    Arc::new(move |args: Vec<String>, _env| {
        let dest = init_dest(&args);
        Box::pin(async move {
            if let Some(dest) = dest {
                std::fs::create_dir_all(&dest).ok();
                std::fs::write(format!("{dest}/Dockerfile"), "FROM scratch\n").unwrap();
                std::fs::write(format!("{dest}/tabbify.toml"), own_toml).unwrap();
            }
            Ok(())
        })
    })
}

/// A [`GitRun`] that simulates a clone shipping its own `tabbify.toml` pointing
/// `[build].dockerfile` at a NON-ROOT path (`deploy/Dockerfile`), and creates
/// that Dockerfile under the subdir. Used by the honor-[build] test.
fn git_with_toml_custom_dockerfile() -> GitRun {
    Arc::new(move |args: Vec<String>, _env| {
        let dest = init_dest(&args);
        Box::pin(async move {
            if let Some(dest) = dest {
                std::fs::create_dir_all(format!("{dest}/deploy")).ok();
                std::fs::write(format!("{dest}/deploy/Dockerfile"), "FROM scratch\n").unwrap();
                std::fs::write(
                    format!("{dest}/tabbify.toml"),
                    "[app]\nname = \"custom-df\"\n[build]\nkind = \"docker\"\ndockerfile = \"deploy/Dockerfile\"\n",
                )
                .unwrap();
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
        manifest_toml: None,
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
    tool_runner: &CommandRunner,
    skopeo_bin: &str,
    workdir: &Path,
) -> anyhow::Result<ArtifactRef> {
    let git_capture = git_capture_head_sha();
    run_build(
        job,
        backend,
        git,
        &git_capture,
        tool_runner,
        skopeo_bin,
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
        manifest_toml: None,
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

    // ArtifactRef must encode the full registry path, tagged with the resolved
    // IMMUTABLE commit SHA (NOT the input `git_ref` "abc123").
    assert_eq!(art.reff, format!("[fd5a::1]:5000/acme/u:{TEST_HEAD_SHA}"));
    assert!(art.digest.is_none());
    assert_eq!(
        art.commit_sha.as_deref(),
        Some(TEST_HEAD_SHA),
        "the resolved commit sha must be returned in the ArtifactRef"
    );

    // backend.build must have been called with (<workdir>/src, local-tag).
    let b = built.lock().unwrap();
    assert!(b.is_some(), "backend.build must be called");
    let (ctx, tag, _df) = b.as_ref().unwrap();
    assert!(
        ctx.ends_with("src"),
        "build context must be <workdir>/src, got {ctx:?}"
    );
    assert_eq!(tag, "tbf-build-u");

    // The two-step push must be issued through the tool runner (argv[0] =
    // binary): skopeo reads the built image out of the docker daemon into an
    // OCI layout, then oras pushes the layout to the registry reff (skopeo
    // cannot parse the bracketed-IPv6 registry ref).
    let cmds = pushed.lock().unwrap();
    assert!(
        cmds.iter().any(|c| {
            c.first() == Some(&"skopeo".to_string())
                && c.contains(&"docker-daemon:tbf-build-u:latest".to_string())
        }),
        "skopeo daemon->layout step must be issued; got {cmds:?}"
    );
    assert!(
        cmds.iter().any(|c| {
            c.first() == Some(&"oras".to_string())
                && c.contains(&format!("[fd5a::1]:5000/acme/u:{TEST_HEAD_SHA}"))
        }),
        "oras layout->registry step must push the sha-tagged reff; got {cmds:?}"
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
        err.contains(&format!("[fd5a::1]:5000/acme/u:{TEST_HEAD_SHA}")),
        "error must contain the sha-tagged registry ref; got: {err}"
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
        manifest_toml: None,
    };
    let art = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    // The tag component is the resolved IMMUTABLE commit SHA, NOT the input
    // `git_ref` "deadbeef" (which is a too-short, mutable-looking value).
    assert_eq!(
        art.reff,
        format!("[fd5a:1f02:cc::1]:5000/myteam/my-app-uuid:{TEST_HEAD_SHA}")
    );
}

/// A BRANCH-style `git_ref` (e.g. "main") must NOT leak into the image tag:
/// the builder re-resolves HEAD to the immutable commit SHA and tags with THAT.
/// This is the core TAB-10 fix — a mutable tag would let "deploy success" serve
/// a stale commit.
#[tokio::test]
async fn run_build_tags_with_resolved_sha_not_branch_ref() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
    let backend = FakeBackend {
        built: Arc::new(Mutex::new(None)),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let mut job = test_job();
    job.git_ref = "main".into(); // a MUTABLE branch ref

    let art = run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    assert!(
        art.reff.ends_with(&format!(":{TEST_HEAD_SHA}")),
        "image tag must be the resolved SHA, never the branch ref; got {}",
        art.reff
    );
    assert!(
        !art.reff.contains(":main"),
        "the mutable branch ref must NOT appear in the tag; got {}",
        art.reff
    );
}

/// FAIL-CLOSED at the build level: when the HEAD cannot be resolved to a valid
/// SHA (the capture seam yields garbage), `run_build` must abort — it must NOT
/// fall back to a mutable tag, and must NOT proceed to build/push.
#[tokio::test]
async fn run_build_fails_closed_when_head_unresolvable() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
    let built = Arc::new(Mutex::new(None));
    let backend = FakeBackend {
        built: built.clone(),
    };
    let pushed = Arc::new(Mutex::new(Vec::new()));
    let skopeo_runner = record_runner(pushed.clone());

    // Capture seam returns a non-SHA (e.g. a leaked branch name).
    let bad_capture: GitCapture =
        Arc::new(|_args: Vec<String>| Box::pin(async { Ok("main\n".to_owned()) }));

    let job = test_job();
    let err = run_build(
        &job,
        &backend,
        &git,
        &bad_capture,
        &skopeo_runner,
        "skopeo",
        "oras",
        dir.path(),
    )
    .await
    .expect_err("an unprovable commit sha must fail the build closed");

    assert!(
        err.to_string().contains("resolve clone commit sha")
            || format!("{err:#}").contains("commit sha"),
        "error must reference the failed sha resolution; got: {err:#}"
    );
    // Fail-closed means the build/push never ran (no mutable tag shipped).
    assert!(
        built.lock().unwrap().is_none(),
        "backend.build must NOT run when the sha is unprovable"
    );
    assert!(
        pushed.lock().unwrap().is_empty(),
        "no registry push must happen when the sha is unprovable"
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

// ── managed tabbify.toml injection (repo-wins) ────────────────────────────

const MANAGED_TOML: &str = r#"[app]
name = "hello-deploy"

[build]
kind = "docker"
dockerfile = "Dockerfile"

[runtime]
lifecycle = "on_request"
memory_mb = 512
vcpus = 1

[routes]
dynamic_prefixes = ["/"]
"#;

/// A clone tree WITHOUT its own `tabbify.toml` + a provided managed toml →
/// `run_build` writes `<src>/tabbify.toml` with the managed content.
#[tokio::test]
async fn run_build_injects_managed_toml_when_repo_has_none() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
    let backend = FakeBackend {
        built: Arc::new(Mutex::new(None)),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let mut job = test_job();
    job.manifest_toml = Some(MANAGED_TOML.to_owned());

    run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    let toml_path = dir.path().join("src").join("tabbify.toml");
    let written = std::fs::read_to_string(&toml_path)
        .unwrap_or_else(|e| panic!("managed toml must be written to {toml_path:?}: {e}"));
    assert_eq!(
        written, MANAGED_TOML,
        "the managed toml must be written verbatim"
    );
}

/// A clone tree that ships its OWN `tabbify.toml` + a provided managed toml →
/// the repo's own toml is left UNCHANGED (repo-wins).
#[tokio::test]
async fn run_build_keeps_repo_own_toml_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    const REPO_OWN_TOML: &str = "[app]\nname = \"repo-owned\"\n[build]\nkind = \"docker\"\n";
    let git = git_with_dockerfile_and_own_toml(REPO_OWN_TOML);
    let backend = FakeBackend {
        built: Arc::new(Mutex::new(None)),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let mut job = test_job();
    job.manifest_toml = Some(MANAGED_TOML.to_owned());

    run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    let toml_path = dir.path().join("src").join("tabbify.toml");
    let kept = std::fs::read_to_string(&toml_path).unwrap();
    assert_eq!(
        kept, REPO_OWN_TOML,
        "the repo's own tabbify.toml must NOT be overwritten by the managed one"
    );
}

/// No managed toml + a repo with none → no `tabbify.toml` is created (today's
/// behaviour is unchanged for a build with no managed config).
#[tokio::test]
async fn run_build_writes_no_toml_when_none_provided() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
    let backend = FakeBackend {
        built: Arc::new(Mutex::new(None)),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let job = test_job(); // manifest_toml: None
    run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    let toml_path = dir.path().join("src").join("tabbify.toml");
    assert!(
        !toml_path.exists(),
        "no managed toml provided + repo ships none → no tabbify.toml must be written"
    );
}

// ── honor [build] (dockerfile/context) from tabbify.toml ───────────────────

/// A `tabbify.toml` with `[build].dockerfile = "deploy/Dockerfile"` → the docker
/// build is invoked with that resolved Dockerfile path (under the clone root),
/// not the hardcoded root `Dockerfile`.
#[tokio::test]
async fn run_build_honors_custom_dockerfile_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_toml_custom_dockerfile();
    let built = Arc::new(Mutex::new(None));
    let backend = FakeBackend {
        built: built.clone(),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let job = test_job(); // repo ships its own toml; manifest_toml: None
    run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    let b = built.lock().unwrap();
    let (ctx, _tag, df) = b.as_ref().expect("backend.build must be called");
    let expected_df = dir.path().join("src").join("deploy").join("Dockerfile");
    assert_eq!(
        df.as_deref(),
        Some(expected_df.as_path()),
        "the build must use the toml's [build].dockerfile path"
    );
    // Context defaults to the clone root (`.` relative to <src>).
    assert_eq!(
        ctx,
        &dir.path().join("src"),
        "the build context must be the clone root by default"
    );
}

/// With no toml and a repo shipping a root `Dockerfile`, the build defaults to
/// the clone root as context and Docker's default Dockerfile (`None` passed to
/// the backend so `docker build` resolves `<context>/Dockerfile`).
#[tokio::test]
async fn run_build_defaults_to_root_dockerfile_without_toml() {
    let dir = tempfile::tempdir().unwrap();
    let git = git_with_dockerfile(Arc::new(Mutex::new(None)));
    let built = Arc::new(Mutex::new(None));
    let backend = FakeBackend {
        built: built.clone(),
    };
    let skopeo_runner = record_runner(Arc::new(Mutex::new(Vec::new())));

    let job = test_job(); // no toml in tree, manifest_toml: None
    run_docker_test(&job, &backend, &git, &skopeo_runner, "skopeo", dir.path())
        .await
        .unwrap();

    let b = built.lock().unwrap();
    let (ctx, _tag, df) = b.as_ref().expect("backend.build must be called");
    assert_eq!(ctx, &dir.path().join("src"));
    assert!(
        df.is_none(),
        "no toml → no explicit -f; Docker resolves <context>/Dockerfile"
    );
}

// ── BuildSpec default-layout detection (fc-sandbox guard) ──────────────────

/// No toml at the clone root → `resolve_build_spec` yields the default layout
/// (`context "."` + `Dockerfile`), so `is_default_layout()` is true (the
/// fc-sandbox path is allowed).
#[test]
fn resolve_build_spec_no_toml_is_default_layout() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let spec = resolve_build_spec(&src, &src.join("tabbify.toml")).unwrap();
    assert!(spec.is_default_layout());
    assert_eq!(spec.raw_context, ".");
    assert_eq!(spec.raw_dockerfile, "Dockerfile");
}

/// A toml with default `[build]` values (or none set) is still the default
/// layout → `is_default_layout()` true.
#[test]
fn resolve_build_spec_default_toml_is_default_layout() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let toml_path = src.join("tabbify.toml");
    std::fs::write(
        &toml_path,
        "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\n",
    )
    .unwrap();
    let spec = resolve_build_spec(&src, &toml_path).unwrap();
    assert!(
        spec.is_default_layout(),
        "absent [build].context/dockerfile default to \".\"/\"Dockerfile\""
    );
}

/// A toml with a NON-default `[build].dockerfile` → `is_default_layout()` false
/// (the fc-sandbox path must reject it rather than silently build the wrong
/// context).
#[test]
fn resolve_build_spec_custom_dockerfile_is_not_default_layout() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let toml_path = src.join("tabbify.toml");
    std::fs::write(
        &toml_path,
        "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\ndockerfile = \"deploy/Dockerfile\"\n",
    )
    .unwrap();
    let spec = resolve_build_spec(&src, &toml_path).unwrap();
    assert!(!spec.is_default_layout());
    assert_eq!(spec.raw_dockerfile, "deploy/Dockerfile");
}

/// A toml with a NON-default `[build].context` → `is_default_layout()` false.
#[test]
fn resolve_build_spec_custom_context_is_not_default_layout() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let toml_path = src.join("tabbify.toml");
    std::fs::write(
        &toml_path,
        "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\ncontext = \"service\"\n",
    )
    .unwrap();
    let spec = resolve_build_spec(&src, &toml_path).unwrap();
    assert!(!spec.is_default_layout());
    assert_eq!(spec.raw_context, "service");
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
