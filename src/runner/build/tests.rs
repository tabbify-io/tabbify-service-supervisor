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
            Box::pin(async { Err("command failed".to_owned()) })
                as BoxFut<'static, Result<(), String>>
        })
    }

    /// A no-op [`CommandRunner`] (returns `Ok(())`, records nothing). Used for the
    /// runner the active build path ignores (e.g. the oras runner on a docker
    /// build, or the docker push runner on a wasm build).
    fn noop_runner() -> CommandRunner {
        Arc::new(|_args: Vec<String>| {
            Box::pin(async { Ok(()) }) as BoxFut<'static, Result<(), String>>
        })
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
