//! Swappable OCI-image build back-ends.
//!
//! The [`BuildBackend`] trait is the seam between the build-runner orchestration
//! ([`crate::runner`] P3.4) and the concrete build strategy.  The only backend
//! shipped here is [`HostDockerBackend`], which runs `docker build` on the host
//! daemon.  A future Firecracker-sandbox backend (for untrusted source) will
//! implement the same trait.
//!
//! ## Design note
//!
//! `async-trait` is not a dependency of this crate — we mirror the manual
//! `BoxFut` pattern used by [`crate::runtime::AppRuntime`] so trait objects
//! (`dyn BuildBackend`) work without any macro overhead.

use std::path::Path;

use crate::docker::{CommandRunner, production_command_runner, push_image};
use crate::runtime::BoxFut;

// ---- pure argv helpers -------------------------------------------------------

/// `docker build -t <tag> <context_dir>` argv (sans the leading binary):
/// build from a SOURCE-DIRECTORY context (not stdin). Used by
/// [`HostDockerBackend`] where the source tree is already checked out on disk.
///
/// Distinct from the existing `docker::protocol::build_args` which builds from
/// a gzipped-tar piped on stdin (`docker build -t <tag> -`); this variant
/// passes the directory path directly, which is the natural form for a
/// post-`git clone` build.
#[must_use]
pub fn build_dir_args(tag: &str, context_dir: &Path) -> Vec<String> {
    vec![
        "build".to_owned(),
        "-t".to_owned(),
        tag.to_owned(),
        context_dir.to_string_lossy().into_owned(),
    ]
}

// ---- trait -------------------------------------------------------------------

/// Swappable OCI image build strategy.
///
/// Implementors take a source directory and a local image tag, run whatever
/// build machinery is appropriate (e.g. `docker build` on the host daemon, or
/// a future Firecracker sandbox), and return `Ok(())` on success.
///
/// The trait is intentionally minimal: only `build` is required.  Push
/// (tagging the result for the mesh registry) is a separate concern handled by
/// the orchestration layer ([`crate::docker::push_image`]).
///
/// Object-safe via the manual `BoxFut` pattern (no `async-trait` dependency).
pub trait BuildBackend: Send + Sync {
    /// Build an OCI image from the source tree at `context_dir`, tagging the
    /// result as `tag` in the local Docker daemon.
    ///
    /// # Errors
    /// Any build failure: spawn error, non-zero `docker build` exit, timeout,
    /// or a backend-specific error.
    fn build<'a>(&'a self, context_dir: &'a Path, tag: &'a str) -> BoxFut<'a, anyhow::Result<()>>;
}

// ---- push helper -------------------------------------------------------------

/// Tag the locally-built `local_tag` as `registry_ref` and push it to the mesh
/// OCI registry.  This is the publish step that follows a successful
/// [`BuildBackend::build`] call: it wires `docker tag <local_tag> <registry_ref>`
/// then `docker push <registry_ref>` via the injectable [`CommandRunner`].
///
/// Returns `Ok(())` only if BOTH commands succeed; `Err(stderr)` on any
/// failure, carrying the captured registry diagnostic so the caller can bail
/// with the real reason instead of just the image ref.
///
/// Wraps [`crate::docker::push_image`] for direct use by the build-runner
/// orchestration layer (P3.4) so it has a single import point in this module.
///
/// Convenience alias: tag the locally-built `local_tag` as `registry_ref`
/// and push to the mesh OCI registry.
///
/// Wraps [`crate::docker::push_image`] so callers inside this module have a
/// single import point.  The P3.4 orchestration calls `crate::docker::push_image`
/// directly; this wrapper remains for future callers within `build_backend`.
#[allow(dead_code)]
pub(crate) async fn push_to_registry(
    docker_bin: &str,
    local_tag: &str,
    registry_ref: &str,
    runner: &CommandRunner,
) -> Result<(), String> {
    push_image(docker_bin, local_tag, registry_ref, runner).await
}

// ---- host docker backend -----------------------------------------------------

/// Builds an OCI image by running `docker build -t <tag> <context_dir>` on the
/// host Docker daemon.
///
/// This is the **trusted-source** backend: the source tree is expected to come
/// from a controlled repository.  Untrusted-source isolation (a future
/// Firecracker-sandbox backend) is out of scope for this task.
///
/// The [`CommandRunner`] seam makes the backend unit-testable without a real
/// Docker daemon.
pub struct HostDockerBackend {
    /// The `docker` binary path (e.g. `"docker"`).
    docker_bin: String,
    /// Injectable command runner: production uses the real `docker` CLI;
    /// tests substitute a recording closure.
    runner: CommandRunner,
}

impl HostDockerBackend {
    /// Create a production backend that shells out to `docker_bin`.
    #[must_use]
    pub fn new(docker_bin: String) -> Self {
        let runner = production_command_runner(docker_bin.clone());
        Self { docker_bin, runner }
    }

    /// Create a test backend with an injected runner (no real Docker daemon).
    #[cfg(test)]
    pub(crate) fn with_runner(docker_bin: String, runner: CommandRunner) -> Self {
        Self { docker_bin, runner }
    }
}

impl BuildBackend for HostDockerBackend {
    /// Run `docker build -t <tag> <context_dir>` via the injected runner and
    /// return `Ok(())` on success or an error if the build fails.
    fn build<'a>(&'a self, context_dir: &'a Path, tag: &'a str) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let args = build_dir_args(tag, context_dir);
            match (self.runner)(args).await {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(
                    "`{} build -t {} {}` failed: {e}",
                    self.docker_bin,
                    tag,
                    context_dir.display()
                )),
            }
        })
    }
}

// ---- tests -------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::runtime::BoxFut;

    // ---- build_dir_args (pure, deterministic) --------------------------------

    /// `build_dir_args` must produce `["build", "-t", <tag>, <dir>]`.
    #[test]
    fn build_dir_args_returns_correct_argv() {
        let dir = PathBuf::from("/work/src/my-app");
        assert_eq!(
            build_dir_args("tbf-img-x", &dir),
            vec!["build", "-t", "tbf-img-x", "/work/src/my-app"]
        );
    }

    /// `build_dir_args` preserves the full path including any subdirectories.
    #[test]
    fn build_dir_args_preserves_full_path() {
        let dir = PathBuf::from("/clone/abc123/repo");
        let args = build_dir_args("tbf-img-abc", &dir);
        assert_eq!(args[0], "build");
        assert_eq!(args[1], "-t");
        assert_eq!(args[2], "tbf-img-abc");
        assert_eq!(args[3], "/clone/abc123/repo");
    }

    // ---- HostDockerBackend::build (injected runner) --------------------------

    /// When the injected runner succeeds, `build` must issue
    /// `["build", "-t", <tag>, <dir>]` and return `Ok(())`.
    #[tokio::test]
    async fn host_docker_backend_build_issues_correct_args_on_success() {
        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            issued2.lock().unwrap().push(args);
            let fut: BoxFut<'static, Result<(), String>> = Box::pin(async { Ok(()) });
            fut
        });

        let backend = HostDockerBackend::with_runner("docker".to_owned(), runner);
        let dir = PathBuf::from("/work/repo");
        let result = backend.build(&dir, "tbf-img-x").await;

        assert!(result.is_ok(), "runner success → build must return Ok");

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 1, "must issue exactly one docker command");
        assert_eq!(
            cmds[0],
            vec![
                "build".to_owned(),
                "-t".to_owned(),
                "tbf-img-x".to_owned(),
                "/work/repo".to_owned(),
            ],
            "must issue `docker build -t <tag> <dir>`"
        );
    }

    /// When the injected runner returns `Err` (build failed), `build` must
    /// return an `Err`.
    #[tokio::test]
    async fn host_docker_backend_build_returns_err_on_runner_failure() {
        let runner: CommandRunner = Arc::new(|_args: Vec<String>| {
            Box::pin(async { Err("docker build failed".to_owned()) })
                as BoxFut<'static, Result<(), String>>
        });

        let backend = HostDockerBackend::with_runner("docker".to_owned(), runner);
        let dir = PathBuf::from("/work/repo");
        let result = backend.build(&dir, "tbf-img-fail").await;

        assert!(
            result.is_err(),
            "runner failure → build must return Err; got Ok"
        );
    }

    /// `build` works through a `dyn BuildBackend` trait object — confirms
    /// object-safety of the trait.
    #[tokio::test]
    async fn host_docker_backend_build_works_via_trait_object() {
        let runner: CommandRunner = Arc::new(|_args| Box::pin(async { Ok(()) }));
        let backend: Box<dyn BuildBackend> =
            Box::new(HostDockerBackend::with_runner("docker".to_owned(), runner));

        let dir = PathBuf::from("/work/repo");
        let result = backend.build(&dir, "tbf-img-trait").await;
        assert!(result.is_ok(), "trait-object dispatch must succeed");
    }

    /// `build` issued through `Arc<dyn BuildBackend>` also works — confirming
    /// the trait is usable in the orchestration pattern.
    #[tokio::test]
    async fn host_docker_backend_build_works_via_arc_trait_object() {
        let runner: CommandRunner = Arc::new(|_args| Box::pin(async { Ok(()) }));
        let backend: Arc<dyn BuildBackend> =
            Arc::new(HostDockerBackend::with_runner("docker".to_owned(), runner));

        let dir = PathBuf::from("/work/repo");
        let result = backend.build(&dir, "tbf-img-arc").await;
        assert!(
            result.is_ok(),
            "Arc<dyn BuildBackend> dispatch must succeed"
        );
    }

    // ---- push_to_registry (docker push seam) ---------------------------------

    /// `push_to_registry` must issue `["tag", <local_tag>, <reff>]` then
    /// `["push", <reff>]` in order and return `Ok(())` when the runner succeeds.
    #[tokio::test]
    async fn push_to_registry_issues_tag_then_push_on_success() {
        let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let issued2 = issued.clone();

        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            issued2.lock().unwrap().push(args);
            let fut: BoxFut<'static, Result<(), String>> = Box::pin(async { Ok(()) });
            fut
        });

        let result = push_to_registry(
            "docker",
            "tbf-img-uuid-v3",
            "[fd5a::1]:5000/acme/app:sha",
            &runner,
        )
        .await;

        assert!(
            result.is_ok(),
            "runner success → push_to_registry must return Ok"
        );

        let cmds = issued.lock().unwrap();
        assert_eq!(cmds.len(), 2, "must issue exactly 2 commands (tag + push)");
        assert_eq!(
            cmds[0],
            vec![
                "tag".to_owned(),
                "tbf-img-uuid-v3".to_owned(),
                "[fd5a::1]:5000/acme/app:sha".to_owned(),
            ],
            "first command must be docker tag <local_tag> <ref>"
        );
        assert_eq!(
            cmds[1],
            vec!["push".to_owned(), "[fd5a::1]:5000/acme/app:sha".to_owned()],
            "second command must be docker push <ref>"
        );
    }

    /// When the tag step fails, `push_to_registry` must return `Err` carrying
    /// the runner's stderr without issuing the push command.
    #[tokio::test]
    async fn push_to_registry_returns_err_on_tag_failure() {
        let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let cc = call_count.clone();

        let runner: CommandRunner = Arc::new(move |_args| {
            *cc.lock().unwrap() += 1;
            let fut: BoxFut<'static, Result<(), String>> =
                Box::pin(async { Err("tag failed".to_owned()) });
            fut
        });

        let result = push_to_registry(
            "docker",
            "tbf-img-uuid-v4",
            "[fd5a::1]:5000/acme/app:sha",
            &runner,
        )
        .await;

        let err = result.expect_err("tag failure → push_to_registry must return Err");
        assert!(
            err.contains("tag failed"),
            "Err must surface the runner stderr; got: {err}"
        );
        assert_eq!(
            *call_count.lock().unwrap(),
            1,
            "must issue only the tag command (push must NOT be called)"
        );
    }
}
