//! Registry push/pull plumbing for the docker runtime.
//!
//! Mirror seams: [`pull_and_tag`] fetches a remote ref + aliases it under the
//! local versioned tag (used inside [`super::DockerRuntime::launch_with_id`]);
//! [`push_image`] aliases a locally-built tag as the registry ref + pushes it
//! (used by the build-runner via [`crate::build_backend::push_to_registry`]
//! and directly by the P3.4 orchestration).
//!
//! Both go through the injectable [`super::CommandRunner`] so unit tests can
//! record the exact argv without invoking a real Docker daemon.

use super::protocol::{pull_args, push_args, tag_args};
use super::runtime::CommandRunner;

/// Pull `reff` from the mesh OCI registry and immediately tag it as `vtag`
/// (the supervisor's versioned local tag `tbf-img-<uuid>-v<N>`).
///
/// Returns `true` only if BOTH `docker pull <reff>` AND `docker tag <reff>
/// <vtag>` succeed; `false` on any failure (the caller falls through to the
/// W2 / build path).
///
/// Uses the injectable [`CommandRunner`] so tests can record the issued
/// commands without a real Docker daemon. The runner's `Err(stderr)` is mapped
/// to `false` here: a failed registry pull is non-fatal (the caller falls
/// through to the W2 / build path), so only success/failure matters.
pub(crate) async fn pull_and_tag(
    _docker_bin: &str,
    reff: &str,
    vtag: &str,
    runner: &CommandRunner,
) -> bool {
    if (runner)(pull_args(reff)).await.is_err() {
        return false;
    }
    (runner)(tag_args(reff, vtag)).await.is_ok()
}

/// Tag `local_tag` as `reff` and push `reff` to the mesh OCI registry.
///
/// This is the mirror of [`pull_and_tag`]: where pull fetches a remote ref
/// and aliases it locally, push takes a local build result (`local_tag`),
/// aliases it as the registry ref, and pushes it so other supervisors can
/// pull it.
///
/// Returns `Ok(())` only if BOTH `docker tag <local_tag> <reff>` AND
/// `docker push <reff>` succeed; `Err(stderr)` on any failure, carrying the
/// captured registry diagnostic (e.g. `unauthorized: authentication
/// required`) so the build runner can bail with the real reason instead of
/// just the image ref.
///
/// Uses the injectable [`CommandRunner`] so tests can record the issued
/// commands without a real Docker daemon.
///
/// Wrapped by [`crate::build_backend::push_to_registry`]; called directly
/// by the P3.4 `run_build` orchestration via `crate::docker::push_image`.
pub(crate) async fn push_image(
    _docker_bin: &str,
    local_tag: &str,
    reff: &str,
    runner: &CommandRunner,
) -> Result<(), String> {
    (runner)(tag_args(local_tag, reff)).await?;
    (runner)(push_args(reff)).await
}
