//! Registry push plumbing for the docker BUILD path.
//!
//! [`push_image`] aliases a locally-built tag as the mesh registry ref + pushes
//! it (used by the build-runner via [`crate::build_backend::push_to_registry`]
//! and directly by the P3.4 orchestration). It goes through the injectable
//! [`super::CommandRunner`] so unit tests can record the exact argv without
//! invoking a real Docker daemon.

use super::CommandRunner;
use super::protocol::{push_args, tag_args};

/// Tag `local_tag` as `reff` and push `reff` to the mesh OCI registry.
///
/// Takes a local build result (`local_tag`), aliases it as the registry ref,
/// and pushes it so other supervisors can pull it.
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
