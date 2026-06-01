//! Docker build sub-pipeline: require a `Dockerfile`, build the local image
//! via the [`BuildBackend`], then push it to the mesh registry via the
//! supervisor-side `skopeo`.

use std::path::Path;

use anyhow::Context as _;

use super::{ArtifactRef, BuildJob};
use crate::build_backend::BuildBackend;
use crate::docker::CommandRunner;

/// The DOCKER build path: require a `Dockerfile`, build the local image via
/// `backend`, then push it to the mesh registry via `skopeo`.
///
/// The push runs `skopeo copy docker-daemon:<local_tag>:latest docker://<reff>`
/// in the supervisor process (which is on the mesh): skopeo reads the built image
/// straight from the local docker daemon and copies it to the registry, so the
/// docker daemon itself never needs a mesh route (which it does not have).
///
/// # Errors
/// Missing `Dockerfile`, build error, or push failure.
pub(super) async fn run_docker_build(
    job: &BuildJob,
    backend: &dyn BuildBackend,
    skopeo_runner: &CommandRunner,
    skopeo_bin: &str,
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

    // Push to the mesh registry via the supervisor-side skopeo (the docker
    // daemon has no mesh route; skopeo runs in the supervisor's mesh-routed
    // netns and copies the built image straight from the daemon to the registry).
    // On failure bail with the captured registry stderr (e.g. `unauthorized:
    // authentication required`) so the diagnostic survives instead of being
    // collapsed to just the image ref.
    if let Err(e) = crate::skopeo::skopeo_push(skopeo_bin, &local_tag, &reff, skopeo_runner).await {
        anyhow::bail!("push to registry failed: {reff}: {e}");
    }

    Ok(ArtifactRef { reff, digest: None })
}
