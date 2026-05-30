//! Docker build sub-pipeline: require a `Dockerfile`, build the local image
//! via the [`BuildBackend`], then tag + push to the mesh registry.

use std::path::Path;

use anyhow::Context as _;

use super::{ArtifactRef, BuildJob};
use crate::build_backend::BuildBackend;
use crate::docker::CommandRunner;

/// The DOCKER build path (unchanged behaviour): require a `Dockerfile`, build
/// the local image via `backend`, then tag + push to the mesh registry.
///
/// # Errors
/// Missing `Dockerfile`, build error, or push failure.
pub(super) async fn run_docker_build(
    job: &BuildJob,
    backend: &dyn BuildBackend,
    push_runner: &CommandRunner,
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

    // Tag + push to the mesh registry. On failure bail with the captured
    // registry stderr (e.g. `unauthorized: authentication required`) so the
    // diagnostic survives instead of being collapsed to just the image ref.
    if let Err(e) = crate::docker::push_image(docker_bin, &local_tag, &reff, push_runner).await {
        anyhow::bail!("push to registry failed: {reff}: {e}");
    }

    Ok(ArtifactRef { reff, digest: None })
}
