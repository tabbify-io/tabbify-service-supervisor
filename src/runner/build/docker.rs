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
    tool_runner: &CommandRunner,
    skopeo_bin: &str,
    oras_bin: &str,
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

    // Push to the mesh registry in TWO supervisor-side steps: skopeo copies
    // the built image out of the docker daemon into an OCI layout (local
    // transports only — skopeo cannot parse the registry's bracketed-IPv6
    // ref), then oras pushes the layout to the registry (oras parses such
    // refs; the run-side pulls with the same form). The docker daemon never
    // needs a mesh route. On failure bail with the captured stderr (e.g.
    // `unauthorized`) so the diagnostic survives instead of being collapsed
    // to just the image ref.
    let layout_dir = src
        .parent()
        .unwrap_or(src)
        .join("oci-out")
        .to_string_lossy()
        .into_owned();
    if let Err(e) = crate::skopeo::push_to_registry(
        skopeo_bin,
        oras_bin,
        &local_tag,
        &reff,
        &layout_dir,
        tool_runner,
    )
    .await
    {
        anyhow::bail!("push to registry failed: {reff}: {e}");
    }

    Ok(ArtifactRef { reff, digest: None })
}
