//! Wasm build sub-pipeline: run the job's `build_cmd` in the cloned source
//! dir, verify the produced `.wasm` at `artifact_path`, then `oras push` it
//! to the mesh registry as a wasm OCI artifact.

use std::path::Path;

use super::{ArtifactRef, BuildCmdRunner, BuildJob};
use crate::docker::CommandRunner;

/// The WASM build path: run `job.build_cmd` in the cloned `src` dir, verify the
/// produced `.wasm` at `job.artifact_path`, then `oras push` it to the mesh
/// registry as a wasm OCI artifact.
///
/// # Errors
/// Missing `build_cmd`/`artifact_path`, a failing build command, a missing
/// produced artifact, or an `oras push` failure.
pub(super) async fn run_wasm_build(
    job: &BuildJob,
    build_cmd_runner: &BuildCmdRunner,
    oras_runner: &CommandRunner,
    oras_bin: &str,
    src: &Path,
    reff: String,
) -> anyhow::Result<ArtifactRef> {
    // Require build_cmd + artifact_path for a wasm job.
    let build_cmd = job.build_cmd.as_deref().ok_or_else(|| {
        anyhow::anyhow!("wasm build job requires `build_cmd` (the command that produces the .wasm)")
    })?;
    let artifact_path = job.artifact_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "wasm build job requires `artifact_path` (path to the produced .wasm, relative to repo root)"
        )
    })?;

    // Run the build command in the cloned source dir.
    let built = (build_cmd_runner)(build_cmd.to_owned(), src.to_path_buf()).await;
    if !built {
        anyhow::bail!("wasm build command failed: {build_cmd}");
    }

    // Verify the produced artifact exists at <src>/<artifact_path>.
    let artifact_abs = src.join(artifact_path);
    if !artifact_abs.is_file() {
        anyhow::bail!(
            "wasm build produced no artifact at {} (expected from build_cmd `{build_cmd}`)",
            artifact_abs.display()
        );
    }

    // oras push the wasm artifact to the mesh registry.
    let artifact_abs_str = artifact_abs.to_string_lossy().into_owned();
    let pushed = crate::oras::oras_push(oras_bin, &reff, &artifact_abs_str, oras_runner).await;
    if !pushed {
        anyhow::bail!("oras push to registry failed: {reff}");
    }

    Ok(ArtifactRef { reff, digest: None })
}
