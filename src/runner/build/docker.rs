//! Docker build sub-pipeline: require a `Dockerfile`, build the local image
//! via the [`BuildBackend`], then push it to the mesh registry via the
//! supervisor-side `skopeo`.

use anyhow::Context as _;

use super::{ArtifactRef, BuildJob, BuildSpec};
use crate::build_backend::BuildBackend;
use crate::docker::CommandRunner;

/// The DOCKER build path: require a `Dockerfile`, build the local image via
/// `backend`, then push it to the mesh registry via `skopeo`.
///
/// `[build]` from `tabbify.toml` is honoured via `spec`: the image is built from
/// `spec.context_dir` (default the clone root) using `spec.dockerfile` (default
/// `<context_dir>/Dockerfile`). The push runs `skopeo copy
/// docker-daemon:<local_tag>:latest docker://<reff>` in the supervisor process
/// (which is on the mesh): skopeo reads the built image straight from the local
/// docker daemon and copies it to the registry, so the docker daemon itself never
/// needs a mesh route (which it does not have).
///
/// # Errors
/// Missing `Dockerfile`, build error, or push failure.
// All args are injected build seams (job + backend + tool runner + binaries +
// resolved spec + ref + commit sha) threaded from `run_build`; bundling them
// would not improve clarity over the explicit pipeline wiring.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_docker_build(
    job: &BuildJob,
    backend: &dyn BuildBackend,
    tool_runner: &CommandRunner,
    skopeo_bin: &str,
    oras_bin: &str,
    spec: &BuildSpec,
    reff: String,
    commit_sha: String,
) -> anyhow::Result<ArtifactRef> {
    // Resolve the Dockerfile to require + pass to the build: the
    // `[build].dockerfile` (relative to the clone root) when the toml set one,
    // else Docker's default `<context_dir>/Dockerfile`. When the toml is absent
    // (`spec.dockerfile == None`) we pass `None` to the backend so the argv stays
    // exactly today's `docker build -t <tag> <context>` (no explicit `-f`).
    let dockerfile_for_check: std::path::PathBuf = spec
        .dockerfile
        .clone()
        .unwrap_or_else(|| spec.context_dir.join("Dockerfile"));

    // Require the resolved Dockerfile to exist.
    if !dockerfile_for_check.is_file() {
        anyhow::bail!(
            "no Dockerfile at {} (set [build].dockerfile in tabbify.toml to point at it)",
            dockerfile_for_check.display()
        );
    }

    // Build the local image from the resolved context, honouring the Dockerfile
    // path when the toml specified one. Local tag is scoped to this build so
    // concurrent builds don't collide.
    let local_tag = format!("tbf-build-{}", job.app_uuid);
    backend
        .build(&spec.context_dir, &local_tag, spec.dockerfile.as_deref())
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
    let layout_dir = spec
        .clone_root
        .parent()
        .unwrap_or(&spec.clone_root)
        .join("oci-out")
        .to_string_lossy()
        .into_owned();

    // Phase-A registry auth: when a push token is supplied (minted by the
    // node before dispatching the build job), write a docker-format auth
    // config and pass its dir to oras. When None (today's default), oras
    // pushes anonymously — no behaviour change for existing jobs.
    let oras_cfg_owned: Option<String> = if let Some(ref token) = job.push_token {
        let cfg_dir = spec
            .clone_root
            .parent()
            .unwrap_or(&spec.clone_root)
            .join("oras-cfg");
        crate::skopeo::write_registry_config(token, &job.registry_ula, &cfg_dir)
            .with_context(|| {
                format!("write oras registry auth config to {}", cfg_dir.display())
            })?;
        Some(cfg_dir.to_string_lossy().into_owned())
    } else {
        None
    };

    if let Err(e) = crate::skopeo::push_to_registry(
        skopeo_bin,
        oras_bin,
        &local_tag,
        &reff,
        &layout_dir,
        tool_runner,
        oras_cfg_owned.as_deref(),
    )
    .await
    {
        anyhow::bail!("push to registry failed: {reff}: {e}");
    }

    Ok(ArtifactRef {
        reff,
        digest: None,
        commit_sha: Some(commit_sha),
    })
}
