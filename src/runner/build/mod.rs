//! One-shot builder mode for `tabbify-runner`.
//!
//! When launched with `--build-spec <file>`, the runner reads a [`BuildJob`] from
//! the JSON file, runs the build pipeline end-to-end, prints the resulting
//! [`ArtifactRef`] as JSON to stdout, and exits — it never joins the mesh or
//! starts a serve loop.
//!
//! The orchestration pipeline ([`run_build`]) is fully injection-seamed so tests
//! can drive clone/build/push without any real git or Docker daemon.  The
//! production wiring (real `git`, `docker`) lives only in [`run_one_shot_build`].
//!
//! ## Module layout
//! - [`docker`] — the docker sub-pipeline (`Dockerfile` → build → docker push).
//! - [`firecracker`] — the generic Firecracker RUNTIME-build (OCI image →
//!   bootable `rootfs.ext4` + PID-1 init); `pub` so the KVM-gated integration
//!   test can drive [`firecracker::run_firecracker_build`] end-to-end.
//! - Everything else (the [`BuildJob`] type, the dispatcher, the production
//!   wiring, and the tests) lives in this file.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

mod docker;
pub(crate) mod fc_sandbox;
pub mod firecracker;

/// Which build pipeline a [`BuildJob`] drives.
///
/// Absent in the JSON spec ⇒ [`BuildKind::Docker`] (the original behaviour), so
/// every pre-existing docker job + test is unchanged. The in-process WASM
/// runtime was removed, so the only build pipeline is the docker one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum BuildKind {
    /// Clone → require `Dockerfile` → `docker build` → `docker push`.
    #[default]
    Docker,
}

/// A one-shot build job: clone `repo_url`@`git_ref`, build an artifact, push it
/// to the mesh registry at `registry_ula` as `<tenant>/<app_uuid>:<sha>`.
///
/// `build_kind` selects the pipeline (docker — the only kind today). The
/// `build_cmd` / `artifact_path` fields are retained as inert wire surface
/// (the WASM build path that consumed them was removed).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct BuildJob {
    /// HTTPS URL of the Git repository to clone.
    pub repo_url: String,
    /// Git ref (branch, tag, or full SHA) to check out.
    ///
    /// This value is the ref FETCHED + checked out; it is NO LONGER used as the
    /// image tag. The builder re-resolves the checked-out HEAD to its immutable
    /// 40-hex commit SHA (`git rev-parse HEAD`) and tags the image with that SHA,
    /// so a "deploy now" with a branch ref (e.g. `main`) ships an immutable tag
    /// instead of a floating one. A push-webhook job whose `git_ref` is already a
    /// SHA re-resolves to the SAME SHA (zero behaviour change on that path).
    pub git_ref: String,
    /// Tenant namespace used as the registry path prefix.
    pub tenant: String,
    /// UUID of the app; used in the image tag as `<tenant>/<app_uuid>:<sha>`.
    pub app_uuid: String,
    /// Mesh ULA + port of the registry to push to, e.g. `"[fd5a:1f02:aa::1]:5000"`.
    pub registry_ula: String,
    /// Short-lived GitHub token for the clone (`None` = public repo).
    #[serde(default)]
    pub clone_token: Option<String>,
    /// Token for pushing to the registry (`None` = anonymous registry).
    #[serde(default)]
    pub push_token: Option<String>,
    /// Which build pipeline to run. Absent ⇒ [`BuildKind::Docker`].
    #[serde(default)]
    pub build_kind: BuildKind,
    /// Inert wire field (formerly the wasm `build_cmd`). Retained so existing
    /// build specs still parse; no build path consumes it.
    #[serde(default)]
    pub build_cmd: Option<String>,
    /// Inert wire field (formerly the wasm `artifact_path`). Retained so existing
    /// build specs still parse; no build path consumes it.
    #[serde(default)]
    pub artifact_path: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (a raw TOML string) for a connect-repo
    /// deploy. Injected into the clone ONLY when the repo ships none (repo-wins);
    /// then parsed to drive `[build]` (dockerfile/context) + `[runtime]`/`[routes]`.
    /// `None` (the default) = no managed config (a `tcli`/local build, or a repo
    /// expected to carry its own toml).
    #[serde(default)]
    pub manifest_toml: Option<String>,
}

/// The result of a build: the immutable image ref and (optionally) its digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ArtifactRef {
    /// Fully-qualified OCI image reference, e.g. `"[fd5a::1]:5000/acme/myapp:abc1234"`.
    pub reff: String,
    /// OCI content-digest of the pushed manifest, e.g. `"sha256:deadbeef..."`.
    /// `None` if the registry did not return a digest.
    #[serde(default)]
    pub digest: Option<String>,
    /// The IMMUTABLE git commit SHA the image was built from (the resolved
    /// `git rev-parse HEAD` of the shallow clone). DISTINCT from `digest` (which
    /// is the OCI manifest content-hash): this is the SOURCE commit, surfaced so
    /// the control plane can record exactly which commit a deploy shipped. The
    /// `<...>:<sha>` tag component of `reff` is built from this value.
    #[serde(default)]
    pub commit_sha: Option<String>,
}

/// Run a build job end-to-end with injected dependencies.
///
/// Dispatches on [`BuildJob::build_kind`]:
/// - [`BuildKind::Docker`] (the only kind): clone → require `Dockerfile` →
///   `backend.build` → `docker tag`+`push` to the mesh registry.
///
/// Computes the ref scheme `job.registry_ula/<tenant>/<app_uuid>:<sha>` where
/// `<sha>` is the IMMUTABLE commit SHA the clone resolved at HEAD (NOT the
/// possibly-mutable `git_ref`), and returns an [`ArtifactRef`].
///
/// All dependencies are injected so the function is fully unit-testable without
/// a real git binary, Docker daemon, or `skopeo` binary. `git` drives the
/// (side-effecting) clone steps; `git_capture` drives the read-only
/// `git rev-parse HEAD` that resolves the immutable commit SHA. The
/// `skopeo_runner` + `skopeo_bin` drive the registry PUSH (the image is built by
/// `backend`, then `skopeo` copies it from the local docker daemon to the mesh
/// registry — the daemon never needs a mesh route).
///
/// # Errors
/// Clone failure; an unprovable commit SHA (fail-closed — see
/// [`crate::git::resolve_cloned_head`]); missing `Dockerfile`, build error, or
/// push failure.
// Every dependency is an injected seam (git side-effect + git capture + tool
// runners + binaries) so the build pipeline is fully unit-testable without real
// git/docker/skopeo; grouping them into a struct would only obscure the wiring.
#[allow(clippy::too_many_arguments)]
pub async fn run_build(
    job: &BuildJob,
    backend: &dyn crate::build_backend::BuildBackend,
    git: &crate::git::GitRun,
    git_capture: &crate::git::GitCapture,
    tool_runner: &crate::docker::CommandRunner,
    skopeo_bin: &str,
    oras_bin: &str,
    workdir: &Path,
) -> anyhow::Result<ArtifactRef> {
    // 1. Clone into <workdir>/src.
    let src = workdir.join("src");
    crate::git::clone(
        &job.repo_url,
        &job.git_ref,
        job.clone_token.as_deref(),
        &src,
        git,
    )
    .await
    .context("clone")?;

    // Resolve the IMMUTABLE commit SHA the clone left at HEAD. For a "deploy now"
    // job whose `git_ref` is a branch (e.g. `main`), this turns the floating ref
    // into the exact commit; for a push-webhook job whose `git_ref` is already a
    // SHA, re-resolving the checked-out HEAD yields the SAME SHA (no behaviour
    // change on that path). FAIL-CLOSED: an unprovable SHA aborts the build
    // rather than shipping a mutable tag.
    let commit_sha = crate::git::resolve_cloned_head(&src, git_capture)
        .await
        .context("resolve clone commit sha")?;

    // Inject the Tabbify-MANAGED `tabbify.toml` ONLY when the repo ships none
    // (repo-wins): a repo that carries its own `tabbify.toml` keeps using it,
    // while a repo with none gets the managed default written at the clone root.
    let toml_path = src.join("tabbify.toml");
    if !toml_path.exists() {
        if let Some(t) = &job.manifest_toml {
            std::fs::write(&toml_path, t).with_context(|| {
                format!("write managed tabbify.toml to {}", toml_path.display())
            })?;
        }
    }

    // Resolve `[build]` (dockerfile/context) from the toml now present at the
    // clone root (repo's own or the injected managed one). Absent ⇒ today's
    // defaults: context = `<src>` and Docker's default `<src>/Dockerfile`.
    let build_spec = resolve_build_spec(&src, &toml_path)?;

    // Image ref: <registry_ula>/<tenant>/<app_uuid>:<commit_sha>.
    // The tag component is the IMMUTABLE commit SHA (resolved above), NOT the
    // possibly-mutable `git_ref` — a branch ref like `main` would otherwise tag a
    // floating image and "deploy success" could serve a stale commit (TAB-10).
    // OCI distribution requires lowercase repository names; the tenant (GitHub
    // owner, e.g. "Lsneg") is the only mixed-case component, so lowercase it —
    // otherwise `oras`/`skopeo` reject the push with "invalid repository". The
    // runtime PULL lowercases the same way (see firecracker.rs) so refs match.
    // The SHA is already lowercase hex (validated by `resolve_cloned_head`).
    let reff = format!(
        "{}/{}/{}:{}",
        job.registry_ula,
        job.tenant.to_lowercase(),
        job.app_uuid,
        commit_sha
    );

    match job.build_kind {
        BuildKind::Docker => {
            // Phase-2 sandbox: build inside an ephemeral Firecracker VM
            // (explicit opt-in + KVM). The host clones (above), the VM
            // builds, the host pushes — no docker daemon anywhere.
            if fc_sandbox::enabled() {
                let fc_runner = firecracker::production_fc_build_runner();
                // The one-shot build child resolves data_dir from
                // SUPERVISOR_DATA_DIR (the spawner injects the supervisor's
                // resolved value); fall back to the install default.
                let data_dir = std::env::var("SUPERVISOR_DATA_DIR")
                    .unwrap_or_else(|_| "/var/lib/tabbify".to_owned());
                // Thread the resolved `[build]` layout (context + dockerfile,
                // RELATIVE to the clone root) into the job contract the guest
                // reads (job.json v2). Defaults (`.` / `Dockerfile` at the root)
                // keep the v1 behaviour; a subdir Dockerfile/context is honoured
                // by the v2 guest builder (the buildkit-toolchain image).
                let layout = fc_sandbox::run_sandboxed_build(
                    &job.app_uuid,
                    &src,
                    &build_spec.raw_context,
                    &build_spec.raw_dockerfile,
                    &job.registry_ula,
                    std::path::Path::new(&data_dir),
                    workdir,
                    &fc_runner,
                )
                .await
                .context("sandboxed (firecracker) build")?;

                // Phase-A: write oras auth config when a push token is supplied.
                let oras_cfg_owned =
                    oras_push_cfg_file(workdir, job.push_token.as_deref(), &job.registry_ula)?;

                if let Err(e) = (tool_runner)(crate::skopeo::oras_push_args(
                    oras_bin,
                    &layout.to_string_lossy(),
                    &reff,
                    oras_cfg_owned.as_deref(),
                ))
                .await
                {
                    anyhow::bail!("push to registry failed: {reff}: {e}");
                }
                return Ok(ArtifactRef {
                    reff,
                    digest: None,
                    commit_sha: Some(commit_sha),
                });
            }
            docker::run_docker_build(
                job,
                backend,
                tool_runner,
                skopeo_bin,
                oras_bin,
                &build_spec,
                reff,
                commit_sha,
            )
            .await
        }
    }
}

/// Build the oras `--to-registry-config` value for a SANDBOXED-build push.
///
/// [`crate::skopeo::write_registry_config`] writes `<workdir>/oras-cfg/config.json`;
/// oras's `--to-registry-config` flag wants that config.json FILE, NOT the
/// containing dir — the SAME dir-vs-file contract the v1.4.79 PULL/copy fix
/// established for `--from-registry-config` (this sandbox PUSH path was missed).
/// Returns `None` when no push token is supplied (anonymous registry — today's
/// default), so an unauthenticated push is byte-for-byte unchanged.
///
/// # Errors
/// Auth-config write failure.
fn oras_push_cfg_file(
    workdir: &Path,
    push_token: Option<&str>,
    registry_ula: &str,
) -> anyhow::Result<Option<String>> {
    let Some(token) = push_token else {
        return Ok(None);
    };
    let cfg_dir = workdir.join("oras-cfg");
    crate::skopeo::write_registry_config(token, registry_ula, &cfg_dir)
        .with_context(|| format!("write oras registry auth config to {}", cfg_dir.display()))?;
    // The FILE, not the dir — `--to-registry-config` decodes it as config.json.
    Ok(Some(
        cfg_dir.join("config.json").to_string_lossy().into_owned(),
    ))
}

/// The resolved `[build]` directives for a docker build: the clone root, the
/// context directory the image is built from, and the optional Dockerfile path.
/// All are absolute paths under the clone root.
#[derive(Debug, Clone)]
pub(crate) struct BuildSpec {
    /// The clone root (`<workdir>/src`); used to place the `oci-out` push layout
    /// stably at `<workdir>/oci-out` regardless of a non-root build context.
    pub clone_root: std::path::PathBuf,
    /// The build context dir (`<src>/<[build].context>`, default `<src>`).
    pub context_dir: std::path::PathBuf,
    /// The Dockerfile path (`<src>/<[build].dockerfile>`). `None` ⇒ Docker's
    /// default (`<context_dir>/Dockerfile`).
    pub dockerfile: Option<std::path::PathBuf>,
    /// The raw `[build].context` from the toml (default `"."`), RELATIVE to the
    /// clone root. Threaded verbatim into the fc-sandbox job contract (job.json
    /// v2) so the guest builder can honor a subdir context; the docker path uses
    /// the absolute [`Self::context_dir`].
    pub raw_context: String,
    /// The raw `[build].dockerfile` from the toml (default `"Dockerfile"`),
    /// RELATIVE to the clone root. Threaded into job.json v2 alongside
    /// [`Self::raw_context`]; the guest splits it into a dockerfile DIR +
    /// `--opt filename=` basename for buildkit.
    pub raw_dockerfile: String,
}

#[cfg(test)]
impl BuildSpec {
    /// `true` when `[build].context`/`[build].dockerfile` are at their defaults
    /// (`"."` / `"Dockerfile"`). Test-only predicate over the resolved raw
    /// fields: the fc-sandbox path no longer rejects a non-default layout (the
    /// v2 guest honors it), so this is just a readability helper for the
    /// `resolve_build_spec` resolution tests.
    fn is_default_layout(&self) -> bool {
        self.raw_context == "." && self.raw_dockerfile == "Dockerfile"
    }
}

/// Resolve the [`BuildSpec`] from the `tabbify.toml` at `toml_path` (if present).
///
/// - No toml at the clone root ⇒ today's defaults: `context_dir = src`,
///   `dockerfile = None` (Docker's default `<src>/Dockerfile`).
/// - Toml present ⇒ parse it with the vendored [`crate::unified_manifest::UnifiedManifest`]
///   and resolve `[build].context` (default `.`) + `[build].dockerfile`
///   (default `Dockerfile`) relative to `src`.
///
/// # Errors
/// A malformed `tabbify.toml` (parse error) is surfaced so the build fails with
/// a clear diagnostic rather than silently ignoring a broken managed config.
fn resolve_build_spec(src: &Path, toml_path: &Path) -> anyhow::Result<BuildSpec> {
    if !toml_path.exists() {
        return Ok(BuildSpec {
            clone_root: src.to_path_buf(),
            context_dir: src.to_path_buf(),
            dockerfile: None,
            raw_context: ".".to_owned(),
            raw_dockerfile: "Dockerfile".to_owned(),
        });
    }
    let text = std::fs::read_to_string(toml_path)
        .with_context(|| format!("read tabbify.toml at {}", toml_path.display()))?;
    let manifest: crate::unified_manifest::UnifiedManifest = toml::from_str(&text)
        .with_context(|| format!("parse tabbify.toml at {}", toml_path.display()))?;
    Ok(BuildSpec {
        clone_root: src.to_path_buf(),
        context_dir: src.join(manifest.context()),
        dockerfile: Some(src.join(manifest.dockerfile())),
        raw_context: manifest.context().to_owned(),
        raw_dockerfile: manifest.dockerfile().to_owned(),
    })
}

/// Read + parse a [`BuildJob`] from `spec_path` and run it with production
/// backends (real `git`, `docker`).
///
/// This is the one-shot builder-mode entry point invoked by `--build-spec`.
/// Returns the [`ArtifactRef`] on success or a descriptive error on failure.
pub async fn run_one_shot_build(spec_path: &Path) -> anyhow::Result<ArtifactRef> {
    let text = std::fs::read_to_string(spec_path)
        .map_err(|e| anyhow::anyhow!("read build spec {}: {e}", spec_path.display()))?;
    let job: BuildJob =
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("parse build spec: {e}"))?;

    // Production backends.
    // Allow overriding the docker binary via env (follows the supervisor pattern).
    let docker_bin = std::env::var("RUNNER_DOCKER_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_DOCKER_BIN.to_owned());
    let git_bin = std::env::var("RUNNER_GIT_BIN").unwrap_or_else(|_| "git".to_owned());

    // The docker build path pushes via supervisor-side `skopeo` (copies the
    // built image from the local docker daemon to the mesh registry), so the
    // docker daemon — which has no mesh route — never talks to the registry.
    let skopeo_bin = std::env::var("SUPERVISOR_SKOPEO_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_SKOPEO_BIN.to_owned());
    // oras does the registry leg of the push (bracketed-IPv6-capable refs);
    // same env override pattern as the run-side pull.
    let oras_bin = std::env::var("SUPERVISOR_ORAS_BIN")
        .unwrap_or_else(|_| crate::config::DEFAULT_ORAS_BIN.to_owned());

    let backend = crate::build_backend::HostDockerBackend::new(docker_bin.clone());
    let git = crate::git::real_git_run(git_bin.clone());
    // Read-only stdout-capturing git seam used to resolve the clone's HEAD commit
    // SHA (`git rev-parse HEAD`) so the image is tagged with an immutable SHA.
    let git_capture = crate::git::real_git_capture(git_bin);
    let tool_runner = crate::skopeo::production_tool_runner();

    // Work directory: a fresh sub-dir under a tempdir for this build.
    // Using tempdir keeps build artefacts off any persistent volume without
    // requiring a configured data dir in build-only mode.
    let workdir = tempfile::Builder::new()
        .prefix(&format!("tbf-build-{}-", job.app_uuid))
        .tempdir()
        .context("create build workdir")?;

    run_build(
        &job,
        &backend,
        &git,
        &git_capture,
        &tool_runner,
        &skopeo_bin,
        &oras_bin,
        workdir.path(),
    )
    .await
}

#[cfg(test)]
mod tests;
