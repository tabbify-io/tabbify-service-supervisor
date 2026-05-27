//! One-shot builder mode for `tabbify-runner`.
//!
//! When launched with `--build-spec <file>`, the runner reads a [`BuildJob`] from
//! the JSON file, runs the build pipeline end-to-end, prints the resulting
//! [`ArtifactRef`] as JSON to stdout, and exits — it never joins the mesh or
//! starts a serve loop.
//!
//! The actual clone→build→push orchestration is implemented in Task P3.4; here
//! `run_build` is a stub that fails loudly so the plumbing can be tested first.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// A one-shot build job: clone `repo_url`@`git_ref`, build an OCI image, push it
/// to the mesh registry at `registry_ula` as `<tenant>/<app_uuid>:<sha>`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildJob {
    /// HTTPS URL of the Git repository to clone.
    pub repo_url: String,
    /// Git ref (branch, tag, or full SHA) to check out.
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
}

/// The result of a build: the immutable image ref and (optionally) its digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    /// Fully-qualified OCI image reference, e.g. `"[fd5a::1]:5000/acme/myapp:abc1234"`.
    pub reff: String,
    /// OCI content-digest of the pushed manifest, e.g. `"sha256:deadbeef..."`.
    /// `None` if the registry did not return a digest.
    #[serde(default)]
    pub digest: Option<String>,
}

/// Run a build job end-to-end.
///
/// **STUB for P3.1** — the real clone→build→push lands in Task P3.4.
pub async fn run_build(_job: &BuildJob) -> anyhow::Result<ArtifactRef> {
    anyhow::bail!("build orchestration not yet implemented (lands in Phase 3 task P3.4)")
}

/// Read + parse a [`BuildJob`] from `spec_path` and run it.
///
/// This is the one-shot builder-mode entry point invoked by `--build-spec`.
/// Returns the [`ArtifactRef`] on success or a descriptive error on failure.
pub async fn run_one_shot_build(spec_path: &Path) -> anyhow::Result<ArtifactRef> {
    let text = std::fs::read_to_string(spec_path)
        .map_err(|e| anyhow::anyhow!("read build spec {}: {e}", spec_path.display()))?;
    let job: BuildJob =
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("parse build spec: {e}"))?;
    run_build(&job).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn run_one_shot_build_parses_spec_then_reaches_run_build() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        std::fs::write(
            &path,
            r#"{"repo_url":"r","git_ref":"v1","tenant":"t","app_uuid":"u","registry_ula":"[::1]:5000"}"#,
        )
        .unwrap();
        // run_build is a stub that bails with a known message — proves the spec was
        // parsed and dispatch reached run_build (the real orchestration is P3.4).
        let err = run_one_shot_build(&path).await.unwrap_err().to_string();
        assert!(
            err.contains("not yet implemented"),
            "expected stub message, got: {err}"
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
}
