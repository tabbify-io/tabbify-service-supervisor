//! Build-path helpers for the docker runtime: `docker build` orchestration,
//! the launch precheck, and the build-context-stem id derivation.
//!
//! `build_image` streams the gzipped-tar build context to `docker build -`'s
//! stdin and waits for the daemon to finish, bounded by a timeout. The other
//! helpers (`run_docker`, `run_docker_check`, `precheck`, `derive_id`) are the
//! shared subprocess-spawning + guard utilities the launch path relies on.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::protocol::build_args;

/// `docker build -t <tag> -` with the build-context tarball at `context`
/// streamed to the child's stdin, bounded by `timeout_secs`.
pub(super) async fn build_image(
    docker_bin: &str,
    tag: &str,
    context: &Path,
    timeout_secs: u64,
) -> Result<()> {
    let tarball = tokio::fs::read(context)
        .await
        .with_context(|| format!("read build context {}", context.display()))?;

    let mut child = Command::new(docker_bin)
        .args(build_args(tag))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn `{docker_bin} build`"))?;

    // Stream the gzipped-tar build context to docker's stdin, then close it
    // so docker sees EOF and starts the build.
    let mut stdin = child
        .stdin
        .take()
        .context("docker build child has no stdin")?;
    stdin
        .write_all(&tarball)
        .await
        .context("write build context to docker stdin")?;
    stdin.flush().await.context("flush docker stdin")?;
    drop(stdin);

    let output = tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
        .await
        .map_err(|_| anyhow::anyhow!("docker build timed out after {timeout_secs}s"))?
        .context("wait for docker build")?;

    if !output.status.success() {
        bail!(
            "docker build failed (exit {:?}): {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Run a `docker <args>` command to completion, erroring on a non-zero exit
/// with the captured stderr.
pub(super) async fn run_docker(docker_bin: &str, args: &[String]) -> Result<()> {
    let out = Command::new(docker_bin)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("spawn `{docker_bin} {}`", args.join(" ")))?;
    if !out.status.success() {
        bail!(
            "`docker {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// Run a `docker <args>` command and return `true` iff it exits 0. Never
/// errors: spawn failures or non-zero exits both yield `false`. Used for the
/// W2 build-cache check (`docker image inspect`) where absence is normal.
pub(super) async fn run_docker_check(docker_bin: &str, args: &[String]) -> bool {
    match Command::new(docker_bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
    {
        Ok(s) => s.success(),
        Err(_) => false,
    }
}

/// Derive a stable id from the build-context path (its parent dir name, which
/// in the cache layout is `v<N>` under `apps/<uuid>/`, plus the file stem).
/// Used only by [`super::DockerRuntime::launch`]; the registry calls
/// [`super::DockerRuntime::launch_with_id`] with the real uuid.
pub(super) fn derive_id(context: &Path) -> String {
    context
        .file_stem()
        .and_then(|s| s.to_str())
        .map_or_else(|| "context".to_owned(), str::to_owned)
}

/// Pre-launch guards: the Docker daemon must be reachable AND the build
/// context tarball must exist on disk. Pure (takes `available` + the path)
/// so the clear-error messages are unit-testable without a real daemon — the
/// `no-docker → clear Err` case the runtime-selection branch relies on.
///
/// # Errors
/// `available == false` (clear "requires a reachable Docker daemon"), or a
/// missing context file.
pub(super) fn precheck(available: bool, context: &Path) -> Result<()> {
    if !available {
        bail!("docker runtime requires a reachable Docker daemon (`docker info` failed)");
    }
    if !context.is_file() {
        bail!("docker build context not found at {}", context.display());
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn derive_id_uses_file_stem() {
        assert_eq!(
            derive_id(Path::new("/cache/apps/u/v1/context.tar.gz")),
            "context.tar"
        );
    }

    #[test]
    fn precheck_without_docker_errors_clearly() {
        // available = false → the clear "no docker daemon" error, regardless
        // of the context path. This is the `no-docker → clear Err` arm.
        let err = precheck(false, Path::new("/whatever/context.tar.gz")).unwrap_err();
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("docker") && msg.contains("daemon"),
            "got: {err}"
        );
    }

    #[test]
    fn precheck_with_docker_but_missing_context_errors() {
        let err = precheck(true, Path::new("/does/not/exist.tar.gz")).unwrap_err();
        assert!(
            err.to_string().contains("build context not found"),
            "got: {err}"
        );
    }

    #[test]
    fn precheck_passes_when_available_and_context_present() {
        let f = tempfile::NamedTempFile::new().unwrap();
        assert!(precheck(true, f.path()).is_ok());
    }
}
