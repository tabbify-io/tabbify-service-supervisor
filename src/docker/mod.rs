//! Docker BUILD-side helpers — the supervisor's `docker build` + `skopeo push`
//! seam (fly.io-style remote builder), the daemon capability probe, and image
//! purge.
//!
//! The platform no longer RUNS apps via `docker run`; an OCI image is converted
//! to an ext4 rootfs and booted as a Firecracker microVM. Docker survives ONLY
//! as the way images are BUILT and PUSHED to the mesh OCI registry:
//! - [`docker_available`] gates the build path (and drives the `docker` mesh tag).
//! - [`CommandRunner`] / [`production_command_runner`] are the injectable
//!   subprocess seam reused by [`crate::build_backend`] (host `docker build`)
//!   and [`crate::skopeo`] (registry push).
//! - [`push::push_image`] is the `tag + push` mirror used by the build runner.
//! - [`purge_image`] reclaims a built image from disk on app purge.
//!
//! Docker is **cross-platform** here: the engine runs on macOS + Linux alike and
//! this module only ever shells out to the `docker` CLI, so there is NO
//! `cfg(target_os)` split. A host without a reachable Docker daemon can't build
//! docker images: [`docker_available`] reports `false` and the build path is
//! refused loudly.

use std::process::Stdio;
use std::sync::Arc;

use anyhow::Result;
use tokio::process::Command;

use crate::runtime::BoxFut;

pub(crate) mod protocol;
mod push;

/// Tag a local image and push it to the mesh OCI registry — wrapped by
/// [`crate::build_backend::push_to_registry`] for the P3.4 orchestration layer.
pub(crate) use push::push_image;

/// Command-runner seam for the docker build/push paths: given a list of
/// `docker` sub-command arguments (e.g. `["tag", <local>, <ref>]`), run the
/// command and return `Ok(())` on success or `Err(diagnostic)` carrying the
/// captured stderr on failure.
///
/// The `Err(String)` payload is load-bearing: a `docker push` failure surfaces
/// the registry's stderr (e.g. `unauthorized: authentication required`) instead
/// of being collapsed to a bare `false`, so callers can bail with the real
/// diagnostic.
///
/// Production: the real `docker` binary via [`tokio::process::Command`].
/// Tests: an injected closure that records which commands were issued without
/// invoking a real Docker daemon.
///
/// Re-exported at the module level so [`crate::build_backend`] and
/// [`crate::skopeo`] can reuse the same seam.
pub(crate) type CommandRunner =
    Arc<dyn Fn(Vec<String>) -> BoxFut<'static, Result<(), String>> + Send + Sync>;

/// Build the production [`CommandRunner`]: spawns `<docker_bin> <args>`,
/// captures stderr, and returns `Ok(())` iff the process exits 0. On a
/// non-zero exit the captured stderr (trimmed) is returned in `Err` so a
/// `docker push` failure surfaces the registry diagnostic; a spawn failure
/// returns the OS error in `Err`.
///
/// Re-exported at the module level so [`crate::build_backend`] can construct
/// a production runner for the host-docker build backend.
pub(crate) fn production_command_runner(docker_bin: String) -> CommandRunner {
    Arc::new(move |args: Vec<String>| {
        let docker_bin = docker_bin.clone();
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async move {
            match Command::new(&docker_bin)
                .args(&args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .output()
                .await
            {
                Ok(out) if out.status.success() => Ok(()),
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let stderr = stderr.trim();
                    let code = out
                        .status
                        .code()
                        .map_or_else(|| "signal".to_owned(), |c| c.to_string());
                    let argv = args.join(" ");
                    Err(if stderr.is_empty() {
                        format!("`{docker_bin} {argv}` exited with status {code}")
                    } else {
                        format!("`{docker_bin} {argv}` exited with status {code}: {stderr}")
                    })
                }
                Err(e) => Err(format!(
                    "failed to spawn `{docker_bin} {}`: {e}",
                    args.join(" ")
                )),
            }
        });
        fut
    })
}

#[cfg(test)]
pub(crate) use push::push_image as push_image_for_test;

/// Is this host able to build Docker images? True iff the Docker daemon is
/// reachable (`docker info` succeeds). A host where Docker isn't installed or
/// the daemon isn't running returns `false` and the supervisor refuses to build
/// docker images (it also drops the `docker` mesh tag).
#[must_use]
pub fn docker_available() -> bool {
    protocol::docker_available_with(|| default_docker_check(crate::config::DEFAULT_DOCKER_BIN))
}

/// Default Docker probe used by [`docker_available`]: run `<docker_bin> info`
/// and succeed iff it exits 0 (the daemon answered). `docker info` talks to the
/// daemon (unlike `docker version`, whose client half succeeds even with no
/// daemon), so a zero exit means we can actually build.
fn default_docker_check(docker_bin: &str) -> bool {
    std::process::Command::new(docker_bin)
        .arg("info")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Best-effort FULL image teardown for app `id` at `version` — the disk-reclaiming
/// half of a purge.
///
/// Removes any containers created from the app's image FIRST (so the image is
/// not "in use"), then force-removes the image. Best-effort: it logs but never
/// errors, so a purge still forgets the app + clears the cache even if Docker is
/// unreachable or already clean.
pub async fn purge_image(docker_bin: &str, id: &str, version: u64) {
    use tokio::process::Command;
    let tag = protocol::versioned_image_tag(id, version);

    // Remove containers built from this image (running or stopped) so `rmi`
    // isn't blocked by an in-use image.
    if let Ok(out) = Command::new(docker_bin)
        .args(["ps", "-aq", "--filter", &format!("ancestor={tag}")])
        .stdin(std::process::Stdio::null())
        .output()
        .await
    {
        let ids: Vec<&str> = std::str::from_utf8(&out.stdout)
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        if !ids.is_empty() {
            let mut rm = vec!["rm", "-f"];
            rm.extend(ids);
            let _ = Command::new(docker_bin)
                .args(rm)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
        }
    }

    // Force-remove the image itself.
    if let Err(e) = Command::new(docker_bin)
        .args(protocol::rmi_args(&tag))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
    {
        tracing::warn!(image = %tag, error = %e, "docker rmi -f failed during purge (continuing)");
    }
}

#[cfg(test)]
mod tests;
