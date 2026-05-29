//! Docker container runtime (third [`crate::runtime::AppRuntime`]).
//!
//! Hosts an app by BUILDING a container image from the app's source on the
//! supervisor (`docker build`) and RUNNING it (`docker run`), then proxying HTTP
//! to the app's server inside the container. The fetched artifact is a `.tar.gz`
//! of the app directory INCLUDING a `Dockerfile`; that gzipped tar IS a Docker
//! build context, so it is piped straight to `docker build -` on stdin.
//!
//! Unlike [`crate::firecracker`], Docker is **cross-platform**: Docker Desktop /
//! the engine runs on macOS + Linux alike, and this runtime only ever shells out
//! to the `docker` CLI. So there is NO `cfg(target_os)` split — one impl serves
//! every host. A host without a reachable Docker daemon simply can't host docker
//! apps: [`docker_available`] gates that (it also drives the `docker` mesh tag),
//! and [`DockerRuntime::launch`] returns a clear `Err` when the daemon is absent.
//!
//! ## How an app runs
//! 1. `docker build -t tabbify-app-<hash> -` ← the build-context tarball on stdin.
//! 2. `docker run -d --name tbf-<hash-seq> -p 127.0.0.1:<host_port>:<app_port>`
//!    publishes the container's app port onto an ephemeral LOOPBACK host port.
//! 3. Poll `http://127.0.0.1:<host_port>` until the app answers (bounded).
//! 4. `handle(req)` proxies the whole path to that loopback base via `reqwest`.
//! 5. [`Drop`] removes the container (`docker rm -f`), best-effort.
//!
//! ## Module layout
//! - [`protocol`] — pure argv builders + the proxy core (no I/O, no daemon).
//! - [`runtime`] — [`DockerRuntime`] struct, [`AppRuntime`](crate::runtime::AppRuntime) impl,
//!   production runner factories, the launch state machine.
//! - [`build`]   — `docker build` orchestration, the launch precheck, id
//!   derivation, shared subprocess spawn helpers.
//! - [`push`]    — `pull + tag` and `tag + push` seams against the mesh registry.

use crate::config::DockerConfig;

mod build;
pub(crate) mod protocol;
mod push;
mod runtime;

/// Shared injectable command-runner seam — re-exported for [`crate::build_backend`].
pub(crate) use runtime::CommandRunner;
pub use runtime::DockerRuntime;
/// Production command-runner constructor — re-exported for [`crate::build_backend`].
pub(crate) use runtime::production_command_runner;

/// Tag a local image and push it to the mesh OCI registry — wrapped by
/// [`crate::build_backend::push_to_registry`] for the P3.4 orchestration layer.
pub(crate) use push::push_image;

#[cfg(test)]
pub(crate) use push::pull_and_tag as pull_and_tag_for_test;
#[cfg(test)]
pub(crate) use push::push_image as push_image_for_test;

/// Is this host able to run Docker containers? True iff the Docker daemon is
/// reachable (`docker info` succeeds). A host where Docker isn't installed or
/// the daemon isn't running returns `false` and the supervisor degrades to
/// WASM-only (+ firecracker on KVM), refusing docker apps loudly.
#[must_use]
pub fn docker_available() -> bool {
    protocol::docker_available_with(|| default_docker_check(crate::config::DEFAULT_DOCKER_BIN))
}

/// Default Docker probe used by [`docker_available`]: run `<docker_bin> info`
/// and succeed iff it exits 0 (the daemon answered). `docker info` talks to the
/// daemon (unlike `docker version`, whose client half succeeds even with no
/// daemon), so a zero exit means we can actually build + run.
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
/// half of a purge. `stop` / [`DockerRuntime`]'s `Drop` remove only the *container*,
/// leaving the built image on disk so a restart is fast; purge removes it.
///
/// Removes any containers created from the app's image FIRST (so the image is
/// not "in use" — independent of when the runtime's `Drop` removes its own
/// container), then force-removes the image. Best-effort: it logs but never
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
