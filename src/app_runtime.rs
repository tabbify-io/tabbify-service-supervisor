//! The runtime seam shared by every app runtime.
//!
//! [`AppRuntime`] is the object-safe trait the per-app listener ([`crate::host`])
//! dispatches to; the Firecracker microVM runtime
//! ([`crate::firecracker::FirecrackerRuntime`]) implements it, so the
//! hosting/serving code is identical regardless of how an app actually runs.
//! (The in-process WASM and `docker run` runtimes were both removed: an OCI
//! image is now converted to ext4 and booted as a Firecracker microVM.)
//!
//! This module holds ONLY the seam (the trait + its small value types + the
//! boxed-future aliases). Concrete runtimes live in their own modules.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response};

/// A boxed, `Send` future — the object-safe return shape for [`AppRuntime`]
/// (avoids the `async-trait` dependency, mirroring [`crate::host::MeshHost`]).
pub type BoxRespFut<'a> = Pin<Box<dyn Future<Output = Result<Response<Bytes>>> + Send + 'a>>;

/// A generic boxed, `Send` future for any output type — used by
/// [`AppRuntime::health`] so the trait stays object-safe without `async-trait`.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Liveness of the app itself (not the runner process).
///
/// Returned by [`AppRuntime::health`]. `Serving` means the runtime considers
/// the app reachable and ready; `Unavailable` carries a human-readable reason
/// (e.g. "TCP connect refused" or "container stopped").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHealth {
    /// The app is up and serving requests.
    Serving,
    /// The app is not reachable; the String explains why.
    Unavailable(String),
}

/// The reason an app runtime exited unexpectedly.
///
/// Resolved by [`AppRuntime::watch_for_exit`] when the runtime dies without an
/// explicit [`AppRuntime::shutdown`] request. The runner uses this to trigger a
/// fail-fast `process::exit(1)` so the supervisor's L2 monitor respawns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    /// The runtime process / container died; the String carries a detail
    /// (e.g. the container name and exit code).
    Died(String),
}

/// The runtime seam the per-app listener ([`crate::host`]) dispatches to. The
/// Firecracker microVM runtime ([`crate::firecracker::FirecrackerRuntime`])
/// implements it, so the hosting/serving code is identical regardless of how an
/// app runs.
///
/// Object-safe (`Arc<dyn AppRuntime>`): the registry picks the concrete runtime
/// from the deploy-time runtime selection and hands the listener a trait object.
pub trait AppRuntime: Send + Sync {
    /// Drive one HTTP request through the app and return its response.
    ///
    /// # Errors
    /// Runtime-specific: a proxy failure talking to the guest/container.
    fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a>;

    /// Liveness of the app itself (not the runner process).
    ///
    /// Default: [`RuntimeHealth::Serving`]. Firecracker and Docker override this
    /// with a real probe (TCP connect to the guest/container).
    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        Box::pin(async { RuntimeHealth::Serving })
    }

    /// Resolves when the runtime dies UNEXPECTEDLY (without an explicit
    /// [`shutdown`] call). The runner selects on this alongside its shutdown
    /// signal: if this resolves first the runner calls `process::exit(1)` so
    /// the supervisor's L2 monitor respawns it with backoff.
    ///
    /// Default: **never resolves**. Docker and Firecracker override this with
    /// real process/container watching.
    ///
    /// [`shutdown`]: AppRuntime::shutdown
    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
        Box::pin(std::future::pending())
    }

    /// Graceful teardown of the runtime's resources. Idempotent. Default: no-op.
    ///
    /// Called by the runner on the [`RunnerExit::CleanShutdown`] path — BEFORE
    /// `process::exit(0)` — so the runtime can release its external resources
    /// (stop a container, kill a VM + tear down the tap) cleanly. NOT called on
    /// [`RunnerExit::Crashed`]: the runtime already died; [`Drop`] + the L2
    /// kill-before-respawn handle remnants instead.
    ///
    /// Implementations MUST be idempotent: a second call must be a no-op (the
    /// container / VM may already be gone by the time `Drop` runs its own
    /// best-effort cleanup).
    ///
    /// Default: **no-op** — a runtime with no external resources drops cleanly.
    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
        Box::pin(async {})
    }

    /// The IPv4 TCP address of the guest's SSH daemon, if any.
    ///
    /// For a Firecracker microVM running a dev/devbox image, returns
    /// `Some(<guest_ip>:2222)` — the host-side tap IPv4 address where the
    /// guest's sshd is listening. The runner's L4 TCP forwarder
    /// ([`crate::tcp_forward`]) binds `[app_ula]:2222` on the host mesh
    /// interface and proxies every accepted connection here so the node can SSH
    /// into the guest via `root@[app_ula]:2222`.
    ///
    /// Default: `None` — non-FC runtimes (docker, stub, test fakes) do not
    /// expose a guest SSH target.
    fn guest_ssh_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// The IPv4 TCP address of the guest's structured code-service RPC port, if
    /// any.
    ///
    /// For a Firecracker microVM running a WORKSPACE image, returns
    /// `Some(<guest_ip>:CODE_SERVICE_PORT)` (8731). The runner's L4 forwarder
    /// binds `[app_ula]:8731` on the mesh interface and proxies to it, so the
    /// node can call `POST http://[app_ula]:8731/v1/code/<method>` (Seam 1).
    /// This is a SEPARATE port from the axum runner's :8730 app port and from
    /// the :2222 ssh exec port.
    ///
    /// Default: `None` — non-workspace runtimes (regular apps, docker, stub)
    /// expose no code-service target, so no :8731 forwarder is started.
    fn guest_code_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// Refresh this runtime's warm-restore snapshot IN-PLACE — without
    /// stopping or swapping the running app.
    ///
    /// For a Firecracker microVM running a workspace image this pauses the
    /// guest, writes a fresh `/snapshot/create` to the per-uuid cache dir, and
    /// resumes the guest (it keeps serving). It is the explicit POST-INDEX
    /// refresh path: the node issues it AFTER the code-service reports
    /// `indexed && idle`, so the captured RAM holds a warm LSP index. A plain
    /// restart must NOT re-snapshot (cold_boot only snapshots when
    /// `!files_present`); the only way to refresh a present snapshot is this
    /// method, driven by `Cmd::Snapshot`.
    ///
    /// Default: **no-op `Ok(())`** — a runtime with no snapshottable state
    /// (docker, stub, test fakes) reports success without doing anything.
    ///
    /// # Errors
    /// Runtime-specific: the snapshot create/pause/resume sequence failed. The
    /// VM is left RUNNING on any error (the FC impl always `ensure_resumed`s).
    fn snapshot<'a>(&'a self) -> BoxFut<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A minimal runtime that relies entirely on the trait defaults — it only
    /// implements the one required method, `handle`.
    struct StubRuntime;

    impl AppRuntime for StubRuntime {
        fn handle<'a>(&'a self, _request: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }
        // health / watch_for_exit / shutdown / guest_ssh_addr all use defaults.
    }

    /// The `AppRuntime::guest_ssh_addr` default returns `None`: a runtime that
    /// does not override it (docker, stub, any non-FC fake) exposes no guest
    /// SSH target, so the runner never starts an SSH forwarder for it.
    #[test]
    fn guest_ssh_addr_default_is_none() {
        assert_eq!(StubRuntime.guest_ssh_addr(), None);
    }

    /// The `AppRuntime::snapshot` default is a no-op `Ok(())`: a runtime with
    /// no snapshottable state (docker, stub, test fakes) reports success
    /// without doing anything, so a workspace `Cmd::Snapshot` against such a
    /// runtime never errors.
    #[tokio::test]
    async fn snapshot_default_is_ok_noop() {
        let result = StubRuntime.snapshot().await;
        assert!(result.is_ok(), "default snapshot() must be a no-op Ok(())");
    }

    /// `guest_code_addr` default is `None`: a non-FC runtime exposes no code-
    /// service target, so the runner never starts the :8731 forwarder for it.
    #[test]
    fn guest_code_addr_default_is_none() {
        assert_eq!(StubRuntime.guest_code_addr(), None);
    }
}
