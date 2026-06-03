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
}
