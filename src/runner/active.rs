//! Swappable runtime cell — wraps `Arc<dyn AppRuntime>` in an `ArcSwap`-based
//! cell so P2.3 can atomically replace the active runtime for zero-downtime
//! deploys without touching the listener or the mesh peer.
//!
//! # Why a newtype wrapper?
//! `arc_swap::ArcSwap<T>` requires `T: Sized`, but `dyn AppRuntime` is
//! unsized.  We box it in a `RuntimeSlot` newtype (`struct RuntimeSlot(Arc<dyn
//! AppRuntime>)`).  `ArcSwap<RuntimeSlot>` then works — the slot itself IS
//! sized.  Loading gives an `Arc<RuntimeSlot>`; a cheap `.0.clone()` yields the
//! inner `Arc<dyn AppRuntime>`.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

/// Poll interval for [`perform_swap`]'s health check on the new runtime.
const SWAP_HEALTH_POLL: Duration = Duration::from_millis(100);

/// Why a zero-downtime swap was aborted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwapError {
    /// The new runtime never reported [`RuntimeHealth::Serving`] before the
    /// deadline elapsed. The OLD runtime stays active (no downtime); the new
    /// runtime has already been shut down by [`perform_swap`].
    Unhealthy,
}

impl std::fmt::Display for SwapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unhealthy => write!(f, "new runtime did not become healthy before deadline"),
        }
    }
}

impl std::error::Error for SwapError {}

/// Bring `new` into service on `active` with zero downtime.
///
/// The state machine:
/// 1. **Health-gate.** Poll `new.health()` every [`SWAP_HEALTH_POLL`] until it
///    reports [`RuntimeHealth::Serving`] or `deadline` elapses.
/// 2. **Flip (healthy).** Atomically [`ActiveRuntime::swap`] `new` in. Spawn a
///    detached drain task that sleeps `drain`, then calls
///    [`AppRuntime::shutdown`] on the OLD runtime. Return `Ok(())` immediately —
///    in-flight requests on the old runtime finish because each request holds
///    its own `Arc` clone of it (P2.2's design).
/// 3. **Abort (timeout).** Call `new.shutdown()` and return [`SwapError`]. The
///    OLD runtime stays active, so a failed deploy causes **no downtime** and no
///    flip.
///
/// The flip happens ONLY after health passes, so the live runtime is never
/// replaced by an unhealthy one.
///
/// # Errors
/// [`SwapError::Unhealthy`] if `new` never became healthy within `deadline`.
pub async fn perform_swap(
    active: &ActiveRuntime,
    new: Arc<dyn AppRuntime>,
    drain: Duration,
    deadline: Duration,
) -> Result<(), SwapError> {
    // --- 1. health-gate the new runtime ------------------------------------
    if !await_healthy(new.as_ref(), deadline).await {
        // Never became healthy: tear the new one down, leave OLD serving.
        new.shutdown().await;
        return Err(SwapError::Unhealthy);
    }

    // --- 2. flip: install `new`, get the OLD back for draining -------------
    let old = active.swap(new);

    // --- 3. drain the OLD runtime off the hot path (fire-and-forget) -------
    // Spawned so `perform_swap` returns promptly after the flip; in-flight
    // requests on `old` keep their own Arc clone alive until they complete.
    tokio::spawn(async move {
        tokio::time::sleep(drain).await;
        old.shutdown().await;
    });

    Ok(())
}

/// Poll `rt.health()` every [`SWAP_HEALTH_POLL`] until it returns
/// [`RuntimeHealth::Serving`] or `deadline` elapses. Returns `true` iff the
/// runtime became healthy in time.
///
/// The first probe runs immediately (a runtime that is already serving — e.g. a
/// warm-started one — passes without waiting a full interval).
async fn await_healthy(rt: &dyn AppRuntime, deadline: Duration) -> bool {
    let probe = async {
        loop {
            if rt.health().await == RuntimeHealth::Serving {
                return;
            }
            tokio::time::sleep(SWAP_HEALTH_POLL).await;
        }
    };
    tokio::time::timeout(deadline, probe).await.is_ok()
}

/// Sized newtype that holds one `Arc<dyn AppRuntime>`.
///
/// Required because `ArcSwap<T>` requires `T: Sized`, and `dyn AppRuntime` is
/// not.  All callers interact with `ActiveRuntime`, never with this type
/// directly.
pub(crate) struct RuntimeSlot(pub(crate) Arc<dyn AppRuntime>);

/// A swappable holder for the currently-active [`AppRuntime`].
///
/// The runner builds one `Arc<ActiveRuntime>` at startup and passes it where
/// `Arc<dyn AppRuntime>` is expected (via `AppRuntime` impl + coercion).
/// P2.3 calls [`ActiveRuntime::swap`] to atomically install a new runtime;
/// everything waiting on the old runtime drains naturally because the old `Arc`
/// is returned to the caller for graceful shutdown.
///
/// The `swapped` notifier is a seam for P2.3's crash-watch task: it re-arms
/// `watch_for_exit` on the NEW runtime after a swap.  In P2.2 nothing calls
/// `swap`, so the notifier is never fired and behavior is identical to holding
/// a plain `Arc<dyn AppRuntime>`.
pub struct ActiveRuntime {
    cell: ArcSwap<RuntimeSlot>,
    swapped: Notify,
}

impl ActiveRuntime {
    /// Wrap `rt` in a new `ActiveRuntime`.
    pub fn new(rt: Arc<dyn AppRuntime>) -> Self {
        Self {
            cell: ArcSwap::new(Arc::new(RuntimeSlot(rt))),
            swapped: Notify::new(),
        }
    }

    /// Return a cheap `Arc` clone of the currently-active runtime.
    pub fn load(&self) -> Arc<dyn AppRuntime> {
        self.cell.load().0.clone()
    }

    /// Atomically install `new` as the active runtime.
    ///
    /// Returns the **previous** runtime so the caller can drain in-flight
    /// requests and call [`AppRuntime::shutdown`] on it.
    ///
    /// Wakes any task waiting on [`ActiveRuntime::swapped`] so P2.3's
    /// crash-watch loop can re-arm `watch_for_exit` on the new runtime.
    ///
    /// # P2.3 interaction
    /// After a swap the OLD runtime's `watch_for_exit` future is still polled
    /// by `run_until_exit`.  If the old runtime dies after the swap that future
    /// resolves and the runner exits — which is safe because P2.3 will have
    /// already started a new listener before dropping the old runtime.  The
    /// full drain + watch-re-arm logic is implemented in P2.3, NOT here.
    pub fn swap(&self, new: Arc<dyn AppRuntime>) -> Arc<dyn AppRuntime> {
        let old_slot = self.cell.swap(Arc::new(RuntimeSlot(new)));
        // `notify_waiters` (not `notify_one`) wakes tasks that called
        // `swapped().await` BEFORE this swap fires — P2.3 registers the
        // waiter BEFORE awaiting so the notification is not missed.
        self.swapped.notify_waiters();
        old_slot.0.clone()
    }

    /// Resolves the next time [`swap`] is called.
    ///
    /// Intended for P2.3's crash-watch task: register with `notified()` BEFORE
    /// awaiting so the notification is not missed if `swap` fires concurrently.
    pub async fn swapped(&self) {
        self.swapped.notified().await
    }
}

// ---- AppRuntime delegation --------------------------------------------------

// Each method loads the current slot Arc, extracts the inner runtime Arc, moves
// it into the async block so the future outlives the `&'a self` borrow, and
// delegates to the inner runtime.  The `let rt = self.load()` pattern is the
// crux: the loaded `Arc<dyn AppRuntime>` is MOVED into the future.

impl AppRuntime for ActiveRuntime {
    fn handle<'a>(&'a self, request: http::Request<bytes::Bytes>) -> BoxRespFut<'a> {
        let rt = self.load();
        Box::pin(async move { rt.handle(request).await })
    }

    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        let rt = self.load();
        Box::pin(async move { rt.health().await })
    }

    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
        let rt = self.load();
        Box::pin(async move { rt.watch_for_exit().await })
    }

    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
        let rt = self.load();
        Box::pin(async move { rt.shutdown().await })
    }

    fn guest_ssh_addr(&self) -> Option<std::net::SocketAddr> {
        self.load().guest_ssh_addr()
    }
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use http::{Request, Response};

    use super::*;
    use crate::runtime::{AppRuntime, BoxRespFut};

    // Minimal fake runtime that returns a fixed tag in its response body.
    struct FakeRt {
        tag: &'static str,
    }

    impl FakeRt {
        fn new(tag: &'static str) -> Self {
            Self { tag }
        }
    }

    impl AppRuntime for FakeRt {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            let tag = self.tag;
            Box::pin(async move {
                Ok(Response::builder()
                    .status(200)
                    .body(Bytes::from(tag))
                    .unwrap())
            })
        }
        // health / watch_for_exit / shutdown all use trait defaults
    }

    fn req() -> Request<Bytes> {
        Request::builder()
            .method("GET")
            .uri("http://localhost/")
            .body(Bytes::new())
            .unwrap()
    }

    fn body_of(resp: anyhow::Result<Response<Bytes>>) -> String {
        let b = resp.expect("handle must succeed");
        String::from_utf8_lossy(b.body()).into_owned()
    }

    async fn fake_tag(rt: &Arc<dyn AppRuntime>) -> String {
        body_of(rt.handle(req()).await)
    }

    /// ActiveRuntime forwards handle() to the initial runtime.
    #[tokio::test]
    async fn active_runtime_forwards_handle_to_initial() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        assert_eq!(body_of(active.handle(req()).await), "A");
    }

    /// After swap() ActiveRuntime forwards handle() to the NEW runtime.
    #[tokio::test]
    async fn active_runtime_swap_serves_new_runtime() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        let old = active.swap(Arc::new(FakeRt::new("B")));
        assert_eq!(
            body_of(active.handle(req()).await),
            "B",
            "must serve B after swap"
        );
        assert_eq!(
            fake_tag(&old).await,
            "A",
            "swap must return the previous runtime"
        );
    }

    /// swap() returns the OLD runtime (the caller drains + shuts it down).
    #[tokio::test]
    async fn swap_returns_previous_runtime() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("X")));
        let old = active.swap(Arc::new(FakeRt::new("Y")));
        assert_eq!(fake_tag(&old).await, "X");
    }

    /// Multiple swaps: each swap installs the new runtime and discards the old.
    #[tokio::test]
    async fn multiple_swaps_serve_latest_runtime() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        active.swap(Arc::new(FakeRt::new("B")));
        active.swap(Arc::new(FakeRt::new("C")));
        assert_eq!(body_of(active.handle(req()).await), "C");
    }

    /// health() is delegated to the current runtime (default = Serving).
    #[tokio::test]
    async fn active_runtime_health_delegates_to_current() {
        use crate::runtime::RuntimeHealth;
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        assert_eq!(active.health().await, RuntimeHealth::Serving);
    }

    /// watch_for_exit() is delegated (default = never resolves).
    #[tokio::test]
    async fn active_runtime_watch_for_exit_delegates_pending() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            active.watch_for_exit(),
        )
        .await;
        assert!(
            result.is_err(),
            "watch_for_exit must be pending via delegation"
        );
    }

    /// shutdown() completes immediately (default no-op).
    #[tokio::test]
    async fn active_runtime_shutdown_completes() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));
        tokio::time::timeout(std::time::Duration::from_millis(50), active.shutdown())
            .await
            .expect("shutdown must complete immediately");
    }

    /// swap() fires the swapped notifier.
    #[tokio::test]
    async fn swap_notifies_swapped_waiter() {
        let active = Arc::new(ActiveRuntime::new(Arc::new(FakeRt::new("A"))));
        let active2 = active.clone();
        // Register the waiter BEFORE the swap fires.
        let waiter = tokio::spawn(async move { active2.swapped().await });
        // Yield so the waiter task runs and registers with `notified()`.
        tokio::task::yield_now().await;
        active.swap(Arc::new(FakeRt::new("B")));
        tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("swapped() must resolve after swap")
            .expect("waiter task must not panic");
    }

    // ---- perform_swap state machine -----------------------------------------

    use std::sync::atomic::{AtomicBool, Ordering};

    /// A controllable fake runtime for the swap state machine:
    /// - `tag` distinguishes it in `handle()` bodies,
    /// - `healthy` controls whether `health()` reports `Serving`,
    /// - `shutdown_called` records whether `shutdown()` was invoked.
    struct SwapFake {
        tag: &'static str,
        healthy: bool,
        shutdown_called: Arc<AtomicBool>,
    }

    impl SwapFake {
        /// A runtime that reports `Serving` immediately.
        fn healthy(tag: &'static str) -> (Arc<Self>, Arc<AtomicBool>) {
            let flag = Arc::new(AtomicBool::new(false));
            let rt = Arc::new(Self {
                tag,
                healthy: true,
                shutdown_called: flag.clone(),
            });
            (rt, flag)
        }

        /// A runtime that reports `Unavailable` forever.
        fn unhealthy(tag: &'static str) -> (Arc<Self>, Arc<AtomicBool>) {
            let flag = Arc::new(AtomicBool::new(false));
            let rt = Arc::new(Self {
                tag,
                healthy: false,
                shutdown_called: flag.clone(),
            });
            (rt, flag)
        }
    }

    impl AppRuntime for SwapFake {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            let tag = self.tag;
            Box::pin(async move {
                Ok(Response::builder()
                    .status(200)
                    .body(Bytes::from(tag))
                    .unwrap())
            })
        }

        fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
            let healthy = self.healthy;
            Box::pin(async move {
                if healthy {
                    RuntimeHealth::Serving
                } else {
                    RuntimeHealth::Unavailable("fake never ready".to_owned())
                }
            })
        }

        fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
            let flag = self.shutdown_called.clone();
            Box::pin(async move {
                flag.store(true, Ordering::SeqCst);
            })
        }
    }

    /// A healthy new runtime is swapped in; the OLD runtime is drained + shut
    /// down; the NEW runtime is NOT shut down.
    #[tokio::test]
    async fn healthy_new_swaps_and_drains_old() {
        let (old, old_shut) = SwapFake::healthy("OLD");
        let (new, new_shut) = SwapFake::healthy("NEW");
        let active = ActiveRuntime::new(old);

        assert!(
            !old_shut.load(Ordering::SeqCst),
            "precondition: OLD.shutdown not yet called"
        );

        perform_swap(
            &active,
            new,
            Duration::from_millis(0),
            Duration::from_secs(2),
        )
        .await
        .expect("healthy swap must succeed");

        // The active runtime is now NEW.
        assert_eq!(
            body_of(active.handle(req()).await),
            "NEW",
            "active.load() must serve the NEW runtime after a healthy swap"
        );

        // Give the spawned drain task (drain = 0ms) a chance to run.
        for _ in 0..50 {
            if old_shut.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(
            old_shut.load(Ordering::SeqCst),
            "OLD.shutdown() must be called after drain"
        );
        assert!(
            !new_shut.load(Ordering::SeqCst),
            "NEW.shutdown() must NOT be called (it is now serving)"
        );
    }

    /// The `Arc::ptr_eq` predicate that `run_until_exit`'s re-arm guard relies
    /// on: two `load()` calls for the SAME active runtime are pointer-equal, but
    /// a `load()` taken before a swap is pointer-DISTINCT from one taken after.
    ///
    /// This pins the invariant that lets the crash-watch tell "the runtime that
    /// died is still active" (real crash) from "it is a retired old runtime"
    /// (expected post-swap shutdown) without depending on `select!` ordering.
    #[tokio::test]
    async fn load_ptr_eq_tracks_active_runtime_across_swap() {
        let active = ActiveRuntime::new(Arc::new(FakeRt::new("A")));

        let before = active.load();
        assert!(
            Arc::ptr_eq(&before, &active.load()),
            "two loads of the same active runtime must be pointer-equal"
        );

        active.swap(Arc::new(FakeRt::new("B")));

        // The pre-swap handle is now a RETIRED runtime: pointer-distinct from
        // the new active one — exactly what the re-arm guard detects.
        assert!(
            !Arc::ptr_eq(&before, &active.load()),
            "a pre-swap load must be pointer-distinct from the post-swap active runtime"
        );
        // ...and the new active runtime is again self-consistent across loads.
        assert!(Arc::ptr_eq(&active.load(), &active.load()));
    }

    /// An unhealthy new runtime aborts the swap: the OLD runtime stays active
    /// (no downtime), the NEW runtime is shut down, the OLD is not.
    #[tokio::test]
    async fn unhealthy_new_aborts_keeping_old() {
        let (old, old_shut) = SwapFake::healthy("OLD");
        let (new, new_shut) = SwapFake::unhealthy("NEW");
        let active = ActiveRuntime::new(old);

        let err = perform_swap(
            &active,
            new,
            Duration::from_millis(0),
            Duration::from_millis(300),
        )
        .await
        .expect_err("unhealthy new must abort the swap");
        assert_eq!(err, SwapError::Unhealthy);

        // The active runtime is STILL OLD — no flip happened.
        assert_eq!(
            body_of(active.handle(req()).await),
            "OLD",
            "active.load() must STILL be OLD after an aborted swap (no downtime)"
        );

        assert!(
            new_shut.load(Ordering::SeqCst),
            "NEW.shutdown() must be called when the swap aborts"
        );
        assert!(
            !old_shut.load(Ordering::SeqCst),
            "OLD.shutdown() must NOT be called — it stays in service"
        );
    }
}
