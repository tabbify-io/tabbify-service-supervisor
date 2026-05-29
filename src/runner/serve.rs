//! Runner serve core — hosts exactly one app on its app-ULA.
//!
//! [`RunnerServe::start`] is the main entry point: given a [`ServeConfig`] it:
//! 1. creates an [`S3Fetcher`] and fetches the app artifact;
//! 2. derives the app-ULA via [`derive_app_ula`];
//! 3. builds the app runtime via the shared [`crate::build::build_runtime`];
//! 4. creates an [`AppHost`] (loopback when `no_mesh`; otherwise it joins the
//!    mesh claiming `requested_ula = derive_app_ula(uuid)` so the runner's OWN
//!    peer-ULA *is* the app-ULA, then binds `[my_ula]:port` directly) and hosts
//!    the app via [`AppHost::host`];
//! 5. wraps the live [`HostedApp`] in a [`RunnerServe`] that exposes the bound
//!    address — the test (and the binary) dial this to reach the app.
//!
//! The returned [`RunnerServe`] also exposes a [`RunnerServe::lifecycle`] handle
//! that the control server (Task 1.4) uses to share ownership of the live
//! listener, allowing `Stop`, `Purge`, and `Health` commands to operate on the
//! same `HostedApp`.
//!
//! # Mesh path (Task 1.3)
//! When `no_mesh = false` the runner joins the mesh as a `runner`-kind peer
//! ([`build_runner_join_config`] builds the [`mesh_joiner::JoinConfig`]),
//! claiming `requested_ula = derive_app_ula(uuid)` and declaring its
//! `parent` + `app_uuid`. Because the coordinator routes that ULA straight to
//! this peer, the runner binds its OWN ULA via [`AppHost::mesh_self`] — it does
//! NOT need the separate `host_app_ula` app-route layer (that advertised
//! app-ULAs distinct from a peer's own ULA, used by the old multi-app
//! supervisor).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use crate::app_ula::derive_app_ula;
use crate::build::build_runtime;
use crate::config::{DockerConfig, FcConfig};
use crate::fetcher::S3Fetcher;
use crate::host::{AppHost, AppServe};
use crate::mesh::MeshMembership;
use crate::runner::active::ActiveRuntime;
use crate::runner::control::RunnerLifecycle;
use crate::runtime::{AppRuntime, ExitReason};

/// The outcome of the runner's main select loop.
///
/// A pure decision type — `decide_exit` maps the winning branch of the
/// `tokio::select!` to this value. `process::exit` is NOT called here so the
/// decision is unit-testable without side effects.
#[derive(Debug, PartialEq, Eq)]
pub enum RunnerExit {
    /// The runtime died unexpectedly (`watch_for_exit` resolved first). The
    /// runner should `process::exit(1)` so the L2 monitor respawns it.
    Crashed(String),
    /// A clean shutdown was requested (`shutdown_rx` resolved first). The
    /// runner should exit cleanly (return 0).
    CleanShutdown,
}

/// Pure decision: given which branch of the runner's main `select!` won,
/// return the corresponding [`RunnerExit`].
///
/// Keeping this separate from `process::exit` makes it unit-testable. The
/// caller is responsible for acting on the result.
#[must_use]
pub fn decide_exit(exit_reason: Option<ExitReason>) -> RunnerExit {
    match exit_reason {
        Some(ExitReason::Died(reason)) => RunnerExit::Crashed(reason),
        None => RunnerExit::CleanShutdown,
    }
}

/// Drive the runner's main loop: park until the **currently-active** runtime
/// dies unexpectedly (`watch_for_exit` resolves → `Crashed`) or `shutdown_rx`
/// fires (operator shutdown → `CleanShutdown`).
///
/// # Re-arming across swaps (P2.3)
/// A zero-downtime swap ([`perform_swap`]) installs a new runtime and, after a
/// drain, calls `shutdown()` on the OLD one — which makes the OLD runtime's
/// `watch_for_exit` resolve. That MUST NOT be treated as a crash: it is the
/// retired runtime exiting as expected. So the loop:
/// - selects on the active runtime's `watch_for_exit` AND on
///   [`ActiveRuntime::swapped`] (registered BEFORE awaiting so a concurrent swap
///   is not missed);
/// - on `watch_for_exit`: only returns `Crashed` if the runtime that died is
///   STILL the active one (`Arc::ptr_eq`); otherwise it was a retired old
///   runtime — re-arm by looping;
/// - on `swapped`: a swap happened — re-arm the watch on the NEW active runtime;
/// - on `shutdown_rx`: clean shutdown.
///
/// `shutdown_rx` is polled by `&mut` inside the loop so it survives across
/// re-arm iterations (a `oneshot::Receiver` is consumed only when it resolves).
///
/// Returns a [`RunnerExit`] that the binary translates into a `process::exit`
/// call. Extracted as a free function so it is testable with a fake runtime
/// without going through the full `RunnerServe::start` path.
///
/// [`perform_swap`]: crate::runner::active::perform_swap
pub async fn run_until_exit(
    active: Arc<ActiveRuntime>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> RunnerExit {
    loop {
        // The currently-active runtime, and a future that resolves on the NEXT
        // swap. `swapped()` is registered BEFORE we await the select, so a swap
        // racing with this iteration is not missed (P2.2's `notify_waiters`).
        let current = active.load();
        let swapped = active.swapped();

        tokio::select! {
            exit = current.watch_for_exit() => {
                // Only the death of the STILL-active runtime is a crash. After a
                // swap, `current` is a retired old runtime being shut down — its
                // `watch_for_exit` resolving is expected, so we ignore it and
                // re-arm on the new active runtime by looping.
                //
                // `Arc::ptr_eq` compares the pointed-to allocation: `load()`
                // returns a fresh Arc clone each call, but clones of the SAME
                // runtime share one allocation, so this correctly detects
                // "still the active one" (works on `Arc<dyn AppRuntime>`).
                if Arc::ptr_eq(&current, &active.load()) {
                    return decide_exit(Some(exit));
                }
                // else: retired old runtime died as expected — loop to re-arm.
            }
            () = swapped => {
                // A swap occurred — loop to re-arm `watch_for_exit` on the NEW
                // active runtime.
            }
            _ = &mut shutdown_rx => return decide_exit(None),
        }
    }
}

/// Configuration subset the runner serve core needs (decoupled from the full
/// clap [`crate::runner::RunnerConfig`] so the unit tests can construct it
/// without parsing the CLI).
pub struct ServeConfig {
    /// UUID of the app to host (string form).
    pub uuid: String,
    /// S3 base URL for artifact fetch (injected by tests as a wiremock URI).
    pub s3_base_url: String,
    /// Local data dir for the artifact cache.
    pub data_dir: PathBuf,
    /// When `true` the runner binds a loopback listener (no TUN required).
    /// When `false` the runner joins the mesh claiming its app-ULA and binds it.
    pub no_mesh: bool,
    /// Mesh coordinator control-plane URL (used only when `no_mesh = false`).
    pub coordinator_url: String,
    /// Human-readable display name advertised to the coordinator (mesh mode).
    pub display_name: String,
    /// ULA of the parent supervisor that spawned this runner, declared on the
    /// mesh join so the node can build the supervisor → runners topology.
    /// `None` for a standalone runner.
    pub parent: Option<String>,
    /// Listener port used when binding the runner's own mesh ULA.
    pub port: u16,
    /// Firecracker runtime config.
    pub fc: FcConfig,
    /// Docker runtime config.
    pub docker: DockerConfig,
    /// OCI image ref of a previously-deployed version. When `Some`, it is
    /// applied to the fetched manifest's docker `registry_ref` before the
    /// INITIAL [`build_runtime`], so a supervisor-driven respawn comes up on the
    /// deployed version. `None` = build from the S3 manifest as usual.
    pub image_ref: Option<String>,
}

/// A live per-app runner: holds the [`HostedApp`] (and thus its listener task)
/// alive for the duration of this value via a shared [`RunnerLifecycle`] handle
/// that the control server may also hold.
pub struct RunnerServe {
    /// The address the listener bound (loopback ephemeral in `--no-mesh` mode).
    addr: SocketAddr,
    /// The swappable active-runtime cell. Held so the binary can pass it to
    /// [`run_until_exit`] (which needs `load()`/`swapped()` to re-arm its
    /// crash-watch across zero-downtime swaps).
    active: Arc<ActiveRuntime>,
    /// Shared lifecycle state (wraps the live `HostedApp`). Kept here so the
    /// listener task lives as long as the `RunnerServe` does unless the control
    /// server issues a `Stop`.
    lifecycle: RunnerLifecycle,
    /// Mesh membership, held only in mesh mode (`None` under `--no-mesh`). Kept
    /// for the runner's lifetime because dropping it drops the inner `Joiner`,
    /// which aborts the WG/TUN background tasks and closes the tunnel — so the
    /// runner's ULA would stop being reachable. Never read; held only to keep
    /// the mesh up.
    _membership: Option<MeshMembership>,
}

impl RunnerServe {
    /// Fetch the app artifact, build the runtime, and start the per-app
    /// listener. Returns a [`RunnerServe`] holding the live listener.
    ///
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the S3 fetch fails;
    /// - the runtime build fails (wasm compile / firecracker / docker);
    /// - the mesh join fails (mesh mode: no TUN/root, coordinator unreachable);
    /// - the listener fails to bind.
    pub async fn start(cfg: ServeConfig) -> Result<Self> {
        let parsed_uuid = Uuid::parse_str(&cfg.uuid)
            .with_context(|| format!("invalid app uuid: {:?}", cfg.uuid))?;
        let app_ula = derive_app_ula(parsed_uuid);

        let fetcher = S3Fetcher::new(&cfg.s3_base_url, &cfg.data_dir);
        let mut fetched = fetcher
            .fetch(&cfg.uuid)
            .await
            .with_context(|| format!("fetch app {}", cfg.uuid))?;

        // If a deployed image ref was passed (`--image-ref`, set by the
        // orchestrator on a respawn), apply it to the manifest's docker
        // `registry_ref` so the INITIAL build comes up on the deployed version
        // (a `docker pull <ref>` instead of a source build). The override is
        // also reflected in the `fetched` we store for later `Deploy` calls, so
        // the runner's baseline version is the deployed one. Ignored for
        // wasm/firecracker (`build_runtime` does not read `registry_ref` there).
        if let Some(reff) = cfg.image_ref.as_deref() {
            fetched = crate::build::fetched_with_ref(&fetched, reff);
            tracing::info!(uuid = %cfg.uuid, %reff, "applied deployed image ref to manifest for initial build");
        }

        let initial_runtime =
            build_runtime(None, &cfg.uuid, &fetched, &cfg.fc, &cfg.docker, &cfg.data_dir)
                .await
                .with_context(|| format!("build runtime for {}", cfg.uuid))?;

        // Wrap the initial runtime in a swappable cell so P2.3 can atomically
        // replace it for zero-downtime deploys without touching the listener or
        // the mesh peer.  In P2.2 no swap happens; behavior is identical to
        // holding a plain Arc<dyn AppRuntime>.
        let active: Arc<ActiveRuntime> = Arc::new(ActiveRuntime::new(initial_runtime));

        // Clone the active-runtime handle so the lifecycle can call health() on
        // it independently of the AppServe's copy.  Both coerce to
        // Arc<dyn AppRuntime> via the AppRuntime impl on ActiveRuntime.
        let runtime_for_lifecycle: Arc<dyn AppRuntime> = active.clone();

        // No idle-reaper in the runner yet — the on_request callback is a no-op.
        let on_request: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let serve = AppServe::new(active.clone() as Arc<dyn AppRuntime>, on_request);

        // Build the host + (in mesh mode) the membership that MUST outlive this
        // function — dropping it tears down the WG/TUN tunnel (see field doc).
        let (host, membership) = if cfg.no_mesh {
            (AppHost::loopback(), None)
        } else {
            // Mesh mode: join the coordinator claiming `requested_ula = app_ula`
            // (+ kind=runner, parent, app_uuid). The coordinator routes that
            // ULA to us, so our OWN peer-ULA *is* the app-ULA: bind it directly
            // via `mesh_self` — no separate `host_app_ula` app-route needed.
            let join = build_runner_join_config(&cfg);
            let membership = MeshMembership::join_runner(join)
                .await
                .context("join mesh as runner")?;
            let my_ula = membership.my_ula();
            tracing::info!(
                %my_ula,
                peer_id = %membership.peer_id(),
                %app_ula,
                "runner joined mesh; binding own ULA"
            );
            (AppHost::mesh_self(my_ula, cfg.port), Some(membership))
        };

        let hosted = host
            .host(app_ula, serve)
            .await
            .with_context(|| format!("host app {} on {:?}", cfg.uuid, app_ula))?;

        let addr = hosted.addr;

        let lifecycle = RunnerLifecycle {
            uuid: cfg.uuid.clone(),
            version: fetched.version,
            app_ula: app_ula.to_string(),
            hosted: Arc::new(Mutex::new(Some(hosted))),
            fetcher,
            docker: cfg.docker,
            runtime: runtime_for_lifecycle,
            // Shared with this RunnerServe + the binary's run_until_exit loop so
            // a `Deploy` swaps the live runtime here and the crash-watch re-arms.
            active: active.clone(),
            // The build context `Deploy` rebuilds from (manifest + cached path).
            fetched,
            fc: cfg.fc,
            data_dir: cfg.data_dir,
            // Wired by the binary after start(); None here so the control
            // server falls back to the legacy direct-exit path until the
            // binary calls lifecycle.set_shutdown_tx(tx).
            shutdown_tx: Arc::new(Mutex::new(None)),
        };

        Ok(Self {
            addr,
            active,
            lifecycle,
            _membership: membership,
        })
    }

    /// The address the per-app listener is bound on. Dial this to reach the app.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A cloneable handle to the runner's lifecycle state, for use by the
    /// control server ([`crate::runner::control::serve`]).
    #[must_use]
    pub fn lifecycle(&self) -> RunnerLifecycle {
        self.lifecycle.clone()
    }

    /// The swappable active-runtime cell held by this runner.
    ///
    /// Exposed so the binary can pass it to [`run_until_exit`] for the
    /// re-arming `watch_for_exit` select loop (which needs `load()`/`swapped()`
    /// to distinguish a real crash from a post-swap retirement of the old
    /// runtime). Coerces to `Arc<dyn AppRuntime>` via the `AppRuntime` impl when
    /// a plain runtime handle is needed (e.g. the binary's clean-shutdown path).
    #[must_use]
    pub fn runtime(&self) -> Arc<ActiveRuntime> {
        self.active.clone()
    }
}

/// Build the [`mesh_joiner::JoinConfig`] the runner uses to join the mesh.
///
/// This is the runner's defining mesh contract (per-app-runner arch §0.2/§0.1):
/// - `requested_ula = derive_app_ula(uuid)` — the runner claims its app-ULA so
///   the coordinator routes it straight to this peer (its peer-ULA == app-ULA);
/// - `kind = "runner"` — tags this peer as a per-app runner in the roster;
/// - `parent` — the spawning supervisor's ULA (so the node can build the
///   supervisor → runners topology); `None` for a standalone runner;
/// - `app_uuid` — the app this runner serves.
///
/// Extracted as a pure function so the construction is unit-testable without a
/// live mesh join (which needs a real TUN/root + coordinator — exercised in the
/// Phase-4 Lima e2e test, not here).
///
/// # Panics
/// Never panics; an invalid `uuid` would already have been rejected by
/// [`RunnerServe::start`] before this is called. Here it falls back to the
/// nil UUID's ULA if parsing somehow fails, keeping the function total.
#[must_use]
pub fn build_runner_join_config(cfg: &ServeConfig) -> mesh_joiner::JoinConfig {
    let app_uuid = Uuid::parse_str(&cfg.uuid).unwrap_or(Uuid::nil());
    let app_ula = derive_app_ula(app_uuid);
    mesh_joiner::JoinConfig {
        coordinator_url: cfg.coordinator_url.clone(),
        display_name: cfg.display_name.clone(),
        tags: vec!["runner".to_owned()],
        insecure_no_mtls: true,
        requested_ula: Some(app_ula.to_string()),
        kind: Some("runner".to_owned()),
        parent: cfg.parent.clone(),
        app_uuid: Some(cfg.uuid.clone()),
        ..Default::default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::{IpAddr, Ipv6Addr, SocketAddr};

    use super::*;
    use crate::host::HostBind;

    const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

    fn mesh_cfg() -> ServeConfig {
        ServeConfig {
            uuid: APP_UUID.to_owned(),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/tmp/tabbify-runner-test"),
            no_mesh: false,
            coordinator_url: "http://10.0.0.1:8888".to_owned(),
            display_name: "runner-test".to_owned(),
            parent: Some("fd5a:1f00:0:3::1".to_owned()),
            port: 8730,
            fc: FcConfig::default(),
            docker: DockerConfig::default(),
            image_ref: None,
        }
    }

    /// The runner's mesh join must claim its app-ULA + declare its role,
    /// parent, and app uuid — so the coordinator routes the app-ULA to this
    /// peer and the node can build the supervisor → runners topology.
    #[test]
    fn runner_join_config_claims_app_ula_and_declares_parent() {
        let cfg = mesh_cfg();
        let join = build_runner_join_config(&cfg);

        let expected_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        assert_eq!(
            join.requested_ula.as_deref(),
            Some(expected_ula.to_string().as_str()),
            "runner must request its derived app-ULA"
        );
        assert_eq!(join.kind.as_deref(), Some("runner"), "kind must be runner");
        assert_eq!(
            join.parent.as_deref(),
            Some("fd5a:1f00:0:3::1"),
            "parent supervisor ULA must be forwarded"
        );
        assert_eq!(
            join.app_uuid.as_deref(),
            Some(APP_UUID),
            "app_uuid must be the served app's uuid"
        );
        assert_eq!(join.coordinator_url, "http://10.0.0.1:8888");
        // Runners derive their ULA directly; identity persistence is unused.
        assert!(join.identity_path.is_none());
    }

    /// In mesh mode the runner binds its OWN ULA (== app-ULA) with no separate
    /// app-route layer — `AppHost::mesh_self` selects `[my_ula]:port`.
    #[test]
    fn mesh_self_binds_own_ula_without_app_route() {
        let my_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        let host = AppHost::mesh_self(my_ula, 8730);

        // No app-route layer: `mesh_self` does not carry a MeshHost joiner (the
        // coordinator already routes the runner's own ULA to it).
        assert!(
            !host.is_mesh(),
            "mesh_self must not engage the host_app_ula app-route layer"
        );
        // The selected bind address is the runner's own ULA on the given port.
        assert_eq!(
            host.bind_addr_for(my_ula),
            SocketAddr::new(IpAddr::V6(my_ula), 8730),
            "runner must bind its own ULA, not an ephemeral/loopback addr"
        );
        // Sanity: it really is the app-ULA prefix, distinct from loopback.
        assert_ne!(my_ula, Ipv6Addr::LOCALHOST);
        assert!(matches!(host.bind(), HostBind::OwnUla(8730)));
    }

    // ---- decide_exit / run_until_exit tests --------------------------------

    use bytes::Bytes;
    use http::{Request, Response};
    use tokio::sync::oneshot;

    use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason};

    /// Fake runtime whose `watch_for_exit` resolves immediately to Died.
    struct CrashingRuntime {
        reason: String,
    }

    impl AppRuntime for CrashingRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }

        fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
            let reason = self.reason.clone();
            Box::pin(async move { ExitReason::Died(reason) })
        }
    }

    /// Fake runtime whose `watch_for_exit` never resolves (wasm-like).
    struct StableRuntime;

    impl AppRuntime for StableRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }
        // watch_for_exit uses the default: std::future::pending()
    }

    /// Fake runtime whose `watch_for_exit` resolves only when an external
    /// trigger ([`tokio::sync::Notify`]) is fired — lets a test drive the exact
    /// moment a runtime "dies", deterministically.
    struct WatchableRuntime {
        trigger: Arc<tokio::sync::Notify>,
        reason: String,
    }

    impl WatchableRuntime {
        fn new(reason: &str) -> (Arc<Self>, Arc<tokio::sync::Notify>) {
            let trigger = Arc::new(tokio::sync::Notify::new());
            let rt = Arc::new(Self {
                trigger: trigger.clone(),
                reason: reason.to_owned(),
            });
            (rt, trigger)
        }
    }

    impl AppRuntime for WatchableRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }

        fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
            let trigger = self.trigger.clone();
            let reason = self.reason.clone();
            Box::pin(async move {
                trigger.notified().await;
                ExitReason::Died(reason)
            })
        }
    }

    /// decide_exit: when exit_reason is Some(Died) → Crashed with the reason.
    #[test]
    fn decide_exit_died_returns_crashed() {
        let result = decide_exit(Some(ExitReason::Died("container exited(1)".to_owned())));
        assert_eq!(
            result,
            RunnerExit::Crashed("container exited(1)".to_owned())
        );
    }

    /// decide_exit: when exit_reason is None (shutdown branch won) → CleanShutdown.
    #[test]
    fn decide_exit_none_returns_clean_shutdown() {
        let result = decide_exit(None);
        assert_eq!(result, RunnerExit::CleanShutdown);
    }

    /// run_until_exit: when watch_for_exit resolves first → Crashed.
    #[tokio::test]
    async fn run_until_exit_crash_wins_returns_crashed() {
        let active = Arc::new(ActiveRuntime::new(Arc::new(CrashingRuntime {
            reason: "container tbf-test-0 exited with code 1".to_owned(),
        })));
        let (_tx, rx) = oneshot::channel::<()>();
        // Deliberately drop tx so the channel is open but never sent to —
        // watch_for_exit resolves first.
        let result = run_until_exit(active, rx).await;
        assert_eq!(
            result,
            RunnerExit::Crashed("container tbf-test-0 exited with code 1".to_owned())
        );
    }

    /// run_until_exit: when shutdown_rx fires first → CleanShutdown.
    #[tokio::test]
    async fn run_until_exit_shutdown_wins_returns_clean() {
        let active = Arc::new(ActiveRuntime::new(Arc::new(StableRuntime)));
        let (tx, rx) = oneshot::channel::<()>();
        // Fire the shutdown immediately before awaiting.
        tx.send(()).unwrap();
        let result = run_until_exit(active, rx).await;
        assert_eq!(result, RunnerExit::CleanShutdown);
    }

    // ---- re-arming crash-watch across swaps (P2.3) --------------------------

    /// After a swap, the OLD runtime is drained + shut down, which makes its
    /// `watch_for_exit` resolve. That MUST NOT be treated as a crash: the
    /// retired runtime is no longer the active one. The runner must keep
    /// running (watching the NEW, pending runtime).
    #[tokio::test]
    async fn swap_then_old_death_does_not_exit() {
        let (old, old_trigger) = WatchableRuntime::new("OLD died");
        let active = Arc::new(ActiveRuntime::new(old));
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn({
            let active = active.clone();
            async move { run_until_exit(active, rx).await }
        });

        // Let the loop register its first `swapped()`/`watch_for_exit` select.
        tokio::task::yield_now().await;

        // Swap in a NEW runtime whose `watch_for_exit` pends forever.
        active.swap(Arc::new(StableRuntime));

        // Let the loop observe the swap and re-arm on the NEW runtime.
        tokio::task::yield_now().await;

        // Now fire the OLD runtime's death (post-swap, post-drain). The loop
        // must recognise `current != active.load()` and re-arm, NOT exit.
        old_trigger.notify_waiters();

        // The task must STILL be running after a short grace period.
        let still_running =
            tokio::time::timeout(std::time::Duration::from_millis(150), handle).await;
        assert!(
            still_running.is_err(),
            "run_until_exit must NOT return after a retired runtime dies post-swap"
        );
    }

    /// When the runtime that dies IS the active one, the runner exits Crashed
    /// promptly.
    #[tokio::test]
    async fn active_death_exits() {
        let (only, only_trigger) = WatchableRuntime::new("the only runtime died");
        let active = Arc::new(ActiveRuntime::new(only));
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn({
            let active = active.clone();
            async move { run_until_exit(active, rx).await }
        });

        // Let the loop arm its watch.
        tokio::task::yield_now().await;

        // Fire the ACTIVE runtime's death — there was no swap, so `current` is
        // still the active one and the loop must return Crashed.
        only_trigger.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("run_until_exit must return promptly when the active runtime dies")
            .expect("run_until_exit task must not panic");
        assert_eq!(
            result,
            RunnerExit::Crashed("the only runtime died".to_owned())
        );
    }
}
