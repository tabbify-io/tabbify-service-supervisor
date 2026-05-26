//! App registry + lifecycle state machine (contract §5).
//!
//! Tracks every app this supervisor knows about, its running WASM instance, and
//! its per-app-ULA listener. The *policy* (when to spawn, when to reap) is
//! expressed as small pure functions ([`spawn_on_register`], [`should_reap`]) so
//! it is unit-testable without a clock or a real wasm runtime; [`AppRegistry`]
//! wires those policies to a [`DashMap`] of records + the [`S3Fetcher`] +
//! [`WasmRuntime`] + the [`AppHost`].
//!
//! Lifecycle rules:
//! - `always_on`: hosted as soon as the app is registered/known; never reaped.
//! - `on_request`: hosted on the first reference (API `start` or the first dial
//!   that reaches it through the node); an idle reaper unhosts it after
//!   `idle_timeout_sec` of no requests to its per-app listener, UNLESS pinned.
//! - API `start` pins (sticky — reaper skips it); API `stop` unpins + unhosts.
//!
//! Hosting an app means: compute `app_ula = derive_app_ula(uuid)`, ask the
//! joiner to route it here (mesh mode), and bind a dedicated listener on
//! `[app_ula]:8730` whose whole path goes to the app's [`WasmRuntime`]. See
//! [`crate::host`].

use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::app_ula::derive_app_ula;
use crate::config::{DockerConfig, FcConfig};
use crate::docker::DockerRuntime;
use crate::fetcher::{FetchError, FetchedApp, S3Fetcher};
use crate::firecracker::FirecrackerRuntime;
use crate::host::{AppHost, AppServe, HostedApp};
use crate::manifest::{AppManifest, LifecycleMode};
use crate::runtime::{AppRuntime, WasmRuntime};

/// Externally-visible lifecycle state of an app (contract §5 `state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppState {
    /// Artifact known/fetchable but not running.
    Available,
    /// An instance is live.
    Running,
    /// Explicitly stopped (via API stop).
    Stopped,
}

impl AppState {
    /// Lowercase wire string used in the HTTP API JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AppState::Available => "available",
            AppState::Running => "running",
            AppState::Stopped => "stopped",
        }
    }
}

/// A single known app + its (optional) live per-app-ULA listener.
///
/// `hosted` holds the live listener (which owns the compiled [`WasmRuntime`]
/// and the app-ULA route) iff `state == Running`. The record is NOT `Clone`
/// because `hosted` owns a task handle; the map is mutated in place.
pub struct AppRecord {
    /// App UUID (string form, as used in S3 keys).
    pub uuid: String,
    /// The app's deterministic ULA (`derive_app_ula(uuid)`).
    pub app_ula: Ipv6Addr,
    /// Resolved version.
    pub version: u64,
    /// Manifest (lifecycle, runtime, name, …).
    pub manifest: AppManifest,
    /// Current lifecycle state.
    pub state: AppState,
    /// Sticky pin set by API `start`; reaper never stops a pinned app.
    pub pinned: bool,
    /// Last time a request reached the per-app listener (for the idle reaper).
    pub last_activity: Instant,
    /// Live per-app-ULA listener, present iff `state == Running`.
    pub hosted: Option<HostedApp>,
}

impl AppRecord {
    /// Display name (from the manifest).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.manifest.app.name
    }

    /// Lifecycle mode (from the manifest).
    #[must_use]
    pub fn lifecycle(&self) -> LifecycleMode {
        self.manifest.lifecycle.mode
    }

    /// Idle timeout (from the manifest).
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.manifest.lifecycle.idle_timeout_sec)
    }
}

/// Build an [`AppSummary`] snapshot row from a record.
fn summarize(r: &AppRecord) -> AppSummary {
    AppSummary {
        uuid: r.uuid.clone(),
        app_ula: r.app_ula,
        version: r.version,
        name: r.name().to_owned(),
        lifecycle: r.lifecycle(),
        state: r.state,
        bound_addr: r.hosted.as_ref().map(|h| h.addr),
    }
}

/// Parse `uuid` and derive its app-ULA, or error if malformed (we can't host an
/// app on a deterministic ULA without a valid uuid).
fn require_app_ula(uuid: &str) -> anyhow::Result<Ipv6Addr> {
    let parsed =
        Uuid::parse_str(uuid).map_err(|e| anyhow::anyhow!("invalid app uuid {uuid:?}: {e}"))?;
    Ok(derive_app_ula(parsed))
}

/// PURE policy: should an app be spawned immediately on registration?
///
/// Only `always_on` apps spawn eagerly; `on_request` apps wait for traffic.
#[must_use]
pub const fn spawn_on_register(mode: LifecycleMode) -> bool {
    matches!(mode, LifecycleMode::AlwaysOn)
}

/// PURE policy: should a running app be reaped right now?
///
/// Reap iff it is `on_request`, currently `Running`, NOT pinned, and idle for
/// at least `idle_timeout`. `always_on` and pinned apps are never reaped.
#[must_use]
pub fn should_reap(
    mode: LifecycleMode,
    state: AppState,
    pinned: bool,
    idle: Duration,
    idle_timeout: Duration,
) -> bool {
    matches!(mode, LifecycleMode::OnRequest)
        && state == AppState::Running
        && !pinned
        && idle >= idle_timeout
}

/// Registry of known apps + their running instances + per-app-ULA hosting.
#[derive(Clone)]
pub struct AppRegistry {
    apps: Arc<DashMap<String, AppRecord>>,
    fetcher: S3Fetcher,
    /// Per-app-ULA hosting (mesh-backed or loopback for `--no-mesh`/tests).
    app_host: AppHost,
    /// Firecracker runtime config (only consulted for `firecracker` apps).
    fc_config: FcConfig,
    /// Docker runtime config (only consulted for `docker` apps).
    docker_config: DockerConfig,
    /// Per-uuid spawn lock so concurrent first-requests don't double-host.
    spawn_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

/// A snapshot row for the `GET /v1/apps` listing.
#[derive(Debug, Clone)]
pub struct AppSummary {
    /// App UUID.
    pub uuid: String,
    /// The app's deterministic ULA.
    pub app_ula: Ipv6Addr,
    /// Resolved version.
    pub version: u64,
    /// Display name.
    pub name: String,
    /// Lifecycle mode (wire string `always_on` / `on_request`).
    pub lifecycle: LifecycleMode,
    /// Current state.
    pub state: AppState,
    /// The address the per-app listener is bound on, iff hosted (the app-ULA
    /// in mesh mode; a loopback ephemeral addr in `--no-mesh`/tests).
    pub bound_addr: Option<std::net::SocketAddr>,
}

impl AppRegistry {
    /// Build a registry backed by `fetcher`, hosting apps via `app_host`, with
    /// default firecracker + docker config (those runtimes use the baked-in
    /// defaults). Use [`AppRegistry::with_runtime_configs`] to supply operator
    /// config.
    #[must_use]
    pub fn new(fetcher: S3Fetcher, app_host: AppHost) -> Self {
        Self::with_runtime_configs(
            fetcher,
            app_host,
            FcConfig::default(),
            DockerConfig::default(),
        )
    }

    /// Build a registry with an explicit [`FcConfig`] (docker uses defaults).
    /// Kept for callers that only customize firecracker.
    #[must_use]
    pub fn with_fc_config(fetcher: S3Fetcher, app_host: AppHost, fc_config: FcConfig) -> Self {
        Self::with_runtime_configs(fetcher, app_host, fc_config, DockerConfig::default())
    }

    /// Build a registry with explicit [`FcConfig`] + [`DockerConfig`] (the main
    /// binary passes the parsed CLI config so firecracker apps boot with the
    /// operator's kernel / vcpus / tap subnet / app port, and docker apps build
    /// + run with the operator's docker binary / app port / build timeout).
    #[must_use]
    pub fn with_runtime_configs(
        fetcher: S3Fetcher,
        app_host: AppHost,
        fc_config: FcConfig,
        docker_config: DockerConfig,
    ) -> Self {
        Self {
            apps: Arc::new(DashMap::new()),
            fetcher,
            app_host,
            fc_config,
            docker_config,
            spawn_locks: Arc::new(DashMap::new()),
        }
    }

    /// Snapshot all known apps for the listing endpoint.
    #[must_use]
    pub fn list(&self) -> Vec<AppSummary> {
        self.apps.iter().map(|kv| summarize(kv.value())).collect()
    }

    /// Look up a known app's summary (state-only, no fetch).
    #[must_use]
    pub fn get(&self, uuid: &str) -> Option<AppSummary> {
        self.apps.get(uuid).map(|kv| summarize(kv.value()))
    }

    /// Is the app in the known set already?
    #[must_use]
    pub fn is_known(&self, uuid: &str) -> bool {
        self.apps.contains_key(uuid)
    }

    /// Register an app from S3: fetch metadata + (for `always_on`) host it on
    /// its per-app-ULA right away.
    ///
    /// Idempotent-ish: re-registering refreshes the record to the latest
    /// version. For `always_on` the runtime is compiled, the app-ULA hosted
    /// (listener bound + joiner route), and the state set to `Running`; for
    /// `on_request` the state is `Available` (lazy host on first reference).
    ///
    /// # Errors
    /// Propagates [`FetchError`] from the S3 path, a runtime-compile error, an
    /// invalid uuid, or a hosting failure (listener bind / joiner route).
    pub async fn register(&self, uuid: &str) -> anyhow::Result<AppState> {
        let app_ula = require_app_ula(uuid)?;
        let fetched = self.fetcher.fetch(uuid).await?;
        let mode = fetched.manifest.lifecycle.mode;

        let (state, hosted) = if spawn_on_register(mode) {
            let rt = self.build_runtime(uuid, &fetched).await?;
            let hosted = self.host_app(uuid, app_ula, rt).await?;
            (AppState::Running, Some(hosted))
        } else {
            (AppState::Available, None)
        };

        self.insert_record(uuid, app_ula, &fetched, state, false, hosted);
        Ok(state)
    }

    /// Ensure metadata is known (fetch + register if not), without forcing a
    /// host for `on_request`. Returns the resulting summary. Used by discovery
    /// (`GET /v1/apps/<uuid>`): present iff known OR fetchable from S3.
    ///
    /// # Errors
    /// Propagates [`FetchError`] (notably [`FetchError::NotFound`]).
    pub async fn ensure_known(&self, uuid: &str) -> anyhow::Result<AppSummary> {
        if let Some(s) = self.get(uuid) {
            return Ok(s);
        }
        self.register(uuid).await?;
        self.get(uuid)
            .ok_or_else(|| anyhow::anyhow!("app vanished after register"))
    }

    /// Host (compile + bind the per-app-ULA listener + mark Running) an app,
    /// fetching it first if unknown. Used by `start` and by the lazy-host on
    /// first reference. `pin` makes it sticky (API `start`); the request path
    /// passes `pin = false`.
    ///
    /// # Errors
    /// Propagates fetch / compile / hosting / invalid-uuid errors.
    pub async fn ensure_running(&self, uuid: &str, pin: bool) -> anyhow::Result<AppState> {
        let app_ula = require_app_ula(uuid)?;

        // Serialize per-uuid so two concurrent first-references don't both
        // fetch+compile+bind. We hold a dedicated lock, NOT the DashMap entry,
        // so unrelated apps stay concurrent.
        let lock = self
            .spawn_locks
            .entry(uuid.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check under the lock: another task may have just hosted it.
        if let Some(mut rec) = self.apps.get_mut(uuid) {
            if rec.state == AppState::Running && rec.hosted.is_some() {
                rec.last_activity = Instant::now();
                if pin {
                    rec.pinned = true;
                }
                return Ok(AppState::Running);
            }
        }

        // Need the wasm bytes. Reuse the cached version if the record exists;
        // otherwise fetch latest.
        let fetched = match self.apps.get(uuid).map(|kv| kv.version) {
            Some(version) => self.fetcher.fetch_version(uuid, version).await?,
            None => self.fetcher.fetch(uuid).await?,
        };
        let rt = self.build_runtime(uuid, &fetched).await?;

        // Host BEFORE touching the map: hosting awaits (joiner route + bind), so
        // we must not hold a DashMap guard across it.
        let hosted = self.host_app(uuid, app_ula, rt).await?;
        self.insert_record(
            uuid,
            app_ula,
            &fetched,
            AppState::Running,
            pin,
            Some(hosted),
        );
        Ok(AppState::Running)
    }

    /// Stop + unpin an app: tear down its per-app-ULA listener + unhost the
    /// app-ULA on the joiner; state becomes `Stopped`. No-op (returns
    /// `Stopped`) if the app is unknown.
    pub async fn stop(&self, uuid: &str) -> AppState {
        // Take the listener out under a brief sync borrow, then unhost it
        // (awaits the joiner) WITHOUT holding the DashMap guard.
        let hosted = self.apps.get_mut(uuid).and_then(|mut rec| {
            rec.state = AppState::Stopped;
            rec.pinned = false;
            rec.hosted.take()
        });
        if let Some(hosted) = hosted {
            self.app_host.unhost(hosted).await;
        }
        AppState::Stopped
    }

    /// Purge an app COMPLETELY: stop it (tear down the listener + its
    /// container/VM), reclaim the on-disk artifact cache, remove the built docker
    /// image (docker apps only), and forget the app from the registry. The
    /// disk-reclaiming counterpart to [`Self::stop`], which only frees memory (it
    /// deliberately leaves the cached artifact + the built docker image so a
    /// restart is fast).
    ///
    /// # Errors
    /// Propagates a cache-removal IO error. Docker image removal is best-effort
    /// (logged, never fatal), so a purge still forgets the app + clears the cache
    /// even if the Docker daemon is unreachable.
    pub async fn purge(&self, uuid: &str) -> anyhow::Result<()> {
        // Capture the runtime type before we forget the record — it drives the
        // docker image cleanup below.
        let runtime_type = self
            .apps
            .get(uuid)
            .map(|r| r.manifest.runtime.r#type.clone());

        // Stop: unhost the listener + drop the runtime (container/VM torn down).
        self.stop(uuid).await;

        // Docker apps: also remove the built image (stop/Drop removed only the
        // container, leaving the image on disk).
        if runtime_type.as_deref() == Some("docker") {
            crate::docker::purge_image(&self.docker_config.docker_bin, uuid).await;
        }

        // Reclaim the on-disk artifact cache (manifest + rootfs.ext4 / app.wasm /
        // context.tar.gz).
        self.fetcher.purge_cache(uuid).await?;

        // Forget the app entirely.
        self.apps.remove(uuid);
        self.spawn_locks.remove(uuid);
        Ok(())
    }

    /// Bump `last_activity` for an app (called from its per-app listener so the
    /// idle reaper sees live traffic).
    pub fn touch(&self, uuid: &str) {
        if let Some(mut rec) = self.apps.get_mut(uuid) {
            rec.last_activity = Instant::now();
        }
    }

    /// Run one reaping pass: unhost every app that [`should_reap`] flags as
    /// idle. Returns the uuids that were reaped (handy for logging + tests).
    pub async fn reap_idle(&self) -> Vec<String> {
        let now = Instant::now();
        // First pass (sync): flip state + take the listeners out of the records
        // so we never await while holding a DashMap guard.
        let mut to_unhost: Vec<(String, HostedApp)> = Vec::new();
        for mut kv in self.apps.iter_mut() {
            let rec = kv.value_mut();
            let idle = now.saturating_duration_since(rec.last_activity);
            if should_reap(
                rec.lifecycle(),
                rec.state,
                rec.pinned,
                idle,
                rec.idle_timeout(),
            ) {
                rec.state = AppState::Stopped;
                if let Some(hosted) = rec.hosted.take() {
                    to_unhost.push((rec.uuid.clone(), hosted));
                }
            }
        }
        // Second pass (async): unhost each reaped app's listener + app-ULA.
        let mut reaped = Vec::with_capacity(to_unhost.len());
        for (uuid, hosted) in to_unhost {
            self.app_host.unhost(hosted).await;
            reaped.push(uuid);
        }
        reaped
    }

    /// Build the [`AppRuntime`] for a fetched app from `manifest.runtime.type`:
    /// `wasm-http` → the in-process [`WasmRuntime`]; `firecracker` → a booted
    /// [`FirecrackerRuntime`] microVM (the launch returns a clear `Err` if this
    /// host lacks `/dev/kvm` or isn't Linux, so a WASM-only supervisor refuses
    /// firecracker apps loudly without affecting WASM apps); `docker` → a built
    /// + run [`DockerRuntime`] container (the launch returns a clear `Err` if no
    ///   Docker daemon is reachable); anything else is a hard error.
    ///
    /// `uuid` makes the docker image tag + container name deterministic.
    ///
    /// # Errors
    /// A wasm compile failure, a firecracker launch failure (no KVM / non-Linux
    /// / boot failure), a docker launch failure (no daemon / build / run
    /// failure), or an unknown runtime type.
    async fn build_runtime(
        &self,
        uuid: &str,
        fetched: &FetchedApp,
    ) -> anyhow::Result<Arc<dyn AppRuntime>> {
        let rt = &fetched.manifest.runtime;
        match rt.r#type.as_str() {
            "wasm-http" => {
                let wasm = WasmRuntime::load_with_fuel(&fetched.wasm, rt.fuel_per_request)?;
                Ok(Arc::new(wasm))
            }
            "firecracker" => {
                let vm =
                    FirecrackerRuntime::launch(&fetched.cached_path, rt, &self.fc_config).await?;
                Ok(Arc::new(vm))
            }
            "docker" => {
                let container = DockerRuntime::launch_with_id(
                    &fetched.cached_path,
                    rt,
                    &self.docker_config,
                    uuid,
                )
                .await?;
                Ok(Arc::new(container))
            }
            other => anyhow::bail!("unknown runtime type: {other}"),
        }
    }

    /// Host an app on its per-app-ULA: build the per-app serve state (runtime +
    /// activity callback into this registry) and delegate to [`AppHost::host`].
    ///
    /// The activity callback captures a registry clone, so while an app is
    /// hosted there is a strong reference cycle (registry → record → listener
    /// task → callback → registry). It is intentional and bounded: unhosting
    /// (stop / idle-reap / drop) aborts the listener task and `take`s the
    /// record's handle, dropping the callback and breaking the cycle.
    async fn host_app(
        &self,
        uuid: &str,
        app_ula: Ipv6Addr,
        rt: Arc<dyn AppRuntime>,
    ) -> anyhow::Result<HostedApp> {
        let registry = self.clone();
        let uuid_owned = uuid.to_owned();
        let on_request: Arc<dyn Fn() + Send + Sync> = Arc::new(move || registry.touch(&uuid_owned));
        self.app_host
            .host(app_ula, AppServe::new(rt, on_request))
            .await
    }

    /// Insert/replace a record after a (possibly hosting) lifecycle transition.
    fn insert_record(
        &self,
        uuid: &str,
        app_ula: Ipv6Addr,
        fetched: &crate::fetcher::FetchedApp,
        state: AppState,
        pin: bool,
        hosted: Option<HostedApp>,
    ) {
        match self.apps.get_mut(uuid) {
            Some(mut rec) => {
                rec.version = fetched.version;
                rec.manifest = fetched.manifest.clone();
                rec.state = state;
                rec.hosted = hosted;
                rec.last_activity = Instant::now();
                if pin {
                    rec.pinned = true;
                }
            }
            None => {
                self.apps.insert(
                    uuid.to_owned(),
                    AppRecord {
                        uuid: uuid.to_owned(),
                        app_ula,
                        version: fetched.version,
                        manifest: fetched.manifest.clone(),
                        state,
                        pinned: pin,
                        last_activity: Instant::now(),
                        hosted,
                    },
                );
            }
        }
    }

    /// Fetcher handle (used by the discovery path to probe S3 for unknown apps).
    #[must_use]
    pub fn fetcher(&self) -> &S3Fetcher {
        &self.fetcher
    }

    /// Probe whether an app is fetchable from S3 (discovery for unknown uuids).
    ///
    /// # Errors
    /// Surfaces [`FetchError::NotFound`] as `Ok(false)`; other errors bubble up.
    pub async fn is_fetchable(&self, uuid: &str) -> Result<bool, FetchError> {
        match self.fetcher.latest_version(uuid).await {
            Ok(_) => Ok(true),
            Err(FetchError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// `always_on` spawns immediately; `on_request` waits.
    #[test]
    fn spawn_policy() {
        assert!(spawn_on_register(LifecycleMode::AlwaysOn));
        assert!(!spawn_on_register(LifecycleMode::OnRequest));
    }

    /// Reaper stops only idle, unpinned, running, `on_request` apps.
    #[test]
    fn reap_policy_matrix() {
        let t = Duration::from_secs(300);
        let idle = Duration::from_secs(301);
        let fresh = Duration::from_secs(1);

        // on_request, running, unpinned, idle -> reap.
        assert!(should_reap(
            LifecycleMode::OnRequest,
            AppState::Running,
            false,
            idle,
            t
        ));
        // not yet idle -> keep.
        assert!(!should_reap(
            LifecycleMode::OnRequest,
            AppState::Running,
            false,
            fresh,
            t
        ));
        // pinned -> keep even when idle (API overrides on_request).
        assert!(!should_reap(
            LifecycleMode::OnRequest,
            AppState::Running,
            true,
            idle,
            t
        ));
        // always_on -> never reap.
        assert!(!should_reap(
            LifecycleMode::AlwaysOn,
            AppState::Running,
            false,
            idle,
            t
        ));
        // not running -> nothing to reap.
        assert!(!should_reap(
            LifecycleMode::OnRequest,
            AppState::Available,
            false,
            idle,
            t
        ));
        // exactly at the threshold -> reap (>=).
        assert!(should_reap(
            LifecycleMode::OnRequest,
            AppState::Running,
            false,
            t,
            t
        ));
    }

    #[test]
    fn state_wire_strings() {
        assert_eq!(AppState::Available.as_str(), "available");
        assert_eq!(AppState::Running.as_str(), "running");
        assert_eq!(AppState::Stopped.as_str(), "stopped");
    }
}
