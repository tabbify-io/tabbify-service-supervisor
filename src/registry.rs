//! App registry + lifecycle state machine (contract §5).
//!
//! Tracks every app this supervisor knows about and the running WASM instances.
//! The *policy* (when to spawn, when to reap) is expressed as small pure
//! functions ([`spawn_on_register`], [`should_reap`]) so it is unit-testable
//! without a clock or a real wasm runtime; [`AppRegistry`] wires those policies
//! to a [`DashMap`] of records + the [`S3Fetcher`] + [`WasmRuntime`].
//!
//! Lifecycle rules:
//! - `always_on`: spawn as soon as the app is registered/known; never reaped.
//! - `on_request`: spawn on the first `/apps/<uuid>/…` request; an idle reaper
//!   stops it after `idle_timeout_sec` of no requests, UNLESS pinned.
//! - API `start` pins (sticky — reaper skips it); API `stop` unpins.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::fetcher::{FetchError, S3Fetcher};
use crate::manifest::{AppManifest, LifecycleMode};
use crate::runtime::WasmRuntime;

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

/// A single known app + its (optional) live instance.
///
/// `runtime` is kept behind an [`Arc`] so a request handler can clone it out
/// and run the wasm without holding the registry map lock for the request
/// duration. `instance_lock` serializes spawn so two concurrent first-requests
/// don't both compile the component.
#[derive(Clone)]
pub struct AppRecord {
    /// App UUID (string form, as used in URLs / S3 keys).
    pub uuid: String,
    /// Resolved version.
    pub version: u64,
    /// Manifest (lifecycle, runtime, name, …).
    pub manifest: AppManifest,
    /// Current lifecycle state.
    pub state: AppState,
    /// Sticky pin set by API `start`; reaper never stops a pinned app.
    pub pinned: bool,
    /// Last time a request was served (for the idle reaper).
    pub last_activity: Instant,
    /// Live runtime, present iff `state == Running`.
    pub runtime: Option<WasmRuntime>,
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

/// Registry of known apps + their running instances.
#[derive(Clone)]
pub struct AppRegistry {
    apps: Arc<DashMap<String, AppRecord>>,
    fetcher: S3Fetcher,
    /// Per-uuid spawn lock so concurrent first-requests don't double-compile.
    spawn_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

/// A snapshot row for the `GET /v1/apps` listing.
#[derive(Debug, Clone)]
pub struct AppSummary {
    /// App UUID.
    pub uuid: String,
    /// Resolved version.
    pub version: u64,
    /// Display name.
    pub name: String,
    /// Lifecycle mode (wire string `always_on` / `on_request`).
    pub lifecycle: LifecycleMode,
    /// Current state.
    pub state: AppState,
}

impl AppRegistry {
    /// Build a registry backed by `fetcher`.
    #[must_use]
    pub fn new(fetcher: S3Fetcher) -> Self {
        Self {
            apps: Arc::new(DashMap::new()),
            fetcher,
            spawn_locks: Arc::new(DashMap::new()),
        }
    }

    /// Snapshot all known apps for the listing endpoint.
    #[must_use]
    pub fn list(&self) -> Vec<AppSummary> {
        self.apps
            .iter()
            .map(|kv| {
                let r = kv.value();
                AppSummary {
                    uuid: r.uuid.clone(),
                    version: r.version,
                    name: r.name().to_owned(),
                    lifecycle: r.lifecycle(),
                    state: r.state,
                }
            })
            .collect()
    }

    /// Look up a known app's summary (state-only, no fetch).
    #[must_use]
    pub fn get(&self, uuid: &str) -> Option<AppSummary> {
        self.apps.get(uuid).map(|kv| {
            let r = kv.value();
            AppSummary {
                uuid: r.uuid.clone(),
                version: r.version,
                name: r.name().to_owned(),
                lifecycle: r.lifecycle(),
                state: r.state,
            }
        })
    }

    /// Is the app in the known set already?
    #[must_use]
    pub fn is_known(&self, uuid: &str) -> bool {
        self.apps.contains_key(uuid)
    }

    /// Register an app from S3: fetch metadata + (for `always_on`) spawn.
    ///
    /// Idempotent-ish: re-registering refreshes the record to the latest
    /// version. For `always_on` the runtime is compiled and the state set to
    /// `Running`; for `on_request` the state is `Available` (lazy spawn).
    ///
    /// # Errors
    /// Propagates [`FetchError`] from the S3 path, or a runtime-compile error
    /// (as [`FetchError::Manifest`] is unsuitable, compile errors surface as a
    /// generic transport-style string via [`anyhow`]).
    pub async fn register(&self, uuid: &str) -> anyhow::Result<AppState> {
        let fetched = self.fetcher.fetch(uuid).await?;
        let mode = fetched.manifest.lifecycle.mode;

        let (state, runtime) = if spawn_on_register(mode) {
            let rt = WasmRuntime::load_with_fuel(
                &fetched.wasm,
                fetched.manifest.runtime.fuel_per_request,
            )?;
            (AppState::Running, Some(rt))
        } else {
            (AppState::Available, None)
        };

        self.apps.insert(
            uuid.to_owned(),
            AppRecord {
                uuid: uuid.to_owned(),
                version: fetched.version,
                manifest: fetched.manifest,
                state,
                pinned: false,
                last_activity: Instant::now(),
                runtime,
            },
        );
        Ok(state)
    }

    /// Ensure metadata is known (fetch + register if not), without forcing a
    /// spawn for `on_request`. Returns the resulting summary. Used by discovery
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

    /// Spawn (compile + mark Running) an app, fetching it first if unknown.
    /// Used by `start` and by the lazy-spawn-on-request path. `pin` makes it
    /// sticky (API `start`); the request path passes `pin = false`.
    ///
    /// # Errors
    /// Propagates fetch / compile errors.
    pub async fn ensure_running(&self, uuid: &str, pin: bool) -> anyhow::Result<AppState> {
        // Serialize per-uuid so two concurrent first-requests don't both
        // fetch+compile. We hold a dedicated lock, NOT the DashMap entry, so
        // unrelated apps stay concurrent.
        let lock = self
            .spawn_locks
            .entry(uuid.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check under the lock: another task may have just spawned it.
        if let Some(mut rec) = self.apps.get_mut(uuid) {
            if rec.state == AppState::Running && rec.runtime.is_some() {
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
        let rt =
            WasmRuntime::load_with_fuel(&fetched.wasm, fetched.manifest.runtime.fuel_per_request)?;

        match self.apps.get_mut(uuid) {
            Some(mut rec) => {
                rec.version = fetched.version;
                rec.manifest = fetched.manifest;
                rec.state = AppState::Running;
                rec.runtime = Some(rt);
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
                        version: fetched.version,
                        manifest: fetched.manifest,
                        state: AppState::Running,
                        pinned: pin,
                        last_activity: Instant::now(),
                        runtime: Some(rt),
                    },
                );
            }
        }
        Ok(AppState::Running)
    }

    /// Stop + unpin an app. Drops its runtime; state becomes `Stopped`.
    /// No-op (returns `Stopped`) if the app is unknown.
    pub fn stop(&self, uuid: &str) -> AppState {
        if let Some(mut rec) = self.apps.get_mut(uuid) {
            rec.state = AppState::Stopped;
            rec.pinned = false;
            rec.runtime = None;
        }
        AppState::Stopped
    }

    /// Clone out the live runtime for a request, bumping `last_activity`.
    /// Returns `None` if the app isn't currently running.
    #[must_use]
    pub fn take_runtime_for_request(&self, uuid: &str) -> Option<WasmRuntime> {
        let mut rec = self.apps.get_mut(uuid)?;
        if rec.state == AppState::Running {
            rec.last_activity = Instant::now();
            rec.runtime.clone()
        } else {
            None
        }
    }

    /// Run one reaping pass: stop every app that [`should_reap`] flags as idle.
    /// Returns the uuids that were reaped (handy for logging + tests).
    pub fn reap_idle(&self) -> Vec<String> {
        let now = Instant::now();
        let mut reaped = Vec::new();
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
                rec.runtime = None;
                reaped.push(rec.uuid.clone());
            }
        }
        reaped
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
