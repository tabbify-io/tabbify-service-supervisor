//! Supervisor orchestrator — spawns, monitors, and re-adopts per-app runner
//! processes.
//!
//! # State
//! [`Orchestrator`] is the long-lived owner of the per-app runner fleet. It
//! holds the [`SharedRunnerConfig`] — the supervisord-level settings every
//! runner on this host has in common — plus the directory where per-runner
//! [`RunnerHandle`] records live. Because all runners share that config, a
//! single [`RunnerHandle`] record (which stores only `uuid` / `pid` /
//! `control_sock` / `app_ula` / `parent`) is enough to reconstruct a runner's
//! full [`SpawnSpec`] for a respawn: [`SharedRunnerConfig::spawn_spec_for`].
//!
//! This one struct is the home for the whole Phase 2 lifecycle — spawn (2.2),
//! monitor + restart (2.4, [`monitor`]), re-adopt on restart (2.5), and the API
//! rewire (2.6).
//!
//! # Phase 2 tasks
//! - Task 2.1 [`handle`] — [`RunnerHandle`] bookkeeping type + on-disk record.
//! - Task 2.2 [`spawn`] — spawn a detached runner process + persist its record.
//! - Task 2.3 [`client`] — control-socket client.
//! - Task 2.4 [`monitor`] — health-monitor loop + restart dead runners.
//! - Task 2.5 — re-adopt runners on supervisor restart.
//! - Task 2.6 — API rewire.

pub mod api;
pub mod build;
pub mod client;
pub mod handle;
pub mod monitor;
pub mod restart;
pub mod spawn;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

pub use api::{AppState, AppSummary};
pub use client::ControlClient;
pub use handle::RunnerHandle;
use monitor::RecordOutcome;
pub use spawn::{SpawnSpec, spawn_runner};

/// What a startup [`readopt`](Orchestrator::readopt) did to the recorded fleet.
///
/// Each runner found on disk lands in exactly one bucket: it was either
/// **adopted** (still alive — left running, pid unchanged) or **respawned**
/// (dead — a fresh process was started). Returned for logging and for tests to
/// assert the headline guarantee that a living runner is adopted, not killed and
/// recreated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadoptSummary {
    /// UUIDs of living runners that were adopted untouched.
    pub adopted: Vec<String>,
    /// UUIDs of dead runners that were respawned.
    pub respawned: Vec<String>,
}

/// supervisord-level configuration shared by EVERY runner this orchestrator
/// manages.
///
/// A [`RunnerHandle`] record persists only the per-runner bits (`uuid`,
/// `control_sock`, `parent`, …); these fields are identical across all runners
/// on one supervisor, so they live here once instead of being duplicated into
/// every record. Together with a record they reconstruct a runner's full
/// [`SpawnSpec`] (see [`spawn_spec_for`](Self::spawn_spec_for)).
#[derive(Debug, Clone)]
pub struct SharedRunnerConfig {
    /// Path to the `tabbify-runner` binary every runner execs.
    pub runner_bin: PathBuf,
    /// S3 base URL for anonymous artifact fetch.
    pub s3_base_url: String,
    /// Local data dir runners cache artifacts under.
    pub data_dir: PathBuf,
    /// This supervisor's own mesh ULA, forwarded to NEWLY-spawned runners as
    /// their `--parent` so the node can build the supervisor → runners topology.
    /// `None` in `--no-mesh` mode (and acceptable as a Phase-4 follow-up to wire
    /// the supervisor's real ULA here once it joins the mesh as a peer).
    ///
    /// NOTE: a *respawn* reuses the parent stored in the runner's own
    /// [`RunnerHandle`] record (see [`Self::spawn_spec_for`]); this field only
    /// seeds the parent for a brand-new runner started via the control API.
    pub parent: Option<String>,
    /// Skip mesh join; bind plain loopback. Used for local runs / tests without
    /// root + TUN.
    pub no_mesh: bool,
    /// Explicit DERP-style mesh relay endpoint (`TABBIFY_MESH_RELAY_URL`),
    /// forwarded to every spawned runner as `--mesh-relay-url <url>` so the
    /// runner's OWN mesh join routes its relay over the same `wss://` endpoint
    /// as the supervisor (the corporate-firewall escape hatch). `None` (the
    /// default) lets each runner derive the relay from the coordinator URL.
    pub relay_url: Option<String>,
    /// Relay-only declaration (`TABBIFY_MESH_RELAY_ONLY`), forwarded to every
    /// spawned runner as the bare `--mesh-relay-only` flag when `true` so the
    /// runner's OWN mesh join tells the coordinator it has no reachable direct
    /// endpoint (it shares the host's NAT/firewall with the supervisor). `false`
    /// (the default) keeps the runner's direct + hole-punch traversal.
    pub relay_only: bool,
}

impl SharedRunnerConfig {
    /// Reconstruct the full [`SpawnSpec`] for `record` by combining this shared
    /// config with the per-runner fields the record carries (`uuid`,
    /// `control_sock`, `parent`).
    ///
    /// The derived `app_ula` is NOT part of [`SpawnSpec`] — the runner re-derives
    /// it from the `uuid`, so a respawn deterministically lands on the same ULA.
    #[must_use]
    pub fn spawn_spec_for(&self, record: &RunnerHandle) -> SpawnSpec {
        SpawnSpec {
            runner_bin: self.runner_bin.clone(),
            uuid: record.uuid.clone(),
            control_sock: record.control_sock.clone(),
            s3_base_url: self.s3_base_url.clone(),
            data_dir: self.data_dir.clone(),
            parent: record.parent.clone(),
            no_mesh: self.no_mesh,
            // Forward the supervisor's relay endpoint so a respawned runner
            // routes its relay over the same `wss://` url (corporate firewall).
            relay_url: self.relay_url.clone(),
            // Forward the relay-only declaration so a respawned runner also
            // declares no reachable direct endpoint (handshake over the relay).
            relay_only: self.relay_only,
            // Respawn on the same deployed version the record was last at.
            image_ref: record.image_ref.clone(),
            // Reuse the PERSISTED managed `tabbify.toml` so a RESPAWN-from-record
            // re-derives the connect-repo app's `[runtime]`/`[routes]` instead of
            // reverting to the hardcoded FC defaults after a crash. `None` for an
            // app with no managed config (a `tcli`/S3-manifest app).
            manifest_toml: record.manifest_toml.clone(),
            // Phase-2: a RESPAWN rejoins the SAME tenant network the record was
            // scoped to (`--network <slug>`).
            network: record.network.clone(),
            // Reuse the runner's PERSISTED join token so a respawn re-joins the
            // validating coordinator with the SAME token instead of 401ing. The
            // token is long-lived (a 1-year TTL minted by the node), so it
            // outlives the runner's idle-outs/crashes for the app's whole life.
            // `None` for an unscoped runner (no tenant network).
            runner_join_token: record.runner_join_token.clone(),
            // Reuse the PERSISTED deploy-time extra env so a RESPAWN-from-record
            // re-bakes the same KEY=VALUE entries into the guest `/init` (devbox
            // SSH key, dev-session git vars, etc.). `None` for an app with no
            // deploy-time env (normal deploys without extra env).
            extra_env: record.extra_env.clone(),
            // Reuse the PERSISTED egress allow-list so a RESPAWN-from-record
            // re-applies the same host-side egress posture (deny-by-default +
            // allowed hosts). `None` for an app with no allow-list (unrestricted).
            egress_allow: record.egress_allow.clone(),
            // The runtime is no longer selectable — every app builds as
            // Firecracker, and a by-ref deploy synthesizes a firecracker manifest
            // — so the record's (now inert) `requested_runtime` is NOT read here.
        }
    }
}

/// How often the background monitor loop probes the runner fleet.
const MONITOR_INTERVAL: Duration = Duration::from_secs(5);

/// Long-lived owner of the per-app runner fleet on one supervisor.
///
/// Construct once at startup with the shared config + the runner-record
/// directory; clone freely (both fields are cheap to clone) to hand to the
/// background monitor task and the API layer.
#[derive(Debug, Clone)]
pub struct Orchestrator {
    /// Settings shared by every runner this orchestrator manages.
    shared: SharedRunnerConfig,
    /// Directory holding one `<uuid>.json` [`RunnerHandle`] record per runner.
    runner_dir: PathBuf,
    /// UUIDs with a deploy currently in flight. Per-UUID lifecycle locks protect
    /// reconciliation; this set separately tells the Linux orphan-FC sweep that
    /// the shared build VM may be active and must be protected. `deploy_app`
    /// inserts before dispatch and an RAII [`DeployGuard`] removes on every exit.
    deploying: Arc<Mutex<HashSet<String>>>,
    /// Per-uuid async locks that serialize every lifecycle mutation and monitor
    /// reconciliation for the same app.
    ///
    /// Start, stop, purge, reset, deploy, and the monitor all read and write the
    /// same durable record. Holding one lock across each complete operation
    /// prevents a stale monitor snapshot from undoing an operator action and
    /// prevents two lifecycle paths from spawning duplicate runners. Different
    /// apps still proceed independently.
    ///
    /// A `tokio::sync::Mutex` (not `std`) because the guard is held ACROSS awaits
    /// — the entire lifecycle operation, including control round-trips and any
    /// cold spawn + health wait. The OUTER `std::sync::Mutex` only guards the
    /// map's get-or-insert and is never held across an await. Shared across
    /// clones via `Arc` (same as `deploying`) so every caller contends on the
    /// same per-uuid lock. Entries are never removed — an empty mutex per app
    /// ever seen is negligible and removal would race a waiting operation.
    lifecycle_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Per-uuid image-pull progress observed by the monitor (bytes landed in the
    /// runner's `.pull` layout dir + when they last GREW). Drives the
    /// PROGRESS-BASED reap deferral in [`monitor`]: a pull that keeps making
    /// byte-progress is deferred indefinitely (a 400+ MB base image over a
    /// few-Mbit home link legitimately needs 20+ min — killing it mid-flight
    /// forces a from-scratch re-pull that can NEVER converge, the livelock that
    /// took MSI's control plane down), while a pull with ZERO progress for a
    /// full stall window is genuinely wedged and reaped. Same sharing model as
    /// `deploying` (std Mutex, never held across an await); entries are cleared
    /// when the monitor observes the pull gone.
    pull_progress: Arc<Mutex<HashMap<String, monitor::PullProgress>>>,
}

/// RAII guard that removes a uuid from the orchestrator's in-flight-deploy set
/// when dropped. Guarantees the uuid is cleared on EVERY exit path of
/// [`Orchestrator::deploy_app`] (early `?` returns, panics, the happy path) so a
/// failed deploy can never leave the orphan sweep permanently shielding the
/// shared build VM.
pub(crate) struct DeployGuard {
    deploying: Arc<Mutex<HashSet<String>>>,
    uuid: String,
}

impl Drop for DeployGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.deploying.lock() {
            set.remove(&self.uuid);
        }
    }
}

impl Orchestrator {
    /// Create an orchestrator over `shared` config, reading/writing runner
    /// records under `runner_dir`.
    #[must_use]
    pub fn new(shared: SharedRunnerConfig, runner_dir: PathBuf) -> Self {
        Self {
            shared,
            runner_dir,
            deploying: Arc::new(Mutex::new(HashSet::new())),
            lifecycle_locks: Arc::new(Mutex::new(HashMap::new())),
            pull_progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The per-uuid async lock that serializes lifecycle mutation and monitor
    /// reconciliation for `uuid`, created on first use. API mutations await it;
    /// the monitor only try-locks it so one busy app never blocks a whole tick.
    /// The outer
    /// `std::sync::Mutex` is only held for the map lookup/insert here, never
    /// across the await.
    pub(crate) fn lifecycle_lock_for(&self, uuid: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self
            .lifecycle_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            map.entry(uuid.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Mark `uuid` as deploy-in-flight and return an RAII [`DeployGuard`] that
    /// clears it on drop. The Linux orphan sweep uses this as a conservative
    /// signal that the record-less build VM may be active.
    pub(crate) fn begin_deploy(&self, uuid: &str) -> DeployGuard {
        if let Ok(mut set) = self.deploying.lock() {
            set.insert(uuid.to_owned());
        }
        DeployGuard {
            deploying: Arc::clone(&self.deploying),
            uuid: uuid.to_owned(),
        }
    }

    /// The shared runner config.
    #[must_use]
    pub fn shared(&self) -> &SharedRunnerConfig {
        &self.shared
    }

    /// The directory holding this orchestrator's runner records.
    #[must_use]
    pub fn runner_dir(&self) -> &std::path::Path {
        &self.runner_dir
    }

    /// Load a runner record and verify that its immutable identity matches the
    /// filename/requested app. Mutable launch fields remain record-owned.
    pub(crate) fn load_runner_record(&self, uuid: &str) -> anyhow::Result<Option<RunnerHandle>> {
        let Some(record) =
            RunnerHandle::load(&self.runner_dir, uuid).map_err(|error| anyhow::anyhow!(error))?
        else {
            return Ok(None);
        };
        let expected_sock = self.runner_dir.join(format!("{uuid}.sock"));
        let parsed = uuid::Uuid::parse_str(uuid)
            .map_err(|error| anyhow::anyhow!("invalid app uuid {uuid:?}: {error}"))?;
        let expected_ula = crate::app_ula::derive_app_ula(parsed).to_string();
        if record.uuid != uuid
            || record.control_sock != expected_sock
            || record.app_ula != expected_ula
        {
            anyhow::bail!(
                "runner record identity mismatch for {uuid}: record_uuid={:?}, control_sock={}, app_ula={}",
                record.uuid,
                record.control_sock.display(),
                record.app_ula
            );
        }
        Ok(Some(record))
    }

    /// Re-adopt the recorded fleet on supervisor startup.
    ///
    /// This is what makes the headline crash-survival property work: when the
    /// supervisor is SIGKILLed its detached runners keep running, and a freshly
    /// started supervisor — which never spawned them and holds no in-memory
    /// table — must RE-DISCOVER them from the on-disk records and ADOPT the
    /// living ones rather than respawn them.
    ///
    /// Mechanically it is one reconcile pass: every recorded runner is checked
    /// via [`reconcile_record`](Self::reconcile_record) (the SAME decision the
    /// periodic [`tick`](Self::tick) makes), so a living runner is left running
    /// untouched (its pid is NOT disturbed) and only a dead one is respawned.
    /// The difference from `tick` is purely that this is the explicit startup
    /// entry point and it returns + logs a [`ReadoptSummary`] of what it adopted
    /// versus respawned.
    ///
    /// # Errors
    /// Returns an [`anyhow::Error`] only if the runner directory cannot be
    /// listed. Per-runner respawn failures are logged and counted as neither
    /// adopted nor respawned (the next [`tick`] retries), never propagated.
    pub async fn readopt(&self) -> anyhow::Result<ReadoptSummary> {
        let records = RunnerHandle::list(&self.runner_dir)?;
        let mut summary = ReadoptSummary::default();

        for record in records {
            match self.reconcile_record(&record).await {
                RecordOutcome::Adopted => summary.adopted.push(record.uuid),
                RecordOutcome::Respawned => summary.respawned.push(record.uuid),
                // A failed respawn, a backoff-gated skip, or a crash-looped
                // (parked) runner are left out of both buckets. The monitor
                // will not retry a parked runner until it is re-deployed; other
                // failures are retried on the next tick.
                RecordOutcome::RespawnFailed
                | RecordOutcome::Backoff
                | RecordOutcome::CrashLooped
                | RecordOutcome::Busy
                | RecordOutcome::Missing => {}
            }
        }

        tracing::info!(
            adopted = summary.adopted.len(),
            respawned = summary.respawned.len(),
            adopted_uuids = ?summary.adopted,
            respawned_uuids = ?summary.respawned,
            "re-adopted runner fleet on startup"
        );

        // F2.2 (audit #93): on startup — especially after a supervisor crash-loop
        // during which NOTHING reaped FCs left by prior runner deaths — sweep for
        // record-less firecracker orphans (incl. the build VM) that reparented to
        // PID 1 and would otherwise spin/hold RAM until reboot. Runs AFTER the
        // re-adopt so every surviving runner's FC is in the live-socket safe-set.
        #[cfg(target_os = "linux")]
        {
            let reaped = self.sweep_orphan_fcs();
            if reaped > 0 {
                tracing::warn!(
                    reaped,
                    "startup orphan-FC sweep reaped record-less FC orphans"
                );
            }
        }

        Ok(summary)
    }

    /// Run the periodic monitor loop forever: re-adopt the existing fleet once
    /// at startup (Task 2.5), then every [`MONITOR_INTERVAL`] run one
    /// [`tick`](Self::tick) (probe + respawn dead runners).
    ///
    /// Mirrors the idle-reaper loop in `main.rs`: a [`tokio::time::interval`]
    /// drives one pass per tick. Spawn this on a background task at startup.
    /// A failure to enumerate records in a single pass is logged and the loop
    /// continues — a transient FS error must not silently kill self-healing.
    pub async fn run_monitor(self) {
        // Re-adopt living runners (and respawn any already-dead ones) before the
        // steady-state loop, so a restarted supervisor reclaims its fleet
        // immediately instead of waiting a full interval.
        if let Err(e) = self.readopt().await {
            tracing::error!(error = %e, "startup re-adopt failed (continuing into monitor loop)");
        }

        let mut ticker = tokio::time::interval(MONITOR_INTERVAL);
        loop {
            ticker.tick().await;
            match self.tick().await {
                Ok(respawned) if !respawned.is_empty() => {
                    tracing::info!(?respawned, "monitor respawned dead runners");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = %e, "monitor tick failed (continuing)");
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn is_deploying(orch: &Orchestrator, uuid: &str) -> bool {
        orch.deploying
            .lock()
            .map(|set| set.contains(uuid))
            .unwrap_or(false)
    }

    fn shared() -> SharedRunnerConfig {
        SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        }
    }

    fn record() -> RunnerHandle {
        RunnerHandle {
            uuid: "0191e7c2-1111-7222-8333-444455556666".to_owned(),
            pid: 4242,
            control_sock: PathBuf::from("/run/tabbify/runners/x.sock"),
            app_ula: "fd5a:1f02:44a5:240b:121a::1".to_owned(),
            parent: Some("fd5a:1f00:1::1".to_owned()),
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            stopped: false,
        }
    }

    /// `spawn_spec_for` reconstructs a faithful spec: shared fields from the
    /// config, per-runner fields (uuid / control_sock / parent) from the record.
    #[test]
    fn spawn_spec_for_combines_shared_and_record() {
        let cfg = shared();
        let rec = record();
        let spec = cfg.spawn_spec_for(&rec);

        // From shared config.
        assert_eq!(spec.runner_bin, cfg.runner_bin);
        assert_eq!(spec.s3_base_url, cfg.s3_base_url);
        assert_eq!(spec.data_dir, cfg.data_dir);
        assert_eq!(spec.no_mesh, cfg.no_mesh);
        assert_eq!(spec.relay_url, cfg.relay_url);
        assert_eq!(spec.relay_only, cfg.relay_only);

        // From the record.
        assert_eq!(spec.uuid, rec.uuid);
        assert_eq!(spec.control_sock, rec.control_sock);
        assert_eq!(spec.parent, rec.parent);
    }

    /// A respawn carries the record's tenant `network` forward AND reuses the
    /// record's PERSISTED join token, so the runner re-joins the validating
    /// coordinator with the SAME token instead of 401ing. The token is
    /// long-lived (a 1-year TTL minted by the node), so it outlives the runner's
    /// idle-outs/crashes for the app's whole life.
    #[test]
    fn spawn_spec_for_respawn_keeps_network_and_token() {
        let cfg = shared();
        let mut rec = record();
        rec.network = Some("n_jpegxik72nng".to_owned());
        rec.runner_join_token = Some("jwt.x".to_owned());
        let spec = cfg.spawn_spec_for(&rec);
        assert_eq!(
            spec.network.as_deref(),
            Some("n_jpegxik72nng"),
            "respawn must rejoin the same tenant network"
        );
        assert_eq!(
            spec.runner_join_token.as_deref(),
            Some("jwt.x"),
            "respawn must reuse the persisted join token (no 401)"
        );
    }

    /// A record with a persisted managed `tabbify.toml` reconstructs a spec that
    /// carries it, so a RESPAWN re-derives the connect-repo app's
    /// `[runtime]`/`[routes]` instead of reverting to the hardcoded FC defaults.
    #[test]
    fn spawn_spec_for_respawn_keeps_manifest_toml() {
        let cfg = shared();
        let mut rec = record();
        rec.manifest_toml =
            Some("[app]\nname = \"sized\"\n[runtime]\nmemory_mb = 1024\n".to_owned());
        let spec = cfg.spawn_spec_for(&rec);
        assert_eq!(
            spec.manifest_toml.as_deref(),
            Some("[app]\nname = \"sized\"\n[runtime]\nmemory_mb = 1024\n"),
            "respawn must reuse the persisted managed tabbify.toml"
        );
    }

    /// A record with a persisted deploy-time `extra_env` reconstructs a spec
    /// that carries it, so a RESPAWN re-bakes the same KEY=VALUE entries into
    /// the guest `/init` (devbox SSH key, dev-session git vars, etc.).
    #[test]
    fn spawn_spec_for_respawn_keeps_extra_env() {
        let cfg = shared();
        let mut rec = record();
        rec.extra_env = Some(
            [(
                "TABBIFY_DEVBOX_AUTHORIZED_KEY".to_owned(),
                "ssh-ed25519 AAAA".to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        let spec = cfg.spawn_spec_for(&rec);
        assert_eq!(
            spec.extra_env
                .as_ref()
                .and_then(|m| m.get("TABBIFY_DEVBOX_AUTHORIZED_KEY"))
                .map(String::as_str),
            Some("ssh-ed25519 AAAA"),
            "respawn must reuse the persisted deploy-time extra env"
        );
    }

    /// A record with no managed config reconstructs a spec with `manifest_toml`
    /// None — a respawn keeps the hardcoded FC defaults (a `tcli`/S3-manifest app).
    #[test]
    fn spawn_spec_for_respawn_no_manifest_toml_when_absent() {
        let cfg = shared();
        let rec = record(); // record() has manifest_toml: None
        let spec = cfg.spawn_spec_for(&rec);
        assert!(
            spec.manifest_toml.is_none(),
            "a record with no managed config must reconstruct a spec without one"
        );
    }

    /// An unscoped record (no tenant network, no token) reconstructs a tokenless
    /// spec — the respawn stays unscoped, exactly as before.
    #[test]
    fn spawn_spec_for_respawn_no_token_when_unscoped() {
        let cfg = shared();
        let rec = record(); // record() has runner_join_token: None
        let spec = cfg.spawn_spec_for(&rec);
        assert!(
            spec.runner_join_token.is_none(),
            "an unscoped record must reconstruct a tokenless spec"
        );
    }

    /// A relay-only supervisor forwards `relay_only` onto every respawn spec, so
    /// a respawned runner also declares no reachable direct endpoint (handshake
    /// converges over the relay behind the host's shared NAT/firewall).
    #[test]
    fn spawn_spec_for_forwards_relay_only() {
        let mut cfg = shared();
        cfg.relay_only = true;
        let spec = cfg.spawn_spec_for(&record());
        assert!(
            spec.relay_only,
            "a relay-only supervisor must forward relay_only to its runners"
        );
    }

    /// A record with no parent reconstructs a parent-less spec (standalone).
    #[test]
    fn spawn_spec_for_preserves_absent_parent() {
        let cfg = shared();
        let mut rec = record();
        rec.parent = None;
        let spec = cfg.spawn_spec_for(&rec);
        assert!(spec.parent.is_none());
    }

    /// A record's (now inert) `requested_runtime` does NOT affect the respawn
    /// spec: the runtime is fixed to Firecracker, so a record carrying a stale
    /// `requested_runtime` still reconstructs a faithful spec (the field is only
    /// kept so old on-disk records deserialize). `SpawnSpec` no longer has a
    /// `runtime_override` field, so there is nothing for it to set.
    #[test]
    fn spawn_spec_for_ignores_requested_runtime() {
        let cfg = shared();
        let mut rec = record();
        rec.requested_runtime = Some("docker".to_owned());
        let spec = cfg.spawn_spec_for(&rec);
        // The spec faithfully reconstructs from the record's other fields; the
        // stale runtime hint is simply not consulted.
        assert_eq!(spec.uuid, rec.uuid);
        assert_eq!(spec.image_ref, rec.image_ref);
    }

    /// Accessors expose the orchestrator's config + record dir.
    #[test]
    fn new_stores_shared_and_runner_dir() {
        let dir = PathBuf::from("/var/lib/tabbify/runners");
        let orch = Orchestrator::new(shared(), dir.clone());
        assert_eq!(orch.runner_dir(), dir);
        assert_eq!(orch.shared().runner_bin, shared().runner_bin);
    }

    // ── deploy-in-flight guard ───────────────────────────────────────────────

    /// `begin_deploy` marks a uuid in-flight; the returned guard clears it on
    /// drop. `is_deploying` reflects the set membership across that lifecycle.
    #[test]
    fn deploy_guard_marks_and_clears_uuid() {
        let orch = Orchestrator::new(shared(), PathBuf::from("/run/tabbify/runners"));
        let uuid = "0191e7c2-1111-7222-8333-444455556666";

        assert!(
            !is_deploying(&orch, uuid),
            "not deploying before begin_deploy"
        );
        {
            let _guard = orch.begin_deploy(uuid);
            assert!(
                is_deploying(&orch, uuid),
                "in-flight while the guard is held"
            );
        }
        assert!(
            !is_deploying(&orch, uuid),
            "guard drop must clear the in-flight mark"
        );
    }

    /// The in-flight set is shared across clones (the orchestrator is cloned to
    /// the monitor task), so a deploy begun on one clone is visible on another.
    #[test]
    fn deploy_guard_visible_across_clones() {
        let orch = Orchestrator::new(shared(), PathBuf::from("/run/tabbify/runners"));
        let monitor_view = orch.clone();
        let uuid = "0191e7c2-1111-7222-8333-444455556666";

        let _guard = orch.begin_deploy(uuid);
        assert!(
            is_deploying(&monitor_view, uuid),
            "a deploy begun on one clone must be visible to the monitor clone"
        );
    }

    /// Distinct uuids are tracked independently — a deploy on one does not
    /// shield another.
    #[test]
    fn deploy_guard_is_per_uuid() {
        let orch = Orchestrator::new(shared(), PathBuf::from("/run/tabbify/runners"));
        let a = "0191e7c2-1111-7222-8333-444455556666";
        let b = "019e7903-aaaa-7bbb-8ccc-ddddeeeeffff";

        let _guard = orch.begin_deploy(a);
        assert!(is_deploying(&orch, a));
        assert!(
            !is_deploying(&orch, b),
            "guard must be scoped to its own uuid"
        );
    }
}
