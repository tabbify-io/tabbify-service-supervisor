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

use std::path::PathBuf;
use std::time::Duration;

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
            // Respawn on the same deployed version the record was last at.
            image_ref: record.image_ref.clone(),
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
}

impl Orchestrator {
    /// Create an orchestrator over `shared` config, reading/writing runner
    /// records under `runner_dir`.
    #[must_use]
    pub fn new(shared: SharedRunnerConfig, runner_dir: PathBuf) -> Self {
        Self { shared, runner_dir }
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
                // A failed respawn or a backoff-gated skip are left out of both
                // buckets; the periodic monitor will retry on the next tick.
                RecordOutcome::RespawnFailed | RecordOutcome::Backoff => {}
            }
        }

        tracing::info!(
            adopted = summary.adopted.len(),
            respawned = summary.respawned.len(),
            adopted_uuids = ?summary.adopted,
            respawned_uuids = ?summary.respawned,
            "re-adopted runner fleet on startup"
        );

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

    fn shared() -> SharedRunnerConfig {
        SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: None,
            no_mesh: true,
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

        // From the record.
        assert_eq!(spec.uuid, rec.uuid);
        assert_eq!(spec.control_sock, rec.control_sock);
        assert_eq!(spec.parent, rec.parent);
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

    /// Accessors expose the orchestrator's config + record dir.
    #[test]
    fn new_stores_shared_and_runner_dir() {
        let dir = PathBuf::from("/var/lib/tabbify/runners");
        let orch = Orchestrator::new(shared(), dir.clone());
        assert_eq!(orch.runner_dir(), dir);
        assert_eq!(orch.shared().runner_bin, shared().runner_bin);
    }
}
