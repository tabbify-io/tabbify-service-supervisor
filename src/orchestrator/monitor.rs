//! Per-record reconcile: keep every recorded runner alive (Task 2.4 + 2.5).
//!
//! The orchestrator has no in-memory fleet table — the [`RunnerHandle`] records
//! on disk ARE its source of truth. The whole self-healing / crash-survival
//! story reduces to ONE per-record decision, [`reconcile_record`]:
//!
//! 1. **process** — is `handle.pid` still a live process
//!    ([`process_is_alive`])?
//! 2. **control socket** — does the runner answer
//!    [`ControlClient::health`] within a short timeout?
//!
//! A runner is alive iff BOTH signals pass. A LIVING runner is **adopted** —
//! left running untouched (its pid is never disturbed). A **dead** runner
//! (either signal fails) is **respawned** by reconstructing its [`SpawnSpec`]
//! from the orchestrator's [`SharedRunnerConfig`] plus the per-runner fields the
//! record already carries (`uuid` / `control_sock` / `parent`), then calling
//! [`spawn_runner`], which overwrites the on-disk record with the new pid.
//!
//! Both the periodic monitor [`tick`](Orchestrator::tick) (Task 2.4) and the
//! startup [`readopt`](Orchestrator::readopt) (Task 2.5) are thin loops over the
//! records that delegate the actual decision to the SAME `reconcile_record`, so
//! "living adopted, dead respawned" is defined in exactly one place.
//!
//! The periodic loop that calls `tick` on an interval lives in
//! [`Orchestrator::run_monitor`] (mirroring `main.rs`'s idle-reaper shape); the
//! single-pass `tick` is exposed so tests can run exactly one pass without
//! waiting real seconds.

use crate::firecracker::pidfile::process_is_alive;
use crate::orchestrator::Orchestrator;
use crate::orchestrator::client::ControlClient;
use crate::orchestrator::handle::RunnerHandle;
use crate::orchestrator::spawn::spawn_runner;

/// Outcome of reconciling a single runner record against its live process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    /// The runner is alive — left running untouched (pid unchanged).
    Adopted,
    /// The runner was dead and a replacement process was spawned.
    Respawned,
    /// The runner was dead but spawning a replacement failed (logged, skipped).
    RespawnFailed,
}

impl Orchestrator {
    /// Run ONE monitor pass over every recorded runner: probe liveness and
    /// respawn any that are dead (adopt the living ones untouched).
    ///
    /// Returns the list of UUIDs that were respawned this pass (empty when every
    /// runner was healthy). A failure to spawn a replacement for one runner is
    /// logged and skipped — it must not abort the pass for the other runners, so
    /// the method itself only returns `Err` for an unrecoverable failure to even
    /// enumerate the records.
    ///
    /// # Errors
    /// Returns an [`anyhow::Error`] only if the runner directory cannot be
    /// listed. Per-runner respawn failures are logged, not propagated.
    pub async fn tick(&self) -> anyhow::Result<Vec<String>> {
        let records = RunnerHandle::list(&self.runner_dir)?;
        let mut respawned = Vec::new();

        for record in records {
            if self.reconcile_record(&record).await == RecordOutcome::Respawned {
                respawned.push(record.uuid);
            }
        }

        Ok(respawned)
    }

    /// Reconcile ONE record: adopt it if its runner is alive, else respawn it.
    ///
    /// This is the single source of truth for "living → adopt (leave the pid
    /// alone), dead → respawn" shared by [`tick`](Self::tick) (the periodic
    /// monitor pass, Task 2.4) and [`readopt`](Self::readopt) (the startup
    /// re-adoption, Task 2.5). A respawn failure is logged and reported as
    /// [`RecordOutcome::RespawnFailed`] — never propagated — so one bad record
    /// cannot abort a whole pass; the next tick retries.
    pub(crate) async fn reconcile_record(&self, record: &RunnerHandle) -> RecordOutcome {
        if self.is_alive(record).await {
            // CRUCIAL: a living runner is ADOPTED, never respawned. Re-spawning a
            // healthy runner would kill+recreate it (a new pid), defeating the
            // crash-survival guarantee. We touch nothing.
            return RecordOutcome::Adopted;
        }

        tracing::warn!(
            uuid = %record.uuid,
            pid = record.pid,
            control_sock = %record.control_sock.display(),
            "runner is dead — respawning"
        );

        let spec = self.shared.spawn_spec_for(record);
        match spawn_runner(&spec, &self.runner_dir).await {
            Ok((new_handle, _child)) => {
                tracing::info!(
                    uuid = %new_handle.uuid,
                    old_pid = record.pid,
                    new_pid = new_handle.pid,
                    "respawned dead runner"
                );
                RecordOutcome::Respawned
            }
            Err(e) => {
                // Best-effort: a single respawn failure (e.g. a transient bind
                // clash on the socket path) must not stop us from healing the
                // other runners this pass. The next tick retries.
                tracing::error!(
                    uuid = %record.uuid,
                    error = %e,
                    "failed to respawn dead runner (will retry next tick)"
                );
                RecordOutcome::RespawnFailed
            }
        }
    }

    /// Liveness check for one record: the runner is alive iff its pid is still a
    /// live process AND its control socket answers `health` within the client's
    /// short timeout.
    ///
    /// The pid check comes first because it is a cheap local syscall with no
    /// I/O: when the process is gone we skip the socket round-trip entirely.
    async fn is_alive(&self, record: &RunnerHandle) -> bool {
        if !process_is_alive(record.pid) {
            return false;
        }
        ControlClient::new(&record.control_sock)
            .health()
            .await
            .is_ok()
    }
}
