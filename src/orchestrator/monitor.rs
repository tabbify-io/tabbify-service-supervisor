//! Health-monitor tick: keep every recorded runner alive (Task 2.4).
//!
//! One [`tick`](Orchestrator::tick) walks every [`RunnerHandle`] on disk and,
//! for each, decides liveness from two independent signals:
//!
//! 1. **process** — is `handle.pid` still a live process
//!    ([`process_is_alive`])?
//! 2. **control socket** — does the runner answer
//!    [`ControlClient::health`] within a short timeout?
//!
//! A runner is considered **dead** if EITHER signal fails (pid gone OR the
//! control socket is unreachable). A dead runner is RESPAWNED by reconstructing
//! its [`SpawnSpec`] from the orchestrator's [`SharedRunnerConfig`] plus the
//! per-runner fields the record already carries (`uuid` / `control_sock` /
//! `parent`), then calling [`spawn_runner`], which overwrites the on-disk
//! record with the new pid.
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

impl Orchestrator {
    /// Run ONE monitor pass over every recorded runner: probe liveness and
    /// respawn any that are dead.
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
            if self.is_alive(&record).await {
                continue;
            }

            tracing::warn!(
                uuid = %record.uuid,
                pid = record.pid,
                control_sock = %record.control_sock.display(),
                "runner is dead — respawning"
            );

            let spec = self.shared.spawn_spec_for(&record);
            match spawn_runner(&spec, &self.runner_dir).await {
                Ok((new_handle, _child)) => {
                    tracing::info!(
                        uuid = %new_handle.uuid,
                        old_pid = record.pid,
                        new_pid = new_handle.pid,
                        "respawned dead runner"
                    );
                    respawned.push(new_handle.uuid);
                }
                Err(e) => {
                    // Best-effort: a single respawn failure (e.g. a transient
                    // bind clash on the socket path) must not stop us from
                    // healing the other runners this pass. The next tick retries.
                    tracing::error!(
                        uuid = %record.uuid,
                        error = %e,
                        "failed to respawn dead runner (will retry next tick)"
                    );
                }
            }
        }

        Ok(respawned)
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
