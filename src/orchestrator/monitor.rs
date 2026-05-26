//! Per-record reconcile: keep every recorded runner alive (Task 2.4 + 2.5 + 2.7).
//!
//! The orchestrator has no in-memory fleet table — the [`RunnerHandle`] records
//! on disk ARE its source of truth. The whole self-healing / crash-survival
//! story reduces to ONE per-record decision, [`reconcile_record`]:
//!
//! 1. **process** — is `handle.pid` still a live process
//!    ([`process_is_alive`])?
//! 2. **grace window** — was the runner spawned very recently (within
//!    [`SPAWN_GRACE`])? A just-spawned process that hasn't yet bound its
//!    control socket MUST be left alone to avoid creating a duplicate.
//! 3. **control socket** — does the runner answer
//!    [`ControlClient::health`] within a short timeout?
//!
//! The decision matrix (Task 2.7):
//!
//! | pid alive | within grace | socket healthy | action |
//! |-----------|-------------|----------------|--------|
//! | no        | any         | any            | respawn (no kill — pid already gone) |
//! | yes       | yes         | any            | **adopt** (socket not required yet) |
//! | yes       | no          | yes            | adopt |
//! | yes       | no          | no             | **kill** old pid, then respawn |
//!
//! The last row is the "kill-before-respawn" fix: a hung runner past the grace
//! window is killed with `SIGKILL` before the replacement is spawned, so the
//! old process is never orphaned.
//!
//! ## Testability
//!
//! The core decision logic lives in [`decide_pid_grace`] (pure, synchronous,
//! injectable clock + pid-liveness), which returns a [`PidDecision`]. The
//! async `reconcile_record` calls it first, then checks the socket only when
//! `PidDecision::CheckSocket` instructs it to, so the two orthogonal
//! concerns (pid/grace vs. socket) are each independently unit-testable.
//! Integration-level socket behavior is covered by the existing 2.4/2.5 tests.

use std::time::Duration;

use crate::orchestrator::Orchestrator;
use crate::orchestrator::client::ControlClient;
use crate::orchestrator::handle::RunnerHandle;
use crate::orchestrator::spawn::spawn_runner;

/// Liveness probe for runner processes: returns `true` iff `pid` is a live,
/// non-zombie process.
///
/// Uses `waitpid(pid, WNOHANG)` first: if the process has already exited (even
/// as a zombie waiting for the parent to reap it), `waitpid` reaps it and we
/// report dead. This prevents the grace-window logic from falsely adopting a
/// just-killed runner whose zombie entry is still in the process table (a `kill
/// -0` on a zombie returns 0 / "alive" on POSIX, which would fool the grace check).
///
/// Falls back to `kill(pid, 0)` for non-child processes (e.g. pre-existing
/// runners discovered via readopt after a supervisor restart — those are NOT
/// children of the current supervisor process, so `waitpid` returns `ECHILD`).
fn runner_is_alive(pid: u32) -> bool {
    // SAFETY: waitpid is a POSIX syscall. WNOHANG makes it non-blocking: it
    // returns 0 if the child has not yet changed state, or `pid` if it has
    // (exited/stopped). ECHILD (-1 with errno ECHILD) means `pid` is not a
    // child of this process, in which case we fall through to kill(pid,0).
    let wait_ret =
        unsafe { libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) };
    if wait_ret == pid as libc::pid_t {
        // waitpid reaped the zombie — process has definitely exited.
        return false;
    }
    if wait_ret == 0 {
        // waitpid returned immediately: child exists and hasn't changed state
        // (i.e. it is still running). Report alive.
        return true;
    }
    // wait_ret < 0: ECHILD (not our child) or some other error. Fall back to
    // the kill(0) existence check — this is the readopt case where we inherit
    // a runner that was spawned by a previous supervisor instance.
    // SAFETY: kill(pid, 0) — POSIX existence check, sends no signal.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// How long after a spawn to treat an alive-but-unhealthy-socket runner as
/// "still starting" (adopt without requiring socket health). This prevents the
/// monitor from respawning a just-spawned runner before its control socket has
/// had time to bind.
pub const SPAWN_GRACE: Duration = Duration::from_secs(10);

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

/// Result of the pure pid+grace decision step (first half of reconciliation).
///
/// This is what [`decide_pid_grace`] returns. It answers "given the pid liveness
/// and the spawn age, what do I need next?" without touching the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PidDecision {
    /// Pid is dead — spawn a replacement immediately (no kill needed).
    RespawnDead,
    /// Pid is alive and within the grace window — adopt unconditionally.
    /// (Do NOT check the socket: the runner may not have bound it yet.)
    AdoptInGrace,
    /// Pid is alive and past the grace window — check the socket next.
    CheckSocket,
}

/// Pure, synchronous decision: given pid liveness and spawn age, return what
/// to do next. All inputs are injected so the function is deterministic in
/// unit tests.
///
/// # Parameters
/// - `pid` — the recorded pid.
/// - `spawned_at` — unix-seconds timestamp of the last spawn (`0` = absent →
///   treated as age = `now_secs`, i.e. always past grace).
/// - `now_secs` — current unix timestamp in seconds (injected clock).
/// - `is_pid_alive` — probe returning `true` iff the pid is a live process.
pub(crate) fn decide_pid_grace(
    pid: u32,
    spawned_at: u64,
    now_secs: u64,
    is_pid_alive: impl Fn(u32) -> bool,
) -> PidDecision {
    if !is_pid_alive(pid) {
        return PidDecision::RespawnDead;
    }
    let age_secs = now_secs.saturating_sub(spawned_at);
    if age_secs < SPAWN_GRACE.as_secs() {
        PidDecision::AdoptInGrace
    } else {
        PidDecision::CheckSocket
    }
}

/// Send `SIGKILL` to `pid`. Best-effort: logs on failure (e.g. permission
/// error or already-reaped pid).
fn kill_pid(pid: u32) {
    // SAFETY: `libc::kill` is a standard POSIX syscall. SIGKILL to a
    // (possibly dead) pid is harmless — ESRCH is simply logged.
    let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(pid, error = %err, "SIGKILL to hung runner failed (may already be gone)");
    }
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
    /// Implements the Task 2.7 decision matrix (grace window + kill-before-respawn).
    /// This is the single source of truth shared by [`tick`](Self::tick) and
    /// [`readopt`](Self::readopt). A respawn failure is logged and reported as
    /// [`RecordOutcome::RespawnFailed`] — never propagated.
    pub(crate) async fn reconcile_record(&self, record: &RunnerHandle) -> RecordOutcome {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        match decide_pid_grace(record.pid, record.spawned_at, now_secs, runner_is_alive) {
            PidDecision::RespawnDead => {
                tracing::warn!(
                    uuid = %record.uuid,
                    pid = record.pid,
                    control_sock = %record.control_sock.display(),
                    "runner is dead — respawning"
                );
                self.do_respawn(record, None).await
            }

            PidDecision::AdoptInGrace => {
                tracing::debug!(
                    uuid = %record.uuid,
                    pid = record.pid,
                    "runner alive within grace window — adopted (socket check skipped)"
                );
                RecordOutcome::Adopted
            }

            PidDecision::CheckSocket => {
                // Past grace window: require the socket to be healthy.
                let socket_ok = ControlClient::new(&record.control_sock)
                    .health()
                    .await
                    .is_ok();

                if socket_ok {
                    tracing::debug!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        "runner alive and socket healthy — adopted"
                    );
                    RecordOutcome::Adopted
                } else {
                    tracing::warn!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        control_sock = %record.control_sock.display(),
                        "runner alive but socket unhealthy past grace window — killing before respawn"
                    );
                    kill_pid(record.pid);
                    self.do_respawn(record, Some(record.pid)).await
                }
            }
        }
    }

    /// Spawn a replacement runner and update the on-disk record. `killed_pid` is
    /// provided when the caller already sent SIGKILL to an old process (logged
    /// for traceability).
    async fn do_respawn(&self, record: &RunnerHandle, killed_pid: Option<u32>) -> RecordOutcome {
        let spec = self.shared.spawn_spec_for(record);
        match spawn_runner(&spec, &self.runner_dir).await {
            Ok((new_handle, _child)) => {
                if let Some(old) = killed_pid {
                    tracing::info!(
                        uuid = %new_handle.uuid,
                        old_pid = old,
                        new_pid = new_handle.pid,
                        "killed hung runner and spawned replacement"
                    );
                } else {
                    tracing::info!(
                        uuid = %new_handle.uuid,
                        old_pid = record.pid,
                        new_pid = new_handle.pid,
                        "respawned dead runner"
                    );
                }
                RecordOutcome::Respawned
            }
            Err(e) => {
                tracing::error!(
                    uuid = %record.uuid,
                    error = %e,
                    "failed to respawn dead runner (will retry next tick)"
                );
                RecordOutcome::RespawnFailed
            }
        }
    }
}

// ── Unit tests for the pure pid+grace decision function ──────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // ── pid dead ─────────────────────────────────────────────────────────────

    /// A dead pid → RespawnDead regardless of spawned_at.
    #[test]
    fn dead_pid_always_respawn_dead() {
        assert_eq!(
            decide_pid_grace(999, 0, now_secs(), |_| false),
            PidDecision::RespawnDead
        );
    }

    #[test]
    fn dead_pid_with_recent_spawn_still_respawn_dead() {
        let n = now_secs();
        assert_eq!(
            decide_pid_grace(999, n, n, |_| false),
            PidDecision::RespawnDead
        );
    }

    // ── within grace window ───────────────────────────────────────────────────

    /// Alive pid spawned NOW (age = 0) → AdoptInGrace (socket not checked).
    #[test]
    fn alive_within_grace_adopts_in_grace() {
        let n = now_secs();
        assert_eq!(
            decide_pid_grace(999, n, n, |_| true),
            PidDecision::AdoptInGrace,
            "runner spawned right now must be AdoptInGrace"
        );
    }

    /// Alive pid spawned SPAWN_GRACE - 1 seconds ago → still within grace.
    #[test]
    fn alive_just_within_grace_adopts_in_grace() {
        let grace = SPAWN_GRACE.as_secs();
        let n = 1_700_000_000u64;
        let spawned_at = n - (grace - 1);
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::AdoptInGrace
        );
    }

    // ── past grace window ─────────────────────────────────────────────────────

    /// Alive pid at EXACTLY the grace boundary (age == SPAWN_GRACE) → past grace
    /// → CheckSocket.
    #[test]
    fn alive_at_grace_boundary_checks_socket() {
        let grace = SPAWN_GRACE.as_secs();
        let n = 1_700_000_000u64;
        let spawned_at = n - grace; // age == SPAWN_GRACE → NOT within grace
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::CheckSocket
        );
    }

    /// Alive pid well past grace → CheckSocket.
    #[test]
    fn alive_past_grace_checks_socket() {
        let n = 1_700_000_000u64;
        let spawned_at = n - SPAWN_GRACE.as_secs() - 60;
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::CheckSocket
        );
    }

    /// `spawned_at = 0` (old record with no timestamp) → age = now_secs
    /// (huge number) → always past grace → CheckSocket.
    #[test]
    fn spawned_at_zero_is_past_grace() {
        let n = 1_700_000_000u64;
        assert_eq!(
            decide_pid_grace(999, 0, n, |_| true),
            PidDecision::CheckSocket,
            "spawned_at=0 must be treated as past the grace window"
        );
    }

    // ── probe call discipline ─────────────────────────────────────────────────

    /// The pid probe is called exactly once, and its result drives the decision.
    #[test]
    fn pid_probe_called_once() {
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let n = now_secs();
        let _ = decide_pid_grace(999, n, n, move |_| {
            cc.fetch_add(1, Ordering::SeqCst);
            true
        });
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "pid probe must be called exactly once"
        );
    }

    /// Dead pid: `decide_pid_grace` returns `RespawnDead` and the caller is not
    /// expected to make a socket call — verify the decision itself conveys this
    /// (no socket closure needed in the pure function).
    #[test]
    fn dead_pid_returns_respawn_dead_no_socket_needed() {
        // The pure function has no socket closure — the caller only calls the
        // socket when PidDecision::CheckSocket is returned. Asserting the
        // decision is sufficient.
        let result = decide_pid_grace(999, 0, now_secs(), |_| false);
        assert_eq!(result, PidDecision::RespawnDead);
        // Callers must not invoke the socket health check for RespawnDead.
        let probed = Arc::new(AtomicBool::new(false));
        // (Simulating the caller's logic: only check socket on CheckSocket)
        if result == PidDecision::CheckSocket {
            probed.store(true, Ordering::SeqCst);
        }
        assert!(
            !probed.load(Ordering::SeqCst),
            "socket must not be checked when pid is dead"
        );
    }

    /// Within grace: caller must NOT invoke socket check.
    #[test]
    fn within_grace_returns_adopt_in_grace_no_socket_needed() {
        let n = now_secs();
        let result = decide_pid_grace(999, n, n, |_| true);
        assert_eq!(result, PidDecision::AdoptInGrace);
        let probed = Arc::new(AtomicBool::new(false));
        if result == PidDecision::CheckSocket {
            probed.store(true, Ordering::SeqCst);
        }
        assert!(
            !probed.load(Ordering::SeqCst),
            "socket must not be checked within grace window"
        );
    }
}
