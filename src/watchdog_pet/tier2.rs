//! Track B tier-2: reboot-on-persistent-failure.
//!
//! Tier-1 ([`super`]) restarts supervisord on a black hole. If N CONSECUTIVE
//! watchdog-restarts still don't restore the data plane, a wedged kernel-TUN /
//! stuck NAT mapping needs a host `reboot` (a process restart can't clear it).
//! The reboot is hard-capped at ≤3/hr by the SHARED Track-C `RebootGuard` (one
//! host-wide budget for B's tier-2 AND C's `RebootHost`), and the nix
//! `StartLimit*` parks the unit `failed` for a human past that.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::mesh_command::reboot_guard::RebootGuard;

/// Consecutive watchdog-restarts that left the data plane dead before tier-2
/// escalates to a host reboot. A black hole that survives this many restarts is
/// almost certainly a wedged kernel-TUN / NAT mapping a process restart cannot
/// clear (spec §5 B2).
pub const REBOOT_AFTER_CONSECUTIVE_DEAD: u32 = 3;

/// Grace window after boot before the tier-2 check samples the data plane. Must
/// exceed a realistic fresh-handshake convergence time (Track-K threshold ~90s +
/// WAN slack) so a node that DID self-heal via the tier-1 restart reads healthy
/// and resets its streak — we only escalate when the restart demonstrably failed
/// to restore the tunnel.
pub const TIER2_GRACE: Duration = Duration::from_secs(180);

/// What tier-2 should do given the consecutive-dead-restart count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier2Action {
    /// Under the threshold — keep relying on tier-1 restarts; do not reboot.
    Hold,
    /// `threshold` consecutive restarts left the data plane dead — escalate to a
    /// host reboot (still subject to the shared ≤3/hr `RebootGuard`).
    Reboot,
}

/// Pure tier-2 decision: reboot once `consecutive_dead` reaches `threshold`.
#[must_use]
pub const fn escalate_decision(consecutive_dead: u32, threshold: u32) -> Tier2Action {
    if consecutive_dead >= threshold {
        Tier2Action::Reboot
    } else {
        Tier2Action::Hold
    }
}

/// Persisted consecutive-dead-restart counter (`<data_dir>/self-heal/
/// consecutive-dead.json`, durable-sidecar pattern à la the dev-session record
/// #63). Distinct from the reboot loop-guard's history: this counts how many
/// restarts in a row failed to restore the data plane (the tier-2 TRIGGER); the
/// `RebootGuard` caps how many reboots may fire per hour (the tier-2 LIMITER).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeadStreak {
    /// Consecutive watchdog-restarts that found the data plane still dead.
    pub consecutive_dead: u32,
}

/// Path of the consecutive-dead sidecar under `data_dir`.
#[must_use]
pub fn dead_streak_path(data_dir: &Path) -> PathBuf {
    data_dir.join("self-heal").join("consecutive-dead.json")
}

/// Load the persisted streak (missing/corrupt ⇒ a fresh zero streak).
#[must_use]
pub fn load_dead_streak(path: &Path) -> DeadStreak {
    fs::read_to_string(path).map_or_else(
        |_| DeadStreak::default(),
        |json| serde_json::from_str(&json).unwrap_or_default(),
    )
}

/// Persist the streak (best-effort; a write failure is logged by the caller).
///
/// # Errors
/// Propagates a directory-create or write I/O error.
pub fn save_dead_streak(path: &Path, streak: DeadStreak) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json =
        serde_json::to_string(&streak).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    fs::write(path, json)
}

/// Fold a fresh data-plane observation into the persisted streak: a healthy
/// sample RESETS the streak to zero (and the boot is no longer counting);
/// otherwise the count rolls forward. Returns the NEW streak (caller persists).
#[must_use]
pub const fn fold_observation(prev: DeadStreak, data_plane_healthy: bool) -> DeadStreak {
    if data_plane_healthy {
        DeadStreak { consecutive_dead: 0 }
    } else {
        DeadStreak {
            consecutive_dead: prev.consecutive_dead.saturating_add(1),
        }
    }
}

/// The reboot exec seam: run `systemctl reboot`. Real impl shells out; tests
/// inject a spy. Mirrors `selfupdate::swap::production_restart_runner`.
pub trait RebootRunner {
    /// Trigger a host reboot.
    fn reboot(&self);
}

/// Production reboot runner: `systemctl reboot`.
pub struct SystemctlReboot;
impl RebootRunner for SystemctlReboot {
    fn reboot(&self) {
        let _ = Command::new("systemctl").arg("reboot").status();
    }
}

/// Apply a [`Tier2Action`] through the shared reboot loop-guard. A `Reboot` only
/// reaches `runner.reboot()` when [`RebootGuard::try_reboot_now`] grants a slot
/// (≤3/hr, persisted); past the cap it parks for a human (the nix `StartLimit*`
/// is the systemd-level backstop). Returns `true` iff a reboot was triggered.
pub fn apply_tier2<R: RebootRunner + ?Sized>(
    action: Tier2Action,
    guard: &RebootGuard,
    runner: &R,
) -> bool {
    if action != Tier2Action::Reboot {
        return false;
    }
    if !guard.try_reboot_now() {
        tracing::error!(
            "watchdog tier-2: data plane dead across {REBOOT_AFTER_CONSECUTIVE_DEAD} restarts \
             but reboot loop-guard PARKED (≤3/hr exhausted) — leaving failed for a human"
        );
        return false;
    }
    tracing::error!(
        "watchdog tier-2: data plane dead across {REBOOT_AFTER_CONSECUTIVE_DEAD} consecutive \
         watchdog-restarts — escalating to systemctl reboot (guard slot consumed)"
    );
    runner.reboot();
    true
}

/// The once-per-boot tier-2 check: sample the data plane, fold it into the
/// persisted consecutive-dead streak (healthy ⇒ reset to 0, dead ⇒ +1), persist,
/// then escalate to a host reboot (guarded) once the streak hits
/// [`REBOOT_AFTER_CONSECUTIVE_DEAD`]. A reboot RESETS the streak so the next boot
/// starts fresh (and the ≤3/hr `RebootGuard` is the real loop cap). Pure given
/// its injected collaborators — the live task just calls this after [`TIER2_GRACE`].
/// Returns `true` iff a reboot was triggered.
pub fn run_tier2_boot_check<R: RebootRunner + ?Sized>(
    probe: &dyn Fn() -> bool,
    streak_path: &Path,
    guard: &RebootGuard,
    runner: &R,
) -> bool {
    let healthy = probe();
    let folded = fold_observation(load_dead_streak(streak_path), healthy);
    let action = escalate_decision(folded.consecutive_dead, REBOOT_AFTER_CONSECUTIVE_DEAD);

    // On a reboot we reset the streak so a post-reboot boot doesn't immediately
    // re-escalate; otherwise persist the folded streak as-is.
    let to_persist = if action == Tier2Action::Reboot {
        DeadStreak::default()
    } else {
        folded
    };
    if let Err(e) = save_dead_streak(streak_path, to_persist) {
        tracing::warn!(error = %e, "watchdog tier-2: dead-streak persist failed");
    }

    if healthy {
        tracing::debug!("watchdog tier-2: data plane healthy at boot-check — streak reset");
    } else {
        tracing::warn!(
            consecutive_dead = folded.consecutive_dead,
            "watchdog tier-2: data plane STILL dead {TIER2_GRACE:?} after restart"
        );
    }
    apply_tier2(action, guard, runner)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn escalate_holds_below_threshold_and_reboots_at_it() {
        assert_eq!(escalate_decision(0, 3), Tier2Action::Hold);
        assert_eq!(escalate_decision(2, 3), Tier2Action::Hold);
        assert_eq!(escalate_decision(3, 3), Tier2Action::Reboot);
        assert_eq!(escalate_decision(9, 3), Tier2Action::Reboot);
    }

    #[test]
    fn fold_resets_on_healthy_and_rolls_forward_on_dead() {
        let s0 = DeadStreak::default();
        assert_eq!(s0.consecutive_dead, 0);
        // dead → +1, +1, +1
        let s1 = fold_observation(s0, false);
        let s2 = fold_observation(s1, false);
        let s3 = fold_observation(s2, false);
        assert_eq!(s3.consecutive_dead, 3);
        // one healthy sample wipes the streak
        let healthy = fold_observation(s3, true);
        assert_eq!(healthy.consecutive_dead, 0);
    }

    #[test]
    fn dead_streak_round_trips_through_the_sidecar() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dead_streak_path(dir.path());
        // missing file ⇒ zero
        assert_eq!(load_dead_streak(&path), DeadStreak::default());
        let streak = DeadStreak { consecutive_dead: 2 };
        save_dead_streak(&path, streak).unwrap();
        assert_eq!(load_dead_streak(&path), streak);
    }

    /// Spy reboot runner: records whether `reboot()` fired.
    struct SpyReboot {
        fired: Cell<bool>,
    }
    impl RebootRunner for SpyReboot {
        fn reboot(&self) {
            self.fired.set(true);
        }
    }

    #[test]
    fn apply_tier2_holds_without_rebooting() {
        let dir = tempfile::TempDir::new().unwrap();
        let guard = RebootGuard::new(dir.path().join("reboot-guard.json"));
        let spy = SpyReboot {
            fired: Cell::new(false),
        };
        assert!(!apply_tier2(Tier2Action::Hold, &guard, &spy));
        assert!(!spy.fired.get(), "Hold must NOT reboot");
    }

    #[test]
    fn apply_tier2_reboots_once_then_is_parked_by_the_shared_guard() {
        use crate::mesh_command::reboot_guard::MAX_PER_WINDOW;
        let dir = tempfile::TempDir::new().unwrap();
        let guard_path = dir.path().join("reboot-guard.json");
        // Reboot up to the shared ≤3/hr cap — each consumes one guard slot.
        for _ in 0..MAX_PER_WINDOW {
            let guard = RebootGuard::new(guard_path.clone());
            let spy = SpyReboot {
                fired: Cell::new(false),
            };
            assert!(apply_tier2(Tier2Action::Reboot, &guard, &spy));
            assert!(spy.fired.get(), "within-cap escalation must reboot");
        }
        // The next escalation is PARKED by the shared guard (no reboot fires).
        let guard = RebootGuard::new(guard_path);
        let spy = SpyReboot {
            fired: Cell::new(false),
        };
        assert!(!apply_tier2(Tier2Action::Reboot, &guard, &spy));
        assert!(
            !spy.fired.get(),
            "past the shared ≤3/hr cap, tier-2 must park (no reboot)"
        );
    }

    #[test]
    fn tier2_boot_check_escalates_only_after_the_streak_threshold() {
        let dir = tempfile::TempDir::new().unwrap();
        let streak_path = dead_streak_path(dir.path());
        let guard_path = dir.path().join("reboot-guard.json");
        let dead = || false;

        // First REBOOT_AFTER_CONSECUTIVE_DEAD-1 dead boots only accrue the streak.
        for _ in 0..REBOOT_AFTER_CONSECUTIVE_DEAD - 1 {
            let guard = RebootGuard::new(guard_path.clone());
            let spy = SpyReboot {
                fired: Cell::new(false),
            };
            assert!(!run_tier2_boot_check(&dead, &streak_path, &guard, &spy));
            assert!(!spy.fired.get());
        }
        // The threshold-th dead boot escalates to a reboot AND resets the streak.
        let guard = RebootGuard::new(guard_path);
        let spy = SpyReboot {
            fired: Cell::new(false),
        };
        assert!(run_tier2_boot_check(&dead, &streak_path, &guard, &spy));
        assert!(spy.fired.get(), "threshold reached ⇒ reboot");
        assert_eq!(
            load_dead_streak(&streak_path),
            DeadStreak::default(),
            "a reboot resets the persisted streak"
        );
    }

    #[test]
    fn tier2_boot_check_resets_streak_when_healthy_and_never_reboots() {
        let dir = tempfile::TempDir::new().unwrap();
        let streak_path = dead_streak_path(dir.path());
        // Seed a near-threshold streak on disk.
        save_dead_streak(
            &streak_path,
            DeadStreak {
                consecutive_dead: REBOOT_AFTER_CONSECUTIVE_DEAD - 1,
            },
        )
        .unwrap();
        let guard = RebootGuard::new(dir.path().join("reboot-guard.json"));
        let spy = SpyReboot {
            fired: Cell::new(false),
        };
        let healthy = || true;
        assert!(!run_tier2_boot_check(&healthy, &streak_path, &guard, &spy));
        assert!(!spy.fired.get(), "a healthy boot must never reboot");
        assert_eq!(
            load_dead_streak(&streak_path),
            DeadStreak::default(),
            "a healthy sample resets the streak"
        );
    }
}
