//! Crash-at-startup loop-guard (spec §3.3): a durable boot-attempt counter that
//! works WITHOUT a live process.
//!
//! Every existing resilience track (B sd_notify watchdog, K data-plane liveness,
//! C signed remote-restart, D OTA rollback) is BLIND to "the process won't even
//! start" — they all arm only AFTER `notify_ready()` + a mesh join. A binary that
//! exits in ~31ms BEFORE bind/READY never reaches any of them, and the systemd
//! `StartLimit` turns the crash-loop into a full-dark outage with no automated
//! recovery (the 2026-06-22 MSI brick).
//!
//! This sidecar is THE mechanism that makes the `OnFailure=tabbify-boot-revert`
//! catch-net correct under systemd v256 (which fires `OnFailure=` on EVERY failed
//! `ExecStart`, not once at the `StartLimit` park). Each boot bumps the counter at
//! the very TOP of `main` (before any fallible startup step); a healthy boot
//! clears it right after `notify_ready()`. The OnFailure script reads the counter
//! and only the fire on which `count` crossed the threshold performs a revert —
//! every other fire is a no-op (so the start-rate-limit counter is never reset on
//! a sub-threshold fire, keeping the §4 circuit-breaker intact).
//!
//! Same durable-sidecar pattern as [`crate::watchdog_pet::tier1_backoff`] and
//! [`crate::watchdog_pet::tier2`], persisted under `<data_dir>/self-heal/` — but
//! written with an ATOMIC tmp+rename (mirroring
//! [`crate::selfupdate::swap::write_version_file`]) so a crash mid-write can never
//! tear the loop-guard's own state.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Default crash-at-startup threshold: how many consecutive boot attempts that
/// never reached READY are tolerated before the OnFailure catch-net reverts to
/// the previous-good release. Three spaced attempts (`RestartSec≈10s` each) span
/// ~60s of real retry — a one-off transient recovers on the first spaced retry,
/// a persistent bad binary exhausts the budget and triggers the revert.
///
/// ⚠ FIX 10 — LOCKSTEP with the nix OnFailure script's `THRESHOLD=3`
/// (`nixos/tabbify-node.nix`, in `tabbifyBootRevertScript`). That BASH constant
/// is what ACTUALLY gates the production revert (the script reads the sidecar
/// `count` directly and decides whether to call `revert-to-previous`); THIS Rust
/// const is consumed ONLY by [`BootAttempts::should_revert`] in unit tests — the
/// boot path never calls it. They MUST stay equal: if you change one, change the
/// other. (They are deliberately NOT shared via codegen — the script must work
/// with a binary that predates any threshold mechanism.)
pub const REVERT_THRESHOLD: u32 = 3;

/// Persisted crash-at-startup boot-attempt counter
/// (`<data_dir>/self-heal/boot-attempts.json`). `count` is the number of boot
/// attempts since the last healthy boot (cleared on READY); `first_attempt_ts` is
/// the unix-seconds of the boot that opened the current streak (for diagnostics /
/// future windowing); `reverted_to` records the version this loop-guard already
/// reverted TO, so a second escalation (the reverted binary ALSO crash-loops) can
/// distinguish "revert once" from "reboot-as-last-resort".
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootAttempts {
    /// Consecutive boot attempts that have not yet reached READY.
    pub count: u32,
    /// Unix-seconds of the boot that opened the current streak (0 when idle).
    pub first_attempt_ts: u64,
    /// The version this loop-guard already reverted to, if any. `Some` means a
    /// revert has already happened this streak — a further escalation reboots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverted_to: Option<String>,
}

/// Path of the boot-attempts sidecar under `data_dir`.
#[must_use]
pub fn boot_attempts_path(data_dir: &Path) -> PathBuf {
    data_dir.join("self-heal").join("boot-attempts.json")
}

/// Unix-seconds now (the production clock for [`BootAttempts::bump`]). A clock
/// error degrades to 0 (a benign `first_attempt_ts`, never a panic).
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl BootAttempts {
    /// Load the persisted counter for `data_dir` (missing/corrupt ⇒ a fresh zero
    /// state — NEVER panic on a torn sidecar; defaulting to `count = 0` only ever
    /// DELAYS a revert by one boot, the fail-safe direction for a loop-guard).
    #[must_use]
    pub fn load(data_dir: &Path) -> Self {
        let path = boot_attempts_path(data_dir);
        std::fs::read_to_string(&path).map_or_else(
            |_| Self::default(),
            |json| serde_json::from_str(&json).unwrap_or_default(),
        )
    }

    /// Atomically persist this counter under `data_dir` (tmp + rename, so a crash
    /// mid-write cannot tear the loop-guard's own state). Best-effort — a write
    /// failure is logged, not propagated, so the boot path is never blocked by a
    /// failure to persist the loop-guard.
    pub fn save(&self, data_dir: &Path) {
        let path = boot_attempts_path(data_dir);
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!(error = %e, "boot-health: create self-heal dir failed");
                return;
            }
        }
        let json = match serde_json::to_string_pretty(self) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(error = %e, "boot-health: serialize boot-attempts failed");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, &json) {
            tracing::warn!(error = %e, "boot-health: write tmp boot-attempts failed");
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            tracing::warn!(error = %e, "boot-health: rename boot-attempts into place failed");
        }
    }

    /// Increment the counter and persist (atomic). Called at the very TOP of
    /// `main` on every real boot, BEFORE any fallible startup step, so a
    /// crash-at-startup that never reaches READY is still counted. Stamps
    /// `first_attempt_ts` when opening a fresh streak (count was 0). Returns the
    /// bumped state.
    #[must_use]
    pub fn bump(mut self, data_dir: &Path) -> Self {
        if self.count == 0 {
            self.first_attempt_ts = now_unix();
        }
        self.count = self.count.saturating_add(1);
        self.save(data_dir);
        self
    }

    /// Zero the counter and persist (atomic). Called right after `notify_ready()`:
    /// this boot reached READY, so the binary can at least boot and serve — the
    /// streak (and any `reverted_to` marker from a prior revert that this healthy
    /// boot vindicates) is cleared so the NEXT incident starts from a clean slate.
    pub fn clear(data_dir: &Path) {
        Self::default().save(data_dir);
    }

    /// Stamp `version` as the version this loop-guard reverted TO and persist
    /// (atomic). After a revert the streak is reset to zero so the reverted binary
    /// gets its OWN fresh boot budget; if it ALSO crosses the threshold,
    /// `reverted_to` being `Some` is what escalates to reboot-as-last-resort.
    pub fn mark_reverted(version: &str, data_dir: &Path) {
        Self {
            count: 0,
            first_attempt_ts: 0,
            reverted_to: Some(version.to_owned()),
        }
        .save(data_dir);
    }

    /// Whether the crash-at-startup streak has crossed `threshold`. The OnFailure
    /// catch-net consults this: below the threshold it lets `RestartSec` re-try
    /// (transient); at/above it performs a revert.
    #[must_use]
    pub const fn should_revert(&self, threshold: u32) -> bool {
        self.count >= threshold
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_sidecar_is_a_fresh_zero_state() {
        let dir = tempfile::TempDir::new().unwrap();
        // No file on disk ⇒ default (count 0, no revert marker).
        assert_eq!(BootAttempts::load(dir.path()), BootAttempts::default());
        assert_eq!(BootAttempts::load(dir.path()).count, 0);
    }

    #[test]
    fn corrupt_sidecar_defaults_to_zero_no_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = boot_attempts_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{ this is not json").unwrap();
        // No panic; a torn sidecar reads as zero (the fail-safe direction: a
        // revert is at most delayed by one boot, never spuriously triggered).
        assert_eq!(BootAttempts::load(dir.path()), BootAttempts::default());
    }

    #[test]
    fn bump_increments_and_persists() {
        let dir = tempfile::TempDir::new().unwrap();
        let after_one = BootAttempts::load(dir.path()).bump(dir.path());
        assert_eq!(after_one.count, 1);
        // Persisted: a fresh load sees the bumped count.
        assert_eq!(BootAttempts::load(dir.path()).count, 1);

        let after_two = BootAttempts::load(dir.path()).bump(dir.path());
        assert_eq!(after_two.count, 2);
        assert_eq!(BootAttempts::load(dir.path()).count, 2);
    }

    #[test]
    fn bump_stamps_first_attempt_ts_only_when_opening_a_streak() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = BootAttempts::load(dir.path()).bump(dir.path());
        assert!(
            first.first_attempt_ts > 0,
            "opening a streak stamps the boot time"
        );
        let second = BootAttempts::load(dir.path()).bump(dir.path());
        assert_eq!(
            second.first_attempt_ts, first.first_attempt_ts,
            "a streak that is already open keeps its original first_attempt_ts",
        );
    }

    #[test]
    fn clear_zeroes_the_counter() {
        let dir = tempfile::TempDir::new().unwrap();
        let _ = BootAttempts::load(dir.path()).bump(dir.path());
        let _ = BootAttempts::load(dir.path()).bump(dir.path());
        assert_eq!(BootAttempts::load(dir.path()).count, 2);

        BootAttempts::clear(dir.path());
        assert_eq!(BootAttempts::load(dir.path()), BootAttempts::default());
        assert_eq!(BootAttempts::load(dir.path()).count, 0);
    }

    #[test]
    fn should_revert_true_at_or_above_threshold() {
        let below = BootAttempts {
            count: REVERT_THRESHOLD - 1,
            ..Default::default()
        };
        assert!(!below.should_revert(REVERT_THRESHOLD));

        let at = BootAttempts {
            count: REVERT_THRESHOLD,
            ..Default::default()
        };
        assert!(at.should_revert(REVERT_THRESHOLD));

        let above = BootAttempts {
            count: REVERT_THRESHOLD + 5,
            ..Default::default()
        };
        assert!(above.should_revert(REVERT_THRESHOLD));
    }

    #[test]
    fn mark_reverted_records_the_target_and_zeroes_count() {
        let dir = tempfile::TempDir::new().unwrap();
        // Build up a streak, then mark a revert away from it.
        let _ = BootAttempts::load(dir.path()).bump(dir.path());
        let _ = BootAttempts::load(dir.path()).bump(dir.path());
        let _ = BootAttempts::load(dir.path()).bump(dir.path());

        BootAttempts::mark_reverted("v1.0.0", dir.path());

        let after = BootAttempts::load(dir.path());
        assert_eq!(after.count, 0, "a revert resets the streak for a fresh budget");
        assert_eq!(
            after.reverted_to.as_deref(),
            Some("v1.0.0"),
            "the reverted-to version is recorded so a re-crash escalates to reboot",
        );
    }

    #[test]
    fn round_trips_full_state_through_the_sidecar() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = BootAttempts {
            count: 4,
            first_attempt_ts: 1_700_000_000,
            reverted_to: Some("v1.2.3".to_owned()),
        };
        state.save(dir.path());
        assert_eq!(BootAttempts::load(dir.path()), state);
    }
}
