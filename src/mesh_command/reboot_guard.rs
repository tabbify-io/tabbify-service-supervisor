//! Persisted reboot loop-guard (Track C `RebootHost` + Track B2 reuse).
//!
//! A wedged worker must NOT reboot-loop forever — MSI has no remote console and
//! SSM is blocked, so an unbounded reboot would brick it for a human. This guard
//! persists reboot timestamps to a JSON sidecar and refuses a reboot once
//! `MAX_PER_WINDOW` have fired within `WINDOW_SECS`. Same durable-sidecar
//! pattern as the dev-session record (#63).

use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Max reboots permitted within [`WINDOW_SECS`] before the guard parks.
pub const MAX_PER_WINDOW: usize = 3;
/// Rolling window length (1 hour).
pub const WINDOW_SECS: u64 = 3600;

/// Persisted reboot history (unix-seconds timestamps).
#[derive(Debug, Default, Serialize, Deserialize)]
struct RebootHistory {
    reboots: Vec<u64>,
}

/// File-backed reboot loop-guard.
pub struct RebootGuard {
    path: PathBuf,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl RebootGuard {
    /// Build a guard backed by `path` (created on first record).
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn load(&self) -> RebootHistory {
        fs::read_to_string(&self.path).map_or_else(
            |_| RebootHistory::default(),
            |json| serde_json::from_str(&json).unwrap_or_default(),
        )
    }

    fn save(&self, h: &RebootHistory) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json =
            serde_json::to_string(h).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(&self.path, json)
    }

    /// Try to consume a reboot slot. Returns `true` (and records the reboot)
    /// when under the limit within the window; `false` (parked) otherwise.
    /// Prunes timestamps older than the window on every call.
    pub fn try_reboot(&self, now: u64) -> bool {
        let mut h = self.load();
        let cutoff = now.saturating_sub(WINDOW_SECS);
        h.reboots.retain(|&t| t >= cutoff);
        if h.reboots.len() >= MAX_PER_WINDOW {
            tracing::error!(
                count = h.reboots.len(),
                "reboot loop-guard PARKED — {MAX_PER_WINDOW} reboots within the window, refusing further reboots"
            );
            // Persist the pruned history even on a park so the window keeps moving.
            let _ = self.save(&h);
            return false;
        }
        h.reboots.push(now);
        if let Err(e) = self.save(&h) {
            tracing::warn!(error = %e, "reboot guard persist failed");
        }
        true
    }

    /// Convenience over `try_reboot(now_unix())`.
    pub fn try_reboot_now(&self) -> bool {
        self.try_reboot(now_unix())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn allows_up_to_max_then_parks() {
        let dir = TempDir::new().unwrap();
        let g = RebootGuard::new(dir.path().join("reboots.json"));
        let t = 1_000_000;
        for _ in 0..MAX_PER_WINDOW {
            assert!(g.try_reboot(t), "within-limit reboot must be allowed");
        }
        assert!(
            !g.try_reboot(t),
            "the {MAX_PER_WINDOW}+1-th reboot must be parked"
        );
    }

    #[test]
    fn window_rolls_off_old_reboots() {
        let dir = TempDir::new().unwrap();
        let g = RebootGuard::new(dir.path().join("reboots.json"));
        let t0 = 1_000_000;
        for _ in 0..MAX_PER_WINDOW {
            assert!(g.try_reboot(t0));
        }
        assert!(!g.try_reboot(t0), "parked at the limit");
        // Far past the window → old reboots prune, a slot frees.
        let later = t0 + WINDOW_SECS + 1;
        assert!(g.try_reboot(later), "after the window the guard allows again");
    }

    #[test]
    fn guard_survives_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("reboots.json");
        let t = 5_000_000;
        for _ in 0..MAX_PER_WINDOW {
            assert!(RebootGuard::new(path.clone()).try_reboot(t));
        }
        // A fresh guard from the SAME file is still parked (history persisted).
        assert!(!RebootGuard::new(path).try_reboot(t));
    }
}
