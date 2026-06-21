//! Track B tier-1 ESCALATING RESTART BACKOFF (B-fix-2).
//!
//! Tier-1 ([`super`]) withholds the systemd pet on a sustained data-plane black
//! hole, so systemd SIGKILL+restarts the unit for a fresh handshake. On its own
//! that is unbounded: MSI's chronically-flaky WAN can keep the data plane
//! black-holed across restart after restart, and the tier-1 self-restart would
//! then TIGHT-LOOP — a restart every `WatchdogSec` (~120s) forever, never giving
//! a slow fresh handshake the room to converge.
//!
//! This module enforces a BACKOFF FLOOR between successive black-hole-driven
//! tier-1 restarts. A persisted attempt counter (`<data_dir>/self-heal/
//! restart-attempts.json`, the same durable-sidecar pattern as the tier-2
//! [`super::tier2::DeadStreak`]) gates the withhold path: the pet may not be
//! withheld until the process has been up at least [`backoff_floor`] for the
//! current attempt count. Leo's default: attempt #0 is INSTANT (a one-off wedge
//! recovers in ~120s — no point delaying the first self-restart); the floor only
//! ramps from the 2nd consecutive black-hole kill.
//!
//! Distinct sidecars, distinct jobs:
//! * tier-1 backoff ([`RestartAttempts`] here) — RATE-limits the tier-1 *process*
//!   restart (this file).
//! * tier-2 [`super::tier2::DeadStreak`] — TRIGGERS a *host reboot* after N
//!   consecutive dead restarts.
//! * [`crate::mesh_command::reboot_guard::RebootGuard`] — host-wide ≤3/hr reboot
//!   LIMITER (tier-2 / Track-C).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// First backoff floor (the 2nd consecutive black-hole kill). Doubles each
/// further consecutive kill up to [`BACKOFF_MAX`].
pub const BACKOFF_BASE: Duration = Duration::from_secs(30);
/// Cap on the backoff floor — a wedged WAN must still retry periodically.
pub const BACKOFF_MAX: Duration = Duration::from_secs(600);
/// Sustained-healthy uptime after which the attempt counter resets to zero (so a
/// node that genuinely recovered starts its NEXT incident from an instant kill).
pub const HEALTHY_RESET: Duration = Duration::from_secs(600);

/// Persisted tier-1 restart-attempt counter (`<data_dir>/self-heal/
/// restart-attempts.json`). `attempts` counts CONSECUTIVE black-hole-driven
/// tier-1 restarts; `window_start_micros` stamps when the current count began
/// (the unix-micros of the boot that opened it) so a sustained-healthy boot can
/// reset the count once [`HEALTHY_RESET`] has elapsed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RestartAttempts {
    /// Consecutive black-hole-driven tier-1 restarts.
    pub attempts: u32,
    /// Unix-micros marking the start of the current attempt window.
    pub window_start_micros: i64,
}

/// Path of the tier-1 restart-attempts sidecar under `data_dir`.
#[must_use]
pub fn restart_attempts_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join("self-heal")
        .join("restart-attempts.json")
}

/// Load the persisted attempts (missing/corrupt ⇒ a fresh zero state — NEVER
/// panic on a corrupt sidecar; defaulting to `attempts = 0` only ever lets the
/// next black-hole kill happen INSTANTLY, the fail-safe direction).
#[must_use]
pub fn load_restart_attempts(path: &Path) -> RestartAttempts {
    fs::read_to_string(path).map_or_else(
        |_| RestartAttempts::default(),
        |json| serde_json::from_str(&json).unwrap_or_default(),
    )
}

/// Persist the attempts (best-effort; a write failure is logged by the caller).
///
/// # Errors
/// Propagates a directory-create or write I/O error.
pub fn save_restart_attempts(path: &Path, state: RestartAttempts) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json =
        serde_json::to_string(&state).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    fs::write(path, json)
}

/// The minimum uptime that must elapse before a tier-1 withhold (self-restart)
/// is permitted, given how many consecutive black-hole restarts already
/// happened. PURE — no clock inside (the caller threads uptime).
///
/// * `attempts == 0` ⇒ `0` (INSTANT — Leo default: the first black-hole kill is
///   never delayed; a one-off wedge recovers in ~one `WatchdogSec`).
/// * `attempts >= 1` ⇒ `min(BACKOFF_BASE * 2^(attempts-1), BACKOFF_MAX)`
///   (30s, 60s, 120s, 240s, 480s, 600s, 600s, …).
///
/// Saturating throughout: a pathologically large `attempts` clamps to
/// [`BACKOFF_MAX`] without overflow.
#[must_use]
pub fn backoff_floor(attempts: u32) -> Duration {
    if attempts == 0 {
        return Duration::ZERO;
    }
    // Shift exponent: attempts=1 ⇒ 2^0, attempts=2 ⇒ 2^1, … Clamp the exponent
    // so the shift never overflows; anything past the cap saturates anyway.
    let exp = attempts - 1;
    // 2^exp seconds-multiplier as u64, saturating: once exp >= 64 it is already
    // far past BACKOFF_MAX, so treat as the max multiplier.
    let mult: u64 = if exp >= 64 { u64::MAX } else { 1u64 << exp };
    let secs = BACKOFF_BASE.as_secs().saturating_mul(mult);
    Duration::from_secs(secs).min(BACKOFF_MAX)
}

/// Whether the tier-1 withhold (self-restart) may fire this tick: the process
/// must have been up at least the [`backoff_floor`] for the current attempt
/// count. PURE — `uptime` is threaded in (no `Instant::now` here). When the
/// floor has NOT yet elapsed the caller keeps petting (suppresses the kill)
/// instead, so a flaky-WAN node cannot tight-loop restarts faster than the floor.
#[must_use]
pub fn withhold_allowed(uptime: Duration, attempts: u32) -> bool {
    uptime >= backoff_floor(attempts)
}

/// Fold the boot observation into the persisted attempt state. PURE — the clock
/// (`now_micros`) is threaded in.
///
/// Driven once per boot by the tier-2 grace check (which already samples the data
/// plane after a fresh-handshake grace window):
///
/// * `black_hole_restart == true` (this boot found the data plane STILL dead
///   after the grace window — i.e. the previous tier-1 restart did NOT heal it):
///   the previous restart was a black-hole-driven tier-1 kill that failed, so
///   roll the count forward (+1) and re-stamp the window.
/// * `black_hole_restart == false` AND the window has been open at least
///   [`HEALTHY_RESET`]: the node has been demonstrably healthy long enough —
///   RESET to zero so the NEXT incident starts from an instant kill.
/// * otherwise: hold the count as-is (healthy but not yet past `HEALTHY_RESET`,
///   or no window open yet) — only re-stamp if there was no window.
///
/// Returns the new state (caller persists).
#[must_use]
pub fn fold_boot_attempts(
    prev: RestartAttempts,
    black_hole_restart: bool,
    now_micros: i64,
) -> RestartAttempts {
    if black_hole_restart {
        return RestartAttempts {
            attempts: prev.attempts.saturating_add(1),
            window_start_micros: now_micros,
        };
    }
    // Healthy boot. Reset only once sustained healthy past HEALTHY_RESET, judged
    // from the window start. A zero/absent window_start_micros (fresh sidecar)
    // means "no incident in progress" — keep attempts at 0 and stamp the window.
    if prev.attempts == 0 {
        return RestartAttempts {
            attempts: 0,
            window_start_micros: now_micros,
        };
    }
    let healthy_for_micros = now_micros.saturating_sub(prev.window_start_micros);
    let reset_after_micros = i64::try_from(HEALTHY_RESET.as_micros()).unwrap_or(i64::MAX);
    if healthy_for_micros >= reset_after_micros {
        RestartAttempts {
            attempts: 0,
            window_start_micros: now_micros,
        }
    } else {
        // Healthy but not yet long enough to clear the streak — leave the count
        // and its window untouched so the floor keeps ramping if it flaps again.
        prev
    }
}

/// Unix-micros now (the production clock for [`fold_boot_attempts`]). Mirrors the
/// self-clocking pattern in [`crate::mesh::MeshMembership::dataplane_healthy`].
#[must_use]
pub fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ── backoff_floor (the pure ladder, fully unit-tested like escalate_decision) ──

    #[test]
    fn backoff_floor_attempt_zero_is_instant() {
        // Leo default: the FIRST black-hole kill is never delayed.
        assert_eq!(backoff_floor(0), Duration::ZERO);
    }

    #[test]
    fn backoff_floor_ramps_from_the_second_kill() {
        assert_eq!(backoff_floor(1), Duration::from_secs(30)); // BACKOFF_BASE
        assert_eq!(backoff_floor(2), Duration::from_secs(60));
        assert_eq!(backoff_floor(3), Duration::from_secs(120));
        assert_eq!(backoff_floor(4), Duration::from_secs(240));
        assert_eq!(backoff_floor(5), Duration::from_secs(480));
    }

    #[test]
    fn backoff_floor_caps_at_max() {
        // 30*2^5 = 960 > 600 ⇒ clamps. And it STAYS clamped past that.
        assert_eq!(backoff_floor(6), Duration::from_secs(600)); // BACKOFF_MAX
        assert_eq!(backoff_floor(7), Duration::from_secs(600));
        assert_eq!(backoff_floor(100), Duration::from_secs(600));
    }

    #[test]
    fn backoff_floor_never_overflows_on_huge_attempts() {
        // The shift must not panic and must saturate to the cap.
        assert_eq!(backoff_floor(u32::MAX), BACKOFF_MAX);
        assert_eq!(backoff_floor(64), BACKOFF_MAX);
        assert_eq!(backoff_floor(63), BACKOFF_MAX);
    }

    // ── withhold_allowed (the gate) ────────────────────────────────────────────

    #[test]
    fn withhold_always_allowed_on_first_attempt() {
        // attempts=0 ⇒ floor 0 ⇒ even a 0s-uptime withhold (instant self-restart).
        assert!(withhold_allowed(Duration::ZERO, 0));
        assert!(withhold_allowed(Duration::from_secs(1), 0));
    }

    #[test]
    fn withhold_blocked_below_floor_then_allowed_at_floor() {
        // attempts=2 ⇒ floor 60s. Below ⇒ keep petting; at/above ⇒ may withhold.
        assert!(!withhold_allowed(Duration::from_secs(59), 2));
        assert!(withhold_allowed(Duration::from_secs(60), 2));
        assert!(withhold_allowed(Duration::from_secs(61), 2));
    }

    #[test]
    fn withhold_respects_the_cap() {
        // attempts past the cap ⇒ floor BACKOFF_MAX (600s).
        assert!(!withhold_allowed(Duration::from_secs(599), 100));
        assert!(withhold_allowed(Duration::from_secs(600), 100));
    }

    // ── fold_boot_attempts (increment / reset, clock threaded) ─────────────────

    const SEC: i64 = 1_000_000; // micros per second

    #[test]
    fn fold_increments_and_restamps_on_black_hole_boot() {
        let prev = RestartAttempts {
            attempts: 1,
            window_start_micros: 5 * SEC,
        };
        let next = fold_boot_attempts(prev, true, 100 * SEC);
        assert_eq!(next.attempts, 2, "a failed-restart boot rolls the count +1");
        assert_eq!(next.window_start_micros, 100 * SEC, "window re-stamped to now");
    }

    #[test]
    fn fold_holds_when_healthy_but_within_healthy_reset() {
        // attempts=3, window opened at t=0, now t=300s < HEALTHY_RESET(600s).
        let prev = RestartAttempts {
            attempts: 3,
            window_start_micros: 0,
        };
        let next = fold_boot_attempts(prev, false, 300 * SEC);
        assert_eq!(next, prev, "healthy but not yet 600s — count + window untouched");
    }

    #[test]
    fn fold_resets_when_healthy_past_healthy_reset() {
        // attempts=3, window opened at t=0, now t=600s == HEALTHY_RESET ⇒ reset.
        let prev = RestartAttempts {
            attempts: 3,
            window_start_micros: 0,
        };
        let next = fold_boot_attempts(prev, false, 600 * SEC);
        assert_eq!(next.attempts, 0, "sustained-healthy past HEALTHY_RESET clears the streak");
        assert_eq!(next.window_start_micros, 600 * SEC);
    }

    #[test]
    fn fold_on_fresh_zero_state_stays_zero_and_stamps() {
        // No incident in progress (attempts=0): a healthy boot just opens a window.
        let prev = RestartAttempts::default();
        let next = fold_boot_attempts(prev, false, 42 * SEC);
        assert_eq!(next.attempts, 0);
        assert_eq!(next.window_start_micros, 42 * SEC);
    }

    #[test]
    fn fold_clock_skew_never_panics() {
        // now < window_start (clock moved backwards) ⇒ saturating_sub ⇒ 0 elapsed
        // ⇒ treated as "not yet past HEALTHY_RESET" ⇒ hold (no panic, no reset).
        let prev = RestartAttempts {
            attempts: 2,
            window_start_micros: 1_000 * SEC,
        };
        let next = fold_boot_attempts(prev, false, 10 * SEC);
        assert_eq!(next, prev, "backwards clock holds the streak, never panics");
    }

    // ── sidecar round-trip + corrupt-file safety ───────────────────────────────

    #[test]
    fn restart_attempts_round_trips_through_the_sidecar() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = restart_attempts_path(dir.path());
        // missing file ⇒ zero
        assert_eq!(load_restart_attempts(&path), RestartAttempts::default());
        let state = RestartAttempts {
            attempts: 4,
            window_start_micros: 7 * SEC,
        };
        save_restart_attempts(&path, state).unwrap();
        assert_eq!(load_restart_attempts(&path), state);
    }

    #[test]
    fn corrupt_sidecar_defaults_to_zero_attempts_no_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = restart_attempts_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{ this is not json").unwrap();
        // No panic; defaults to instant-kill-eligible (attempts=0), the fail-safe.
        assert_eq!(load_restart_attempts(&path), RestartAttempts::default());
    }
}
