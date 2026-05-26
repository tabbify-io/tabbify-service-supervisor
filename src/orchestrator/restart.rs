//! Pure restart-policy state machine: exponential backoff + crash-loop
//! detection for the supervisor's runner monitor.
//!
//! No I/O, no async. The wall-clock is an injected `now_secs: u64` parameter
//! so tests are deterministic and the module stays trivially unit-testable.

use serde::{Deserialize, Serialize};

// ─── Parameters ──────────────────────────────────────────────────────────────

/// Tunable backoff / crash-loop parameters.
///
/// The defaults match the spec:
/// - 10 s base delay, doubled per failure, capped at 300 s (5 min).
/// - A runner is considered "stable" once it has been healthy for 60 s.
/// - 5 consecutive failures without a stable window → crash-loop.
#[derive(Debug, Clone, Copy)]
pub struct BackoffParams {
    /// Base delay in seconds for the first failure (before doubling).
    pub base_secs: u64,
    /// Maximum delay in seconds; no retry will wait longer than this.
    pub cap_secs: u64,
    /// Seconds a runner must stay alive after its last exit to be considered
    /// stable and have its failure counter reset.
    pub stable_secs: u64,
    /// Number of consecutive failures that triggers the crash-loop state.
    pub crashloop_threshold: u32,
}

impl Default for BackoffParams {
    fn default() -> Self {
        Self {
            base_secs: 10,
            cap_secs: 300,
            stable_secs: 60,
            crashloop_threshold: 5,
        }
    }
}

// ─── Per-runner state ─────────────────────────────────────────────────────────

/// Per-runner restart state.
///
/// This is stored inside the runner record (persisted to disk later). All
/// fields are `u64`/`u32` so the type is `Copy` and `Default`-zero is a clean
/// "never failed, never exited" sentinel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartState {
    /// Number of consecutive exits without a stable-health window in between.
    pub consecutive_failures: u32,
    /// Unix timestamp of the most recent exit (0 = never exited).
    pub last_exit_at: u64,
    /// Earliest Unix timestamp at which it is safe to respawn again.
    pub next_retry_at: u64,
    /// Unix timestamp of the most recent healthy observation (0 = never seen healthy).
    pub last_healthy_at: u64,
}

// ─── Status ───────────────────────────────────────────────────────────────────

/// Coarse lifecycle status derived from [`RestartState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartStatus {
    /// Running normally (no recorded failures).
    Running,
    /// Failing, but below the crash-loop threshold — will be retried.
    Backoff,
    /// Exceeded the crash-loop threshold — intervention required.
    CrashLoop,
}

// ─── Pure functions ───────────────────────────────────────────────────────────

/// Compute the next backoff delay in seconds for a given failure count.
///
/// - `failures == 0` → `0` (no delay needed before a first-ever start).
/// - Otherwise → `min(base * 2^(failures-1), cap)`, saturating on overflow.
pub fn backoff_delay(failures: u32, p: BackoffParams) -> u64 {
    if failures == 0 {
        return 0;
    }
    // Compute base * 2^(failures-1) with overflow protection.
    // `checked_shl` can silently yield 0 for large shifts on some targets
    // (shift ≥ bit-width is undefined / masked in hardware), so we use
    // repeated `checked_mul` instead: multiply by 2 `failures-1` times and
    // fall back to `u64::MAX` the moment any multiplication overflows.
    let mut delay = p.base_secs;
    for _ in 0..(failures - 1) {
        delay = match delay.checked_mul(2) {
            Some(v) => v,
            None => {
                delay = u64::MAX;
                break;
            }
        };
        if delay >= p.cap_secs {
            // Short-circuit: already at or above the cap, no point continuing.
            break;
        }
    }
    delay.min(p.cap_secs)
}

/// Record a crash: increment the failure counter and schedule the next retry.
///
/// `last_healthy_at` is preserved unchanged — it reflects the last *healthy*
/// observation and is unaffected by subsequent failures.
pub fn on_exit(state: RestartState, p: BackoffParams, now: u64) -> RestartState {
    let new_failures = state.consecutive_failures.saturating_add(1);
    let delay = backoff_delay(new_failures, p);
    RestartState {
        consecutive_failures: new_failures,
        last_exit_at: now,
        next_retry_at: now.saturating_add(delay),
        last_healthy_at: state.last_healthy_at,
    }
}

/// Record a healthy observation.
///
/// If the runner has been healthy for long enough (≥ `stable_secs` since the
/// last exit, or has never exited at all), the failure counter is reset.
/// Otherwise the failure counter is preserved but `last_healthy_at` is updated.
pub fn on_healthy(state: RestartState, p: BackoffParams, now: u64) -> RestartState {
    let never_exited = state.last_exit_at == 0;
    let stable = now.saturating_sub(state.last_exit_at) >= p.stable_secs;

    if never_exited || stable {
        RestartState {
            last_healthy_at: now,
            ..Default::default()
        }
    } else {
        RestartState {
            last_healthy_at: now,
            ..state
        }
    }
}

/// Return `true` when the next retry window has arrived or passed.
pub fn should_respawn(state: RestartState, now: u64) -> bool {
    now >= state.next_retry_at
}

/// Derive the coarse lifecycle status from the current state.
///
/// The `now` parameter is accepted for future time-aware refinements (e.g.
/// distinguishing "backoff window expired" from "still waiting").
pub fn status(state: RestartState, p: BackoffParams, _now: u64) -> RestartStatus {
    if state.consecutive_failures == 0 {
        RestartStatus::Running
    } else if state.consecutive_failures >= p.crashloop_threshold {
        RestartStatus::CrashLoop
    } else {
        RestartStatus::Backoff
    }
}

/// Reset a runner's restart state unconditionally (e.g. after a manual restart
/// command clears the crash-loop flag).
pub fn reset(_state: RestartState) -> RestartState {
    RestartState::default()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> BackoffParams {
        BackoffParams::default()
    }

    // ── backoff_delay ─────────────────────────────────────────────────────────

    #[test]
    fn backoff_delay_zero_failures_is_zero() {
        assert_eq!(backoff_delay(0, p()), 0);
    }

    #[test]
    fn backoff_delay_one_failure_is_base() {
        assert_eq!(backoff_delay(1, p()), 10);
    }

    #[test]
    fn backoff_delay_two_failures_is_doubled() {
        assert_eq!(backoff_delay(2, p()), 20);
    }

    #[test]
    fn backoff_delay_three_failures_is_40() {
        assert_eq!(backoff_delay(3, p()), 40);
    }

    #[test]
    fn backoff_delay_five_failures_is_160() {
        assert_eq!(backoff_delay(5, p()), 160);
    }

    #[test]
    fn backoff_delay_ten_failures_is_capped() {
        // 10 * 2^9 = 5120 > 300 → capped to 300.
        assert_eq!(backoff_delay(10, p()), 300);
    }

    #[test]
    fn backoff_delay_64_does_not_panic() {
        // 2^63 overflows u64; must saturate and return the cap.
        assert_eq!(backoff_delay(64, p()), 300);
    }

    // ── on_exit ───────────────────────────────────────────────────────────────

    #[test]
    fn on_exit_first_failure_from_default() {
        let s = on_exit(RestartState::default(), p(), 1000);
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.last_exit_at, 1000);
        assert_eq!(s.next_retry_at, 1010); // 1000 + 10
    }

    #[test]
    fn on_exit_second_failure_doubles_delay() {
        let s1 = on_exit(RestartState::default(), p(), 1000);
        let s2 = on_exit(s1, p(), 1010);
        assert_eq!(s2.consecutive_failures, 2);
        assert_eq!(s2.last_exit_at, 1010);
        assert_eq!(s2.next_retry_at, 1030); // 1010 + 20
    }

    // ── on_healthy ────────────────────────────────────────────────────────────

    #[test]
    fn on_healthy_resets_when_stable() {
        // Set up a state with some failures, last exit 200 s ago.
        let state = RestartState {
            consecutive_failures: 3,
            last_exit_at: 900,
            next_retry_at: 940,
            last_healthy_at: 0,
        };
        // now=960, stable_secs=60, 960-900=60 ≥ 60 → reset.
        let s = on_healthy(state, p(), 960);
        assert_eq!(s.consecutive_failures, 0);
        assert_eq!(s.last_healthy_at, 960);
    }

    #[test]
    fn on_healthy_preserves_failures_when_not_yet_stable() {
        let state = RestartState {
            consecutive_failures: 3,
            last_exit_at: 1000,
            next_retry_at: 1040,
            last_healthy_at: 0,
        };
        // now=1010, 1010-1000=10 < 60 → preserve failures, update last_healthy_at.
        let s = on_healthy(state, p(), 1010);
        assert_eq!(s.consecutive_failures, 3);
        assert_eq!(s.last_healthy_at, 1010);
    }

    // ── should_respawn ────────────────────────────────────────────────────────

    #[test]
    fn should_respawn_false_before_window() {
        let state = RestartState {
            next_retry_at: 2000,
            ..Default::default()
        };
        assert!(!should_respawn(state, 1999));
    }

    #[test]
    fn should_respawn_true_at_window() {
        let state = RestartState {
            next_retry_at: 2000,
            ..Default::default()
        };
        assert!(should_respawn(state, 2000));
    }

    #[test]
    fn should_respawn_true_past_window() {
        let state = RestartState {
            next_retry_at: 2000,
            ..Default::default()
        };
        assert!(should_respawn(state, 2001));
    }

    // ── status ────────────────────────────────────────────────────────────────

    #[test]
    fn status_running_when_no_failures() {
        let s = RestartState::default();
        assert_eq!(status(s, p(), 0), RestartStatus::Running);
    }

    #[test]
    fn status_backoff_for_one_to_four_failures() {
        for f in 1u32..=4 {
            let s = RestartState {
                consecutive_failures: f,
                ..Default::default()
            };
            assert_eq!(status(s, p(), 0), RestartStatus::Backoff, "failures={f}");
        }
    }

    #[test]
    fn status_crash_loop_at_threshold() {
        let s = RestartState {
            consecutive_failures: 5,
            ..Default::default()
        };
        assert_eq!(status(s, p(), 0), RestartStatus::CrashLoop);
    }

    #[test]
    fn status_crash_loop_above_threshold() {
        let s = RestartState {
            consecutive_failures: 6,
            ..Default::default()
        };
        assert_eq!(status(s, p(), 0), RestartStatus::CrashLoop);
    }
}
