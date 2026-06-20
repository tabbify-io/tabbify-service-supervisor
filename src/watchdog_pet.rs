//! Track B tier-1: the watchdog-pet decision core (pure) + the live pet task.

use std::time::Duration;

/// What the pet loop should do this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PetAction {
    /// Data plane is healthy — send `sd_notify(WATCHDOG=1)`.
    Pet,
    /// Data plane is a sustained black hole — WITHHOLD the pet so systemd
    /// SIGKILLs + restarts the unit for a fresh handshake.
    Skip,
}

/// The pet cadence: half the systemd `WatchdogSec` (spec: pet every `W/2`), so a
/// single skipped pet still leaves a full half-window of slack before systemd
/// fires. Floored at 1ms so a (pathologically tiny) window never yields a
/// zero-duration sleep (a busy loop).
#[must_use]
pub fn pet_interval(watchdog_window: Duration) -> Duration {
    (watchdog_window / 2).max(Duration::from_millis(1))
}

/// Pure pet decision: pet iff the data plane is healthy. The black-hole case
/// (`false`) withholds the pet — the only path that ever lets systemd kill us.
#[must_use]
pub const fn decide_pet(data_plane_healthy: bool) -> PetAction {
    if data_plane_healthy {
        PetAction::Pet
    } else {
        PetAction::Skip
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn pet_interval_is_half_the_watchdog_window() {
        // W/2 with flooring; never zero even for a tiny window.
        assert_eq!(
            pet_interval(Duration::from_secs(120)),
            Duration::from_secs(60)
        );
        assert_eq!(
            pet_interval(Duration::from_millis(1)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn healthy_data_plane_pets() {
        assert_eq!(decide_pet(true), PetAction::Pet);
    }

    #[test]
    fn dead_data_plane_withholds_the_pet() {
        // The ONLY way the unit ever gets killed: a sustained black hole makes
        // dataplane_healthy() false, so we withhold WATCHDOG=1 and let systemd
        // SIGKILL+restart the unit for a fresh handshake.
        assert_eq!(decide_pet(false), PetAction::Skip);
    }
}
