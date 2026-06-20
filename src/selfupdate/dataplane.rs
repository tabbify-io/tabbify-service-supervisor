//! Data-plane revert debounce (Track D, D2): require K consecutive dead
//! data-plane observations before a revert, decoupled from the 45s control
//! window. MSI is WAN-limited and its tunnel flaps; a single missed decap-RX
//! must never roll a healthy build back. A live observation resets the streak.

/// How many CONSECUTIVE dead-data-plane observations force a revert. 3 keeps a
/// single WAN flap from thrashing a healthy build while still catching a true
/// black hole within the data-plane confirm window.
pub const DATA_PLANE_REVERT_STREAK: u32 = 3;

/// Consecutive-dead-poll counter for the data-plane revert (D2). `observe`
/// returns `true` the moment the streak reaches [`DATA_PLANE_REVERT_STREAK`]
/// (and stays `true` on subsequent dead polls); any live observation resets the
/// streak to zero. Pure + cheap, so the watchdog can re-evaluate every tick.
#[derive(Debug, Default, Clone, Copy)]
pub struct DataPlaneDebounce {
    consecutive_dead: u32,
}

impl DataPlaneDebounce {
    /// Record one data-plane observation. `live=false` extends the dead streak;
    /// `live=true` resets it. Returns whether the revert is now armed (streak
    /// reached the threshold).
    pub fn observe(&mut self, live: bool) -> bool {
        if live {
            self.consecutive_dead = 0;
        } else {
            self.consecutive_dead = self.consecutive_dead.saturating_add(1);
        }
        self.armed()
    }

    /// Whether the consecutive-dead streak has reached the revert threshold.
    #[must_use]
    pub fn armed(&self) -> bool {
        self.consecutive_dead >= DATA_PLANE_REVERT_STREAK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A live observation resets the streak; only K=3 CONSECUTIVE dead polls
    /// arm the revert.
    #[test]
    fn streak_arms_only_after_k_consecutive_dead_polls() {
        let mut d = DataPlaneDebounce::default();
        assert!(!d.observe(false), "1 dead poll must not arm");
        assert!(!d.observe(false), "2 dead polls must not arm");
        // A single live poll resets the streak — the flap is forgiven.
        assert!(!d.observe(true), "live poll resets, must not arm");
        assert!(!d.observe(false), "streak restarts at 1");
        assert!(!d.observe(false), "2");
        assert!(d.observe(false), "3 consecutive dead polls arm the revert");
    }

    /// Once armed it STAYS armed (the watchdog acts on the first armed tick;
    /// idempotent if polled again before it reverts).
    #[test]
    fn stays_armed_once_streak_reached() {
        let mut d = DataPlaneDebounce::default();
        for _ in 0..DATA_PLANE_REVERT_STREAK {
            d.observe(false);
        }
        assert!(d.armed(), "must report armed after the streak");
        assert!(d.observe(false), "still armed on a further dead poll");
    }

    /// A LIVE data plane never arms regardless of how long we poll.
    #[test]
    fn never_arms_while_live() {
        let mut d = DataPlaneDebounce::default();
        for _ in 0..10 {
            assert!(!d.observe(true));
        }
        assert!(!d.armed());
    }
}
