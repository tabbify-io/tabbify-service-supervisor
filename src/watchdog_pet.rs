//! Track B tier-1: the watchdog-pet decision core (pure) + the live pet task.

use std::io;
use std::time::Duration;

use sd_notify::NotifyState;

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

/// The pet seam: emit `sd_notify(WATCHDOG=1)` to systemd. Real impl delegates to
/// `sd_notify::notify`; tests inject a spy. Mirrors `readiness::Notifier`.
pub trait Petter {
    /// Send `WATCHDOG=1`. A no-op `Ok(())` off systemd (`NOTIFY_SOCKET` unset).
    ///
    /// # Errors
    /// The underlying I/O error on a real send failure under systemd.
    fn pet(&self) -> io::Result<()>;
}

/// Production petter: `sd_notify(&[WATCHDOG=1])`.
pub struct SdPetter;
impl Petter for SdPetter {
    fn pet(&self) -> io::Result<()> {
        sd_notify::notify(&[NotifyState::Watchdog])
    }
}

/// Outcome of applying a [`PetAction`] through a [`Petter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PetOutcome {
    /// `WATCHDOG=1` was sent.
    Petted,
    /// The pet was deliberately withheld (black hole) — systemd will restart us.
    Withheld,
    /// We tried to pet but the send failed; swallowed (best-effort), keep going.
    PetFailed,
}

/// Apply a decision through the petter. Best-effort: a send error is swallowed
/// (reported as [`PetOutcome::PetFailed`]) — a transient notify-socket hiccup
/// must NOT itself become a black-hole kill. A `Skip` never touches the socket.
#[must_use]
pub fn apply_decision<P: Petter + ?Sized>(action: PetAction, petter: &P) -> PetOutcome {
    match action {
        PetAction::Skip => PetOutcome::Withheld,
        PetAction::Pet => match petter.pet() {
            Ok(()) => PetOutcome::Petted,
            Err(_) => PetOutcome::PetFailed,
        },
    }
}

/// Gate a black-hole skip on the §3 invariant: we only ever WITHHOLD the pet
/// (let systemd kill the unit) when `relay_only` is still set — the restart
/// re-joins relay_only, never a silent flip to direct. If we somehow observe a
/// black hole while `relay_only` is OFF, that is a logic error: FAIL-OPEN (keep
/// petting) so we never weaponize the watchdog against a misconfigured-direct
/// node; the caller logs it loudly. A healthy node always pets.
#[must_use]
pub const fn guard_black_hole(data_plane_healthy: bool, relay_only: bool) -> PetAction {
    match decide_pet(data_plane_healthy) {
        PetAction::Pet => PetAction::Pet,
        PetAction::Skip if relay_only => PetAction::Skip,
        PetAction::Skip => PetAction::Pet, // fail-open: relay_only OFF ⇒ never kill
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io;
    use std::time::Duration;

    /// Spy petter: records pet count + that the emitted state is WATCHDOG=1.
    struct SpyPetter {
        result: fn() -> io::Result<()>,
        pets: Cell<usize>,
        saw_watchdog: Cell<bool>,
    }
    impl SpyPetter {
        fn new(result: fn() -> io::Result<()>) -> Self {
            Self {
                result,
                pets: Cell::new(0),
                saw_watchdog: Cell::new(false),
            }
        }
    }
    impl Petter for SpyPetter {
        fn pet(&self) -> io::Result<()> {
            self.pets.set(self.pets.get() + 1);
            self.saw_watchdog.set(true); // the only state we ever send is WATCHDOG=1
            (self.result)()
        }
    }

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

    #[test]
    fn apply_pets_when_healthy_and_skips_when_dead() {
        let spy = SpyPetter::new(|| Ok(()));
        // Healthy → pet once, report Pet.
        assert_eq!(apply_decision(PetAction::Pet, &spy), PetOutcome::Petted);
        assert_eq!(spy.pets.get(), 1);
        assert!(spy.saw_watchdog.get(), "must emit WATCHDOG=1");
        // Dead → no pet, report Withheld.
        assert_eq!(apply_decision(PetAction::Skip, &spy), PetOutcome::Withheld);
        assert_eq!(spy.pets.get(), 1, "a Skip must NOT pet");
    }

    #[test]
    fn apply_swallows_petter_error_as_best_effort() {
        // A failed pet must not panic/propagate; report PetFailed and keep going.
        let spy = SpyPetter::new(|| Err(io::Error::new(io::ErrorKind::NotConnected, "x")));
        assert_eq!(apply_decision(PetAction::Pet, &spy), PetOutcome::PetFailed);
        assert_eq!(spy.pets.get(), 1);
    }

    #[test]
    fn black_hole_requires_relay_only_to_still_be_set() {
        // Invariant §3: when we are about to let systemd kill us, relay_only MUST
        // still be true — the restart re-joins relay_only, never flips to direct.
        // A black hole with relay_only somehow OFF is a logic bug → we REFUSE to
        // withhold (fail-open: keep petting) and the caller logs loudly.
        assert_eq!(
            guard_black_hole(false /*healthy*/, true /*relay_only*/),
            PetAction::Skip
        );
        assert_eq!(
            guard_black_hole(false /*healthy*/, false /*relay_only*/),
            PetAction::Pet,
            "black hole with relay_only OFF must NOT trigger a kill (fail-open)"
        );
        // A healthy node always pets regardless of relay_only.
        assert_eq!(guard_black_hole(true, true), PetAction::Pet);
        assert_eq!(guard_black_hole(true, false), PetAction::Pet);
    }
}
