//! Track B self-heal watchdog. Tier-1: the watchdog-pet decision core (pure) +
//! the live pet task that pets systemd (`WATCHDOG=1`) every `W/2` ONLY while the
//! mesh data plane is healthy (Track-K `dataplane_healthy`). Tier-2 (in
//! [`tier2`]): reboot-on-persistent-failure, behind the SHARED Track-C reboot
//! loop-guard ([`crate::mesh_command::reboot_guard::RebootGuard`], ≤3/hr).

use std::io;
use std::path::Path;
use std::time::Duration;

use sd_notify::NotifyState;

pub mod tier1_backoff;
pub mod tier2;

pub use tier1_backoff::{
    BACKOFF_BASE, BACKOFF_MAX, HEALTHY_RESET, RestartAttempts, backoff_floor, fold_boot_attempts,
    load_restart_attempts, now_micros, restart_attempts_path, save_restart_attempts,
    withhold_allowed,
};
pub use tier2::{
    DeadStreak, REBOOT_AFTER_CONSECUTIVE_DEAD, RebootRunner, SystemctlReboot, TIER2_GRACE,
    Tier2Action, apply_tier2, dead_streak_path, escalate_decision, fold_observation,
    load_dead_streak, run_tier2_boot_check, save_dead_streak,
};

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

/// A data-plane liveness probe the live loop polls each tick. The supervisor
/// supplies [`crate::mesh::MeshMembership::data_plane_probe`] (a self-clocking
/// closure over the Track-K accessor); tests inject a fake. `Send + Sync` so the
/// closure can move into the spawned task.
pub type DataPlaneProbe = std::sync::Arc<dyn Fn() -> bool + Send + Sync>;

/// One pet tick: sample the data plane, gate on `relay_only` (§3), gate on the
/// ESCALATING RESTART BACKOFF (B-fix-2), and apply the decision through the
/// petter. Pure given its injected collaborators — the live loop is just this on
/// a `W/2` timer. The probe self-clocks (Track K reads the current monotonic
/// clock internally); `uptime` is threaded in (no `Instant::now` here) so the
/// backoff floor stays a pure function of injected state.
///
/// Backoff gate: even on a real black hole, the pet is only WITHHELD once the
/// process has been up at least [`tier1_backoff::backoff_floor`] for the current
/// consecutive-black-hole-restart count (`attempts`). Below the floor we keep
/// PETTING (suppress the kill) so a chronically-flaky WAN (MSI) cannot tight-loop
/// restarts faster than the floor. `attempts == 0` ⇒ floor 0 ⇒ the FIRST
/// black-hole kill is instant (Leo default).
#[must_use]
pub fn run_once<Pe: Petter + ?Sized>(
    probe: &dyn Fn() -> bool,
    petter: &Pe,
    relay_only: bool,
    uptime: Duration,
    attempts: u32,
) -> PetOutcome {
    let healthy = probe();
    let mut action = guard_black_hole(healthy, relay_only);
    if action == PetAction::Skip && !tier1_backoff::withhold_allowed(uptime, attempts) {
        // A real black hole, but the backoff floor for this attempt count has not
        // yet elapsed — SUPPRESS the kill (keep petting) so successive black-hole
        // restarts can't recur faster than the floor. Below the floor the unit
        // stays alive and keeps trying to converge on its own.
        tracing::warn!(
            relay_only,
            attempts,
            uptime_secs = uptime.as_secs(),
            backoff_floor_secs = tier1_backoff::backoff_floor(attempts).as_secs(),
            "watchdog-pet: black hole but within restart-backoff floor — \
             SUPPRESSING the self-restart this tick (still petting)"
        );
        action = PetAction::Pet;
    } else if action == PetAction::Skip {
        // About to let systemd SIGKILL+restart us — make the cause forensically
        // obvious in the journal (the restart itself otherwise looks unexplained).
        tracing::error!(
            relay_only,
            attempts,
            backoff_floor_secs = tier1_backoff::backoff_floor(attempts).as_secs(),
            "watchdog-pet: sustained data-plane black hole past the backoff floor — \
             WITHHOLDING WATCHDOG=1; systemd will restart the unit for a fresh \
             handshake (relay_only preserved)"
        );
    }
    apply_decision(action, petter)
}

/// Spawn the independent watchdog-pet task (tier-1) + the tier-2 boot-check.
/// No-op (returns `None`) when `watchdog_enabled()` is `None`: dev / `--no-mesh`
/// / not under systemd / no `WatchdogSec=` — those runs are NEVER watchdog-killed
/// (fail-open). Otherwise loops forever, petting every `W/2` while the data plane
/// is healthy, and ONCE per boot (after a grace window) folds a data-plane
/// observation into the persisted consecutive-dead streak, escalating to a host
/// reboot (tier-2) past [`REBOOT_AFTER_CONSECUTIVE_DEAD`] — behind the shared
/// ≤3/hr `RebootGuard`. The `probe` closure (`MeshMembership::data_plane_probe`)
/// holds a clone of the joiner handle so the data plane stays observable for the
/// process lifetime; `relay_only` is the static node policy (env-derived) so the
/// §3 assertion needs no extra plumbing. `data_dir` backs the persisted sidecars.
pub fn spawn_watchdog_pet(
    probe: DataPlaneProbe,
    relay_only: bool,
    data_dir: &Path,
) -> Option<tokio::task::JoinHandle<()>> {
    let Some(window) = sd_notify::watchdog_enabled() else {
        tracing::debug!("no systemd WatchdogSec (WATCHDOG_USEC unset) — watchdog-pet disabled");
        return None;
    };
    let interval = pet_interval(window);

    // ── B-fix-2 ESCALATING RESTART BACKOFF ───────────────────────────────────
    // The attempt count from PRIOR boots fixes the backoff floor for THIS
    // incarnation: how long this process must be up before it may withhold the
    // pet (self-restart) again. Loaded ONCE here (corrupt/missing ⇒ 0 ⇒ instant
    // kill eligible — the fail-safe direction). The tier-2 boot-check below
    // re-folds the sidecar for the NEXT boot.
    let attempts_path = restart_attempts_path(data_dir);
    let attempts = load_restart_attempts(&attempts_path).attempts;
    let backoff = backoff_floor(attempts);

    tracing::info!(
        watchdog_window_secs = window.as_secs(),
        pet_interval_secs = interval.as_secs(),
        prior_black_hole_restarts = attempts,
        restart_backoff_floor_secs = backoff.as_secs(),
        "watchdog-pet armed: petting every W/2 while data-plane healthy \
         (self-restart suppressed below the backoff floor)"
    );

    // Tier-2 boot-check: one-shot, after a grace window long enough for a fresh
    // post-restart handshake to converge. Spawned independently so a slow grace
    // never delays the pet. It ALSO folds the tier-1 restart-attempts sidecar
    // off the SAME post-grace data-plane sample: still-dead ⇒ the prior tier-1
    // restart failed ⇒ ++attempts (the floor ramps); healthy past HEALTHY_RESET
    // ⇒ reset to 0 (the next incident starts instant again).
    let tier2_probe = std::sync::Arc::clone(&probe);
    let data_dir = data_dir.to_path_buf();
    tokio::spawn(async move {
        tokio::time::sleep(TIER2_GRACE).await;
        let healthy = tier2_probe();
        // Fold the tier-1 backoff sidecar (still-dead ⇒ a black-hole restart just
        // failed). Best-effort persist; a corrupt/failed write only ever lets the
        // next kill happen sooner (fail-safe).
        let folded = fold_boot_attempts(
            load_restart_attempts(&attempts_path),
            !healthy,
            now_micros(),
        );
        if let Err(e) = save_restart_attempts(&attempts_path, folded) {
            tracing::warn!(error = %e, "watchdog tier-1: restart-attempts persist failed");
        }
        // Then run the tier-2 escalation off the SAME observation (re-samples the
        // probe internally; the grace window already elapsed so it is consistent).
        run_tier2_boot_check(
            tier2_probe.as_ref(),
            &dead_streak_path(&data_dir),
            &crate::mesh_command::reboot_guard::RebootGuard::new(
                crate::mesh_command::sink::reboot_history_path(&data_dir),
            ),
            &SystemctlReboot,
        );
    });

    let petter = SdPetter;
    let started = std::time::Instant::now();
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // First tick fires immediately: pet right away so the unit is fed before
        // the very first W elapses (we just reached READY).
        loop {
            tick.tick().await;
            let _ = run_once(
                probe.as_ref(),
                &petter,
                relay_only,
                started.elapsed(),
                attempts,
            );
        }
    }))
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

    #[test]
    fn run_once_pets_then_withholds_on_black_hole() {
        // A probe that flips healthy→dead after the first sample, so two ticks
        // exercise both branches. attempts=0 ⇒ floor 0 ⇒ the black hole withholds
        // INSTANTLY (Leo default: the first kill is never delayed).
        let calls = Cell::new(0u32);
        let probe = || {
            let n = calls.get();
            calls.set(n + 1);
            n == 0 // healthy on the first call, dead thereafter
        };
        let spy = SpyPetter::new(|| Ok(()));
        // First tick: healthy + relay_only ⇒ Petted.
        assert_eq!(
            run_once(&probe, &spy, true, Duration::ZERO, 0),
            PetOutcome::Petted
        );
        // Second tick: dead + relay_only + attempts=0 ⇒ Withheld (systemd restarts).
        assert_eq!(
            run_once(&probe, &spy, true, Duration::ZERO, 0),
            PetOutcome::Withheld
        );
        assert_eq!(spy.pets.get(), 1, "exactly one pet across the two ticks");
    }

    #[test]
    fn run_once_fails_open_when_dead_but_not_relay_only() {
        let probe = || false; // dead data plane
        let spy = SpyPetter::new(|| Ok(()));
        // Dead data plane but relay_only=false ⇒ fail-open ⇒ Petted (never kill).
        assert_eq!(
            run_once(&probe, &spy, false, Duration::from_secs(9999), 5),
            PetOutcome::Petted
        );
        assert_eq!(spy.pets.get(), 1);
    }

    #[test]
    fn run_once_suppresses_black_hole_kill_below_the_backoff_floor() {
        // A real black hole (dead + relay_only), but attempts=2 ⇒ floor 60s and we
        // are only 30s up ⇒ the kill is SUPPRESSED: we keep PETTING so successive
        // restarts cannot recur faster than the floor (the B-fix-2 guarantee).
        let probe = || false;
        let spy = SpyPetter::new(|| Ok(()));
        assert_eq!(
            run_once(&probe, &spy, true, Duration::from_secs(30), 2),
            PetOutcome::Petted,
            "below the 60s floor a black hole must NOT withhold — it pets"
        );
        assert_eq!(spy.pets.get(), 1, "suppression still pets the unit");
    }

    #[test]
    fn run_once_withholds_black_hole_once_above_the_backoff_floor() {
        // Same black hole, attempts=2 ⇒ floor 60s, now 60s up ⇒ the floor elapsed
        // ⇒ the withhold is finally allowed (self-restart fires).
        let probe = || false;
        let spy = SpyPetter::new(|| Ok(()));
        assert_eq!(
            run_once(&probe, &spy, true, Duration::from_secs(60), 2),
            PetOutcome::Withheld,
            "at/above the floor the black hole withholds (restart fires)"
        );
        assert_eq!(spy.pets.get(), 0, "a real withhold never pets");
    }

    #[test]
    fn run_once_first_attempt_is_instant_kill() {
        // attempts=0 ⇒ floor 0 ⇒ even a 0s-uptime black hole withholds instantly.
        let probe = || false;
        let spy = SpyPetter::new(|| Ok(()));
        assert_eq!(
            run_once(&probe, &spy, true, Duration::ZERO, 0),
            PetOutcome::Withheld,
            "the first black-hole kill is instant (no backoff on attempt #0)"
        );
    }
}
