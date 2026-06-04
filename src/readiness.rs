//! systemd readiness notification (`sd_notify(READY=1)`).
//!
//! The NixOS unit (`nixos/tabbify-node.nix`) runs supervisord as
//! `Type=notify` with `TimeoutStartSec=60`. Such a unit is considered "started"
//! ONLY once the main process tells systemd it is ready via `sd_notify(READY=1)`.
//! Without that signal systemd waits the full timeout, declares the start a
//! failure, and (with `Restart=on-failure`) keeps retrying — so a self-update
//! restart would brick the node. This module emits that readiness signal
//! EXACTLY ONCE, right after the listener is bound and (when meshed) the mesh is
//! joined, i.e. when the supervisor is genuinely able to serve.
//!
//! Best-effort by design: the `sd-notify` crate is a no-op (returns `Ok`) when
//! `NOTIFY_SOCKET` is unset, which is the case for dev / `--no-mesh` /
//! non-systemd runs — those are completely unaffected. Any real send error
//! under systemd is logged and swallowed: a failed readiness ping must never
//! abort an otherwise-healthy supervisor.
//!
//! # Testability
//!
//! The actual socket send is hidden behind a [`Notifier`] seam so the
//! best-effort contract is unit-testable without a live `NOTIFY_SOCKET`. The
//! production path ([`notify_ready`]) wires the real `sd_notify::notify`. The
//! end-to-end behavior (systemd actually marking the unit active) is only
//! verifiable live under systemd — see the crate-level docs and the NixOS unit.

use std::io;

use sd_notify::NotifyState;

/// The outcome of a readiness emission. Returned so callers / tests can observe
/// what happened without the emission ever failing the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyOutcome {
    /// The readiness state was handed to the notifier without error. Under
    /// systemd this means `READY=1` was sent; with no `NOTIFY_SOCKET` the
    /// underlying crate no-ops and still reports success.
    Notified,
    /// The notifier returned an error (a real send failure under systemd). It
    /// was logged and swallowed — the supervisor keeps running.
    Failed,
}

/// The readiness-notification seam: something that can hand a set of
/// [`NotifyState`]s to the service manager. The real implementation is
/// `sd_notify::notify`; tests inject a closure to exercise the best-effort
/// error handling without a live `NOTIFY_SOCKET`.
pub trait Notifier {
    /// Send the given states to the service manager.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if the send fails. With no
    /// `NOTIFY_SOCKET` the real implementation returns `Ok(())` (no-op).
    fn notify(&self, state: &[NotifyState]) -> io::Result<()>;
}

/// The production notifier: delegates to `sd_notify::notify`, which no-ops when
/// `NOTIFY_SOCKET` is unset (dev / `--no-mesh` / non-systemd).
struct SdNotifier;

impl Notifier for SdNotifier {
    fn notify(&self, state: &[NotifyState]) -> io::Result<()> {
        sd_notify::notify(state)
    }
}

/// Emit systemd readiness (`READY=1`) EXACTLY ONCE via the real `sd_notify`.
///
/// Call this from `main` precisely once, after the control listener is bound and
/// (when meshed) the mesh is joined, immediately before `axum::serve`. Best-
/// effort: a no-op when `NOTIFY_SOCKET` is unset, and any real error is logged
/// and swallowed (never propagated).
pub fn notify_ready() -> ReadyOutcome {
    emit_ready(&SdNotifier)
}

/// Emit readiness through an arbitrary [`Notifier`] (the seam). Logs and
/// swallows any error so the caller can never be failed by the readiness ping.
/// Returns the observed [`ReadyOutcome`].
pub fn emit_ready<N: Notifier>(notifier: &N) -> ReadyOutcome {
    match notifier.notify(&[NotifyState::Ready]) {
        Ok(()) => {
            tracing::debug!("sd_notify(READY=1) sent (no-op if not under systemd)");
            ReadyOutcome::Notified
        }
        Err(error) => {
            // Best-effort: a failed readiness ping must not abort a healthy
            // supervisor. Under systemd this is unexpected; log and continue.
            tracing::warn!(%error, "sd_notify(READY=1) failed; continuing (best-effort)");
            ReadyOutcome::Failed
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A test seam that records how many times it was called and returns a
    /// configurable result, so we can assert "exactly once" and best-effort.
    struct SpyNotifier {
        result: fn() -> io::Result<()>,
        calls: Cell<usize>,
        last_states: Cell<Option<bool>>, // Some(contains_ready)
    }

    impl SpyNotifier {
        fn new(result: fn() -> io::Result<()>) -> Self {
            Self {
                result,
                calls: Cell::new(0),
                last_states: Cell::new(None),
            }
        }
    }

    impl Notifier for SpyNotifier {
        fn notify(&self, state: &[NotifyState]) -> io::Result<()> {
            self.calls.set(self.calls.get() + 1);
            let contains_ready = state.iter().any(|s| matches!(s, NotifyState::Ready));
            self.last_states.set(Some(contains_ready));
            (self.result)()
        }
    }

    #[test]
    fn emit_ready_sends_ready_state_once_on_success() {
        let spy = SpyNotifier::new(|| Ok(()));
        let outcome = emit_ready(&spy);
        assert_eq!(outcome, ReadyOutcome::Notified);
        assert_eq!(spy.calls.get(), 1, "readiness must be emitted exactly once");
        assert_eq!(
            spy.last_states.get(),
            Some(true),
            "the emitted state set must contain NotifyState::Ready"
        );
    }

    #[test]
    fn emit_ready_swallows_error_and_does_not_propagate() {
        // A real send failure (e.g. under systemd with a broken socket) must be
        // swallowed: the function returns Failed, never panics, never propagates.
        let spy = SpyNotifier::new(|| {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no NOTIFY_SOCKET",
            ))
        });
        let outcome = emit_ready(&spy);
        assert_eq!(outcome, ReadyOutcome::Failed);
        assert_eq!(spy.calls.get(), 1, "still emitted exactly once on failure");
    }

    /// The real `sd_notify::notify` is a no-op returning `Ok(())` when
    /// `NOTIFY_SOCKET` is unset. This guards the "dev / --no-mesh / non-systemd
    /// runs are unaffected" contract: with the env var absent the production
    /// path reports success and never errors. The full systemd round-trip is
    /// only verifiable live (documented at the module level).
    #[test]
    fn notify_ready_is_a_noop_without_notify_socket() {
        // SAFETY: single-threaded unit test; we remove the env var (if any) to
        // model a non-systemd run, then restore nothing because it was absent.
        let had = std::env::var_os("NOTIFY_SOCKET");
        // Only assert the no-op invariant when the var is genuinely absent, so
        // we never tamper with a real systemd socket in a hosted CI runner.
        if had.is_none() {
            let outcome = notify_ready();
            assert_eq!(
                outcome,
                ReadyOutcome::Notified,
                "without NOTIFY_SOCKET sd_notify no-ops and reports success"
            );
        }
    }
}
