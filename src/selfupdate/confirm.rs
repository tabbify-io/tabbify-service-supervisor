//! Post-restart self-watchdog (spec §7) — how the audited [`super::watchdog`]
//! actually runs in production.
//!
//! After a `self-update` swap ([`super::run::self_update_to`]) re-points the
//! symlinks and restarts the unit, the NEXT normal `supervisord` boot is running
//! an UNCONFIRMED binary. The swap recorded a pending-confirm marker in the
//! VERSION ledger ([`super::swap::mark_pending_confirm`]); on startup the binary
//! reads it and, if present, spawns [`super::watchdog::run_watchdog`] for the
//! stability window against the LIVE local supervisor:
//!   - healthy through the window  -> CLEAR the marker (confirm the swap),
//!   - any failure / crash-loop     -> [`super::watchdog::revert_to_previous`]
//!     (re-point to previous-good + restart).
//!
//! The live `/health` + `/v1/about` polling is process/network-dependent, so it
//! is hidden behind the [`super::watchdog::ObserveFn`] seam: production wires
//! [`live_local_observe`] (HTTP against the just-bound local control addr);
//! tests inject a closure. The marker read + clear is pure and unit-tested
//! against an on-disk ledger.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

use super::swap::{
    RestartRunner, clear_pending_confirm, pending_confirm_of, read_version_file, write_version_file,
};
use super::watchdog::{ObserveFn, WatchdogObservations, run_watchdog};
use crate::orchestrator::restart::RestartState;

/// The pending-confirm version recorded in `<install_dir>/VERSION`, if this boot
/// is running an UNCONFIRMED self-update swap. `None` in steady state or when no
/// ledger exists yet (a fresh / bash-bootstrapped install).
#[must_use]
pub fn pending_swap(install_dir: &Path) -> Option<String> {
    read_version_file(install_dir)
        .ok()
        .and_then(|vf| pending_confirm_of(&vf).map(str::to_owned))
}

/// Clear the pending-confirm marker in `<install_dir>/VERSION` (confirm a
/// healthy swap). Touches ONLY the VERSION ledger — never the symlinks.
///
/// # Errors
/// The ledger is missing/malformed, or the rewrite fails.
pub fn confirm_swap(install_dir: &Path) -> Result<()> {
    let vf = read_version_file(install_dir).context("read VERSION to confirm swap")?;
    let confirmed = clear_pending_confirm(vf);
    write_version_file(install_dir, &confirmed).context("write confirmed VERSION")
}

/// Drive the post-restart self-watchdog over a pending swap: run the audited
/// [`run_watchdog`] for `stability_window` using the injected `observe`/`restart`
/// seams, then ON HEALTHY clear the pending-confirm marker (confirm). On a
/// failure `run_watchdog` already performed the symlink rollback + restart, so
/// here we only log the rolled-back version and leave the marker as the
/// rollback wrote it (the revert clears it).
///
/// Returns the rolled-back version if a revert happened, else `None` (confirmed).
///
/// # Errors
/// A rollback failure from [`run_watchdog`], or a confirm-clear write failure.
pub async fn confirm_or_revert(
    install_dir: &Path,
    releases_dir: &Path,
    stability_window: Duration,
    poll_interval: Duration,
    observe: ObserveFn<'_>,
    restart: &RestartRunner,
) -> Result<Option<String>> {
    match run_watchdog(
        install_dir,
        releases_dir,
        stability_window,
        poll_interval,
        observe,
        restart,
    )
    .await?
    {
        // Healthy through the window: confirm by clearing the marker.
        None => {
            confirm_swap(install_dir)?;
            tracing::info!("post-swap watchdog confirmed the new version");
            Ok(None)
        }
        // Rolled back: revert_to_previous already restored the ledger (marker
        // cleared) + restarted; nothing more to do here.
        Some(rolled_back) => {
            tracing::warn!(%rolled_back, "post-swap watchdog reverted to previous-good");
            Ok(Some(rolled_back))
        }
    }
}

/// Build the production [`ObserveFn`] that samples the LIVE local supervisor:
/// `GET http://<local>/health` (gate part 2) + `GET http://<local>/v1/about`
/// (gate part 3, the distinct liveness route the candidate probe also uses).
/// Both must return 2xx for the observation to be healthy. The restart state is
/// not tracked in-process here (systemd owns restarts), so it is reported as
/// the default (no consecutive failures): a hard crash is caught by systemd's
/// own restart policy + the heartbeat-timeout, while THIS watchdog catches a
/// process that came up but cannot serve.
#[must_use]
pub fn live_local_observe(local: SocketAddr) -> ObserveFn<'static> {
    let client = reqwest::Client::new();
    Box::new(move || {
        let client = client.clone();
        Box::pin(async move {
            let health_200 = probe_2xx(&client, &format!("http://{local}/health")).await;
            let pong = probe_2xx(&client, &format!("http://{local}/v1/about")).await;
            WatchdogObservations {
                health_200,
                pong,
                // Overwritten by run_watchdog from the elapsed window.
                window_elapsed: false,
                restart: RestartState::default(),
            }
        })
    })
}

/// GET `url` and return whether it answered 2xx (any error / non-2xx => false).
async fn probe_2xx(client: &reqwest::Client, url: &str) -> bool {
    matches!(client.get(url).send().await, Ok(r) if r.status().is_success())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::selfupdate::swap::{
        SUPERVISOR_UNIT, SWAP_BINARIES, VersionFile, mark_pending_confirm, repoint_symlink,
    };

    fn recording_restart() -> (RestartRunner, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::default();
        let recorded = Arc::clone(&calls);
        let runner: RestartRunner = Arc::new(move |args: Vec<String>| {
            let recorded = Arc::clone(&recorded);
            Box::pin(async move {
                recorded.lock().unwrap().push(args);
                true
            })
        });
        (runner, calls)
    }

    fn stage_release(releases: &Path, version: &str) {
        use std::os::unix::fs::PermissionsExt;
        let dir = releases.join(version);
        std::fs::create_dir_all(&dir).unwrap();
        for bin in SWAP_BINARIES {
            let path = dir.join(bin);
            std::fs::write(&path, format!("{version}-{bin}")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    fn healthy_observe() -> ObserveFn<'static> {
        Box::new(|| {
            Box::pin(async {
                WatchdogObservations {
                    health_200: true,
                    pong: true,
                    window_elapsed: false,
                    restart: RestartState::default(),
                }
            })
        })
    }

    fn unhealthy_observe() -> ObserveFn<'static> {
        Box::new(|| {
            Box::pin(async {
                WatchdogObservations {
                    health_200: false, // came up but cannot serve -> revert
                    pong: true,
                    window_elapsed: false,
                    restart: RestartState::default(),
                }
            })
        })
    }

    /// `pending_swap` reads the marker the swap stamped, and `None` once cleared.
    #[test]
    fn pending_swap_reads_and_confirm_clears_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();

        // No ledger yet: no pending swap.
        assert_eq!(pending_swap(install), None);

        write_version_file(
            install,
            &mark_pending_confirm(
                VersionFile {
                    current: "v2.0.0".into(),
                    previous: vec!["v1.0.0".into()],
                    pending_confirm: None,
                },
                "v2.0.0",
            ),
        )
        .unwrap();
        assert_eq!(pending_swap(install), Some("v2.0.0".to_owned()));

        // Confirm clears it but leaves current + history intact.
        confirm_swap(install).unwrap();
        assert_eq!(pending_swap(install), None);
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0");
        assert_eq!(vf.previous, vec!["v1.0.0".to_owned()]);
    }

    /// Healthy through the window: the marker is CONFIRMED (cleared), no restart.
    #[tokio::test]
    async fn confirm_or_revert_confirms_when_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        write_version_file(
            install,
            &mark_pending_confirm(
                VersionFile {
                    current: "v2.0.0".into(),
                    previous: vec!["v1.0.0".into()],
                    pending_confirm: None,
                },
                "v2.0.0",
            ),
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let rolled = confirm_or_revert(
            install,
            &install.join("releases"),
            Duration::ZERO, // first poll already sees window_elapsed
            Duration::from_millis(1),
            healthy_observe(),
            &restart,
        )
        .await
        .unwrap();

        assert_eq!(rolled, None, "healthy window confirms, no rollback");
        assert_eq!(pending_swap(install), None, "marker cleared on confirm");
        assert!(calls.lock().unwrap().is_empty(), "no restart on confirm");
    }

    /// Unhealthy: the self-watchdog rolls back to previous-good (re-points the
    /// symlinks + restarts) and the marker is cleared by the revert.
    #[tokio::test]
    async fn confirm_or_revert_rolls_back_when_unhealthy() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        for ver in ["v1.0.0", "v2.0.0"] {
            stage_release(&releases, ver);
        }
        for bin in SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &mark_pending_confirm(
                VersionFile {
                    current: "v2.0.0".into(),
                    previous: vec!["v1.0.0".into()],
                    pending_confirm: None,
                },
                "v2.0.0",
            ),
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let rolled = confirm_or_revert(
            install,
            &releases,
            Duration::from_secs(90),
            Duration::from_millis(1),
            unhealthy_observe(),
            &restart,
        )
        .await
        .unwrap();

        assert_eq!(rolled, Some("v1.0.0".to_owned()));
        // Symlinks rolled back to previous-good.
        for bin in SWAP_BINARIES {
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v1.0.0-{bin}").into_bytes(),
            );
        }
        // Revert restored the ledger: previous-good current, marker cleared.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v1.0.0");
        assert_eq!(vf.pending_confirm, None);
        assert_eq!(
            *calls.lock().unwrap(),
            vec![vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]],
        );
    }
}
