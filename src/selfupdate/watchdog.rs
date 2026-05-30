//! Post-swap watchdog (spec §7): a ~90s stability window (< coordinator
//! heartbeat-timeout) polling /health + control Ping, crash-loop aware via
//! restart.rs. On failure: re-point the symlink to previous-good + restart.
//! Rollback touches ONLY the binary symlink — never data_dir / runner_dir.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::orchestrator::restart::{BackoffParams, RestartState, RestartStatus, status};

use super::swap::{
    RestartRunner, SUPERVISOR_UNIT, SWAP_BINARIES, VersionFile, read_version_file, repoint_symlink,
    write_version_file,
};

/// One watchdog observation snapshot.
#[derive(Debug, Clone, Copy)]
pub struct WatchdogObservations {
    /// `GET /health` returned 200 on the latest poll.
    pub health_200: bool,
    /// Control `Cmd::Ping` returned `Reply::Pong` on the latest poll.
    pub pong: bool,
    /// The ~90s stability window has fully elapsed.
    pub window_elapsed: bool,
    /// Restart/backoff state of the freshly-swapped supervisor (restart.rs).
    pub restart: RestartState,
}

/// What the watchdog should do given the latest observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevertDecision {
    /// Healthy through the whole window — keep the new version.
    KeepNewVersion,
    /// Still inside the window, healthy so far — keep polling.
    Watching,
    /// A failure was observed — revert the symlink to previous-good. Carries why.
    Revert(String),
}

/// Pure revert decision (spec §7). Crash-loop / health-fail / no-pong → Revert;
/// healthy + window elapsed → KeepNewVersion; healthy mid-window → Watching.
#[must_use]
pub fn decide_revert(o: WatchdogObservations) -> RevertDecision {
    if status(o.restart, BackoffParams::default(), 0) == RestartStatus::CrashLoop {
        return RevertDecision::Revert("post-swap crash-loop detected".into());
    }
    if !o.health_200 {
        return RevertDecision::Revert("post-swap /health not 200".into());
    }
    if !o.pong {
        return RevertDecision::Revert("post-swap control Ping had no Pong".into());
    }
    if o.window_elapsed {
        RevertDecision::KeepNewVersion
    } else {
        RevertDecision::Watching
    }
}

/// Roll the binary symlinks back to the previous-good version recorded in
/// `<install_dir>/VERSION`, restore the VERSION ledger (the rolled-back version
/// becomes `current` again), then trigger a unit restart.
///
/// Touches ONLY the binary symlinks + VERSION (spec invariant #2) — never
/// `data_dir` / `runner_dir` / `mesh-identity.json`. The previous-good binaries
/// are expected to still be staged under `<releases_dir>/<previous>/`.
///
/// # Errors
/// No previous-good version is recorded, the VERSION ledger is missing, or a
/// symlink re-point / VERSION write fails.
pub async fn revert_to_previous(
    install_dir: &Path,
    releases_dir: &Path,
    restart: &RestartRunner,
) -> Result<String> {
    let current = read_version_file(install_dir).context("read VERSION for rollback")?;
    let Some(previous) = current.previous.first().cloned() else {
        bail!("no previous-good version recorded — cannot roll back");
    };

    let version_dir = releases_dir.join(&previous);
    for bin in SWAP_BINARIES {
        repoint_symlink(install_dir, bin, &version_dir.join(bin))
            .with_context(|| format!("rollback re-point {bin} -> {previous}"))?;
    }

    // The rolled-back version becomes current again; the rest of the history is
    // preserved (drop the head we just promoted back to current).
    let mut remaining = current.previous;
    remaining.remove(0);
    write_version_file(
        install_dir,
        &VersionFile {
            current: previous.clone(),
            previous: remaining,
        },
    )
    .context("write VERSION after rollback")?;

    if !restart(vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]).await {
        tracing::warn!(unit = SUPERVISOR_UNIT, "rollback restart trigger reported failure");
    }
    Ok(previous)
}

/// Source of fresh watchdog observations: re-evaluated on every poll tick. The
/// production loop wires this to a live `GET /health` + control `Cmd::Ping`
/// (plus the swapped supervisor's restart state); tests inject a closure.
pub type ObserveFn<'a> =
    Box<dyn FnMut() -> crate::runtime::BoxFut<'a, WatchdogObservations> + Send + 'a>;

/// Run the post-swap watchdog: poll `observe` every `poll_interval` until the
/// `~stability_window` elapses or a failure is seen. On [`RevertDecision::Revert`]
/// it rolls the symlinks back to previous-good ([`revert_to_previous`]) and
/// returns the rolled-back version; on [`RevertDecision::KeepNewVersion`] it
/// returns `Ok(None)`.
///
/// The full process restart performed by the rollback (not an in-process
/// hot-swap) is what re-loads the mesh fabric (spec invariant #1).
///
/// # Errors
/// A rollback failure (no previous-good version, or a symlink / VERSION write
/// error) is propagated.
pub async fn run_watchdog(
    install_dir: &Path,
    releases_dir: &Path,
    stability_window: Duration,
    poll_interval: Duration,
    mut observe: ObserveFn<'_>,
    restart: &RestartRunner,
) -> Result<Option<String>> {
    let started = Instant::now();
    loop {
        let mut snapshot = observe().await;
        snapshot.window_elapsed = started.elapsed() >= stability_window;

        match decide_revert(snapshot) {
            RevertDecision::KeepNewVersion => return Ok(None),
            RevertDecision::Watching => tokio::time::sleep(poll_interval).await,
            RevertDecision::Revert(reason) => {
                tracing::warn!(%reason, "post-swap watchdog rolling back to previous-good");
                let rolled_back = revert_to_previous(install_dir, releases_dir, restart).await?;
                return Ok(Some(rolled_back));
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::orchestrator::restart::RestartState;

    fn healthy() -> WatchdogObservations {
        WatchdogObservations {
            health_200: true,
            pong: true,
            window_elapsed: true,
            restart: RestartState::default(),
        }
    }

    #[test]
    fn stays_when_healthy_through_window() {
        assert_eq!(decide_revert(healthy()), RevertDecision::KeepNewVersion);
    }

    #[test]
    fn reverts_on_health_fail() {
        let o = WatchdogObservations {
            health_200: false,
            ..healthy()
        };
        assert!(matches!(decide_revert(o), RevertDecision::Revert(_)));
    }

    #[test]
    fn reverts_on_missing_pong() {
        let o = WatchdogObservations {
            pong: false,
            ..healthy()
        };
        assert!(matches!(decide_revert(o), RevertDecision::Revert(_)));
    }

    /// Crash-loop (>= threshold consecutive failures, restart.rs) forces a revert
    /// even before the window fully elapses.
    #[test]
    fn reverts_on_crash_loop() {
        let o = WatchdogObservations {
            window_elapsed: false,
            restart: RestartState {
                consecutive_failures: 5,
                ..Default::default()
            },
            ..healthy()
        };
        assert!(matches!(decide_revert(o), RevertDecision::Revert(_)));
    }

    /// Still inside the window, healthy so far, no crash-loop → keep watching
    /// (not yet a final keep decision).
    #[test]
    fn keeps_watching_mid_window() {
        let o = WatchdogObservations {
            window_elapsed: false,
            ..healthy()
        };
        assert_eq!(decide_revert(o), RevertDecision::Watching);
    }

    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// A no-op [`RestartRunner`] that records every systemctl argument vector.
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

    /// revert_to_previous re-points BOTH binary symlinks back at the previous-good
    /// staged version, restores the VERSION ledger so the rolled-back version is
    /// current again, and triggers exactly one restart — touching ONLY symlinks +
    /// VERSION (spec invariant #2).
    #[tokio::test]
    async fn revert_to_previous_repoints_symlinks_and_restores_version() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");

        // Stage both the (bad) current v2.0.0 and the previous-good v1.0.0.
        for ver in ["v1.0.0", "v2.0.0"] {
            let dir = releases.join(ver);
            std::fs::create_dir_all(&dir).unwrap();
            for bin in SWAP_BINARIES {
                std::fs::write(dir.join(bin), format!("{ver}-{bin}")).unwrap();
            }
        }

        // Live state: symlinks point at the bad v2.0.0, VERSION records the swap.
        for bin in SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
            },
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let rolled_back = revert_to_previous(install, &releases, &restart).await.unwrap();
        assert_eq!(rolled_back, "v1.0.0");

        // Symlinks now resolve to the previous-good binaries.
        for bin in SWAP_BINARIES {
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v1.0.0-{bin}").into_bytes(),
                "{bin} must roll back to previous-good",
            );
        }

        // VERSION ledger: previous-good is current again, history drained.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v1.0.0");
        assert!(vf.previous.is_empty());

        // Exactly one restart with the expected systemctl arguments.
        assert_eq!(
            *calls.lock().unwrap(),
            vec![vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]],
        );
    }

    /// With no previous-good version recorded, a rollback is impossible and errors
    /// instead of silently leaving a broken install.
    #[tokio::test]
    async fn revert_to_previous_errors_without_previous_good() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec![],
            },
        )
        .unwrap();

        let (restart, _) = recording_restart();
        let err = revert_to_previous(install, &install.join("releases"), &restart)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no previous-good"), "got: {err}");
    }

    /// run_watchdog keeps the new version when every poll is healthy through the
    /// stability window — no rollback, no restart.
    #[tokio::test]
    async fn run_watchdog_keeps_new_version_when_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let (restart, calls) = recording_restart();

        let observe: ObserveFn<'_> = Box::new(|| {
            Box::pin(async {
                WatchdogObservations {
                    health_200: true,
                    pong: true,
                    window_elapsed: false, // overwritten by run_watchdog
                    restart: RestartState::default(),
                }
            })
        });

        // Zero-length window: the first poll already sees window_elapsed = true.
        let kept = run_watchdog(
            install,
            &install.join("releases"),
            Duration::ZERO,
            Duration::from_millis(1),
            observe,
            &restart,
        )
        .await
        .unwrap();

        assert_eq!(kept, None, "healthy window must keep the new version");
        assert!(calls.lock().unwrap().is_empty(), "no rollback restart expected");
    }

    /// run_watchdog rolls back to previous-good the moment an unhealthy poll is
    /// observed, returning the rolled-back version.
    #[tokio::test]
    async fn run_watchdog_rolls_back_on_unhealthy_poll() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");

        for ver in ["v1.0.0", "v2.0.0"] {
            let dir = releases.join(ver);
            std::fs::create_dir_all(&dir).unwrap();
            for bin in SWAP_BINARIES {
                std::fs::write(dir.join(bin), format!("{ver}-{bin}")).unwrap();
            }
        }
        for bin in SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
            },
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let observe: ObserveFn<'_> = Box::new(|| {
            Box::pin(async {
                WatchdogObservations {
                    health_200: false, // unhealthy → revert
                    pong: true,
                    window_elapsed: false,
                    restart: RestartState::default(),
                }
            })
        });

        let rolled_back = run_watchdog(
            install,
            &releases,
            Duration::from_secs(90),
            Duration::from_millis(1),
            observe,
            &restart,
        )
        .await
        .unwrap();

        assert_eq!(rolled_back, Some("v1.0.0".to_owned()));
        assert_eq!(read_version_file(install).unwrap().current, "v1.0.0");
        assert_eq!(calls.lock().unwrap().len(), 1, "rollback triggers one restart");
    }
}
