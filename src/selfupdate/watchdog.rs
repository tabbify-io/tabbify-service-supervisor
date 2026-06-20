//! Post-swap watchdog (spec §7): a stability window held UNDER the coordinator
//! heartbeat-timeout (default 45s < 60s — see
//! [`super::DEFAULT_STABILITY_WINDOW`]) polling /health + control Ping,
//! crash-loop aware via restart.rs. The window must close before the
//! coordinator GC's a bad node from the roster, so it can never outrun the
//! heartbeat-timeout.
//!
//! Revert is PROMPT: a crash / health-fail / no-pong observation on ANY poll
//! tick reverts immediately ([`decide_revert`] checks failures before the
//! window); only the all-healthy path waits for the window to elapse before
//! committing [`RevertDecision::KeepNewVersion`].
//!
//! On failure: re-point the symlink to previous-good + restart. Rollback
//! touches ONLY the binary symlink — never data_dir / runner_dir.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::orchestrator::restart::{BackoffParams, RestartState, RestartStatus, status};

use super::swap::{
    RestartRunner, SUPERVISOR_UNIT, SWAP_BINARIES, VersionFile, read_version_file,
    release_is_complete, repoint_symlink, write_version_file,
};

/// One watchdog observation snapshot.
#[derive(Debug, Clone, Copy)]
pub struct WatchdogObservations {
    /// `GET /health` returned 200 on the latest poll.
    pub health_200: bool,
    /// Control `Cmd::Ping` returned `Reply::Pong` on the latest poll.
    pub pong: bool,
    /// The stability window (default 45s, < heartbeat-timeout) has elapsed.
    pub window_elapsed: bool,
    /// Restart/backoff state of the freshly-swapped supervisor (restart.rs).
    pub restart: RestartState,
    /// The live WG data plane has an inbound decap frame within the staleness
    /// threshold (Track-K `dataplane_healthy`). The post-restart watchdog runs
    /// as the REAL rooted production process (unlike the `--no-mesh` candidate
    /// probe), so it CAN observe the live tunnel. `true` for an idle/quiet node.
    pub data_plane_live: bool,
    /// The previous-good version (the rollback target) demonstrably had a live
    /// tunnel before this swap. Gate for the data-plane revert: we only roll
    /// back a build for breaking WG if rolling back would actually restore it.
    /// `false` means the environment itself is down — rolling back is futile,
    /// so we DON'T (fail-open for availability, spec §7).
    pub previous_good_had_tunnel: bool,
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
    // D1: data-plane-aware revert. The control plane (HTTP /health + /v1/about,
    // the relay-WS + HTTPS heartbeat transports) can stay green while the WG
    // tunnel is a black hole (the MSI incident). Revert ONLY when the tunnel is
    // dead AND the rollback target had it — never thrash when the env is down.
    if !o.data_plane_live && o.previous_good_had_tunnel {
        return RevertDecision::Revert("post-swap WG data plane dead (tunnel black-holed)".into());
    }
    if o.window_elapsed {
        RevertDecision::KeepNewVersion
    } else {
        RevertDecision::Watching
    }
}

/// Roll the binary symlinks back to the newest previous-good version recorded in
/// `<install_dir>/VERSION` whose binaries are still completely staged, restore
/// the VERSION ledger (the rolled-back version becomes `current` again), then
/// trigger a unit restart.
///
/// Before re-pointing, every candidate is validated with [`release_is_complete`]:
/// BOTH [`SWAP_BINARIES`] must exist as runnable regular files under
/// `<releases_dir>/<version>/`. Re-pointing a symlink at a missing target would
/// install a DANGLING symlink and brick the node, so an incomplete candidate is
/// skipped and the next-newest is tried. If no candidate is complete, this fails
/// loudly WITHOUT touching any symlink — a broken install is left untouched
/// rather than turned into an unbootable one.
///
/// Touches ONLY the binary symlinks + VERSION (spec invariant #2) — never
/// `data_dir` / `runner_dir` / `mesh-identity.json`.
///
/// # Errors
/// No previous-good version is recorded, the VERSION ledger is missing, none of
/// the recorded previous versions are completely staged, or a symlink re-point /
/// VERSION write fails.
pub async fn revert_to_previous(
    install_dir: &Path,
    releases_dir: &Path,
    restart: &RestartRunner,
) -> Result<String> {
    let current = read_version_file(install_dir).context("read VERSION for rollback")?;
    if current.previous.is_empty() {
        bail!("no previous-good version recorded — cannot roll back");
    }

    // Pick the newest recorded previous version whose binaries are fully staged.
    // Skipping an incomplete entry (rather than re-pointing at it) is what keeps
    // us from installing a dangling symlink.
    let Some(skipped) = current
        .previous
        .iter()
        .position(|version| release_is_complete(&releases_dir.join(version)))
    else {
        bail!(
            "no completely-staged previous-good version among {:?} under {} \
             — refusing to install a dangling symlink",
            current.previous,
            releases_dir.display(),
        );
    };
    let previous = current.previous[skipped].clone();

    let version_dir = releases_dir.join(&previous);
    for bin in SWAP_BINARIES {
        repoint_symlink(install_dir, bin, &version_dir.join(bin))
            .with_context(|| format!("rollback re-point {bin} -> {previous}"))?;
    }

    // The rolled-back version becomes current again; any incomplete entries we
    // skipped over (plus the promoted entry itself) are dropped from history,
    // and the rest is preserved.
    let mut remaining = current.previous;
    remaining.drain(..=skipped);
    write_version_file(
        install_dir,
        &VersionFile {
            current: previous.clone(),
            previous: remaining,
            // A completed rollback IS a confirmed state — the rolled-back
            // version is known-good, so drop any pending-confirm marker.
            pending_confirm: None,
        },
    )
    .context("write VERSION after rollback")?;

    if !restart(vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]).await {
        tracing::warn!(
            unit = SUPERVISOR_UNIT,
            "rollback restart trigger reported failure"
        );
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
    // Floor the poll cadence so a misconfigured `poll_interval == 0` cannot turn
    // the watch loop into a CPU-spinning busy-loop while still inside the
    // window. The window is the safety bound for total wait time; this only
    // bounds the per-tick wait.
    let poll_interval = poll_interval.max(MIN_POLL_INTERVAL);
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

/// Per-poll floor so a `poll_interval` of zero cannot busy-loop the watch loop.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
            // D1: a healthy baseline has a live tunnel and a previous-good that
            // also had one (so the data-plane revert clause is armed but unmet).
            data_plane_live: true,
            previous_good_had_tunnel: true,
        }
    }

    #[test]
    fn stays_when_healthy_through_window() {
        assert_eq!(decide_revert(healthy()), RevertDecision::KeepNewVersion);
    }

    /// D1: a dead data plane AFTER a swap, when the previous-good build
    /// demonstrably HAD the tunnel, reverts — the build broke WG even though
    /// /health + /v1/about (control-plane transports) stay green.
    #[test]
    fn reverts_on_dead_data_plane_when_previous_good_had_tunnel() {
        let o = WatchdogObservations {
            data_plane_live: false,
            previous_good_had_tunnel: true,
            ..healthy()
        };
        match decide_revert(o) {
            RevertDecision::Revert(why) => assert!(
                why.contains("data plane"),
                "revert reason must name the data plane, got: {why}"
            ),
            other => panic!("expected Revert, got {other:?}"),
        }
    }

    /// D1 fail-open: a dead data plane is NOT a revert trigger when the
    /// previous-good build ALSO lacked the tunnel — that means the ENVIRONMENT
    /// (relay/coordinator/host network) is down, not this build. Rolling back
    /// would thrash without fixing anything (§7 fail-open for availability).
    #[test]
    fn does_not_revert_dead_data_plane_when_env_itself_is_down() {
        let o = WatchdogObservations {
            data_plane_live: false,
            previous_good_had_tunnel: false, // env down, not the new build
            window_elapsed: true,
            ..healthy()
        };
        // Healthy control plane + window elapsed ⇒ keep (do not thrash).
        assert_eq!(decide_revert(o), RevertDecision::KeepNewVersion);
    }

    /// D1: a LIVE data plane keeps the new version through the window exactly
    /// as before (no behavioural regression to the existing control-plane gate).
    #[test]
    fn keeps_new_version_when_data_plane_live() {
        let o = WatchdogObservations {
            data_plane_live: true,
            previous_good_had_tunnel: true,
            ..healthy()
        };
        assert_eq!(decide_revert(o), RevertDecision::KeepNewVersion);
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

    /// Stage `version`'s [`SWAP_BINARIES`] under `<releases>/<version>/` as
    /// runnable (0o755) regular files — what [`release_is_complete`] expects of a
    /// valid rollback target.
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
            stage_release(&releases, ver);
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
                pending_confirm: None,
            },
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let rolled_back = revert_to_previous(install, &releases, &restart)
            .await
            .unwrap();
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
                pending_confirm: None,
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
                    data_plane_live: true,
                    previous_good_had_tunnel: true,
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
        assert!(
            calls.lock().unwrap().is_empty(),
            "no rollback restart expected"
        );
    }

    /// run_watchdog rolls back to previous-good the moment an unhealthy poll is
    /// observed, returning the rolled-back version.
    #[tokio::test]
    async fn run_watchdog_rolls_back_on_unhealthy_poll() {
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
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
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
                    data_plane_live: true,
                    previous_good_had_tunnel: true,
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
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "rollback triggers one restart"
        );
    }

    /// I3: a failing observation reverts PROMPTLY — on the very first poll tick
    /// — and is NOT deferred to the end of the (long) stability window. With a
    /// 90s window, if revert waited for window-elapse this test would hang /
    /// not roll back; instead it must roll back after exactly ONE observation.
    #[tokio::test]
    async fn run_watchdog_reverts_on_first_failing_observation() {
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
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
            },
        )
        .unwrap();

        // Count how many times the watchdog sampled health before reverting.
        let observations = Arc::new(Mutex::new(0u32));
        let counter = Arc::clone(&observations);
        let observe: ObserveFn<'_> = Box::new(move || {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                *counter.lock().unwrap() += 1;
                WatchdogObservations {
                    health_200: false, // failing from the very first tick
                    pong: true,
                    window_elapsed: false, // overwritten; window is NOT yet elapsed
                    restart: RestartState::default(),
                    data_plane_live: true,
                    previous_good_had_tunnel: true,
                }
            })
        });

        let (restart, calls) = recording_restart();
        let rolled_back = run_watchdog(
            install,
            &releases,
            // A window far larger than any test wall-clock: if the revert were
            // gated on window-elapse, it could never fire here.
            Duration::from_secs(90),
            Duration::from_millis(1),
            observe,
            &restart,
        )
        .await
        .unwrap();

        assert_eq!(rolled_back, Some("v1.0.0".to_owned()));
        assert_eq!(
            *observations.lock().unwrap(),
            1,
            "revert must fire on the FIRST failing observation, not wait for the window",
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "exactly one rollback restart"
        );
    }

    /// A crash-loop seen on the first tick (mid-window) reverts immediately via
    /// run_watchdog, mirroring the pure decide_revert crash-loop case.
    #[tokio::test]
    async fn run_watchdog_reverts_on_first_crash_loop_observation() {
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
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
            },
        )
        .unwrap();

        let observations = Arc::new(Mutex::new(0u32));
        let counter = Arc::clone(&observations);
        let observe: ObserveFn<'_> = Box::new(move || {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                *counter.lock().unwrap() += 1;
                WatchdogObservations {
                    health_200: true,
                    pong: true,
                    window_elapsed: false,
                    restart: RestartState {
                        consecutive_failures: 5, // >= crashloop threshold
                        ..Default::default()
                    },
                    data_plane_live: true,
                    previous_good_had_tunnel: true,
                }
            })
        });

        let (restart, _calls) = recording_restart();
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
        assert_eq!(
            *observations.lock().unwrap(),
            1,
            "crash-loop revert must fire on the first observation",
        );
    }

    /// I3 constant invariant: the default stability window is held strictly
    /// under the documented coordinator heartbeat-timeout, so a bad node is
    /// reverted before the coordinator GC's it from the roster.
    #[test]
    fn default_stability_window_under_heartbeat_timeout() {
        assert!(
            super::super::DEFAULT_STABILITY_WINDOW < super::super::COORDINATOR_HEARTBEAT_TIMEOUT,
            "stability window {:?} must be < heartbeat-timeout {:?}",
            super::super::DEFAULT_STABILITY_WINDOW,
            super::super::COORDINATOR_HEARTBEAT_TIMEOUT,
        );
    }

    /// Resolve `<install>/<bin>` as a symlink and assert it does NOT dangle: the
    /// link target must exist. Returns whether a symlink is present at all.
    fn symlink_is_live(install: &Path, bin: &str) -> bool {
        let link = install.join(bin);
        match std::fs::symlink_metadata(&link) {
            // No link node at all — nothing was installed.
            Err(_) => false,
            Ok(meta) if meta.file_type().is_symlink() => {
                // `metadata` follows the link; Ok ⇒ the target exists (not dangling).
                std::fs::metadata(&link).is_ok()
            }
            // A non-symlink node where a symlink was expected — treat as present.
            Ok(_) => true,
        }
    }

    /// The whole point of C3: when the only recorded previous-good version is
    /// missing a binary under `<releases>/<version>/`, revert_to_previous must
    /// fail loudly and must NOT install a dangling symlink (which would brick the
    /// node). Here the head v1.0.0 has only `supervisord` staged — `tabbify-runner`
    /// is absent — so rollback has no complete target and errors.
    #[tokio::test]
    async fn revert_does_not_install_dangling_symlink_when_previous_binary_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");

        // Stage v1.0.0 INCOMPLETE: supervisord present, tabbify-runner missing.
        let v1 = releases.join("v1.0.0");
        std::fs::create_dir_all(&v1).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            let path = v1.join("supervisord");
            std::fs::write(&path, b"v1-supervisord").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
            },
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let err = revert_to_previous(install, &releases, &restart)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("completely-staged"),
            "expected a loud no-complete-target error, got: {err}",
        );

        // No symlink was installed for EITHER binary — not even the one whose
        // staged file happened to exist. A partial roll-back is still a brick.
        for bin in SWAP_BINARIES {
            assert!(
                !symlink_is_live(install, bin),
                "{bin}: revert must not leave any (dangling) rollback symlink behind",
            );
        }

        // VERSION ledger untouched, no restart triggered.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0");
        assert_eq!(vf.previous, vec!["v1.0.0".to_owned()]);
        assert!(
            calls.lock().unwrap().is_empty(),
            "no restart on failed rollback"
        );
    }

    /// When the newest previous entry is incomplete but an older one is fully
    /// staged, revert_to_previous skips the incomplete head and rolls back to the
    /// next valid version — never installing a dangling symlink along the way.
    #[tokio::test]
    async fn revert_skips_incomplete_previous_and_uses_next_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");

        // v1.5.0 (newest previous) is INCOMPLETE; v1.0.0 (older) is complete.
        let bad = releases.join("v1.5.0");
        std::fs::create_dir_all(&bad).unwrap(); // empty: no binaries staged
        stage_release(&releases, "v1.0.0");

        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.5.0".into(), "v1.0.0".into()],
                pending_confirm: None,
            },
        )
        .unwrap();

        let (restart, calls) = recording_restart();
        let rolled_back = revert_to_previous(install, &releases, &restart)
            .await
            .unwrap();
        assert_eq!(rolled_back, "v1.0.0", "must skip incomplete v1.5.0");

        // Symlinks resolve to the live v1.0.0 binaries (and are not dangling).
        for bin in SWAP_BINARIES {
            assert!(symlink_is_live(install, bin), "{bin} symlink must be live");
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v1.0.0-{bin}").into_bytes(),
            );
        }

        // Ledger: v1.0.0 is current; the skipped incomplete head is dropped too.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v1.0.0");
        assert!(vf.previous.is_empty());
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "exactly one restart on success"
        );
    }
}
