//! Production `self-update --to <version>` execution path (spec §3-§6).
//!
//! This is the single REAL self-update driver: it composes the audited engine
//! pieces ([`super::fetch`], [`super::swap`]) into the ordered flow the NixOS
//! `tabbify-update` unit invokes. The legacy bash fetch/probe/swap is replaced
//! by `/opt/tabbify/supervisord self-update --to <ver>`.
//!
//! Ordered flow:
//! 1. [`fetch::VersionFetcher::fetch_version`] — download + sha256-verify the
//!    versioned binary set into `<releases_dir>/<version>/`.
//! 2. Launch the freshly-staged candidate OUT-OF-BAND via
//!    `supervisord --check --candidate-identity-path <transient>` (transient
//!    identity + alt loopback bind + ephemeral port — NEVER the sticky identity
//!    / ULA). The candidate self-evaluates the 3-part gate and exits 0/1.
//! 3. On gate PASS: [`swap::swap_to`] (re-point symlinks + rotate VERSION) +
//!    stamp the pending-confirm marker, then restart the live unit.
//! 4. On gate FAIL: clean up the candidate identity, do NOT swap, return an
//!    error (the binary maps this to a non-zero exit).
//!
//! The candidate launch is process/env-dependent, so it is hidden behind the
//! [`CandidateProbe`] seam: the production probe spawns the real binary; tests
//! inject a closure to drive PASS / FAIL without a child process.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

use super::SelfUpdateConfig;
use super::swap::{
    KEEP_PREVIOUS, RestartRunner, SUPERVISOR_UNIT, SWAP_BINARIES, mark_pending_confirm,
    push_version, read_version_file, repoint_symlink, write_version_file,
};
use crate::runtime::BoxFut;

/// The out-of-band candidate-probe seam: given the staged candidate binary, the
/// transient identity path, and the gate timeout, run the candidate behind the
/// 3-part health gate and report whether it PASSED. Production wires
/// [`production_candidate_probe`]; tests inject a closure.
pub type CandidateProbe =
    Box<dyn Fn(PathBuf, PathBuf, Duration) -> BoxFut<'static, Result<bool>> + Send + Sync>;

/// The production candidate probe: spawn the freshly-staged `supervisord` with
/// `--check --candidate-identity-path <transient>` and a loopback ephemeral
/// bind, wait up to `gate_timeout` for it to exit, and treat a clean exit-0 as
/// a gate PASS (the candidate self-evaluates the 3-part gate in `--check`).
///
/// The candidate joins the mesh with the TRANSIENT identity (so it never claims
/// the sticky ULA), binds `127.0.0.1:0` (an OS-ephemeral port, never the
/// production bind), and exits 1 on any gate failure. A spawn error, a non-zero
/// exit, or a timeout are all reported as "did NOT pass" (fail-closed).
#[must_use]
pub fn production_candidate_probe() -> CandidateProbe {
    Box::new(
        |candidate_bin: PathBuf, transient_identity: PathBuf, gate_timeout: Duration| {
            let fut: BoxFut<'static, Result<bool>> = Box::pin(async move {
                let mut child = Command::new(&candidate_bin)
                    .arg("--check")
                    .arg("--candidate-identity-path")
                    .arg(&transient_identity)
                    // Loopback ephemeral bind: the candidate must not contend
                    // for the sticky ULA / production bind. `--check` already
                    // defaults to a loopback ephemeral addr, but we pin it
                    // explicitly so the contract does not hinge on that default.
                    .env("SUPERVISOR_BIND", "127.0.0.1:0")
                    .stdin(Stdio::null())
                    .spawn()
                    .with_context(|| format!("spawn candidate {candidate_bin:?}"))?;

                match tokio::time::timeout(gate_timeout, child.wait()).await {
                    Ok(Ok(status)) => Ok(status.success()),
                    Ok(Err(e)) => Err(anyhow::Error::new(e).context("await candidate exit")),
                    Err(_elapsed) => {
                        // The candidate outran the gate timeout: kill it and
                        // fail closed.
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        Ok(false)
                    }
                }
            });
            fut
        },
    )
}

/// What `self-update` did, for the binary to log + map to an exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelfUpdateOutcome {
    /// `<version>` was already current — nothing to do (exit 0).
    AlreadyCurrent(String),
    /// Gate passed, symlinks swapped, pending-confirm stamped, restart triggered
    /// (exit 0). The next boot's self-watchdog confirms or reverts.
    Swapped(String),
}

/// Run the production `self-update --to <version>` flow against `cfg`, using the
/// injected `probe` (candidate launch) + `restart` (systemctl) seams.
///
/// # Errors
/// - the fetch / sha256-verify failed (corrupt or missing release),
/// - the candidate gate FAILED (no swap is performed; the candidate identity is
///   cleaned up),
/// - the freshly-staged release is missing a binary (would install a dangling
///   symlink), or
/// - a symlink re-point / VERSION write failed.
pub async fn self_update_to(
    version: &str,
    cfg: &SelfUpdateConfig,
    probe: &CandidateProbe,
    restart: &RestartRunner,
) -> Result<SelfUpdateOutcome> {
    // Short-circuit a no-op update so a re-run (or a desired == current trigger)
    // does not re-stage / re-probe / restart for nothing.
    if let Ok(vf) = read_version_file(&cfg.install_dir) {
        if vf.current == version {
            return Ok(SelfUpdateOutcome::AlreadyCurrent(version.to_owned()));
        }
    }

    // 1. FETCH + sha256-verify into <releases_dir>/<version>/.
    let version_dir = cfg
        .fetcher()
        .fetch_version(version)
        .await
        .with_context(|| format!("fetch+verify {version}"))?;

    // Guard: BOTH binaries must be staged before we ever consider swapping —
    // otherwise the swap would install a dangling symlink. (fetch_version writes
    // both, but a corrupt partial dir on disk would otherwise slip through.)
    for bin in SWAP_BINARIES {
        if !version_dir.join(bin).is_file() {
            bail!(
                "staged release {version} missing {bin} under {} — refusing to swap",
                version_dir.display()
            );
        }
    }

    // 2. OUT-OF-BAND PROBE: launch the candidate under --check with the TRANSIENT
    //    identity. The candidate self-evaluates the 3-part gate and exits 0/1.
    let candidate_bin = version_dir.join("supervisord");
    let passed = probe(
        candidate_bin,
        cfg.candidate_identity_path.clone(),
        cfg.gate_timeout,
    )
    .await
    .context("candidate probe")?;

    if !passed {
        cleanup_candidate_identity(&cfg.candidate_identity_path);
        bail!("candidate gate FAILED for {version} — NOT swapping");
    }

    // 3. PASS: re-point the symlinks + rotate VERSION + stamp the pending-confirm
    //    marker, then restart the live unit. The marker tells the NEXT boot's
    //    self-watchdog (see `super::confirm`) to hold the stability window and
    //    confirm-or-revert. Touches ONLY symlinks + VERSION (spec invariant #2).
    for bin in SWAP_BINARIES {
        repoint_symlink(&cfg.install_dir, bin, &version_dir.join(bin))?;
    }
    let current = read_version_file(&cfg.install_dir).unwrap_or_default();
    let next = mark_pending_confirm(push_version(current, version, KEEP_PREVIOUS), version);
    write_version_file(&cfg.install_dir, &next)?;

    // The candidate identity is transient: drop it once it has served its probe.
    cleanup_candidate_identity(&cfg.candidate_identity_path);

    if !restart(vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]).await {
        // A failed restart trigger is NOT fatal: the post-restart watchdog (or
        // systemd's own restart policy) will observe liveness. Log and proceed.
        tracing::warn!(
            unit = SUPERVISOR_UNIT,
            "self-update restart trigger reported failure"
        );
    }

    Ok(SelfUpdateOutcome::Swapped(version.to_owned()))
}

/// Remove the transient candidate identity file (best-effort). It is recreated
/// on the next probe; a leftover would be reused as a stale ULA claim.
fn cleanup_candidate_identity(path: &Path) {
    if path.exists() {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(path = %path.display(), error = %e, "could not remove candidate identity");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::selfupdate::swap::{VersionFile, read_version_file, write_version_file};

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use sha2::{Digest, Sha256};

    fn hex_sha256(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        hex::encode(h.finalize())
    }

    /// A recording restart seam (no real systemd poke).
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

    /// A probe seam returning a fixed PASS/FAIL, recording the binary it was
    /// asked to launch (so we can assert it was the freshly-staged candidate).
    fn fixed_probe(pass: bool) -> (CandidateProbe, Arc<Mutex<Vec<PathBuf>>>) {
        let seen: Arc<Mutex<Vec<PathBuf>>> = Arc::default();
        let recorded = Arc::clone(&seen);
        let probe: CandidateProbe =
            Box::new(move |bin: PathBuf, _identity: PathBuf, _t: Duration| {
                let recorded = Arc::clone(&recorded);
                Box::pin(async move {
                    recorded.lock().unwrap().push(bin);
                    Ok(pass)
                })
            });
        (probe, seen)
    }

    /// Stand up a mock release server serving `supervisor/latest` + the two
    /// versioned binaries for `version`, and return a `SelfUpdateConfig` wired to
    /// it with `install_dir`/`releases_dir` under `tmp`.
    async fn mock_release(tmp: &Path, version: &str, arch: &str) -> (MockServer, SelfUpdateConfig) {
        let server = MockServer::start().await;
        let sup = format!("FAKE-supervisord-{version}").into_bytes();
        let run = format!("FAKE-runner-{version}").into_bytes();
        let manifest = format!(
            r#"{{"latest":"{version}","versions":["{version}"],"sha256":{{"supervisord":"{}","tabbify-runner":"{}"}},"ts":"t"}}"#,
            hex_sha256(&sup),
            hex_sha256(&run),
        );
        Mock::given(method("GET"))
            .and(path("/supervisor/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/{version}/{arch}/supervisord")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(sup))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/{version}/{arch}/tabbify-runner")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(run))
            .mount(&server)
            .await;

        let cfg = SelfUpdateConfig {
            release_base_url: server.uri(),
            arch: arch.to_owned(),
            releases_dir: tmp.join("releases"),
            install_dir: tmp.to_path_buf(),
            candidate_identity_path: tmp.join("candidate-identity.json"),
            gate_timeout: Duration::from_secs(5),
            stability_window: Duration::from_secs(5),
        };
        (server, cfg)
    }

    /// Happy path: fetch+verify, gate PASS, swap re-points BOTH symlinks, the
    /// VERSION ledger records the new current + a pending-confirm marker, the
    /// candidate identity is cleaned up, and exactly one restart fires.
    #[tokio::test]
    async fn self_update_swaps_and_stamps_pending_on_gate_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let arch = "x86_64";
        let (_server, cfg) = mock_release(tmp.path(), "v2.0.0", arch).await;

        // A pre-existing current so the promotion path is exercised.
        write_version_file(
            &cfg.install_dir,
            &VersionFile {
                current: "v1.0.0".into(),
                previous: vec![],
                pending_confirm: None,
            },
        )
        .unwrap();
        // A leftover candidate identity that must be cleaned up.
        std::fs::write(&cfg.candidate_identity_path, b"stale").unwrap();

        let (probe, seen) = fixed_probe(true);
        let (restart, calls) = recording_restart();

        let outcome = self_update_to("v2.0.0", &cfg, &probe, &restart)
            .await
            .unwrap();
        assert_eq!(outcome, SelfUpdateOutcome::Swapped("v2.0.0".to_owned()));

        // The probe was asked to launch the freshly-staged candidate binary.
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], cfg.releases_dir.join("v2.0.0").join("supervisord"));

        // Symlinks re-pointed to the new staged binaries.
        for bin in SWAP_BINARIES {
            assert_eq!(
                std::fs::read_link(cfg.install_dir.join(bin)).unwrap(),
                cfg.releases_dir.join("v2.0.0").join(bin),
            );
        }

        // Ledger: new current, old promoted, pending-confirm stamped.
        let vf = read_version_file(&cfg.install_dir).unwrap();
        assert_eq!(vf.current, "v2.0.0");
        assert_eq!(vf.previous, vec!["v1.0.0".to_owned()]);
        assert_eq!(vf.pending_confirm.as_deref(), Some("v2.0.0"));

        // Candidate identity cleaned up; exactly one restart with the unit arg.
        assert!(!cfg.candidate_identity_path.exists());
        assert_eq!(
            *calls.lock().unwrap(),
            vec![vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]],
        );
    }

    /// Gate FAIL: NO swap, NO restart, NO pending marker — the live install is
    /// left exactly as it was, and the candidate identity is cleaned up.
    #[tokio::test]
    async fn self_update_does_not_swap_on_gate_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let (_server, cfg) = mock_release(tmp.path(), "v2.0.0", "x86_64").await;
        write_version_file(
            &cfg.install_dir,
            &VersionFile {
                current: "v1.0.0".into(),
                previous: vec![],
                pending_confirm: None,
            },
        )
        .unwrap();
        std::fs::write(&cfg.candidate_identity_path, b"stale").unwrap();

        let (probe, _seen) = fixed_probe(false);
        let (restart, calls) = recording_restart();

        let err = self_update_to("v2.0.0", &cfg, &probe, &restart)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("gate FAILED"), "got: {err}");

        // No symlink installed, ledger untouched, no restart, identity cleaned.
        for bin in SWAP_BINARIES {
            assert!(std::fs::symlink_metadata(cfg.install_dir.join(bin)).is_err());
        }
        let vf = read_version_file(&cfg.install_dir).unwrap();
        assert_eq!(vf.current, "v1.0.0");
        assert_eq!(vf.pending_confirm, None);
        assert!(calls.lock().unwrap().is_empty());
        assert!(!cfg.candidate_identity_path.exists());
    }

    /// `--to <current>` is a no-op: no fetch, no probe, no restart.
    #[tokio::test]
    async fn self_update_is_noop_when_already_current() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = SelfUpdateConfig {
            release_base_url: "http://127.0.0.1:1/none".to_owned(),
            arch: "x86_64".to_owned(),
            releases_dir: tmp.path().join("releases"),
            install_dir: tmp.path().to_path_buf(),
            candidate_identity_path: tmp.path().join("candidate-identity.json"),
            gate_timeout: Duration::from_secs(1),
            stability_window: Duration::from_secs(1),
        };
        write_version_file(
            &cfg.install_dir,
            &VersionFile {
                current: "v9.9.9".into(),
                previous: vec![],
                pending_confirm: None,
            },
        )
        .unwrap();

        // Probe / restart that would PANIC if invoked: a no-op must not call them.
        let probe: CandidateProbe = Box::new(|_b, _i, _t| {
            Box::pin(async { panic!("probe must not run for a no-op update") })
        });
        let restart: RestartRunner =
            Arc::new(|_args| Box::pin(async { panic!("restart must not run for a no-op update") }));

        let outcome = self_update_to("v9.9.9", &cfg, &probe, &restart)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            SelfUpdateOutcome::AlreadyCurrent("v9.9.9".to_owned())
        );
    }
}
