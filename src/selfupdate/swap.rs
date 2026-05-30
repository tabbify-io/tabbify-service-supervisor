//! Health-gated atomic swap (spec §5): re-point the binary symlinks + write the
//! VERSION ledger atomically, then trigger a unit restart. The swap touches
//! ONLY symlinks + VERSION — never data_dir / runner_dir / mesh-identity.json.

use std::os::unix::fs::symlink;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::runtime::BoxFut;

/// The binaries whose symlinks the swap re-points. Single source of truth: the
/// watchdog rolls back this exact set, so adding a binary here keeps rollback in
/// sync automatically.
pub(crate) const SWAP_BINARIES: [&str; 2] = ["supervisord", "tabbify-runner"];

/// How many previous-good versions the [`VersionFile`] keeps as rollback targets.
const KEEP_PREVIOUS: usize = 3;

/// systemd unit re-started after the symlinks are re-pointed. Single source of
/// truth shared with the watchdog's rollback restart.
pub(crate) const SUPERVISOR_UNIT: &str = "tabbify-supervisor";

/// Restart-trigger seam: given `systemctl` arguments (e.g.
/// `["restart", "tabbify-supervisor"]`), run the command and return whether it
/// succeeded. Production: the real `systemctl` via [`production_restart_runner`].
/// Tests: an injected no-op closure so no real unit is poked.
pub type RestartRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, bool> + Send + Sync>;

/// The `/opt/tabbify/VERSION` ledger: the live version + previous-good history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionFile {
    /// The version the symlinks currently point at (`"v2.0.0"`).
    pub current: String,
    /// Previous-good versions, newest first (rollback targets).
    pub previous: Vec<String>,
}

/// Whether `<version_dir>/<bin>` exists as a regular file (following symlinks)
/// and carries at least one executable bit. A symlink target is resolved, so a
/// dangling staged binary counts as missing.
fn staged_binary_is_runnable(version_dir: &Path, bin: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    // `metadata` follows symlinks, so a staged symlink whose target is gone
    // (a dangling stage) is reported as missing — exactly what we want.
    match std::fs::metadata(version_dir.join(bin)) {
        Ok(meta) => meta.is_file() && (meta.permissions().mode() & 0o111 != 0),
        Err(_) => false,
    }
}

/// Whether a staged release directory is a safe rollback target: BOTH
/// [`SWAP_BINARIES`] exist under `<version_dir>/` as runnable regular files.
///
/// Rolling the symlinks back to a directory missing either binary would install
/// a dangling symlink and brick the node, so the watchdog must skip such an
/// entry instead. This is the guard for that decision.
#[must_use]
pub fn release_is_complete(version_dir: &Path) -> bool {
    SWAP_BINARIES
        .iter()
        .all(|bin| staged_binary_is_runnable(version_dir, bin))
}

/// Atomically point `<install_dir>/<name>` at `target` (overwriting any prior
/// symlink): create a temp symlink alongside, then `rename` over the live one.
///
/// # Errors
/// A filesystem error creating the temp symlink or renaming it into place.
pub fn repoint_symlink(install_dir: &Path, name: &str, target: &Path) -> Result<()> {
    let link = install_dir.join(name);
    let tmp = install_dir.join(format!(".{name}.swap"));
    let _ = std::fs::remove_file(&tmp);
    symlink(target, &tmp).with_context(|| format!("symlink {tmp:?} -> {target:?}"))?;
    std::fs::rename(&tmp, &link).with_context(|| format!("rename {tmp:?} -> {link:?}"))?;
    Ok(())
}

/// Atomically write the VERSION ledger (tempfile + rename).
///
/// # Errors
/// A serialisation or filesystem error.
pub fn write_version_file(install_dir: &Path, vf: &VersionFile) -> Result<()> {
    let path = install_dir.join("VERSION");
    let tmp = install_dir.join(".VERSION.swap");
    let json = serde_json::to_string_pretty(vf).context("serialize VERSION")?;
    std::fs::write(&tmp, json).with_context(|| format!("write {tmp:?}"))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

/// Read the VERSION ledger (errors if absent / malformed).
///
/// # Errors
/// The file is missing or its JSON does not parse as a [`VersionFile`].
pub fn read_version_file(install_dir: &Path) -> Result<VersionFile> {
    let path = install_dir.join("VERSION");
    let json = std::fs::read_to_string(&path).with_context(|| format!("read {path:?}"))?;
    serde_json::from_str(&json).with_context(|| format!("parse {path:?}"))
}

/// Promote the old current into `previous` (capped at `keep`) and set `new`.
#[must_use]
pub fn push_version(mut vf: VersionFile, new: &str, keep: usize) -> VersionFile {
    if !vf.current.is_empty() && vf.current != new {
        vf.previous.insert(0, vf.current.clone());
        vf.previous.truncate(keep);
    }
    vf.current = new.to_owned();
    vf
}

/// Build the production [`RestartRunner`]: spawns `systemctl <args>` and returns
/// `true` iff the process exits 0. A spawn failure or non-zero exit yields
/// `false` (the swap path logs and lets the watchdog catch a stuck unit).
#[must_use]
pub fn production_restart_runner() -> RestartRunner {
    Arc::new(move |args: Vec<String>| {
        let fut: BoxFut<'static, bool> = Box::pin(async move {
            match Command::new("systemctl")
                .args(&args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
            {
                Ok(s) => s.success(),
                Err(_) => false,
            }
        });
        fut
    })
}

/// Health-gated atomic swap to `version` (staged under `version_dir`): re-point
/// the `supervisord` + `tabbify-runner` symlinks under `install_dir`, promote
/// the prior version into the VERSION ledger's `previous` list, then trigger a
/// unit restart via `restart`.
///
/// Touches ONLY the binary symlinks + VERSION — never `data_dir` / `runner_dir`
/// / `mesh-identity.json` (spec invariant #2). The full process restart (not an
/// in-process hot-swap) is what re-loads the mesh fabric (spec invariant #1).
///
/// # Errors
/// A symlink re-point or VERSION write failure. A failed restart trigger is NOT
/// fatal here — the post-swap watchdog observes liveness and rolls back.
pub async fn swap_to(
    version_dir: &Path,
    version: &str,
    install_dir: &Path,
    restart: &RestartRunner,
) -> Result<()> {
    for bin in SWAP_BINARIES {
        repoint_symlink(install_dir, bin, &version_dir.join(bin))?;
    }

    let current = read_version_file(install_dir).unwrap_or(VersionFile {
        current: String::new(),
        previous: Vec::new(),
    });
    let next = push_version(current, version, KEEP_PREVIOUS);
    write_version_file(install_dir, &next)?;

    if !restart(vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]).await {
        tracing::warn!(unit = SUPERVISOR_UNIT, "restart trigger reported failure");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// repoint_symlink atomically points <install>/supervisord at the version
    /// dir's binary, overwriting any pre-existing symlink.
    #[test]
    fn repoint_symlink_points_at_versioned_binary() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let v1 = install.join("releases/v1.0.0");
        let v2 = install.join("releases/v2.0.0");
        std::fs::create_dir_all(&v1).unwrap();
        std::fs::create_dir_all(&v2).unwrap();
        std::fs::write(v1.join("supervisord"), b"v1").unwrap();
        std::fs::write(v2.join("supervisord"), b"v2").unwrap();

        repoint_symlink(install, "supervisord", &v1.join("supervisord")).unwrap();
        assert_eq!(std::fs::read(install.join("supervisord")).unwrap(), b"v1");

        // Re-point to v2 — must overwrite, not error.
        repoint_symlink(install, "supervisord", &v2.join("supervisord")).unwrap();
        assert_eq!(std::fs::read(install.join("supervisord")).unwrap(), b"v2");
    }

    /// release_is_complete is the rollback guard: true only when BOTH binaries
    /// exist as runnable regular files. A dir missing a binary, or one staged
    /// without an executable bit, is rejected — that is what stops the watchdog
    /// from re-pointing a symlink at a missing target.
    #[test]
    fn release_is_complete_requires_both_runnable_binaries() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("v1.0.0");
        std::fs::create_dir_all(&dir).unwrap();

        // Empty dir: incomplete.
        assert!(!release_is_complete(&dir));

        // Only one binary staged: still incomplete.
        let runner = dir.join("tabbify-runner");
        std::fs::write(&runner, b"runner").unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!release_is_complete(&dir));

        // Second binary present but NOT executable: rejected.
        let sup = dir.join("supervisord");
        std::fs::write(&sup, b"sup").unwrap();
        std::fs::set_permissions(&sup, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!release_is_complete(&dir), "non-executable binary must be rejected");

        // Both present and executable: complete.
        std::fs::set_permissions(&sup, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(release_is_complete(&dir));
    }

    /// A dangling staged symlink (target gone) counts as missing, so a release
    /// dir whose binary is a broken symlink is NOT a valid rollback target.
    #[test]
    fn release_is_complete_rejects_dangling_staged_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("v1.0.0");
        std::fs::create_dir_all(&dir).unwrap();
        // tabbify-runner -> nonexistent target.
        symlink(tmp.path().join("gone"), dir.join("tabbify-runner")).unwrap();
        std::fs::write(dir.join("supervisord"), b"sup").unwrap();
        assert!(!release_is_complete(&dir));
    }

    /// write_version_file atomically writes VERSION and keeps the previous list.
    #[test]
    fn write_version_file_records_current_and_previous() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let vf = VersionFile {
            current: "v2.0.0".into(),
            previous: vec!["v1.0.0".into()],
        };
        write_version_file(install, &vf).unwrap();

        let loaded = read_version_file(install).unwrap();
        assert_eq!(loaded.current, "v2.0.0");
        assert_eq!(loaded.previous, vec!["v1.0.0".to_owned()]);
    }

    /// push_version derives the next VersionFile: old current becomes the head
    /// of previous (capped at N), new current is set.
    #[test]
    fn push_version_promotes_old_current_into_previous() {
        let before = VersionFile {
            current: "v1.0.0".into(),
            previous: vec![],
        };
        let after = push_version(before, "v2.0.0", 3);
        assert_eq!(after.current, "v2.0.0");
        assert_eq!(after.previous, vec!["v1.0.0".to_owned()]);
    }

    /// swap_to composes the leaf helpers: it re-points BOTH binary symlinks at
    /// the staged version dir, promotes the prior current into the VERSION
    /// ledger's previous list, and triggers the restart exactly once with the
    /// systemctl arguments. The restart side-effect itself is not unit-tested —
    /// here a no-op closure (the [`RestartRunner`] seam) only records the call.
    #[tokio::test]
    async fn swap_to_repoints_symlinks_records_version_and_triggers_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();

        // A pre-existing live version so the promotion path is exercised.
        write_version_file(
            install,
            &VersionFile {
                current: "v1.0.0".into(),
                previous: vec![],
            },
        )
        .unwrap();

        // Stage the new version's binaries under releases/v2.0.0.
        let v2 = install.join("releases/v2.0.0");
        std::fs::create_dir_all(&v2).unwrap();
        for bin in SWAP_BINARIES {
            std::fs::write(v2.join(bin), format!("v2-{bin}")).unwrap();
        }

        // No-op restart seam: record every invocation instead of poking systemd.
        let calls: Arc<std::sync::Mutex<Vec<Vec<String>>>> = Arc::default();
        let recorded = Arc::clone(&calls);
        let restart: RestartRunner = Arc::new(move |args: Vec<String>| {
            let recorded = Arc::clone(&recorded);
            Box::pin(async move {
                recorded.lock().unwrap().push(args);
                true
            })
        });

        swap_to(&v2, "v2.0.0", install, &restart).await.unwrap();

        // Both symlinks now resolve to the freshly staged binaries.
        for bin in SWAP_BINARIES {
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v2-{bin}").into_bytes(),
                "{bin} symlink must point at the new version dir",
            );
            assert_eq!(
                std::fs::read_link(install.join(bin)).unwrap(),
                v2.join(bin),
            );
        }

        // VERSION ledger: new current, old current promoted to previous.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0");
        assert_eq!(vf.previous, vec!["v1.0.0".to_owned()]);

        // Restart triggered exactly once with the expected systemctl arguments.
        let calls = calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![vec!["restart".to_owned(), SUPERVISOR_UNIT.to_owned()]],
        );
    }
}
