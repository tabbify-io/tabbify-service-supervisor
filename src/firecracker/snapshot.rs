//! Snapshot file paths derived from an app's cache directory.
//!
//! Both files must exist for a warm start to be possible:
//! - `snap.vmstate` — Firecracker vmstate (small, ~KB).
//! - `snap.mem`     — guest RAM dump (large, equals `mem_size_mib`).
//!
//! # Notes
//! Snapshots are host-kernel + CPU-template specific. Because the supervisor
//! only creates and consumes snapshots on the same host (same kernel, same CPU),
//! this is safe. Cross-host snapshot reuse (e.g. migrating an app to a new
//! supervisor) must NOT be attempted without verifying CPU/kernel compatibility —
//! that is future work and out of scope here.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::path::{Path, PathBuf};

/// Path to the vmstate file in `cache_dir`.
pub fn vmstate_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("snap.vmstate")
}

/// Path to the RAM dump file in `cache_dir`.
pub fn mem_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("snap.mem")
}

/// Path to the companion `.snapshot_ref` file in `cache_dir`. It records the
/// `image_ref` (or its digest) the snapshot was created from, so a warm restore
/// can be REJECTED when the deployed image_ref has changed (the snapshot cache
/// is keyed by UUID only, not by image digest — see `ref_matches`).
pub fn ref_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(".snapshot_ref")
}

/// Returns `true` iff both snapshot files exist in `cache_dir`, meaning a
/// warm load is possible. The cached-vs-cold launch decision is made here so
/// it can be unit-tested on macOS without a real VM or KVM.
pub fn files_present(cache_dir: &Path) -> bool {
    vmstate_path(cache_dir).is_file() && mem_path(cache_dir).is_file()
}

/// Record the `image_ref` a freshly-created snapshot was built from. Best-effort
/// (logs on failure): a missing/garbled `.snapshot_ref` is treated by
/// [`ref_matches`] as "no usable snapshot" → cold boot, which is the safe path.
pub fn write_ref(cache_dir: &Path, image_ref: &str) {
    let p = ref_path(cache_dir);
    if let Err(e) = std::fs::write(&p, image_ref.as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .snapshot_ref");
    }
}

/// Is the snapshot in `cache_dir` valid for `image_ref`?
///
/// Requires BOTH snapshot files present AND the stored `.snapshot_ref` to MATCH
/// `image_ref`. Any mismatch — a different image_ref (redeploy of a new image),
/// a missing `.snapshot_ref` (snapshot from before this check existed), or a
/// read error — returns `false` so the caller COLD-boots (re-creating a fresh
/// snapshot + a new `.snapshot_ref`). This is the fix for warm-restoring STALE
/// image content after a redeploy: the snapshot cache is keyed by UUID only, so
/// without this guard an `on_request` respawn after a new-image deploy would
/// resurrect the old image from the snapshot. Safe-by-default: on ANY doubt we
/// cold boot (slower, never stale).
pub fn ref_matches(cache_dir: &Path, image_ref: &str) -> bool {
    if !files_present(cache_dir) {
        return false;
    }
    match std::fs::read_to_string(ref_path(cache_dir)) {
        Ok(stored) => stored == image_ref,
        Err(_) => false,
    }
}

/// Remove any snapshot files in `cache_dir` (best-effort). Called on a deploy:
/// the snapshot is keyed per-uuid (not per-image), so a stale snapshot from the
/// PREVIOUS image must NOT be warm-restored over the newly-deployed one. After
/// clearing, the deploy's cold boot recreates a fresh snapshot for the new
/// image, keeping later restarts correct.
pub fn clear(cache_dir: &Path) {
    let _ = std::fs::remove_file(vmstate_path(cache_dir));
    let _ = std::fs::remove_file(mem_path(cache_dir));
    // Drop the companion ref too so a later `ref_matches` never reads a stale
    // ref against orphaned/recreated snapshot files.
    let _ = std::fs::remove_file(ref_path(cache_dir));
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn files_present_false_when_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!files_present(dir.path()));
    }

    #[test]
    fn files_present_false_when_only_vmstate_exists() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        assert!(!files_present(dir.path()));
    }

    #[test]
    fn files_present_false_when_only_mem_exists() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        assert!(!files_present(dir.path()));
    }

    #[test]
    fn files_present_true_when_both_files_exist() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        assert!(files_present(dir.path()));
    }

    /// A snapshot whose `.snapshot_ref` matches the requested image_ref is a
    /// valid warm-restore candidate.
    #[test]
    fn ref_matches_true_when_files_present_and_ref_matches() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        write_ref(dir.path(), "reg:5000/a/b@sha256:abc");
        assert!(ref_matches(dir.path(), "reg:5000/a/b@sha256:abc"));
    }

    /// A redeploy of a NEW image_ref over an existing snapshot must NOT warm-
    /// restore (the snapshot holds the OLD image): a mismatched `.snapshot_ref`
    /// forces cold boot.
    #[test]
    fn ref_matches_false_when_ref_differs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        write_ref(dir.path(), "reg:5000/a/b@sha256:OLD");
        assert!(
            !ref_matches(dir.path(), "reg:5000/a/b@sha256:NEW"),
            "a changed image_ref must invalidate the snapshot → cold boot"
        );
    }

    /// A snapshot from before `.snapshot_ref` existed (files present, no ref
    /// file) is treated as "no usable snapshot" → cold boot (safe default).
    #[test]
    fn ref_matches_false_when_ref_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        assert!(!ref_matches(dir.path(), "reg:5000/a/b@sha256:abc"));
    }

    /// Even with a matching ref file, a missing snapshot file means no warm
    /// restore is possible.
    #[test]
    fn ref_matches_false_when_snapshot_files_absent() {
        let dir = tempfile::tempdir().unwrap();
        write_ref(dir.path(), "reg:5000/a/b@sha256:abc");
        assert!(!ref_matches(dir.path(), "reg:5000/a/b@sha256:abc"));
    }

    /// `clear` removes the `.snapshot_ref` companion alongside the snapshot
    /// files so no stale ref survives a deploy.
    #[test]
    fn clear_removes_ref_file_too() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        write_ref(dir.path(), "reg:5000/a/b@sha256:abc");
        clear(dir.path());
        assert!(!ref_path(dir.path()).exists(), "clear must drop .snapshot_ref");
        assert!(!files_present(dir.path()));
    }

    #[test]
    fn vmstate_and_mem_paths_are_deterministic() {
        let base = Path::new("/data/apps/abc/cache");
        assert_eq!(
            vmstate_path(base),
            PathBuf::from("/data/apps/abc/cache/snap.vmstate")
        );
        assert_eq!(
            mem_path(base),
            PathBuf::from("/data/apps/abc/cache/snap.mem")
        );
    }
}
