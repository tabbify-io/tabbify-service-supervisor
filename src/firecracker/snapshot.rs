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

/// Returns `true` iff both snapshot files exist in `cache_dir`, meaning a
/// warm load is possible. The cached-vs-cold launch decision is made here so
/// it can be unit-tested on macOS without a real VM or KVM.
pub fn files_present(cache_dir: &Path) -> bool {
    vmstate_path(cache_dir).is_file() && mem_path(cache_dir).is_file()
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
