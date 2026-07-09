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

/// Path to the companion `.snapshot_env` file in `cache_dir`. It records the
/// #106 env/cap FINGERPRINT (`rootfs_env_fingerprint`) the snapshot's guest
/// `/init` was baked with, so a warm restore can be REJECTED when the deploy's
/// effective env/cap set changed even though the image ref/digest did NOT — e.g.
/// a workspace `add_repo` adds a new clone cap (SAME image, NEW `/init`). Without
/// this the env-blind `.snapshot_ref` gate would warm-restore the STALE guest and
/// the broker's boot-clone would never run for the new repo (#108).
pub fn env_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(".snapshot_env")
}

/// Record the env/cap fingerprint a freshly-created snapshot was baked with.
/// Best-effort (logs on failure): a missing/garbled `.snapshot_env` is treated
/// by [`restore_matches`] as "no usable snapshot" → cold boot (the safe path).
pub fn write_env(cache_dir: &Path, env_hash: &str) {
    let p = env_path(cache_dir);
    if let Err(e) = std::fs::write(&p, env_hash.as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .snapshot_env");
    }
}

/// Does the snapshot's stamped env fingerprint match `env_hash`? A missing or
/// unreadable `.snapshot_env` returns `false` (safe default: cold boot). Kept
/// private — callers use [`restore_matches`], which ANDs this with the image-ref
/// check so a warm restore requires BOTH the image AND the env to be unchanged.
fn env_matches(cache_dir: &Path, env_hash: &str) -> bool {
    match std::fs::read_to_string(env_path(cache_dir)) {
        Ok(stored) => stored == env_hash,
        Err(_) => false,
    }
}

/// Is the snapshot in `cache_dir` a valid warm-restore candidate for BOTH
/// `image_ref` AND `env_hash`?
///
/// A warm restore resurrects the FULL guest (rootfs + frozen RAM), so it is
/// sound ONLY when neither the image NOR the `/init`-baked env/cap set changed.
/// [`ref_matches`] guards the image; [`env_matches`] guards the env (the #106
/// `rootfs_env_fingerprint`). Any mismatch, a missing companion (a snapshot from
/// before this check existed), or a read error returns `false` so the caller
/// COLD-boots — which re-runs `/init` (a workspace's broker then re-clones the
/// new cap set, #108) and re-stamps fresh `.snapshot_ref` + `.snapshot_env`
/// companions. Safe-by-default: on ANY doubt we cold boot (slower, never stale).
pub fn restore_matches(cache_dir: &Path, image_ref: &str, env_hash: &str) -> bool {
    ref_matches(cache_dir, image_ref) && env_matches(cache_dir, env_hash)
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
    // Drop the env companion for the SAME reason: a stale `.snapshot_env` must
    // not outlive the snapshot files it described (else a later `restore_matches`
    // could read a fingerprint that no longer matches any on-disk snapshot).
    let _ = std::fs::remove_file(env_path(cache_dir));
}

/// Path to the companion `.app_port` file in `cache_dir`. It records the IMAGE's
/// own declared port (the lowest `ExposedPorts` TCP entry — see
/// `resolve_port`'s tier 2) that this app was launched with, so a LATER launch
/// that SKIPS the OCI-config read — a cold respawn or a warm restore that hits the
/// per-uuid rootfs/snapshot cache without re-pulling the image — can still target
/// the app's OWN port instead of falling back to 8080. Written by the production
/// `launch_with_uuid` path when the image port is known, read back when it is not.
pub fn app_port_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(".app_port")
}

/// Persist the IMAGE's exposed port for this uuid so a later config-read-less
/// launch (cache-hit respawn / warm restore) recovers it. Best-effort: a write
/// failure just means the next such launch falls back to the 8080 default (the
/// pre-existing behaviour, never worse). Creates `cache_dir` if absent so the
/// write does not fail on a first cold boot that has not yet made the dir.
pub fn write_app_port(cache_dir: &Path, port: u16) {
    if let Err(e) = std::fs::create_dir_all(cache_dir) {
        tracing::warn!(path = %cache_dir.display(), error = %e, "app-port persist: mkdir failed");
        return;
    }
    let p = app_port_path(cache_dir);
    if let Err(e) = std::fs::write(&p, port.to_string().as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .app_port");
    }
}

/// Read the IMAGE's exposed port persisted by [`write_app_port`], or `None` if
/// absent/garbled (a snapshot from before this companion existed, or a first
/// launch). `None` makes `resolve_port` fall through to its 8080 default — the
/// safe, unchanged behaviour.
#[must_use]
pub fn read_app_port(cache_dir: &Path) -> Option<u16> {
    std::fs::read_to_string(app_port_path(cache_dir))
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
}

/// The per-app snapshot/cache directory: `<data_dir>/apps/<uuid>/cache`. Single
/// source of the convention shared by the firecracker launcher (`launch_with_uuid`)
/// and the dev-session snapshot-suppression below.
pub fn cache_dir(data_dir: &Path, uuid: &str) -> PathBuf {
    data_dir.join("apps").join(uuid).join("cache")
}

/// Path to the `.no-snapshot` marker in `cache_dir`. Its presence SUPPRESSES
/// warm-start snapshot creation for this app (see [`is_suppressed`]).
pub fn suppress_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(".no-snapshot")
}

/// Mark `cache_dir` so the firecracker cold boot NEVER creates a warm-start
/// snapshot for this app.
///
/// Used for DEV-SESSIONS: the dev guest's `/init` clones `/workspace`
/// ASYNCHRONOUSLY — it finishes AFTER the host readiness probe (the app port
/// answers) returns, which is exactly when the cold boot would snapshot. So a
/// snapshot would freeze a pre-/mid-clone rootfs, and a later warm-restore
/// (e.g. a crash respawn) would resurrect an EMPTY `/workspace`. Suppressing the
/// snapshot forces every (re)launch to COLD-boot, which re-runs `/init` and
/// re-clones `/workspace` correctly (self-healing). Best-effort: a write failure
/// just means a snapshot MAY still be taken — the pre-existing behaviour, never
/// worse.
pub fn suppress(cache_dir: &Path) {
    if let Err(e) = std::fs::create_dir_all(cache_dir) {
        tracing::warn!(path = %cache_dir.display(), error = %e, "snapshot suppress: mkdir failed");
        return;
    }
    if let Err(e) = std::fs::write(suppress_path(cache_dir), b"") {
        tracing::warn!(path = %suppress_path(cache_dir).display(), error = %e, "snapshot suppress: write failed");
    }
}

/// Is snapshot creation suppressed for `cache_dir` (the `.no-snapshot` marker
/// present)? Checked by the firecracker cold boot before creating a snapshot.
pub fn is_suppressed(cache_dir: &Path) -> bool {
    suppress_path(cache_dir).is_file()
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

    /// Helper: lay down a complete warm-restore candidate (both snapshot files +
    /// matching ref + matching env companion).
    fn seed_snapshot(dir: &Path, image_ref: &str, env_hash: &str) {
        std::fs::write(vmstate_path(dir), b"vmstate").unwrap();
        std::fs::write(mem_path(dir), b"mem").unwrap();
        write_ref(dir, image_ref);
        write_env(dir, env_hash);
    }

    /// A snapshot whose BOTH image_ref AND env-hash match is a valid warm-restore
    /// candidate (#108: the env-aware gate must still permit an unchanged
    /// workspace to warm-restore, preserving the warm-LSP index).
    #[test]
    fn restore_matches_true_when_ref_and_env_match() {
        let dir = tempfile::tempdir().unwrap();
        seed_snapshot(dir.path(), "reg:5000/a/b@sha256:abc", "envhash-1");
        assert!(restore_matches(
            dir.path(),
            "reg:5000/a/b@sha256:abc",
            "envhash-1"
        ));
    }

    /// #108 CORE: same image_ref but a CHANGED env-hash (a workspace `add_repo`
    /// added a new clone cap — same image, new `/init`) must NOT warm-restore.
    /// Cold boot instead so the broker re-runs its boot-clone for the new repo.
    #[test]
    fn restore_matches_false_when_env_differs() {
        let dir = tempfile::tempdir().unwrap();
        seed_snapshot(dir.path(), "reg:5000/a/b@sha256:abc", "envhash-OLD");
        assert!(
            !restore_matches(dir.path(), "reg:5000/a/b@sha256:abc", "envhash-NEW"),
            "a changed env/cap fingerprint must invalidate the snapshot → cold boot"
        );
    }

    /// A snapshot from before `.snapshot_env` existed (files + ref present, no env
    /// companion) is treated as "no usable snapshot" → cold boot (safe default).
    /// This is the one-time cost of rolling out the env gate over old snapshots.
    #[test]
    fn restore_matches_false_when_env_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(vmstate_path(dir.path()), b"vmstate").unwrap();
        std::fs::write(mem_path(dir.path()), b"mem").unwrap();
        write_ref(dir.path(), "reg:5000/a/b@sha256:abc");
        assert!(
            !restore_matches(dir.path(), "reg:5000/a/b@sha256:abc", "envhash-1"),
            "a missing .snapshot_env must force cold boot"
        );
    }

    /// A changed image_ref invalidates the snapshot even when the env matches
    /// (the ref guard still applies under `restore_matches`).
    #[test]
    fn restore_matches_false_when_ref_differs_even_if_env_matches() {
        let dir = tempfile::tempdir().unwrap();
        seed_snapshot(dir.path(), "reg:5000/a/b@sha256:OLD", "envhash-1");
        assert!(!restore_matches(
            dir.path(),
            "reg:5000/a/b@sha256:NEW",
            "envhash-1"
        ));
    }

    /// DEEP-ROOT (P1-4) moving-tag snapshot invalidation. The per-uuid snapshot
    /// cache is keyed by UUID; the platform base images (workspace/devbox) deploy
    /// under a MOVING tag (`…:current`). The launcher now stamps/matches the
    /// RESOLVED DIGEST (`sha256:…`) — NOT the tag string — into `.snapshot_ref`.
    /// This test proves the fix: a snapshot taken when `:current` resolved to an
    /// OLD digest must NOT warm-restore once `:current` has advanced to a NEW digest
    /// (a base-image OTA carrying, e.g., the broker cred-reload fix), even though the
    /// env fingerprint is unchanged. If the launcher had stamped the tag string, the
    /// two would match (tag == tag) and resurrect the STALE broker — the exact bug.
    #[test]
    fn restore_matches_false_when_moving_tag_resolves_to_new_digest() {
        let dir = tempfile::tempdir().unwrap();
        // Warm snapshot stamped when `:current` → OLD digest (env unchanged).
        seed_snapshot(dir.path(), "sha256:OLDdigest", "envhash-1");
        // Respawn after the base-image OTA: `:current` now resolves to a NEW digest.
        assert!(
            !restore_matches(dir.path(), "sha256:NEWdigest", "envhash-1"),
            "a moved :current tag (new resolved digest) must invalidate the snapshot → cold boot the new image"
        );
        // Sanity: the SAME resolved digest still warm-restores — the fix must not
        // force a needless cold boot when the tag's content did NOT change.
        assert!(
            restore_matches(dir.path(), "sha256:OLDdigest", "envhash-1"),
            "an UNCHANGED resolved digest must still warm-restore (no spurious cold boot)"
        );
    }

    /// `clear` drops the `.snapshot_env` companion too so no stale env fingerprint
    /// outlives the snapshot files it described.
    #[test]
    fn clear_removes_env_file_too() {
        let dir = tempfile::tempdir().unwrap();
        seed_snapshot(dir.path(), "reg:5000/a/b@sha256:abc", "envhash-1");
        clear(dir.path());
        assert!(
            !env_path(dir.path()).exists(),
            "clear must drop .snapshot_env"
        );
    }

    /// The image's exposed port round-trips through the `.app_port` companion so a
    /// later config-read-less launch (cache-hit respawn / warm restore) recovers it.
    #[test]
    fn app_port_round_trips_through_companion() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_app_port(dir.path()), None, "absent ⇒ None");
        write_app_port(dir.path(), 80);
        assert_eq!(read_app_port(dir.path()), Some(80));
        // Overwrite (e.g. a redeploy to an image exposing a different port).
        write_app_port(dir.path(), 3000);
        assert_eq!(read_app_port(dir.path()), Some(3000));
    }

    /// `write_app_port` creates the cache dir if it does not yet exist (a first
    /// cold boot may persist the port before the snapshot dir is otherwise made).
    #[test]
    fn write_app_port_creates_missing_dir() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("apps").join("u").join("cache"); // not yet created
        assert!(!dir.exists());
        write_app_port(&dir, 8788);
        assert_eq!(read_app_port(&dir), Some(8788));
    }

    /// A garbled `.app_port` reads back as `None` (safe: `resolve_port` then falls
    /// through to its 8080 default rather than probing a bogus port).
    #[test]
    fn read_app_port_none_on_garbled_contents() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(app_port_path(dir.path()), b"not-a-port").unwrap();
        assert_eq!(read_app_port(dir.path()), None);
    }

    /// No `.no-snapshot` marker ⇒ snapshots are NOT suppressed (regular apps).
    #[test]
    fn is_suppressed_false_without_marker() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_suppressed(dir.path()));
    }

    /// `suppress` writes the marker (creating the dir if absent) ⇒ `is_suppressed`
    /// then true, and it does NOT create snapshot files (just the marker).
    #[test]
    fn suppress_creates_marker_and_is_idempotent() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("apps").join("u").join("cache"); // not yet created
        assert!(!is_suppressed(&dir));
        suppress(&dir);
        assert!(is_suppressed(&dir), "marker must exist after suppress");
        assert!(!files_present(&dir), "suppress must not create snapshot files");
        // Idempotent: a second suppress is harmless.
        suppress(&dir);
        assert!(is_suppressed(&dir));
    }

    /// `cache_dir` is the single source of the `<data_dir>/apps/<uuid>/cache`
    /// convention shared with the firecracker launcher.
    #[test]
    fn cache_dir_path_convention() {
        assert_eq!(
            cache_dir(Path::new("/data"), "019ed025"),
            PathBuf::from("/data/apps/019ed025/cache")
        );
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
