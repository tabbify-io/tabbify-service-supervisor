//! Rootfs variant bookkeeping for the per-uuid cache
//! (`apps/<uuid>/fc/<digest>/<env_hash>/rootfs.ext4`).
//!
//! Two jobs, both born from the stale-caps incident (a rotated
//! `forge-admin.token` silently reusing a cached rootfs baked with the dead
//! value):
//!
//! 1. **Miss attribution** — every rootfs conversion writes a
//!    [`FingerprintManifest`] (`fingerprint.json`) next to its `rootfs.ext4`:
//!    per-key blake3 DIGESTS of the baked env + cap-file values (never a raw
//!    value). On a later cache MISS the new manifest is diffed against the
//!    cached sibling's, so the log says exactly WHICH key component differed
//!    ("cap value rotated for forge-admin.token") instead of a bare miss.
//! 2. **Stale-variant GC** — a rotation re-bakes at a NEW `<env_hash>` dir,
//!    stranding the old ~2.3 GB `rootfs.ext4`. [`prune_stale_variants`] removes
//!    sibling variant dirs under the same digest after every launch. Removal is
//!    safe mid-use: the rootfs is opened read-only, and Linux keeps an
//!    unlinked-but-open inode alive until the VM exits.
//!
//! Log hygiene: manifests and diffs carry key NAMES and value DIGESTS only.
//! Raw env/cap values never reach a manifest, a diff, or a log line.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The manifest file written next to each cached `rootfs.ext4`.
pub const FINGERPRINT_MANIFEST: &str = "fingerprint.json";

/// Short (8-hex, 32-bit) blake3 digest of a baked value. Enough to TELL APART
/// two generations of the same key for diagnostics; deliberately truncated so it
/// is useless as a verifier of the value itself.
fn value_digest(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex()[..8].to_string()
}

/// Per-variant record of WHAT was baked into a cached rootfs's `/init`, in
/// diff-able (digest) form. Written at conversion time, read on a later cache
/// miss to attribute the miss to the exact differing component.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FingerprintManifest {
    /// The full [`super::firecracker::rootfs_env_fingerprint`] this variant is
    /// keyed by (its `<env_hash>` dir name).
    pub fingerprint: String,
    /// Effective env: key → 8-hex blake3(value). Keys are non-secret; values
    /// are stored ONLY as truncated digests.
    pub env: BTreeMap<String, String>,
    /// Cap-files: name → 8-hex blake3(value). Same digest-only rule.
    pub cap_values: BTreeMap<String, String>,
}

impl FingerprintManifest {
    /// Build the manifest for the inputs a conversion is about to bake.
    #[must_use]
    pub fn compute(
        fingerprint: &str,
        extra_env: Option<&std::collections::HashMap<String, String>>,
        cap_files: &[(String, String)],
    ) -> Self {
        let env = extra_env
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), value_digest(v)))
                    .collect()
            })
            .unwrap_or_default();
        let cap_values = cap_files
            .iter()
            .map(|(n, v)| (n.clone(), value_digest(v)))
            .collect();
        Self {
            fingerprint: fingerprint.to_owned(),
            env,
            cap_values,
        }
    }

    /// Persist into `variant_dir` (the `<env_hash>` dir holding `rootfs.ext4`).
    /// Best-effort: a write failure only forfeits future miss attribution.
    pub fn save(&self, variant_dir: &Path) {
        let path = variant_dir.join(FINGERPRINT_MANIFEST);
        match serde_json::to_vec_pretty(self) {
            Ok(bytes) => {
                if let Err(e) = std::fs::write(&path, bytes) {
                    tracing::warn!(path = %path.display(), error = %e, "fingerprint manifest write failed (miss attribution degraded)");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "fingerprint manifest serialize failed");
            }
        }
    }

    /// Load the manifest from `variant_dir`, `None` when absent/malformed (a
    /// variant built before value-aware fingerprinting has none).
    #[must_use]
    pub fn load(variant_dir: &Path) -> Option<Self> {
        let bytes = std::fs::read(variant_dir.join(FINGERPRINT_MANIFEST)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Diff `self` (the CACHED generation) against `wanted` (what THIS build
    /// declares). Key names only — safe to log verbatim.
    #[must_use]
    pub fn diff(&self, wanted: &Self) -> FingerprintDiff {
        fn keys_diff(
            old: &BTreeMap<String, String>,
            new: &BTreeMap<String, String>,
        ) -> (Vec<String>, Vec<String>, Vec<String>) {
            let added = new
                .keys()
                .filter(|k| !old.contains_key(*k))
                .cloned()
                .collect();
            let removed = old
                .keys()
                .filter(|k| !new.contains_key(*k))
                .cloned()
                .collect();
            let changed = new
                .iter()
                .filter(|(k, v)| old.get(*k).is_some_and(|ov| ov != *v))
                .map(|(k, _)| k.clone())
                .collect();
            (added, removed, changed)
        }
        let (env_added, env_removed, env_value_changed) = keys_diff(&self.env, &wanted.env);
        let (caps_added, caps_removed, cap_values_rotated) =
            keys_diff(&self.cap_values, &wanted.cap_values);
        FingerprintDiff {
            env_added,
            env_removed,
            env_value_changed,
            caps_added,
            caps_removed,
            cap_values_rotated,
        }
    }
}

/// Which key components differ between a cached variant and the wanted build.
/// All fields are key NAMES (loggable); no value or value-digest rides here.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FingerprintDiff {
    /// Env keys present in the wanted build but not the cached variant.
    pub env_added: Vec<String>,
    /// Env keys the cached variant baked that the wanted build no longer has.
    pub env_removed: Vec<String>,
    /// Env keys present in both whose VALUE digest differs.
    pub env_value_changed: Vec<String>,
    /// Cap-file names new in the wanted build (an `add_repo` clone cap).
    pub caps_added: Vec<String>,
    /// Cap-file names the wanted build dropped.
    pub caps_removed: Vec<String>,
    /// Cap-file names whose VALUE rotated (the stale-caps incident class:
    /// a rotated `forge-admin.token` with an unchanged name set).
    pub cap_values_rotated: Vec<String>,
}

impl FingerprintDiff {
    /// No component differs (same generation).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.env_added.is_empty()
            && self.env_removed.is_empty()
            && self.env_value_changed.is_empty()
            && self.caps_added.is_empty()
            && self.caps_removed.is_empty()
            && self.cap_values_rotated.is_empty()
    }
}

impl std::fmt::Display for FingerprintDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return write!(f, "no component differs");
        }
        let mut parts: Vec<String> = Vec::new();
        let mut push = |label: &str, keys: &[String]| {
            if !keys.is_empty() {
                parts.push(format!("{label}: [{}]", keys.join(", ")));
            }
        };
        push("env keys added", &self.env_added);
        push("env keys removed", &self.env_removed);
        push("env values changed for", &self.env_value_changed);
        push("cap-files added", &self.caps_added);
        push("cap-files removed", &self.caps_removed);
        push("cap values rotated for", &self.cap_values_rotated);
        write!(f, "{}", parts.join("; "))
    }
}

/// Human-readable WHY for the orchestrator's force-cold-on-env-change gate: diff
/// the persisted (live-runtime) deploy env against this deploy's env, through
/// the SAME split the rootfs bake uses. Key names only — loggable verbatim.
#[must_use]
pub fn describe_env_change(
    old_env: Option<&std::collections::HashMap<String, String>>,
    new_env: Option<&std::collections::HashMap<String, String>>,
) -> String {
    let manifest_of = |env: Option<&std::collections::HashMap<String, String>>| {
        let (eff, caps) = super::firecracker::split_env_and_caps(env);
        let eff_ref = if eff.is_empty() { None } else { Some(&eff) };
        FingerprintManifest::compute(
            &super::firecracker::rootfs_env_fingerprint(eff_ref, &caps),
            eff_ref,
            &caps,
        )
    };
    manifest_of(old_env).diff(&manifest_of(new_env)).to_string()
}

/// On a per-uuid rootfs cache MISS, explain WHY by diffing the wanted manifest
/// against the cached sibling variants under the digest dir. One log line per
/// sibling (there is normally at most one). Best-effort + read-only.
pub fn log_cache_miss_attribution(
    uuid: &str,
    digest: &str,
    digest_dir: &Path,
    wanted: &FingerprintManifest,
) {
    let Ok(entries) = std::fs::read_dir(digest_dir) else {
        tracing::info!(
            uuid,
            digest,
            env_hash = %wanted.fingerprint,
            "rootfs cache miss: no prior variant for this digest (first build for this image)"
        );
        return;
    };
    let mut saw_sibling = false;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || entry.file_name().to_string_lossy() == wanted.fingerprint {
            continue;
        }
        saw_sibling = true;
        match FingerprintManifest::load(&path) {
            Some(cached) => {
                tracing::info!(
                    uuid,
                    digest,
                    wanted_env_hash = %wanted.fingerprint,
                    cached_env_hash = %cached.fingerprint,
                    differing = %cached.diff(wanted),
                    "rootfs cache miss: env/cap fingerprint changed vs cached variant → re-bake (key names only; values never logged)"
                );
            }
            None => {
                tracing::info!(
                    uuid,
                    digest,
                    wanted_env_hash = %wanted.fingerprint,
                    cached_variant = %entry.file_name().to_string_lossy(),
                    "rootfs cache miss: cached variant has no fingerprint manifest (built before value-aware fingerprinting); cannot attribute the differing component"
                );
            }
        }
    }
    if !saw_sibling {
        tracing::info!(
            uuid,
            digest,
            env_hash = %wanted.fingerprint,
            "rootfs cache miss: no prior variant for this digest (first build for this image)"
        );
    }
}

/// Remove STALE sibling variant dirs (older env/cap generations) under
/// `apps/<uuid>/fc/<digest-sanitized>/`, keeping ONLY `keep_env_hash` — the
/// variant the current launch runs. A cap rotation re-bakes ~2.3 GB at a new
/// `<env_hash>`; without pruning, generations accumulate until the disk fills
/// (this host has had that outage). Called once per launch, after the winning
/// variant is known.
///
/// Safe mid-use: the rootfs is attached read-only and an unlinked-but-open inode
/// outlives the removal until the (old) VM exits. Best-effort: a failed removal
/// is logged and retried implicitly on the next launch.
pub async fn prune_stale_variants(data_dir: &Path, uuid: &str, digest: &str, keep_env_hash: &str) {
    let digest_dir = data_dir
        .join("apps")
        .join(uuid)
        .join("fc")
        .join(digest.replace(':', "-"));
    let Ok(mut rd) = tokio::fs::read_dir(&digest_dir).await else {
        return; // no dir yet (first build) — nothing to prune
    };
    while let Ok(Some(entry)) = rd.next_entry().await {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !path.is_dir() || name == keep_env_hash {
            continue;
        }
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {
                tracing::info!(
                    uuid,
                    digest,
                    stale_variant = %name,
                    kept_variant = %keep_env_hash,
                    path = %path.display(),
                    "pruned stale rootfs variant (older env/cap generation; disk reclaimed)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    uuid,
                    digest,
                    stale_variant = %name,
                    error = %e,
                    "stale rootfs variant prune failed (will retry on next launch)"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// Manifest round-trips through its variant-dir file and diffs to empty
    /// against itself.
    #[test]
    fn manifest_save_load_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let m = FingerprintManifest::compute(
            "abcd1234abcd1234",
            Some(&env(&[("TABBIFY_FORGE_ORG", "t_acme")])),
            &[("forge-admin.token".to_owned(), "SECRET-VALUE".to_owned())],
        );
        m.save(tmp.path());
        let loaded = FingerprintManifest::load(tmp.path()).expect("manifest loads back");
        assert_eq!(loaded, m);
        assert!(m.diff(&loaded).is_empty());
    }

    /// A missing / malformed manifest loads as `None` (pre-upgrade variants).
    #[test]
    fn manifest_load_absent_or_malformed_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(FingerprintManifest::load(tmp.path()).is_none());
        std::fs::write(tmp.path().join(FINGERPRINT_MANIFEST), b"not json").unwrap();
        assert!(FingerprintManifest::load(tmp.path()).is_none());
    }

    /// SECRET HYGIENE: neither the persisted manifest bytes nor the diff's
    /// Display carry a raw env/cap value — only key names and 8-hex digests.
    #[test]
    fn manifest_and_diff_never_carry_raw_values() {
        let tmp = tempfile::tempdir().unwrap();
        let old = FingerprintManifest::compute(
            "aaaaaaaaaaaaaaaa",
            Some(&env(&[("TABBIFY_USER_ID", "acct_SECRET_ENV")])),
            &[(
                "forge-admin.token".to_owned(),
                "OLD-SECRET-TOKEN".to_owned(),
            )],
        );
        let new = FingerprintManifest::compute(
            "bbbbbbbbbbbbbbbb",
            Some(&env(&[("TABBIFY_USER_ID", "acct_SECRET_ENV")])),
            &[(
                "forge-admin.token".to_owned(),
                "NEW-SECRET-TOKEN".to_owned(),
            )],
        );
        old.save(tmp.path());
        let on_disk = std::fs::read_to_string(tmp.path().join(FINGERPRINT_MANIFEST)).unwrap();
        for leak in ["OLD-SECRET-TOKEN", "acct_SECRET_ENV"] {
            assert!(!on_disk.contains(leak), "manifest leaked {leak}");
        }
        let rendered = old.diff(&new).to_string();
        for leak in ["OLD-SECRET-TOKEN", "NEW-SECRET-TOKEN", "acct_SECRET_ENV"] {
            assert!(!rendered.contains(leak), "diff Display leaked {leak}");
        }
        // The diff still NAMES the rotated component — the whole point.
        assert!(rendered.contains("cap values rotated for: [forge-admin.token]"));
    }

    /// The diff attributes each component class: added/removed keys and
    /// changed values, for env and cap-files independently.
    #[test]
    fn diff_attributes_each_component() {
        let old = FingerprintManifest::compute(
            "aaaaaaaaaaaaaaaa",
            Some(&env(&[("KEEP", "same"), ("GONE", "x"), ("FLIP", "old")])),
            &[
                ("app.url".to_owned(), "http://g/1".to_owned()),
                ("dead.cap".to_owned(), "x".to_owned()),
            ],
        );
        let new = FingerprintManifest::compute(
            "bbbbbbbbbbbbbbbb",
            Some(&env(&[("KEEP", "same"), ("FLIP", "new"), ("BORN", "y")])),
            &[
                ("app.url".to_owned(), "http://g/2".to_owned()),
                ("fresh.cap".to_owned(), "y".to_owned()),
            ],
        );
        let d = old.diff(&new);
        assert_eq!(d.env_added, vec!["BORN"]);
        assert_eq!(d.env_removed, vec!["GONE"]);
        assert_eq!(d.env_value_changed, vec!["FLIP"]);
        assert_eq!(d.caps_added, vec!["fresh.cap"]);
        assert_eq!(d.caps_removed, vec!["dead.cap"]);
        assert_eq!(d.cap_values_rotated, vec!["app.url"]);
        assert!(!d.is_empty());
    }

    /// `describe_env_change` goes through the SAME `split_env_and_caps` the bake
    /// uses, so a rotated cap value inside `CAP_FILES_ENV` is attributed as a
    /// CAP rotation (not an opaque env change of the reserved key).
    #[test]
    fn describe_env_change_attributes_cap_rotation_inside_cap_files_env() {
        let old = env(&[
            ("TABBIFY_WORKSPACE_UUID", "ws-1"),
            (
                crate::api::CAP_FILES_ENV,
                r#"{"forge-admin.token":"OLD-SECRET"}"#,
            ),
        ]);
        let new = env(&[
            ("TABBIFY_WORKSPACE_UUID", "ws-1"),
            (
                crate::api::CAP_FILES_ENV,
                r#"{"forge-admin.token":"NEW-SECRET"}"#,
            ),
        ]);
        let why = describe_env_change(Some(&old), Some(&new));
        assert!(
            why.contains("cap values rotated for: [forge-admin.token]"),
            "got: {why}"
        );
        assert!(!why.contains("OLD-SECRET") && !why.contains("NEW-SECRET"));
    }

    /// GC removes every sibling variant dir under the digest dir, keeps the
    /// current one, and never touches other digests or the uuid's non-digest
    /// dirs (`.pull` / `.layout` live at the fc/ level, outside the digest dir).
    #[tokio::test]
    async fn prune_removes_stale_siblings_and_keeps_current() {
        let tmp = tempfile::tempdir().unwrap();
        let digest = "sha256:cafe";
        let fc = tmp.path().join("apps").join("ws-1").join("fc");
        let digest_dir = fc.join("sha256-cafe");
        for variant in ["oldhash111111111", "keephash22222222"] {
            std::fs::create_dir_all(digest_dir.join(variant)).unwrap();
            std::fs::write(digest_dir.join(variant).join("rootfs.ext4"), b"\0").unwrap();
        }
        // A DIFFERENT digest's variant + the uuid-level work dirs must survive.
        std::fs::create_dir_all(fc.join("sha256-beef").join("otherhash3333333")).unwrap();
        std::fs::create_dir_all(fc.join(".pull")).unwrap();

        prune_stale_variants(tmp.path(), "ws-1", digest, "keephash22222222").await;

        assert!(
            !digest_dir.join("oldhash111111111").exists(),
            "stale variant pruned"
        );
        assert!(
            digest_dir
                .join("keephash22222222")
                .join("rootfs.ext4")
                .exists()
        );
        assert!(fc.join("sha256-beef").join("otherhash3333333").exists());
        assert!(fc.join(".pull").exists());
    }

    /// Pruning a digest dir that does not exist yet (first build) is a no-op.
    #[tokio::test]
    async fn prune_tolerates_missing_digest_dir() {
        let tmp = tempfile::tempdir().unwrap();
        prune_stale_variants(tmp.path(), "ws-none", "sha256:none", "somehash").await;
    }
}
