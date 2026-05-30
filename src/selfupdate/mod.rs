//! Health-gated self-update engine (spec: node-self-update-health-gated).
//!
//! The supervisor updates itself without losing its mesh identity or its runner
//! fleet: fetch a versioned binary set + verify sha256 ([`fetch`]), probe the
//! candidate out-of-band behind a 3-part health gate, atomically re-point the
//! binary symlinks + record `VERSION`, restart the unit, and roll the symlink
//! back to previous-good if a post-swap watchdog window fails.
//!
//! # Invariants (spec)
//! 1. The in-process mesh fabric is NEVER hot-swapped — only a full process
//!    restart (`Tunn` / `SessionTable` are not serialisable).
//! 2. Rollback touches ONLY the binary symlink — never `data_dir` /
//!    `runner_dir` / `mesh-identity.json`.
//! 3. The gate is held under the coordinator heartbeat-timeout (60s); the
//!    post-swap stability window is ALSO held under it (see
//!    [`COORDINATOR_HEARTBEAT_TIMEOUT`] / [`DEFAULT_STABILITY_WINDOW`]). A
//!    window that outran the heartbeat would let the coordinator GC a bad node
//!    from the roster before the watchdog ever decided to revert it.

use std::path::PathBuf;
use std::time::Duration;

pub mod confirm;
pub mod fetch;
pub mod manifest;
pub mod probe;
pub mod run;
pub mod swap;
pub mod watchdog;

/// Default release base URL (same public bucket as the app artifacts).
/// Overridable via `TABBIFY_RELEASE_BASE_URL` so an operator / NixOS unit can
/// point the engine at the node's release bucket (the versioned-S3 layout is
/// `<base>/supervisor/v<VER>/<arch>/{supervisord,tabbify-runner}` + a sibling
/// `supervisor/latest` manifest).
const DEFAULT_RELEASE_BASE_URL: &str = "https://tabbify-apps.s3.eu-central-1.amazonaws.com";

/// Env var overriding [`DEFAULT_RELEASE_BASE_URL`].
const ENV_RELEASE_BASE_URL: &str = "TABBIFY_RELEASE_BASE_URL";

/// Default install dir holding the live binary symlinks + `VERSION`. Overridable
/// via `TABBIFY_INSTALL_DIR` (the releases dir + candidate identity default
/// under it).
const DEFAULT_INSTALL_DIR: &str = "/opt/tabbify";

/// Env var overriding [`DEFAULT_INSTALL_DIR`].
const ENV_INSTALL_DIR: &str = "TABBIFY_INSTALL_DIR";

/// Env var overriding the staging dir (default `<install_dir>/releases`), under
/// which each fetched version lands as `v<VER>/{supervisord,tabbify-runner}`.
const ENV_RELEASES_DIR: &str = "TABBIFY_RELEASES_DIR";

/// Coordinator heartbeat-timeout: how long the coordinator waits before it
/// garbage-collects a silent node from the mesh roster. Every self-update
/// timer MUST stay comfortably under this so that a bad node is reverted by the
/// watchdog BEFORE the coordinator drops it (a GC'd node can no longer be
/// reasoned about, so the revert window must close first).
pub(crate) const COORDINATOR_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// 3-part health gate timeout — kept under the coordinator heartbeat-timeout.
const DEFAULT_GATE_TIMEOUT: Duration = Duration::from_secs(45);

/// Post-swap stability window — long enough to catch a bad swap, short enough
/// to stay comfortably UNDER [`COORDINATOR_HEARTBEAT_TIMEOUT`].
///
/// INVARIANT: `DEFAULT_STABILITY_WINDOW < COORDINATOR_HEARTBEAT_TIMEOUT`. This
/// is the whole point of I3: the window must close (so the watchdog can decide
/// `KeepNewVersion` vs `Revert`) before the coordinator GC's a bad node from
/// the roster at the heartbeat-timeout. A failed swap is reverted PROMPTLY on
/// the first failing poll regardless of this window (see
/// [`watchdog::decide_revert`]); the window only bounds the time we wait to
/// CONFIRM a healthy swap. Enforced by a unit test.
pub(crate) const DEFAULT_STABILITY_WINDOW: Duration = Duration::from_secs(45);

/// Compile-time enforcement of the I3 invariant: the default stability window
/// (and the gate) must be strictly under the coordinator heartbeat-timeout, so
/// a bad node is reverted before the coordinator GC's it from the roster.
/// Building with a window >= the heartbeat-timeout fails to compile.
const _: () = {
    assert!(
        DEFAULT_STABILITY_WINDOW.as_secs() < COORDINATOR_HEARTBEAT_TIMEOUT.as_secs(),
        "DEFAULT_STABILITY_WINDOW must be < COORDINATOR_HEARTBEAT_TIMEOUT (I3 invariant)",
    );
    assert!(
        DEFAULT_GATE_TIMEOUT.as_secs() < COORDINATOR_HEARTBEAT_TIMEOUT.as_secs(),
        "DEFAULT_GATE_TIMEOUT must be < COORDINATOR_HEARTBEAT_TIMEOUT",
    );
};

/// Static configuration for the self-update engine.
#[derive(Debug, Clone)]
pub struct SelfUpdateConfig {
    /// Release base URL (versioned-S3 layout: `<base>/supervisor/...`).
    pub release_base_url: String,
    /// Target architecture path segment, e.g. `"x86_64"` / `"aarch64"`.
    pub arch: String,
    /// Dir each fetched version is staged into (`<releases_dir>/v<VER>/`).
    pub releases_dir: PathBuf,
    /// Dir holding the live binary symlinks + the `VERSION` ledger.
    pub install_dir: PathBuf,
    /// Transient identity file the out-of-band candidate joins with — NEVER the
    /// sticky `mesh-identity.json`.
    pub candidate_identity_path: PathBuf,
    /// 3-part health gate timeout (must stay under the heartbeat-timeout).
    pub gate_timeout: Duration,
    /// Post-swap watchdog stability window.
    pub stability_window: Duration,
}

impl Default for SelfUpdateConfig {
    /// Build the engine config, honouring the `TABBIFY_RELEASE_BASE_URL`,
    /// `TABBIFY_INSTALL_DIR`, and `TABBIFY_RELEASES_DIR` env overrides so the
    /// NixOS `tabbify-update` unit (different release bucket + install layout)
    /// can drive the binary without changing the code. With no env set the
    /// baked `/opt/tabbify` + public-bucket defaults apply.
    fn default() -> Self {
        Self::from_locations(
            std::env::var(ENV_RELEASE_BASE_URL).ok(),
            std::env::var(ENV_INSTALL_DIR).ok(),
            std::env::var(ENV_RELEASES_DIR).ok(),
        )
    }
}

impl SelfUpdateConfig {
    /// Pure derivation of the layout fields from the three optional env values
    /// (release base URL, install dir, releases dir), so the override logic is
    /// unit-testable without mutating the process environment:
    /// - install dir defaults to `/opt/tabbify`,
    /// - releases dir defaults to `<install_dir>/releases`,
    /// - the candidate identity always lives under the install dir,
    /// - the release base URL defaults to the public app bucket.
    #[must_use]
    fn from_locations(
        release_base_url: Option<String>,
        install_dir: Option<String>,
        releases_dir: Option<String>,
    ) -> Self {
        let install_dir =
            install_dir.map_or_else(|| PathBuf::from(DEFAULT_INSTALL_DIR), PathBuf::from);
        let releases_dir =
            releases_dir.map_or_else(|| install_dir.join("releases"), PathBuf::from);
        Self {
            release_base_url: release_base_url
                .unwrap_or_else(|| DEFAULT_RELEASE_BASE_URL.to_owned()),
            arch: std::env::consts::ARCH.to_owned(),
            releases_dir,
            candidate_identity_path: install_dir.join("candidate-identity.json"),
            install_dir,
            gate_timeout: DEFAULT_GATE_TIMEOUT,
            stability_window: DEFAULT_STABILITY_WINDOW,
        }
    }
}

impl SelfUpdateConfig {
    /// A [`fetch::VersionFetcher`] wired to this config's base URL / arch /
    /// releases dir.
    #[must_use]
    pub fn fetcher(&self) -> fetch::VersionFetcher {
        fetch::VersionFetcher::new(&self.release_base_url, &self.arch, &self.releases_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// I3 invariant: the post-swap stability window MUST close before the
    /// coordinator garbage-collects a silent node at the heartbeat-timeout,
    /// otherwise a bad node would be dropped from the roster before the
    /// watchdog ever got to revert it. Both the gate and the window stay under
    /// the heartbeat-timeout.
    #[test]
    fn stability_window_is_under_coordinator_heartbeat_timeout() {
        assert!(
            DEFAULT_STABILITY_WINDOW < COORDINATOR_HEARTBEAT_TIMEOUT,
            "stability window {DEFAULT_STABILITY_WINDOW:?} must be < heartbeat-timeout \
             {COORDINATOR_HEARTBEAT_TIMEOUT:?} so a bad node is reverted before GC",
        );
        assert!(
            DEFAULT_GATE_TIMEOUT < COORDINATOR_HEARTBEAT_TIMEOUT,
            "gate timeout {DEFAULT_GATE_TIMEOUT:?} must be < heartbeat-timeout \
             {COORDINATOR_HEARTBEAT_TIMEOUT:?}",
        );
    }

    /// The env-override derivation: with all three values supplied the engine
    /// targets the operator's bucket + install layout, with the releases dir +
    /// candidate identity rooted under the install dir.
    #[test]
    fn from_locations_honours_overrides() {
        let cfg = SelfUpdateConfig::from_locations(
            Some("https://tabbify-releases-leo.s3.eu-central-1.amazonaws.com".to_owned()),
            Some("/opt/tabbify".to_owned()),
            Some("/opt/tabbify/releases".to_owned()),
        );
        assert_eq!(
            cfg.release_base_url,
            "https://tabbify-releases-leo.s3.eu-central-1.amazonaws.com"
        );
        assert_eq!(cfg.install_dir, PathBuf::from("/opt/tabbify"));
        assert_eq!(cfg.releases_dir, PathBuf::from("/opt/tabbify/releases"));
        assert_eq!(
            cfg.candidate_identity_path,
            PathBuf::from("/opt/tabbify/candidate-identity.json")
        );
    }

    /// With nothing set the baked defaults apply, and the releases dir + the
    /// candidate identity derive under the install dir.
    #[test]
    fn from_locations_defaults_when_unset() {
        let cfg = SelfUpdateConfig::from_locations(None, None, None);
        assert_eq!(cfg.release_base_url, DEFAULT_RELEASE_BASE_URL);
        assert_eq!(cfg.install_dir, PathBuf::from(DEFAULT_INSTALL_DIR));
        assert_eq!(cfg.releases_dir, PathBuf::from(DEFAULT_INSTALL_DIR).join("releases"));
        assert_eq!(
            cfg.candidate_identity_path,
            PathBuf::from(DEFAULT_INSTALL_DIR).join("candidate-identity.json"),
        );
    }

    /// A custom install dir relocates the derived releases dir + candidate
    /// identity under it (the releases-dir override still wins when present).
    #[test]
    fn from_locations_derives_releases_dir_under_custom_install_dir() {
        let cfg = SelfUpdateConfig::from_locations(None, Some("/srv/tabbify".to_owned()), None);
        assert_eq!(cfg.releases_dir, PathBuf::from("/srv/tabbify/releases"));
        assert_eq!(
            cfg.candidate_identity_path,
            PathBuf::from("/srv/tabbify/candidate-identity.json"),
        );
    }

    /// The default config must carry the I3-safe window, not some larger value.
    #[test]
    fn default_config_stability_window_is_under_heartbeat() {
        let cfg = SelfUpdateConfig::default();
        assert!(
            cfg.stability_window < COORDINATOR_HEARTBEAT_TIMEOUT,
            "default config stability window {:?} must be < heartbeat-timeout {:?}",
            cfg.stability_window,
            COORDINATOR_HEARTBEAT_TIMEOUT,
        );
    }
}
