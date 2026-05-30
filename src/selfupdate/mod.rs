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

/// Default release base URL (same public bucket as the app artifacts). The
/// self-update fetch engine ([`SelfUpdateConfig::fetcher`]) is its only reader;
/// it will become operator-overridable once a live consumer is wired in.
const DEFAULT_RELEASE_BASE_URL: &str = "https://tabbify-apps.s3.eu-central-1.amazonaws.com";

/// Default install dir holding the live binary symlinks + `VERSION`.
const DEFAULT_INSTALL_DIR: &str = "/opt/tabbify";

/// Default dir under which each fetched version is staged (`v<VER>/`).
const DEFAULT_RELEASES_DIR: &str = "/opt/tabbify/releases";

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
    fn default() -> Self {
        Self {
            release_base_url: DEFAULT_RELEASE_BASE_URL.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            releases_dir: PathBuf::from(DEFAULT_RELEASES_DIR),
            install_dir: PathBuf::from(DEFAULT_INSTALL_DIR),
            candidate_identity_path: PathBuf::from(DEFAULT_INSTALL_DIR)
                .join("candidate-identity.json"),
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
