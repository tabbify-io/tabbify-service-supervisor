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
//!    post-swap stability window is ~90s.

use std::path::PathBuf;
use std::time::Duration;

pub mod fetch;
pub mod manifest;
pub mod probe;
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

/// 3-part health gate timeout — kept under the coordinator heartbeat-timeout.
const DEFAULT_GATE_TIMEOUT: Duration = Duration::from_secs(45);

/// Post-swap stability window — long enough to catch a bad swap, short enough
/// to stay under the coordinator heartbeat-timeout headroom.
const DEFAULT_STABILITY_WINDOW: Duration = Duration::from_secs(90);

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
