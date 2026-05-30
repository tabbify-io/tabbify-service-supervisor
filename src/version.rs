//! Single source of truth for the running binary's release version.

/// The version string embedded at build time by `build.rs`
/// (`TABBIFY_BUILD_VERSION`), falling back to `CARGO_PKG_VERSION` if the
/// build-script env was not set (e.g. a rust-analyzer check without the script).
#[must_use]
pub fn binary_version() -> &'static str {
    option_env!("TABBIFY_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// binary_version() is non-empty and looks like a semver core or a
    /// build-injected string (never the literal "unknown").
    #[test]
    fn binary_version_is_non_empty_semverish() {
        let v = binary_version();
        assert!(!v.is_empty(), "version must not be empty");
        assert_ne!(v, "unknown", "version must resolve to a real value");
        // The CARGO_PKG_VERSION fallback is `0.1.0` (Cargo.toml) — at minimum
        // it must contain two dots (major.minor.patch core).
        assert_eq!(v.matches('.').count(), 2, "expected semver core, got {v}");
    }
}
