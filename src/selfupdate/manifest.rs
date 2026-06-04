//! The sibling `<family>/latest` JSON manifest (versioned-S3 layout, SU-7).

use std::collections::BTreeMap;

use serde::Deserialize;

/// `<family>/latest` — describes the published versions + per-binary sha256.
#[derive(Debug, Clone, Deserialize)]
pub struct LatestManifest {
    /// Highest published version, e.g. `"v1.4.0"`.
    pub latest: String,
    /// All published versions, newest first.
    pub versions: Vec<String>,
    /// sha256 hex by binary name (`"supervisord"`, `"tabbify-runner"`).
    pub sha256: BTreeMap<String, String>,
    /// ISO-8601 publish timestamp.
    pub ts: String,
}

impl LatestManifest {
    /// The expected sha256 hex for `binary`, if listed.
    #[must_use]
    pub fn sha256_for(&self, binary: &str) -> Option<&str> {
        self.sha256.get(binary).map(String::as_str)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "latest": "v1.4.0",
        "versions": ["v1.4.0", "v1.3.0"],
        "sha256": {
            "supervisord": "aabbccdd",
            "tabbify-runner": "11223344",
            "supervisord_aarch64": "eeff0011",
            "tabbify-runner_aarch64": "22334455"
        },
        "ts": "2026-05-30T00:00:00Z"
    }"#;

    #[test]
    fn parses_latest_manifest() {
        let m: LatestManifest = serde_json::from_str(SAMPLE).unwrap();
        assert_eq!(m.latest, "v1.4.0");
        assert_eq!(m.versions.len(), 2);
        assert_eq!(m.sha256_for("supervisord"), Some("aabbccdd"));
        assert_eq!(m.sha256_for("tabbify-runner"), Some("11223344"));
        // Per-arch keys (published by the dependent-job manifest) are plain
        // map entries — the fetcher tries `<bin>_<arch>` before `<bin>`.
        assert_eq!(m.sha256_for("supervisord_aarch64"), Some("eeff0011"));
        assert_eq!(m.sha256_for("supervisord_x86_64"), None);
        assert_eq!(m.sha256_for("nope"), None);
    }
}
