//! Versioned release fetch (self-update, spec §2).
//!
//! Resolve a desired version (passed in by the caller), download
//! `supervisor/v<VER>/<arch>/{supervisord,tabbify-runner}` from the release base
//! into `<releases_dir>/v<VER>/`, and verify each binary's sha256 against the
//! sibling `supervisor/latest` manifest. A digest mismatch is a hard error — a
//! corrupt binary is never trusted (and never made executable in place).
//!
//! The base URL is a field so tests point it at a local mock HTTP server
//! (wiremock) instead of real S3. No credentials are used — release objects are
//! public-read, exactly like the app artifacts in [`crate::fetcher`].

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use super::manifest::LatestManifest;

/// The release binaries published per version (and listed in the manifest).
const BINARIES: [&str; 2] = ["supervisord", "tabbify-runner"];

/// Downloads versioned release binaries over anonymous HTTPS and verifies their
/// sha256 against the `latest` manifest before trusting them.
#[derive(Debug, Clone)]
pub struct VersionFetcher {
    /// Release base URL, e.g. `https://tabbify-apps.s3.eu-central-1.amazonaws.com`
    /// (NO trailing slash; one is added internally). Injectable for tests.
    base_url: String,
    /// Target architecture path segment, e.g. `"x86_64"` / `"aarch64"`.
    arch: String,
    /// Local releases root; binaries land under `<releases_dir>/v<VER>/`.
    releases_dir: PathBuf,
    client: reqwest::Client,
}

impl VersionFetcher {
    /// Construct a fetcher against `base_url` for `arch`, staging downloads under
    /// `releases_dir`.
    #[must_use]
    pub fn new(
        base_url: impl Into<String>,
        arch: impl Into<String>,
        releases_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            arch: arch.into(),
            releases_dir: releases_dir.into(),
            client: reqwest::Client::new(),
        }
    }

    /// The on-disk directory a given version is staged into.
    #[must_use]
    pub fn version_dir(&self, version: &str) -> PathBuf {
        self.releases_dir.join(version)
    }

    /// Download both release binaries for `version` into `<releases_dir>/<version>/`
    /// and verify their sha256 against the `latest` manifest.
    ///
    /// On success returns the version directory (containing `supervisord` and
    /// `tabbify-runner`, both `chmod 0o755`).
    ///
    /// # Errors
    /// A transport failure, a non-200 HTTP status, a filesystem error, or a
    /// sha256 mismatch (the latter is a hard rejection of a corrupt binary).
    pub async fn fetch_version(&self, version: &str) -> Result<PathBuf> {
        let manifest = self.fetch_manifest().await?;
        let dir = self.version_dir(version);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create release dir {dir:?}"))?;

        for bin in BINARIES {
            let url = format!("{}/supervisor/{version}/{}/{bin}", self.base_url, self.arch);
            let bytes = self.get_bytes(&url).await?;

            let expected = manifest
                .sha256_for(bin)
                .with_context(|| format!("manifest has no sha256 for {bin}"))?;
            let actual = hex_sha256(&bytes);
            if actual != expected {
                bail!("sha256 mismatch for {bin}: expected {expected}, got {actual}");
            }

            write_executable(&dir.join(bin), &bytes)?;
        }

        Ok(dir)
    }

    /// GET `supervisor/latest` and parse the [`LatestManifest`].
    async fn fetch_manifest(&self) -> Result<LatestManifest> {
        let url = format!("{}/supervisor/latest", self.base_url);
        let body = self
            .get_bytes(&url)
            .await
            .with_context(|| format!("fetch latest manifest from {url}"))?;
        serde_json::from_slice(&body).context("parse latest manifest")
    }

    /// GET `url`, returning the body bytes on 200 (any other status is an error).
    async fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("GET {url} returned status {status}");
        }
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("read body of {url}"))?;
        Ok(bytes.to_vec())
    }
}

/// Lowercase hex sha256 of `bytes`.
fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Atomically write `bytes` to `path` (tempfile + rename) and mark it executable.
fn write_executable(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{path:?} has no parent dir"))?;
    let tmp = parent.join(format!(
        ".{}.download",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("release-binary")
    ));
    std::fs::write(&tmp, bytes).with_context(|| format!("write {tmp:?}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("chmod 0o755 {tmp:?}"))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use sha2::{Digest, Sha256};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn hex_sha256(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        hex::encode(h.finalize())
    }

    /// fetch_version downloads both binaries to releases/v<VER>/ and passes
    /// when their sha256 matches the manifest.
    #[tokio::test]
    async fn fetch_version_writes_binaries_and_verifies_sha256() {
        let server = MockServer::start().await;
        let arch = "x86_64";
        let sup = b"FAKE-supervisord-bytes".to_vec();
        let run = b"FAKE-runner-bytes".to_vec();
        let manifest = format!(
            r#"{{"latest":"v9.9.9","versions":["v9.9.9"],"sha256":{{"supervisord":"{}","tabbify-runner":"{}"}},"ts":"2026-05-30T00:00:00Z"}}"#,
            hex_sha256(&sup),
            hex_sha256(&run)
        );
        Mock::given(method("GET"))
            .and(path("/supervisor/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/v9.9.9/{arch}/supervisord")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(sup.clone()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/v9.9.9/{arch}/tabbify-runner")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(run.clone()))
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let fetcher = VersionFetcher::new(server.uri(), arch, tmp.path());
        let dir = fetcher
            .fetch_version("v9.9.9")
            .await
            .expect("fetch must verify+succeed");

        assert!(dir.join("supervisord").is_file());
        assert!(dir.join("tabbify-runner").is_file());
        assert_eq!(std::fs::read(dir.join("supervisord")).unwrap(), sup);
    }

    /// A sha256 mismatch is a hard error — the corrupt binary is never trusted.
    #[tokio::test]
    async fn fetch_version_rejects_sha256_mismatch() {
        let server = MockServer::start().await;
        let arch = "x86_64";
        let manifest = r#"{"latest":"v9.9.9","versions":["v9.9.9"],"sha256":{"supervisord":"deadbeef","tabbify-runner":"deadbeef"},"ts":"t"}"#;
        Mock::given(method("GET"))
            .and(path("/supervisor/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/v9.9.9/{arch}/supervisord")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"wrong".to_vec()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/supervisor/v9.9.9/{arch}/tabbify-runner")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"wrong".to_vec()))
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let fetcher = VersionFetcher::new(server.uri(), arch, tmp.path());
        let err = fetcher.fetch_version("v9.9.9").await.unwrap_err();
        assert!(err.to_string().contains("sha256"), "got: {err}");
    }
}
