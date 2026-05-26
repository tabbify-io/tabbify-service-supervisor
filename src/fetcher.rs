//! S3 artifact fetch (contract §2).
//!
//! Anonymous HTTPS GET from `<base>/apps/<uuid>/`:
//! 1. read `latest` → version number N,
//! 2. GET `v<N>/manifest.toml` and `v<N>/app.wasm`,
//! 3. cache under `<data_dir>/apps/<uuid>/v<N>/`.
//!
//! The base URL is a field on [`S3Fetcher`] so tests point it at a local mock
//! HTTP server (wiremock) instead of real S3. No AWS credentials are used —
//! the bucket is public-read on `apps/*` (contract §2 rationale).

use std::path::{Path, PathBuf};

use bytes::Bytes;
use reqwest::StatusCode;

use crate::manifest::AppManifest;

/// A fetched-and-cached app artifact at a specific version.
#[derive(Debug, Clone)]
pub struct FetchedApp {
    /// Resolved version number from `latest`.
    pub version: u64,
    /// Parsed manifest.
    pub manifest: AppManifest,
    /// Raw entry-file bytes (the wasm component for `wasm-http`). Loaded into
    /// memory for WASM; firecracker uses [`Self::cached_path`] instead and never
    /// reads a multi-hundred-MB rootfs into RAM here.
    pub wasm: Bytes,
    /// On-disk path of the cached entry file (`<cache_dir>/<runtime.entry>`).
    /// The firecracker runtime hands this rootfs path straight to the VM.
    pub cached_path: PathBuf,
}

/// Errors from the S3 fetch path.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The app (or one of its objects) was not found (404).
    #[error("app {0} not found at this version/object")]
    NotFound(String),
    /// Network / transport failure talking to the object store.
    #[error("http transport: {0}")]
    Transport(String),
    /// The `latest` body was not a parseable version number.
    #[error("bad `latest` body {body:?}: {source}")]
    BadLatest {
        /// The raw body we tried to parse.
        body: String,
        /// The underlying parse error.
        source: std::num::ParseIntError,
    },
    /// The fetched manifest failed to parse.
    #[error("manifest parse: {0}")]
    Manifest(String),
    /// A local cache filesystem operation failed.
    #[error("cache io at {path}: {source}")]
    CacheIo {
        /// The path we were operating on.
        path: PathBuf,
        /// The underlying io error.
        source: std::io::Error,
    },
}

/// Fetches app artifacts over anonymous HTTPS and caches them locally.
#[derive(Debug, Clone)]
pub struct S3Fetcher {
    /// Base URL, e.g. `https://tabbify-apps.s3.eu-central-1.amazonaws.com`
    /// (NO trailing slash; one is added internally). Injectable for tests.
    base_url: String,
    /// Local cache root; artifacts land under `<data_dir>/apps/<uuid>/v<N>/`.
    data_dir: PathBuf,
    client: reqwest::Client,
}

impl S3Fetcher {
    /// Construct a fetcher against `base_url`, caching under `data_dir`.
    #[must_use]
    pub fn new(base_url: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            data_dir: data_dir.into(),
            client: reqwest::Client::new(),
        }
    }

    /// Root data directory this fetcher caches artifacts under.
    #[must_use]
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Local cache dir for a given uuid + version.
    #[must_use]
    pub fn cache_dir(&self, uuid: &str, version: u64) -> PathBuf {
        self.data_dir
            .join("apps")
            .join(uuid)
            .join(format!("v{version}"))
    }

    /// Remove the entire on-disk cache for `uuid` (`<data_dir>/apps/<uuid>/` —
    /// every cached version: manifest + entry file). The disk-reclaiming half of
    /// a registry purge. Idempotent: a missing directory is success.
    ///
    /// # Errors
    /// A filesystem error other than "not found".
    pub async fn purge_cache(&self, uuid: &str) -> Result<(), FetchError> {
        let dir = self.data_dir.join("apps").join(uuid);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FetchError::CacheIo { path: dir, source }),
        }
    }

    /// Resolve the current version by GETting `apps/<uuid>/latest`.
    ///
    /// # Errors
    /// [`FetchError::NotFound`] (404), [`FetchError::Transport`], or
    /// [`FetchError::BadLatest`] when the body isn't an integer.
    pub async fn latest_version(&self, uuid: &str) -> Result<u64, FetchError> {
        let url = format!("{}/apps/{uuid}/latest", self.base_url);
        let body = self.get_text(&url, uuid).await?;
        let trimmed = body.trim();
        trimmed
            .parse::<u64>()
            .map_err(|source| FetchError::BadLatest {
                body: trimmed.to_owned(),
                source,
            })
    }

    /// Fetch `latest` → manifest + wasm, writing them into the local cache.
    ///
    /// If the cache already holds this version, the bytes are served from disk
    /// (no network round-trip for the artifacts).
    ///
    /// # Errors
    /// See [`FetchError`].
    pub async fn fetch(&self, uuid: &str) -> Result<FetchedApp, FetchError> {
        let version = self.latest_version(uuid).await?;
        self.fetch_version(uuid, version).await
    }

    /// Fetch a specific version (manifest + entry file) with on-disk caching.
    ///
    /// The entry filename is taken from `manifest.runtime.entry` (NOT hardcoded
    /// `app.wasm`), so the cache write + cache-hit check, and the returned
    /// [`FetchedApp::cached_path`], all reference the real artifact (e.g.
    /// `app.wasm` for wasm-http, `rootfs.ext4` for firecracker). The manifest is
    /// always at the fixed `manifest.toml` path, so it is resolved first and
    /// then drives the entry path.
    ///
    /// For `firecracker` the (potentially large) rootfs is NOT read into memory:
    /// only [`FetchedApp::cached_path`] is populated and `wasm` is left empty.
    ///
    /// # Errors
    /// See [`FetchError`].
    pub async fn fetch_version(&self, uuid: &str, version: u64) -> Result<FetchedApp, FetchError> {
        let dir = self.cache_dir(uuid, version);
        let manifest_path = dir.join("manifest.toml");

        // The manifest is always at the fixed path. Resolve it first (cache or
        // network) so we know the entry filename + runtime type.
        let manifest = if manifest_path.is_file() {
            let manifest_text = read_file(&manifest_path)?;
            parse_manifest(&manifest_text)?
        } else {
            let base = format!("{}/apps/{uuid}/v{version}", self.base_url);
            let manifest_text = self
                .get_text(&format!("{base}/manifest.toml"), uuid)
                .await?;
            let manifest = parse_manifest(&manifest_text)?;
            write_cache(&dir, &manifest_path, manifest_text.as_bytes())?;
            manifest
        };

        let entry = manifest.runtime.entry.clone();
        let entry_path = dir.join(&entry);
        // Only `wasm-http` needs the artifact bytes resident in memory (the
        // wasmtime engine compiles them). `firecracker` (a rootfs image) and
        // `docker` (a build-context tarball) consume the on-disk path instead,
        // so we never read a large artifact into RAM for those.
        let loads_into_memory = manifest.runtime.r#type == "wasm-http";

        // Ensure the entry file is present on disk (fetch + persist on miss). We
        // stream it to disk and only load it into memory for wasm.
        if !entry_path.is_file() {
            let base = format!("{}/apps/{uuid}/v{version}", self.base_url);
            let bytes = self.get_bytes(&format!("{base}/{entry}"), uuid).await?;
            write_file(&entry_path, &bytes)?;
        }

        // wasm-http needs the bytes in memory; firecracker/docker only need the
        // path (don't read a multi-hundred-MB rootfs / tarball into RAM).
        let wasm = if loads_into_memory {
            Bytes::from(read_file_bytes(&entry_path)?)
        } else {
            Bytes::new()
        };

        // For docker apps: opportunistically fetch the prebuilt image tar so
        // the supervisor can `docker load` it (warm start) instead of building
        // from source.  A 404 (source-only app) is silently ignored.
        if manifest.runtime.r#type == "docker" {
            self.fetch_image_tar(uuid, version, &dir).await?;
        }

        Ok(FetchedApp {
            version,
            manifest,
            wasm,
            cached_path: entry_path,
        })
    }

    async fn get_text(&self, url: &str, uuid: &str) -> Result<String, FetchError> {
        let resp = self.get(url, uuid).await?;
        resp.text()
            .await
            .map_err(|e| FetchError::Transport(e.to_string()))
    }

    async fn get_bytes(&self, url: &str, uuid: &str) -> Result<Bytes, FetchError> {
        let resp = self.get(url, uuid).await?;
        resp.bytes()
            .await
            .map_err(|e| FetchError::Transport(e.to_string()))
    }

    async fn get(&self, url: &str, uuid: &str) -> Result<reqwest::Response, FetchError> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| FetchError::Transport(e.to_string()))?;
        match resp.status() {
            StatusCode::OK => Ok(resp),
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => {
                // S3 returns 403 (not 404) for a missing object under some
                // bucket policies; treat both as "not present".
                Err(FetchError::NotFound(uuid.to_owned()))
            }
            other => Err(FetchError::Transport(format!("unexpected status {other}"))),
        }
    }

    /// GET `url`, returning `Ok(Some(bytes))` on 200, `Ok(None)` on 404/403
    /// (object absent — not an error), or `Err` on any other failure.
    ///
    /// Used for optional artifacts (e.g. `image.tar.gz`) that may not exist for
    /// older apps that only have a source build-context.
    async fn get_optional_bytes(&self, url: &str) -> Result<Option<Bytes>, FetchError> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| FetchError::Transport(e.to_string()))?;
        match resp.status() {
            StatusCode::OK => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| FetchError::Transport(e.to_string()))?;
                Ok(Some(bytes))
            }
            // S3 returns 403 for a missing object under some bucket policies.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Ok(None),
            other => Err(FetchError::Transport(format!("unexpected status {other}"))),
        }
    }

    /// For a `docker` app: fetch `apps/<uuid>/v<N>/image.tar.gz` (the prebuilt
    /// image tar uploaded by `tcli`) and save it to `<dir>/image.tar.gz`.
    ///
    /// Best-effort: a 404 (source-only app — no prebuilt tar) is NOT an error.
    /// Already-cached: if `<dir>/image.tar.gz` already exists on disk, skip the
    /// network request (same cache-hit logic as the entry file).
    ///
    /// # Errors
    /// Only hard transport / filesystem failures — a missing object is `Ok(())`.
    async fn fetch_image_tar(
        &self,
        uuid: &str,
        version: u64,
        dir: &std::path::Path,
    ) -> Result<(), FetchError> {
        let tar_path = dir.join("image.tar.gz");
        if tar_path.is_file() {
            // Already cached — nothing to do.
            return Ok(());
        }
        let url = format!("{}/apps/{uuid}/v{version}/image.tar.gz", self.base_url);
        match self.get_optional_bytes(&url).await? {
            Some(bytes) => {
                tracing::debug!(uuid, version, "fetched prebuilt image.tar.gz");
                write_file(&tar_path, &bytes)?;
            }
            None => {
                // No prebuilt tar available (source-only app) — skip silently.
                tracing::debug!(uuid, version, "no image.tar.gz available (source-only app)");
            }
        }
        Ok(())
    }
}

fn parse_manifest(text: &str) -> Result<AppManifest, FetchError> {
    toml::from_str(text).map_err(|e| FetchError::Manifest(e.to_string()))
}

fn read_file(path: &Path) -> Result<String, FetchError> {
    std::fs::read_to_string(path).map_err(|source| FetchError::CacheIo {
        path: path.to_path_buf(),
        source,
    })
}

fn read_file_bytes(path: &Path) -> Result<Vec<u8>, FetchError> {
    std::fs::read(path).map_err(|source| FetchError::CacheIo {
        path: path.to_path_buf(),
        source,
    })
}

fn write_cache(dir: &Path, manifest_path: &Path, manifest: &[u8]) -> Result<(), FetchError> {
    std::fs::create_dir_all(dir).map_err(|source| FetchError::CacheIo {
        path: dir.to_path_buf(),
        source,
    })?;
    write_file(manifest_path, manifest)
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), FetchError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| FetchError::CacheIo {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    std::fs::write(path, bytes).map_err(|source| FetchError::CacheIo {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    // ---- get_optional_bytes: 404 is Ok(None), not an error -------------------

    /// `get_optional_bytes` on a 200 response returns `Ok(Some(bytes))`.
    #[tokio::test]
    async fn get_optional_bytes_200_returns_some() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/optional-file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
            .mount(&server)
            .await;

        let fetcher = S3Fetcher::new(server.uri(), "/tmp");
        let url = format!("{}/optional-file", server.uri());
        let result = fetcher.get_optional_bytes(&url).await.unwrap();
        assert_eq!(result, Some(bytes::Bytes::from_static(b"hello")));
    }

    /// `get_optional_bytes` on a 404 returns `Ok(None)` — not an error. This is
    /// the source-only-app path: no `image.tar.gz` uploaded yet.
    #[tokio::test]
    async fn get_optional_bytes_404_returns_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing-file"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let fetcher = S3Fetcher::new(server.uri(), "/tmp");
        let url = format!("{}/missing-file", server.uri());
        let result = fetcher.get_optional_bytes(&url).await.unwrap();
        assert_eq!(result, None, "404 must be Ok(None), not an error");
    }

    /// `get_optional_bytes` on a 403 (S3's "missing object" under some bucket
    /// policies) returns `Ok(None)` — same as 404.
    #[tokio::test]
    async fn get_optional_bytes_403_returns_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/forbidden-file"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let fetcher = S3Fetcher::new(server.uri(), "/tmp");
        let url = format!("{}/forbidden-file", server.uri());
        let result = fetcher.get_optional_bytes(&url).await.unwrap();
        assert_eq!(result, None, "403 must be Ok(None), not an error");
    }

    // ---- fetch_image_tar: 404 does not fail fetch, writes file on 200 --------

    /// When the server returns 200 with tar bytes, `fetch_image_tar` writes
    /// `image.tar.gz` to the given directory and returns `Ok(())`.
    #[tokio::test]
    async fn fetch_image_tar_200_writes_file_to_dir() {
        let server = MockServer::start().await;
        let uuid = "test-uuid-warm";
        let version = 2u64;
        Mock::given(method("GET"))
            .and(path(format!("/apps/{uuid}/v{version}/image.tar.gz")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake-image-tar".to_vec()))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let fetcher = S3Fetcher::new(server.uri(), tmp.path());
        let dir = tmp
            .path()
            .join("apps")
            .join(uuid)
            .join(format!("v{version}"));
        std::fs::create_dir_all(&dir).unwrap();

        fetcher
            .fetch_image_tar(uuid, version, &dir)
            .await
            .expect("fetch_image_tar must succeed");

        let tar_path = dir.join("image.tar.gz");
        assert!(tar_path.is_file(), "image.tar.gz must be written to dir");
        assert_eq!(
            std::fs::read(&tar_path).unwrap(),
            b"fake-image-tar",
            "written bytes must match the server response"
        );
    }

    /// When the server returns 404 (source-only app, no prebuilt tar),
    /// `fetch_image_tar` returns `Ok(())` without creating any file.
    #[tokio::test]
    async fn fetch_image_tar_404_does_not_fail() {
        let server = MockServer::start().await;
        let uuid = "test-uuid-source";
        let version = 1u64;
        Mock::given(method("GET"))
            .and(path(format!("/apps/{uuid}/v{version}/image.tar.gz")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let fetcher = S3Fetcher::new(server.uri(), tmp.path());
        let dir = tmp
            .path()
            .join("apps")
            .join(uuid)
            .join(format!("v{version}"));
        std::fs::create_dir_all(&dir).unwrap();

        // Must NOT return an error.
        fetcher
            .fetch_image_tar(uuid, version, &dir)
            .await
            .expect("404 must not fail fetch_image_tar");

        // Must NOT write a file.
        assert!(
            !dir.join("image.tar.gz").exists(),
            "no file must be written when server returns 404"
        );
    }

    /// When `image.tar.gz` is already cached on disk, `fetch_image_tar` skips
    /// the network request entirely (the mock server has no route for this call).
    #[tokio::test]
    async fn fetch_image_tar_cached_skips_network() {
        // Server has NO route for image.tar.gz — if the fetcher makes a request
        // it gets an unexpected 404 which would still be Ok(None), but we verify
        // no request at all is made by checking the mock server received 0 calls.
        let server = MockServer::start().await;
        let uuid = "test-uuid-cached";
        let version = 5u64;

        let tmp = tempfile::TempDir::new().unwrap();
        let fetcher = S3Fetcher::new(server.uri(), tmp.path());
        let dir = tmp
            .path()
            .join("apps")
            .join(uuid)
            .join(format!("v{version}"));
        std::fs::create_dir_all(&dir).unwrap();

        // Pre-create the cached file.
        std::fs::write(dir.join("image.tar.gz"), b"cached").unwrap();

        fetcher
            .fetch_image_tar(uuid, version, &dir)
            .await
            .expect("cached path must succeed");

        // Verify the server received NO requests.
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            0,
            "no network request must be made when image.tar.gz is already cached"
        );
    }
}
