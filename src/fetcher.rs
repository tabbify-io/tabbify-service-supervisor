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

    /// Local cache dir for a given uuid + version.
    #[must_use]
    pub fn cache_dir(&self, uuid: &str, version: u64) -> PathBuf {
        self.data_dir
            .join("apps")
            .join(uuid)
            .join(format!("v{version}"))
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
