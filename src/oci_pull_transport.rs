//! HTTP transport seam + the resumable-download engine for [`super`].
//!
//! Split out of `oci_pull.rs` to keep each file within the ~500-line guideline:
//! this file owns the injectable `HttpGet` seam (production reqwest impl + the
//! streaming body abstraction) and the generic `download_resumable` loop that
//! resumes a broken transfer via an HTTP `Range` header. The OCI-specific
//! orchestration (ref parse, manifest/config/layer walk, layout writing) lives
//! in the parent module.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::auth::{BearerCache, get_authed};
use crate::runtime::BoxFut;

/// Overall wall-clock budget for a SINGLE blob. A 243 MB layer over a lossy
/// relay can take many minutes (multiple rekey breaks); this is generous
/// headroom so a blob that keeps MAKING PROGRESS is never abandoned.
const BLOB_DEADLINE: Duration = Duration::from_secs(60 * 45);
/// Consecutive retries WITHOUT progress before a blob is abandoned. ANY byte of
/// progress resets this to 0 — the budget is effectively unbounded while data
/// flows, and only a truly stuck transfer (no bytes across this many tries)
/// gives up. Rekey breaks are EXPECTED, not fatal, so this is deliberately high.
const MAX_STALL_RETRIES: u32 = 40;
/// Backoff between a mid-stream break and the resume retry.
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

// ── Transport seam ──────────────────────────────────────────────────────────

/// One issued GET: the HTTP status plus a streaming body the caller pumps
/// chunk-by-chunk. A trait object so a test can inject a transport that BREAKS
/// mid-body and then honours the `Range` retry, with no real socket.
pub struct GetResponse {
    /// HTTP status code.
    pub status: u16,
    /// The `WWW-Authenticate` header value, if the response carried one. Present
    /// on a `401` from a registry that wants OCI Bearer token-exchange; parsed by
    /// the auth layer to drive the exchange.
    pub www_authenticate: Option<String>,
    /// The streaming response body.
    pub body: Box<dyn ByteStream>,
}

/// A streaming HTTP body: pull one chunk at a time. `Ok(Some(bytes))` is the
/// next chunk, `Ok(None)` is a clean end-of-body, and `Err(_)` is a MID-STREAM
/// break (relay rekey / connection reset) — the puller then resumes from the
/// bytes already on disk with a `Range` retry.
pub trait ByteStream: Send {
    /// Pull the next chunk of the body.
    fn next_chunk(&mut self) -> BoxFut<'_, Result<Option<Bytes>, String>>;
}

/// HTTP GET seam. Production hits the plain-HTTP mesh registry with reqwest;
/// tests inject a transport that simulates a lossy relay.
pub trait HttpGet: Send + Sync {
    /// `GET url` with an optional `Range: bytes=<from>-`, an `Accept` header, and
    /// (when present) the VERBATIM `Authorization` header value — either
    /// `Basic <b64>` (the oras-config credential) or `Bearer <jwt>` (an OCI
    /// token-exchange bearer). The transport attaches it as-is; the Basic-vs-Bearer
    /// decision and the exchange live in the auth layer.
    fn get<'a>(
        &'a self,
        url: &'a str,
        range_from: Option<u64>,
        accept: Option<&'a str>,
        authorization: Option<&'a str>,
    ) -> BoxFut<'a, Result<GetResponse, String>>;
}

/// Production [`HttpGet`] over plain HTTP with reqwest streaming bodies.
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    /// A transport backed by a fresh reqwest client.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// A reqwest response wrapped as a [`ByteStream`] (each `next_chunk` is one
/// `Response::chunk`).
struct ReqwestBody {
    resp: reqwest::Response,
}

impl ByteStream for ReqwestBody {
    fn next_chunk(&mut self) -> BoxFut<'_, Result<Option<Bytes>, String>> {
        Box::pin(async move { self.resp.chunk().await.map_err(|e| e.to_string()) })
    }
}

impl HttpGet for ReqwestTransport {
    fn get<'a>(
        &'a self,
        url: &'a str,
        range_from: Option<u64>,
        accept: Option<&'a str>,
        authorization: Option<&'a str>,
    ) -> BoxFut<'a, Result<GetResponse, String>> {
        let client = self.client.clone();
        let url = url.to_owned();
        let accept = accept.map(str::to_owned);
        let authorization = authorization.map(str::to_owned);
        Box::pin(async move {
            let mut req = client.get(&url);
            if let Some(from) = range_from {
                req = req.header(reqwest::header::RANGE, format!("bytes={from}-"));
            }
            if let Some(a) = accept {
                req = req.header(reqwest::header::ACCEPT, a);
            }
            if let Some(a) = authorization {
                req = req.header(reqwest::header::AUTHORIZATION, a);
            }
            let resp = req.send().await.map_err(|e| e.to_string())?;
            let status = resp.status().as_u16();
            // Read the challenge header BEFORE moving `resp` into the body stream.
            let www_authenticate = resp
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            Ok(GetResponse {
                status,
                www_authenticate,
                body: Box::new(ReqwestBody { resp }),
            })
        })
    }
}

// ── Resumable download core ─────────────────────────────────────────────────

/// Download `url` into `dest` with RESUME. On a mid-stream break, retry with a
/// `Range: bytes=<current-file-size>-` header and APPEND, so progress is never
/// lost. Returns once the body ends cleanly. Bounded by a per-blob wall-clock
/// deadline PLUS a no-progress (stall) cap — but ANY byte of progress resets the
/// stall counter, so a transfer that keeps advancing across rekey breaks always
/// completes.
///
/// Auth is transparent: each GET (initial AND every ranged resume) goes through
/// [`get_authed`], which attaches the cached bearer / performs the OCI
/// token-exchange with `basic` on a `401` Bearer challenge. `cache` is shared
/// across the whole pull so a long blob that outlives a short-lived bearer simply
/// re-exchanges once and continues its Range resume with the refreshed token.
///
/// # Errors
/// The per-blob deadline elapses, or the stall cap is hit with no progress.
pub(crate) async fn download_resumable(
    transport: &dyn HttpGet,
    url: &str,
    dest: &Path,
    accept: Option<&str>,
    basic: Option<&str>,
    cache: &BearerCache,
) -> Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create blob dir {}", parent.display()))?;
    }
    let started = Instant::now();
    let mut stall: u32 = 0;
    loop {
        let have = file_len(dest).await;
        let range_from = if have > 0 { Some(have) } else { None };
        let resp = match get_authed(transport, cache, basic, url, range_from, accept).await {
            Ok(r) => r,
            Err(e) => {
                stall = bump_stall(stall, started, url, &e)?;
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            }
        };
        // Decide append-vs-restart from the status, then pump the body.
        let mut append = have > 0;
        match resp.status {
            200 => {
                // Full body from byte 0. If we asked for a Range and still got
                // 200, the server IGNORED it → overwrite from scratch.
                append = false;
            }
            206 => { /* partial content: append from `have` */ }
            416 => return Ok(()), // range past EOF → already complete; let verify judge
            s if is_transient_status(s) => {
                stall = bump_stall(stall, started, url, &format!("http {s}"))?;
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            }
            s => bail!("GET {url} returned unexpected status {s}"),
        }
        let before = if append { have } else { 0 };
        if pump_body(resp.body, dest, append).await {
            return Ok(()); // clean end-of-body
        }
        let after = file_len(dest).await;
        if after > before {
            stall = 0; // progress made → reset the budget
        } else {
            stall = bump_stall(stall, started, url, "mid-stream break, no progress")?;
        }
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// Pump a body into `dest`, appending (or truncating first when `!append`).
/// Returns `true` on a clean end-of-body, `false` on a mid-stream break or a
/// local write error (the caller then resumes / stalls).
async fn pump_body(mut body: Box<dyn ByteStream>, dest: &Path, append: bool) -> bool {
    let Ok(mut file) = open_for_write(dest, append).await else {
        return false;
    };
    loop {
        match body.next_chunk().await {
            Ok(Some(chunk)) => {
                if file.write_all(&chunk).await.is_err() {
                    return false;
                }
            }
            Ok(None) => {
                let _ = file.flush().await;
                return true;
            }
            Err(_) => {
                let _ = file.flush().await;
                return false;
            }
        }
    }
}

/// Increment the stall counter, bailing when the per-blob deadline OR the
/// no-progress retry cap is exceeded; otherwise return the incremented counter.
fn bump_stall(stall: u32, started: Instant, url: &str, why: &str) -> Result<u32> {
    let stall = stall + 1;
    if started.elapsed() >= BLOB_DEADLINE {
        bail!("resumable pull of {url} exceeded the {BLOB_DEADLINE:?} blob deadline ({why})");
    }
    if stall >= MAX_STALL_RETRIES {
        bail!("resumable pull of {url} stalled: {stall} tries with no progress ({why})");
    }
    Ok(stall)
}

/// A retryable (transient) HTTP status: 429 or any 5xx.
fn is_transient_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

/// Open `dest` for writing: append mode when `append`, else create+truncate.
async fn open_for_write(dest: &Path, append: bool) -> std::io::Result<tokio::fs::File> {
    let mut opts = tokio::fs::OpenOptions::new();
    opts.create(true).write(true);
    if append {
        opts.append(true);
    } else {
        opts.truncate(true);
    }
    opts.open(dest).await
}

/// Current size of `path` in bytes, or 0 if absent.
async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Stream-hash `path` and compare to `want_hex`. A missing file is `Ok(false)`
/// (nothing to verify yet). Hashing is chunked so a multi-hundred-MB layer is
/// never read whole into RAM.
pub(crate) async fn verify_file_sha256(path: &Path, want_hex: &str) -> Result<bool> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("open blob for verify {}", path.display()));
        }
    };
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 128 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .await
            .with_context(|| format!("read blob for verify {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()) == want_hex)
}

/// Hex-encoded sha256 of `bytes` (for small in-memory blobs like a manifest).
#[must_use]
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
