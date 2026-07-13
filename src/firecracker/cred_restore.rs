//! Post-restore credential RE-PLUMB — the host-side half of the snapshot
//! scrub/restore bracket. Pure + host-agnostic (NO `cfg(target_os = "linux")`)
//! so the protocol logic is unit-testable on macOS; the Linux runtime
//! (`linux.rs`) only wires these helpers into its launch/snapshot paths.
//!
//! ## The invariant this module exists to uphold
//! **A workspace that comes up (warm OR cold) with declared forge creds MUST
//! have the broker actually holding them.**
//!
//! ## Why (the snapshot/scrub contradiction)
//! The GAP#4 pre-snapshot scrub strips the in-guest broker of its in-RAM creds
//! AND its tmpfs cred files so a Full snapshot freezes NO plaintext secret at
//! rest. That is correct for the snapshot file — but a warm restore resurrects
//! EXACTLY that post-scrub guest: `/init` (the only cold-boot cred channel)
//! never re-runs, so the broker holds nothing forever, no matter how many times
//! the node re-provisions with fresh creds. Worse, the LIVE VM that continues
//! serving after the post-snapshot resume is equally credless. The env/cap
//! fingerprint gate (`snapshot::restore_matches`) cannot see this: the scrub
//! mutates GUEST state, not the declared env (and cap VALUES are deliberately
//! excluded from the fingerprint), so `restore_matches` keeps saying "warm
//! restore is fine".
//!
//! ## The protocol (mirrors the broker's `http_restore`)
//! 1. Before scrubbing, PROBE `GET /v1/restore-creds` — an old broker (image
//!    predating the protocol) 404s, and the runner then REFUSES to scrub
//!    (fail closed: never strip creds that cannot be re-plumbed).
//! 2. The scrub (`POST /v1/pre-snapshot-scrub`, bearer = the authkeys cap the
//!    runner already holds host-side) returns a ONE-TIME restore nonce that the
//!    broker keeps in RAM — the snapshot freezes the NONCE instead of the creds.
//! 3. The runner persists the nonce next to the snapshot files
//!    (`.snapshot_restore_token`) and, after the snapshot create + resume,
//!    RE-PLUMBs the live VM (`POST /v1/restore-creds`, bearer = nonce, body =
//!    the same cap-file set `/init` would bake on a cold boot).
//! 4. A later warm restore resurrects the nonce inside the frozen RAM; the
//!    runner reads its persisted copy and performs the SAME re-plumb against
//!    the restored guest. Any failure falls back to a cold boot (which re-runs
//!    `/init` and re-bakes the creds), so the invariant holds on every path.
//!
//! Secret hygiene: cap-file VALUES are never logged — names, counts, presence
//! and destinations only. The nonce value is never logged either.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};

/// The broker's re-plumb route (GET = capability probe, POST = nonce-gated
/// write). Must byte-match the broker's `http_restore::RESTORE_CREDS_PATH`.
pub const RESTORE_CREDS_PATH: &str = "/v1/restore-creds";

/// The reserved cap-file name carrying the `:8732` bearer cap. The scrub is
/// gated on this cap broker-side, and the runner holds the same value inside
/// its cap-file set (it bakes it into `/init` on a cold boot).
pub const AUTHKEYS_CAP_NAME: &str = "authkeys.cap";

/// Total budget for one re-plumb (the broker inside a freshly-restored /
/// freshly-resumed VM answers within milliseconds; the budget covers a slow
/// scheduler or a mid-restore hiccup, bounded so a dead broker yields an honest
/// error instead of a hang).
pub const REPLUMB_BUDGET: Duration = Duration::from_secs(30);
/// Delay between re-plumb attempts.
pub const REPLUMB_POLL: Duration = Duration::from_secs(1);
/// Per-request timeout for probe / re-plumb HTTP calls.
pub const REPLUMB_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Path of the persisted one-time restore nonce in the per-uuid snapshot cache
/// dir. Lives NEXT TO `snap.vmstate`/`snap.mem` because it is meaningful only
/// for that exact snapshot (the frozen RAM holds the matching copy);
/// `snapshot::clear` removes it together with the snapshot files.
pub fn restore_token_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(".snapshot_restore_token")
}

/// Persist the restore nonce for the snapshot about to be created. Written
/// BEFORE `PUT /snapshot/create` so the (nonce file ↔ frozen-RAM nonce) pair is
/// consistent even if the runner dies mid-create. Best-effort on the mkdir
/// (the cache dir normally already exists); a write failure is returned so the
/// caller can abort the snapshot (a snapshot without a readable token could
/// never be warm-restored for a credential-bearing workspace).
///
/// # Errors
/// The token file could not be written.
pub fn write_restore_token(cache_dir: &Path, token: &str) -> Result<()> {
    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("create snapshot cache dir {}", cache_dir.display()))?;
    let p = restore_token_path(cache_dir);
    std::fs::write(&p, token.as_bytes())
        .with_context(|| format!("write restore token {}", p.display()))?;
    tracing::info!(
        path = %p.display(),
        "cred-restore: one-time restore nonce persisted next to the snapshot (value never logged)"
    );
    Ok(())
}

/// Read back the persisted restore nonce, `None` when absent/empty (a snapshot
/// from before the restore protocol, or a cleared cache) — the caller must then
/// COLD-boot a credential-bearing workspace.
#[must_use]
pub fn read_restore_token(cache_dir: &Path) -> Option<String> {
    std::fs::read_to_string(restore_token_path(cache_dir))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Does this VM need the scrub/restore bracket at all? Only a WORKSPACE with a
/// non-empty cap-file set holds broker credentials; a regular app (or a
/// capless workspace record) has nothing to scrub or re-plumb, and its
/// snapshot flow stays byte-identical to before.
#[must_use]
pub fn needs_cred_restore(is_workspace: bool, has_cap_files: bool) -> bool {
    is_workspace && has_cap_files
}

/// The authkeys-cap VALUE from the runner-held cap-file set — the bearer for
/// the scrub call. `None` when the record carries no `authkeys.cap` (a pre-cap
/// record); the scrub then goes out unauthenticated and the (new) broker
/// rejects it, which correctly ABORTS the snapshot.
#[must_use]
pub fn authkeys_cap_value(cap_files: &[(String, String)]) -> Option<&str> {
    cap_files
        .iter()
        .find(|(n, _)| n == AUTHKEYS_CAP_NAME)
        .map(|(_, v)| v.as_str())
}

/// Parse the one-time restore nonce out of a scrub 200-response body
/// (`{"restore_nonce": "..."}`). `None` on an old broker's plain `ok` body —
/// the caller must then treat the broker as non-restorable.
#[must_use]
pub fn parse_scrub_nonce(body: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("restore_nonce")?
        .as_str()
        .map(str::to_owned)
        .filter(|s| !s.is_empty())
}

/// The re-plumb request body: the SAME (name → value) cap-file set the rootfs
/// bake writes into `/init` on a cold boot, as `{"files": {...}}`.
#[must_use]
pub fn restore_body_json(cap_files: &[(String, String)]) -> serde_json::Value {
    let files: serde_json::Map<String, serde_json::Value> = cap_files
        .iter()
        .map(|(n, v)| (n.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::json!({ "files": files })
}

/// Sorted cap-file NAMES for logging (values never appear).
#[must_use]
pub fn cap_file_names(cap_files: &[(String, String)]) -> Vec<&str> {
    let mut names: Vec<&str> = cap_files.iter().map(|(n, _)| n.as_str()).collect();
    names.sort_unstable();
    names
}

/// Probe whether the in-guest broker supports the restore protocol
/// (`GET {base}/v1/restore-creds` → 2xx). Called BEFORE the scrub: an old
/// broker 404s and the caller must REFUSE to scrub/snapshot (never strip creds
/// that cannot be re-plumbed). One bounded request — the broker is live (the
/// VM is serving) so no retry loop is needed here.
///
/// # Errors
/// The probe did not return 2xx (old broker / transport failure) — the caller
/// must skip the snapshot and keep the live creds untouched.
pub async fn probe_restore_capability(client: &reqwest::Client, base: &str) -> Result<()> {
    let url = format!("{base}{RESTORE_CREDS_PATH}");
    let resp = client
        .get(&url)
        .timeout(REPLUMB_REQUEST_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("restore-capability probe GET {url} failed (broker unreachable)"))?;
    let status = resp.status();
    if !status.is_success() {
        bail!(
            "broker does not support /v1/restore-creds (HTTP {status}); refusing to scrub — \
             a snapshot of this guest could never be re-plumbed (base image predates the \
             restore protocol?)"
        );
    }
    tracing::info!(%url, "cred-restore: broker supports the restore protocol (probe 2xx)");
    Ok(())
}

/// RE-PLUMB the broker: POST the cap-file set with the one-time nonce, retrying
/// a not-yet-listening broker within [`REPLUMB_BUDGET`] (`budget`/`poll`
/// injectable for tests). Connection errors and 5xx are retried (the broker in
/// a just-restored VM may need a beat); 401/403/404 fail IMMEDIATELY (a nonce
/// mismatch or a protocol-less broker never heals by waiting). On success the
/// broker holds the creds again (its per-op reload reads the re-written files).
///
/// # Errors
/// The re-plumb did not achieve a 2xx within the budget. The caller must treat
/// the guest as credless: fall back to a cold boot (restore path) or clear the
/// snapshot + surface the failure (live post-snapshot path).
pub async fn replumb_creds(
    client: &reqwest::Client,
    base: &str,
    nonce: &str,
    cap_files: &[(String, String)],
    budget: Duration,
    poll: Duration,
) -> Result<()> {
    let url = format!("{base}{RESTORE_CREDS_PATH}");
    let body = restore_body_json(cap_files);
    let names = cap_file_names(cap_files);
    let start = tokio::time::Instant::now();
    let deadline = start + budget;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let outcome = client
            .post(&url)
            .timeout(REPLUMB_REQUEST_TIMEOUT)
            .bearer_auth(nonce)
            .json(&body)
            .send()
            .await;
        match outcome {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(
                    %url,
                    attempt,
                    elapsed_ms = start.elapsed().as_millis(),
                    files = names.len(),
                    names = ?names,
                    "cred-restore: broker re-plumbed with the cap-file set (values never logged)"
                );
                return Ok(());
            }
            Ok(resp) => {
                let status = resp.status();
                // Auth/protocol rejections are FINAL — retrying cannot help.
                if matches!(status.as_u16(), 400 | 401 | 403 | 404 | 405) {
                    bail!(
                        "broker rejected the credential re-plumb (HTTP {status}, attempt \
                         {attempt}): nonce mismatch or protocol-less broker — the guest \
                         is credless, fall back to cold boot"
                    );
                }
                if tokio::time::Instant::now() >= deadline {
                    bail!(
                        "credential re-plumb exhausted its {budget:?} budget on HTTP \
                         {status} (attempt {attempt}) — the guest is credless"
                    );
                }
                tracing::warn!(%url, attempt, %status, "cred-restore: transient re-plumb failure; retrying");
            }
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    bail!(
                        "credential re-plumb exhausted its {budget:?} budget (attempt \
                         {attempt}): broker never answered: {e}"
                    );
                }
                tracing::debug!(%url, attempt, error = %e, "cred-restore: broker not answering yet; retrying");
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::sleep(poll.min(remaining)).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn caps() -> Vec<(String, String)> {
        vec![
            ("app.url".to_owned(), "http://h:8788/git/SECRET-CAP".to_owned()),
            ("authkeys.cap".to_owned(), "AK-SECRET".to_owned()),
            (
                "forge-admin.token".to_owned(),
                r#"{"owner_user":"u","owner_password":"p","admin_token":"t"}"#.to_owned(),
            ),
        ]
    }

    /// A tiny canned-response HTTP server: replies `status` (+ `body`) to every
    /// request, recording each raw request. Returns `(base_url, requests)`.
    async fn stub_server(status: u16, body: &'static str) -> (String, Arc<tokio::sync::Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Arc<tokio::sync::Mutex<Vec<String>>> = Arc::default();
        let seen2 = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let seen = seen2.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    seen.lock().await.push(String::from_utf8_lossy(&buf[..n]).into_owned());
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}"), seen)
    }

    #[test]
    fn restore_token_round_trips_and_absent_is_none() {
        let td = tempfile::tempdir().unwrap();
        assert_eq!(read_restore_token(td.path()), None, "absent ⇒ None");
        write_restore_token(td.path(), "NONCE-123").unwrap();
        assert_eq!(read_restore_token(td.path()).as_deref(), Some("NONCE-123"));
        // Empty file reads back as None (fail toward cold boot).
        std::fs::write(restore_token_path(td.path()), "\n").unwrap();
        assert_eq!(read_restore_token(td.path()), None);
    }

    #[test]
    fn write_restore_token_creates_missing_cache_dir() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("apps").join("u").join("cache");
        write_restore_token(&dir, "N").unwrap();
        assert_eq!(read_restore_token(&dir).as_deref(), Some("N"));
    }

    #[test]
    fn needs_cred_restore_only_for_cap_bearing_workspaces() {
        assert!(needs_cred_restore(true, true), "workspace with caps");
        assert!(!needs_cred_restore(true, false), "capless workspace");
        assert!(!needs_cred_restore(false, true), "regular app never");
        assert!(!needs_cred_restore(false, false));
    }

    #[test]
    fn authkeys_cap_value_is_found_by_reserved_name() {
        assert_eq!(authkeys_cap_value(&caps()), Some("AK-SECRET"));
        assert_eq!(authkeys_cap_value(&[]), None);
    }

    #[test]
    fn parse_scrub_nonce_handles_new_and_old_brokers() {
        assert_eq!(
            parse_scrub_nonce(r#"{"restore_nonce":"abc123"}"#).as_deref(),
            Some("abc123")
        );
        // Old broker replies a plain "ok" body → None (non-restorable).
        assert_eq!(parse_scrub_nonce("ok"), None);
        assert_eq!(parse_scrub_nonce(r#"{"restore_nonce":""}"#), None);
        assert_eq!(parse_scrub_nonce("{}"), None);
    }

    #[test]
    fn restore_body_carries_all_files_and_names_are_loggable() {
        let body = restore_body_json(&caps());
        assert_eq!(body["files"]["app.url"], "http://h:8788/git/SECRET-CAP");
        assert_eq!(body["files"]["authkeys.cap"], "AK-SECRET");
        assert_eq!(
            cap_file_names(&caps()),
            vec!["app.url", "authkeys.cap", "forge-admin.token"]
        );
    }

    /// Probe: 2xx passes, 404 (old broker) is an error instructing the caller
    /// to REFUSE the scrub.
    #[tokio::test]
    async fn probe_accepts_2xx_and_rejects_404() {
        let (base, _seen) = stub_server(204, "").await;
        let client = reqwest::Client::new();
        assert!(probe_restore_capability(&client, &base).await.is_ok());

        let (base404, _seen) = stub_server(404, "not found").await;
        let err = probe_restore_capability(&client, &base404)
            .await
            .expect_err("404 must refuse");
        assert!(
            err.to_string().contains("refusing to scrub"),
            "the error must instruct the caller to keep the creds: {err}"
        );
    }

    /// Re-plumb happy path: POSTs the nonce as bearer + the full file map, and
    /// returns Ok on 200.
    #[tokio::test]
    async fn replumb_posts_nonce_and_files_and_succeeds_on_200() {
        let (base, seen) = stub_server(200, "ok").await;
        let client = reqwest::Client::new();
        replumb_creds(
            &client,
            &base,
            "THE-NONCE",
            &caps(),
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .await
        .expect("200 must succeed");
        let reqs = seen.lock().await;
        let raw = reqs.first().expect("one request");
        assert!(raw.starts_with(&format!("POST {RESTORE_CREDS_PATH}")), "{raw}");
        assert!(raw.contains("authorization: Bearer THE-NONCE") || raw.contains("Authorization: Bearer THE-NONCE"), "{raw}");
        assert!(raw.contains("SECRET-CAP"), "body must carry the cap values");
    }

    /// A 401 (nonce mismatch / consumed) fails IMMEDIATELY — no futile retry
    /// loop, an honest error, never a hang.
    #[tokio::test]
    async fn replumb_fails_fast_on_401() {
        let (base, seen) = stub_server(401, "unauthorized").await;
        let client = reqwest::Client::new();
        let start = std::time::Instant::now();
        let err = replumb_creds(
            &client,
            &base,
            "WRONG",
            &caps(),
            Duration::from_secs(30),
            Duration::from_millis(50),
        )
        .await
        .expect_err("401 must fail");
        assert!(start.elapsed() < Duration::from_secs(5), "must not burn the budget");
        assert!(err.to_string().contains("cold boot"), "actionable error: {err}");
        assert_eq!(seen.lock().await.len(), 1, "exactly one attempt on 401");
    }

    /// A broker SLOW to come up is waited for (bounded): the listener starts
    /// only after a delay, and the retry loop lands the re-plumb once it is up.
    #[tokio::test]
    async fn replumb_waits_for_a_slow_broker_within_budget() {
        // Reserve an address, then delay the actual listener.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let hits = Arc::new(AtomicU32::new(0));
        let hits2 = hits.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            loop {
                let Ok((mut s, _)) = listener.accept().await else { return };
                hits2.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 65536];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                    .await;
            }
        });
        let client = reqwest::Client::new();
        replumb_creds(
            &client,
            &format!("http://{addr}"),
            "N",
            &caps(),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await
        .expect("must succeed once the broker comes up");
        assert!(hits.load(Ordering::SeqCst) >= 1);
    }

    /// A broker that NEVER comes up yields an honest error within the budget —
    /// not a hang.
    #[tokio::test]
    async fn replumb_gives_up_honestly_when_broker_never_answers() {
        // An address nothing listens on.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let client = reqwest::Client::new();
        let start = std::time::Instant::now();
        let err = replumb_creds(
            &client,
            &format!("http://{addr}"),
            "N",
            &caps(),
            Duration::from_millis(600),
            Duration::from_millis(100),
        )
        .await
        .expect_err("must give up");
        assert!(start.elapsed() < Duration::from_secs(10), "bounded, no hang");
        assert!(err.to_string().contains("budget"), "honest error: {err}");
        // The error must NEVER leak a cap value.
        assert!(!err.to_string().contains("SECRET-CAP"));
        assert!(!err.to_string().contains("AK-SECRET"));
    }
}
