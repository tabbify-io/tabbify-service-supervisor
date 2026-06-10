//! Smart-HTTP git proxy: the tokenless in-VM remote for dev sessions.
//!
//! A dev-session VM clones/pushes against `http://[sup-ula]:8730/git/<cap>`;
//! this module validates the capability and forwards the three git smart-HTTP
//! endpoints to the provider with `Authorization` injected OUTSIDE the VM —
//! credentials never enter the sandbox (spec: 2026-06-10-mcp-dev-sandbox-design §B).
//! The upstream URL is FIXED at registration: a capability can never reach any
//! other repo, regardless of what the request says.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use http::StatusCode;

use crate::api::SharedState;

// ── Registry ─────────────────────────────────────────────────────────────────

/// One registered dev-session's git access.
pub struct GitSessionEntry {
    /// Provider clone URL WITHOUT credentials, no trailing `.git` requirement —
    /// e.g. `https://github.com/Lsneg/tabbify-presentation.git`.
    pub upstream_url: String,
    /// Short-lived provider token (e.g. GitHub App installation token).
    /// Injected as `Basic x-access-token:<token>`; never logged.
    pub token: String,
    /// When this session's token expires; lookups after this instant return `None`.
    pub expires_at: Instant,
}

/// Capability → session registry. `Mutex<HashMap>` is fine: a handful of
/// concurrent dev sessions, requests are seconds apart.
#[derive(Default)]
pub struct GitSessions(Mutex<HashMap<String, GitSessionEntry>>);

impl GitSessions {
    /// Register a new capability with its session entry. Overwrites any existing entry for the same cap.
    pub fn register(&self, cap: String, entry: GitSessionEntry) {
        self.0.lock().expect("git sessions lock").insert(cap, entry);
    }

    /// Remove the capability from the registry (revoke access immediately).
    pub fn revoke(&self, cap: &str) {
        self.0.lock().expect("git sessions lock").remove(cap);
    }

    /// Replace the token (mint refresh); returns false if cap unknown.
    pub fn refresh_token(&self, cap: &str, token: String, expires_at: Instant) -> bool {
        let mut guard = self.0.lock().expect("git sessions lock");
        if let Some(entry) = guard.get_mut(cap) {
            entry.token = token;
            entry.expires_at = expires_at;
            true
        } else {
            false
        }
    }

    /// Returns `(upstream_url, token)` if registered and unexpired.
    fn lookup(&self, cap: &str) -> Option<(String, String)> {
        let guard = self.0.lock().expect("git sessions lock");
        let entry = guard.get(cap)?;
        if entry.expires_at < Instant::now() {
            return None;
        }
        Some((entry.upstream_url.clone(), entry.token.clone()))
    }
}

// ── Shared reqwest client ─────────────────────────────────────────────────────

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// Proxy handler for the three git smart-HTTP endpoints.
///
/// Mounted as `.route("/git/:cap/*tail", get(git_proxy).post(git_proxy))`.
///
/// The `:cap` segment is the session capability token; `*tail` is the git
/// wire-protocol path segment (one of `info/refs`, `git-upload-pack`,
/// `git-receive-pack`). The upstream URL is fixed at registration — the VM
/// cannot redirect to any other repo regardless of the request's Host header
/// or query string.
pub async fn git_proxy(
    State(state): State<SharedState>,
    Path((cap, tail)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    // Byte-safe prefix: `cap` is untrusted VM input and may be multibyte UTF-8 —
    // `&cap[..8]` would panic on a non-char-boundary; `get` returns None instead.
    let cap_prefix = cap.get(..8).unwrap_or(&cap);
    tracing::debug!(cap = %cap_prefix, tail = %tail, "git proxy request");

    let Some((upstream, token)) = state.git_sessions.lookup(&cap) else {
        return (StatusCode::FORBIDDEN, "unknown or expired git session").into_response();
    };

    // STRICT allow-list — exactly the three smart-HTTP endpoints:
    //   GET  info/refs?service=git-upload-pack|git-receive-pack
    //   POST git-upload-pack
    //   POST git-receive-pack
    if !matches!(
        tail.as_str(),
        "info/refs" | "git-upload-pack" | "git-receive-pack"
    ) {
        return (StatusCode::NOT_FOUND, "not a git smart-HTTP endpoint").into_response();
    }

    // Build the upstream URL: registered base + tail + original query string.
    // The upstream base is FIXED — a request cannot influence which repo is reached.
    let url = format!(
        "{}/{}{}",
        upstream.trim_end_matches('/'),
        tail,
        req.uri()
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default()
    );

    // Authorization: Basic base64("x-access-token:<token>")
    let credentials = BASE64.encode(format!("x-access-token:{token}"));
    let auth_header = format!("Basic {credentials}");

    // Extract method, headers, and query BEFORE consuming the request body.
    // Copy only the git wire-protocol headers (Content-Type + Accept).
    // Notably we do NOT forward Authorization, Host, or any other header that
    // could carry credentials or routing hints from the VM.
    let method = req.method().clone();
    let inbound_headers = req.headers();
    let content_type = inbound_headers.get(http::header::CONTENT_TYPE).cloned();
    let accept = inbound_headers.get(http::header::ACCEPT).cloned();

    // Stream the request body to the upstream so multi-MB pack files don't
    // buffer in memory.
    let body_stream = req.into_body().into_data_stream();
    let body = reqwest::Body::wrap_stream(body_stream);

    let client = http_client();

    let mut builder = client
        .request(
            reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
            &url,
        )
        .header(reqwest::header::AUTHORIZATION, &auth_header);

    if let Some(ct) = content_type {
        builder = builder.header(reqwest::header::CONTENT_TYPE, ct.as_bytes());
    }
    if let Some(acc) = accept {
        builder = builder.header(reqwest::header::ACCEPT, acc.as_bytes());
    }

    let upstream_resp = match builder.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            // Never echo the token — log only the error kind.
            tracing::warn!(cap = %cap_prefix, error = %e, "git proxy upstream transport error");
            return (StatusCode::BAD_GATEWAY, "upstream transport error").into_response();
        }
    };

    // Mirror the upstream status and copy wire-protocol headers to the client.
    let status = upstream_resp.status();
    let upstream_ct = upstream_resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let upstream_cc = upstream_resp
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .cloned();

    // Stream the upstream response body back without buffering.
    let resp_stream = upstream_resp.bytes_stream();
    let axum_body = axum::body::Body::from_stream(resp_stream);

    let mut response = axum::response::Response::builder().status(status.as_u16());

    if let Some(ct) = upstream_ct {
        response = response.header(http::header::CONTENT_TYPE, ct.as_bytes());
    }
    if let Some(cc) = upstream_cc {
        response = response.header(http::header::CACHE_CONTROL, cc.as_bytes());
    }

    response
        .body(axum_body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use axum::body::Body;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use http::{Request, StatusCode};
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;
    use wiremock::matchers::{body_bytes, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::api::{SupervisorState, router};
    use crate::fetcher::S3Fetcher;
    use crate::orchestrator::{Orchestrator, SharedRunnerConfig};

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_state() -> SupervisorState {
        let runner_dir = PathBuf::from("/tmp/git-proxy-test-runners");
        let data_dir = PathBuf::from("/tmp/git-proxy-test-data");
        let orchestrator = Orchestrator::new(
            SharedRunnerConfig {
                runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
                s3_base_url: "http://s3.invalid".to_owned(),
                data_dir: data_dir.clone(),
                parent: None,
                no_mesh: true,
                relay_url: None,
                relay_only: false,
            },
            runner_dir,
        );
        let fetcher = S3Fetcher::new("http://s3.invalid", data_dir);
        SupervisorState::new(
            orchestrator,
            fetcher,
            "test-supervisor".to_owned(),
            "::1".to_owned(),
        )
    }

    fn register_cap(state: &SupervisorState, cap: &str, upstream: &str, token: &str) {
        state.git_sessions.register(
            cap.to_owned(),
            GitSessionEntry {
                upstream_url: upstream.to_owned(),
                token: token.to_owned(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            },
        );
    }

    async fn collect_body(resp: axum::response::Response) -> bytes::Bytes {
        resp.into_body()
            .collect()
            .await
            .expect("body collection failed")
            .to_bytes()
    }

    // ── 1. GET info/refs forwarded with injected Authorization ─────────────────

    #[tokio::test]
    async fn git_proxy_forwards_info_refs_with_injected_auth() {
        let server = MockServer::start().await;
        let expected_auth = format!("Basic {}", BASE64.encode("x-access-token:tok123"));

        Mock::given(method("GET"))
            .and(path("/info/refs"))
            .and(query_param("service", "git-upload-pack"))
            .and(header("Authorization", expected_auth.as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"001e# service=git-upload-pack\n0000".to_vec())
                    .insert_header(
                        "Content-Type",
                        "application/x-git-upload-pack-advertisement",
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;

        let state = make_state();
        register_cap(&state, "cap-abc", &server.uri(), "tok123");
        let app = router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/git/cap-abc/info/refs?service=git-upload-pack")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("git-upload-pack"),
            "Content-Type must round-trip from upstream; got: {ct}"
        );

        let body = collect_body(resp).await;
        assert_eq!(
            &body[..],
            b"001e# service=git-upload-pack\n0000",
            "body must round-trip verbatim"
        );

        server.verify().await;
    }

    // ── 2. POST git-upload-pack streams body ──────────────────────────────────

    #[tokio::test]
    async fn git_proxy_streams_post_body_to_upload_pack() {
        let server = MockServer::start().await;
        let pack_request = b"0011command=ls-refs\n".to_vec();

        Mock::given(method("POST"))
            .and(path("/git-upload-pack"))
            // Pin the proxied request body: a dropped/garbled stream must fail
            // the match (the mock then 404s and `.expect(1)` fails on verify).
            .and(body_bytes(pack_request.clone()))
            .and(header(
                "Content-Type",
                "application/x-git-upload-pack-request",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(pack_request.clone())
                    .insert_header("Content-Type", "application/x-git-upload-pack-result"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let state = make_state();
        register_cap(&state, "cap-post", &server.uri(), "tokpost");
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/git/cap-post/git-upload-pack")
            .header("Content-Type", "application/x-git-upload-pack-request")
            .body(Body::from(pack_request.clone()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("git-upload-pack-result"),
            "Content-Type must round-trip; got: {ct}"
        );

        let body = collect_body(resp).await;
        assert_eq!(
            &body[..],
            &pack_request[..],
            "response body must round-trip"
        );

        server.verify().await;
    }

    // ── 3. Unknown cap → 403 ──────────────────────────────────────────────────

    #[tokio::test]
    async fn git_proxy_unknown_cap_403() {
        let state = make_state();
        let app = router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/git/no-such-cap/info/refs?service=git-upload-pack")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── 4. Expired cap → 403 ─────────────────────────────────────────────────

    #[tokio::test]
    async fn git_proxy_expired_cap_403() {
        let state = make_state();
        state.git_sessions.register(
            "cap-expired".to_owned(),
            GitSessionEntry {
                upstream_url: "http://upstream.invalid".to_owned(),
                token: "expiredtok".to_owned(),
                // expired in the past
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );
        let app = router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/git/cap-expired/info/refs?service=git-upload-pack")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── 4b. Multibyte-UTF-8 cap must not panic the handler ───────────────────

    /// Regression: an untrusted VM can send a multibyte-UTF-8 cap (here `€€€`,
    /// 9 bytes / 3 chars — byte 8 is NOT a char boundary). The old
    /// `&cap[..8]` log-prefix slice panicked "not a char boundary"; the
    /// byte-safe `cap.get(..8)` must instead fall through to a clean 403.
    #[tokio::test]
    async fn git_proxy_multibyte_cap_does_not_panic() {
        let state = make_state();
        let app = router(state);

        // Percent-encoded `€€€` — axum decodes the path param to the raw UTF-8.
        let req = Request::builder()
            .method("GET")
            .uri("/git/%E2%82%AC%E2%82%AC%E2%82%AC/info/refs?service=git-upload-pack")
            .body(Body::empty())
            .unwrap();

        // A handler panic would propagate out of `oneshot` and fail the test.
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "multibyte cap must be a clean 403, not a panic/500"
        );
    }

    // ── 5. Unlisted tail → 404, upstream gets zero requests ──────────────────

    #[tokio::test]
    async fn git_proxy_unlisted_tail_404() {
        let server = MockServer::start().await;
        // No mock routes registered — any request to the server would fail the test.

        let state = make_state();
        register_cap(&state, "cap-head", &server.uri(), "tokhd");
        let app = router(state);

        // `/HEAD` is a non-smart-HTTP path some git operations probe.
        let req = Request::builder()
            .method("GET")
            .uri("/git/cap-head/HEAD")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Upstream must have received zero requests.
        let received = server.received_requests().await.unwrap();
        assert_eq!(
            received.len(),
            0,
            "upstream must not be contacted for unlisted tail; got {} requests",
            received.len()
        );
    }

    // ── 6. Cap cannot reach other repo — upstream URL is fixed at registration ─

    #[tokio::test]
    async fn git_proxy_cap_cannot_reach_other_repo() {
        let registered_server = MockServer::start().await;
        let other_server = MockServer::start().await;

        // Only the registered server has a mock; the other has none.
        Mock::given(method("GET"))
            .and(path("/info/refs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"pkt-line".to_vec())
                    .insert_header(
                        "Content-Type",
                        "application/x-git-upload-pack-advertisement",
                    ),
            )
            .expect(1)
            .mount(&registered_server)
            .await;

        let state = make_state();
        register_cap(&state, "cap-fixed", &registered_server.uri(), "tokfixed");
        let app = router(state);

        // The request carries a different Host header pointing at `other_server`.
        let req = Request::builder()
            .method("GET")
            .uri("/git/cap-fixed/info/refs?service=git-upload-pack")
            .header("Host", other_server.address().to_string())
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // Should succeed against the REGISTERED upstream, not the one in Host.
        assert_eq!(resp.status(), StatusCode::OK);

        // The registered server received the request; the other server got none.
        registered_server.verify().await;
        let other_requests = other_server.received_requests().await.unwrap();
        assert_eq!(
            other_requests.len(),
            0,
            "other server must not be contacted; request must go to registered upstream"
        );
    }

    // ── 7. Registry unit tests ────────────────────────────────────────────────

    #[test]
    fn registry_register_lookup() {
        let sessions = GitSessions::default();
        sessions.register(
            "cap1".to_owned(),
            GitSessionEntry {
                upstream_url: "https://github.com/acme/app.git".to_owned(),
                token: "tok1".to_owned(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            },
        );
        let result = sessions.lookup("cap1");
        assert!(result.is_some(), "registered cap must be found");
        let (url, tok) = result.unwrap();
        assert_eq!(url, "https://github.com/acme/app.git");
        assert_eq!(tok, "tok1");
    }

    #[test]
    fn registry_revoke_returns_none() {
        let sessions = GitSessions::default();
        sessions.register(
            "cap2".to_owned(),
            GitSessionEntry {
                upstream_url: "https://github.com/acme/app.git".to_owned(),
                token: "tok2".to_owned(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            },
        );
        sessions.revoke("cap2");
        assert!(
            sessions.lookup("cap2").is_none(),
            "revoked cap must not be found"
        );
    }

    #[test]
    fn registry_refresh_token_on_unknown_cap_returns_false() {
        let sessions = GitSessions::default();
        let refreshed = sessions.refresh_token(
            "no-such",
            "newtok".to_owned(),
            Instant::now() + Duration::from_secs(60),
        );
        assert!(!refreshed, "refresh_token on unknown cap must return false");
    }

    #[test]
    fn registry_expired_cap_returns_none() {
        let sessions = GitSessions::default();
        sessions.register(
            "cap-expired".to_owned(),
            GitSessionEntry {
                upstream_url: "https://github.com/acme/app.git".to_owned(),
                token: "tok-expired".to_owned(),
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );
        assert!(
            sessions.lookup("cap-expired").is_none(),
            "expired cap must return None"
        );
    }

    #[test]
    fn registry_refresh_token_updates_entry() {
        let sessions = GitSessions::default();
        sessions.register(
            "cap3".to_owned(),
            GitSessionEntry {
                upstream_url: "https://github.com/acme/app.git".to_owned(),
                token: "old-tok".to_owned(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            },
        );
        let ok = sessions.refresh_token(
            "cap3",
            "new-tok".to_owned(),
            Instant::now() + Duration::from_secs(7200),
        );
        assert!(ok, "refresh_token on known cap must return true");
        let (_, tok) = sessions.lookup("cap3").unwrap();
        assert_eq!(tok, "new-tok", "token must be updated after refresh");
    }
}
