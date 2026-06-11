//! Tests for [`super`] (git_proxy) — handler forwarding, registry, and the
//! IPv4 router (B1 tests). Out-of-line module (`#[path]` in git_proxy.rs) per
//! the <500-line file rule.

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

// ── helpers ───────────────────────────────────────────────────────────────────

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

// ── 1. GET info/refs forwarded with injected Authorization ────────────────────

#[tokio::test]
async fn git_proxy_forwards_info_refs_with_injected_auth() {
    let server = MockServer::start().await;
    let expected_auth = format!("Basic {}", BASE64.encode("x-access-token:tok123"));

    Mock::given(method("GET"))
        .and(path("/info/refs"))
        .and(query_param("service", "git-upload-pack"))
        .and(header("Authorization", expected_auth.as_str()))
        // Modern git negotiates protocol v2 via this header on info/refs;
        // the proxy must forward it or clones silently downgrade to v1.
        .and(header("Git-Protocol", "version=2"))
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
        .header("Git-Protocol", "version=2")
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

// ── 2. POST git-upload-pack streams body ──────────────────────────────────────

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

// ── 3. Unknown cap → 403 ─────────────────────────────────────────────────────

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

// ── 4. Expired cap → 403 ─────────────────────────────────────────────────────

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

// ── 4b. Multibyte-UTF-8 cap must not panic the handler ───────────────────────

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

// ── 5. Unlisted tail → 404, upstream gets zero requests ──────────────────────

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

// ── 6. Cap cannot reach other repo — upstream URL is fixed at registration ────

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

// ── B1: IPv4 router tests ─────────────────────────────────────────────────────

/// B1-a: the IPv4 router serves the git route against the SHARED registry.
/// Register a cap on the state's `git_sessions`, hit the route via the IPv4
/// router — must forward (200) to the mock upstream, proving the Arc is shared.
#[tokio::test]
async fn git_proxy_ipv4_router_forwards_known_cap() {
    let server = MockServer::start().await;
    let expected_auth = format!("Basic {}", BASE64.encode("x-access-token:tok-ipv4"));

    Mock::given(method("GET"))
        .and(path("/info/refs"))
        .and(query_param("service", "git-upload-pack"))
        .and(header("Authorization", expected_auth.as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"pkt".to_vec())
                .insert_header(
                    "Content-Type",
                    "application/x-git-upload-pack-advertisement",
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let state = make_state();
    register_cap(&state, "cap-ipv4", &server.uri(), "tok-ipv4");

    // Build the IPv4 router with the SAME state Arc (shared registry).
    let shared = std::sync::Arc::new(state);
    let ipv4_app = super::git_proxy_ipv4_router(shared.clone());

    let req = Request::builder()
        .method("GET")
        .uri("/git/cap-ipv4/info/refs?service=git-upload-pack")
        .body(Body::empty())
        .unwrap();

    let resp = ipv4_app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "IPv4 router must forward known cap");
    server.verify().await;
}

/// B1-b: the IPv4 router returns 403 for an unknown cap — same guard as the
/// mesh router.
#[tokio::test]
async fn git_proxy_ipv4_router_unknown_cap_403() {
    let state = make_state();
    let shared = std::sync::Arc::new(state);
    let ipv4_app = super::git_proxy_ipv4_router(shared);

    let req = Request::builder()
        .method("GET")
        .uri("/git/no-such-cap/info/refs?service=git-upload-pack")
        .body(Body::empty())
        .unwrap();

    let resp = ipv4_app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "IPv4 router must 403 unknown cap");
}

/// B1-c: registry is SHARED — a cap registered once is accessible via both
/// the mesh router and the IPv4 router (same Arc, no duplication).
#[tokio::test]
async fn git_proxy_ipv4_and_mesh_routers_share_registry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/info/refs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"pkt".to_vec())
                .insert_header(
                    "Content-Type",
                    "application/x-git-upload-pack-advertisement",
                ),
        )
        .expect(2) // called once via each router
        .mount(&server)
        .await;

    let state = make_state();
    register_cap(&state, "cap-shared", &server.uri(), "tok-shared");

    let shared = std::sync::Arc::new(state);
    let mesh_app = router((*shared).clone());
    let ipv4_app = super::git_proxy_ipv4_router(shared.clone());

    // Hit via mesh router.
    let req1 = Request::builder()
        .method("GET")
        .uri("/git/cap-shared/info/refs?service=git-upload-pack")
        .body(Body::empty())
        .unwrap();
    let resp1 = mesh_app.oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // Hit via IPv4 router — same cap must work (shared Arc).
    let req2 = Request::builder()
        .method("GET")
        .uri("/git/cap-shared/info/refs?service=git-upload-pack")
        .body(Body::empty())
        .unwrap();
    let resp2 = ipv4_app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    server.verify().await;
}

// ── 7. Registry unit tests ────────────────────────────────────────────────────

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
