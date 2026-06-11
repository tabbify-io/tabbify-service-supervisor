//! Smart-HTTP git proxy: the tokenless in-VM remote for dev sessions.
//!
//! Two listeners expose the same git routes:
//! 1. The mesh router (IPv6 ULA :8730) — reached by non-FC clients on the mesh.
//! 2. An IPv4-only listener on `0.0.0.0:GIT_PROXY_IPV4_PORT` — the ONLY
//!    address a Firecracker guest can reach (IPv4-only /30 tap, no IPv6/mesh).
//!    Any guest reaches it via its default gateway = the tap's `host_ip`.
//!
//! Both listeners share the SAME `GitSessions` Arc — a cap registered once is
//! valid on both ports simultaneously. The IPv4 listener is auth-free at the
//! transport layer: the 256-bit capability token in the URL IS the auth.
//!
//! SECURITY NOTE: `0.0.0.0:GIT_PROXY_IPV4_PORT` is also reachable on the WiFi
//! uplink. The 256-bit capability is the primary guard (unguessable,
//! single-repo, short-lived, revoked on session close). As depth-in-defence,
//! `setup_git_proxy_nat` in `linux.rs` DROPs inbound on the uplink and ACCEPTs
//! only from the FC tap subnet (172.31.0.0/16). The iptables guard is
//! best-effort and is logged when absent; the cap is the real gate.
//!
//! Original design (spec: 2026-06-10-mcp-dev-sandbox-design §B): credentials
//! injected outside the VM — the VM never sees a provider token.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use http::StatusCode;

use crate::api::SharedState;

// ── Port constant ─────────────────────────────────────────────────────────────

/// IPv4 port the git proxy binds on `0.0.0.0` so FC guests can reach it via
/// their tap gateway. Port 8788 is Tabbify-internal; not IANA-assigned to any
/// conflicting service. Shared by the listener bind (main.rs) and the
/// `git_remote` URL builder (dev_sessions.rs) so the two never drift.
pub const GIT_PROXY_IPV4_PORT: u16 = 8788;

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

    /// Test-only: snapshot the registered capability tokens. Lets the
    /// dev-session tests assert a failed create leaves NO cap behind (the cap
    /// never leaves the handler on the error path, so the registry is the only
    /// place revocation is observable).
    #[cfg(test)]
    pub(crate) fn registered_caps(&self) -> Vec<String> {
        self.0
            .lock()
            .expect("git sessions lock")
            .keys()
            .cloned()
            .collect()
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

// ── IPv4 router ───────────────────────────────────────────────────────────────

/// Build a minimal axum [`Router`] that exposes ONLY the git smart-HTTP proxy
/// routes on an IPv4 listener (`0.0.0.0:GIT_PROXY_IPV4_PORT`).
///
/// The shared `git_sessions` Arc is threaded in via [`SharedState`] — no second
/// registry is created. There is NO authentication middleware here: the 256-bit
/// capability in the URL is the sole auth gate (the VM has no node-key).
pub fn git_proxy_ipv4_router(state: Arc<crate::api::SupervisorState>) -> Router {
    Router::new()
        .route("/git/:cap/*tail", get(git_proxy).post(git_proxy))
        .with_state(state)
}

// ── Shared reqwest client ─────────────────────────────────────────────────────

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    // connect_timeout only — an unreachable provider must fail fast (502 to the
    // VM) instead of hanging git ops on the OS TCP timeout (~2min). No TOTAL
    // timeout: pack transfers are legitimately minutes-long.
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("git proxy http client")
    })
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
    // Copy only the git wire-protocol headers (Content-Type + Accept +
    // Git-Protocol — modern git sends `Git-Protocol: version=2` on info/refs;
    // dropping it silently downgrades to v1 and defeats `--filter` clones).
    // Notably we do NOT forward Authorization, Host, or any other header that
    // could carry credentials or routing hints from the VM.
    let method = req.method().clone();
    let inbound_headers = req.headers();
    let content_type = inbound_headers.get(http::header::CONTENT_TYPE).cloned();
    let accept = inbound_headers.get(http::header::ACCEPT).cloned();
    let git_protocol = inbound_headers.get("git-protocol").cloned();

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
    if let Some(gp) = git_protocol {
        builder = builder.header("Git-Protocol", gp.as_bytes());
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

// Tests live out-of-line (<500-line file rule).
#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "git_proxy_tests.rs"]
mod tests;
