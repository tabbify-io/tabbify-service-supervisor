//! Self-provisioning the supervisor's mesh join token at STARTUP.
//!
//! # Why
//!
//! The coordinator REVOKES a peer's join token whenever the peer is recreated —
//! the token is bound to an ephemeral wireguard key that regenerates. A
//! supervisor reads a STATIC token from `TABBIFY_JOIN_TOKEN`, so a rebuilt or
//! replaced box comes up holding a dead token, fails the join, and needs a human
//! to re-mint by hand (see [`crate::mesh::JOIN_401_GUIDANCE`]). Worse, that
//! static token lives only in the box's own env file: recreating the instance
//! restores a template without it.
//!
//! The node solved the same problem by self-minting at boot, but it can only do
//! so because it already holds `AUTH_ADMIN_TOKEN`. Copying that credential onto
//! every supervisor would make every edge box able to mint a token for ANY
//! subject, network and tag set — one compromised box would compromise the
//! platform's identity system.
//!
//! # How
//!
//! Instead the supervisor holds a NARROW credential (`TABBIFY_RENEWAL_SECRET`)
//! that auth has bound, at creation time, to exactly one identity. The renew
//! endpoint sends NO identity — network, subject and tags all come from auth's
//! stored row — so the worst a stolen secret can do is obtain a token its holder
//! already had.
//!
//! Auth is not reachable from a box that is not on the mesh (it is bridge-only
//! on the serving host), so the call goes through the node's public edge, which
//! relays it verbatim and adds no authority of its own.
//!
//! # Failure posture
//!
//! Every failure degrades to the static token: a supervisor that cannot renew is
//! never WORSE off than today. Bounded by attempt count AND by per-request
//! timeout, so an unreachable or hung endpoint cannot delay boot indefinitely.

use std::time::Duration;

/// The narrow, identity-bound credential authorizing THIS peer's own renewal.
/// A secret — never logged.
pub const RENEWAL_SECRET_ENV: &str = "TABBIFY_RENEWAL_SECRET";

/// Base URL of the public edge relaying the renewal to auth.
pub const RENEW_URL_ENV: &str = "TABBIFY_RENEW_URL";

/// Gate, default ON, so self-mint can be switched off without a redeploy.
pub const SELF_MINT_ENV: &str = "TABBIFY_MESH_SELF_MINT";

/// The platform's public API edge.
pub const DEFAULT_RENEW_URL: &str = "https://api.tabbify.io";

/// Per-request bound. Auth answers in milliseconds; this exists so a hung
/// endpoint (TCP accepted, nothing sent) cannot wedge boot.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Attempts before falling back. Covers auth still starting in a shared compose
/// bring-up without turning a genuine outage into a long boot stall.
const MAX_ATTEMPTS: u32 = 3;

/// Backoff between attempts.
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// Is self-mint enabled? Default ON; any of `0`/`false`/`off` disables it.
#[must_use]
pub fn self_mint_enabled(raw: Option<&str>) -> bool {
    match raw.map(str::trim) {
        None | Some("") => true,
        Some(v) => !matches!(v.to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"),
    }
}

/// The renewal endpoint URL for a base.
#[must_use]
pub fn renew_endpoint(base_url: &str) -> String {
    format!(
        "{}/v1/mesh/renew-join-token",
        base_url.trim_end_matches('/')
    )
}

/// The minted token from a successful renewal response body.
///
/// Tolerant on purpose: the response is auth's `TokenResponse`, and this reads
/// only the one field it needs, so an added field never breaks boot.
///
/// # Errors
/// When the body is not JSON, or carries no non-empty `token`.
pub fn token_from_response(body: &str) -> anyhow::Result<String> {
    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| anyhow::anyhow!("renew response is not JSON: {e}"))?;
    let token = value
        .get("token")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if token.is_empty() {
        anyhow::bail!("renew response carried no token");
    }
    Ok(token.to_owned())
}

/// Exchange the renewal credential for a fresh join token. One attempt.
///
/// # Errors
/// Transport failure, a non-2xx from the edge, or an unusable body.
pub async fn renew_once(
    client: &reqwest::Client,
    base_url: &str,
    secret: &str,
) -> anyhow::Result<String> {
    let resp = client
        .post(renew_endpoint(base_url))
        .bearer_auth(secret)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        // The relay returns auth's own body, so the reason for a refusal
        // (revoked, unknown) is right here rather than in another service's log.
        anyhow::bail!("join renewal refused: status {status}: {}", body.trim());
    }
    token_from_response(&body)
}

/// Self-mint a fresh join token, or `None` to fall back to the static one.
///
/// Reads `TABBIFY_RENEWAL_SECRET` / `TABBIFY_RENEW_URL` / `TABBIFY_MESH_SELF_MINT`
/// from the environment. Absent secret ⇒ `None` immediately (an un-migrated box
/// behaves exactly as before).
pub async fn self_minted_join_token() -> Option<String> {
    if !self_mint_enabled(std::env::var(SELF_MINT_ENV).ok().as_deref()) {
        tracing::info!("mesh self-mint disabled by {SELF_MINT_ENV}; using the static join token");
        return None;
    }
    let secret = std::env::var(RENEWAL_SECRET_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let base_url =
        std::env::var(RENEW_URL_ENV).unwrap_or_else(|_| DEFAULT_RENEW_URL.to_owned());

    let client = match reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "mesh self-mint: http client build failed");
            return None;
        }
    };

    for attempt in 1..=MAX_ATTEMPTS {
        match renew_once(&client, &base_url, &secret).await {
            Ok(token) => {
                // Never the token, never the secret — only that it happened.
                tracing::info!(
                    attempt,
                    endpoint = %renew_endpoint(&base_url),
                    "mesh self-mint: fresh join token obtained"
                );
                return Some(token);
            }
            Err(e) => {
                tracing::warn!(
                    attempt,
                    max = MAX_ATTEMPTS,
                    error = %e,
                    "mesh self-mint attempt failed"
                );
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
    }
    tracing::warn!(
        "mesh self-mint exhausted {MAX_ATTEMPTS} attempts — falling back to the static \
         {} (the join will fail if the coordinator revoked it)",
        crate::mesh::JOIN_TOKEN_ENV
    );
    None
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::*;

    #[test]
    fn self_mint_is_on_unless_explicitly_disabled() {
        assert!(self_mint_enabled(None));
        assert!(self_mint_enabled(Some("")));
        assert!(self_mint_enabled(Some("1")));
        assert!(self_mint_enabled(Some("true")));
        for off in ["0", "false", "off", "no", "FALSE", " off "] {
            assert!(!self_mint_enabled(Some(off)), "{off} must disable self-mint");
        }
    }

    #[test]
    fn endpoint_is_built_without_a_double_slash() {
        assert_eq!(
            renew_endpoint("https://api.tabbify.io/"),
            "https://api.tabbify.io/v1/mesh/renew-join-token"
        );
        assert_eq!(
            renew_endpoint("https://api.tabbify.io"),
            "https://api.tabbify.io/v1/mesh/renew-join-token"
        );
    }

    #[test]
    fn the_token_is_read_from_the_response_tolerantly() {
        assert_eq!(
            token_from_response(r#"{"token":"jwt.abc","kind":"join","unknown":1}"#).unwrap(),
            "jwt.abc",
            "an added field must never break boot"
        );
        assert!(token_from_response("not json").is_err());
        assert!(token_from_response(r#"{"kind":"join"}"#).is_err());
        assert!(
            token_from_response(r#"{"token":""}"#).is_err(),
            "an empty token is a failure, not a token"
        );
    }

    #[tokio::test]
    async fn a_successful_renewal_sends_the_secret_as_a_bearer() {
        let edge = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/mesh/renew-join-token"))
            .and(header("authorization", "Bearer tbf_renew_secret"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"fresh.jwt"}"#))
            .mount(&edge)
            .await;

        let client = reqwest::Client::new();
        let token = renew_once(&client, &edge.uri(), "tbf_renew_secret")
            .await
            .expect("a live credential renews");
        assert_eq!(token, "fresh.jwt");
    }

    /// A refusal must carry the reason the edge relayed from auth — the whole
    /// point of the passthrough is that the operator reads auth's own words.
    #[tokio::test]
    async fn a_refusal_surfaces_the_upstream_reason() {
        let edge = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/mesh/renew-join-token"))
            .respond_with(
                ResponseTemplate::new(401).set_body_string(r#"{"error":"credential revoked"}"#),
            )
            .mount(&edge)
            .await;

        let client = reqwest::Client::new();
        let error = renew_once(&client, &edge.uri(), "tbf_renew_dead")
            .await
            .expect_err("a revoked credential must fail");
        let rendered = error.to_string();
        assert!(rendered.contains("401"), "{rendered}");
        assert!(rendered.contains("credential revoked"), "{rendered}");
    }

    /// A body that is 200 but unusable is a failure, not a silently empty token
    /// handed to the joiner.
    #[tokio::test]
    async fn a_success_with_no_token_is_an_error() {
        let edge = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/mesh/renew-join-token"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"ok":true}"#))
            .mount(&edge)
            .await;

        let client = reqwest::Client::new();
        assert!(renew_once(&client, &edge.uri(), "tbf_renew_x").await.is_err());
    }
}
