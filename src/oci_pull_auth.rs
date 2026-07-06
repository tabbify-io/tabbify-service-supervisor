//! OCI Bearer token-exchange auth for the resumable puller.
//!
//! ## Почему это существует (причина важнее следствия)
//! Resumable-пуллер (`oci_pull_transport.rs`) отправлял ТОЛЬКО `Basic` auth.
//! Но in-mesh registry (zot-подобный gate) на tenant-образах (`n_<net>/<uuid>`)
//! отвечает СТАНДАРТНЫМ OCI Bearer token-exchange: `401` + заголовок
//! `WWW-Authenticate: Bearer realm=…,service=…[,scope=…]`. Клиент обязан пойти
//! на `realm` с Basic-кредом (это runner join-token из oras-конфига), получить
//! `{"token":"<jwt>"}` и повторить исходный запрос с `Authorization: Bearer`.
//! `platform/*` gate отдаёт анониму/любому токену → там Basic/anon хватало,
//! поэтому регрессию словили только на tenant-образах (deploy падал на 401).
//! Старый `oras copy` делал этот обмен сам; переписанный resumable-пуллер — нет.
//!
//! Здесь — разбор челленджа, обмен токена и кэш bearer'а. Транспорт (голый GET,
//! который лишь прикрепляет вербатим `Authorization`) и resume-движок — в
//! `oci_pull_transport.rs`; OCI-оркестрация — в `oci_pull.rs`.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use super::transport::{ByteStream, GetResponse, HttpGet};

/// Upper bound on a token-exchange response body. A token JSON is tiny; this
/// guards against a misbehaving endpoint streaming an unbounded body into RAM.
const MAX_TOKEN_BODY: usize = 64 * 1024;

// ── WWW-Authenticate challenge ───────────────────────────────────────────────

/// A parsed `Bearer` challenge from a `WWW-Authenticate` header: the token
/// endpoint (`realm`), the `service`, and an OPTIONAL `scope` (the prod gate
/// omitted it and derived scope from the token — both shapes are handled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

/// The bearer-cache key: identical (realm, service, scope) triples share a
/// token. In a single-repo pull this is one triple, so the cache holds one live
/// token — but keying keeps it correct if a registry ever varies scope per
/// resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChallengeKey {
    realm: String,
    service: String,
    scope: Option<String>,
}

impl BearerChallenge {
    fn key(&self) -> ChallengeKey {
        ChallengeKey {
            realm: self.realm.clone(),
            service: self.service.clone().unwrap_or_default(),
            scope: self.scope.clone(),
        }
    }

    /// The token GET URL: `<realm>?service=<enc>&scope=<enc>`, omitting `scope`
    /// when the challenge carried none. Query values are percent-encoded so a
    /// `scope` like `repository:foo/bar:pull` survives intact.
    fn token_url(&self) -> String {
        let mut params: Vec<String> = Vec::new();
        if let Some(service) = &self.service {
            params.push(format!("service={}", percent_encode_query(service)));
        }
        if let Some(scope) = &self.scope {
            params.push(format!("scope={}", percent_encode_query(scope)));
        }
        if params.is_empty() {
            return self.realm.clone();
        }
        let sep = if self.realm.contains('?') { '&' } else { '?' };
        format!("{}{sep}{}", self.realm, params.join("&"))
    }
}

/// Parse a `WWW-Authenticate` header value into a [`BearerChallenge`], or `None`
/// when it is not a `Bearer` challenge (e.g. `Basic` — the puller then surfaces
/// the 401 rather than attempting an exchange). Robust to param order, optional
/// quoting, and a missing `scope`; `realm` is required.
pub(crate) fn parse_bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let trimmed = header.trim_start();
    // Scheme token is case-insensitive per RFC 7235.
    let rest = trimmed
        .get(..6)
        .filter(|s| s.eq_ignore_ascii_case("Bearer"))
        .map(|_| &trimmed[6..])?;
    let params = parse_auth_params(rest);
    let find = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    let realm = find("realm").filter(|s| !s.is_empty())?;
    Some(BearerChallenge {
        realm,
        service: find("service").filter(|s| !s.is_empty()),
        scope: find("scope").filter(|s| !s.is_empty()),
    })
}

/// Tokenise `key=value` auth params (comma/space separated), honouring quoted
/// values (which may themselves contain commas — e.g. multi-scope) and simple
/// backslash escapes inside quotes. Auth headers are ASCII, but this walks
/// `char`s so a stray multibyte byte can never split a boundary mid-codepoint.
fn parse_auth_params(s: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        // Skip inter-param separators.
        while i < n && (chars[i] == ' ' || chars[i] == ',' || chars[i] == '\t') {
            i += 1;
        }
        // Key up to '=' (or a bare separator → malformed, stop).
        let mut key = String::new();
        while i < n && chars[i] != '=' && chars[i] != ',' {
            key.push(chars[i]);
            i += 1;
        }
        if i >= n || chars[i] != '=' {
            break;
        }
        i += 1; // consume '='
        // Value: quoted (verbatim until the closing quote) or bare (until ',').
        let mut value = String::new();
        if i < n && chars[i] == '"' {
            i += 1;
            while i < n && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < n {
                    i += 1; // take the escaped char literally
                }
                value.push(chars[i]);
                i += 1;
            }
            if i < n {
                i += 1; // consume closing quote
            }
        } else {
            while i < n && chars[i] != ',' {
                value.push(chars[i]);
                i += 1;
            }
        }
        let key = key.trim().to_owned();
        if !key.is_empty() {
            out.push((key, value.trim().to_owned()));
        }
    }
    out
}

/// Percent-encode a query value, keeping only the RFC 3986 unreserved set. Used
/// for the `service`/`scope` query params of the token GET.
fn percent_encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b));
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ── Bearer cache ─────────────────────────────────────────────────────────────

/// A per-pull cache of exchanged bearer tokens. Shared across the manifest,
/// config, every layer, AND every ranged resume so a pull does exactly ONE
/// exchange (not one per request); on bearer expiry mid-blob a single
/// re-exchange refreshes it. `newest` is the token tried PROACTIVELY on the next
/// request, so after the first exchange no further 401 round-trips occur.
#[derive(Default)]
pub(crate) struct BearerCache {
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    by_key: HashMap<ChallengeKey, String>,
    newest: Option<String>,
}

impl BearerCache {
    /// An empty cache for one `pull_image` invocation.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn newest(&self) -> Option<String> {
        self.lock().newest.clone()
    }

    fn get(&self, key: &ChallengeKey) -> Option<String> {
        self.lock().by_key.get(key).cloned()
    }

    fn store(&self, key: ChallengeKey, token: String) {
        let mut inner = self.lock();
        inner.by_key.insert(key, token.clone());
        inner.newest = Some(token);
    }

    /// Lock, recovering the guard on poison — the critical sections never panic,
    /// so poison cannot corrupt invariants; this just avoids an unwrap.
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// ── Authenticated GET ────────────────────────────────────────────────────────

/// Issue `GET url` with OCI Bearer token-exchange layered over the raw
/// transport, reusing the per-pull [`BearerCache`].
///
/// Flow: send with the cached bearer (proactive) or the `Basic` credential; on a
/// `401` carrying a `Bearer` challenge, resolve a token — a cached one for this
/// challenge key, else ONE exchange against the `realm` with the `Basic`
/// credential — then retry ONCE with `Authorization: Bearer <jwt>`. A `401` that
/// is not a Bearer challenge (or a still-`401` bearer'd retry) is returned as-is
/// for the caller to surface: at most one exchange + one retry per call, so a bad
/// credential can never spin an unbounded auth loop.
///
/// `range_from`/`accept` are forwarded verbatim, so the bearer is attached to
/// every ranged resume continuation exactly like the initial request.
pub(crate) async fn get_authed(
    transport: &dyn HttpGet,
    cache: &BearerCache,
    basic: Option<&str>,
    url: &str,
    range_from: Option<u64>,
    accept: Option<&str>,
) -> Result<GetResponse, String> {
    // 1. Send proactively with the cached bearer, else the Basic credential.
    let proactive = cache.newest();
    let first_header = match &proactive {
        Some(token) => Some(format!("Bearer {token}")),
        None => basic.map(|b| format!("Basic {b}")),
    };
    let resp = transport
        .get(url, range_from, accept, first_header.as_deref())
        .await?;
    if resp.status != 401 {
        return Ok(resp);
    }
    // 2. A 401 WITHOUT a Bearer challenge → nothing to exchange; surface it.
    let Some(challenge) = resp
        .www_authenticate
        .as_deref()
        .and_then(parse_bearer_challenge)
    else {
        return Ok(resp);
    };
    // 3. Resolve a bearer: a cached one that ISN'T the (expired) token we just
    //    tried, otherwise exchange exactly once and cache it.
    let key = challenge.key();
    let token = match cache.get(&key) {
        Some(cached) if Some(cached.as_str()) != proactive.as_deref() => cached,
        _ => {
            let fresh = exchange_token(transport, &challenge, basic).await?;
            cache.store(key, fresh.clone());
            fresh
        }
    };
    // 4. Retry ONCE with the bearer. If it still 401s, return it — the caller
    //    surfaces the error rather than looping.
    let bearer_header = format!("Bearer {token}");
    transport
        .get(url, range_from, accept, Some(&bearer_header))
        .await
}

/// Exchange the `Basic` credential for a bearer at the challenge's `realm`.
/// Accepts `{"token":…}` or the `{"access_token":…}` alias.
async fn exchange_token(
    transport: &dyn HttpGet,
    challenge: &BearerChallenge,
    basic: Option<&str>,
) -> Result<String, String> {
    let url = challenge.token_url();
    let header = basic.map(|b| format!("Basic {b}"));
    let resp = transport
        .get(&url, None, Some("application/json"), header.as_deref())
        .await?;
    if !(200..300).contains(&resp.status) {
        return Err(format!(
            "OCI token exchange GET {url} returned status {}",
            resp.status
        ));
    }
    let body = read_body_to_vec(resp.body).await?;
    let doc: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| format!("OCI token exchange response from {url} is not JSON: {e}"))?;
    doc.get("token")
        .or_else(|| doc.get("access_token"))
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("OCI token exchange response from {url} carries no token"))
}

/// Drain a streaming body fully into memory, bounded by [`MAX_TOKEN_BODY`].
async fn read_body_to_vec(mut body: Box<dyn ByteStream>) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next_chunk().await? {
        if out.len() + chunk.len() > MAX_TOKEN_BODY {
            return Err("OCI token exchange response exceeds the size cap".to_owned());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{BearerChallenge, parse_bearer_challenge, percent_encode_query};

    fn challenge(realm: &str, service: Option<&str>, scope: Option<&str>) -> BearerChallenge {
        BearerChallenge {
            realm: realm.to_owned(),
            service: service.map(str::to_owned),
            scope: scope.map(str::to_owned),
        }
    }

    #[test]
    fn parses_realm_service_scope_in_any_order() {
        let got = parse_bearer_challenge(
            r#"Bearer service="tabbify-registry",scope="repository:n_x/app:pull",realm="http://[fd5a:1f00:0:3::1]:5000/auth/token""#,
        )
        .expect("must parse");
        assert_eq!(
            got,
            challenge(
                "http://[fd5a:1f00:0:3::1]:5000/auth/token",
                Some("tabbify-registry"),
                Some("repository:n_x/app:pull"),
            )
        );
    }

    #[test]
    fn parses_missing_scope() {
        // The prod gate omitted scope and derived it from the token.
        let got = parse_bearer_challenge(
            r#"Bearer realm="http://[fd5a:1f00:0:3::1]:5000/auth/token",service="tabbify-registry""#,
        )
        .expect("must parse");
        assert_eq!(got.scope, None);
        assert!(!got.token_url().contains("scope="), "token URL omits scope");
        assert!(got.token_url().contains("service=tabbify-registry"));
    }

    #[test]
    fn scheme_is_case_insensitive_and_unquoted_values_ok() {
        let got = parse_bearer_challenge(
            "bearer realm=http://reg:5000/token,service=svc,scope=repository:a:pull",
        )
        .expect("must parse");
        assert_eq!(got.realm, "http://reg:5000/token");
        assert_eq!(got.service.as_deref(), Some("svc"));
        assert_eq!(got.scope.as_deref(), Some("repository:a:pull"));
    }

    #[test]
    fn non_bearer_challenge_is_ignored() {
        assert!(parse_bearer_challenge(r#"Basic realm="reg""#).is_none());
        assert!(parse_bearer_challenge("Bearer service=svc").is_none(), "realm is required");
    }

    #[test]
    fn token_url_percent_encodes_scope_colons_and_slashes() {
        let url = challenge("http://reg:5000/token", Some("svc"), Some("repository:n_x/app:pull"))
            .token_url();
        assert_eq!(
            url,
            "http://reg:5000/token?service=svc&scope=repository%3An_x%2Fapp%3Apull"
        );
    }

    #[test]
    fn percent_encode_keeps_unreserved() {
        assert_eq!(percent_encode_query("Aa0-_.~"), "Aa0-_.~");
        assert_eq!(percent_encode_query("a:b/c"), "a%3Ab%2Fc");
    }
}
