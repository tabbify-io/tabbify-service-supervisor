//! Bearer token-exchange tests for [`super`] — the OCI puller's auth path.
//!
//! An injectable transport simulates the in-mesh registry's standard OCI Bearer
//! flow: resource GETs without a bearer answer `401` + a `WWW-Authenticate:
//! Bearer …` challenge; the token endpoint mints a JWT for the `Basic`
//! credential; resource GETs WITH the bearer serve the bytes. This exercises the
//! full exchange + cache + resume interplay with no real socket.
#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{ByteStream, GetResponse, HttpGet, pull_image, sha256_hex};
use crate::runtime::BoxFut;

/// The verbatim oras-config `Basic` credential (base64 of a runner join-token).
const BASIC: &str = "eDpKV1RUT0tFTg==";
/// The JWT the token endpoint mints for that credential.
const BEARER: &str = "registry-bearer-jwt-abc123";
/// The token-exchange endpoint advertised by the challenge's `realm`.
const REALM: &str = "http://reg:5000/auth/token";
/// The `service` the challenge advertises.
const SERVICE: &str = "tabbify-registry";
/// The scope the challenge advertises (when it carries one).
const SCOPE: &str = "repository:acme/app:pull";

fn basic_header() -> String {
    format!("Basic {BASIC}")
}
fn bearer_header() -> String {
    format!("Bearer {BEARER}")
}

// ── Injectable fake transport (records the Authorization header too) ─────────

/// A recorded request: enough to assert the auth handshake.
#[derive(Clone)]
struct AuthReq {
    url: String,
    range_from: Option<u64>,
    authorization: Option<String>,
}

/// A scripted response.
struct FakeResp {
    status: u16,
    www_authenticate: Option<String>,
    chunks: Vec<Vec<u8>>,
    break_after: bool,
}

/// A [`ByteStream`] over scripted chunks that optionally breaks mid-stream.
struct FakeBody {
    chunks: VecDeque<Vec<u8>>,
    break_after: bool,
}

impl ByteStream for FakeBody {
    fn next_chunk(&mut self) -> BoxFut<'_, Result<Option<Bytes>, String>> {
        Box::pin(async move {
            if let Some(c) = self.chunks.pop_front() {
                Ok(Some(Bytes::from(c)))
            } else if self.break_after {
                self.break_after = false;
                Err("simulated relay rekey break".to_owned())
            } else {
                Ok(None)
            }
        })
    }
}

type Handler = Box<dyn Fn(&AuthReq) -> FakeResp + Send + Sync>;

struct AuthFakeTransport {
    handler: Handler,
    calls: Arc<Mutex<Vec<AuthReq>>>,
}

impl HttpGet for AuthFakeTransport {
    fn get<'a>(
        &'a self,
        url: &'a str,
        range_from: Option<u64>,
        _accept: Option<&'a str>,
        authorization: Option<&'a str>,
    ) -> BoxFut<'a, Result<GetResponse, String>> {
        let req = AuthReq {
            url: url.to_owned(),
            range_from,
            authorization: authorization.map(str::to_owned),
        };
        self.calls.lock().unwrap().push(req.clone());
        let resp = (self.handler)(&req);
        Box::pin(async move {
            Ok(GetResponse {
                status: resp.status,
                www_authenticate: resp.www_authenticate,
                body: Box::new(FakeBody {
                    chunks: resp.chunks.into(),
                    break_after: resp.break_after,
                }),
            })
        })
    }
}

// ── Response + challenge helpers ─────────────────────────────────────────────

fn full(status: u16, body: &[u8]) -> FakeResp {
    FakeResp {
        status,
        www_authenticate: None,
        chunks: vec![body.to_vec()],
        break_after: false,
    }
}

/// A `401` carrying a `Bearer` challenge (with or without a `scope` param).
fn unauthorized(scope: Option<&str>) -> FakeResp {
    let header = match scope {
        Some(s) => format!("Bearer realm=\"{REALM}\",service=\"{SERVICE}\",scope=\"{s}\""),
        None => format!("Bearer realm=\"{REALM}\",service=\"{SERVICE}\""),
    };
    FakeResp {
        status: 401,
        www_authenticate: Some(header),
        chunks: vec![],
        break_after: false,
    }
}

fn token_json() -> Vec<u8> {
    format!("{{\"token\":\"{BEARER}\"}}").into_bytes()
}

fn is_basic(req: &AuthReq) -> bool {
    req.authorization.as_deref() == Some(basic_header().as_str())
}
fn is_bearer(req: &AuthReq) -> bool {
    req.authorization.as_deref() == Some(bearer_header().as_str())
}

/// Build a minimal single-platform image: an (opaque) config blob + the given
/// layers. Returns `(manifest, manifest_hex, config, config_hex, layer_hexes)`.
#[allow(clippy::type_complexity)]
fn build_image(layers: &[&[u8]]) -> (Vec<u8>, String, Vec<u8>, String, Vec<String>) {
    let config = br#"{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}"#
        .to_vec();
    let config_hex = sha256_hex(&config);
    let layer_hexes: Vec<String> = layers.iter().map(|l| sha256_hex(l)).collect();
    let layer_descs: Vec<serde_json::Value> = layers
        .iter()
        .zip(&layer_hexes)
        .map(|(l, h)| {
            serde_json::json!({
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": format!("sha256:{h}"),
                "size": l.len(),
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{config_hex}"),
            "size": config.len(),
        },
        "layers": layer_descs,
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_hex = sha256_hex(&manifest_bytes);
    (manifest_bytes, manifest_hex, config, config_hex, layer_hexes)
}

/// Serve a resource (manifest/config/layer) BY the bearer, else the `401`
/// challenge; mint a token at the endpoint for the Basic cred. Shared by the
/// tests that don't need a mid-stream break.
fn serve_resources(
    req: &AuthReq,
    resources: &[(String, Vec<u8>)],
    scope: Option<&str>,
) -> FakeResp {
    if req.url.contains("/auth/token") {
        return if is_basic(req) {
            full(200, &token_json())
        } else {
            unauthorized(scope)
        };
    }
    if !is_bearer(req) {
        return unauthorized(scope);
    }
    for (key, bytes) in resources {
        if req.url.contains(key) {
            return full(200, bytes);
        }
    }
    full(404, b"")
}

fn resources_of(
    manifest_hex: &str,
    manifest: &[u8],
    config_hex: &str,
    config: &[u8],
    layers: &[(&str, &[u8])],
) -> Vec<(String, Vec<u8>)> {
    let mut out = vec![
        (format!("/manifests/sha256:{manifest_hex}"), manifest.to_vec()),
        (format!("/blobs/sha256:{config_hex}"), config.to_vec()),
    ];
    for (hex, bytes) in layers {
        out.push((format!("/blobs/sha256:{hex}"), bytes.to_vec()));
    }
    out
}

// ── (1) 401 challenge → token exchange → bearer retry → success ──────────────

#[tokio::test]
async fn bearer_challenge_triggers_exchange_and_bearer_retry() {
    let layer = b"the-layer-bytes".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&layer]);
    let layer_hex = layer_hexes[0].clone();

    let calls: Arc<Mutex<Vec<AuthReq>>> = Arc::new(Mutex::new(Vec::new()));
    let resources = resources_of(
        &manifest_hex,
        &manifest,
        &config_hex,
        &config,
        &[(&layer_hex, &layer)],
    );
    let handler = move |req: &AuthReq| serve_resources(req, &resources, Some(SCOPE));
    let transport = AuthFakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, Some(BASIC), &transport)
        .await
        .expect("pull must converge via the bearer token-exchange");

    let recorded = calls.lock().unwrap().clone();
    // Exactly one token exchange, carrying the Basic cred + realm/service/scope.
    let token_calls: Vec<&AuthReq> = recorded
        .iter()
        .filter(|r| r.url.starts_with(REALM))
        .collect();
    assert_eq!(token_calls.len(), 1, "exactly one token exchange");
    let tok = token_calls[0];
    assert_eq!(
        tok.authorization.as_deref(),
        Some(basic_header().as_str()),
        "the token GET sends the Basic credential"
    );
    assert!(
        tok.url.contains(&format!("service={SERVICE}")),
        "service query present: {}",
        tok.url
    );
    assert!(
        tok.url.contains("scope=repository%3Aacme%2Fapp%3Apull"),
        "scope query is percent-encoded: {}",
        tok.url
    );
    // The manifest was tried with Basic (→401) then RETRIED with Bearer (→200).
    let man_key = format!("/manifests/sha256:{manifest_hex}");
    let man_auths: Vec<Option<String>> = recorded
        .iter()
        .filter(|r| r.url.contains(&man_key))
        .map(|r| r.authorization.clone())
        .collect();
    assert_eq!(
        man_auths,
        vec![Some(basic_header()), Some(bearer_header())],
        "manifest: Basic first, then the Bearer retry"
    );
    // The layout assembled correctly.
    let blob = layout.join("blobs").join("sha256").join(&layer_hex);
    assert_eq!(std::fs::read(&blob).unwrap(), layer);
}

// ── (2) the bearer is cached: N requests → ONE exchange ──────────────────────

#[tokio::test]
async fn bearer_is_cached_one_exchange_for_many_blobs() {
    let l1 = b"layer-one".to_vec();
    let l2 = b"layer-two-xx".to_vec();
    let l3 = b"layer-three-yyy".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&l1, &l2, &l3]);

    let calls: Arc<Mutex<Vec<AuthReq>>> = Arc::new(Mutex::new(Vec::new()));
    let resources = resources_of(
        &manifest_hex,
        &manifest,
        &config_hex,
        &config,
        &[
            (&layer_hexes[0], &l1),
            (&layer_hexes[1], &l2),
            (&layer_hexes[2], &l3),
        ],
    );
    let handler = move |req: &AuthReq| serve_resources(req, &resources, Some(SCOPE));
    let transport = AuthFakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, Some(BASIC), &transport).await.unwrap();

    let recorded = calls.lock().unwrap().clone();
    let exchanges = recorded.iter().filter(|r| r.url.starts_with(REALM)).count();
    assert_eq!(
        exchanges, 1,
        "the cached bearer is reused: N blob requests cause exactly ONE token exchange"
    );
    // Sanity: many resource GETs happened (manifest twice + config + 3 layers).
    let resource_gets = recorded.iter().filter(|r| r.url.contains("/v2/")).count();
    assert!(
        resource_gets >= 6,
        "expected ≥6 resource GETs (manifest basic+bearer, config, 3 layers), got {resource_gets}"
    );
    // Every subsequent request after the first exchange used the bearer proactively.
    let bearer_gets = recorded.iter().filter(|r| is_bearer(r)).count();
    assert!(bearer_gets >= 5, "config + 3 layers + manifest-retry are bearer'd");
}

// ── (3) resume/Range interoperates with bearer auth ──────────────────────────

#[tokio::test(start_paused = true)]
async fn resume_after_break_carries_range_and_bearer() {
    let layer = b"0123456789".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&layer]);
    let layer_hex = layer_hexes[0].clone();

    let calls: Arc<Mutex<Vec<AuthReq>>> = Arc::new(Mutex::new(Vec::new()));
    let man_key = format!("/manifests/sha256:{manifest_hex}");
    let cfg_key = format!("/blobs/sha256:{config_hex}");
    let layer_key = format!("/blobs/sha256:{layer_hex}");
    let layer_key_h = layer_key.clone();
    let (man_c, cfg_c, layer_c) = (manifest.clone(), config.clone(), layer.clone());
    let handler = move |req: &AuthReq| -> FakeResp {
        if req.url.contains("/auth/token") {
            return if is_basic(req) {
                full(200, &token_json())
            } else {
                unauthorized(Some(SCOPE))
            };
        }
        if !is_bearer(req) {
            return unauthorized(Some(SCOPE));
        }
        if req.url.contains(&man_key) {
            full(200, &man_c)
        } else if req.url.contains(&cfg_key) {
            full(200, &cfg_c)
        } else if req.url.contains(&layer_key_h) {
            match req.range_from {
                // First bearer'd attempt: a prefix, then a mid-stream break.
                None => FakeResp {
                    status: 200,
                    www_authenticate: None,
                    chunks: vec![layer_c[..4].to_vec()],
                    break_after: true,
                },
                // Resume: partial content from the requested offset.
                Some(n) => full(206, &layer_c[n as usize..]),
            }
        } else {
            full(404, b"")
        }
    };
    let transport = AuthFakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, Some(BASIC), &transport)
        .await
        .expect("resume + bearer must converge across the mid-stream break");

    let recorded = calls.lock().unwrap().clone();
    let layer_calls: Vec<&AuthReq> = recorded
        .iter()
        .filter(|r| r.url.contains(&layer_key))
        .collect();
    assert_eq!(layer_calls.len(), 2, "layer fetched twice: break then resume");
    assert_eq!(layer_calls[0].range_from, None, "first GET has no Range");
    assert!(is_bearer(layer_calls[0]), "first layer GET is bearer'd");
    assert_eq!(
        layer_calls[1].range_from,
        Some(4),
        "the resume GET starts at the current offset (4)"
    );
    assert!(
        is_bearer(layer_calls[1]),
        "the ranged resume request ALSO carries the bearer"
    );
    // Still exactly one exchange despite the break (cached bearer reused on resume).
    assert_eq!(recorded.iter().filter(|r| r.url.starts_with(REALM)).count(), 1);
    let blob = layout.join("blobs").join("sha256").join(&layer_hex);
    assert_eq!(std::fs::read(&blob).unwrap(), layer, "resume reassembled the full blob");
}

// ── (4) a missing scope in the challenge is handled ──────────────────────────

#[tokio::test]
async fn missing_scope_challenge_is_handled() {
    let layer = b"scopeless-layer".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&layer]);
    let layer_hex = layer_hexes[0].clone();

    let calls: Arc<Mutex<Vec<AuthReq>>> = Arc::new(Mutex::new(Vec::new()));
    let resources = resources_of(
        &manifest_hex,
        &manifest,
        &config_hex,
        &config,
        &[(&layer_hex, &layer)],
    );
    // The challenge omits scope (as the prod gate did — it derives scope itself).
    let handler = move |req: &AuthReq| serve_resources(req, &resources, None);
    let transport = AuthFakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, Some(BASIC), &transport)
        .await
        .expect("a scope-less challenge still converges");

    let recorded = calls.lock().unwrap().clone();
    let tok = recorded
        .iter()
        .find(|r| r.url.starts_with(REALM))
        .expect("a token exchange happened");
    assert!(
        !tok.url.contains("scope="),
        "the token GET omits scope when the challenge carried none: {}",
        tok.url
    );
    assert!(
        tok.url.contains(&format!("service={SERVICE}")),
        "service is still sent: {}",
        tok.url
    );
    assert!(layout.join("oci-layout").is_file(), "the layout was produced");
}
