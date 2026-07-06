//! Tests for [`super`] — the resumable OCI puller.
//!
//! The HTTP transport is INJECTED (a fake that can break mid-body and honour a
//! `Range` retry), so the resume + verification logic is exercised
//! deterministically with no real socket.
#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{
    ByteStream, GetResponse, HttpGet, ParsedRef, host_arch, parse_ref, pull_image, read_basic_auth,
    sha256_hex,
};
use crate::runtime::BoxFut;

// ── Injectable fake transport ───────────────────────────────────────────────

/// A scripted response: status, the chunks to yield, and whether to BREAK
/// (return a mid-stream error) after the chunks instead of ending cleanly.
struct FakeResp {
    status: u16,
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

/// A recorded (url, range_from) log, shared with the test body.
type Calls = Arc<Mutex<Vec<(String, Option<u64>)>>>;
/// The per-test response handler.
type Handler = Box<dyn Fn(&str, Option<u64>) -> FakeResp + Send + Sync>;

/// A fake [`HttpGet`] that records every (url, range_from) and answers via a
/// user-supplied handler.
struct FakeTransport {
    handler: Handler,
    calls: Calls,
}

impl HttpGet for FakeTransport {
    fn get<'a>(
        &'a self,
        url: &'a str,
        range_from: Option<u64>,
        _accept: Option<&'a str>,
        _auth: Option<&'a str>,
    ) -> BoxFut<'a, Result<GetResponse, String>> {
        self.calls.lock().unwrap().push((url.to_owned(), range_from));
        let resp = (self.handler)(url, range_from);
        Box::pin(async move {
            Ok(GetResponse {
                status: resp.status,
                www_authenticate: None,
                body: Box::new(FakeBody {
                    chunks: resp.chunks.into(),
                    break_after: resp.break_after,
                }),
            })
        })
    }
}

/// Build a minimal single-platform image: an (opaque) config blob + the given
/// layers. Returns `(manifest_bytes, manifest_hex, config_bytes, config_hex,
/// layer_hexes)`.
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

fn full(status: u16, body: &[u8]) -> FakeResp {
    FakeResp {
        status,
        chunks: vec![body.to_vec()],
        break_after: false,
    }
}

// ── (1) resume appends with a Range from the current offset ─────────────────

/// A layer whose first GET yields a 4-byte prefix then BREAKS mid-stream must be
/// resumed with a `Range: bytes=4-` header on the retry, APPENDING the remainder
/// so no progress is lost — exactly the `curl -C -` behaviour the fix relies on.
#[tokio::test(start_paused = true)]
async fn resume_appends_with_range_after_midstream_break() {
    let layer = b"0123456789".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&layer]);
    let layer_hex = layer_hexes[0].clone();

    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let man_key = format!("/manifests/sha256:{manifest_hex}");
    let cfg_key = format!("/blobs/sha256:{config_hex}");
    let layer_key = format!("/blobs/sha256:{layer_hex}");
    let (man_c, cfg_c, layer_c) = (manifest.clone(), config.clone(), layer.clone());
    let handler = move |url: &str, range: Option<u64>| -> FakeResp {
        if url.contains(&man_key) {
            full(200, &man_c)
        } else if url.contains(&cfg_key) {
            full(200, &cfg_c)
        } else if url.contains(&layer_key) {
            match range {
                // First attempt: prefix, then a mid-stream break.
                None => FakeResp {
                    status: 200,
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
    let transport = FakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, None, &transport)
        .await
        .expect("pull must converge across the mid-stream break");

    // The layer was fetched twice: first without a Range, then resuming at offset 4.
    let recorded = calls.lock().unwrap().clone();
    let layer_gets: Vec<Option<u64>> = recorded
        .iter()
        .filter(|(u, _)| u.contains(&format!("/blobs/sha256:{layer_hex}")))
        .map(|(_, r)| *r)
        .collect();
    assert_eq!(
        layer_gets,
        vec![None, Some(4)],
        "first GET has no Range; the resume GET starts at the current offset (4)"
    );
    // The assembled layer blob is complete and byte-correct.
    let blob = layout.join("blobs").join("sha256").join(&layer_hex);
    assert_eq!(std::fs::read(&blob).unwrap(), layer, "resume reassembled the full blob");
}

// ── (2) sha256 mismatch triggers a full re-pull ─────────────────────────────

/// A layer served with the WRONG bytes on the first (clean) pull must be
/// discarded on the sha256 mismatch and RE-PULLED from zero; the second pull
/// serves the correct bytes and the pull succeeds.
#[tokio::test]
async fn sha256_mismatch_triggers_full_repull() {
    let layer = b"correct-layer-bytes".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&layer]);
    let layer_hex = layer_hexes[0].clone();

    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let layer_pulls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let layer_pulls_c = layer_pulls.clone();
    let man_key = format!("/manifests/sha256:{manifest_hex}");
    let cfg_key = format!("/blobs/sha256:{config_hex}");
    let layer_key = format!("/blobs/sha256:{layer_hex}");
    let (man_c, cfg_c, layer_c) = (manifest.clone(), config.clone(), layer.clone());
    let handler = move |url: &str, _range: Option<u64>| -> FakeResp {
        if url.contains(&man_key) {
            full(200, &man_c)
        } else if url.contains(&cfg_key) {
            full(200, &cfg_c)
        } else if url.contains(&layer_key) {
            let n = layer_pulls_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n == 1 {
                // Corrupt bytes: assemble cleanly but fail sha256 verification.
                full(200, b"totally-wrong-bytes")
            } else {
                full(200, &layer_c)
            }
        } else {
            full(404, b"")
        }
    };
    let transport = FakeTransport {
        handler: Box::new(handler),
        calls: calls.clone(),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, None, &transport)
        .await
        .expect("pull recovers after discarding the corrupt blob and re-pulling");

    assert_eq!(
        layer_pulls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "corrupt blob → discard → re-pull from zero (two full layer pulls)"
    );
    let blob = layout.join("blobs").join("sha256").join(&layer_hex);
    assert_eq!(std::fs::read(&blob).unwrap(), layer, "the verified blob is the correct one");
}

// ── (3) the produced layout is spec-compliant with all blobs ────────────────

/// A clean pull produces `oci-layout` + `index.json` + a `blobs/sha256/<hex>`
/// for the manifest, config, and every layer, and `index.json` points at the
/// manifest blob with an image-manifest media type — the exact contract
/// `firecracker.rs` reads.
#[tokio::test]
async fn produces_spec_compliant_layout_with_all_blobs() {
    let l1 = b"layer-one-bytes".to_vec();
    let l2 = b"layer-two-different".to_vec();
    let (manifest, manifest_hex, config, config_hex, layer_hexes) = build_image(&[&l1, &l2]);

    let man_key = format!("/manifests/sha256:{manifest_hex}");
    let cfg_key = format!("/blobs/sha256:{config_hex}");
    let (h1, h2) = (layer_hexes[0].clone(), layer_hexes[1].clone());
    let (man_c, cfg_c, l1_c, l2_c) = (manifest.clone(), config.clone(), l1.clone(), l2.clone());
    let (k1, k2) = (format!("/blobs/sha256:{h1}"), format!("/blobs/sha256:{h2}"));
    let handler = move |url: &str, _range: Option<u64>| -> FakeResp {
        if url.contains(&man_key) {
            full(200, &man_c)
        } else if url.contains(&cfg_key) {
            full(200, &cfg_c)
        } else if url.contains(&k1) {
            full(200, &l1_c)
        } else if url.contains(&k2) {
            full(200, &l2_c)
        } else {
            full(404, b"")
        }
    };
    let transport = FakeTransport {
        handler: Box::new(handler),
        calls: Arc::new(Mutex::new(Vec::new())),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{manifest_hex}");
    pull_image(&reff, &layout, None, &transport).await.unwrap();

    // oci-layout marker.
    let marker = std::fs::read_to_string(layout.join("oci-layout")).unwrap();
    assert!(marker.contains("\"imageLayoutVersion\":\"1.0.0\""), "got {marker}");

    // Every blob is present (manifest + config + both layers).
    let blobs = layout.join("blobs").join("sha256");
    for hex in [&manifest_hex, &config_hex, &layer_hexes[0], &layer_hexes[1]] {
        assert!(blobs.join(hex).is_file(), "blob {hex} must be present");
    }

    // index.json points at the manifest blob with an image-manifest media type.
    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(layout.join("index.json")).unwrap()).unwrap();
    let desc = &index["manifests"][0];
    assert_eq!(desc["digest"], format!("sha256:{manifest_hex}"));
    assert_eq!(desc["mediaType"], "application/vnd.oci.image.manifest.v1+json");
    assert_eq!(desc["size"], manifest.len());
}

// ── multi-arch index descent ────────────────────────────────────────────────

/// A multi-arch image INDEX is descended to the host-arch child manifest, so
/// `index.json` still ends up with a single image-manifest descriptor.
#[tokio::test]
async fn descends_multiarch_index_to_host_arch_child() {
    let layer = b"host-arch-layer".to_vec();
    let (manifest, manifest_hex, config, config_hex, _) = build_image(&[&layer]);
    let layer_hex = sha256_hex(&layer);

    // An index that offers the host arch plus a decoy foreign arch.
    let index_doc = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
             "size": 1, "platform": {"architecture": "s390x", "os": "linux"}},
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "digest": format!("sha256:{manifest_hex}"),
             "size": manifest.len(), "platform": {"architecture": host_arch(), "os": "linux"}},
        ],
    });
    let index_bytes = serde_json::to_vec(&index_doc).unwrap();
    let index_hex = sha256_hex(&index_bytes);

    let man_by_index = format!("/manifests/sha256:{index_hex}");
    let man_by_child = format!("/manifests/sha256:{manifest_hex}");
    let cfg_key = format!("/blobs/sha256:{config_hex}");
    let layer_key = format!("/blobs/sha256:{layer_hex}");
    let (idx_c, man_c, cfg_c, layer_c) =
        (index_bytes.clone(), manifest.clone(), config.clone(), layer.clone());
    let handler = move |url: &str, _range: Option<u64>| -> FakeResp {
        if url.contains(&man_by_index) {
            full(200, &idx_c)
        } else if url.contains(&man_by_child) {
            full(200, &man_c)
        } else if url.contains(&cfg_key) {
            full(200, &cfg_c)
        } else if url.contains(&layer_key) {
            full(200, &layer_c)
        } else {
            full(404, b"")
        }
    };
    let transport = FakeTransport {
        handler: Box::new(handler),
        calls: Arc::new(Mutex::new(Vec::new())),
    };

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    let reff = format!("reg:5000/acme/app@sha256:{index_hex}");
    pull_image(&reff, &layout, None, &transport).await.unwrap();

    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(layout.join("index.json")).unwrap()).unwrap();
    assert_eq!(
        index["manifests"][0]["digest"],
        format!("sha256:{manifest_hex}"),
        "index.json must point at the HOST-arch child manifest, not the index"
    );
    assert!(
        layout.join("blobs").join("sha256").join(&layer_hex).is_file(),
        "the host-arch child's layer must be pulled"
    );
}

// ── ref parsing ─────────────────────────────────────────────────────────────

#[test]
fn parse_ref_digest_form() {
    assert_eq!(
        parse_ref("[fd5a:1f00:0:3::1]:5000/platform/app@sha256:abcd").unwrap(),
        ParsedRef {
            host: "[fd5a:1f00:0:3::1]:5000".to_owned(),
            repo: "platform/app".to_owned(),
            reference: "sha256:abcd".to_owned(),
            is_digest: true,
        }
    );
}

#[test]
fn parse_ref_tag_form_host_port_not_a_tag() {
    // The host's `:5000` must NOT be mistaken for a tag boundary.
    assert_eq!(
        parse_ref("reg:5000/acme/app:v1").unwrap(),
        ParsedRef {
            host: "reg:5000".to_owned(),
            repo: "acme/app".to_owned(),
            reference: "v1".to_owned(),
            is_digest: false,
        }
    );
}

#[test]
fn parse_ref_no_tag_defaults_to_latest() {
    assert_eq!(
        parse_ref("reg:5000/acme/app").unwrap(),
        ParsedRef {
            host: "reg:5000".to_owned(),
            repo: "acme/app".to_owned(),
            reference: "latest".to_owned(),
            is_digest: false,
        }
    );
}

// ── auth from the oras registry-config ──────────────────────────────────────

#[test]
fn read_basic_auth_reads_host_entry_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("config.json");
    std::fs::write(
        &cfg,
        r#"{"auths":{"[fd5a::1]:5000":{"auth":"eDpKV1RUT0tFTg=="}}}"#,
    )
    .unwrap();
    let got = read_basic_auth(Some(cfg.to_str().unwrap()), "[fd5a::1]:5000");
    assert_eq!(got.as_deref(), Some("eDpKV1RUT0tFTg=="));
}

#[test]
fn read_basic_auth_none_when_no_config() {
    assert_eq!(read_basic_auth(None, "[fd5a::1]:5000"), None);
}
