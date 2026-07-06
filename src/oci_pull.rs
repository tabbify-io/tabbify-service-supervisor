//! Resumable OCI image puller over plain HTTP for the in-mesh registry.
//!
//! ## Почему это существует (причина важнее следствия)
//! Раньше run-side тянул образ через `oras copy --to-oci-layout`. Над mesh-relay
//! boringtun перегенерирует ключи (~каждые 120с); под потоком данных handshake
//! на rekey таймаутит и TCP-стрим рвётся ПОСЕРЕДИНЕ блоба. `oras copy` НЕ
//! возобновляет — на разорванном блобе он перезапускает этот блоб С НУЛЯ. Так
//! большой слой (наблюдалось: 243 MB в ~435 MB образе), который качается дольше
//! интервала rekey, НИКОГДА не докачивается: рвётся → рестарт → снова рвётся.
//!
//! Здесь — докачка с HTTP `Range`: на разрыве посреди стрима запрос повторяется
//! с `Range: bytes=<текущий-размер-файла>-` и ДОПИСЫВАЕТ, поэтому прогресс не
//! теряется (эмпирически `curl -C -` на том же 243 MB блобе через тот же линк
//! накапливает прогресс через разрывы: 145 MB → выше → готово). Каждый блоб
//! (manifest, config, каждый layer) качается так; итог — тот же spec-compliant
//! OCI LAYOUT (`oci-layout` + `index.json` + `blobs/sha256/<hex>`), который уже
//! потребляет `runner::build::firecracker`.
//!
//! Транспорт + сам resumable-движок (инъектируемый в тестах) — в
//! `oci_pull_transport.rs`; здесь — OCI-специфичная оркестрация.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

#[path = "oci_pull_auth.rs"]
mod auth;
#[path = "oci_pull_transport.rs"]
mod transport;

use auth::BearerCache;
pub use transport::{ByteStream, GetResponse, HttpGet, ReqwestTransport};
use transport::{download_resumable, sha256_hex, verify_file_sha256};

/// The manifest-pull `Accept` header: image manifest + index, both OCI and the
/// Docker v2s2 spellings real images ship with, so the registry serves the
/// document we can parse rather than a v1 fallback.
const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.manifest.v1+json,\
application/vnd.oci.image.index.v1+json,\
application/vnd.docker.distribution.manifest.v2+json,\
application/vnd.docker.distribution.manifest.list.v2+json";

/// The default OCI image-manifest media type, used for the `index.json`
/// descriptor when the manifest carries no `mediaType` of its own.
const OCI_MANIFEST_MEDIA: &str = "application/vnd.oci.image.manifest.v1+json";

/// Re-pull-from-zero attempts when the assembled blob fails sha256 verification
/// (corruption guard). Bounded so a persistently corrupt source can't loop.
const VERIFY_REPULL_ATTEMPTS: u32 = 3;

// ── Ref parsing ─────────────────────────────────────────────────────────────

/// A parsed OCI reference: registry host, repository path, and the manifest
/// reference (a `sha256:<hex>` digest or a tag).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedRef {
    host: String,
    repo: String,
    reference: String,
    is_digest: bool,
}

impl ParsedRef {
    /// The pinned digest (`sha256:<hex>`) when the ref is digest-form, so the
    /// fetched manifest can be verified against it; `None` for a tag ref.
    fn expected_digest(&self) -> Option<&str> {
        if self.is_digest {
            Some(&self.reference)
        } else {
            None
        }
    }
}

/// Split `reff` (`<host>/<repo…>@sha256:<hex>` or `<host>/<repo…>:<tag>`) into
/// host, repo, and reference. The tag/digest boundary is sought in the LAST path
/// segment, so the host's `:port` is never mistaken for a tag.
fn parse_ref(reff: &str) -> Result<ParsedRef> {
    let (host, rest) = reff
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("OCI ref {reff:?} has no '/' separating host from repo"))?;
    // Digest form wins (`@sha256:…`); the digest boundary is unambiguous.
    if let Some((repo, dig)) = rest.rsplit_once('@') {
        if !dig.starts_with("sha256:") {
            bail!("OCI ref {reff:?} digest {dig:?} is not a sha256 digest");
        }
        return Ok(ParsedRef {
            host: host.to_owned(),
            repo: repo.to_owned(),
            reference: dig.to_owned(),
            is_digest: true,
        });
    }
    // Tag form: the `:` lives in the LAST path segment (host already stripped).
    let seg_start = rest.rfind('/').map_or(0, |i| i + 1);
    if let Some(rel) = rest[seg_start..].find(':') {
        let split = seg_start + rel;
        return Ok(ParsedRef {
            host: host.to_owned(),
            repo: rest[..split].to_owned(),
            reference: rest[split + 1..].to_owned(),
            is_digest: false,
        });
    }
    Ok(ParsedRef {
        host: host.to_owned(),
        repo: rest.to_owned(),
        reference: "latest".to_owned(),
        is_digest: false,
    })
}

/// Map the HOST CPU architecture to the OCI image architecture name
/// (`x86_64 -> amd64`, `aarch64 -> arm64`); used to select the host-arch child
/// of a multi-arch image INDEX.
fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

// ── Auth ────────────────────────────────────────────────────────────────────

/// Read the verbatim `Authorization: Basic <auth>` value from an oras
/// docker-format registry-config file (`{"auths":{"<host>":{"auth":"<b64>"}}}`)
/// for `host`. Returns `None` when the file/entry is absent (anonymous pull).
/// The `auth` value is used VERBATIM (it is already `base64("x:<jwt>")`).
#[must_use]
pub fn read_basic_auth(config_file: Option<&str>, host: &str) -> Option<String> {
    let text = std::fs::read_to_string(config_file?).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let auths = v.get("auths")?.as_object()?;
    // Exact host key first, else the sole entry (the config is single-registry).
    let entry = auths.get(host).or_else(|| auths.values().next())?;
    entry.get("auth")?.as_str().map(str::to_owned)
}

// ── Public pull API ─────────────────────────────────────────────────────────

/// The layout directory the firecracker build reads (`<out>/oci`), exposed so
/// callers can reference the same convention.
#[must_use]
pub fn layout_subdir(out_dir: &Path) -> PathBuf {
    out_dir.join("oci")
}

/// Read auth from the oras registry-config, build the production reqwest
/// transport, and pull `reff` into `layout_dir`. This is the seam that replaces
/// `oras copy --to-oci-layout` at the firecracker build's pull call site.
///
/// # Errors
/// Ref-parse failure, a blob that never converges within its budget, or a
/// blob that fails sha256 verification after the bounded re-pulls.
pub async fn pull_image_http(
    reff: &str,
    layout_dir: &Path,
    registry_config_file: Option<&str>,
) -> Result<()> {
    let host = reff.split('/').next().unwrap_or(reff);
    let auth = read_basic_auth(registry_config_file, host);
    let transport = ReqwestTransport::new();
    pull_image(reff, layout_dir, auth.as_deref(), &transport).await
}

/// Pull the image `reff` into a spec-compliant OCI LAYOUT at `layout_dir`
/// (`oci-layout` + `index.json` + `blobs/sha256/<hex>` for manifest, config, and
/// every layer), RESUMABLY. `auth` is the verbatim `Authorization: Basic <auth>`
/// value or `None` (anonymous). `transport` is the injected HTTP GET seam.
///
/// # Errors
/// See [`pull_image_http`].
pub async fn pull_image(
    reff: &str,
    layout_dir: &Path,
    auth: Option<&str>,
    transport: &dyn HttpGet,
) -> Result<()> {
    let parsed = parse_ref(reff)?;
    let base = format!("http://{}", parsed.host);
    let blobs = layout_dir.join("blobs").join("sha256");
    tokio::fs::create_dir_all(&blobs)
        .await
        .with_context(|| format!("create blobs dir {}", blobs.display()))?;

    // Per-pull bearer cache: the OCI token-exchange (if the registry challenges
    // with Bearer) runs at most once, and the resulting token is reused across
    // the manifest, config, every layer, and every ranged resume.
    let cache = BearerCache::new();

    // 1. MANIFEST — fetch by ref (tag or digest). Descend a multi-arch INDEX to
    //    the host-arch child so `index.json` always points at an image manifest.
    let man_url = format!("{base}/v2/{}/manifests/{}", parsed.repo, parsed.reference);
    let top =
        fetch_manifest(transport, &man_url, auth, parsed.expected_digest(), &blobs, &cache).await?;
    let manifest = match maybe_select_index_child(&top.bytes)? {
        Some(child_digest) => {
            let child_url = format!("{base}/v2/{}/manifests/{}", parsed.repo, child_digest);
            fetch_manifest(transport, &child_url, auth, Some(&child_digest), &blobs, &cache).await?
        }
        None => top,
    };

    // 2. Parse the image manifest → config + layer descriptors.
    let man: serde_json::Value =
        serde_json::from_slice(&manifest.bytes).context("parse OCI image manifest JSON")?;
    let config_digest = man["config"]["digest"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("image manifest has no config.digest"))?
        .to_owned();

    // 3. CONFIG blob (resumable; verified).
    pull_blob(transport, &base, &parsed.repo, &config_digest, &blobs, auth, &cache).await?;

    // 4. LAYER blobs (resumable; verified; skipped if already complete on disk).
    if let Some(layers) = man["layers"].as_array() {
        for layer in layers {
            let dig = layer["digest"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("layer descriptor has no digest"))?;
            pull_blob(transport, &base, &parsed.repo, dig, &blobs, auth, &cache).await?;
        }
    }

    // 5. oci-layout marker + index.json → the single image-manifest descriptor.
    write_layout_marker(layout_dir).await?;
    write_index_json(
        layout_dir,
        &manifest.media_type,
        &manifest.hex,
        manifest.bytes.len(),
    )
    .await?;
    Ok(())
}

// ── Manifest + blobs ────────────────────────────────────────────────────────

/// A fetched-and-persisted manifest blob.
struct FetchedManifest {
    bytes: Vec<u8>,
    hex: String,
    media_type: String,
}

/// Download a manifest (resumably) to a scratch file, verify it against the
/// pinned digest when the ref carried one, then move it into its
/// content-addressed `blobs/sha256/<hex>` slot. Returns the bytes + hex + media
/// type (the manifest's own `mediaType`, defaulting to the OCI image-manifest).
async fn fetch_manifest(
    transport: &dyn HttpGet,
    url: &str,
    auth: Option<&str>,
    expected_digest: Option<&str>,
    blobs_dir: &Path,
    cache: &BearerCache,
) -> Result<FetchedManifest> {
    let tmp = blobs_dir.join(".manifest.tmp");
    download_resumable(transport, url, &tmp, Some(MANIFEST_ACCEPT), auth, cache)
        .await
        .with_context(|| format!("download manifest {url}"))?;
    let bytes = tokio::fs::read(&tmp)
        .await
        .with_context(|| format!("read manifest scratch {}", tmp.display()))?;
    let hex = sha256_hex(&bytes);
    if let Some(exp) = expected_digest {
        if exp != format!("sha256:{hex}") {
            let _ = tokio::fs::remove_file(&tmp).await;
            bail!("manifest digest mismatch: expected {exp}, got sha256:{hex}");
        }
    }
    let final_path = blobs_dir.join(&hex);
    if tokio::fs::rename(&tmp, &final_path).await.is_err() {
        tokio::fs::write(&final_path, &bytes)
            .await
            .with_context(|| format!("persist manifest blob {}", final_path.display()))?;
    }
    Ok(FetchedManifest {
        media_type: manifest_media_type(&bytes),
        bytes,
        hex,
    })
}

/// If `bytes` is a multi-arch image INDEX (has `manifests`, no `layers`), pick
/// the host-arch child (else the first) and return its digest; otherwise `None`
/// (it is already an image manifest).
fn maybe_select_index_child(bytes: &[u8]) -> Result<Option<String>> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).context("parse manifest/index JSON")?;
    let is_index = v.get("layers").is_none()
        && v.get("manifests")
            .and_then(serde_json::Value::as_array)
            .is_some();
    if !is_index {
        return Ok(None);
    }
    let empty = Vec::new();
    let arr = v["manifests"].as_array().unwrap_or(&empty);
    let want = host_arch();
    let pick = arr
        .iter()
        .find(|d| {
            d["platform"]["architecture"].as_str() == Some(want)
                && d["platform"]["os"].as_str().is_none_or(|os| os == "linux")
        })
        .or_else(|| arr.first());
    let digest = pick
        .and_then(|d| d["digest"].as_str())
        .ok_or_else(|| anyhow::anyhow!("image index has no selectable child manifest"))?;
    Ok(Some(digest.to_owned()))
}

/// The manifest's own `mediaType`, or the OCI image-manifest default. This is
/// the media type stamped on the `index.json` descriptor.
fn manifest_media_type(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| v["mediaType"].as_str().map(str::to_owned))
        .unwrap_or_else(|| OCI_MANIFEST_MEDIA.to_owned())
}

/// Pull one content-addressed blob (`sha256:<hex>`) into `blobs_dir/<hex>`,
/// RESUMABLY, verifying the assembled bytes against the digest; on a mismatch,
/// discard and re-pull from zero (bounded). Skips the download entirely when the
/// blob is already present and verified (cross-invocation resume idempotency).
async fn pull_blob(
    transport: &dyn HttpGet,
    base: &str,
    repo: &str,
    digest: &str,
    blobs_dir: &Path,
    auth: Option<&str>,
    cache: &BearerCache,
) -> Result<()> {
    let hex = digest.strip_prefix("sha256:").ok_or_else(|| {
        anyhow::anyhow!("unsupported digest {digest:?} (only sha256 is supported)")
    })?;
    let dest = blobs_dir.join(hex);
    if verify_file_sha256(&dest, hex).await? {
        return Ok(()); // already present + verified
    }
    let url = format!("{base}/v2/{repo}/blobs/{digest}");
    for attempt in 1..=VERIFY_REPULL_ATTEMPTS {
        download_resumable(transport, &url, &dest, None, auth, cache)
            .await
            .with_context(|| format!("download blob {digest}"))?;
        if verify_file_sha256(&dest, hex).await? {
            return Ok(());
        }
        tracing::warn!(
            digest,
            attempt,
            "pulled blob failed sha256 verify; discarding and re-pulling from zero"
        );
        let _ = tokio::fs::remove_file(&dest).await; // discard → next pull starts at 0
    }
    bail!("blob {digest} failed sha256 verification after {VERIFY_REPULL_ATTEMPTS} full re-pulls");
}

// ── Layout writing ──────────────────────────────────────────────────────────

/// Write the `oci-layout` marker (`{"imageLayoutVersion":"1.0.0"}`).
async fn write_layout_marker(layout_dir: &Path) -> Result<()> {
    tokio::fs::write(
        layout_dir.join("oci-layout"),
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    )
    .await
    .with_context(|| format!("write oci-layout marker in {}", layout_dir.display()))
}

/// Write `index.json` with a SINGLE image-manifest descriptor pointing at the
/// manifest blob — matching what `oras copy --to-oci-layout` produces for a
/// single-platform image (the shape `read_manifest_descriptor_from_layout`
/// consumes: a `manifests[]` array whose descriptor carries `mediaType`,
/// `digest`, `size`).
async fn write_index_json(
    layout_dir: &Path,
    manifest_media: &str,
    manifest_hex: &str,
    manifest_size: usize,
) -> Result<()> {
    let index = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": manifest_media,
            "digest": format!("sha256:{manifest_hex}"),
            "size": manifest_size,
        }],
    });
    let bytes = serde_json::to_vec(&index).context("serialize index.json")?;
    tokio::fs::write(layout_dir.join("index.json"), &bytes)
        .await
        .with_context(|| format!("write index.json in {}", layout_dir.display()))
}

#[cfg(test)]
#[path = "oci_pull_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "oci_pull_bearer_tests.rs"]
mod bearer_tests;
