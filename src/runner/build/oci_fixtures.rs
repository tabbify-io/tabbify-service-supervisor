//! OCI-layout test fixtures for the docker-less Firecracker build tests.
//!
//! Extracted from `firecracker_tests.rs` to keep that file within the
//! ~500-line guideline: it hosts the on-disk OCI-layout writer and the
//! in-memory tar builder the conversion tests stage their inputs with.

use std::path::Path;

/// OCI media type for an uncompressed tar layer.
pub(crate) const MEDIA_TAR: &str = "application/vnd.oci.image.layer.v1.tar";
/// OCI media type for a gzip-compressed tar layer (real images ship this).
pub(crate) const MEDIA_TAR_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
/// OCI media type for a zstd-compressed tar layer.
pub(crate) const MEDIA_TAR_ZSTD: &str = "application/vnd.oci.image.layer.v1.tar+zstd";

/// Write a minimal spec-compliant OCI layout under `dir`: a config blob, an
/// image manifest referencing it (+ given layer descriptors), and an index.json
/// pointing at the manifest. Returns the layout dir. `layers` = (digest, bytes).
///
/// Layer descriptors carry the UNCOMPRESSED tar media type. For layers that
/// carry a compressed media type (gzip / zstd) — needed by the real-conversion
/// integration test so the production `tar_decompress_flag` selects `-z`/`--zstd`
/// — use [`write_min_oci_layout_typed`].
pub(crate) fn write_min_oci_layout(
    dir: &Path,
    config_json: &serde_json::Value,
    layers: &[(&str, &[u8])],
) -> std::path::PathBuf {
    let typed: Vec<(&str, &[u8], &str)> =
        layers.iter().map(|(d, b)| (*d, *b, MEDIA_TAR)).collect();
    write_min_oci_layout_typed(dir, config_json, &typed)
}

/// Like [`write_min_oci_layout`] but each layer carries an explicit OCI media
/// type, so a layout can stage gzip- or zstd-compressed layers
/// (`application/vnd.oci.image.layer.v1.tar+gzip` / `+zstd`). The production
/// `tar_decompress_flag` keys the host-`tar` decompression flag off this media
/// type, so the integration test stages real compressed bytes (via
/// [`make_tar_gzip`] / [`make_tar_zstd`]) under the matching media type.
/// `layers` = (diff_id, bytes, media_type).
pub(crate) fn write_min_oci_layout_typed(
    dir: &Path,
    config_json: &serde_json::Value,
    layers: &[(&str, &[u8], &str)],
) -> std::path::PathBuf {
    use sha2::{Digest as _, Sha256};
    let blobs = dir.join("blobs").join("sha256");
    std::fs::create_dir_all(&blobs).unwrap();
    let put = |bytes: &[u8]| -> String {
        let hex = format!("{:x}", Sha256::digest(bytes));
        std::fs::write(blobs.join(&hex), bytes).unwrap();
        hex
    };
    let cfg_bytes = serde_json::to_vec(config_json).unwrap();
    let cfg_hex = put(&cfg_bytes);
    let layer_descs: Vec<serde_json::Value> = layers
        .iter()
        .map(|(d, b, mt)| {
            let hex = put(b);
            serde_json::json!({
                "mediaType": *mt,
                "digest": format!("sha256:{hex}"), "size": b.len(),
                "annotations": {"diffid": *d}
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType":"application/vnd.oci.image.config.v1+json",
                   "digest": format!("sha256:{cfg_hex}"), "size": cfg_bytes.len()},
        "layers": layer_descs
    });
    let man_bytes = serde_json::to_vec(&manifest).unwrap();
    let man_hex = put(&man_bytes);
    let index = serde_json::json!({
        "schemaVersion": 2,
        "manifests": [{"mediaType":"application/vnd.oci.image.manifest.v1+json",
                       "digest": format!("sha256:{man_hex}"), "size": man_bytes.len()}]
    });
    std::fs::write(dir.join("index.json"), serde_json::to_vec(&index).unwrap()).unwrap();
    std::fs::write(dir.join("oci-layout"), br#"{"imageLayoutVersion":"1.0.0"}"#).unwrap();
    dir.to_path_buf()
}

/// Build an uncompressed tar (`application/vnd.oci.image.layer.v1.tar`) in
/// memory from (path, bytes) entries at the default `0o644` mode, using the
/// `tar` dev-dependency.
pub(crate) fn make_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let with_mode: Vec<(&str, &[u8], u32)> =
        entries.iter().map(|(n, b)| (*n, *b, 0o644u32)).collect();
    make_tar_modes(&with_mode)
}

/// Build a tar in memory with one explicit mode per entry, so a fixture can
/// stage e.g. an executable (`0o755`) `/init` or entrypoint binary the
/// real-conversion test then asserts survives into the ext4 at that mode. The
/// 2-tuple [`make_tar`] keeps every entry at the default `0o644`.
pub(crate) fn make_tar_modes(entries: &[(&str, &[u8], u32)]) -> Vec<u8> {
    let mut ar = tar::Builder::new(Vec::new());
    for (name, data, mode) in entries {
        let mut hdr = tar::Header::new_gnu();
        hdr.set_size(data.len() as u64);
        hdr.set_mode(*mode);
        hdr.set_cksum();
        ar.append_data(&mut hdr, name, *data as &[u8]).unwrap();
    }
    ar.into_inner().unwrap()
}

/// Build an uncompressed tar then gzip it, yielding the bytes a real
/// `application/vnd.oci.image.layer.v1.tar+gzip` layer ships. The production
/// `tar_decompress_flag` selects `-z` for this media type, so the host `tar -z`
/// unpack must be able to inflate exactly these bytes.
pub(crate) fn make_tar_gzip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    gzip_bytes(&make_tar(entries))
}

/// Build an uncompressed tar (with explicit per-entry modes) then gzip it.
pub(crate) fn make_tar_gzip_modes(entries: &[(&str, &[u8], u32)]) -> Vec<u8> {
    gzip_bytes(&make_tar_modes(entries))
}

/// Build an uncompressed tar then zstd-compress it, yielding the bytes a real
/// `application/vnd.oci.image.layer.v1.tar+zstd` layer ships. The production
/// `tar_decompress_flag` selects `--zstd` for this media type.
pub(crate) fn make_tar_zstd(entries: &[(&str, &[u8])]) -> Vec<u8> {
    zstd::encode_all(make_tar(entries).as_slice(), 0).unwrap()
}

/// gzip-compress `raw` with the pure-Rust `flate2` backend.
fn gzip_bytes(raw: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(raw).unwrap();
    enc.finish().unwrap()
}
