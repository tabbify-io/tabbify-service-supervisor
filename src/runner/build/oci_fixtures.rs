//! OCI-layout test fixtures for the docker-less Firecracker build tests.
//!
//! Extracted from `firecracker_tests.rs` to keep that file within the
//! ~500-line guideline: it hosts the on-disk OCI-layout writer and the
//! in-memory tar builder the conversion tests stage their inputs with.

use std::path::Path;

/// Write a minimal spec-compliant OCI layout under `dir`: a config blob, an
/// image manifest referencing it (+ given layer descriptors), and an index.json
/// pointing at the manifest. Returns the layout dir. `layers` = (digest, bytes).
pub(crate) fn write_min_oci_layout(
    dir: &Path,
    config_json: &serde_json::Value,
    layers: &[(&str, &[u8])],
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
        .map(|(d, b)| {
            let hex = put(b);
            serde_json::json!({
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
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
/// memory from (path, bytes) entries, using the `tar` dev-dependency.
pub(crate) fn make_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut ar = tar::Builder::new(Vec::new());
    for (name, data) in entries {
        let mut hdr = tar::Header::new_gnu();
        hdr.set_size(data.len() as u64);
        hdr.set_mode(0o644);
        hdr.set_cksum();
        ar.append_data(&mut hdr, name, *data as &[u8]).unwrap();
    }
    ar.into_inner().unwrap()
}
