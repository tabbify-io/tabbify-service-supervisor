//! Generic Firecracker runtime-build: convert an OCI image into a
//! `rootfs.ext4` + a minimal PID-1 init, then boot it via the existing
//! `FirecrackerRuntime` contract.
//!
//! This is a RUNTIME-build helper (OCI image → bootable rootfs), invoked from
//! [`crate::build::build_runtime`] — NOT the CI-build pipeline in the sibling
//! `docker.rs` / `wasm.rs` (clone → build → push). See plan 04.

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    /// `oci-spec` links and parses an OCI image config's entrypoint/cmd/env/
    /// workdir. This proves the dependency is wired before we build on it.
    #[test]
    fn oci_spec_parses_image_config_json() {
        let json = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Entrypoint": ["/app/server"],
                "Cmd": ["--port", "8080"],
                "Env": ["RUST_LOG=info", "PORT=8080"],
                "WorkingDir": "/app"
            },
            "rootfs": { "type": "layers", "diff_ids": [] }
        }"#;
        let cfg: oci_spec::image::ImageConfiguration =
            serde_json::from_str(json).unwrap();
        let inner = cfg.config().as_ref().unwrap();
        assert_eq!(
            inner.entrypoint().as_ref().unwrap(),
            &vec!["/app/server".to_owned()]
        );
        assert_eq!(
            inner.cmd().as_ref().unwrap(),
            &vec!["--port".to_owned(), "8080".to_owned()]
        );
        assert_eq!(inner.working_dir().as_ref().unwrap(), "/app");
        assert!(
            inner
                .env()
                .as_ref()
                .unwrap()
                .contains(&"RUST_LOG=info".to_owned())
        );
    }
}
