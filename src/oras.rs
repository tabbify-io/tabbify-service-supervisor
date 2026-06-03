//! `oras` argv helper for the generic-Firecracker build.
//!
//! The mesh OCI registry (Zot) serves plain HTTP over the encrypted WireGuard
//! tunnel. The Firecracker RUNTIME-build pulls a deployed image into a
//! spec-compliant OCI LAYOUT via `oras copy --to-oci-layout` (the only form that
//! yields a full layout for a normal container image); this module builds that
//! argv. The command itself is run by the Firecracker build's own runner.

/// Build the `oras copy --to-oci-layout` argument list (sans the leading binary
/// name) for pulling a container image into a spec-compliant OCI LAYOUT.
///
/// `oras pull -o <dir>` does NOT produce a layout for a normal container image:
/// it skips every layer that lacks an `org.opencontainers.image.title`
/// annotation (all docker-built layers) and leaves the output dir EMPTY
/// (`"Skipped pulling layers without file name ... Use 'oras copy ...
/// --to-oci-layout'"`). `oras copy <ref> --to-oci-layout <dir>` is the form that
/// yields the full layout (`oci-layout` + `index.json` + `blobs/<alg>/<hex>` for
/// manifest+config+layers).
///
/// For the plain-HTTP mesh registry the SOURCE flag is `--from-plain-http` (the
/// source is plain HTTP on the encrypted WireGuard overlay `[ula]:5000`), NOT
/// `--plain-http`: `--plain-http` would not register as the copy SOURCE flag.
///
/// # Example
/// ```
/// # use tabbify_supervisor::oras::oras_copy_to_oci_layout_args;
/// let args = oras_copy_to_oci_layout_args("[fd5a::1]:5000/acme/vm@sha256:abc", "/tmp/oci");
/// assert_eq!(args[0], "copy");
/// assert!(args.contains(&"--from-plain-http".to_owned()));
/// assert!(args.contains(&"--to-oci-layout".to_owned()));
/// ```
#[must_use]
pub fn oras_copy_to_oci_layout_args(reff: &str, layout_dir: &str) -> Vec<String> {
    vec![
        "copy".to_owned(),
        "--from-plain-http".to_owned(),
        reff.to_owned(),
        "--to-oci-layout".to_owned(),
        layout_dir.to_owned(),
    ]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ---- oras_copy_to_oci_layout_args ----------------------------------------

    /// The copy args must be the `oras copy --from-plain-http <ref>
    /// --to-oci-layout <dir>` form (the probe-proven layout-producing form), NOT
    /// `oras pull -o` and NOT the `--plain-http` flag.
    #[test]
    fn copy_to_oci_layout_args_uses_from_plain_http_and_to_oci_layout() {
        let reff = "[fd5a:1f02::1]:5000/acme/vm@sha256:abc";
        let dir = "/tmp/oci";
        let args = oras_copy_to_oci_layout_args(reff, dir);
        assert_eq!(args[0], "copy", "first arg must be 'copy'");
        assert!(
            args.contains(&"--from-plain-http".to_owned()),
            "mesh registry source is plain http; must use --from-plain-http; got {args:?}"
        );
        assert!(
            !args.contains(&"--plain-http".to_owned()),
            "--plain-http is not the copy SOURCE flag; got {args:?}"
        );
        assert!(
            args.contains(&"--to-oci-layout".to_owned()),
            "must copy into an OCI layout; got {args:?}"
        );
        assert!(args.contains(&reff.to_owned()), "must carry the ref; got {args:?}");
        assert!(args.contains(&dir.to_owned()), "must carry the layout dir; got {args:?}");
        assert!(
            !args.contains(&"-o".to_owned()) && !args.contains(&"pull".to_owned()),
            "must NOT be the empty-layout `oras pull -o` form; got {args:?}"
        );
    }

    /// Exact argv shape:
    /// `["copy", "--from-plain-http", <reff>, "--to-oci-layout", <dir>]`.
    #[test]
    fn copy_to_oci_layout_args_exact_shape() {
        let args = oras_copy_to_oci_layout_args("reg/app@sha256:abc", "/out/oci");
        assert_eq!(
            args,
            vec![
                "copy",
                "--from-plain-http",
                "reg/app@sha256:abc",
                "--to-oci-layout",
                "/out/oci",
            ]
        );
    }
}
