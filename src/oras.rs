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
/// When `registry_config_dir` is `Some(dir)`, prepends `--registry-config <dir>`
/// so oras reads credentials from `<dir>/config.json` (docker-format auth).
/// Callers that do not need auth pass `None` (anonymous; today's default).
///
/// # Example
/// ```
/// # use tabbify_supervisor::oras::oras_copy_to_oci_layout_args;
/// let args = oras_copy_to_oci_layout_args("[fd5a::1]:5000/acme/vm@sha256:abc", "/tmp/oci", None);
/// assert_eq!(args[0], "copy");
/// assert!(args.contains(&"--from-plain-http".to_owned()));
/// assert!(args.contains(&"--to-oci-layout".to_owned()));
/// ```
#[must_use]
pub fn oras_copy_to_oci_layout_args(
    reff: &str,
    layout_dir: &str,
    registry_config_dir: Option<&str>,
) -> Vec<String> {
    let mut args = vec!["copy".to_owned()];
    if let Some(dir) = registry_config_dir {
        args.push("--registry-config".to_owned());
        args.push(dir.to_owned());
    }
    args.push("--from-plain-http".to_owned());
    args.push(reff.to_owned());
    args.push("--to-oci-layout".to_owned());
    args.push(layout_dir.to_owned());
    args
}

/// Build the `oras resolve --plain-http <ref>` argument list (sans the leading
/// binary name): resolve a TAG (or digest) ref to its immutable manifest digest
/// (`sha256:…`) by fetching ONLY the manifest (a few KB), NOT the layer blobs.
///
/// This is the key to the digest-shared rootfs cache: a tag ref's immutable
/// digest is otherwise unknown until the (multi-minute, multi-MB) `oras copy`
/// pull completes, so the cache could only be consulted AFTER paying the pull.
/// `oras resolve` returns the digest in ~0.2 s, so the runtime build can check
/// the digest-keyed cache BEFORE pulling and skip the pull entirely on a hit.
///
/// Single-target command ⇒ the plain-HTTP flag is `--plain-http` (NOT the
/// copy-specific `--from-plain-http`). The digest is printed to stdout as a
/// single `sha256:<hex>` line.
///
/// When `registry_config_dir` is `Some(dir)`, prepends `--registry-config <dir>`
/// so oras reads credentials from `<dir>/config.json` (docker-format auth).
/// Callers that do not need auth pass `None` (anonymous; today's default).
///
/// # Example
/// ```
/// # use tabbify_supervisor::oras::oras_resolve_args;
/// let args = oras_resolve_args("[fd5a::1]:5000/acme/vm:tag", None);
/// assert_eq!(args, vec!["resolve", "--plain-http", "[fd5a::1]:5000/acme/vm:tag"]);
/// ```
#[must_use]
pub fn oras_resolve_args(reff: &str, registry_config_dir: Option<&str>) -> Vec<String> {
    let mut args = vec!["resolve".to_owned()];
    if let Some(dir) = registry_config_dir {
        args.push("--registry-config".to_owned());
        args.push(dir.to_owned());
    }
    args.push("--plain-http".to_owned());
    args.push(reff.to_owned());
    args
}

/// Lowercase the repository portion of an OCI reference, preserving the tag /
/// digest. The OCI distribution spec requires repository names to be lowercase;
/// a GitHub owner like `Lsneg` would otherwise make the registry reject the
/// push AND the pull with `invalid reference: invalid repository "Lsneg/…"`. The
/// tag (`:tag`) and digest (`@sha256:…`) keep their original case (the spec
/// permits uppercase in a tag). The registry host (`[ula]:5000`) is hex/digits,
/// so lowercasing it is a no-op. Used symmetrically by the build PUSH and the
/// runtime PULL so the two refs always match.
#[must_use]
pub fn lowercase_oci_repo(reff: &str) -> String {
    // The tag/digest boundary lives in the LAST path segment (after the final
    // `/`), so the registry host's `:port` is never mistaken for a tag.
    let seg_start = reff.rfind('/').map_or(0, |i| i + 1);
    match reff[seg_start..].find(['@', ':']) {
        Some(rel) => {
            let split = seg_start + rel;
            format!("{}{}", reff[..split].to_lowercase(), &reff[split..])
        }
        None => reff.to_lowercase(),
    }
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
        let args = oras_copy_to_oci_layout_args(reff, dir, None);
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
        assert!(
            args.contains(&reff.to_owned()),
            "must carry the ref; got {args:?}"
        );
        assert!(
            args.contains(&dir.to_owned()),
            "must carry the layout dir; got {args:?}"
        );
        assert!(
            !args.contains(&"-o".to_owned()) && !args.contains(&"pull".to_owned()),
            "must NOT be the empty-layout `oras pull -o` form; got {args:?}"
        );
    }

    /// Exact argv shape without auth (None):
    /// `["copy", "--from-plain-http", <reff>, "--to-oci-layout", <dir>]`.
    #[test]
    fn copy_to_oci_layout_args_exact_shape_anonymous() {
        let args = oras_copy_to_oci_layout_args("reg/app@sha256:abc", "/out/oci", None);
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

    /// With `registry_config_dir = Some(dir)`, `--registry-config <dir>` is
    /// prepended right after the `copy` subcommand so oras loads credentials
    /// from `<dir>/config.json` before attempting the pull.
    #[test]
    fn copy_to_oci_layout_args_prepends_registry_config_when_some() {
        let args = oras_copy_to_oci_layout_args(
            "reg/app@sha256:abc",
            "/out/oci",
            Some("/tmp/oras-cfg"),
        );
        assert_eq!(args[0], "copy");
        assert_eq!(args[1], "--registry-config");
        assert_eq!(args[2], "/tmp/oras-cfg");
        // The rest of the flags must still be present.
        assert!(args.contains(&"--from-plain-http".to_owned()));
        assert!(args.contains(&"--to-oci-layout".to_owned()));
        assert!(args.contains(&"reg/app@sha256:abc".to_owned()));
    }

    // ---- oras_resolve_args ---------------------------------------------------

    /// The resolve args must be `oras resolve --plain-http <ref>` (single-target
    /// command ⇒ `--plain-http`, NOT the copy-only `--from-plain-http`).
    #[test]
    fn resolve_args_exact_shape_anonymous() {
        let args = oras_resolve_args("[fd5a::1]:5000/acme/vm:tag", None);
        assert_eq!(
            args,
            vec!["resolve", "--plain-http", "[fd5a::1]:5000/acme/vm:tag"]
        );
        assert!(
            !args.contains(&"--from-plain-http".to_owned()),
            "resolve is single-target; must use --plain-http; got {args:?}"
        );
    }

    /// With `registry_config_dir = Some(dir)`, `--registry-config <dir>` is
    /// prepended so oras loads credentials before resolving.
    #[test]
    fn resolve_args_prepends_registry_config_when_some() {
        let args = oras_resolve_args("[fd5a::1]:5000/acme/vm:tag", Some("/tmp/oras-cfg"));
        assert_eq!(args[0], "resolve");
        assert_eq!(args[1], "--registry-config");
        assert_eq!(args[2], "/tmp/oras-cfg");
        assert!(args.contains(&"--plain-http".to_owned()));
        assert!(args.contains(&"[fd5a::1]:5000/acme/vm:tag".to_owned()));
    }

    // ---- lowercase_oci_repo --------------------------------------------------

    /// The namespace/owner is lowercased; the `:tag` keeps its original case.
    #[test]
    fn lowercase_oci_repo_lowercases_namespace_keeps_tag() {
        assert_eq!(
            lowercase_oci_repo("[fd5a:1f00:0:3::1]:5000/Lsneg/98f9eba0-AB:Main"),
            "[fd5a:1f00:0:3::1]:5000/lsneg/98f9eba0-ab:Main"
        );
    }

    /// A digest ref: the `@sha256:…` boundary wins over the colon inside it, and
    /// the digest is preserved verbatim while the repo path is lowercased.
    #[test]
    fn lowercase_oci_repo_preserves_digest() {
        assert_eq!(
            lowercase_oci_repo("[fd5a::1]:5000/Acme/App@sha256:ABCdef"),
            "[fd5a::1]:5000/acme/app@sha256:ABCdef"
        );
    }

    /// The registry host's `:port` must NOT be mistaken for a tag boundary.
    #[test]
    fn lowercase_oci_repo_host_port_not_a_tag() {
        assert_eq!(
            lowercase_oci_repo("[fd5a::1]:5000/Owner/app"),
            "[fd5a::1]:5000/owner/app"
        );
    }
}
