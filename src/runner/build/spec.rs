//! `[build]` directive resolution for a one-shot build: the [`BuildSpec`]
//! resolved from the clone's `tabbify.toml`, the optional stable "moving" tag
//! ref, and the oras push-auth config plumbing. Split out of `mod.rs` so the
//! pipeline orchestration stays readable.

use std::path::Path;

use anyhow::Context as _;

/// The resolved `[build]` directives for a docker build: the clone root, the
/// context directory the image is built from, and the optional Dockerfile path.
/// All are absolute paths under the clone root.
#[derive(Debug, Clone)]
pub(crate) struct BuildSpec {
    /// The clone root (`<workdir>/src`); used to place the `oci-out` push layout
    /// stably at `<workdir>/oci-out` regardless of a non-root build context.
    pub clone_root: std::path::PathBuf,
    /// The build context dir (`<src>/<[build].context>`, default `<src>`).
    pub context_dir: std::path::PathBuf,
    /// The Dockerfile path (`<src>/<[build].dockerfile>`). `None` ⇒ Docker's
    /// default (`<context_dir>/Dockerfile`).
    pub dockerfile: Option<std::path::PathBuf>,
    /// The raw `[build].context` from the toml (default `"."`), RELATIVE to the
    /// clone root. Threaded verbatim into the fc-sandbox job contract (job.json
    /// v2) so the guest builder can honor a subdir context; the docker path uses
    /// the absolute [`Self::context_dir`].
    pub raw_context: String,
    /// The raw `[build].dockerfile` from the toml (default `"Dockerfile"`),
    /// RELATIVE to the clone root. Threaded into job.json v2 alongside
    /// [`Self::raw_context`]; the guest splits it into a dockerfile DIR +
    /// `--opt filename=` basename for buildkit.
    pub raw_dockerfile: String,
    /// OPTIONAL stable "moving" tag (`[build].stable_tag`, e.g. `"current"`) the
    /// build publishes ALONGSIDE the immutable `:<commit_sha>` artifact tag. When
    /// `Some`, the push ALSO registers `<registry>/<tenant>/<uuid>:<stable_tag>`
    /// re-pointed at the freshly-built digest — the mechanism that makes a BASE
    /// image rebuild auto-take-effect for a consumer that references the stable
    /// tag (the node's workspace/devbox base ref). `None` ⇒ only `:<sha>` (today).
    pub stable_tag: Option<String>,
}

#[cfg(test)]
impl BuildSpec {
    /// `true` when `[build].context`/`[build].dockerfile` are at their defaults
    /// (`"."` / `"Dockerfile"`). Test-only predicate over the resolved raw
    /// fields: the fc-sandbox path no longer rejects a non-default layout (the
    /// v2 guest honors it), so this is just a readability helper for the
    /// `resolve_build_spec` resolution tests.
    pub(crate) fn is_default_layout(&self) -> bool {
        self.raw_context == "." && self.raw_dockerfile == "Dockerfile"
    }
}

/// Resolve the [`BuildSpec`] from the `tabbify.toml` at `toml_path` (if present).
///
/// - No toml at the clone root ⇒ today's defaults: `context_dir = src`,
///   `dockerfile = None` (Docker's default `<src>/Dockerfile`).
/// - Toml present ⇒ parse it with the vendored [`crate::unified_manifest::UnifiedManifest`]
///   and resolve `[build].context` (default `.`) + `[build].dockerfile`
///   (default `Dockerfile`) relative to `src`.
///
/// # Errors
/// A malformed `tabbify.toml` (parse error) is surfaced so the build fails with
/// a clear diagnostic rather than silently ignoring a broken managed config.
pub(crate) fn resolve_build_spec(src: &Path, toml_path: &Path) -> anyhow::Result<BuildSpec> {
    if !toml_path.exists() {
        return Ok(BuildSpec {
            clone_root: src.to_path_buf(),
            context_dir: src.to_path_buf(),
            dockerfile: None,
            raw_context: ".".to_owned(),
            raw_dockerfile: "Dockerfile".to_owned(),
            stable_tag: None,
        });
    }
    let text = std::fs::read_to_string(toml_path)
        .with_context(|| format!("read tabbify.toml at {}", toml_path.display()))?;
    let manifest: crate::unified_manifest::UnifiedManifest = toml::from_str(&text)
        .with_context(|| format!("parse tabbify.toml at {}", toml_path.display()))?;
    Ok(BuildSpec {
        clone_root: src.to_path_buf(),
        context_dir: src.join(manifest.context()),
        dockerfile: Some(src.join(manifest.dockerfile())),
        raw_context: manifest.context().to_owned(),
        raw_dockerfile: manifest.dockerfile().to_owned(),
        stable_tag: manifest.stable_tag().map(str::to_owned),
    })
}

/// Reconstruct the STABLE "moving"-tag registry ref
/// (`<registry_ula>/<tenant>/<app_uuid>:<stable_tag>`) the build publishes
/// alongside the immutable `:<commit_sha>` artifact when `[build].stable_tag` is
/// set — byte-for-byte the same scheme as the primary `run_build` `reff`, only
/// the tag component differs. `stable_tag == None` ⇒ `None` (no extra tag).
///
/// Kept as a pure fn (mirrors the node's `deterministic_reff`) so the ref shape
/// is unit-testable without a live build. The tenant is lowercased for the SAME
/// reason the primary ref lowercases it (OCI repository names are lowercase; a
/// mixed-case GitHub owner would otherwise be rejected by oras/skopeo).
#[must_use]
pub(crate) fn build_stable_reff(
    registry_ula: &str,
    tenant: &str,
    app_uuid: &str,
    stable_tag: Option<&str>,
) -> Option<String> {
    stable_tag.map(|tag| {
        format!(
            "{}/{}/{}:{}",
            registry_ula,
            tenant.to_lowercase(),
            app_uuid,
            tag
        )
    })
}

/// Build the oras `--to-registry-config` value for a SANDBOXED-build push.
///
/// [`crate::skopeo::write_registry_config`] writes `<workdir>/oras-cfg/config.json`;
/// oras's `--to-registry-config` flag wants that config.json FILE, NOT the
/// containing dir — the SAME dir-vs-file contract the v1.4.79 PULL/copy fix
/// established for `--from-registry-config` (this sandbox PUSH path was missed).
/// Returns `None` when no push token is supplied (anonymous registry — today's
/// default), so an unauthenticated push is byte-for-byte unchanged.
///
/// # Errors
/// Auth-config write failure.
pub(crate) fn oras_push_cfg_file(
    workdir: &Path,
    push_token: Option<&str>,
    registry_ula: &str,
) -> anyhow::Result<Option<String>> {
    let Some(token) = push_token else {
        return Ok(None);
    };
    let cfg_dir = workdir.join("oras-cfg");
    crate::skopeo::write_registry_config(token, registry_ula, &cfg_dir)
        .with_context(|| format!("write oras registry auth config to {}", cfg_dir.display()))?;
    // The FILE, not the dir — `--to-registry-config` decodes it as config.json.
    Ok(Some(
        cfg_dir.join("config.json").to_string_lossy().into_owned(),
    ))
}
