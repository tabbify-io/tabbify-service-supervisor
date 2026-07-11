//! Unified `tabbify.toml` schema (spec §4) — the single source of truth for
//! project + build + runtime + deploy topology.
//!
//! Single-runtime model: there is exactly ONE runtime (Firecracker-from-OCI-image).
//! The manifest no longer exposes a runtime-selection surface — every app builds a
//! Docker/OCI image and deploys as Firecracker. A legacy `tabbify.toml` that still
//! carries `[runtime] type = "..."` (or `[[deploy]].runtime`) parses fine: the
//! field is simply ignored (no `deny_unknown_fields`, contract D8).
//!
//! Vendored IDENTICALLY across cli / node / supervisor (mirror of
//! `tabbify-cli/src/unified_manifest.rs`); lives in its own module (NOT
//! `manifest.rs`, contract D11) to keep each file focused. `BuildKind` is the
//! FROZEN wire type from the contract — vendored identically across cli / node /
//! supervisor (in the supervisor it lives in [`crate::runner::build`]).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::manifest::{
    AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime as ManifestRuntime,
};
use crate::runner::build::BuildKind;
use crate::runtime::Runtime;

/// Map a `[runtime].lifecycle` string to a [`LifecycleMode`].
///
/// CANONICAL fallback (single source of truth, used by both
/// [`UnifiedManifest::derive_app_manifest`] and the connect-repo synthesis in
/// [`crate::build::fetched_from_ref`]): `"on_request"` ⇒ `OnRequest`; everything
/// else (`"always_on"` AND any unknown value) ⇒ `AlwaysOn`. `AlwaysOn` is the
/// FC live-path default — a deployed app should come up immediately, and an
/// unknown lifecycle must not silently flip an app to lazy-start.
#[must_use]
pub fn lifecycle_mode_from_str(lifecycle: &str) -> LifecycleMode {
    match lifecycle {
        "on_request" => LifecycleMode::OnRequest,
        _ => LifecycleMode::AlwaysOn,
    }
}

/// Top-level `tabbify.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnifiedManifest {
    /// `[app]` — identity + display metadata.
    pub app: AppSection,
    /// `[build]` — how to produce the ONE artifact.
    pub build: BuildSection,
    /// `[runtime]` — runtime resource limits + lifecycle. No runtime SELECTION:
    /// the runtime is always Firecracker-from-OCI-image (single-runtime model).
    #[serde(default)]
    pub runtime: RuntimeSection,
    /// `[routes]` — routing hints.
    #[serde(default)]
    pub routes: RoutesSection,
    /// `[[deploy]]` — WHERE to run, one block per placement. Empty ⇒ fallback
    /// to the node's default supervisor (contract D2, handled downstream).
    #[serde(default)]
    pub deploy: Vec<DeployTarget>,
    /// `[env]` — env shared by all targets; `[deploy.env]` overrides per key (D6).
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// `[app]` table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppSection {
    /// App UUID v7; `tcli`/node stamps it when absent.
    #[serde(default)]
    pub id: Option<Uuid>,
    /// Display name.
    pub name: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

/// `[build]` table — how to build the ONE artifact.
///
/// Single-runtime model: the build always produces an OCI image from a Dockerfile,
/// so `kind` is Docker-only. A legacy `kind = "wasm"` (or a stray `command = ...`)
/// is tolerated by serde (no `deny_unknown_fields`) and resolves to Docker.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BuildSection {
    /// Build kind — always `docker` (OCI image). FROZEN wire: `"docker"`.
    #[serde(default)]
    pub kind: BuildKind,
    /// Build context dir.
    #[serde(default = "default_context")]
    pub context: String,
    /// Dockerfile path.
    #[serde(default)]
    pub dockerfile: Option<String>,
    /// Supervisor (`display_name` | ULA) that builds + pushes the artifact.
    #[serde(default)]
    pub builder: Option<String>,
    /// OPTIONAL stable "moving" tag published ALONGSIDE the immutable
    /// `:<commit_sha>` artifact tag (e.g. `"current"`). When set, the build
    /// ALSO pushes `<registry>/<tenant>/<uuid>:<stable_tag>` re-pointed at the
    /// freshly-built digest, so a downstream consumer that references the image
    /// by this stable tag auto-picks-up EVERY rebuild without a digest hunt.
    /// This is how a BASE image (the per-user workspace / devbox root) makes a
    /// rebuild "just take effect": the node references `platform/<uuid>:current`
    /// and the supervisor resolves that tag → the current digest at provision
    /// time (the rootfs cache is keyed by the IMMUTABLE digest, so a moved tag
    /// forces a fresh convert — never a stale rootfs). Trust: the tag lives in
    /// the SAME write-gated namespace as the artifact (`platform/*` is writable
    /// only by the platform token), so a moving `platform/` tag is trusted.
    /// Absent / empty ⇒ today's behaviour exactly (only the `:<sha>` tag).
    #[serde(default)]
    pub stable_tag: Option<String>,
}
fn default_context() -> String {
    ".".to_owned()
}

/// `[runtime]` table — runtime resource limits + lifecycle.
///
/// Single-runtime model: NO runtime selection. The runtime is always
/// Firecracker-from-OCI-image. A legacy `type = "..."` (and the WASM-only
/// `fuel_per_request`) is tolerated by serde and ignored.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeSection {
    /// `always_on` | `on_request` (free string, Phase-1 vocabulary).
    #[serde(default = "default_lifecycle")]
    pub lifecycle: String,
    /// Idle stop timeout (seconds) for `on_request`.
    #[serde(default = "default_idle")]
    pub idle_timeout_sec: u64,
    /// Memory cap (MB).
    #[serde(default = "default_mem")]
    pub memory_mb: u32,
    /// vCPUs (firecracker).
    #[serde(default = "default_vcpus")]
    pub vcpus: u32,
    /// Port the guest app listens on (firecracker). `None` → the supervisor's
    /// configured default (8080). Lets a service whose image serves a non-8080
    /// port (e.g. www-backend `8788`, www-frontend `3000`) run as an FC app
    /// unchanged. Optional + forward-compat.
    #[serde(default)]
    pub port: Option<u16>,
    /// Whether this app uses a persistent data disk (Firecracker block device).
    /// `false` (default) = ephemeral rootfs only; `true` = a durable disk is
    /// provisioned and survives VM restarts. Gated by later tasks — this task
    /// ONLY adds the flag so manifests can declare intent.
    #[serde(default)]
    pub stateful: bool,
    /// Guest mount path for the persistent data disk (e.g. `"/var/lib/tabbify-forge"`).
    /// Ignored when `stateful = false`. `None` when absent (default).
    #[serde(default)]
    pub data_mount: Option<String>,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            lifecycle: default_lifecycle(),
            idle_timeout_sec: default_idle(),
            memory_mb: default_mem(),
            vcpus: default_vcpus(),
            port: None,
            stateful: false,
            data_mount: None,
        }
    }
}
fn default_lifecycle() -> String {
    "on_request".to_owned()
}
fn default_idle() -> u64 {
    300
}
fn default_mem() -> u32 {
    512
}
fn default_vcpus() -> u32 {
    1
}

/// `[routes]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RoutesSection {
    /// Path prefixes routed to the app (`["/"]` = everything).
    #[serde(default)]
    pub dynamic_prefixes: Vec<String>,
}

/// One `[[deploy]]` block — a placement.
///
/// Single-runtime model: there is no per-target runtime override. A legacy
/// `runtime = "..."` key is tolerated by serde and ignored.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeployTarget {
    /// Target supervisor (`display_name` | ULA).
    pub supervisor: String,
    /// Per-target env, merged shallow OVER `[env]` (D6: this wins per key).
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
}

impl UnifiedManifest {
    /// Effective env for a target: `[env]` shallow-merged with `[deploy.env]`,
    /// the deploy-level value winning per key (contract D6).
    #[must_use]
    pub fn effective_env(&self, target: &DeployTarget) -> HashMap<String, String> {
        let mut out = self.env.clone();
        if let Some(over) = &target.env {
            for (k, v) in over {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }
}

impl UnifiedManifest {
    /// The build kind for this app (`[build].kind`). Decides whether the builder
    /// runs the docker pipeline (contract: build kind, NOT `runtime.type`, drives
    /// the build path).
    #[must_use]
    pub fn effective_build_kind(&self) -> BuildKind {
        self.build.kind
    }

    /// The resolved Dockerfile path (`[build].dockerfile`, default `Dockerfile`).
    #[must_use]
    pub fn dockerfile(&self) -> &str {
        self.build.dockerfile.as_deref().unwrap_or("Dockerfile")
    }

    /// The OPTIONAL stable "moving" tag (`[build].stable_tag`) the build
    /// publishes alongside the immutable `:<sha>` artifact tag. Trimmed;
    /// empty / whitespace-only ⇒ `None` (treated as unset, so a blank value can
    /// never push a `:""` tag).
    #[must_use]
    pub fn stable_tag(&self) -> Option<&str> {
        self.build
            .stable_tag
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// The resolved build context dir (`[build].context`, default `.`).
    #[must_use]
    pub fn context(&self) -> &str {
        &self.build.context
    }

    /// Translate this `tabbify.toml` into the legacy `AppManifest` body uploaded
    /// to S3 and parsed by the supervisor. Single-runtime model: the runtime is
    /// always Firecracker-from-OCI-image, so there is no override to thread.
    #[must_use]
    pub fn derive_app_manifest(&self) -> AppManifest {
        let mode = lifecycle_mode_from_str(&self.runtime.lifecycle);
        AppManifest {
            app: AppMeta {
                id: self.app.id,
                name: self.app.name.clone(),
                version: String::new(),
                kind: "headless".to_owned(),
                description: self.app.description.clone(),
            },
            lifecycle: Lifecycle {
                mode,
                idle_timeout_sec: self.runtime.idle_timeout_sec,
            },
            runtime: ManifestRuntime {
                // Single-runtime model: always Firecracker, built from a Dockerfile.
                r#type: Runtime::Firecracker.as_wire().to_owned(),
                entry: default_entry().to_owned(),
                // `fuel_per_request` is inert (was WASM-only); carry the legacy default.
                fuel_per_request: 1_000_000_000,
                memory_mb: self.runtime.memory_mb,
                vcpus: Some(self.runtime.vcpus),
                port: self.runtime.port,
                kernel: None,
                registry_ref: None,
                // Carry the persistent-disk intent ACROSS the unified→app-manifest
                // conversion (single source of truth = the unified `[runtime]`),
                // so the serving boot path can attach `/dev/vdb` + suppress
                // snapshots for a stateful app.
                stateful: self.runtime.stateful,
                data_mount: self.runtime.data_mount.clone(),
            },
            routes: Routes {
                dynamic_prefixes: self.routes.dynamic_prefixes.clone(),
            },
        }
    }
}

/// The build entry is ALWAYS a `Dockerfile` (single-runtime model: every app
/// builds an OCI image).
fn default_entry() -> &'static str {
    "Dockerfile"
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const CANONICAL: &str = r#"
[app]
id          = "0191e7c2-1111-7222-8333-444455556666"
name        = "my-app"
description = "demo"

[build]
kind       = "docker"
context    = "."
dockerfile = "Dockerfile"
builder    = "ec2-prod"

[runtime]
type             = "docker"
lifecycle        = "on_request"
idle_timeout_sec = 300
memory_mb        = 512
vcpus            = 1

[routes]
dynamic_prefixes = ["/"]

[[deploy]]
supervisor = "thinkpad"
runtime    = "firecracker"
[deploy.env]
LOG_LEVEL = "debug"

[[deploy]]
supervisor = "ec2-prod"
runtime    = "docker"

[env]
RUST_LOG = "info"
"#;

    #[test]
    fn parses_canonical_unified_manifest() {
        // CANONICAL still carries a LEGACY `[runtime] type` + per-target `runtime`.
        // Single-runtime model: those keys are ignored, not errors.
        let m: UnifiedManifest = toml::from_str(CANONICAL).unwrap();
        assert_eq!(m.app.name, "my-app");
        assert_eq!(m.build.kind, BuildKind::Docker);
        assert_eq!(m.build.builder.as_deref(), Some("ec2-prod"));
        assert_eq!(m.runtime.lifecycle, "on_request");
        assert_eq!(m.runtime.idle_timeout_sec, 300);
        // The Task-8 canary: the canonical example resolves memory_mb == 512.
        assert_eq!(m.runtime.memory_mb, 512);
        assert_eq!(m.deploy.len(), 2);
        assert_eq!(m.deploy[0].supervisor, "thinkpad");
        assert_eq!(m.deploy[1].supervisor, "ec2-prod");
        assert_eq!(m.env.get("RUST_LOG").map(String::as_str), Some("info"));
    }

    #[test]
    fn defaults_apply_for_minimal_manifest() {
        let src = r#"
[app]
name = "minimal"

[build]
kind = "docker"
"#;
        let m: UnifiedManifest = toml::from_str(src).unwrap();
        assert_eq!(m.build.kind, BuildKind::Docker);
        assert_eq!(m.build.context, ".");
        assert_eq!(m.runtime.lifecycle, "on_request");
        assert_eq!(m.runtime.idle_timeout_sec, 300);
        assert_eq!(m.runtime.memory_mb, 512);
        assert_eq!(m.runtime.vcpus, 1);
        assert!(m.deploy.is_empty()); // fallback D2 — handled downstream
        assert!(m.env.is_empty());
    }

    #[test]
    fn runtime_section_is_optional() {
        // Single-runtime model: no `[runtime]` table needed at all.
        let src = r#"
[app]
name = "no-runtime-table"

[build]
kind = "docker"
"#;
        let m: UnifiedManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.lifecycle, "on_request");
        assert_eq!(m.runtime.memory_mb, 512);
        assert_eq!(m.runtime.vcpus, 1);
    }

    #[test]
    fn deploy_env_overrides_top_level_env_per_key() {
        // D6: shallow merge, [deploy.env] wins per key.
        let m: UnifiedManifest = toml::from_str(CANONICAL).unwrap();
        let eff = m.effective_env(&m.deploy[0]);
        assert_eq!(eff.get("LOG_LEVEL").map(String::as_str), Some("debug")); // from deploy.env
        assert_eq!(eff.get("RUST_LOG").map(String::as_str), Some("info")); // from top-level
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        // D8: no deny_unknown_fields.
        let src = r#"
[app]
name = "fc"
future_app_field = "ignored"

[build]
kind = "docker"
some_future_build_field = 7

[runtime]
type = "docker"
"#;
        let m: UnifiedManifest = toml::from_str(src).unwrap();
        assert_eq!(m.app.name, "fc");
    }

    /// FROZEN `build.kind` wire test: the `"docker"` wire string round-trips and
    /// is the default (mirrors the cli's frozen-wire guard).
    #[test]
    fn build_kind_wire_is_frozen_docker() {
        let docker: UnifiedManifest = toml::from_str(CANONICAL).unwrap();
        assert_eq!(docker.effective_build_kind(), BuildKind::Docker);
        // Default (absent) resolves to Docker too.
        let minimal: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
"#,
        )
        .unwrap();
        assert_eq!(minimal.build.kind, BuildKind::Docker);
    }

    /// `[build].dockerfile` / `[build].context` resolve with sane defaults and
    /// honor explicit overrides.
    #[test]
    fn build_dockerfile_and_context_resolve() {
        let default: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
kind = "docker"
"#,
        )
        .unwrap();
        assert_eq!(default.dockerfile(), "Dockerfile");
        assert_eq!(default.context(), ".");

        let custom: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
kind = "docker"
context = "service"
dockerfile = "deploy/Dockerfile"
"#,
        )
        .unwrap();
        assert_eq!(custom.dockerfile(), "deploy/Dockerfile");
        assert_eq!(custom.context(), "service");
    }

    /// `[build].stable_tag` parses to `Some` when set, `None` when absent, and
    /// collapses a blank / whitespace-only value to `None` (never a `:""` tag).
    #[test]
    fn build_stable_tag_resolves() {
        let none: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
kind = "docker"
"#,
        )
        .unwrap();
        assert_eq!(none.stable_tag(), None, "absent ⇒ None (unchanged)");

        let set: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
kind = "docker"
stable_tag = "current"
"#,
        )
        .unwrap();
        assert_eq!(set.stable_tag(), Some("current"));

        let blank: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "x"
[build]
stable_tag = "   "
"#,
        )
        .unwrap();
        assert_eq!(blank.stable_tag(), None, "whitespace ⇒ None (no :\"\" tag)");
    }

    /// `[runtime].stateful` + `[runtime].data_mount` parse correctly, and both
    /// default to `false` / `None` when absent (backward-compatible serde defaults).
    #[test]
    fn runtime_stateful_and_data_mount_parse() {
        // Present: both fields explicit.
        let with_fields: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "forge"
[build]
kind = "docker"
[runtime]
stateful   = true
data_mount = "/var/lib/tabbify-forge"
"#,
        )
        .unwrap();
        assert!(with_fields.runtime.stateful);
        assert_eq!(
            with_fields.runtime.data_mount.as_deref(),
            Some("/var/lib/tabbify-forge")
        );

        // Absent: serde defaults kick in — old manifests parse unchanged.
        let without_fields: UnifiedManifest = toml::from_str(
            r#"
[app]
name = "stateless"
[build]
kind = "docker"
[runtime]
lifecycle = "always_on"
"#,
        )
        .unwrap();
        assert!(!without_fields.runtime.stateful);
        assert_eq!(without_fields.runtime.data_mount, None);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod derive_tests {
    use super::*;
    use crate::manifest::LifecycleMode;
    use crate::runner::build::BuildKind;

    const DOCKER_TOML: &str = r#"
[app]
id          = "0191e7c2-1111-7222-8333-444455556666"
name        = "my-app"
description = "demo"

[build]
kind       = "docker"
context    = "."
dockerfile = "Dockerfile"

[runtime]
lifecycle        = "on_request"
idle_timeout_sec = 300
memory_mb        = 512
vcpus            = 1

[[deploy]]
supervisor = "thinkpad"

[[deploy]]
supervisor = "ec2-prod"

[env]
RUST_LOG = "info"
"#;

    #[test]
    fn effective_build_kind_is_always_docker() {
        let docker: UnifiedManifest = toml::from_str(DOCKER_TOML).unwrap();
        assert_eq!(docker.effective_build_kind(), BuildKind::Docker);
    }

    #[test]
    fn derive_app_manifest_maps_core_fields() {
        let m: UnifiedManifest = toml::from_str(DOCKER_TOML).unwrap();
        // Single-runtime model: AppManifest runtime.type is ALWAYS "firecracker",
        // and the build entry is ALWAYS "Dockerfile".
        let am = m.derive_app_manifest();
        assert_eq!(am.app.name, "my-app");
        assert_eq!(am.app.description, "demo");
        assert_eq!(am.app.id, m.app.id);
        assert_eq!(am.lifecycle.mode, LifecycleMode::OnRequest);
        assert_eq!(am.lifecycle.idle_timeout_sec, 300);
        assert_eq!(am.runtime.r#type, "firecracker");
        assert_eq!(am.runtime.entry, "Dockerfile");
        assert_eq!(am.runtime.memory_mb, 512);
        assert_eq!(am.runtime.vcpus, Some(1));
    }

    /// GOLDEN: a LEGACY `tabbify.toml` that still declares `[runtime] type = "docker"`
    /// (and a per-target `runtime = "..."`) must NOT error, and the derived
    /// `AppManifest` must resolve to the single runtime: `firecracker`.
    #[test]
    fn legacy_runtime_type_docker_parses_and_resolves_to_firecracker() {
        let legacy = r#"
[app]
name = "legacy-docker"

[build]
kind = "docker"

[runtime]
type             = "docker"
lifecycle        = "on_request"
memory_mb        = 256

[[deploy]]
supervisor = "thinkpad"
runtime    = "docker"
"#;
        let m: UnifiedManifest =
            toml::from_str(legacy).expect("legacy [runtime] type must parse (ignored, not error)");
        // Resource limits are still honoured.
        assert_eq!(m.runtime.memory_mb, 256);
        // The selection field is gone; the runtime is always firecracker.
        let am = m.derive_app_manifest();
        assert_eq!(am.runtime.r#type, "firecracker");
        assert_eq!(am.runtime.entry, "Dockerfile");
        assert_eq!(am.runtime.memory_mb, 256);
    }

    #[test]
    fn derive_app_manifest_lifecycle_always_on_maps() {
        let src = r#"
[app]
name = "svc"
[build]
kind = "docker"
[runtime]
lifecycle = "always_on"
"#;
        let m: UnifiedManifest = toml::from_str(src).unwrap();
        let am = m.derive_app_manifest();
        assert_eq!(am.lifecycle.mode, LifecycleMode::AlwaysOn);
    }

    /// CANONICAL lifecycle fallback (single source of truth shared with
    /// `crate::build::fetched_from_ref`): `on_request` ⇒ OnRequest; `always_on`
    /// AND any UNKNOWN value ⇒ AlwaysOn (the FC live-path default).
    #[test]
    fn lifecycle_mode_fallback_is_consistent() {
        assert_eq!(
            lifecycle_mode_from_str("on_request"),
            LifecycleMode::OnRequest
        );
        assert_eq!(
            lifecycle_mode_from_str("always_on"),
            LifecycleMode::AlwaysOn
        );
        // Unknown → AlwaysOn (NOT OnRequest): aligns derive_app_manifest with the
        // connect-repo synthesis so an unknown lifecycle never silently lazy-starts.
        assert_eq!(lifecycle_mode_from_str("weird"), LifecycleMode::AlwaysOn);
        assert_eq!(lifecycle_mode_from_str(""), LifecycleMode::AlwaysOn);
    }
}
