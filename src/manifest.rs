//! `manifest.toml` schema — canonical, vendored IDENTICALLY in `tabbify-cli`
//! and `tabbify-service-supervisor` (contract §3).
//!
//! Derived from substrate `tabbify-app-manifest`, simplified to the Phase-1
//! lifecycle vocabulary. Do NOT add `deny_unknown_fields` (forward-compat).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Top-level app manifest as stored at `apps/<uuid>/v<N>/manifest.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppManifest {
    /// App metadata (id, name, kind, …).
    pub app: AppMeta,
    /// Lifecycle policy (always-on vs on-request).
    pub lifecycle: Lifecycle,
    /// Runtime parameters (wasm entry, fuel, memory).
    pub runtime: Runtime,
    /// Routing hints (Phase-1: dynamic prefixes).
    #[serde(default)]
    pub routes: Routes,
}

/// `[app]` table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppMeta {
    /// Optional in source; `tcli` stamps it before upload.
    #[serde(default)]
    pub id: Option<Uuid>,
    /// Display name.
    pub name: String,
    /// Display-only version string; S3 `latest` is authoritative.
    #[serde(default)]
    pub version: String,
    /// Free-form kind ("headless" | "widget" | …).
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}
fn default_kind() -> String {
    "headless".into()
}

/// `[lifecycle]` table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Lifecycle {
    /// Spawn policy.
    pub mode: LifecycleMode,
    /// Idle timeout (seconds) used by `on_request` to stop idle instances.
    #[serde(default = "default_idle")]
    pub idle_timeout_sec: u64,
}
fn default_idle() -> u64 {
    300
}

/// How the supervisor schedules an app's instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleMode {
    /// Spawn on deploy/registration, keep running.
    AlwaysOn,
    /// Lazy spawn on first request, stop after `idle_timeout_sec`.
    OnRequest,
}

/// `[runtime]` table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Runtime {
    /// Runtime type: `"wasm-http"` (any host), `"firecracker"` (Linux + KVM),
    /// or `"docker"` (any host with a reachable Docker daemon).
    #[serde(rename = "type", default = "default_rt")]
    pub r#type: String,
    /// Entry filename: the wasm component for `wasm-http`, the rootfs image
    /// (e.g. `rootfs.ext4`) for `firecracker`, the build-context tarball (e.g.
    /// `context.tar.gz`, the app dir + `Dockerfile`) for `docker`.
    #[serde(default = "default_entry")]
    pub entry: String,
    /// Per-request fuel budget (wasm-http only).
    #[serde(default = "default_fuel")]
    pub fuel_per_request: u64,
    /// Memory cap (MB). Advisory for wasm-http; the guest RAM size for
    /// firecracker.
    #[serde(default = "default_mem")]
    pub memory_mb: u32,
    /// Guest kernel image path/name (firecracker only). `None` → the
    /// supervisor's configured default kernel.
    #[serde(default)]
    pub kernel: Option<String>,
    /// Optional OCI image ref to pull instead of building from source (docker
    /// only), e.g. `"[fd5a:1f02:aaaa::1]:5000/acme/app:<sha>"`. When set, the
    /// runtime pulls the image from the mesh OCI registry and tags it locally.
    /// The host docker daemon must list the registry under `insecure-registries`.
    #[serde(default)]
    pub registry_ref: Option<String>,
}
fn default_rt() -> String {
    "wasm-http".into()
}
fn default_entry() -> String {
    "app.wasm".into()
}
fn default_fuel() -> u64 {
    1_000_000_000
}
fn default_mem() -> u32 {
    64
}

/// `[routes]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Routes {
    /// Phase-1: `["/"]` = all paths go to wasm.
    #[serde(default)]
    pub dynamic_prefixes: Vec<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The §3 canonical example must parse with the expected typed values.
    #[test]
    fn parses_canonical_manifest() {
        let src = r#"
[app]
name        = "hello-tabbify"
kind        = "headless"
description = "Phase-1 hello-world WASI-HTTP component"

[lifecycle]
mode             = "on_request"
idle_timeout_sec = 300

[runtime]
type             = "wasm-http"
entry            = "app.wasm"
fuel_per_request = 1000000000
memory_mb        = 64

[routes]
dynamic_prefixes = ["/"]
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.app.name, "hello-tabbify");
        assert_eq!(m.app.kind, "headless");
        assert!(m.app.id.is_none());
        assert_eq!(m.lifecycle.mode, LifecycleMode::OnRequest);
        assert_eq!(m.lifecycle.idle_timeout_sec, 300);
        assert_eq!(m.runtime.r#type, "wasm-http");
        assert_eq!(m.runtime.entry, "app.wasm");
        assert_eq!(m.runtime.fuel_per_request, 1_000_000_000);
        assert_eq!(m.runtime.memory_mb, 64);
        assert_eq!(m.routes.dynamic_prefixes, vec!["/".to_owned()]);
    }

    /// Defaults must apply when optional tables/fields are omitted.
    #[test]
    fn applies_defaults() {
        let src = r#"
[app]
name = "minimal"

[lifecycle]
mode = "always_on"

[runtime]
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.app.kind, "headless");
        assert_eq!(m.app.version, "");
        assert_eq!(m.lifecycle.mode, LifecycleMode::AlwaysOn);
        assert_eq!(m.lifecycle.idle_timeout_sec, 300);
        assert_eq!(m.runtime.r#type, "wasm-http");
        assert_eq!(m.runtime.entry, "app.wasm");
        assert_eq!(m.runtime.fuel_per_request, 1_000_000_000);
        assert_eq!(m.runtime.memory_mb, 64);
        assert!(m.routes.dynamic_prefixes.is_empty());
    }

    /// Unknown fields must be tolerated (forward-compat — no `deny_unknown_fields`).
    #[test]
    fn tolerates_unknown_fields() {
        let src = r#"
[app]
name = "future"
some_future_field = "ignored"

[lifecycle]
mode = "on_request"

[runtime]

[future_section]
whatever = 1
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.app.name, "future");
    }

    /// A firecracker-runtime manifest parses its type, the rootfs `entry`, and
    /// the optional `kernel` override.
    #[test]
    fn parses_firecracker_runtime_with_kernel() {
        let src = r#"
[app]
name = "vm-app"

[lifecycle]
mode = "always_on"

[runtime]
type      = "firecracker"
entry     = "rootfs.ext4"
kernel    = "vmlinux-6.1"
memory_mb = 512
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.r#type, "firecracker");
        assert_eq!(m.runtime.entry, "rootfs.ext4");
        assert_eq!(m.runtime.kernel.as_deref(), Some("vmlinux-6.1"));
        assert_eq!(m.runtime.memory_mb, 512);
    }

    /// A firecracker manifest without an explicit `kernel` leaves it `None`
    /// (supervisor falls back to its configured default kernel).
    #[test]
    fn firecracker_kernel_defaults_to_none() {
        let src = r#"
[app]
name = "vm-app"

[lifecycle]
mode = "always_on"

[runtime]
type  = "firecracker"
entry = "rootfs.ext4"
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.r#type, "firecracker");
        assert!(m.runtime.kernel.is_none());
    }

    /// A docker-runtime manifest parses its type + the build-context tarball as
    /// `entry`.
    #[test]
    fn parses_docker_runtime() {
        let src = r#"
[app]
name = "container-app"

[lifecycle]
mode = "always_on"

[runtime]
type  = "docker"
entry = "context.tar.gz"
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.r#type, "docker");
        assert_eq!(m.runtime.entry, "context.tar.gz");
        // docker uses no kernel.
        assert!(m.runtime.kernel.is_none());
    }

    /// The wasm-http default still leaves `kernel` `None`.
    #[test]
    fn wasm_runtime_has_no_kernel() {
        let src = r#"
[app]
name = "w"

[lifecycle]
mode = "on_request"

[runtime]
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.r#type, "wasm-http");
        assert!(m.runtime.kernel.is_none());
    }

    /// A stamped id must round-trip through (de)serialization.
    #[test]
    fn parses_stamped_id() {
        let src = r#"
[app]
id   = "0191e7c2-1111-7222-8333-444455556666"
name = "stamped"

[lifecycle]
mode = "on_request"

[runtime]
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(
            m.app.id,
            Some(Uuid::parse_str("0191e7c2-1111-7222-8333-444455556666").unwrap())
        );
    }

    /// A docker manifest with `registry_ref` set must parse and expose the ref.
    /// Without the field, `registry_ref` defaults to `None`.
    #[test]
    fn parses_docker_runtime_with_registry_ref() {
        let src = r#"
[app]
name = "registry-app"

[lifecycle]
mode = "always_on"

[runtime]
type         = "docker"
entry        = "context.tar.gz"
registry_ref = "[fd5a:1f02:aaaa::1]:5000/acme/app:sha256abc"
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert_eq!(m.runtime.r#type, "docker");
        assert_eq!(
            m.runtime.registry_ref.as_deref(),
            Some("[fd5a:1f02:aaaa::1]:5000/acme/app:sha256abc")
        );
    }

    /// When `registry_ref` is absent, the field defaults to `None`.
    #[test]
    fn docker_registry_ref_defaults_to_none() {
        let src = r#"
[app]
name = "no-ref-app"

[lifecycle]
mode = "always_on"

[runtime]
type  = "docker"
entry = "context.tar.gz"
"#;
        let m: AppManifest = toml::from_str(src).unwrap();
        assert!(
            m.runtime.registry_ref.is_none(),
            "registry_ref must default to None when absent"
        );
    }
}
