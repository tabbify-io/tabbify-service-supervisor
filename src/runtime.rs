//! Deploy-time runtime selection (the FROZEN wire enum) + re-exports of the
//! shared runtime seam.
//!
//! The [`AppRuntime`] trait and its value types live in [`crate::app_runtime`];
//! they are re-exported here so existing `crate::runtime::AppRuntime` /
//! `crate::runtime::RuntimeHealth` / `crate::runtime::BoxFut` import paths keep
//! working unchanged.

// Re-export the runtime seam so `crate::runtime::*` paths keep resolving.
pub use crate::app_runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

use serde::{Deserialize, Serialize};

// Deploy-time runtime selection enum — the FROZEN wire type from the contract.
//
// Distinct from `manifest::Runtime` (the `[runtime]` TABLE). This enum is the
// `runtime` field in `[runtime].type`, each `[[deploy]].runtime`, and the
// node→supervisor request body. Wire strings are FROZEN (contract D4):
// `"docker"` / `"firecracker"` / `"wasm-http"`. Vendor IDENTICALLY in
// cli / node / supervisor; every repo carries the golden round-trip test below.

/// How a runner EXECUTES the artifact, chosen at deploy time per target.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum Runtime {
    /// `docker run` the OCI image.
    Docker,
    /// Boot the OCI image as a Firecracker microVM.
    Firecracker,
    /// Legacy in-process wasm selection. The in-process WASM runtime has been
    /// removed (the platform serves a single FC-from-image runtime); this
    /// variant is retained ONLY to keep the wire string frozen until the enum
    /// is collapsed cross-repo in a later lockstep step.
    #[default]
    WasmHttp,
}

impl Runtime {
    /// The exact wire string (FROZEN, contract D4). Mirrors serde output.
    #[must_use]
    pub fn as_wire(self) -> &'static str {
        match self {
            Runtime::Docker => "docker",
            Runtime::Firecracker => "firecracker",
            Runtime::WasmHttp => "wasm-http",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// GOLDEN round-trip: the wire string for each variant is FROZEN (contract D4).
    /// If this test changes, the contract changed — coordinate all three repos.
    #[test]
    fn runtime_wire_strings_are_frozen() {
        for (variant, wire) in [
            (Runtime::Docker, "docker"),
            (Runtime::Firecracker, "firecracker"),
            (Runtime::WasmHttp, "wasm-http"),
        ] {
            // serialize → exact string
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{wire}\""), "serialize mismatch for {variant:?}");
            // deserialize ← exact string
            let back: Runtime = serde_json::from_str(&format!("\"{wire}\"")).unwrap();
            assert_eq!(back, variant, "deserialize mismatch for {wire}");
        }
    }

    #[test]
    fn runtime_default_is_wasm_http() {
        assert_eq!(Runtime::default(), Runtime::WasmHttp);
    }

    #[test]
    fn runtime_rejects_unknown_string() {
        assert!(serde_json::from_str::<Runtime>("\"podman\"").is_err());
    }
}
