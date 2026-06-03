//! Deploy-time runtime selection (the FROZEN wire enum) + re-exports of the
//! shared runtime seam.
//!
//! The [`AppRuntime`] trait and its value types live in [`crate::app_runtime`];
//! they are re-exported here so existing `crate::runtime::AppRuntime` /
//! `crate::runtime::RuntimeHealth` / `crate::runtime::BoxFut` import paths keep
//! working unchanged.

// Re-export the runtime seam so `crate::runtime::*` paths keep resolving.
pub use crate::app_runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The app runtime. Tabbify runs ONE runtime: an OCI image booted as a
/// Firecracker microVM. The enum is retained with a single variant for
/// wire/back-compat — older clients, `tabbify.toml`s, and on-disk records may
/// still carry "docker"/"wasm-http"/"node-firecracker"; deserialize COERCES any
/// string to Firecracker rather than erroring. (contract D4: wire = "firecracker")
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, utoipa::ToSchema, schemars::JsonSchema)]
pub enum Runtime {
    /// Boot the OCI image as a Firecracker microVM. The only runtime Tabbify ships.
    #[default]
    Firecracker,
}

impl Runtime {
    /// The exact wire string (FROZEN, contract D4). Mirrors serde output.
    #[must_use]
    pub fn as_wire(self) -> &'static str {
        "firecracker"
    }
}

impl Serialize for Runtime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_wire())
    }
}

impl<'de> Deserialize<'de> for Runtime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Lenient: tolerate ANY legacy string ("docker"/"wasm-http"/
        // "node-firecracker"/…) and coerce to the single runtime.
        let _ = String::deserialize(deserializer)?;
        Ok(Runtime::Firecracker)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// GOLDEN round-trip: the wire string is FROZEN (contract D4) and deserialize
    /// LENIENTLY coerces any legacy string to Firecracker. If this test changes,
    /// the contract changed — coordinate all three repos.
    #[test]
    fn runtime_wire_strings_are_frozen() {
        // serialize → "firecracker"
        assert_eq!(Runtime::Firecracker.as_wire(), "firecracker");
        let json = serde_json::to_string(&Runtime::Firecracker).unwrap();
        assert_eq!(json, "\"firecracker\"");
        // round-trip
        let back: Runtime = serde_json::from_str("\"firecracker\"").unwrap();
        assert_eq!(back, Runtime::Firecracker);

        // LENIENT-COERCE: legacy wire strings all deserialize to Firecracker.
        for legacy in ["docker", "wasm-http", "node-firecracker"] {
            let back: Runtime = serde_json::from_str(&format!("\"{legacy}\"")).unwrap();
            assert_eq!(back, Runtime::Firecracker, "legacy string {legacy} must coerce");
        }
    }

    #[test]
    fn runtime_defaults_to_firecracker() {
        assert_eq!(Runtime::default(), Runtime::Firecracker);
    }
}
