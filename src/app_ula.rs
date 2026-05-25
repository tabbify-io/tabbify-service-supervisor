//! Deterministic per-app ULA derivation — vendored IDENTICALLY in
//! `tabbify-service-supervisor` and `tabbify-service-node` (contract §4).
//!
//! NOTE for Phase-1: this is vendored + golden-tested for forward-compat but
//! NOT used for binding. The supervisor serves apps on its own peer-ULA
//! (contract §5). Per-app-ULA binding is a deferred optimization.

use std::net::Ipv6Addr;

use uuid::Uuid;

const APP_ULA_PREFIX_HI: u16 = 0xfd5a;
const APP_ULA_PREFIX_LO: u16 = 0x1f02;

/// Deterministic per-app ULA: `fd5a:1f02:<blake3(uuid)[0..6] as 3×u16 BE>::1`.
#[must_use]
pub fn derive_app_ula(app_uuid: Uuid) -> Ipv6Addr {
    let digest = blake3::hash(app_uuid.as_bytes());
    let b = digest.as_bytes();
    let h0 = u16::from_be_bytes([b[0], b[1]]);
    let h1 = u16::from_be_bytes([b[2], b[3]]);
    let h2 = u16::from_be_bytes([b[4], b[5]]);
    Ipv6Addr::new(APP_ULA_PREFIX_HI, APP_ULA_PREFIX_LO, h0, h1, h2, 0, 0, 1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Golden test (MUST pass identically in supervisor + node). The literal
    /// is the blake3 of those 16 bytes, pinned once.
    #[test]
    fn app_ula_is_stable() {
        let u = Uuid::parse_str("0191e7c2-1111-7222-8333-444455556666").unwrap();
        let ula = derive_app_ula(u);
        assert!(ula.to_string().starts_with("fd5a:1f02:"));
        assert_eq!(ula.to_string(), "fd5a:1f02:44a5:240b:121a::1");
    }

    /// Distinct UUIDs derive distinct ULAs (cheap collision sanity).
    #[test]
    fn distinct_uuids_distinct_ulas() {
        let a = derive_app_ula(Uuid::parse_str("0191e7c2-1111-7222-8333-444455556666").unwrap());
        let b = derive_app_ula(Uuid::parse_str("0191e7c2-2222-7222-8333-444455556666").unwrap());
        assert_ne!(a, b);
    }
}
