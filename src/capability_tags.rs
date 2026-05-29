//! Supervisor capability-tag computation (contract §4 / D7 — supervisor side).
//!
//! A supervisor advertises one mesh tag per *gated* runtime it can host so the
//! node/coordinator route an app of that runtime only to a capable supervisor.
//! `wasm-http` is implicit-always (NO tag, NEVER gated).

/// FROZEN tag strings (contract D4/D7). `firecracker` iff `/dev/kvm` is R/W;
/// `docker` iff the docker daemon is reachable; order is firecracker, docker.
#[must_use]
pub fn capability_tags(kvm: bool, docker: bool) -> Vec<String> {
    let mut tags = Vec::new();
    if kvm {
        tags.push("firecracker".to_owned());
    }
    if docker {
        tags.push("docker".to_owned());
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FROZEN tag strings — if these change, the contract changed (D4/D7).
    #[test]
    fn capability_tag_strings_are_frozen() {
        assert_eq!(capability_tags(true, true), vec!["firecracker", "docker"]);
        assert_eq!(capability_tags(true, false), vec!["firecracker"]);
        assert_eq!(capability_tags(false, true), vec!["docker"]);
        assert!(
            capability_tags(false, false).is_empty(),
            "wasm-only supervisor advertises NO capability tag"
        );
    }
}
