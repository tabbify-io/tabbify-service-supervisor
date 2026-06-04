//! Supervisor capability-tag computation (contract §4 / D7 — supervisor side).
//!
//! A supervisor advertises one mesh tag per *gated* runtime it can host so the
//! node/coordinator route an app of that runtime only to a capable supervisor.
//! `wasm-http` is implicit-always (NO tag, NEVER gated).

/// FROZEN tag strings (contract D4/D7). `firecracker` iff `/dev/kvm` is R/W;
/// `docker` iff the docker daemon is reachable; `builder` iff the operator
/// DESIGNATED this supervisor a build host (`SUPERVISOR_BUILDER` — an
/// explicit fleet-composition decision, never auto-detected). Order is
/// firecracker, docker, builder.
#[must_use]
pub fn capability_tags(kvm: bool, docker: bool, builder: bool) -> Vec<String> {
    let mut tags = Vec::new();
    if kvm {
        tags.push("firecracker".to_owned());
    }
    if docker {
        tags.push("docker".to_owned());
    }
    if builder {
        tags.push("builder".to_owned());
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FROZEN tag strings — if these change, the contract changed (D4/D7).
    #[test]
    fn capability_tag_strings_are_frozen() {
        assert_eq!(
            capability_tags(true, true, true),
            vec!["firecracker", "docker", "builder"]
        );
        assert_eq!(
            capability_tags(true, true, false),
            vec!["firecracker", "docker"]
        );
        assert_eq!(capability_tags(true, false, false), vec!["firecracker"]);
        assert_eq!(capability_tags(false, true, false), vec!["docker"]);
        // A designated builder advertises the tag even while its docker
        // daemon is down (the daemon may come up later; selection is the
        // node's concern, runtime failure surfaces in the build status).
        assert_eq!(capability_tags(false, false, true), vec!["builder"]);
        assert!(
            capability_tags(false, false, false).is_empty(),
            "wasm-only supervisor advertises NO capability tag"
        );
    }
}
