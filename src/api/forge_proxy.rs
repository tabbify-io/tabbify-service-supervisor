//! Forge-proxy: the IPv4 tap-gateway path a workspace FC guest uses to reach the
//! in-mesh Forgejo.
//!
//! A workspace Firecracker guest is IPv4-only on a /30 tap — it has NO IPv6/mesh
//! route, so the forge's raw v6 mesh ULA (`http://[fd5a:…]:8730`, the value the
//! node passes as `TABBIFY_FORGE_URL`) is unreachable from inside the VM (`route
//! get <ula>` = "Network is unreachable"). This is the SAME problem the git-proxy
//! already solves for git (`git_proxy.rs`): expose an IPv4 listener on
//! `0.0.0.0:<port>` that the guest reaches via its tap default gateway (`host_ip`).
//!
//! Unlike the git-proxy (an HTTP reverse-proxy that injects `Authorization` and
//! rewrites the upstream), the forge-proxy needs NO application-layer work: the
//! in-FC broker already holds the forge-admin token (off-env cap-file
//! `/run/tabbify/caps/forge-admin.token`) and speaks plain HTTP directly to the
//! forge. So the host side is a TRANSPARENT L4 forward
//! (`crate::tcp_forward::spawn_forwarder`): `host_ip:FORGE_PROXY_IPV4_PORT` →
//! forge-ULA:8730. The forge target ULA is host-global configuration
//! (`--forge-mesh-url` / `TABBIFY_FORGE_MESH_URL`), never hardcoded.
//!
//! SECURITY: mirrors the git-proxy posture EXACTLY. `0.0.0.0:FORGE_PROXY_IPV4_PORT`
//! is also reachable on the WiFi uplink, so `setup_forge_proxy_firewall`
//! (`firecracker/linux.rs`) DROPs inbound on the uplink and ACCEPTs only from the
//! FC tap subnet, installed BEFORE the listener accepts. The mesh ACL is the real
//! gate (the forge only answers peers its ACL permits); the firewall is
//! depth-in-defence, best-effort, and logged when absent.

/// IPv4 port the forge proxy binds on `0.0.0.0` so FC guests can reach it via
/// their tap gateway (`host_ip`). 8789 is Tabbify-internal and sits immediately
/// after the git-proxy's [`super::GIT_PROXY_IPV4_PORT`] (8788); not IANA-assigned
/// to any conflicting service. Shared by the listener bind (main.rs), the
/// firewall install, and the [`forge_proxy_gateway_url`] rewrite so the three
/// never drift.
pub const FORGE_PROXY_IPV4_PORT: u16 = 8789;

/// Build the guest-facing `TABBIFY_FORGE_URL` value: the FC's OWN tap gateway
/// (`host_ip`, its default route) on [`FORGE_PROXY_IPV4_PORT`]. The supervisor
/// injects THIS in place of the raw v6 mesh ULA the node supplies, so the in-FC
/// broker dials a route the IPv4-only guest can actually reach. Always plain
/// `http://` — the tap hop is unencrypted L4 to the host, which then forwards to
/// the forge over the WireGuard overlay.
#[must_use]
pub fn forge_proxy_gateway_url(host_ip: &str) -> String {
    format!("http://{host_ip}:{FORGE_PROXY_IPV4_PORT}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The gateway URL points at the guest's tap `host_ip` on the forge-proxy
    /// port — the exact value that replaces the node's v6 ULA in the FC env.
    #[test]
    fn gateway_url_is_host_ip_on_forge_proxy_port() {
        assert_eq!(
            forge_proxy_gateway_url("172.31.14.61"),
            "http://172.31.14.61:8789",
        );
    }

    /// The port is stable + distinct from the git-proxy's (they bind the same
    /// `0.0.0.0` host, so a collision would EADDRINUSE one of them).
    #[test]
    fn forge_proxy_port_is_distinct_from_git_proxy_port() {
        assert_eq!(FORGE_PROXY_IPV4_PORT, 8789);
        assert_ne!(
            FORGE_PROXY_IPV4_PORT,
            super::super::GIT_PROXY_IPV4_PORT,
            "forge + git IPv4 proxies bind the same host — ports must differ",
        );
    }
}
