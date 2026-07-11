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

/// Request body for [`set_forge_proxy_target`]: the new forge upstream `ula`
/// (an infra-slot `fd5a:1f00:` ULA) and an optional `port` (defaults to the
/// contract [`tabbify_workspace_contract::FORGE_PORT`]).
#[derive(Debug, serde::Deserialize)]
pub struct ForgeTargetBody {
    /// The forge's mesh ULA — MUST be a `1f00` infra-slot address.
    pub ula: String,
    /// Optional forge port; defaults to the contract forge port.
    #[serde(default)]
    pub port: Option<u16>,
}

/// `POST /v1/forge-proxy/target` — hot-reroute the forge-proxy L4 forwarder to a
/// new upstream WITHOUT restarting the supervisor or re-baking any workspace.
///
/// Used during a forge host migration: the new host serves the SAME fixed infra
/// ULA (`FORGE_INFRA_ULA`), the coordinator reroutes the /128, and this endpoint
/// swaps the forwarder's upstream so in-flight guests keep reaching the forge.
///
/// Only a `1f00` infra-slot ULA is accepted — the forge is a non-ephemeral infra
/// service, and pointing the proxy at an ephemeral (`1f02`) app ULA would be a
/// misconfiguration (and the exact class of bug this indirection removes). The
/// swap is atomic ([`arc_swap::ArcSwap`]); each new connection reads the current
/// target at dial time.
///
/// Mesh-internal control seam (like the git proxy): gated by the same mesh ACL
/// as every other supervisor control route, so only permitted peers reach it.
///
/// # Errors
/// `400` if `ula` does not parse as an IPv6 address, or is not a `1f00` infra
/// ULA.
pub async fn set_forge_proxy_target(
    axum::extract::State(state): axum::extract::State<super::SharedState>,
    axum::Json(body): axum::Json<ForgeTargetBody>,
) -> Result<http::StatusCode, (http::StatusCode, String)> {
    let ula: std::net::Ipv6Addr = body.ula.parse().map_err(|_| {
        (
            http::StatusCode::BAD_REQUEST,
            format!("invalid ULA: {}", body.ula),
        )
    })?;
    // The forge is an infra service on the non-ephemeral `1f00` slot; refuse an
    // ephemeral (`1f02`) app ULA or any other prefix — pointing the proxy there
    // is precisely the misroute this indirection exists to prevent.
    if ula.segments()[1] != 0x1f00 {
        return Err((
            http::StatusCode::BAD_REQUEST,
            "target must be a 1f00 infra ULA".to_owned(),
        ));
    }
    let port = body.port.unwrap_or(tabbify_workspace_contract::FORGE_PORT);
    let new_target = std::net::SocketAddr::new(ula.into(), port);
    state.forge_target.store(std::sync::Arc::new(new_target));
    tracing::info!(%new_target, "forge-proxy upstream hot-swapped via control API");
    Ok(http::StatusCode::NO_CONTENT)
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
