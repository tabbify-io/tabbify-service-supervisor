//! Mesh membership wiring (contract §5).
//!
//! Wraps [`mesh_joiner::Joiner::join`] with the supervisor's tag + the prod
//! coordinator default, surfaces the assigned peer-ULA + peer id used to bind
//! the CONTROL listener, and hands out an [`Arc<dyn MeshHost>`] the per-app-ULA
//! hosting layer ([`crate::host::AppHost`]) uses to route app-ULAs.

use std::net::Ipv6Addr;
use std::sync::Arc;

use anyhow::Context;
use mesh_joiner::{JoinConfig, Joiner};

use crate::host::MeshHost;

/// A joined mesh membership: holds the [`Joiner`] (kept alive for the process
/// so the TUN device + WG background tasks stay up) and the addressing info the
/// control API server needs.
pub struct MeshMembership {
    joiner: Arc<Joiner>,
    my_ula: Ipv6Addr,
    peer_id: String,
}

/// Extra metadata a peer declares on its mesh join (per-app-runner arch).
///
/// All fields are forwarded onto the [`JoinConfig`]. The supervisor passes the
/// default (all `None`) until its sticky-identity join lands (Phase 2); the
/// runner builds its own config via
/// [`crate::runner::serve::build_runner_join_config`] and uses
/// [`MeshMembership::join_runner`] instead.
#[derive(Debug, Default, Clone)]
pub struct JoinMetadata {
    /// Explicit IPv6 ULA to claim from the coordinator (`requested_ula`).
    pub requested_ula: Option<String>,
    /// Peer role for the roster (`"runner"` for a per-app runner).
    pub kind: Option<String>,
    /// ULA of the owning supervisor (lets the node build the topology tree).
    pub parent: Option<String>,
    /// UUID of the app this peer serves.
    pub app_uuid: Option<String>,
    /// Path to a persistent identity file (`{private_key, ula}`).
    pub identity_path: Option<std::path::PathBuf>,
    /// The peer's running binary release version (`build.rs`-embedded). `None`
    /// means unknown and MUST never be treated as a downgrade trigger.
    pub software_version: Option<String>,
}

impl MeshMembership {
    /// Join the mesh tagged `["supervisor"]` (plus any `extra_tags`, e.g.
    /// `"firecracker"` on a KVM-capable host) against `coordinator_url`,
    /// plaintext (no mTLS) per Phase-1. `metadata` forwards the per-app-runner
    /// join fields onto the [`JoinConfig`]; the supervisor passes
    /// [`JoinMetadata::default`] (all `None`) for now.
    ///
    /// `relay_url` is the explicit DERP-style relay endpoint (from
    /// `TABBIFY_MESH_RELAY_URL`): when `Some`, the joiner connects its relay over
    /// that url verbatim (e.g. `wss://relay.tabbify.io/v1/mesh/relay`) instead of
    /// deriving `ws://` from the coordinator URL — required to reach the relay
    /// through corporate proxies/firewalls. `None` keeps the existing behavior.
    ///
    /// # Errors
    /// Propagates the broad `Joiner::join` failure surface (HTTP, TUN setup,
    /// UDP bind, sudo). On a host without root / TUN this fails — callers that
    /// want to run without the mesh should use `--no-mesh` and skip this.
    pub async fn join(
        coordinator_url: &str,
        display_name: &str,
        extra_tags: &[String],
        metadata: JoinMetadata,
        relay_url: Option<String>,
    ) -> anyhow::Result<Self> {
        let config = build_supervisor_join_config(
            coordinator_url,
            display_name,
            extra_tags,
            metadata,
            relay_url,
        );
        Self::from_config(config, "join mesh as supervisor").await
    }

    /// Join the mesh from a fully-built [`JoinConfig`] — used by the per-app
    /// runner, which constructs the config (claiming its app-ULA + declaring
    /// `kind = "runner"`, `parent`, `app_uuid`) via the pure, unit-tested
    /// [`crate::runner::serve::build_runner_join_config`] seam.
    ///
    /// # Errors
    /// Same broad `Joiner::join` failure surface as [`Self::join`].
    pub async fn join_runner(config: JoinConfig) -> anyhow::Result<Self> {
        Self::from_config(config, "join mesh as runner").await
    }

    /// Shared core: drive [`Joiner::join`] and capture the assigned addressing.
    async fn from_config(config: JoinConfig, ctx: &'static str) -> anyhow::Result<Self> {
        let joiner = Joiner::join(config).await.context(ctx)?;
        let my_ula = joiner.my_ula();
        let peer_id = joiner.my_peer_id().to_string();
        Ok(Self {
            joiner: Arc::new(joiner),
            my_ula,
            peer_id,
        })
    }

    /// Our assigned peer-ULA (bind the CONTROL listener on `[my_ula]:port`).
    #[must_use]
    pub const fn my_ula(&self) -> Ipv6Addr {
        self.my_ula
    }

    /// Our coordinator-assigned peer id (as a string for the API JSON).
    #[must_use]
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// A handle the [`crate::host::AppHost`] uses to host/unhost app-ULAs on the
    /// joiner. Cloned out of the shared joiner (which stays alive as long as the
    /// membership is held).
    #[must_use]
    pub fn mesh_host(&self) -> Arc<dyn MeshHost> {
        self.joiner.clone()
    }

    /// Advertise this runner's OWN peer-ULA as a hosted app-ULA on the joiner
    /// (FIX #9).
    ///
    /// A per-app runner joins claiming `requested_ula = derive_app_ula(uuid)`,
    /// so the coordinator routes that ULA straight to this peer and its OWN
    /// peer-ULA *is* the app-ULA — already reachable. But the runner binds it
    /// via [`crate::host::AppHost::mesh_self`] (no [`MeshHost`] joiner), so the
    /// joiner's locally-hosted set stays empty and the heartbeat carries an
    /// empty `hosted_app_ulas` — making `GET /v1/supervisors` report no hosted
    /// apps even though the app serves 200.
    ///
    /// This records `my_ula` in the joiner's hosted set so it rides every
    /// heartbeat. The underlying `host_app_ula` also re-asserts the `/128` TUN
    /// alias, which is idempotent for the peer's own ULA (already assigned on
    /// join), so the call is a harmless no-op on the interface side.
    ///
    /// # Errors
    /// Propagates a joiner `host_app_ula` failure (e.g. no TUN), so the caller
    /// can decide whether an un-advertised runner is fatal.
    pub async fn host_own_ula(&self) -> anyhow::Result<()> {
        let mesh = self.mesh_host();
        advertise_own_ula(&mesh, self.my_ula).await
    }
}

/// Advertise `my_ula` as a hosted app-ULA on the given mesh handle (FIX #9).
///
/// Pure seam over [`MeshHost::mesh_host_ula`] (the joiner's `host_app_ula`) so
/// the advertise-own-ULA behaviour is unit-testable with a fake `MeshHost` —
/// no real TUN, no live join. Used by [`MeshMembership::host_own_ula`].
///
/// # Errors
/// Propagates the joiner's `host_app_ula` failure surface.
pub async fn advertise_own_ula(mesh: &Arc<dyn MeshHost>, my_ula: Ipv6Addr) -> anyhow::Result<()> {
    mesh.mesh_host_ula(my_ula)
        .await
        .with_context(|| format!("joiner host_app_ula(own ULA {my_ula})"))
}

/// Build the supervisor's [`JoinConfig`] from its identity + per-app-runner
/// [`JoinMetadata`]. Pure (no I/O), so the field wiring — notably
/// `software_version` riding onto the wire — is unit-testable without joining
/// the mesh. Mirrors the runner's
/// [`crate::runner::serve::build_runner_join_config`] seam.
fn build_supervisor_join_config(
    coordinator_url: &str,
    display_name: &str,
    extra_tags: &[String],
    metadata: JoinMetadata,
    relay_url: Option<String>,
) -> JoinConfig {
    let mut tags = vec!["supervisor".to_owned()];
    tags.extend_from_slice(extra_tags);
    JoinConfig {
        coordinator_url: coordinator_url.to_owned(),
        display_name: display_name.to_owned(),
        tags,
        insecure_no_mtls: true,
        requested_ula: metadata.requested_ula,
        kind: metadata.kind,
        parent: metadata.parent,
        app_uuid: metadata.app_uuid,
        identity_path: metadata.identity_path,
        // Ride the host's binary version onto the wire: the coordinator
        // surfaces it as the roster `software_version` (spec P0 OBSERVE) so
        // version drift is visible. `None` stays back-compatible (the joiner
        // never invents a value).
        software_version: metadata.software_version,
        // Explicit DERP-style relay endpoint (`TABBIFY_MESH_RELAY_URL`). `Some`
        // makes the joiner connect its relay over this url verbatim instead of
        // deriving `ws://` from the coordinator URL — the corporate-firewall
        // escape hatch (route the relay over `wss://`/443). `None` keeps the
        // default derivation, unchanged for AWS-side peers.
        relay_url,
        // The supervisor is the HOST daemon: it owns the main-table routes
        // (no source-scoping — its per-app runners scope themselves), and
        // it keeps the host firewall from dropping inbound overlay dials
        // to its control listener (:8730), tailscaled-style. Best-effort:
        // a container without ip6tables only logs a warning.
        manage_firewall: true,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;
    use std::sync::Arc;

    use anyhow::Result;
    use dashmap::DashMap;

    use super::*;
    use crate::host::{BoxFut, MeshHost};

    /// A fake [`MeshHost`] that records the app-ULAs it was asked to host, so a
    /// test can assert the advertise-own-ULA call fires without a real TUN.
    #[derive(Default)]
    struct FakeMeshHost {
        hosted: DashMap<Ipv6Addr, ()>,
    }

    impl MeshHost for FakeMeshHost {
        fn tun_iface(&self) -> Option<String> {
            Some("utun-fake".to_owned())
        }
        fn mesh_host_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
            self.hosted.insert(app_ula, ());
            Box::pin(async { Ok(()) })
        }
        fn mesh_unhost_ula(&self, _app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// FIX #9: the per-app runner's OWN peer-ULA (== its app-ULA) must be
    /// advertised as a hosted app on the joiner so it rides every heartbeat —
    /// otherwise `GET /v1/supervisors` reports `hosted_app_ulas` empty even
    /// though the app serves 200. `advertise_own_ula` routes the runner's own
    /// ULA through the joiner's `host_app_ula` (idempotent re-assert of the
    /// already-assigned alias) and records it in the advertised set.
    #[tokio::test]
    async fn advertise_own_ula_hosts_the_runners_ula_on_the_joiner() {
        let fake: Arc<dyn MeshHost> = Arc::new(FakeMeshHost::default());
        let my_ula: Ipv6Addr = "fd5a:1f02:dead:beef:cafe::1".parse().unwrap();

        advertise_own_ula(&fake, my_ula)
            .await
            .expect("advertise own ula");

        // Downcast-free assertion: re-run against a concrete fake we keep.
        let concrete = Arc::new(FakeMeshHost::default());
        let dynh: Arc<dyn MeshHost> = concrete.clone();
        advertise_own_ula(&dynh, my_ula).await.expect("advertise");
        assert!(
            concrete.hosted.contains_key(&my_ula),
            "advertise_own_ula must host the runner's own ULA on the joiner"
        );
    }

    /// JoinMetadata carries the supervisor's software_version so it rides onto
    /// the mesh join (and every heartbeat) via `JoinConfig.software_version`.
    #[test]
    fn join_metadata_carries_software_version() {
        let md = JoinMetadata {
            software_version: Some("1.4.0".to_owned()),
            ..Default::default()
        };
        assert_eq!(md.software_version.as_deref(), Some("1.4.0"));
        // Default omits it (None = unknown, never a downgrade trigger).
        assert!(JoinMetadata::default().software_version.is_none());
    }

    /// The supervisor's software_version is wired onto `JoinConfig` so it rides
    /// to the coordinator roster — the fix for the `software_version=null` bug.
    #[test]
    fn build_config_wires_software_version_onto_join_config() {
        let md = JoinMetadata {
            software_version: Some("1.4.0".to_owned()),
            ..Default::default()
        };
        let config = build_supervisor_join_config("http://coord:8888", "node-1", &[], md, None);
        assert_eq!(config.software_version.as_deref(), Some("1.4.0"));
        // The supervisor tag is always present; metadata fields ride through.
        assert!(config.tags.contains(&"supervisor".to_owned()));
    }

    /// A `None` software_version stays `None` on the config (back-compat: the
    /// joiner never invents a value, so it is never a downgrade trigger).
    #[test]
    fn build_config_omits_absent_software_version() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &["firecracker".to_owned()],
            JoinMetadata::default(),
            None,
        );
        assert!(config.software_version.is_none());
        assert!(config.tags.contains(&"firecracker".to_owned()));
    }

    /// An explicit `relay_url` (from `TABBIFY_MESH_RELAY_URL`) rides onto the
    /// supervisor's `JoinConfig.relay_url` verbatim, so the joiner connects its
    /// relay over that url (the corporate-firewall escape hatch) instead of
    /// deriving `ws://` from the coordinator URL.
    #[test]
    fn build_config_wires_relay_url_onto_join_config() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            Some("wss://relay.tabbify.io/v1/mesh/relay".to_owned()),
        );
        assert_eq!(
            config.relay_url.as_deref(),
            Some("wss://relay.tabbify.io/v1/mesh/relay"),
            "explicit relay_url must ride onto JoinConfig verbatim"
        );
    }

    /// The supervisor is the host daemon: it manages the firewall trust for
    /// its own TUN (tailscaled-style, so inbound :8730 dials survive distro
    /// default firewalls) but does NOT source-scope its routes — it is the
    /// rightful owner of the main-table `/128`s; its per-app runners scope
    /// themselves instead.
    #[test]
    fn build_config_sets_host_integration_for_host_daemon() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
        );
        assert!(
            config.manage_firewall,
            "supervisor must manage the firewall"
        );
        assert!(
            !config.source_scoped_routes,
            "supervisor must keep main-table routes (runners scope themselves)"
        );
    }

    /// Absent `relay_url` (the default) leaves `JoinConfig.relay_url` `None`, so
    /// the joiner derives the relay endpoint from the coordinator URL as before.
    #[test]
    fn build_config_omits_absent_relay_url() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
        );
        assert!(
            config.relay_url.is_none(),
            "None relay_url must keep the default coordinator-derived relay"
        );
    }
}
