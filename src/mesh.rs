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
}

impl MeshMembership {
    /// Join the mesh tagged `["supervisor"]` (plus any `extra_tags`, e.g.
    /// `"firecracker"` on a KVM-capable host) against `coordinator_url`,
    /// plaintext (no mTLS) per Phase-1. `metadata` forwards the per-app-runner
    /// join fields onto the [`JoinConfig`]; the supervisor passes
    /// [`JoinMetadata::default`] (all `None`) for now.
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
    ) -> anyhow::Result<Self> {
        let mut tags = vec!["supervisor".to_owned()];
        tags.extend_from_slice(extra_tags);
        let config = JoinConfig {
            coordinator_url: coordinator_url.to_owned(),
            display_name: display_name.to_owned(),
            tags,
            insecure_no_mtls: true,
            requested_ula: metadata.requested_ula,
            kind: metadata.kind,
            parent: metadata.parent,
            app_uuid: metadata.app_uuid,
            identity_path: metadata.identity_path,
            ..Default::default()
        };
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
}
