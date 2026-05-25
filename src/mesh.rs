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

impl MeshMembership {
    /// Join the mesh tagged `["supervisor"]` against `coordinator_url`,
    /// plaintext (no mTLS) per Phase-1.
    ///
    /// # Errors
    /// Propagates the broad `Joiner::join` failure surface (HTTP, TUN setup,
    /// UDP bind, sudo). On a host without root / TUN this fails — callers that
    /// want to run without the mesh should use `--no-mesh` and skip this.
    pub async fn join(coordinator_url: &str, display_name: &str) -> anyhow::Result<Self> {
        let joiner = Joiner::join(JoinConfig {
            coordinator_url: coordinator_url.to_owned(),
            display_name: display_name.to_owned(),
            tags: vec!["supervisor".to_owned()],
            insecure_no_mtls: true,
            ..Default::default()
        })
        .await
        .context("join mesh as supervisor")?;

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
