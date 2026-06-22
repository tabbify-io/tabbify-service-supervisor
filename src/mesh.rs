//! Mesh membership wiring (contract §5).
//!
//! Wraps [`mesh_joiner::Joiner::join`] with the supervisor's tag + the prod
//! coordinator default, surfaces the assigned peer-ULA + peer id used to bind
//! the CONTROL listener, and hands out an [`Arc<dyn MeshHost>`] the per-app-ULA
//! hosting layer ([`crate::host::AppHost`]) uses to route app-ULAs.

use std::{net::Ipv6Addr, path::Path, sync::Arc};

use anyhow::Context;
use mesh_joiner::{JoinConfig, Joiner, JoinerError};

use crate::host::MeshHost;
use crate::mesh_command::sink::SupervisorCommandSink;

/// Env var carrying the super-admin Ed25519 pubkey as 64-char hex (Track C).
/// Unset / malformed → remote commands disabled (fail-closed).
pub const SUPER_ADMIN_PUBKEY_ENV: &str = "TABBIFY_MESH_SUPER_ADMIN_PUBKEY";

/// The systemd unit a `RestartJoiner` verb restarts (the supervisor's own unit).
pub const SUPERVISOR_UNIT: &str = "tabbify-supervisor";

/// Filename of the Track-C executed-nonce replay-guard sidecar under `data_dir`.
pub const COMMAND_NONCE_FILENAME: &str = "mesh-command-nonces.json";

/// Parse a 32-byte Ed25519 pubkey from optional 64-char hex (Track C). `None` on
/// absent / malformed / wrong-length input — the caller then disables remote
/// commands (fail-closed: a wedged pubkey can never become an open door).
#[must_use]
pub fn parse_super_admin_pubkey(hex_opt: Option<&str>) -> Option<[u8; 32]> {
    let raw = hex::decode(hex_opt?.trim()).ok()?;
    raw.as_slice().try_into().ok()
}

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
    /// Node-join JWT (Phase-2 contract §join-token). Sent to the coordinator as
    /// `Authorization: Bearer <token>` on register so a validating coordinator
    /// derives the peer's authoritative `network` + `tags` from the claims.
    /// `None` (the default) keeps today's behavior — only works against a
    /// coordinator started without `AUTH_URL` (the dev/E1 escape hatch). When
    /// `None`, [`build_supervisor_join_config`] falls back to the
    /// `TABBIFY_JOIN_TOKEN` environment variable so infra peers (supervisor /
    /// node / registry joiners) pick the token up from their env transparently.
    pub join_token: Option<String>,
}

/// Environment variable infra peers read their node-join token from (Phase-2
/// contract). Empty / unset = no token (current, backward-compatible behavior).
pub const JOIN_TOKEN_ENV: &str = "TABBIFY_JOIN_TOKEN";

/// Resolve the supervisor's node-join token: an explicit
/// [`JoinMetadata::join_token`] wins; otherwise fall back to the
/// `TABBIFY_JOIN_TOKEN` env (Phase-2 contract). A present-but-empty env value
/// is treated as absent so an `export TABBIFY_JOIN_TOKEN=` does not send a blank
/// bearer.
fn resolve_join_token(metadata_token: Option<String>) -> Option<String> {
    metadata_token.or_else(|| {
        std::env::var(JOIN_TOKEN_ENV)
            .ok()
            .filter(|t| !t.trim().is_empty())
    })
}

/// Operator-facing guidance appended to a coordinator 401 (expired / revoked
/// join token). This is the EXACT message the 2026-06-22 MSI brick lacked: the
/// supervisor exited with a bare `coordinator http status 401: "join token
/// invalid or revoked"`, giving no hint that the fix is re-minting the token in
/// `/etc/tabbify/supervisor.env`. Kept as a `const` so the wording is testable.
pub const JOIN_401_GUIDANCE: &str = "join rejected (HTTP 401): the join token is expired or revoked — re-mint via admin 'Add a node' and update /etc/tabbify/supervisor.env (TABBIFY_JOIN_TOKEN=<jwt>), then `systemctl restart tabbify-supervisor`";

/// Wrap a failed `Joiner::join` so an authentication failure is self-explaining.
///
/// BUG 4 (defensive): `Joiner::join` surfaces a coordinator rejection as an
/// `anyhow` error wrapping [`JoinerError::HttpStatus`]. On a 401 the raw chain
/// is the opaque `coordinator http status 401: "join token invalid or revoked"`
/// — true but unactionable. We DOWNCAST the error chain to [`JoinerError`] and,
/// only on a 401, append [`JOIN_401_GUIDANCE`] so the next person sees the cause
/// (and the fix) in one second. Any other failure (transport, TUN, 4xx/5xx) is
/// passed through with just the `ctx` it already had — we never mislabel a
/// non-auth failure as a token problem.
fn enrich_join_error(err: anyhow::Error, ctx: &'static str) -> anyhow::Error {
    let is_401 = err
        .chain()
        .filter_map(|cause| cause.downcast_ref::<JoinerError>())
        .any(|je| matches!(je, JoinerError::HttpStatus { status: 401, .. }));
    if is_401 {
        err.context(JOIN_401_GUIDANCE).context(ctx)
    } else {
        err.context(ctx)
    }
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
    /// `relay_only` declares the peer has NO reachable direct endpoint (behind a
    /// NAT/firewall with the WG UDP port dropped, reachable ONLY over its
    /// outbound relay): when `true` the coordinator never advertises a reflexive
    /// direct endpoint for it nor emits a hole-punch directive for any pair
    /// involving it, so the handshake completes single-sided over the relay.
    /// `false` keeps direct + hole-punch traversal.
    ///
    /// `advertise_endpoint` overrides the endpoint the coordinator advertises to
    /// other peers for this node. When `Some`, the coordinator uses this value
    /// verbatim (e.g. `10.17.21.133:51820`) instead of the reflexive (public)
    /// endpoint it observes on the incoming UDP register — useful for LAN-local
    /// peers that share a NAT and want to hole-punch each other directly over
    /// the LAN. `None` (the default) keeps reflexive-endpoint behavior,
    /// unchanged for cloud/public peers (`TABBIFY_MESH_ADVERTISE_ENDPOINT`).
    ///
    /// # Errors
    /// Propagates the broad `Joiner::join` failure surface (HTTP, TUN setup,
    /// UDP bind, sudo). On a host without root / TUN this fails — callers that
    /// want to run without the mesh should use `--no-mesh` and skip this.
    #[allow(clippy::too_many_arguments)]
    pub async fn join(
        coordinator_url: &str,
        display_name: &str,
        extra_tags: &[String],
        metadata: JoinMetadata,
        relay_url: Option<String>,
        relay_only: bool,
        advertise_endpoint: Option<String>,
        data_dir: &Path,
    ) -> anyhow::Result<Self> {
        let config = build_supervisor_join_config(
            coordinator_url,
            display_name,
            extra_tags,
            metadata,
            relay_url,
            relay_only,
            advertise_endpoint,
        );
        // Track C: resolve the super-admin pubkey (fail-closed when unset /
        // malformed) + build the production restart/reboot sink. A node without
        // `TABBIFY_MESH_SUPER_ADMIN_PUBKEY` set gets a fail-closed gate (every
        // signed command rejected), so remote restart is OFF by default.
        let super_admin_pubkey =
            parse_super_admin_pubkey(std::env::var(SUPER_ADMIN_PUBKEY_ENV).ok().as_deref());
        if super_admin_pubkey.is_some() {
            tracing::info!("Track C: super-admin pubkey configured — signed remote commands ENABLED");
        }
        let nonce_path = data_dir.join(COMMAND_NONCE_FILENAME);
        let sink: Arc<dyn mesh_joiner::coordinator::command_exec::CommandSink> =
            Arc::new(SupervisorCommandSink::new(data_dir, SUPERVISOR_UNIT));
        let joiner = Joiner::join_with_commands(
            config,
            super_admin_pubkey,
            Some(nonce_path),
            Some(sink),
        )
        .await
        // BUG 4: a coordinator 401 (expired/revoked join token) is enriched with
        // actionable recovery guidance instead of the opaque `http status 401`.
        .map_err(|e| enrich_join_error(e, "join mesh as supervisor"))?;
        Ok(Self::from_joiner(joiner))
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
    /// Used by the runner path (which does NOT wire Track-C remote commands —
    /// only the long-lived supervisor is a restart target).
    async fn from_config(config: JoinConfig, ctx: &'static str) -> anyhow::Result<Self> {
        // BUG 4: same 401-enrichment as the supervisor join path.
        let joiner = Joiner::join(config)
            .await
            .map_err(|e| enrich_join_error(e, ctx))?;
        Ok(Self::from_joiner(joiner))
    }

    /// Capture the assigned addressing from a freshly-joined [`Joiner`].
    fn from_joiner(joiner: Joiner) -> Self {
        let my_ula = joiner.my_ula();
        let peer_id = joiner.my_peer_id().to_string();
        Self {
            joiner: Arc::new(joiner),
            my_ula,
            peer_id,
        }
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

    /// Track K keystone: is this supervisor's WG data plane alive right now?
    /// Delegates to the joiner's `dataplane_healthy`. `false` ⇒ this node is a
    /// black hole (heartbeat alive, WG decap-RX dead). Read by the self-heal
    /// watchdog (Track B) and the OTA data-plane gate (Track D).
    #[must_use]
    pub fn dataplane_healthy(&self) -> bool {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
        self.joiner.dataplane_healthy(now_micros)
    }

    /// A cheap, `Send + Sync` data-plane probe closure for the OTA watchdog
    /// (Track D, D6): captures a clone of the joiner handle and evaluates
    /// [`Joiner::dataplane_healthy`] against the current clock on each call
    /// (self-clocked exactly like [`Self::dataplane_healthy`]). Used by `main`'s
    /// post-restart watchdog wiring to feed `live_local_observe`'s data-plane
    /// seam without coupling `main` to the joiner directly.
    #[must_use]
    pub fn data_plane_probe(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let joiner = Arc::clone(&self.joiner);
        Arc::new(move || {
            let now_micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
            joiner.dataplane_healthy(now_micros)
        })
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
    relay_only: bool,
    advertise_endpoint: Option<String>,
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
        // Node-join token: explicit metadata wins, else the `TABBIFY_JOIN_TOKEN`
        // env (Phase-2 contract). The coordinator validates it and derives the
        // peer's authoritative network + tags from the claims. `None` keeps the
        // current tokenless behavior (dev/E1 coordinator without `AUTH_URL`).
        join_token: resolve_join_token(metadata.join_token),
        // Explicit DERP-style relay endpoint (`TABBIFY_MESH_RELAY_URL`). `Some`
        // makes the joiner connect its relay over this url verbatim instead of
        // deriving `ws://` from the coordinator URL — the corporate-firewall
        // escape hatch (route the relay over `wss://`/443). `None` keeps the
        // default derivation, unchanged for AWS-side peers.
        relay_url,
        // Relay-only declaration (`TABBIFY_MESH_RELAY_ONLY`). When `true` the
        // coordinator never advertises a reflexive direct endpoint for this peer
        // and never emits a hole-punch directive for any pair involving it, so
        // the WG handshake completes single-sided over the relay — the fix for a
        // peer (this supervisor + its runners) that has no reachable direct
        // endpoint behind a NAT/firewall. `false` keeps direct + hole-punch.
        relay_only,
        // Explicit advertise endpoint (`TABBIFY_MESH_ADVERTISE_ENDPOINT`). When
        // `Some`, the coordinator advertises THIS endpoint to other peers instead
        // of the reflexive (public) one — enables same-LAN peers to hole-punch
        // each other directly (e.g. `10.17.21.133:51820`). `None` keeps the
        // default reflexive-endpoint behavior, unchanged for cloud/public peers.
        advertise_endpoint,
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
    use std::{net::Ipv6Addr, sync::Arc};

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

    /// Track C: the super-admin pubkey resolves from 64-char hex; wrong length
    /// or absence → `None` (fail-closed: remote commands disabled).
    #[test]
    fn resolve_super_admin_pubkey_from_hex_env() {
        // 32 bytes of 0xAB, hex-encoded.
        let hex = "ab".repeat(32);
        let pk = super::parse_super_admin_pubkey(Some(&hex));
        assert!(pk.is_some(), "valid 32-byte hex must resolve");
        assert_eq!(pk.unwrap(), [0xAB; 32]);

        // Wrong length → None (fail-closed: no remote commands).
        assert!(super::parse_super_admin_pubkey(Some("abcd")).is_none());
        // Absent → None.
        assert!(super::parse_super_admin_pubkey(None).is_none());
        // Non-hex → None.
        assert!(super::parse_super_admin_pubkey(Some(&"zz".repeat(32))).is_none());
    }

    /// BUG 4 (defensive): a coordinator 401 in the join error chain is enriched
    /// with actionable recovery guidance (re-mint the token + the canonical /etc
    /// env path), so the next operator sees the cause in one second instead of an
    /// opaque `http status 401`. Non-401 failures are passed through unchanged.
    #[test]
    fn enrich_join_error_appends_guidance_on_401_only() {
        // A 401 wrapped exactly as `Joiner::join` surfaces it.
        let raw_401: anyhow::Error = JoinerError::HttpStatus {
            status: 401,
            body: "join token invalid or revoked".to_owned(),
        }
        .into();
        let enriched = super::enrich_join_error(raw_401, "join mesh as supervisor");
        let rendered = format!("{enriched:#}");
        assert!(
            rendered.contains("/etc/tabbify/supervisor.env"),
            "401 must surface the canonical /etc token path: {rendered}"
        );
        assert!(
            rendered.contains("TABBIFY_JOIN_TOKEN"),
            "401 must name the token env var: {rendered}"
        );
        assert!(
            rendered.contains("join mesh as supervisor"),
            "the original context must be preserved: {rendered}"
        );

        // A NON-401 failure (e.g. 500) is passed through WITHOUT the token
        // guidance — we never mislabel a non-auth failure as a token problem.
        let raw_500: anyhow::Error = JoinerError::HttpStatus {
            status: 500,
            body: "coordinator boom".to_owned(),
        }
        .into();
        let passthrough = super::enrich_join_error(raw_500, "join mesh as supervisor");
        let rendered = format!("{passthrough:#}");
        assert!(
            !rendered.contains("/etc/tabbify/supervisor.env"),
            "a 500 must NOT get the token-401 guidance: {rendered}"
        );
        assert!(rendered.contains("join mesh as supervisor"));
    }

    /// The supervisor's software_version is wired onto `JoinConfig` so it rides
    /// to the coordinator roster — the fix for the `software_version=null` bug.
    #[test]
    fn build_config_wires_software_version_onto_join_config() {
        let md = JoinMetadata {
            software_version: Some("1.4.0".to_owned()),
            ..Default::default()
        };
        let config =
            build_supervisor_join_config("http://coord:8888", "node-1", &[], md, None, false, None);
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
            false,
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
            false,
            None,
        );
        assert_eq!(
            config.relay_url.as_deref(),
            Some("wss://relay.tabbify.io/v1/mesh/relay"),
            "explicit relay_url must ride onto JoinConfig verbatim"
        );
    }

    /// A `true` `relay_only` (from `TABBIFY_MESH_RELAY_ONLY`) rides onto the
    /// supervisor's `JoinConfig.relay_only`, so the coordinator never advertises a
    /// reflexive direct endpoint for it nor emits a hole-punch directive — the WG
    /// handshake completes single-sided over the relay (the fix for a peer with no
    /// reachable direct endpoint behind a NAT/firewall).
    #[test]
    fn build_config_wires_relay_only_onto_join_config() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
            true,
            None,
        );
        assert!(
            config.relay_only,
            "relay_only=true must ride onto JoinConfig"
        );
    }

    /// A `false` `relay_only` (the default) leaves `JoinConfig.relay_only` off, so
    /// the peer participates in direct + hole-punch traversal as before.
    #[test]
    fn build_config_omits_relay_only_when_false() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
            false,
            None,
        );
        assert!(
            !config.relay_only,
            "relay_only=false must keep direct + hole-punch traversal"
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
            false,
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

    /// An explicit `JoinMetadata::join_token` rides onto the supervisor's
    /// `JoinConfig.join_token` so a validating coordinator can authenticate the
    /// register and derive the peer's authoritative network + tags (Phase-2).
    #[test]
    fn build_config_wires_explicit_join_token() {
        let md = JoinMetadata {
            join_token: Some("infra-join-jwt".to_owned()),
            ..Default::default()
        };
        let config =
            build_supervisor_join_config("http://coord:8888", "node-1", &[], md, None, false, None);
        assert_eq!(
            config.join_token.as_deref(),
            Some("infra-join-jwt"),
            "explicit metadata join_token must ride onto JoinConfig"
        );
    }

    /// `resolve_join_token` prefers an explicit metadata token and otherwise
    /// reads the `TABBIFY_JOIN_TOKEN` env, treating a blank value as absent.
    /// (Env-var mutation is process-global, so all cases live in ONE test to
    /// avoid cross-test races.)
    #[test]
    fn resolve_join_token_prefers_metadata_then_env() {
        // SAFETY: single-threaded test body; we set + clear the env around the
        // assertions and restore the prior value at the end. `set_var` /
        // `remove_var` are `unsafe` since the 2024 edition.
        let prior = std::env::var(JOIN_TOKEN_ENV).ok();

        // No metadata + no env → None.
        unsafe { std::env::remove_var(JOIN_TOKEN_ENV) };
        assert_eq!(resolve_join_token(None), None);

        // Env set → falls back to env.
        unsafe { std::env::set_var(JOIN_TOKEN_ENV, "env-token") };
        assert_eq!(resolve_join_token(None).as_deref(), Some("env-token"));

        // Metadata wins over env.
        assert_eq!(
            resolve_join_token(Some("md-token".to_owned())).as_deref(),
            Some("md-token")
        );

        // Blank env is treated as absent (no blank bearer).
        unsafe { std::env::set_var(JOIN_TOKEN_ENV, "   ") };
        assert_eq!(resolve_join_token(None), None);

        // Restore prior env so other tests are unaffected.
        match prior {
            Some(v) => unsafe { std::env::set_var(JOIN_TOKEN_ENV, v) },
            None => unsafe { std::env::remove_var(JOIN_TOKEN_ENV) },
        }
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
            false,
            None,
        );
        assert!(
            config.relay_url.is_none(),
            "None relay_url must keep the default coordinator-derived relay"
        );
    }

    /// An explicit `advertise_endpoint` (from `TABBIFY_MESH_ADVERTISE_ENDPOINT`)
    /// rides onto the supervisor's `JoinConfig.advertise_endpoint` verbatim, so
    /// the coordinator advertises that endpoint to other peers instead of the
    /// reflexive one — enables same-LAN peers to hole-punch each other directly
    /// without going through the relay.
    #[test]
    fn build_config_wires_advertise_endpoint_onto_join_config() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
            false,
            Some("10.17.21.133:51820".to_owned()),
        );
        assert_eq!(
            config.advertise_endpoint.as_deref(),
            Some("10.17.21.133:51820"),
            "explicit advertise_endpoint must ride onto JoinConfig verbatim"
        );
    }

    /// A `None` `advertise_endpoint` (the default) leaves
    /// `JoinConfig.advertise_endpoint` `None`, so the coordinator uses the
    /// reflexive (public) endpoint it observes — unchanged behavior for
    /// cloud/public peers.
    #[test]
    fn build_config_omits_absent_advertise_endpoint() {
        let config = build_supervisor_join_config(
            "http://coord:8888",
            "node-1",
            &[],
            JoinMetadata::default(),
            None,
            false,
            None,
        );
        assert!(
            config.advertise_endpoint.is_none(),
            "None advertise_endpoint must keep reflexive-endpoint behavior"
        );
    }
}
