//! Runner serve core — hosts exactly one app on its app-ULA.
//!
//! [`RunnerServe::start`] is the main entry point: given a [`ServeConfig`] it:
//! 1. creates an [`S3Fetcher`] and fetches the app artifact;
//! 2. derives the app-ULA via [`derive_app_ula`];
//! 3. builds the app runtime via the shared [`crate::build::build_runtime`];
//! 4. creates an [`AppHost`] (loopback when `no_mesh`; otherwise it joins the
//!    mesh claiming `requested_ula = derive_app_ula(uuid)` so the runner's OWN
//!    peer-ULA *is* the app-ULA, then binds `[my_ula]:port` directly) and hosts
//!    the app via [`AppHost::host`];
//! 5. wraps the live [`HostedApp`] in a [`RunnerServe`] that exposes the bound
//!    address — the test (and the binary) dial this to reach the app.
//!
//! The returned [`RunnerServe`] also exposes a [`RunnerServe::lifecycle`] handle
//! that the control server (Task 1.4) uses to share ownership of the live
//! listener, allowing `Stop`, `Purge`, and `Health` commands to operate on the
//! same `HostedApp`.
//!
//! # Mesh path
//! When `no_mesh = false` the runner joins the mesh as a `runner`-kind peer
//! ([`build_runner_join_config`] builds the [`mesh_joiner::JoinConfig`]),
//! claiming `requested_ula = derive_app_ula(uuid)` and declaring its
//! `parent` + `app_uuid`. Because the coordinator routes that ULA straight to
//! this peer, the runner binds its OWN ULA via [`AppHost::mesh_self`] — it does
//! NOT need the separate `host_app_ula` app-route layer for *routing* (that
//! advertised app-ULAs distinct from a peer's own ULA, used by the old
//! multi-app supervisor).
//!
//! It does, however, advertise that own ULA as a hosted app via
//! [`MeshMembership::host_own_ula`] so the joiner's heartbeat carries it in
//! `hosted_app_ulas` (FIX #9) — otherwise `GET /v1/supervisors` reports the
//! runner hosts nothing even though its app serves 200. The advertise call's
//! `/128` TUN alias re-assert is idempotent for the peer's already-assigned
//! own ULA.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use crate::{
    app_ula::derive_app_ula,
    build::build_runtime,
    config::{DockerConfig, FcConfig},
    fetcher::{FetchedApp, S3Fetcher},
    host::{AppHost, AppServe},
    mesh::MeshMembership,
    runner::{active::ActiveRuntime, control::RunnerLifecycle},
    runtime::{AppRuntime, ExitReason},
    tcp_forward::TcpForwarder,
};

/// The outcome of the runner's main select loop.
///
/// A pure decision type — `decide_exit` maps the winning branch of the
/// `tokio::select!` to this value. `process::exit` is NOT called here so the
/// decision is unit-testable without side effects.
#[derive(Debug, PartialEq, Eq)]
pub enum RunnerExit {
    /// The runtime died unexpectedly (`watch_for_exit` resolved first). The
    /// runner should `process::exit(1)` so the L2 monitor respawns it.
    Crashed(String),
    /// A clean shutdown was requested (`shutdown_rx` resolved first). The
    /// runner should exit cleanly (return 0).
    CleanShutdown,
}

/// Pure decision: given which branch of the runner's main `select!` won,
/// return the corresponding [`RunnerExit`].
///
/// Keeping this separate from `process::exit` makes it unit-testable. The
/// caller is responsible for acting on the result.
#[must_use]
pub fn decide_exit(exit_reason: Option<ExitReason>) -> RunnerExit {
    match exit_reason {
        Some(ExitReason::Died(reason)) => RunnerExit::Crashed(reason),
        None => RunnerExit::CleanShutdown,
    }
}

/// Drive the runner's main loop: park until the **currently-active** runtime
/// dies unexpectedly (`watch_for_exit` resolves → `Crashed`) or `shutdown_rx`
/// fires (operator shutdown → `CleanShutdown`).
///
/// # Re-arming across swaps (P2.3)
/// A zero-downtime swap ([`perform_swap`]) installs a new runtime and, after a
/// drain, calls `shutdown()` on the OLD one — which makes the OLD runtime's
/// `watch_for_exit` resolve. That MUST NOT be treated as a crash: it is the
/// retired runtime exiting as expected. So the loop:
/// - selects on the active runtime's `watch_for_exit` AND on
///   [`ActiveRuntime::swapped`] (registered BEFORE awaiting so a concurrent swap
///   is not missed);
/// - on `watch_for_exit`: only returns `Crashed` if the runtime that died is
///   STILL the active one (`Arc::ptr_eq`); otherwise it was a retired old
///   runtime — re-arm by looping;
/// - on `swapped`: a swap happened — re-arm the watch on the NEW active runtime;
/// - on `shutdown_rx`: clean shutdown.
///
/// `shutdown_rx` is polled by `&mut` inside the loop so it survives across
/// re-arm iterations (a `oneshot::Receiver` is consumed only when it resolves).
///
/// Returns a [`RunnerExit`] that the binary translates into a `process::exit`
/// call. Extracted as a free function so it is testable with a fake runtime
/// without going through the full `RunnerServe::start` path.
///
/// [`perform_swap`]: crate::runner::active::perform_swap
pub async fn run_until_exit(
    active: Arc<ActiveRuntime>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> RunnerExit {
    loop {
        // The currently-active runtime, and a future that resolves on the NEXT
        // swap. `swapped()` is registered BEFORE we await the select, so a swap
        // racing with this iteration is not missed (P2.2's `notify_waiters`).
        let current = active.load();
        let swapped = active.swapped();

        tokio::select! {
            exit = current.watch_for_exit() => {
                // Only the death of the STILL-active runtime is a crash. After a
                // swap, `current` is a retired old runtime being shut down — its
                // `watch_for_exit` resolving is expected, so we ignore it and
                // re-arm on the new active runtime by looping.
                //
                // `Arc::ptr_eq` compares the pointed-to allocation: `load()`
                // returns a fresh Arc clone each call, but clones of the SAME
                // runtime share one allocation, so this correctly detects
                // "still the active one" (works on `Arc<dyn AppRuntime>`).
                if Arc::ptr_eq(&current, &active.load()) {
                    return decide_exit(Some(exit));
                }
                // else: retired old runtime died as expected — loop to re-arm.
            }
            () = swapped => {
                // A swap occurred — loop to re-arm `watch_for_exit` on the NEW
                // active runtime.
            }
            _ = &mut shutdown_rx => return decide_exit(None),
        }
    }
}

/// Configuration subset the runner serve core needs (decoupled from the full
/// clap [`crate::runner::RunnerConfig`] so the unit tests can construct it
/// without parsing the CLI).
pub struct ServeConfig {
    /// UUID of the app to host (string form).
    pub uuid: String,
    /// S3 base URL for artifact fetch (injected by tests as a wiremock URI).
    pub s3_base_url: String,
    /// Local data dir for the artifact cache.
    pub data_dir: PathBuf,
    /// When `true` the runner binds a loopback listener (no TUN required).
    /// When `false` the runner joins the mesh claiming its app-ULA and binds it.
    pub no_mesh: bool,
    /// Mesh coordinator control-plane URL (used only when `no_mesh = false`).
    pub coordinator_url: String,
    /// Explicit DERP-style mesh relay endpoint (`TABBIFY_MESH_RELAY_URL`). When
    /// `Some`, the runner's mesh join routes its relay over this url verbatim
    /// (e.g. `wss://relay.tabbify.io/v1/mesh/relay`) instead of deriving `ws://`
    /// from the coordinator URL — the corporate-firewall escape hatch. `None`
    /// (the default) keeps the coordinator-derived relay.
    pub relay_url: Option<String>,
    /// Relay-only declaration (`TABBIFY_MESH_RELAY_ONLY`, forwarded by the
    /// supervisor as `--mesh-relay-only`). When `true`, [`build_runner_join_config`]
    /// sets `JoinConfig.relay_only` so the coordinator never advertises a reflexive
    /// direct endpoint for this runner nor emits a hole-punch directive for any
    /// pair involving it — the runner's WG handshake completes single-sided over
    /// the relay (the runner has no reachable direct endpoint, sharing the host's
    /// NAT/firewall with the supervisor). `false` (the default) keeps direct +
    /// hole-punch traversal.
    pub relay_only: bool,
    /// Human-readable display name advertised to the coordinator (mesh mode).
    pub display_name: String,
    /// ULA of the parent supervisor that spawned this runner, declared on the
    /// mesh join so the node can build the supervisor → runners topology.
    /// `None` for a standalone runner.
    pub parent: Option<String>,
    /// Listener port used when binding the runner's own mesh ULA.
    pub port: u16,
    /// Firecracker runtime config.
    pub fc: FcConfig,
    /// Docker runtime config.
    pub docker: DockerConfig,
    /// OCI image ref of a previously-deployed version. When `Some`, it is
    /// applied to the fetched manifest's docker `registry_ref` before the
    /// INITIAL [`build_runtime`], so a supervisor-driven respawn comes up on the
    /// deployed version. `None` = build from the S3 manifest as usual.
    pub image_ref: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (raw TOML) for a connect-repo deploy.
    /// When `Some` and the app has NO S3 manifest (the BUILD-pipeline path),
    /// [`crate::build::fetched_from_ref`] applies its `[runtime]`/`[routes]` to
    /// the synthesized manifest instead of the hardcoded FC defaults. `None`
    /// keeps today's defaults.
    pub manifest_toml: Option<String>,
    /// Tenant network slug (Phase-2 contract). When `Some`, the runner joins the
    /// mesh scoped to this network: [`build_runner_join_config`] advertises
    /// `tag:net-<slug>` so the coordinator (when validating the scoped join
    /// token) places this runner in the tenant's network. `None` (the default)
    /// keeps today's unscoped `runner`-only tag.
    pub network: Option<String>,
    /// Scoped node-join JWT for THIS runner (Phase-2 contract), read from
    /// `TABBIFY_RUNNER_JOIN_TOKEN`. [`build_runner_join_config`] sets it as
    /// `JoinConfig.join_token` so the coordinator authenticates the register and
    /// derives the runner's authoritative `network` + `tags` from the claims.
    /// `None` keeps the current tokenless join (valid against a coordinator
    /// without `AUTH_URL`).
    pub runner_join_token: Option<String>,
    /// Deploy-time extra `KEY=VALUE` environment variables baked into the guest
    /// `/init`. Decoded from `RUNNER_EXTRA_ENV` (a JSON object) by the binary and
    /// threaded into [`crate::build::build_runtime`] so the runner merges them
    /// AFTER the OCI image's `config.Env` before calling `render_init`. `None`
    /// means the guest gets exactly the OCI image's env (normal deploys).
    pub extra_env: Option<std::collections::HashMap<String, String>>,
}

/// A live per-app runner: holds the [`HostedApp`] (and thus its listener task)
/// alive for the duration of this value via a shared [`RunnerLifecycle`] handle
/// that the control server may also hold.
pub struct RunnerServe {
    /// The address the listener bound (loopback ephemeral in `--no-mesh` mode).
    addr: SocketAddr,
    /// The swappable active-runtime cell. Held so the binary can pass it to
    /// [`run_until_exit`] (which needs `load()`/`swapped()` to re-arm its
    /// crash-watch across zero-downtime swaps).
    active: Arc<ActiveRuntime>,
    /// Shared lifecycle state (wraps the live `HostedApp`). Kept here so the
    /// listener task lives as long as the `RunnerServe` does unless the control
    /// server issues a `Stop`.
    lifecycle: RunnerLifecycle,
    /// Mesh membership, held only in mesh mode (`None` under `--no-mesh`). Kept
    /// for the runner's lifetime because dropping it drops the inner `Joiner`,
    /// which aborts the WG/TUN background tasks and closes the tunnel — so the
    /// runner's ULA would stop being reachable. Never read; held only to keep
    /// the mesh up.
    _membership: Option<MeshMembership>,
    /// L4 SSH forwarder: bridges `[app_ula]:2222` → `{guest_ip}:2222` so the
    /// node can SSH into a dev/devbox FC guest via the mesh address. `None`
    /// when the runtime reports no guest SSH target (non-FC, `--no-mesh`).
    /// Held so the forwarder task lives exactly as long as the runner; dropping
    /// it aborts the accept loop and stops forwarding new SSH connections.
    _ssh_fwd: Option<TcpForwarder>,
}

impl RunnerServe {
    /// Fetch the app artifact, build the runtime, and start the per-app
    /// listener. Returns a [`RunnerServe`] holding the live listener.
    ///
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the S3 fetch fails;
    /// - the runtime build fails (wasm compile / firecracker / docker);
    /// - the mesh join fails (mesh mode: no TUN/root, coordinator unreachable);
    /// - the listener fails to bind.
    pub async fn start(cfg: ServeConfig) -> Result<Self> {
        let parsed_uuid = Uuid::parse_str(&cfg.uuid)
            .with_context(|| format!("invalid app uuid: {:?}", cfg.uuid))?;
        let app_ula = derive_app_ula(parsed_uuid);

        // The lifecycle holds an `S3Fetcher` for later cache purges; construct it
        // here so it stays available even though `resolve_app` does its own fetch.
        let fetcher = S3Fetcher::new(&cfg.s3_base_url, &cfg.data_dir);

        let fetched = resolve_app(&cfg).await?;

        // Cold start (first boot / monitor respawn): reconcile a stale VM +
        // warm-restore allowed (`is_swap = false`). Deploy-time extra env is
        // merged AFTER the OCI image's config.Env inside run_firecracker_build.
        let initial_runtime = build_runtime(
            &cfg.uuid,
            &fetched,
            &cfg.fc,
            &cfg.data_dir,
            false,
            cfg.extra_env.as_ref(),
        )
        .await
        .with_context(|| format!("build runtime for {}", cfg.uuid))?;

        // Wrap the initial runtime in a swappable cell so P2.3 can atomically
        // replace it for zero-downtime deploys without touching the listener or
        // the mesh peer.  In P2.2 no swap happens; behavior is identical to
        // holding a plain Arc<dyn AppRuntime>.
        let active: Arc<ActiveRuntime> = Arc::new(ActiveRuntime::new(initial_runtime));

        // Clone the active-runtime handle so the lifecycle can call health() on
        // it independently of the AppServe's copy.  Both coerce to
        // Arc<dyn AppRuntime> via the AppRuntime impl on ActiveRuntime.
        let runtime_for_lifecycle: Arc<dyn AppRuntime> = active.clone();

        // No idle-reaper in the runner yet — the on_request callback is a no-op.
        let on_request: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let serve = AppServe::new(active.clone() as Arc<dyn AppRuntime>, on_request);

        // Build the host + (in mesh mode) the membership that MUST outlive this
        // function — dropping it tears down the WG/TUN tunnel (see field doc).
        let (host, membership) = if cfg.no_mesh {
            (AppHost::loopback(), None)
        } else {
            // Mesh mode: join the coordinator claiming `requested_ula = app_ula`
            // (+ kind=runner, parent, app_uuid). The coordinator routes that
            // ULA to us, so our OWN peer-ULA *is* the app-ULA: bind it directly
            // via `mesh_self` — no separate `host_app_ula` app-route needed.
            let join = build_runner_join_config(&cfg);
            let membership = MeshMembership::join_runner(join)
                .await
                .context("join mesh as runner")?;
            let my_ula = membership.my_ula();
            // FIX #9: advertise our OWN peer-ULA (== app-ULA) as a hosted app on
            // the joiner so it rides every heartbeat. `mesh_self` binds the ULA
            // directly with NO MeshHost joiner, so without this the joiner's
            // hosted set stays empty and `GET /v1/supervisors` reports
            // `hosted_app_ulas` empty even though the app serves 200. The
            // underlying `host_app_ula` re-asserts the /128 TUN alias, which is
            // idempotent for our already-assigned own ULA.
            membership
                .host_own_ula()
                .await
                .context("advertise runner own ULA as hosted app")?;
            tracing::info!(
                %my_ula,
                peer_id = %membership.peer_id(),
                %app_ula,
                "runner joined mesh; advertised own ULA as hosted app; binding own ULA"
            );
            (AppHost::mesh_self(my_ula, cfg.port), Some(membership))
        };

        let hosted = host
            .host(app_ula, serve)
            .await
            .with_context(|| format!("host app {} on {:?}", cfg.uuid, app_ula))?;

        let addr = hosted.addr;

        // FIX A: L4 SSH forwarder — bind [app_ula]:GUEST_SSH_PORT →
        // guest_ip:GUEST_SSH_PORT so the node can `ssh root@[app_ula]:2222`
        // into the FC guest's sshd. Only started in mesh mode AND when the
        // runtime exposes a guest SSH target (Firecracker dev/devbox image).
        // The listener uses SO_REUSEPORT, so a deploy/swap's NEW runner can
        // bind while the OLD still holds the port. Bind errors are logged and
        // tolerated — they must not crash the runner.
        let ssh_fwd = if cfg.no_mesh {
            None
        } else if let Some(ssh_target) = active.guest_ssh_addr() {
            let ssh_bind = std::net::SocketAddr::new(
                std::net::IpAddr::V6(app_ula),
                crate::tcp_forward::GUEST_SSH_PORT,
            );
            match crate::tcp_forward::spawn_forwarder(ssh_bind, ssh_target).await {
                Ok(fwd) => {
                    tracing::info!(
                        bind = %ssh_bind,
                        target = %ssh_target,
                        "runner: SSH forwarder bound (app_ula:2222 → guest_ip:2222)"
                    );
                    Some(fwd)
                }
                Err(e) => {
                    tracing::warn!(
                        bind = %ssh_bind,
                        target = %ssh_target,
                        error = %e,
                        "runner: SSH forwarder bind failed — exec/ssh unavailable until rebind (next respawn/restart re-attempts)"
                    );
                    None
                }
            }
        } else {
            None
        };

        // The ref the INITIAL runtime was built from: the resolved manifest's
        // `runtime.registry_ref` (a deployed version applied via `--image-ref`,
        // or `None` for a plain source/S3 build). Seeds the same-ref re-deploy
        // guard so a deploy of the already-running ref is a no-op.
        let initial_ref = fetched.manifest.runtime.registry_ref.clone();

        // Best-effort cold-start digest seed: resolve the initial ref to its OCI
        // manifest digest so the deploy guard's digest comparison has a baseline
        // from the first cold start. On any error (no initial ref, registry
        // unreachable) leave `None` — a `None` digest simply means the first
        // deploy rebuilds rather than short-circuiting, which is always safe.
        let initial_digest: Option<String> = match &initial_ref {
            Some(reff) => {
                let runner = crate::runner::build::firecracker::production_fc_build_runner();
                match crate::runner::build::firecracker::resolve_oci_digest(reff, &runner).await {
                    Ok(d) => Some(d),
                    Err(e) => {
                        tracing::warn!(
                            uuid = %cfg.uuid,
                            reff = %reff,
                            error = %e,
                            "cold start: initial ref digest resolve failed — current_digest=None (first deploy rebuilds)"
                        );
                        None
                    }
                }
            }
            None => None,
        };

        let lifecycle = RunnerLifecycle {
            uuid: cfg.uuid.clone(),
            version: fetched.version,
            app_ula: app_ula.to_string(),
            hosted: Arc::new(Mutex::new(Some(hosted))),
            fetcher,
            docker: cfg.docker,
            runtime: runtime_for_lifecycle,
            // Shared with this RunnerServe + the binary's run_until_exit loop so
            // a `Deploy` swaps the live runtime here and the crash-watch re-arms.
            active: active.clone(),
            // The build context `Deploy` rebuilds from (manifest + cached path).
            fetched,
            fc: cfg.fc,
            data_dir: cfg.data_dir,
            // Wired by the binary after start(); None here so the control
            // server falls back to the legacy direct-exit path until the
            // binary calls lifecycle.set_shutdown_tx(tx).
            shutdown_tx: Arc::new(Mutex::new(None)),
            // Seed the same-ref guard with the initial build's ref.
            current_ref: Arc::new(Mutex::new(initial_ref)),
            // Seed the digest guard with the initial ref's resolved digest (or
            // None — first deploy then rebuilds, which is safe).
            current_digest: Arc::new(Mutex::new(initial_digest)),
            // Carry the deploy-time extra env into the lifecycle so a
            // zero-downtime swap re-bakes the same vars into the new rootfs.
            extra_env: cfg.extra_env,
            // Production: resolve digests via the real `oras` runner (no
            // override). Only tests inject a fake resolver here.
            digest_resolver: None,
        };

        Ok(Self {
            addr,
            active,
            lifecycle,
            _membership: membership,
            _ssh_fwd: ssh_fwd,
        })
    }

    /// The address the per-app listener is bound on. Dial this to reach the app.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A cloneable handle to the runner's lifecycle state, for use by the
    /// control server ([`crate::runner::control::serve`]).
    #[must_use]
    pub fn lifecycle(&self) -> RunnerLifecycle {
        self.lifecycle.clone()
    }

    /// The swappable active-runtime cell held by this runner.
    ///
    /// Exposed so the binary can pass it to [`run_until_exit`] for the
    /// re-arming `watch_for_exit` select loop (which needs `load()`/`swapped()`
    /// to distinguish a real crash from a post-swap retirement of the old
    /// runtime). Coerces to `Arc<dyn AppRuntime>` via the `AppRuntime` impl when
    /// a plain runtime handle is needed (e.g. the binary's clean-shutdown path).
    #[must_use]
    pub fn runtime(&self) -> Arc<ActiveRuntime> {
        self.active.clone()
    }
}

/// Resolve the [`FetchedApp`] the runner builds its INITIAL runtime from.
///
/// - The historical path. S3 fetch SUCCEEDS (tcli-push apps +
///   wasm/firecracker): when an `--image-ref` was passed (orchestrator respawn),
///   apply it to the manifest's docker `registry_ref` so the INITIAL build comes
///   up on the deployed version (a `docker pull <ref>` instead of a source
///   build); ignored for wasm/firecracker. S3 fetch FAILS but an `--image-ref`
///   is present: this is a BUILD-pipeline app (`POST /v1/deploy` with a
///   repo_url) — its image is in the mesh registry and it has NO S3 manifest;
///   synthesize a minimal docker manifest from the ref and run the deployed
///   image directly instead of crash-looping on the absent S3 fetch. S3 fetch
///   FAILS with no image-ref: propagate the error (a genuine missing app).
///
/// Extracted from [`RunnerServe::start`] so the fetch/resolve decision is
/// directly unit-testable without binding a listener or joining the mesh.
///
/// # Errors
/// Propagates the [`crate::build::resolve_fetched`] error on the non-exempt path
/// (S3 fetch failed and no `image_ref` was supplied, or a filesystem error
/// materializing the build-context placeholder).
pub async fn resolve_app(cfg: &ServeConfig) -> Result<FetchedApp> {
    let fetch_result = S3Fetcher::new(&cfg.s3_base_url, &cfg.data_dir)
        .fetch(&cfg.uuid)
        .await;

    if fetch_result.is_err() && cfg.image_ref.is_some() {
        tracing::info!(
            uuid = %cfg.uuid,
            image_ref = ?cfg.image_ref,
            "no S3 manifest for app — running deployed image from registry ref directly"
        );
    }

    crate::build::resolve_fetched(
        fetch_result,
        &cfg.uuid,
        cfg.image_ref.as_deref(),
        cfg.manifest_toml.as_deref(),
        &cfg.data_dir,
    )
    .with_context(|| format!("fetch app {}", cfg.uuid))
}

/// Per-uuid path for a runner's persistent WireGuard keypair, under `data_dir`.
///
/// Scheme: `<data_dir>/runners/<uuid>.meshkey`. Each per-app runner MUST own a
/// DISTINCT keypair so it presents a UNIQUE public key to the coordinator —
/// otherwise multiple runners sharing the ambient `$HOME/.tabbify-mesh/keypair`
/// all hit the coordinator's by-pubkey re-registration path, get assigned the
/// SAME ULA (whichever runner registered that pubkey first), and collide:
/// their distinct `requested_ula = derive_app_ula(uuid)` is ignored, so only
/// one app is reachable at a time and routing flaps across respawns.
///
/// Keying the path by uuid makes the keypair (a) unique per app → the
/// coordinator takes the first-time path and honours `requested_ula`, and (b)
/// persistent across respawns (the file is loaded if present), so the runner's
/// pubkey — and therefore its assigned app-ULA — stays sticky.
#[must_use]
pub fn runner_keypair_path(data_dir: &std::path::Path, uuid: &str) -> PathBuf {
    data_dir.join("runners").join(format!("{uuid}.meshkey"))
}

/// Prefix for the per-tenant network tag (Phase-2 contract): the runner's tag
/// is `tag:net-<slug>` where `<slug>` is the auth network slug (e.g.
/// `n_jpegxik72nng` → `tag:net-n_jpegxik72nng`).
pub const NET_TAG_PREFIX: &str = "tag:net-";

/// The mesh tags a runner advertises for the given optional network `slug`.
///
/// Always includes the base `runner` role tag; when a `slug` is present it ALSO
/// includes `tag:net-<slug>` (Phase-2) so a non-validating coordinator routes
/// the runner into its tenant network. A validating coordinator ignores these
/// advisory tags and uses the scoped `join_token` claims instead. Pure +
/// unit-tested so the tag-format contract is pinned without a live join.
#[must_use]
pub fn network_tags(slug: Option<&str>) -> Vec<String> {
    let mut tags = vec!["runner".to_owned()];
    if let Some(slug) = slug.filter(|s| !s.trim().is_empty()) {
        tags.push(format!("{NET_TAG_PREFIX}{slug}"));
    }
    tags
}

/// Build the [`mesh_joiner::JoinConfig`] the runner uses to join the mesh.
///
/// This is the runner's defining mesh contract (per-app-runner arch §0.2/§0.1):
/// - `requested_ula = derive_app_ula(uuid)` — the runner claims its app-ULA so
///   the coordinator routes it straight to this peer (its peer-ULA == app-ULA);
/// - `keypair_path` — a per-uuid persistent WireGuard keypair (see
///   [`runner_keypair_path`]) so EACH runner is a DISTINCT mesh peer with a
///   UNIQUE pubkey; this is what makes the coordinator honour each runner's
///   distinct `requested_ula` instead of collapsing them all onto one ULA;
/// - `kind = "runner"` — tags this peer as a per-app runner in the roster;
/// - `parent` — the spawning supervisor's ULA (so the node can build the
///   supervisor → runners topology); `None` for a standalone runner;
/// - `app_uuid` — the app this runner serves.
///
/// `identity_path` is left `None`: the ULA is derived deterministically from
/// the uuid via `derive_app_ula` and requested explicitly, so the richer
/// `{keypair, ULA}` identity file is not needed (and setting it would override
/// `requested_ula`, which we want to stay explicit).
///
/// Extracted as a pure function so the construction is unit-testable without a
/// live mesh join (which needs a real TUN/root + coordinator — exercised in the
/// Phase-4 Lima e2e test, not here).
///
/// # Panics
/// Never panics; an invalid `uuid` would already have been rejected by
/// [`RunnerServe::start`] before this is called. Here it falls back to the
/// nil UUID's ULA if parsing somehow fails, keeping the function total.
#[must_use]
pub fn build_runner_join_config(cfg: &ServeConfig) -> mesh_joiner::JoinConfig {
    let app_uuid = Uuid::parse_str(&cfg.uuid).unwrap_or(Uuid::nil());
    let app_ula = derive_app_ula(app_uuid);
    // Phase-2: scope the runner to its tenant network. The base `runner` tag is
    // always advertised; when a network slug is set we ALSO advertise
    // `tag:net-<slug>` so the coordinator's per-network self-rule
    // (`tag:net-<slug>` ↔ `tag:net-<slug>`, PUT by auth on network-create) makes
    // this runner visible to the rest of the tenant network and the system
    // infra (`tag:system` → `tag:net-*`). These advisory tags are honored only
    // by a coordinator WITHOUT `AUTH_URL`; a validating coordinator derives the
    // authoritative tags from the scoped `join_token` claims instead. `None`
    // keeps the unscoped `runner`-only tag (today's behavior).
    let tags = network_tags(cfg.network.as_deref());
    mesh_joiner::JoinConfig {
        coordinator_url: cfg.coordinator_url.clone(),
        display_name: cfg.display_name.clone(),
        tags,
        insecure_no_mtls: true,
        // Scoped node-join token (Phase-2). The coordinator validates it and
        // stamps the runner `network=<slug>`, `tags=["tag:net-<slug>"]`. `None`
        // keeps the current tokenless join.
        join_token: cfg.runner_join_token.clone(),
        requested_ula: Some(app_ula.to_string()),
        // Per-uuid keypair → unique pubkey → coordinator honours requested_ula.
        keypair_path: Some(runner_keypair_path(&cfg.data_dir, &cfg.uuid)),
        kind: Some("runner".to_owned()),
        parent: cfg.parent.clone(),
        app_uuid: Some(cfg.uuid.clone()),
        // Explicit DERP-style relay endpoint (`TABBIFY_MESH_RELAY_URL`),
        // forwarded by the supervisor as `--mesh-relay-url`. `Some` routes the
        // runner's relay over this url verbatim (corporate-firewall escape
        // hatch); `None` keeps the coordinator-derived relay, unchanged.
        relay_url: cfg.relay_url.clone(),
        // Relay-only declaration (`--mesh-relay-only`, forwarded by the
        // supervisor). `true` makes the coordinator suppress this runner's
        // reflexive direct endpoint + any hole-punch directive, so the runner's
        // WG handshake completes single-sided over the relay (it has no reachable
        // direct endpoint behind the host's NAT/firewall). `false` keeps direct +
        // hole-punch traversal, unchanged.
        relay_only: cfg.relay_only,
        // Report the runner's version (= the supervisor's binary, same image) to
        // the roster — matches how the supervisor sets its own in main.rs, so
        // runners stop showing `software_version = null` in the admin.
        software_version: Some(crate::version::binary_version().to_owned()),
        // A runner ALWAYS shares its netns with the supervisor (and other
        // runners), so its peer `/128`s must live in its own source-scoped
        // table — otherwise the supervisor's main-table routes win and the
        // runner's return traffic egresses via the WRONG TUN (dropped by
        // the remote peer's §5.5 source allowed-set). Intrinsic, not
        // configurable: there is no single-netns runner deployment.
        source_scoped_routes: true,
        // Keep the host firewall from dropping inbound overlay dials to
        // the app listener (tailscaled-style, best-effort).
        manage_firewall: true,
        ..Default::default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::{IpAddr, Ipv6Addr, SocketAddr};

    use super::*;
    use crate::host::HostBind;

    const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

    fn mesh_cfg() -> ServeConfig {
        ServeConfig {
            uuid: APP_UUID.to_owned(),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/tmp/tabbify-runner-test"),
            no_mesh: false,
            coordinator_url: "http://10.0.0.1:8888".to_owned(),
            relay_url: None,
            relay_only: false,
            display_name: "runner-test".to_owned(),
            parent: Some("fd5a:1f00:0:3::1".to_owned()),
            port: 8730,
            fc: FcConfig::default(),
            docker: DockerConfig::default(),
            image_ref: None,
            manifest_toml: None,
            network: None,
            runner_join_token: None,
            extra_env: None,
        }
    }

    /// The runner's mesh join must claim its app-ULA + declare its role,
    /// parent, and app uuid — so the coordinator routes the app-ULA to this
    /// peer and the node can build the supervisor → runners topology.
    #[test]
    fn runner_join_config_claims_app_ula_and_declares_parent() {
        let cfg = mesh_cfg();
        let join = build_runner_join_config(&cfg);

        let expected_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        assert_eq!(
            join.requested_ula.as_deref(),
            Some(expected_ula.to_string().as_str()),
            "runner must request its derived app-ULA"
        );
        assert_eq!(join.kind.as_deref(), Some("runner"), "kind must be runner");
        assert_eq!(
            join.parent.as_deref(),
            Some("fd5a:1f00:0:3::1"),
            "parent supervisor ULA must be forwarded"
        );
        assert_eq!(
            join.app_uuid.as_deref(),
            Some(APP_UUID),
            "app_uuid must be the served app's uuid"
        );
        assert_eq!(join.coordinator_url, "http://10.0.0.1:8888");
        // Each runner gets its OWN persistent WireGuard keypair under data_dir,
        // keyed by uuid — so it presents a UNIQUE pubkey and the coordinator
        // honours its distinct `requested_ula` (the per-app-ULA collision fix).
        // Identity persistence (the richer {keypair, ULA} file) stays unused:
        // the ULA is derived deterministically from the uuid, not persisted.
        let kp_path = join
            .keypair_path
            .as_ref()
            .expect("runner must carry a per-uuid keypair_path");
        assert!(
            kp_path.starts_with(&cfg.data_dir),
            "keypair_path must live under data_dir, got {kp_path:?}"
        );
        assert!(
            kp_path.to_string_lossy().contains(APP_UUID),
            "keypair_path must contain the app uuid, got {kp_path:?}"
        );
        assert!(join.identity_path.is_none());
        // No explicit relay endpoint by default → derive from coordinator URL.
        assert!(
            join.relay_url.is_none(),
            "default ServeConfig must leave relay_url None"
        );
    }

    /// An explicit `relay_url` on the runner's [`ServeConfig`] (forwarded by the
    /// supervisor as `--mesh-relay-url`) rides onto the runner's
    /// [`mesh_joiner::JoinConfig`] verbatim, so the runner routes its relay over
    /// that `wss://` endpoint (the corporate-firewall escape hatch) instead of
    /// deriving `ws://` from the coordinator URL.
    #[test]
    fn runner_join_config_wires_relay_url() {
        let mut cfg = mesh_cfg();
        cfg.relay_url = Some("wss://relay.tabbify.io/v1/mesh/relay".to_owned());
        let join = build_runner_join_config(&cfg);
        assert_eq!(
            join.relay_url.as_deref(),
            Some("wss://relay.tabbify.io/v1/mesh/relay"),
            "explicit relay_url must ride onto the runner's JoinConfig"
        );
    }

    /// A `true` `relay_only` on the runner's [`ServeConfig`] (forwarded by the
    /// supervisor as `--mesh-relay-only`) rides onto the runner's
    /// [`mesh_joiner::JoinConfig`], so the coordinator suppresses the runner's
    /// reflexive direct endpoint + hole-punch directives and its WG handshake
    /// completes single-sided over the relay.
    #[test]
    fn runner_join_config_wires_relay_only() {
        let mut cfg = mesh_cfg();
        cfg.relay_only = true;
        let join = build_runner_join_config(&cfg);
        assert!(
            join.relay_only,
            "relay_only=true must ride onto the runner's JoinConfig"
        );
    }

    /// The default `relay_only` (false) leaves the runner's `JoinConfig.relay_only`
    /// off, so the runner keeps direct + hole-punch traversal.
    #[test]
    fn runner_join_config_omits_relay_only_when_false() {
        let join = build_runner_join_config(&mesh_cfg());
        assert!(
            !join.relay_only,
            "default ServeConfig must leave relay_only off"
        );
    }

    /// Phase-2: with a network slug + scoped token set, the runner's join
    /// config carries the token as `join_token` and advertises both `runner`
    /// and `tag:net-<slug>` so the coordinator scopes it to the tenant network.
    #[test]
    fn runner_join_config_scopes_to_network_with_token() {
        let mut cfg = mesh_cfg();
        cfg.network = Some("n_jpegxik72nng".to_owned());
        cfg.runner_join_token = Some("scoped-runner-jwt".to_owned());
        let join = build_runner_join_config(&cfg);

        assert_eq!(
            join.join_token.as_deref(),
            Some("scoped-runner-jwt"),
            "scoped node-join token must ride onto JoinConfig.join_token"
        );
        assert!(
            join.tags.iter().any(|t| t == "runner"),
            "base runner tag must remain, got: {:?}",
            join.tags
        );
        assert!(
            join.tags.iter().any(|t| t == "tag:net-n_jpegxik72nng"),
            "must advertise tag:net-<slug>, got: {:?}",
            join.tags
        );
    }

    /// With NO network/token (today's behavior), the runner joins unscoped: only
    /// the `runner` tag, and `join_token` stays `None`.
    #[test]
    fn runner_join_config_unscoped_by_default() {
        let join = build_runner_join_config(&mesh_cfg());
        assert!(join.join_token.is_none(), "default join must be tokenless");
        assert_eq!(
            join.tags,
            vec!["runner".to_owned()],
            "default join must carry only the runner tag"
        );
    }

    /// The `tag:net-<slug>` format is pinned by the contract; `network_tags`
    /// produces exactly `["runner", "tag:net-<slug>"]` (and drops a blank slug).
    #[test]
    fn network_tags_format_matches_contract() {
        assert_eq!(
            network_tags(Some("n_jpegxik72nng")),
            vec!["runner".to_owned(), "tag:net-n_jpegxik72nng".to_owned()]
        );
        assert_eq!(network_tags(None), vec!["runner".to_owned()]);
        // A blank slug is treated as no network (no empty `tag:net-` tag).
        assert_eq!(network_tags(Some("   ")), vec!["runner".to_owned()]);
        assert_eq!(NET_TAG_PREFIX, "tag:net-");
    }

    /// A runner ALWAYS shares its netns with the supervisor, so its joiner
    /// must (a) source-scope its peer routes — otherwise the supervisor's
    /// main-table `/128`s win and the runner's return traffic egresses via
    /// the WRONG TUN (dropped by the remote §5.5 source check — the exact
    /// 2026-06-04 outage) — and (b) self-manage the host-firewall trust for
    /// its TUN so inbound app dials aren't dropped by `nixos-fw`-style
    /// default firewalls.
    #[test]
    fn runner_join_config_enables_host_integration() {
        let join = build_runner_join_config(&mesh_cfg());
        assert!(
            join.source_scoped_routes,
            "runner must source-scope its peer routes (shared netns)"
        );
        assert!(
            join.manage_firewall,
            "runner must self-manage host-firewall TUN trust"
        );
    }

    /// Distinct uuids must yield distinct keypair paths — otherwise two runners
    /// would share a keypair, present the same pubkey, and collide on one ULA
    /// (the exact production bug this fix addresses).
    #[test]
    fn runner_keypair_path_is_unique_per_uuid() {
        let data_dir = PathBuf::from("/tmp/tabbify-runner-test");
        let uuid_a = "0191e7c2-1111-7222-8333-444455556666";
        let uuid_b = "019e7903-aaaa-7bbb-8ccc-ddddeeeeffff";

        let path_a = runner_keypair_path(&data_dir, uuid_a);
        let path_b = runner_keypair_path(&data_dir, uuid_b);

        assert_ne!(
            path_a, path_b,
            "two uuids must produce two distinct keypair paths"
        );
        assert_eq!(
            path_a,
            data_dir.join("runners").join(format!("{uuid_a}.meshkey")),
            "path scheme must be <data_dir>/runners/<uuid>.meshkey"
        );
        assert!(path_a.starts_with(&data_dir));
        assert!(path_a.to_string_lossy().contains(uuid_a));
    }

    /// In mesh mode the runner binds its OWN ULA (== app-ULA) with no separate
    /// app-route layer — `AppHost::mesh_self` selects `[my_ula]:port`.
    #[test]
    fn mesh_self_binds_own_ula_without_app_route() {
        let my_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        let host = AppHost::mesh_self(my_ula, 8730);

        // No app-route layer: `mesh_self` does not carry a MeshHost joiner (the
        // coordinator already routes the runner's own ULA to it).
        assert!(
            !host.is_mesh(),
            "mesh_self must not engage the host_app_ula app-route layer"
        );
        // The selected bind address is the runner's own ULA on the given port.
        assert_eq!(
            host.bind_addr_for(my_ula),
            SocketAddr::new(IpAddr::V6(my_ula), 8730),
            "runner must bind its own ULA, not an ephemeral/loopback addr"
        );
        // Sanity: it really is the app-ULA prefix, distinct from loopback.
        assert_ne!(my_ula, Ipv6Addr::LOCALHOST);
        assert!(matches!(host.bind(), HostBind::OwnUla(8730)));
    }

    // ---- decide_exit / run_until_exit tests --------------------------------

    use bytes::Bytes;
    use http::{Request, Response};
    use tokio::sync::oneshot;

    use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason};

    /// Fake runtime whose `watch_for_exit` resolves immediately to Died.
    struct CrashingRuntime {
        reason: String,
    }

    impl AppRuntime for CrashingRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }

        fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
            let reason = self.reason.clone();
            Box::pin(async move { ExitReason::Died(reason) })
        }
    }

    /// Fake runtime whose `watch_for_exit` never resolves (wasm-like).
    struct StableRuntime;

    impl AppRuntime for StableRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }
        // watch_for_exit uses the default: std::future::pending()
    }

    /// Fake runtime whose `watch_for_exit` resolves only when an external
    /// trigger ([`tokio::sync::Notify`]) is fired — lets a test drive the exact
    /// moment a runtime "dies", deterministically.
    struct WatchableRuntime {
        trigger: Arc<tokio::sync::Notify>,
        reason: String,
    }

    impl WatchableRuntime {
        fn new(reason: &str) -> (Arc<Self>, Arc<tokio::sync::Notify>) {
            let trigger = Arc::new(tokio::sync::Notify::new());
            let rt = Arc::new(Self {
                trigger: trigger.clone(),
                reason: reason.to_owned(),
            });
            (rt, trigger)
        }
    }

    impl AppRuntime for WatchableRuntime {
        fn handle<'a>(&'a self, _req: Request<Bytes>) -> BoxRespFut<'a> {
            Box::pin(async { Ok(Response::builder().status(200).body(Bytes::new()).unwrap()) })
        }

        fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
            let trigger = self.trigger.clone();
            let reason = self.reason.clone();
            Box::pin(async move {
                trigger.notified().await;
                ExitReason::Died(reason)
            })
        }
    }

    /// decide_exit: when exit_reason is Some(Died) → Crashed with the reason.
    #[test]
    fn decide_exit_died_returns_crashed() {
        let result = decide_exit(Some(ExitReason::Died("container exited(1)".to_owned())));
        assert_eq!(
            result,
            RunnerExit::Crashed("container exited(1)".to_owned())
        );
    }

    /// decide_exit: when exit_reason is None (shutdown branch won) → CleanShutdown.
    #[test]
    fn decide_exit_none_returns_clean_shutdown() {
        let result = decide_exit(None);
        assert_eq!(result, RunnerExit::CleanShutdown);
    }

    /// run_until_exit: when watch_for_exit resolves first → Crashed.
    #[tokio::test]
    async fn run_until_exit_crash_wins_returns_crashed() {
        let active = Arc::new(ActiveRuntime::new(Arc::new(CrashingRuntime {
            reason: "container tbf-test-0 exited with code 1".to_owned(),
        })));
        let (_tx, rx) = oneshot::channel::<()>();
        // Deliberately drop tx so the channel is open but never sent to —
        // watch_for_exit resolves first.
        let result = run_until_exit(active, rx).await;
        assert_eq!(
            result,
            RunnerExit::Crashed("container tbf-test-0 exited with code 1".to_owned())
        );
    }

    /// run_until_exit: when shutdown_rx fires first → CleanShutdown.
    #[tokio::test]
    async fn run_until_exit_shutdown_wins_returns_clean() {
        let active = Arc::new(ActiveRuntime::new(Arc::new(StableRuntime)));
        let (tx, rx) = oneshot::channel::<()>();
        // Fire the shutdown immediately before awaiting.
        tx.send(()).unwrap();
        let result = run_until_exit(active, rx).await;
        assert_eq!(result, RunnerExit::CleanShutdown);
    }

    // ---- re-arming crash-watch across swaps (P2.3) --------------------------

    /// After a swap, the OLD runtime is drained + shut down, which makes its
    /// `watch_for_exit` resolve. That MUST NOT be treated as a crash: the
    /// retired runtime is no longer the active one. The runner must keep
    /// running (watching the NEW, pending runtime).
    #[tokio::test]
    async fn swap_then_old_death_does_not_exit() {
        let (old, old_trigger) = WatchableRuntime::new("OLD died");
        let active = Arc::new(ActiveRuntime::new(old));
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn({
            let active = active.clone();
            async move { run_until_exit(active, rx).await }
        });

        // Let the loop register its first `swapped()`/`watch_for_exit` select.
        tokio::task::yield_now().await;

        // Swap in a NEW runtime whose `watch_for_exit` pends forever.
        active.swap(Arc::new(StableRuntime));

        // Let the loop observe the swap and re-arm on the NEW runtime.
        tokio::task::yield_now().await;

        // Now fire the OLD runtime's death (post-swap, post-drain). The loop
        // must recognise `current != active.load()` and re-arm, NOT exit.
        old_trigger.notify_waiters();

        // The task must STILL be running after a short grace period.
        let still_running =
            tokio::time::timeout(std::time::Duration::from_millis(150), handle).await;
        assert!(
            still_running.is_err(),
            "run_until_exit must NOT return after a retired runtime dies post-swap"
        );
    }

    /// When the runtime that dies IS the active one, the runner exits Crashed
    /// promptly.
    #[tokio::test]
    async fn active_death_exits() {
        let (only, only_trigger) = WatchableRuntime::new("the only runtime died");
        let active = Arc::new(ActiveRuntime::new(only));
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn({
            let active = active.clone();
            async move { run_until_exit(active, rx).await }
        });

        // Let the loop arm its watch.
        tokio::task::yield_now().await;

        // Fire the ACTIVE runtime's death — there was no swap, so `current` is
        // still the active one and the loop must return Crashed.
        only_trigger.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("run_until_exit must return promptly when the active runtime dies")
            .expect("run_until_exit task must not panic");
        assert_eq!(
            result,
            RunnerExit::Crashed("the only runtime died".to_owned())
        );
    }
}
