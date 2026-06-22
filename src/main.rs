//! Thin entrypoint: parse config, init logging, optionally join the mesh, build
//! the runner ORCHESTRATOR, re-adopt any living runners, start the background
//! monitor, pre-start `--app` uuids (spawn their runners), bind the control API,
//! and run the axum server. All logic lives in the `tabbify_supervisor` library.
//!
//! Since the per-app-runner refactor (Task 2.6) the supervisor no longer hosts
//! apps in-process: it orchestrates DETACHED `tabbify-runner` processes (one per
//! app), each serving its app on its own mesh ULA. The control API drives that
//! orchestrator.

use std::net::SocketAddr;

use anyhow::Context;
use tabbify_supervisor::api::{
    GIT_PROXY_IPV4_PORT, SupervisorState, git_proxy_ipv4_router, router,
};
use tabbify_supervisor::config::Config;
use tabbify_supervisor::docker::docker_available;
use tabbify_supervisor::fetcher::S3Fetcher;
use tabbify_supervisor::firecracker::kvm_available;
use tabbify_supervisor::mesh::MeshMembership;
use tabbify_supervisor::orchestrator::spawn::default_runner_bin;
use tabbify_supervisor::orchestrator::{Orchestrator, SharedRunnerConfig};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // ── Crash-at-startup loop-guard: BUMP first (spec §3.3) ──────────────────
    // Bump the durable boot-attempt counter at the very TOP of main — BEFORE
    // `Config::from_env` and any fallible startup step — so a binary that exits
    // before READY (the 2026-06-22 ~31ms pre-network crash class) is still
    // counted. The `OnFailure=tabbify-boot-revert` catch-net reads this counter
    // and reverts to previous-good once the streak crosses the threshold.
    //
    // GUARDED: the `self-update` / `--check` / `revert-to-previous` invocations
    // are NOT "boots" (they run-to-completion-and-exit, or are out-of-band
    // candidates) — counting them would poison the streak. We inspect argv
    // directly (a lightweight check that does not depend on the full clap parse,
    // so a config-parse crash on a REAL boot is still counted). `data_dir` is
    // read from the same `SUPERVISOR_DATA_DIR` env the config uses.
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let boot_data_dir = boot_health_data_dir();
    if is_boot_invocation(&raw_args) {
        let _ = tabbify_supervisor::boot_health::BootAttempts::load(&boot_data_dir)
            .bump(&boot_data_dir);
    }

    let config = Config::from_env();

    // ── self-update subcommand (production self-update flow, spec §3-§6) ─────
    // `supervisord self-update --to <ver>` is the SINGLE real self-update path:
    // it fetches + sha256-verifies the release, probes the candidate out-of-band
    // behind the 3-part gate, and on PASS swaps the symlinks + restarts (the
    // next boot's self-watchdog confirms or reverts). It runs to completion and
    // exits — it never falls through to the daemon boot below.
    if let Some(tabbify_supervisor::config::Command::SelfUpdate { to }) = &config.command {
        let code = run_self_update(to).await;
        std::process::exit(code);
    }

    // ── revert-to-previous subcommand (crash-at-startup catch-net, spec §3.2) ─
    // `supervisord revert-to-previous [--reboot-on-exhausted]` is the audited
    // remediation the `OnFailure=tabbify-boot-revert` unit invokes when the start
    // path keeps crashing. It re-points the binary symlinks to the previous-good
    // release (reusing `watchdog::revert_to_previous` — symlink + VERSION only,
    // systemd owns the restart), stamps the reverted-from version into the
    // VERSION `quarantine` list (so the OTA poller can't re-swap the known-bad
    // tag), and records the revert in the BootAttempts sidecar. It runs to
    // completion and exits with a code the OnFailure script reads (see
    // `RevertExit`). It never falls through to the daemon boot.
    if let Some(tabbify_supervisor::config::Command::RevertToPrevious {
        reboot_on_exhausted,
    }) = &config.command
    {
        let code = run_revert_to_previous(*reboot_on_exhausted).await;
        std::process::exit(code);
    }

    // ── Probe entrypoint (self-update candidate, spec §4) ───────────────────
    // If `--check` is set, this process is an OUT-OF-BAND candidate: it joins
    // the mesh with a TRANSIENT identity, runs the 3-part health gate against
    // itself, and exits 0 (pass) / 1 (fail). It never claims the sticky ULA and
    // never serves real traffic. This branch must come BEFORE any sticky-ULA
    // join / orchestrator setup so the two modes are completely disjoint.
    if config.check_mode {
        let outcome = run_check_mode(&config).await;
        match outcome {
            tabbify_supervisor::selfupdate::probe::ProbeOutcome::Pass => {
                tracing::info!("candidate gate PASSED");
                std::process::exit(0);
            }
            tabbify_supervisor::selfupdate::probe::ProbeOutcome::Fail(reason) => {
                tracing::error!(%reason, "candidate gate FAILED");
                std::process::exit(1);
            }
        }
    }
    // ────────────────────────────────────────────────────────────────────────

    tracing::info!(
        coordinator = %config.coordinator_url,
        s3 = %config.s3_base_url,
        no_mesh = config.no_mesh,
        "starting supervisord"
    );

    // Runtime capability gates. Each capable runtime advertises a mesh tag so
    // the coordinator/node route an app of that runtime to a supervisor that can
    // host it; WASM is always available. A host advertises only what it can run.
    let kvm = kvm_available();
    let docker = docker_available();
    let capability_tags =
        tabbify_supervisor::capability_tags::capability_tags(kvm, docker, config.builder);
    if kvm {
        tracing::info!("KVM available (/dev/kvm) — advertising `firecracker` capability");
    } else {
        tracing::info!("no /dev/kvm — firecracker apps unsupported on this host");
    }
    if docker {
        tracing::info!("Docker daemon reachable — advertising `docker` capability");
    } else {
        tracing::info!("no Docker daemon — docker apps unsupported on this host");
    }
    if config.builder {
        tracing::info!(
            "designated BUILDER (SUPERVISOR_BUILDER) — advertising `builder` capability"
        );
        if !docker {
            // Warn, not fatal: the daemon may come up later, and the node's
            // builder selection happens against the roster tag — a build
            // landing here without docker fails loudly in the build status.
            tracing::warn!(
                "designated builder but no reachable docker daemon — builds will fail until docker is up"
            );
        }
    }
    if capability_tags.is_empty() {
        tracing::info!("WASM-only supervisor (no firecracker / docker capability)");
    }

    // Join the mesh (unless --no-mesh). The membership is held for the process
    // lifetime so the TUN device + WG background tasks stay up. The CONTROL API
    // binds the peer-ULA; each app is served by its own runner on its own ULA.
    //
    // The supervisor's own ULA (when joined) is passed to newly-spawned runners
    // as their `--parent` so the node can build the supervisor → runners
    // topology. The supervisor joins with a sticky identity persisted under
    // `data_dir` (`mesh_identity_path`), so it keeps a STABLE ULA across
    // restarts when `data_dir` is a mounted volume. `--no-mesh` runs pass
    // `parent = None` (no ULA to declare).
    let (bind_addr, supervisor_id, ula_str, parent, _membership) = if config.no_mesh {
        let addr = config
            .bind
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], config.port)));
        tracing::warn!(
            %addr,
            "running WITHOUT mesh (--no-mesh): control on plain addr; runners on loopback"
        );
        (addr, "local".to_owned(), addr.ip().to_string(), None, None)
    } else {
        let membership = MeshMembership::join(
            &config.coordinator_url,
            &config.display_name,
            &capability_tags,
            tabbify_supervisor::mesh::JoinMetadata {
                identity_path: Some(config.mesh_identity_path()),
                software_version: Some(tabbify_supervisor::version::binary_version().to_owned()),
                ..Default::default()
            },
            config.effective_relay_url(),
            config.relay_only,
            config.advertise_endpoint.clone(),
            &config.data_dir,
        )
        .await
        .context("join mesh")?;
        let my_ula = membership.my_ula();
        let addr = config
            .bind
            .unwrap_or_else(|| SocketAddr::new(my_ula.into(), config.port));
        tracing::info!(%my_ula, peer_id = %membership.peer_id(), %addr, "joined mesh");
        let id = membership.peer_id().to_owned();
        (
            addr,
            id,
            my_ula.to_string(),
            Some(my_ula.to_string()),
            Some(membership),
        )
    };

    // Build the orchestrator over the supervisord config. Runners live under
    // `<data_dir>/runners`; the orchestrator persists one record per runner there.
    let runner_dir = config.data_dir.join("runners");
    std::fs::create_dir_all(&runner_dir)
        .with_context(|| format!("create runner dir {}", runner_dir.display()))?;
    let shared = SharedRunnerConfig {
        runner_bin: default_runner_bin(),
        s3_base_url: config.s3_base_url.clone(),
        data_dir: config.data_dir.clone(),
        parent,
        no_mesh: config.no_mesh,
        // Forward the supervisor's explicit relay endpoint to every runner.
        relay_url: config.effective_relay_url(),
        // Forward the relay-only declaration so every spawned runner ALSO tells
        // the coordinator it has no reachable direct endpoint (the supervisor +
        // its runners share the host's NAT/firewall) — handshakes converge over
        // the relay instead of thrashing on unreachable direct dials.
        relay_only: config.relay_only,
    };
    let orchestrator = Orchestrator::new(shared, runner_dir);

    // Pre-start configured apps: spawn a runner per `--app` uuid (replaces the
    // old in-process pre-register). Best-effort — a transient failure must not
    // stop the supervisor from coming up and serving other apps.
    for uuid in &config.apps {
        let uuid_s = uuid.to_string();
        match orchestrator.start_app(&uuid_s, None).await {
            Ok(s) => tracing::info!(uuid = %uuid_s, app_ula = %s.app_ula, "pre-started app runner"),
            Err(e) => tracing::warn!(uuid = %uuid_s, error = %e, "pre-start failed (continuing)"),
        }
    }

    // Start the background monitor: it re-adopts the living runner fleet once on
    // startup (idempotent) and then ticks on an interval, respawning any dead
    // runner. We let `run_monitor` own the single readopt to avoid double-work.
    tokio::spawn(orchestrator.clone().run_monitor());

    // The discovery fetcher (GET /v1/apps/:uuid for an app with no runner).
    let fetcher = S3Fetcher::new(&config.s3_base_url, &config.data_dir);

    let state = SupervisorState::new(orchestrator, fetcher, supervisor_id, ula_str)
        .with_version(tabbify_supervisor::version::binary_version().to_owned())
        .with_firecracker(kvm)
        .with_docker(docker)
        .with_tap_subnet(config.firecracker.tap_subnet.clone());

    // Re-adopt persisted dev-sessions before serving: the dev-VM runners survive
    // a restart/OTA (KillMode=process) but the in-memory dev_sessions/git_sessions
    // do not, so without this the running dev-VMs are orphaned (invisible to
    // `dev_session_*`, git push 403s). Re-insert each from its on-disk sidecar so
    // it reappears in `GET /v1/dev-sessions`; the node's standing token sweep then
    // restores the git token. Runs unconditionally on every start (incl. OTA).
    tabbify_supervisor::api::readopt_dev_sessions(
        state.orchestrator.runner_dir(),
        &state.dev_sessions,
        &state.git_sessions,
    );

    // Re-adopt persisted WORKSPACES on the same principle: the workspace VMs
    // survive a restart/OTA but the in-memory registries do not. Re-register
    // every repo cap from each on-disk WorkspaceRecord so `git push` keeps
    // working; the node's token sweep mints fresh tokens.
    tabbify_supervisor::api::readopt_workspaces(
        state.orchestrator.runner_dir(),
        &state.git_sessions,
    );

    // Spawn the dev-session idle reaper now that `state` (and its Arc) exists.
    // Scans every 60 s for sessions that exceeded idle or max-TTL thresholds.
    // `state.clone()` shares the SAME registries as the router below: the
    // `dev_sessions` / `git_sessions` / orchestrator fields are Arcs, so the
    // clone is a handle, not a copy.
    let reaper_state = std::sync::Arc::new(state.clone());
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            tabbify_supervisor::api::sweep_expired(
                &reaper_state,
                tabbify_supervisor::api::DEV_SESSION_IDLE_TTL,
                tabbify_supervisor::api::DEV_SESSION_MAX_TTL,
            )
            .await;
        }
    });

    let app = router(state.clone());

    // ── IPv4 git-proxy listener (B1) ─────────────────────────────────────────
    // A separate listener on `0.0.0.0:GIT_PROXY_IPV4_PORT` so FC guests can
    // reach the git proxy via their tap default gateway (host_ip). FC VMs are
    // IPv4-only on a /30 tap — they have no IPv6/mesh access, so the mesh ULA
    // port 8730 is unreachable from inside. This listener shares the SAME
    // `GitSessions` Arc as the mesh router (no second registry).
    //
    // ORDERING: bind → install the iptables guard → THEN spawn `serve`. The
    // firewall (DROP inbound on the WiFi uplink, ACCEPT from the FC tap subnet)
    // is awaited BEFORE the serve task accepts any connection, so port 8788 is
    // never WiFi-reachable even for the ~hundreds of ms it takes to install the
    // rules. The 256-bit capability is the real auth gate; iptables is
    // depth-in-defence. Only installed on Linux (where FC runs).
    {
        let tap_subnet = config.firecracker.tap_subnet.clone();
        let ipv4_bind = SocketAddr::from(([0, 0, 0, 0], GIT_PROXY_IPV4_PORT));
        match TcpListener::bind(ipv4_bind).await {
            Ok(ipv4_listener) => {
                tracing::info!(
                    port = GIT_PROXY_IPV4_PORT,
                    "git proxy IPv4 listener bound (FC guest gateway reachable)"
                );

                // Install the firewall BEFORE serving so the exposure window
                // (bound but unguarded) is closed. Best-effort; only on Linux.
                #[cfg(target_os = "linux")]
                {
                    tabbify_supervisor::firecracker::setup_git_proxy_firewall(
                        &tap_subnet,
                        GIT_PROXY_IPV4_PORT,
                    )
                    .await;
                }
                #[cfg(not(target_os = "linux"))]
                let _ = tap_subnet;

                // Only NOW start accepting connections — the guard is in place.
                let shared_state = std::sync::Arc::new(state.clone());
                let ipv4_router = git_proxy_ipv4_router(shared_state);
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(ipv4_listener, ipv4_router).await {
                        tracing::error!(error = %e, "git proxy IPv4 listener error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(
                    port = GIT_PROXY_IPV4_PORT,
                    error = %e,
                    "git proxy IPv4 bind failed; FC guest git clone will not work"
                );
            }
        }
    }

    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("bind {bind_addr}"))?;
    tracing::info!(%bind_addr, "listening");

    // Signal systemd readiness EXACTLY ONCE, now that the control listener is
    // bound and (unless --no-mesh) the mesh is joined — i.e. the supervisor can
    // actually serve. The NixOS unit is `Type=notify` with TimeoutStartSec=60,
    // so without this the unit hangs to timeout and fails on every (re)start,
    // bricking a self-update restart. Best-effort: a no-op when NOTIFY_SOCKET is
    // unset (dev / --no-mesh / non-systemd), and any real error is logged and
    // swallowed inside `notify_ready`. The probe (`--check`) path returns early
    // above and never reaches here, so readiness is emitted only on real boot.
    tabbify_supervisor::readiness::notify_ready();

    // ── Crash-at-startup loop-guard: CLEAR on READY (spec §3.3) ──────────────
    // This boot reached READY — the binary can at least boot, bind, and serve —
    // so the crash-at-startup streak (and any `reverted_to` marker a prior revert
    // left) is cleared. The next incident starts from a clean slate. Guarded by
    // the same `is_boot_invocation` predicate as the bump so the two are
    // symmetric; in practice the self-update/--check/revert paths never reach
    // here (they exit earlier), but keeping the guard explicit documents the
    // invariant.
    if is_boot_invocation(&raw_args) {
        tabbify_supervisor::boot_health::BootAttempts::clear(&boot_data_dir);
    }

    // ── Track B tier-1: arm the independent watchdog-pet ─────────────────────
    // It pets systemd (WATCHDOG=1) every WatchdogSec/2 ONLY while the data plane
    // is healthy (Track-K `dataplane_healthy`, self-clocking). On a sustained
    // black hole it withholds the pet → systemd SIGKILL+restart → fresh register
    // + fresh boringtun Tunns + fresh relay-WS = fresh handshake. `KillMode=process`
    // keeps detached runners alive across the restart. No-op off systemd (no
    // WATCHDOG_USEC ⇒ dev / --no-mesh / non-systemd) and on `--no-mesh` (no
    // membership ⇒ no probe). relay_only is preserved across the restart by the
    // unit env (TABBIFY_MESH_RELAY_ONLY re-read on the fresh process).
    if let Some(membership) = _membership.as_ref() {
        tabbify_supervisor::watchdog_pet::spawn_watchdog_pet(
            membership.data_plane_probe(),
            config.relay_only,
            &config.data_dir,
        );
    }

    // ── Post-restart self-watchdog (spec §7) ────────────────────────────────
    // If a prior `self-update` swap stamped a pending-confirm marker in VERSION,
    // THIS boot is running an UNCONFIRMED binary. Spawn the audited watchdog
    // over the stability window against the LIVE local control addr: healthy
    // through the window -> clear the marker (confirm); failure -> roll the
    // symlinks back to previous-good + restart. This is how the engine's
    // watchdog/rollback actually runs in production. Skipped on `--no-mesh`
    // (the bind addr is not self-connectable; self-update is a meshed-node
    // concern). Best-effort: a watchdog spawn must never block serving.
    if !config.no_mesh {
        // Capture the data-plane probe from the live membership (held for the
        // process lifetime in `_membership`). `data_plane_probe` present ⇒ this
        // boot has a live membership handle, which `spawn_post_restart_watchdog`
        // uses as a PROXY for "the rollback target had the tunnel" to arm the D6
        // data-plane revert clause. ⚠ That proxy is imprecise — see the FIX 9
        // TODO in `spawn_post_restart_watchdog`; the honest per-version
        // `had_live_tunnel` bit is deferred.
        let data_plane = _membership.as_ref().map(MeshMembership::data_plane_probe);
        spawn_post_restart_watchdog(bind_addr, data_plane);
    }

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}

/// Run the production `self-update --to <version>` flow and return the process
/// exit code: 0 on a successful swap (or a no-op when already current), 1 on a
/// fetch / gate / swap failure (so the NixOS `tabbify-update` oneshot reports
/// the failure and leaves the live install untouched). Wires the REAL candidate
/// probe (spawn `supervisord --check`) + systemctl restart seams.
async fn run_self_update(to: &str) -> i32 {
    use tabbify_supervisor::selfupdate::SelfUpdateConfig;
    use tabbify_supervisor::selfupdate::run::{
        SelfUpdateOutcome, production_candidate_probe, self_update_to,
    };
    use tabbify_supervisor::selfupdate::swap::production_restart_runner;

    let cfg = SelfUpdateConfig::default();
    let probe = production_candidate_probe();
    let restart = production_restart_runner();

    match self_update_to(to, &cfg, &probe, &restart).await {
        Ok(SelfUpdateOutcome::AlreadyCurrent(v)) => {
            tracing::info!(version = %v, "self-update: already current, nothing to do");
            0
        }
        Ok(SelfUpdateOutcome::Swapped(v)) => {
            tracing::info!(version = %v, "self-update: swapped + restart triggered (watchdog will confirm/revert)");
            0
        }
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "self-update failed; live install left untouched");
            1
        }
    }
}

/// Exit codes for `revert-to-previous`, read by the `OnFailure=tabbify-boot-revert`
/// shell wrapper to decide its next step. DISTINCT codes let the script tell
/// "a revert was actually performed (now `reset-failed && start`)" from "no
/// previous to roll back to (O4 — escalate to reboot)" from "the revert itself
/// failed". These are the load-bearing contract between the Rust subcommand and
/// the nix OnFailure script (spec §3.5).
mod revert_exit {
    /// A revert was performed this fire (symlinks repointed + quarantine stamped):
    /// the OnFailure script may now `systemctl reset-failed && start`.
    pub const PERFORMED: i32 = 0;
    /// No completely-staged previous-good release (O4 first-boot bail) and
    /// `--reboot-on-exhausted` was NOT set: the script can re-invoke with
    /// `--reboot-on-exhausted` to escalate.
    pub const NO_PREVIOUS: i32 = 2;
    /// The revert was attempted but FAILED for some other reason (a symlink /
    /// VERSION write error) — distinct from NO_PREVIOUS so a real first boot is
    /// never confused with a broken install.
    pub const FAILED: i32 = 3;
    /// `--reboot-on-exhausted` was set, the revert was exhausted (no previous),
    /// and the reboot loop-guard PARKED (≤3/hr exhausted): leave the unit failed
    /// for a human (systemd `StartLimit` is the backstop).
    pub const REBOOT_PARKED: i32 = 4;
}

/// Run the audited `revert-to-previous` flow and return the process exit code
/// (see [`revert_exit`]). Wires the PRODUCTION seams: the default
/// [`SelfUpdateConfig`] layout, a NO-OP restart runner (systemd owns the restart
/// via the OnFailure script's `reset-failed && start`), the real `systemctl
/// reboot` reboot seam, and the shared host-wide `RebootGuard`
/// (`<data_dir>/reboot-guard.json` — the SAME ≤3/hr budget as Track B/C).
async fn run_revert_to_previous(reboot_on_exhausted: bool) -> i32 {
    use tabbify_supervisor::mesh_command::reboot_guard::RebootGuard;
    use tabbify_supervisor::mesh_command::sink::reboot_history_path;
    use tabbify_supervisor::selfupdate::SelfUpdateConfig;

    let cfg = SelfUpdateConfig::default();
    let data_dir = boot_health_data_dir();
    // The revert itself triggers NO restart — systemd owns it (the OnFailure
    // script does `reset-failed && start`). Pass a no-op restart runner so the
    // audited `watchdog::revert_to_previous` repoints symlinks + rewrites VERSION
    // only.
    let restart: tabbify_supervisor::selfupdate::swap::RestartRunner =
        std::sync::Arc::new(|_args| Box::pin(async { true }));
    // Real reboot seam: guarded `systemctl reboot` as a last resort.
    let guard = RebootGuard::new(reboot_history_path(&data_dir));
    let reboot = || {
        if !guard.try_reboot_now() {
            return false;
        }
        tracing::error!(
            "revert-to-previous: no previous-good release and --reboot-on-exhausted set \
             — rebooting as a last resort (guard slot consumed)"
        );
        let _ = std::process::Command::new("systemctl").arg("reboot").status();
        true
    };

    let outcome = revert_to_previous_flow(
        &cfg.install_dir,
        &cfg.releases_dir,
        &data_dir,
        reboot_on_exhausted,
        &restart,
        &reboot,
    )
    .await;
    outcome.exit_code()
}

/// What the revert flow did, mapped to a [`revert_exit`] code.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RevertFlowOutcome {
    /// A revert was performed; carries the rolled-back-to version.
    Performed(String),
    /// No previous-good release (O4 bail); `--reboot-on-exhausted` was not set.
    NoPrevious,
    /// No previous-good release; reboot escalation fired (host is rebooting).
    Rebooted,
    /// No previous-good release; reboot escalation was PARKED by the loop-guard.
    RebootParked,
    /// The revert was attempted but failed for another reason.
    Failed(String),
}

impl RevertFlowOutcome {
    const fn exit_code(&self) -> i32 {
        match self {
            // A reboot is in progress: exit cleanly (the box is going down).
            Self::Performed(_) | Self::Rebooted => revert_exit::PERFORMED,
            Self::NoPrevious => revert_exit::NO_PREVIOUS,
            Self::RebootParked => revert_exit::REBOOT_PARKED,
            Self::Failed(_) => revert_exit::FAILED,
        }
    }
}

/// The testable core of `revert-to-previous` (the production wiring is in
/// [`run_revert_to_previous`]). Reads VERSION to capture the reverted-FROM
/// version, runs the audited [`revert_to_previous`], and on success stamps the
/// reverted-FROM version into the VERSION `quarantine` list + records the revert
/// in the BootAttempts sidecar. On a no-previous bail (O4): with
/// `reboot_on_exhausted` it consults the injected `reboot` seam (guarded); else
/// it returns [`RevertFlowOutcome::NoPrevious`]. A non-bail error returns
/// [`RevertFlowOutcome::Failed`]. The `restart` + `reboot` collaborators are
/// injected so the flow is unit-testable without poking systemd.
///
/// FIX 2 (option-a, spec §3.5 intent — reboot-as-last-resort once already
/// reverted): when the BootAttempts sidecar already records a `reverted_to`
/// (we reverted once this streak and the REVERTED binary is ALSO crash-looping)
/// AND `--reboot-on-exhausted` is set, we SKIP a second `revert_to_previous`
/// (which would walk history DOWN to an even-older release — not the spec intent,
/// and a soft-brick if those are bad too) and go STRAIGHT to the guarded reboot
/// seam. Without the flag the historical behaviour is preserved (a deeper revert
/// is still attempted) so the no-reboot OnFailure path is unchanged.
async fn revert_to_previous_flow(
    install_dir: &std::path::Path,
    releases_dir: &std::path::Path,
    data_dir: &std::path::Path,
    reboot_on_exhausted: bool,
    restart: &tabbify_supervisor::selfupdate::swap::RestartRunner,
    reboot: &dyn Fn() -> bool,
) -> RevertFlowOutcome {
    use tabbify_supervisor::boot_health::BootAttempts;
    use tabbify_supervisor::selfupdate::swap::{read_version_file, write_version_file};
    use tabbify_supervisor::selfupdate::watchdog::revert_to_previous;

    // FIX 2: already-reverted + --reboot-on-exhausted ⇒ reboot-as-last-resort,
    // NOT a deeper revert. The reverted binary (recorded as `reverted_to` by the
    // prior `mark_reverted`) is itself crash-looping; reverting AGAIN just walks
    // the symlinks to an older-still release. Go straight to the guarded reboot
    // seam instead. (Without the flag we fall through to the normal revert below,
    // preserving the historical no-reboot OnFailure behaviour.)
    if reboot_on_exhausted && BootAttempts::load(data_dir).reverted_to.is_some() {
        tracing::warn!(
            "revert-to-previous: already reverted this streak and the reverted binary is \
             still crash-looping — reboot-as-last-resort (skipping a deeper revert)"
        );
        return if reboot() {
            RevertFlowOutcome::Rebooted
        } else {
            tracing::error!(
                "revert-to-previous: reboot loop-guard PARKED (already reverted) — leaving failed for a human"
            );
            RevertFlowOutcome::RebootParked
        };
    }

    // Capture the reverted-FROM version BEFORE the revert rewrites VERSION. A
    // missing/unreadable ledger means there is nothing to revert FROM either.
    let reverted_from = read_version_file(install_dir)
        .ok()
        .map(|vf| vf.current)
        .filter(|c| !c.is_empty());

    match revert_to_previous(install_dir, releases_dir, restart).await {
        Ok(rolled_back) => {
            // Stamp the reverted-FROM version into the quarantine list so the OTA
            // poller can never re-swap the known-bad release. `revert_to_previous`
            // already rewrote VERSION (preserving the existing quarantine), so we
            // re-read, append, and re-write — idempotent (the helper de-dups).
            if let Some(bad) = &reverted_from {
                match read_version_file(install_dir) {
                    Ok(vf) => {
                        let stamped = vf.quarantine_version(bad);
                        if let Err(e) = write_version_file(install_dir, &stamped) {
                            tracing::warn!(error = %format!("{e:#}"), "revert: quarantine stamp write failed");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "revert: re-read VERSION for quarantine failed");
                    }
                }
                // Record the revert in the boot-attempts sidecar: zero the streak
                // (the reverted binary gets its OWN fresh budget) and mark
                // `reverted_to` so a re-crash of the reverted binary escalates to
                // reboot-as-last-resort instead of reverting forever.
                BootAttempts::mark_reverted(&rolled_back, data_dir);
            }
            tracing::info!(version = %rolled_back, "revert-to-previous: rolled back to previous-good (systemd owns the restart)");
            RevertFlowOutcome::Performed(rolled_back)
        }
        Err(e) => {
            // Distinguish the O4 "no previous to roll back to" bail (which is
            // RECOVERABLE only by a reboot/park) from a genuine revert FAILURE.
            // `revert_to_previous` bails with "no previous-good" /
            // "completely-staged" in exactly the no-target cases.
            let msg = format!("{e:#}");
            let is_no_previous =
                msg.contains("no previous-good") || msg.contains("completely-staged");
            if !is_no_previous {
                tracing::error!(error = %msg, "revert-to-previous: revert FAILED");
                return RevertFlowOutcome::Failed(msg);
            }
            tracing::warn!(error = %msg, "revert-to-previous: no completely-staged previous-good release");
            if reboot_on_exhausted {
                if reboot() {
                    RevertFlowOutcome::Rebooted
                } else {
                    tracing::error!("revert-to-previous: reboot loop-guard PARKED — leaving failed for a human");
                    RevertFlowOutcome::RebootParked
                }
            } else {
                RevertFlowOutcome::NoPrevious
            }
        }
    }
}

/// Spawn the post-restart self-watchdog if VERSION records a pending-confirm
/// swap. The watchdog polls the live local `/health` + `/v1/about` over the
/// stability window; on confirm it clears the marker, on failure it rolls back
/// to previous-good + restarts. No-op (returns immediately) when there is no
/// pending swap — the steady-state boot path.
fn spawn_post_restart_watchdog(
    bind_addr: SocketAddr,
    data_plane: Option<std::sync::Arc<dyn Fn() -> bool + Send + Sync>>,
) {
    use tabbify_supervisor::selfupdate::DEFAULT_DATA_PLANE_WINDOW;
    use tabbify_supervisor::selfupdate::SelfUpdateConfig;
    use tabbify_supervisor::selfupdate::confirm::{
        confirm_or_revert, live_local_observe, pending_swap,
    };
    use tabbify_supervisor::selfupdate::swap::production_restart_runner;

    let cfg = SelfUpdateConfig::default();
    let Some(pending) = pending_swap(&cfg.install_dir) else {
        return; // steady state: nothing to confirm
    };
    tracing::info!(version = %pending, "post-restart self-watchdog: confirming pending swap");

    // ⚠ FIX 9 (known imprecision — see TODO): `prev_good_had_tunnel` is PROXIED
    // from `data_plane.is_some()`, i.e. "THIS boot has a live mesh membership
    // handle" — NOT the rollback target's ACTUAL tunnel history. The two usually
    // coincide (the previous-good is normally a build that ran healthy with a live
    // tunnel), but they are NOT the same fact:
    //   - if this boot has a membership handle but the previous-good build NEVER
    //     actually decapped a frame (e.g. it was confirmed purely on the
    //     control-plane gate during a data-plane outage), this proxy reports
    //     `true` and the watchdog's env-down fail-open guard (watchdog.rs §D1,
    //     `decide_revert`'s `previous_good_had_tunnel` clause) is DEFEATED: a dead
    //     data plane would trigger a revert to a target that also lacks the
    //     tunnel — a futile thrash the fail-open is meant to prevent.
    // The HONEST signal is a per-version `had_live_tunnel` bit recorded when a
    // build's OWN data-plane watchdog observed a live decap, read back for the
    // rollback target here. That is deferred (see TODO below) because the writer
    // would have to persist into the VERSION ledger CONCURRENTLY with the
    // swap/revert paths that also rewrite VERSION (a write-write race that needs
    // ledger-level locking to be safe) — out of scope for a one-shot. Until then:
    // no membership handle ⇒ treat the data plane as live (fail-open: the
    // control-plane gate still guards us); a present handle ⇒ arm the clause.
    //
    // TODO(fix-9): add `VersionFile.had_live_tunnel: HashMap<String,bool>`
    // (serde-default for back-compat), set `had_live_tunnel[current]=true` once a
    // build observes its first live decap (both on post-restart confirm AND on a
    // steady-state healthy boot — the rollback target is usually a long-running
    // previously-confirmed build, not a freshly-pending one), guard those writes
    // with a VERSION ledger lock shared with swap/revert, and read the rollback
    // target's bit HERE instead of `data_plane.is_some()`.
    let prev_good_had_tunnel = data_plane.is_some();
    let data_plane = data_plane.unwrap_or_else(|| std::sync::Arc::new(|| true));

    tokio::spawn(async move {
        let restart = production_restart_runner();
        let observe = live_local_observe(bind_addr, data_plane, prev_good_had_tunnel);
        // Poll every 2s; control window 45s, data-plane soak 120s (separate).
        let poll = std::time::Duration::from_secs(2);
        match confirm_or_revert(
            &cfg.install_dir,
            &cfg.releases_dir,
            cfg.stability_window,
            DEFAULT_DATA_PLANE_WINDOW,
            poll,
            observe,
            &restart,
        )
        .await
        {
            Ok(None) => tracing::info!("self-update confirmed by watchdog"),
            Ok(Some(rolled_back)) => {
                tracing::warn!(%rolled_back, "self-update reverted by watchdog")
            }
            Err(e) => tracing::error!(error = %format!("{e:#}"), "post-restart watchdog error"),
        }
    });
}

/// Run the out-of-band candidate probe (`--check`, spec §4) and return the
/// 3-part gate outcome. The candidate binds an alternate ephemeral loopback
/// control addr, serves the router, then gathers the three gate signals: the
/// binary launched + bound its control listener, `GET /health` 200, and control
/// liveness — a `GET /v1/about` round-trip on a DISTINCT route/handler (the
/// candidate has no per-app runners to answer the control `Cmd::Ping → Pong`,
/// so the plan folds pong into router liveness; we keep it honestly distinct
/// from `/health`).
///
/// The candidate does NOT join the production mesh. The gate answers "is the new
/// binary good enough to swap to?" — that it boots, binds, and serves its
/// control surface — which is exactly what a bad binary breaks and is
/// INDEPENDENT of whether a throwaway identity can acquire a TUN and join the
/// live coordinator. Requiring a real join was the root cause of the gate
/// failing on every production node (it needs root for the TUN and contends
/// with the live supervisor); the mesh fabric is exercised only by the full
/// process restart after a swap, and the post-swap watchdog already validates
/// health over local HTTP. See [`tabbify_supervisor::selfupdate::probe`].
///
/// All of this is bounded by the self-update gate timeout. The candidate still
/// requires a TRANSIENT identity path (the production probe always passes it)
/// so it never even incidentally touches the sticky identity; this entrypoint
/// fails closed if that transient identity is absent.
async fn run_check_mode(config: &Config) -> tabbify_supervisor::selfupdate::probe::ProbeOutcome {
    use tabbify_supervisor::selfupdate::probe::{GateInputs, ProbeOutcome, evaluate_gate};

    let gate_timeout = tabbify_supervisor::selfupdate::SelfUpdateConfig::default().gate_timeout;
    let started = std::time::Instant::now();

    // Fail closed if `--check` was passed without a TRANSIENT identity. The
    // production probe always passes `--candidate-identity-path`; enforcing it
    // here keeps the "candidate never touches the sticky identity" invariant
    // guaranteed rather than accidental, even though the candidate now runs
    // `--no-mesh` and never reaches the join path.
    if candidate_identity_required(config.check_mode, config.candidate_identity_path.is_some()) {
        return ProbeOutcome::Fail(
            "candidate (--check) requires --candidate-identity-path (transient identity)"
                .to_owned(),
        );
    }

    // The candidate always binds a loopback ephemeral addr for its self-check —
    // it must NOT contend for the sticky ULA / production bind.
    let bind_addr = config
        .bind
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));

    // 1, 2 & 3. Bring up the router on the alt bind and self-check both distinct
    //    liveness routes. The candidate runs WITHOUT a production mesh join: the
    //    `launched` signal is set once the control listener binds (a bad binary
    //    crashes on boot or fails to bind before this), and the two HTTP probes
    //    cover `/health` + the distinct `/v1/about` liveness route.
    let runner_dir = config.data_dir.join("candidate-runners");
    if let Err(e) = std::fs::create_dir_all(&runner_dir) {
        return tabbify_supervisor::selfupdate::probe::ProbeOutcome::Fail(format!(
            "candidate runner dir: {e}"
        ));
    }
    let shared = SharedRunnerConfig {
        runner_bin: default_runner_bin(),
        s3_base_url: config.s3_base_url.clone(),
        data_dir: config.data_dir.clone(),
        parent: None,
        no_mesh: true,
        // The candidate forwards the relay endpoint too, so its probe runner
        // exercises the same relay path as production.
        relay_url: config.effective_relay_url(),
        // The candidate forwards the relay-only declaration too, so its probe
        // runner exercises the same single-sided-handshake path as production.
        relay_only: config.relay_only,
    };
    let orchestrator = Orchestrator::new(shared, runner_dir);
    let fetcher = S3Fetcher::new(&config.s3_base_url, &config.data_dir);
    let state = SupervisorState::new(
        orchestrator,
        fetcher,
        "candidate".to_owned(),
        bind_addr.ip().to_string(),
    )
    .with_version(tabbify_supervisor::version::binary_version().to_owned());
    let app = router(state);

    // The `launched` signal: the binary booted far enough to bind its control
    // listener. A bad binary panics on boot or fails to bind before this — the
    // decoupled stand-in for the old "joined the mesh" signal.
    let (launched, health_200, pong) = match TcpListener::bind(bind_addr).await {
        Ok(listener) => {
            let local = listener.local_addr().ok();
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let (health_200, pong) = match local {
                Some(addr) => self_check(addr, gate_timeout).await,
                None => (false, false),
            };
            server.abort();
            (true, health_200, pong)
        }
        Err(e) => {
            tracing::error!(error = %e, %bind_addr, "candidate failed to bind control addr");
            (false, false, false)
        }
    };

    let inputs = GateInputs {
        launched,
        health_200,
        pong,
        elapsed_secs: started.elapsed().as_secs(),
    };
    evaluate_gate(inputs, gate_timeout.as_secs())
}

/// Whether the probe entrypoint must fail closed for a missing transient
/// identity.
///
/// `--check` declares an out-of-band candidate. The production probe always
/// passes `--candidate-identity-path`; enforcing it here keeps the "candidate
/// never touches the sticky identity" invariant guaranteed rather than
/// accidental, even though the candidate runs `--no-mesh` and never reaches the
/// join path. Returns `true` when the entrypoint must abort.
#[must_use]
fn candidate_identity_required(check_mode: bool, has_candidate_identity: bool) -> bool {
    check_mode && !has_candidate_identity
}

/// The two DISTINCT router routes the self-check probes, one per gate signal:
/// gate part 2 (`health_200`) hits `/health`, gate part 3 (`pong`) hits a
/// genuinely different liveness route (`/v1/about`). Returning them from one
/// place keeps the gate honestly 3-part: each part exercises its own handler.
const HEALTH_PATH: &str = "/health";
const LIVENESS_PATH: &str = "/v1/about";

/// Self-check the candidate's router with two DISTINCT liveness signals so the
/// gate stays honestly 3-part: gate part 2 (`health_200`) probes `/health`, and
/// gate part 3 (`pong`) probes `/v1/about` — a different route and handler
/// (`about` vs `health`), standing in for the control `Cmd::Ping → Pong` the
/// candidate has no per-app runner to answer. Both must return 2xx. Bounded by
/// `timeout`.
async fn self_check(addr: SocketAddr, timeout: std::time::Duration) -> (bool, bool) {
    let client = reqwest::Client::new();
    let probe = |c: reqwest::Client, u: String| async move {
        matches!(c.get(&u).send().await, Ok(r) if r.status().is_success())
    };
    let both = async {
        let health_200 = probe(client.clone(), format!("http://{addr}{HEALTH_PATH}")).await;
        let pong = probe(client.clone(), format!("http://{addr}{LIVENESS_PATH}")).await;
        (health_200, pong)
    };
    tokio::time::timeout(timeout, both)
        .await
        .unwrap_or((false, false))
}

/// The data dir the crash-at-startup loop-guard persists under, read directly
/// from `SUPERVISOR_DATA_DIR` (the SAME env the clap [`Config`] uses,
/// config.rs:114) with the same `/var/lib/tabbify` default. Resolved WITHOUT a
/// full config parse so the bump can run before `Config::from_env` — a
/// config-parse crash on a real boot is then still counted.
fn boot_health_data_dir() -> std::path::PathBuf {
    std::env::var("SUPERVISOR_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/var/lib/tabbify"))
}

/// Whether this invocation is a REAL daemon boot (so the crash-at-startup
/// loop-guard should bump/clear its counter), as opposed to a run-to-completion
/// subcommand (`self-update`, `revert-to-previous`) or an out-of-band candidate
/// (`--check`). Those are NOT "boots": a `self-update` deliberately exits, a
/// candidate is a throwaway probe, and a `revert-to-previous` is the loop-guard's
/// OWN remediation — counting any of them would poison the streak (and a
/// `revert-to-previous` counting itself would be a feedback loop). Pure over the
/// argv slice (everything after `argv[0]`) so it is unit-testable.
#[must_use]
fn is_boot_invocation(args: &[String]) -> bool {
    // `--check` (the out-of-band candidate) appears as a flag anywhere in argv.
    if args.iter().any(|a| a == "--check") {
        return false;
    }
    // The run-to-completion subcommands are the FIRST non-flag token (clap
    // subcommand position). Scanning for the bare token is robust to leading
    // global flags and matches how clap dispatches the subcommand.
    if args
        .iter()
        .any(|a| a == "self-update" || a == "revert-to-previous")
    {
        return false;
    }
    true
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tabbify_supervisor=debug,supervisord=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tabbify_supervisor::api::{SupervisorState, router};

    // ── Fix 1: candidate must fail closed without a transient identity ──────

    #[test]
    fn check_mode_without_candidate_identity_requires_failing_closed() {
        // `--check` set, but no `--candidate-identity-path` → must abort so the
        // candidate never even incidentally touches the sticky identity.
        assert!(candidate_identity_required(true, false));
    }

    #[test]
    fn check_mode_with_candidate_identity_is_allowed() {
        assert!(!candidate_identity_required(true, true));
    }

    #[test]
    fn non_check_mode_never_requires_failing_closed() {
        // Production boot (no --check) is unaffected regardless of the identity flag.
        assert!(!candidate_identity_required(false, false));
        assert!(!candidate_identity_required(false, true));
    }

    // ── revert-to-previous flow (crash-at-startup catch-net) ────────────────

    use tabbify_supervisor::boot_health::BootAttempts;
    use tabbify_supervisor::selfupdate::swap::{
        RestartRunner, VersionFile, read_version_file, repoint_symlink, write_version_file,
    };

    /// The two binaries the swap/revert re-points (mirrors swap::SWAP_BINARIES,
    /// which is crate-private to the library and not reachable from the bin).
    const TEST_SWAP_BINARIES: [&str; 2] = ["supervisord", "tabbify-runner"];

    /// A no-op restart runner (systemd owns the restart in production).
    fn noop_restart() -> RestartRunner {
        std::sync::Arc::new(|_args| Box::pin(async { true }))
    }

    /// Stage `version`'s binaries under `<releases>/<version>/` as runnable files
    /// so `release_is_complete` accepts the release as a rollback target.
    fn stage(releases: &std::path::Path, version: &str) {
        use std::os::unix::fs::PermissionsExt;
        let dir = releases.join(version);
        std::fs::create_dir_all(&dir).unwrap();
        for bin in TEST_SWAP_BINARIES {
            let p = dir.join(bin);
            std::fs::write(&p, format!("{version}-{bin}")).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    /// Happy path: a complete previous-good release exists → the flow rolls the
    /// symlinks back to v1, rewrites VERSION (current=v1), STAMPS the reverted-from
    /// v2 into quarantine, and records the revert in the BootAttempts sidecar.
    #[tokio::test]
    async fn revert_flow_rolls_back_quarantines_and_marks_reverted() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        for ver in ["v1.0.0", "v2.0.0"] {
            stage(&releases, ver);
        }
        for bin in TEST_SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();

        let restart = noop_restart();
        let reboot = || panic!("reboot must NOT fire when a revert succeeds");
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, false, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::Performed("v1.0.0".to_owned()));
        assert_eq!(outcome.exit_code(), revert_exit::PERFORMED);

        // Symlinks rolled back to v1.0.0.
        for bin in TEST_SWAP_BINARIES {
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v1.0.0-{bin}").into_bytes(),
            );
        }
        // VERSION: current=v1, the reverted-from v2 is QUARANTINED.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v1.0.0");
        assert_eq!(
            vf.quarantine,
            vec!["v2.0.0".to_owned()],
            "the reverted-from version must be stamped into quarantine",
        );
        // BootAttempts: streak reset, reverted_to recorded.
        let ba = BootAttempts::load(&data_dir);
        assert_eq!(ba.count, 0);
        assert_eq!(ba.reverted_to.as_deref(), Some("v1.0.0"));
    }

    /// O4 first-boot: an EMPTY `previous[]` → the audited revert bails. WITHOUT
    /// `--reboot-on-exhausted` the flow returns NoPrevious gracefully (NOT a panic,
    /// NOT a reboot) so a genuinely-first-boot box is never reboot-looped.
    #[tokio::test]
    async fn revert_flow_no_previous_is_graceful_without_reboot() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec![],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();

        let restart = noop_restart();
        let reboot = || panic!("reboot must NOT fire without --reboot-on-exhausted");
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, false, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::NoPrevious);
        assert_eq!(outcome.exit_code(), revert_exit::NO_PREVIOUS);
        // VERSION untouched, nothing quarantined.
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0");
        assert!(vf.quarantine.is_empty());
    }

    /// O4 first-boot WITH `--reboot-on-exhausted`: an empty `previous[]` bails →
    /// the flow consults the (injected) reboot seam. When the seam grants a slot
    /// the outcome is Rebooted (exit 0 — the box is going down).
    #[tokio::test]
    async fn revert_flow_no_previous_reboots_when_exhausted_flag_set() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec![],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();

        let restart = noop_restart();
        let fired = std::cell::Cell::new(false);
        let reboot = || {
            fired.set(true);
            true // guard granted a slot
        };
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, true, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::Rebooted);
        assert_eq!(outcome.exit_code(), revert_exit::PERFORMED);
        assert!(fired.get(), "the reboot seam must fire on exhausted+flag");
    }

    /// O4 first-boot WITH `--reboot-on-exhausted` but the loop-guard PARKS (seam
    /// returns false) → RebootParked (a distinct exit code so the OnFailure script
    /// leaves the unit failed for a human instead of looping).
    #[tokio::test]
    async fn revert_flow_no_previous_parks_when_reboot_guard_exhausted() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec![],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();

        let restart = noop_restart();
        let reboot = || false; // guard exhausted → parked
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, true, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::RebootParked);
        assert_eq!(outcome.exit_code(), revert_exit::REBOOT_PARKED);
    }

    /// FIX 2 (option-a, spec-matching): the reverted binary ALSO crash-loops.
    /// When `reverted_to` is ALREADY set in the BootAttempts sidecar AND
    /// `--reboot-on-exhausted` is passed, the flow must NOT walk history down with
    /// ANOTHER symlink revert — it must go STRAIGHT to the reboot seam. A
    /// COMPLETE previous-good (v1) is staged so the OLD behaviour would have
    /// reverted to it; this test proves we reboot instead and leave the symlinks
    /// pointing at v2 (no deeper revert).
    #[tokio::test]
    async fn revert_flow_reboots_when_already_reverted_even_with_previous_present() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        for ver in ["v1.0.0", "v2.0.0"] {
            stage(&releases, ver);
        }
        for bin in TEST_SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();
        // Already reverted once this streak (the reverted binary is now crash-looping).
        BootAttempts::mark_reverted("v2.0.0", &data_dir);

        let restart = noop_restart();
        let fired = std::cell::Cell::new(false);
        let reboot = || {
            fired.set(true);
            true // guard granted a slot
        };
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, true, &restart, &reboot,
        )
        .await;
        assert_eq!(
            outcome,
            RevertFlowOutcome::Rebooted,
            "already-reverted + --reboot-on-exhausted must reboot, not revert deeper",
        );
        assert_eq!(outcome.exit_code(), revert_exit::PERFORMED);
        assert!(fired.get(), "the reboot seam must fire when already reverted");
        // NO deeper revert: the symlinks + VERSION still point at v2 (untouched),
        // and nothing new was quarantined.
        for bin in TEST_SWAP_BINARIES {
            assert_eq!(
                std::fs::read(install.join(bin)).unwrap(),
                format!("v2.0.0-{bin}").into_bytes(),
                "symlinks must NOT walk down to v1 when already reverted",
            );
        }
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0", "VERSION must stay at v2 — no deeper revert");
        assert!(vf.quarantine.is_empty(), "no new quarantine on a reboot-only path");
    }

    /// FIX 2 companion: already-reverted + `--reboot-on-exhausted` but the
    /// loop-guard PARKS (seam returns false) → RebootParked, and STILL no deeper
    /// revert (symlinks stay at v2).
    #[tokio::test]
    async fn revert_flow_parks_when_already_reverted_and_guard_exhausted() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        for ver in ["v1.0.0", "v2.0.0"] {
            stage(&releases, ver);
        }
        for bin in TEST_SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();
        BootAttempts::mark_reverted("v2.0.0", &data_dir);

        let restart = noop_restart();
        let reboot = || false; // guard exhausted → parked
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, true, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::RebootParked);
        assert_eq!(outcome.exit_code(), revert_exit::REBOOT_PARKED);
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v2.0.0", "no deeper revert even when parked");
    }

    /// FIX 2 negative guard: already-reverted but WITHOUT `--reboot-on-exhausted`
    /// still performs a deeper revert (the historical behaviour is preserved when
    /// reboot is not requested — the reboot shortcut is gated on the flag).
    #[tokio::test]
    async fn revert_flow_still_reverts_when_already_reverted_but_no_reboot_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let install = tmp.path();
        let releases = install.join("releases");
        let data_dir = install.join("data");
        for ver in ["v1.0.0", "v2.0.0"] {
            stage(&releases, ver);
        }
        for bin in TEST_SWAP_BINARIES {
            repoint_symlink(install, bin, &releases.join("v2.0.0").join(bin)).unwrap();
        }
        write_version_file(
            install,
            &VersionFile {
                current: "v2.0.0".into(),
                previous: vec!["v1.0.0".into()],
                pending_confirm: None,
                quarantine: Vec::new(),
            },
        )
        .unwrap();
        BootAttempts::mark_reverted("v2.0.0", &data_dir);

        let restart = noop_restart();
        let reboot = || panic!("reboot must NOT fire without --reboot-on-exhausted");
        let outcome = revert_to_previous_flow(
            install, &releases, &data_dir, false, &restart, &reboot,
        )
        .await;
        assert_eq!(outcome, RevertFlowOutcome::Performed("v1.0.0".to_owned()));
        let vf = read_version_file(install).unwrap();
        assert_eq!(vf.current, "v1.0.0", "without the reboot flag, a deeper revert still happens");
    }

    // ── boot-health loop-guard: which invocations count as a real boot ──────

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn bare_daemon_boot_is_a_boot_invocation() {
        // The plain daemon boot (and one with global flags) bumps the counter.
        assert!(is_boot_invocation(&args(&[])));
        assert!(is_boot_invocation(&args(&["--data-dir", "/var/lib/tabbify"])));
        assert!(is_boot_invocation(&args(&["--relay-only"])));
    }

    #[test]
    fn self_update_is_not_a_boot_invocation() {
        // `self-update --to vX` runs to completion and exits — never a boot.
        assert!(!is_boot_invocation(&args(&["self-update", "--to", "v1.4.0"])));
    }

    #[test]
    fn revert_to_previous_is_not_a_boot_invocation() {
        // The loop-guard's OWN remediation must never count itself (feedback loop).
        assert!(!is_boot_invocation(&args(&["revert-to-previous"])));
        assert!(!is_boot_invocation(&args(&[
            "revert-to-previous",
            "--reboot-on-exhausted"
        ])));
    }

    #[test]
    fn check_candidate_is_not_a_boot_invocation() {
        // The out-of-band `--check` candidate is a throwaway probe, not a boot.
        assert!(!is_boot_invocation(&args(&[
            "--check",
            "--candidate-identity-path",
            "/tmp/id.json"
        ])));
    }

    // ── Fix 2: gate part 3 (pong) probes a DISTINCT route from part 2 ───────

    #[test]
    fn liveness_probe_is_a_distinct_route_from_health() {
        assert_eq!(HEALTH_PATH, "/health");
        assert_eq!(LIVENESS_PATH, "/v1/about");
        assert_ne!(
            HEALTH_PATH, LIVENESS_PATH,
            "gate part 3 must exercise a different route than part 2"
        );
    }

    /// End-to-end: the candidate router answers BOTH gate routes 2xx, and each is
    /// a genuinely distinct signal — `self_check` returns `(true, true)` because
    /// `/health` AND `/v1/about` each succeed on their own handler.
    #[tokio::test]
    async fn self_check_exercises_both_distinct_routes() {
        let tmp = std::env::temp_dir().join(format!("su3-selfcheck-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let shared = SharedRunnerConfig {
            runner_bin: default_runner_bin(),
            s3_base_url: "http://127.0.0.1:1/none".to_owned(),
            data_dir: tmp.clone(),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        };
        let orchestrator = Orchestrator::new(shared, tmp.join("runners"));
        let fetcher = S3Fetcher::new("http://127.0.0.1:1/none", &tmp);
        let state = SupervisorState::new(
            orchestrator,
            fetcher,
            "candidate".to_owned(),
            "127.0.0.1".to_owned(),
        )
        .with_version("0.0.0-test".to_owned());
        let app = router(state);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let (health_200, pong) = self_check(addr, std::time::Duration::from_secs(5)).await;
        server.abort();
        let _ = std::fs::remove_dir_all(&tmp);

        assert!(health_200, "/health must answer 200");
        assert!(
            pong,
            "/v1/about must answer 200 as the distinct part-3 signal"
        );
    }
}
