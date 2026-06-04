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
use tabbify_supervisor::api::{SupervisorState, router};
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
        tracing::info!("designated BUILDER (SUPERVISOR_BUILDER) — advertising `builder` capability");
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
        .with_docker(docker);
    let app = router(state);

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
        spawn_post_restart_watchdog(bind_addr);
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

/// Spawn the post-restart self-watchdog if VERSION records a pending-confirm
/// swap. The watchdog polls the live local `/health` + `/v1/about` over the
/// stability window; on confirm it clears the marker, on failure it rolls back
/// to previous-good + restarts. No-op (returns immediately) when there is no
/// pending swap — the steady-state boot path.
fn spawn_post_restart_watchdog(bind_addr: SocketAddr) {
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

    tokio::spawn(async move {
        let restart = production_restart_runner();
        let observe = live_local_observe(bind_addr);
        // Poll every 2s through the stability window (default 45s < heartbeat).
        let poll = std::time::Duration::from_secs(2);
        match confirm_or_revert(
            &cfg.install_dir,
            &cfg.releases_dir,
            cfg.stability_window,
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
/// 3-part gate outcome. The candidate joins the mesh with a TRANSIENT identity
/// (`candidate_identity_path`), binds an alternate ephemeral loopback control
/// addr, serves the router, then gathers the three gate signals: joined mesh
/// (membership Ok), `GET /health` 200, and control liveness — a `GET /v1/about`
/// round-trip on a DISTINCT route/handler (the candidate has no per-app runners
/// to answer the control `Cmd::Ping → Pong`, so the plan folds pong into router
/// liveness; we keep it honestly distinct from `/health`).
///
/// All of this is bounded by the self-update gate timeout. The candidate NEVER
/// claims the sticky ULA: it joins with its own `candidate_identity_path`, and
/// this entrypoint fails closed if that transient identity is absent.
async fn run_check_mode(config: &Config) -> tabbify_supervisor::selfupdate::probe::ProbeOutcome {
    use tabbify_supervisor::selfupdate::probe::{GateInputs, ProbeOutcome, evaluate_gate};

    let gate_timeout = tabbify_supervisor::selfupdate::SelfUpdateConfig::default().gate_timeout;
    let started = std::time::Instant::now();

    // Fail closed if `--check` was passed without a TRANSIENT identity. Without
    // `--candidate-identity-path` the pinned joiner (mesh-joiner de17a58,
    // `resolve_identity`) falls through to the legacy keypair-only path
    // (`~/.tabbify-mesh/keypair`, `sticky_ula = None`), so the sticky ULA is not
    // claimed only *incidentally* — the safety hinges on an implicit caller
    // contract. SU-3 requires a transient identity (spec §4); enforce it here so
    // the "candidate never claims the sticky ULA" invariant is guaranteed, not
    // accidental, rather than silently joining via the ambient keypair path.
    if candidate_identity_required(config.check_mode, config.candidate_identity_path.is_some()) {
        return ProbeOutcome::Fail(
            "candidate (--check) requires --candidate-identity-path (transient identity); \
             refusing to join via the ambient keypair path"
                .to_owned(),
        );
    }

    // The candidate always binds a loopback ephemeral addr for its self-check —
    // it must NOT contend for the sticky ULA / production bind.
    let bind_addr = config
        .bind
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));

    // 1. Join the mesh with the TRANSIENT identity (separate file, OS-ephemeral
    //    WG port via the joiner default). On a host without root/TUN this fails
    //    and the gate fails closed.
    // The candidate probe advertises the same capabilities a real boot
    // would, including the operator's builder designation.
    let capability_tags = tabbify_supervisor::capability_tags::capability_tags(
        tabbify_supervisor::firecracker::kvm_available(),
        tabbify_supervisor::docker::docker_available(),
        config.builder,
    );
    let joined_mesh = if config.no_mesh {
        true
    } else {
        match MeshMembership::join(
            &config.coordinator_url,
            &config.display_name,
            &capability_tags,
            tabbify_supervisor::mesh::JoinMetadata {
                identity_path: config.candidate_identity_path.clone(),
                software_version: Some(tabbify_supervisor::version::binary_version().to_owned()),
                ..Default::default()
            },
            config.effective_relay_url(),
        )
        .await
        {
            Ok(_membership) => true,
            Err(e) => {
                tracing::error!(error = %e, "candidate failed to join mesh with transient identity");
                false
            }
        }
    };

    // 2 & 3. Bring up the router on the alt bind and self-check `/health`.
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

    let (health_200, pong) = match TcpListener::bind(bind_addr).await {
        Ok(listener) => {
            let local = listener.local_addr().ok();
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let result = match local {
                Some(addr) => self_check(addr, gate_timeout).await,
                None => (false, false),
            };
            server.abort();
            result
        }
        Err(e) => {
            tracing::error!(error = %e, %bind_addr, "candidate failed to bind control addr");
            (false, false)
        }
    };

    let inputs = GateInputs {
        joined_mesh,
        health_200,
        pong,
        elapsed_secs: started.elapsed().as_secs(),
    };
    evaluate_gate(inputs, gate_timeout.as_secs())
}

/// Whether the probe entrypoint must fail closed before joining the mesh.
///
/// `--check` declares an out-of-band candidate that MUST join with a TRANSIENT
/// identity (`--candidate-identity-path`). With no transient identity the pinned
/// joiner silently uses the ambient keypair path, so refusing here keeps the
/// "candidate never claims the sticky ULA" invariant enforced rather than
/// incidental. Returns `true` when the entrypoint must abort.
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
        // `--check` set, but no `--candidate-identity-path` → must abort before
        // joining via the ambient keypair path.
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
