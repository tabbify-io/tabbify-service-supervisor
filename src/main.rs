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
    let capability_tags = tabbify_supervisor::capability_tags::capability_tags(kvm, docker);
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

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tabbify_supervisor=debug,supervisord=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
