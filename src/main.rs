//! Thin entrypoint: parse config, init logging, join the mesh, build the app
//! registry, pre-register `--app` uuids, spawn the idle reaper, bind the
//! control/serve listener, and run the axum server. All logic lives in the
//! `tabbify_supervisor` library.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use tabbify_supervisor::api::{SupervisorState, router};
use tabbify_supervisor::config::Config;
use tabbify_supervisor::docker::docker_available;
use tabbify_supervisor::fetcher::S3Fetcher;
use tabbify_supervisor::firecracker::kvm_available;
use tabbify_supervisor::host::AppHost;
use tabbify_supervisor::mesh::MeshMembership;
use tabbify_supervisor::registry::AppRegistry;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

/// How often the idle reaper runs.
const REAP_INTERVAL: Duration = Duration::from_secs(10);

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

    let fetcher = S3Fetcher::new(&config.s3_base_url, &config.data_dir);

    // Runtime capability gates. Each capable runtime advertises a mesh tag so
    // the coordinator/node route an app of that runtime to a supervisor that can
    // host it; WASM is always available. A host advertises only what it can run.
    let mut capability_tags: Vec<String> = Vec::new();

    // KVM capability: a host with /dev/kvm can run firecracker microVMs.
    let kvm = kvm_available();
    if kvm {
        tracing::info!("KVM available (/dev/kvm) — advertising `firecracker` capability");
        capability_tags.push("firecracker".to_owned());
    } else {
        tracing::info!("no /dev/kvm — firecracker apps unsupported on this host");
    }

    // Docker capability: a host with a reachable Docker daemon can build + run
    // docker apps (cross-platform — macOS + Linux).
    let docker = docker_available();
    if docker {
        tracing::info!("Docker daemon reachable — advertising `docker` capability");
        capability_tags.push("docker".to_owned());
    } else {
        tracing::info!("no Docker daemon — docker apps unsupported on this host");
    }

    if capability_tags.is_empty() {
        tracing::info!("WASM-only supervisor (no firecracker / docker capability)");
    }

    // Join the mesh (unless --no-mesh). The membership is held for the process
    // lifetime so the TUN device + WG background tasks stay up. The CONTROL API
    // binds the peer-ULA; each hosted app binds its OWN app-ULA via `app_host`.
    let (bind_addr, supervisor_id, ula_str, app_host, _membership) = if config.no_mesh {
        let addr = config
            .bind
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], config.port)));
        tracing::warn!(
            %addr,
            "running WITHOUT mesh (--no-mesh): control on plain addr, apps on loopback"
        );
        // No TUN → apps can't bind app-ULAs; host them on loopback instead.
        (
            addr,
            "local".to_owned(),
            addr.ip().to_string(),
            AppHost::loopback(),
            None,
        )
    } else {
        // The supervisor's own sticky-identity join (requested_ula / kind /
        // parent / app_uuid) is Phase 2; pass the default (all `None`) for now.
        let membership = MeshMembership::join(
            &config.coordinator_url,
            &config.display_name,
            &capability_tags,
            tabbify_supervisor::mesh::JoinMetadata::default(),
        )
        .await
        .context("join mesh")?;
        let my_ula = membership.my_ula();
        // Bind the CONTROL listener on the peer-ULA unless an explicit --bind
        // override is set.
        let addr = config
            .bind
            .unwrap_or_else(|| SocketAddr::new(my_ula.into(), config.port));
        tracing::info!(%my_ula, peer_id = %membership.peer_id(), %addr, "joined mesh");
        let id = membership.peer_id().to_owned();
        // App listeners bind `[app_ula]:port` and advertise via the joiner.
        let app_host = AppHost::mesh(membership.mesh_host(), config.port);
        (addr, id, my_ula.to_string(), app_host, Some(membership))
    };

    let registry = AppRegistry::with_runtime_configs(
        fetcher,
        app_host,
        config.firecracker.clone(),
        config.docker.clone(),
    );

    // Pre-register configured apps (fetch metadata; always_on spawns now).
    for uuid in &config.apps {
        let uuid_s = uuid.to_string();
        match registry.register(&uuid_s).await {
            Ok(state) => {
                tracing::info!(uuid = %uuid_s, state = ?state, "pre-registered app");
            }
            Err(e) => {
                // Pre-registration is best-effort: a transient S3 error must not
                // stop the supervisor from coming up and serving other apps.
                tracing::warn!(uuid = %uuid_s, error = %e, "pre-register failed (continuing)");
            }
        }
    }

    // Idle reaper: periodically stop idle on_request instances.
    spawn_reaper(registry.clone());

    let state = SupervisorState::new(registry, supervisor_id, ula_str)
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

/// Spawn the background idle-reaper loop.
fn spawn_reaper(registry: AppRegistry) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(REAP_INTERVAL);
        loop {
            tick.tick().await;
            let reaped = registry.reap_idle().await;
            if !reaped.is_empty() {
                tracing::info!(?reaped, "reaped idle on_request instances");
            }
        }
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tabbify_supervisor=debug,supervisord=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
