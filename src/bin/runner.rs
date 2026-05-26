//! Thin entrypoint for `tabbify-runner`: parse config, init logging, start the
//! per-app serve core (loopback path for now; mesh join deferred to Task 1.3),
//! bind the control socket, and run until shutdown.
//!
//! All logic lives in the `tabbify_supervisor` library; this file only wires
//! the pieces together — matching the `supervisord` `main.rs` pattern.

use anyhow::Context;
use clap::Parser;
use tabbify_supervisor::RunnerConfig;
use tabbify_supervisor::runner::control;
use tabbify_supervisor::runner::serve::RunnerServe;
use tabbify_supervisor::runner::wire::serve_config_from;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cfg = RunnerConfig::parse();
    tracing::info!(
        uuid   = %cfg.uuid,
        s3     = %cfg.s3_base_url,
        no_mesh = cfg.no_mesh,
        control_sock = %cfg.control_sock.display(),
        "starting tabbify-runner"
    );

    let serve_cfg = serve_config_from(&cfg);

    // Start the per-app serve core. In `--no-mesh` mode this binds a loopback
    // listener; when `no_mesh = false` the mesh path is deferred (Task 1.3) and
    // will bail with an explicit error so the binary fails loudly.
    let runner = RunnerServe::start(serve_cfg)
        .await
        .context("start runner serve")?;

    let addr = runner.addr();
    tracing::info!(%addr, uuid = %cfg.uuid, "per-app listener bound");

    // Spawn the control socket server. It runs for the process lifetime;
    // the lifecycle handle it holds keeps the HostedApp alive until Stop/Purge.
    let lifecycle = runner.lifecycle();
    let sock_path = cfg.control_sock.clone();
    tokio::spawn(async move {
        if let Err(e) = control::serve(sock_path, lifecycle).await {
            tracing::error!(error = %e, "control server exited with error");
        }
    });

    tracing::info!(
        control_sock = %cfg.control_sock.display(),
        "control socket listening; runner ready"
    );

    // Park the main task — the runner lives until the process is signalled or
    // the control server issues Shutdown (which calls `std::process::exit`).
    std::future::pending::<()>().await;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tabbify_supervisor=debug,tabbify_runner=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
