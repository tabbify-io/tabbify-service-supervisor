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
use tabbify_supervisor::runner::serve::{RunnerExit, RunnerServe, run_until_exit};
use tabbify_supervisor::runner::wire::serve_config_from;
// `ActiveRuntime` implements `AppRuntime`; the trait must be in scope so the
// clean-shutdown path can call `runtime.shutdown()` on the active-runtime cell.
use tabbify_supervisor::runtime::AppRuntime;
use tokio::sync::oneshot;
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

    // Wire the shutdown notifier: when the control server dispatches Shutdown it
    // sends on this channel, which unblocks the main select below.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let lifecycle = runner.lifecycle();
    lifecycle.set_shutdown_tx(shutdown_tx).await;

    // Spawn the control socket server. It runs for the process lifetime;
    // the lifecycle handle it holds keeps the HostedApp alive until Stop/Purge.
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

    // Fail-fast loop: select between the active runtime dying (crash) and a
    // clean shutdown signal from the control server. `runtime` is the swappable
    // `ActiveRuntime` cell so `run_until_exit` can re-arm its watch across
    // zero-downtime swaps (P2.3).
    let runtime = runner.runtime();
    match run_until_exit(runtime.clone(), shutdown_rx).await {
        RunnerExit::Crashed(reason) => {
            // The runtime already died; Drop + the L2 kill-before-respawn
            // handle remnants. Do NOT call shutdown() here.
            tracing::error!(reason = %reason, "app runtime died unexpectedly; exiting(1) for respawn");
            std::process::exit(1);
        }
        RunnerExit::CleanShutdown => {
            // Graceful stop: release the runtime's external resources (stop the
            // container / VM) before the process exits. shutdown() is idempotent
            // so Drop's best-effort cleanup is harmless if it also runs.
            tracing::info!("clean shutdown requested; shutting down runtime");
            runtime.shutdown().await;
            tracing::info!("runtime shutdown complete; exiting(0)");
            std::process::exit(0);
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tabbify_supervisor=debug,tabbify_runner=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
