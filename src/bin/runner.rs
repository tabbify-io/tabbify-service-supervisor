//! Thin entrypoint for `tabbify-runner`: parse config, init logging, start the
//! per-app serve core (loopback path under `--no-mesh`, otherwise it joins the
//! mesh claiming its app-ULA), bind the control socket, and run until shutdown.
//!
//! All logic lives in the `tabbify_supervisor` library; this file only wires
//! the pieces together — matching the `supervisord` `main.rs` pattern.

use anyhow::Context;
// `ActiveRuntime` implements `AppRuntime`; the trait must be in scope so the
// clean-shutdown path can call `runtime.shutdown()` on the active-runtime cell.
use tabbify_supervisor::runtime::AppRuntime;
use tabbify_supervisor::{
    RunnerConfig,
    runner::{
        build, control,
        serve::{RunnerExit, RunnerServe, run_until_exit},
        wire::serve_config_from,
    },
};
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // `parse_with_env` reads the scoped node-join token from
    // `TABBIFY_RUNNER_JOIN_TOKEN` (Phase-2) — intentionally not a clap flag, so
    // the credential never appears in `--help` / `ps`.
    let cfg = RunnerConfig::parse_with_env();

    // ── Builder mode ────────────────────────────────────────────────────────
    // If `--build-spec` is provided, run a one-shot build and exit.
    // This branch must come BEFORE any mesh-join / serve-forever setup so
    // the two modes are completely disjoint.
    if let Some(ref spec_path) = cfg.build_spec {
        match build::run_one_shot_build(spec_path).await {
            Ok(artifact) => {
                println!(
                    "{}",
                    serde_json::to_string(&artifact).expect("serialize ArtifactRef")
                );
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("build failed: {e:#}");
                std::process::exit(1);
            }
        }
    }
    // ────────────────────────────────────────────────────────────────────────

    tracing::info!(
        uuid   = %cfg.uuid,
        s3     = %cfg.s3_base_url,
        no_mesh = cfg.no_mesh,
        control_sock = %cfg.control_sock.display(),
        "starting tabbify-runner"
    );

    let serve_cfg = serve_config_from(&cfg);

    // Start the per-app serve core. In `--no-mesh` mode this binds a loopback
    // listener; when `no_mesh = false` the runner joins the mesh claiming its
    // app-ULA and binds that ULA directly.
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
    // `tabbify_mesh_joiner=warn` (P2-2): the runner joins the mesh in-process, so
    // the joiner's per-heartbeat/peer_sync `info!` chatter (`heartbeat: pruning
    // timed-out peer`, `peer-stream: applying upsert`) is emitted through THIS
    // subscriber and captured into the detached runner's stdout log
    // (`<data_dir>/runners/<uuid>.log`). At `info` with 100+ peers that spam
    // balloons the per-app log to hundreds of MB and drowns the app's own
    // diagnostics. Pin the joiner to `warn` so errors/warnings still surface
    // while the routine reconciliation noise is dropped. `RUST_LOG` still wins
    // (via `try_from_default_env`) for on-demand debugging.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,tabbify_mesh_joiner=warn,tabbify_supervisor=debug,tabbify_runner=debug")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
