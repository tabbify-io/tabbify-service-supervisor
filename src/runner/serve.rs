//! Runner serve core — hosts exactly one app on its app-ULA (loopback path).
//!
//! [`RunnerServe::start`] is the main entry point: given a [`ServeConfig`] it:
//! 1. creates an [`S3Fetcher`] and fetches the app artifact;
//! 2. derives the app-ULA via [`derive_app_ula`];
//! 3. builds the app runtime via the shared [`crate::build::build_runtime`];
//! 4. creates an [`AppHost`] (loopback when `no_mesh`, mesh deferred to
//!    Task 1.3) and hosts the app via [`AppHost::host`];
//! 5. wraps the live [`HostedApp`] in a [`RunnerServe`] that exposes the bound
//!    address — the test (and the binary) dial this to reach the app.
//!
//! The returned [`RunnerServe`] also exposes a [`RunnerServe::lifecycle`] handle
//! that the control server (Task 1.4) uses to share ownership of the live
//! listener, allowing `Stop`, `Purge`, and `Health` commands to operate on the
//! same `HostedApp`.
//!
//! # Deferred: mesh path
//! When `no_mesh = false` the mesh join is needed (Task 1.3). For now that
//! branch returns an explicit `todo!` error so the binary panics loudly instead
//! of silently binding the wrong address.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::app_ula::derive_app_ula;
use crate::build::build_runtime;
use crate::config::{DockerConfig, FcConfig};
use crate::fetcher::S3Fetcher;
use crate::host::{AppHost, AppServe};
use crate::runner::control::RunnerLifecycle;

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
    /// When `false` the runner would join the mesh — **DEFERRED to Task 1.3**.
    pub no_mesh: bool,
    /// Firecracker runtime config.
    pub fc: FcConfig,
    /// Docker runtime config.
    pub docker: DockerConfig,
}

/// A live per-app runner: holds the [`HostedApp`] (and thus its listener task)
/// alive for the duration of this value via a shared [`RunnerLifecycle`] handle
/// that the control server may also hold.
pub struct RunnerServe {
    /// The address the listener bound (loopback ephemeral in `--no-mesh` mode).
    addr: SocketAddr,
    /// Shared lifecycle state (wraps the live `HostedApp`). Kept here so the
    /// listener task lives as long as the `RunnerServe` does unless the control
    /// server issues a `Stop`.
    lifecycle: RunnerLifecycle,
}

impl RunnerServe {
    /// Fetch the app artifact, build the runtime, and start the per-app
    /// listener. Returns a [`RunnerServe`] holding the live listener.
    ///
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the S3 fetch fails;
    /// - the runtime build fails (wasm compile / firecracker / docker);
    /// - the listener fails to bind;
    /// - `no_mesh = false` (mesh join is deferred to Task 1.3).
    pub async fn start(cfg: ServeConfig) -> Result<Self> {
        let parsed_uuid = Uuid::parse_str(&cfg.uuid)
            .with_context(|| format!("invalid app uuid: {:?}", cfg.uuid))?;
        let app_ula = derive_app_ula(parsed_uuid);

        let fetcher = S3Fetcher::new(&cfg.s3_base_url, &cfg.data_dir);
        let fetched = fetcher
            .fetch(&cfg.uuid)
            .await
            .with_context(|| format!("fetch app {}", cfg.uuid))?;

        let runtime = build_runtime(&cfg.uuid, &fetched, &cfg.fc, &cfg.docker)
            .await
            .with_context(|| format!("build runtime for {}", cfg.uuid))?;

        // No idle-reaper in the runner yet — the on_request callback is a no-op.
        let on_request: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let serve = AppServe::new(runtime, on_request);

        let host = if cfg.no_mesh {
            AppHost::loopback()
        } else {
            // Mesh join (Task 1.3): requires joining the coordinator and wiring
            // the joiner as a MeshHost. Deferred — bail loudly so the binary
            // doesn't silently bind the wrong address.
            bail!("mesh mode: not yet implemented (task 1.3)");
        };

        let hosted = host
            .host(app_ula, serve)
            .await
            .with_context(|| format!("host app {} on {:?}", cfg.uuid, app_ula))?;

        let addr = hosted.addr;

        let lifecycle = RunnerLifecycle {
            uuid: cfg.uuid.clone(),
            app_ula: app_ula.to_string(),
            hosted: Arc::new(Mutex::new(Some(hosted))),
            fetcher,
            docker: cfg.docker,
        };

        Ok(Self { addr, lifecycle })
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
}
