//! Control-API-facing lifecycle operations on the runner fleet (Task 2.6).
//!
//! These are the orchestrator methods the axum control API
//! ([`crate::api`]) drives instead of the old in-process `AppRegistry`:
//!
//! - [`start_app`](Orchestrator::start_app) — if no live runner exists for the
//!   uuid, spawn one DETACHED and wait until its control socket is healthy;
//!   idempotent if one is already running.
//! - [`stop_app`](Orchestrator::stop_app) — `Shutdown` the runner (it exits,
//!   KEEPING its on-disk artifacts + docker image for a fast restart) and MARK
//!   its record stopped (the record is PRESERVED so the deploy artifact survives
//!   a later respawn/reset; the monitor will not respawn a stopped record).
//! - [`purge_app`](Orchestrator::purge_app) — `Purge` the runner (it clears its
//!   cache + removes its docker image) then `Shutdown` it, forget its record,
//!   and reclaim its on-disk cache.
//! - [`app_state`](Orchestrator::app_state) / [`app_summary`] /
//!   [`app_summaries`](Orchestrator::app_summaries) — read the live fleet from
//!   the on-disk records + a quick control-socket health probe.
//!
//! The orchestrator has NO in-memory fleet table: the [`RunnerHandle`] records
//! on disk are the single source of truth (so a restarted supervisor re-adopts
//! the living runners). Every op here loads/saves those records.

use std::{collections::HashSet, net::Ipv6Addr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::{
    app_ula::derive_app_ula,
    control_proto::Reply,
    orchestrator::{
        MONITOR_INTERVAL, Orchestrator,
        client::ControlClient,
        handle::RunnerHandle,
        manifest_retention,
        monitor::{
            RunnerPidIdentity, kill_fc_child_for_uuid, kill_pid, runner_pid_identity,
            runner_pids_for_uuid,
        },
        restart::{self, BackoffParams, RestartStatus},
        spawn::{SpawnSpec, spawn_runner},
    },
};

/// How long [`Orchestrator::start_app`] waits for a freshly-spawned runner to
/// answer its control socket before giving up. A cold start fetches the
/// artifact from S3 and builds a runtime, so this is generous.
///
/// A firecracker cold build is the worst case and drives this value: it pulls a
/// multi-hundred-MB image by digest, converts the OCI layers to an ext4 rootfs,
/// boots the microVM, and waits for in-guest readiness.
///
/// Sized for a COLD pull over the relay-only WAN mesh (MSI↔Frankfurt ~375 KB/s):
/// a ~60 MB image alone is ~165s on the wire, plus rootfs convert + boot +
/// health. The previous 180s was calibrated for a fast/cached pull and TIMED OUT
/// on a genuine cold WAN pull — which, on the async dev-session create path,
/// made `deploy_app` Err while the detached runner kept pulling, so the failure
/// handler REMOVED the session and the VM came up ORPHANED (running but
/// untracked → `dev_session_exec` could not resolve it). 360s covers the cold
/// WAN worst case with margin while still failing a genuinely-doomed start in
/// bounded time. (The real cure for the slowness is direct-UDP p2p; this is the
/// pragmatic bound until then.)
const START_HEALTHY_TIMEOUT: Duration = Duration::from_secs(360);

/// Terminal cold-spawn verdict: a freshly cold-spawned runner never reached
/// `app_health = "serving"` and its process EXITED (or the deadline elapsed).
///
/// The cold-spawn analog of [`crate::runner::active::SwapError::Unhealthy`]:
/// carried as an `anyhow` error so the `deploy_app` HTTP handler can `downcast`
/// it to a DISTINCT 503 (an app crash-loop is the app's own fault — wrong port,
/// PID-1 exits — NOT a platform build/upstream fault). CRITICAL: the monitor
/// keeps respawning the runner in the background; this is only the deploy
/// RESULT, never a stop signal.
#[derive(Debug, thiserror::Error)]
#[error("app crash-looping: {0}")]
pub struct ColdStartUnhealthy(pub String);

/// How many times a live zero-downtime swap re-sends `Cmd::Deploy` when the
/// control round-trip fails at the TRANSPORT layer (connect/write/read error or
/// the build-length round-trip timeout — the "deploy control message failed" symptom a
/// momentarily-wedged or briefly-busy runner socket produces). Each attempt is
/// itself a full zero-downtime swap (the runner keeps the OLD VM serving on
/// failure), and the runner's same-ref guard makes a re-send after a lost reply
/// an idempotent no-op — so retrying never risks the running app. A runner
/// `Reply::Err` (an opaque build / health-gate verdict; mesh-pull transients are
/// already exhausted by the runner's own oras retries) is NOT retried here.
const SWAP_MAX_ATTEMPTS: usize = 3;

/// Backoff between swap retry attempts. Gives a briefly-busy runner a moment to
/// finish whatever wedged its control socket before the next `Cmd::Deploy`.
const SWAP_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// How long the force-cold-on-env-change reap ([`Orchestrator::reap_runner_for_cold_respawn`])
/// waits for a shut-down runner's control socket to STOP answering before it
/// falls back to SIGKILL. A cold spawn is never attempted while either the old
/// process or its control socket remains live.
const COLD_REAP_MAX_WAIT: Duration = Duration::from_secs(5);

/// Poll interval while waiting for the reaped runner's socket to go dead.
const COLD_REAP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Externally-visible lifecycle state of an app, mirrored onto the control-API
/// JSON `state` field. In the orchestrator model an app is `running` iff its
/// runner answers a control-socket health probe; otherwise it is `stopped`
/// (which covers "never started" and "shut down" alike — the orchestrator keeps
/// no record for an app that has no runner).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppState {
    /// A live runner answers its control socket.
    Running,
    /// No live runner (never started, or shut down).
    Stopped,
}

impl AppState {
    /// Lowercase wire string used in the HTTP API JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AppState::Running => "running",
            AppState::Stopped => "stopped",
        }
    }
}

/// A snapshot row describing one app the orchestrator has a runner record for.
#[derive(Debug, Clone)]
pub struct AppSummary {
    /// App UUID (string form).
    pub uuid: String,
    /// The app's deterministic ULA (from the runner record).
    pub app_ula: String,
    /// Live state from a control-socket health probe.
    pub state: AppState,
    /// Coarse restart lifecycle status (`running` / `backoff` / `crashloop`).
    pub restart_status: String,
    /// Number of consecutive failures without a stable window in between.
    pub restart_count: u32,
    /// Earliest Unix timestamp (seconds) at which the runner is eligible to be
    /// respawned again. `0` means "immediately eligible" (no backoff pending).
    pub next_retry_at: u64,
    /// Unix seconds of the runner's most recent exit (`0` = never exited). Once
    /// `restart_status == "crashloop"` (parked) this is FROZEN at park time, so
    /// `now - last_exit_at` measures how long it has been definitively dead — the
    /// auto-reaper's grace clock.
    pub last_exit_at: u64,
    /// Runtime the caller requested as an override (D4 wire string), echoed onto
    /// the action-response JSON. `None` ⇒ manifest default was used (D10).
    pub requested_runtime: Option<String>,
}

/// Phase-2 tenant-network parameters carried on a deploy request.
///
/// The node resolves the deploying tenant's network (slug) and mints a SCOPED
/// node-join token, then passes BOTH here. They are threaded into a COLD spawn
/// so the runner joins the mesh scoped to its tenant network
/// (`--network <slug>` + `TABBIFY_RUNNER_JOIN_TOKEN`). Both `None`
/// (the [`Default`]) keeps today's unscoped, tokenless behavior.
#[derive(Debug, Clone, Default)]
pub struct DeployNetwork {
    /// Tenant network slug (`network=<slug>`), forwarded to the runner as
    /// `--network <slug>` and persisted in the runner record for respawn.
    pub network: Option<String>,
    /// Scoped node-minted node-join JWT (`network=<slug>`,
    /// `tags=["tag:net-<slug>"]`, `subject=<app-uuid>`) passed to the runner via
    /// `TABBIFY_RUNNER_JOIN_TOKEN`. Persisted because these node-minted runner
    /// tokens are long-lived and required again after a supervisor respawn.
    pub runner_join_token: Option<String>,
}

impl Orchestrator {
    /// Deterministic per-uuid control-socket path under this orchestrator's
    /// runner dir (`<runner_dir>/<uuid>.sock`). The runner binds it; the
    /// orchestrator + monitor reach the runner through it.
    #[must_use]
    pub fn control_sock_for(&self, uuid: &str) -> PathBuf {
        self.runner_dir().join(format!("{uuid}.sock"))
    }

    /// The deterministic app-ULA for `uuid` (the address the app serves on —
    /// the runner's own mesh ULA *is* the app-ULA).
    ///
    /// # Errors
    /// Returns an error if `uuid` is not a valid UUID.
    pub fn app_ula_for(&self, uuid: &str) -> Result<Ipv6Addr> {
        let parsed =
            Uuid::parse_str(uuid).with_context(|| format!("invalid app uuid: {uuid:?}"))?;
        Ok(derive_app_ula(parsed))
    }

    /// This runner's WireGuard listen port: the one it already holds, or the
    /// lowest free port in the per-host pool.
    ///
    /// STABILITY FIRST: if `uuid` already has a record carrying a port, that port
    /// is returned unchanged, so a respawn re-binds the same one and peers' cached
    /// dial targets stay valid. Only a runner with no port yet draws a new one,
    /// and then only from the ports no OTHER record holds.
    ///
    /// Returns `None` when the record directory cannot be read or the pool is
    /// exhausted — the spawn then proceeds WITHOUT an explicit port (the joiner
    /// default). That is the pre-existing behavior, so a failure here degrades to
    /// today rather than blocking the app from starting.
    fn wg_port_for_uuid(&self, uuid: &str) -> Option<u16> {
        let records = match RunnerHandle::list(self.runner_dir()) {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(
                    %uuid,
                    error = %e,
                    "could not read runner records to allocate a WireGuard port; \
                     spawning on the joiner default (co-resident joiners may collide)"
                );
                return None;
            }
        };
        // Already assigned -> reuse verbatim.
        if let Some(port) = records
            .iter()
            .find(|r| r.uuid == uuid)
            .and_then(|r| r.wg_listen_port)
        {
            tracing::debug!(%uuid, port, "reusing this runner's persisted WireGuard port");
            return Some(port);
        }
        // Otherwise take the lowest port no OTHER runner holds.
        let taken: Vec<u16> = records
            .iter()
            .filter(|r| r.uuid != uuid)
            .filter_map(|r| r.wg_listen_port)
            .collect();
        let port = crate::orchestrator::wg_port::allocate_wg_port(&taken);
        match port {
            Some(port) => tracing::info!(
                %uuid, port, peers = taken.len(),
                "allocated a WireGuard port for this runner"
            ),
            None => tracing::error!(
                %uuid, peers = taken.len(),
                "WireGuard port pool exhausted; spawning on the joiner default \
                 (this runner will share a port and lose inbound handshakes)"
            ),
        }
        port
    }

    /// Build the [`SpawnSpec`] for `uuid` from this orchestrator's shared config
    /// plus the derived control socket. The runner's `parent` comes from the
    /// shared config's notion of the supervisor's own ULA (currently `None` for
    /// no-mesh runs; see
    /// [`SharedRunnerConfig::parent`](crate::orchestrator::SharedRunnerConfig)).
    fn spawn_spec_for_uuid(&self, uuid: &str) -> SpawnSpec {
        let shared = self.shared();
        SpawnSpec {
            runner_bin: shared.runner_bin.clone(),
            uuid: uuid.to_owned(),
            control_sock: self.control_sock_for(uuid),
            s3_base_url: shared.s3_base_url.clone(),
            data_dir: shared.data_dir.clone(),
            parent: shared.parent.clone(),
            no_mesh: shared.no_mesh,
            // Forward the supervisor's relay endpoint so the runner routes its
            // relay over the same `wss://` url (corporate firewall).
            relay_url: shared.relay_url.clone(),
            // Forward the relay-only declaration so the runner tells the
            // coordinator it has no reachable direct endpoint (handshake over the
            // relay behind the host's shared NAT/firewall).
            relay_only: shared.relay_only,
            // Give this runner its OWN WireGuard port so its joiner does not
            // share one with the supervisor or any sibling runner.
            wg_listen_port: self.wg_port_for_uuid(uuid),
            // A fresh start (no deploy yet) builds from the S3 manifest.
            image_ref: None,
            // A plain start carries no managed config; `deploy_app` overrides it
            // on a connect-repo cold spawn.
            manifest_toml: None,
            // A plain start carries no tenant scoping; `deploy_app` overrides
            // these on a network-scoped cold spawn.
            network: None,
            runner_join_token: None,
            // A plain start carries no deploy-time extra env; `deploy_app`
            // overrides this on a cold spawn that supplies extra vars.
            extra_env: None,
            // A plain start carries no egress allow-list; `deploy_app` overrides
            // this on a network-ACL cold spawn. `None` = unrestricted egress.
            egress_allow: None,
        }
    }

    /// Build the launch spec for an explicit start.
    ///
    /// A known app's on-disk record is its durable launch intent. Reusing it is
    /// essential: replacing it with a fresh S3-only spec drops the registry ref,
    /// tenant identity, managed manifest, environment, and egress policy. Only a
    /// UUID with no record is a genuinely fresh S3-backed start.
    fn spawn_spec_for_start(&self, uuid: &str) -> Result<(SpawnSpec, Option<RunnerHandle>)> {
        match self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid} before start"))?
        {
            Some(record) => {
                tracing::info!(
                    uuid,
                    has_image_ref = record.image_ref.is_some(),
                    has_manifest = record.manifest_toml.is_some(),
                    network = record.network.as_deref().unwrap_or("unscoped"),
                    "start_app: reconstructing launch from durable runner record"
                );
                Ok((self.shared().spawn_spec_for(&record), Some(record)))
            }
            None => {
                tracing::info!(uuid, "start_app: no runner record — using fresh S3 launch");
                Ok((self.spawn_spec_for_uuid(uuid), None))
            }
        }
    }

    /// Build a cold-deploy spec from the durable record when one exists. EVERY
    /// optional context field is patch semantics: `Some` replaces, `None` keeps
    /// — including the managed manifest, which alone used to clear on `None`
    /// and so let a nudge re-deploy strip the app's `[runtime].stateful`
    /// (see [`crate::orchestrator::manifest_retention`]).
    #[allow(clippy::too_many_arguments)]
    fn spawn_spec_for_deploy(
        &self,
        uuid: &str,
        existing: Option<&RunnerHandle>,
        reff: &str,
        manifest_toml: Option<&str>,
        net: &DeployNetwork,
        extra_env: Option<&std::collections::HashMap<String, String>>,
        egress_allow: Option<&[String]>,
    ) -> SpawnSpec {
        let mut spec = existing.map_or_else(
            || self.spawn_spec_for_uuid(uuid),
            |record| self.shared().spawn_spec_for(record),
        );
        spec.image_ref = Some(reff.to_owned());
        if manifest_toml.is_some() {
            spec.manifest_toml = manifest_toml.map(str::to_owned);
        }
        if net.network.is_some() {
            spec.network = net.network.clone();
        }
        if net.runner_join_token.is_some() {
            spec.runner_join_token = net.runner_join_token.clone();
        }
        if extra_env.is_some() {
            spec.extra_env = extra_env.cloned();
        }
        if egress_allow.is_some() {
            spec.egress_allow = egress_allow.map(<[String]>::to_vec);
        }
        spec
    }

    /// A control client for `uuid`'s runner socket.
    #[must_use]
    fn client_for(&self, uuid: &str) -> ControlClient {
        ControlClient::new(self.control_sock_for(uuid))
    }

    async fn runner_health_for(&self, uuid: &str) -> Result<Option<Reply>> {
        match self.client_for(uuid).health().await {
            Ok(reply) => {
                let Reply::Health { app_uuid, .. } = &reply else {
                    anyhow::bail!("unexpected health reply for {uuid}: {reply:?}");
                };
                if app_uuid != uuid {
                    anyhow::bail!(
                        "control socket identity mismatch for {uuid}: runner reported {app_uuid}"
                    );
                }
                Ok(Some(reply))
            }
            Err(_) => Ok(None),
        }
    }

    /// Live state of `uuid`: `Running` iff its runner answers a health probe.
    ///
    /// # Errors
    /// Returns an error only if `uuid` is not a valid UUID (so a caller can
    /// 400). A missing/dead runner is reported as `Stopped`, not an error.
    pub async fn app_state(&self, uuid: &str) -> Result<AppState> {
        // Validate the uuid up front so a malformed id is a clear error.
        let _ = self.app_ula_for(uuid)?;
        Ok(if self.runner_health_for(uuid).await?.is_some() {
            AppState::Running
        } else {
            AppState::Stopped
        })
    }

    /// Start `uuid`: if a live runner already exists, return its summary
    /// (idempotent); otherwise spawn a DETACHED runner and wait until its
    /// control socket is healthy.
    ///
    /// `runtime_override` is accepted for HTTP back-compat and echoed back in the
    /// response summary's `requested_runtime`, but it no longer selects a runtime:
    /// every app builds as Firecracker, so the override is NOT threaded into the
    /// spawned runner.
    ///
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the runner process fails to spawn (binary missing / record write);
    /// - the runner never becomes healthy within [`START_HEALTHY_TIMEOUT`].
    pub async fn start_app(
        &self,
        uuid: &str,
        runtime_override: Option<&str>,
    ) -> Result<AppSummary> {
        let app_ula = self.app_ula_for(uuid)?;

        // Start, stop, purge, reset, and deploy all mutate the same durable
        // runner record. Serialize them per app and shield this spawn from the
        // monitor so no stale spec can overwrite a concurrent lifecycle action.
        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;
        let (spec, mut existing) = self.spawn_spec_for_start(uuid)?;
        let socket_live = self.runner_health_for(uuid).await?.is_some();
        let socket_present = socket_live
            || self
                .client_for(uuid)
                .socket_reachable(COLD_REAP_POLL_INTERVAL)
                .await
                .with_context(|| format!("probe runner socket for {uuid} before start"))?;

        // Idempotent only for an app whose durable state is running. A stopped
        // runner may briefly keep answering after Shutdown acknowledged; treating
        // that socket as started would leave the record stopped and the app would
        // disappear as soon as the old process exits.
        if existing.as_ref().is_some_and(|record| !record.stopped) && socket_live {
            tracing::info!(
                uuid,
                branch = "idempotent-live",
                "start_app: live runner already healthy — returning existing summary (no spawn)"
            );
            return Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
                restart_status: restart_status_str(RestartStatus::Running),
                restart_count: 0,
                next_retry_at: 0,
                last_exit_at: 0,
                requested_runtime: runtime_override.map(str::to_owned),
            });
        }

        if let Some(record) = existing.as_ref().filter(|record| !record.stopped) {
            match runner_pid_identity(record.pid, uuid) {
                RunnerPidIdentity::Matches | RunnerPidIdentity::Unknown => anyhow::bail!(
                    "runner for {uuid} is still alive or its identity is unverified while its control socket is unhealthy; refusing to spawn a duplicate"
                ),
                RunnerPidIdentity::Gone | RunnerPidIdentity::Mismatch => {}
            }
        }

        // Any existing record may still own a process even when its socket is
        // unavailable (mid-pull or mid-shutdown). A stopped record in particular
        // is durable intent, not proof that the old runner already exited. Reap
        // both PID and socket before spawning so Start cannot create a duplicate.
        if let Some(record) = existing.as_mut() {
            self.shutdown_runner_definitive(uuid, record.pid).await?;
            if !kill_fc_child_for_uuid(&self.shared().data_dir, uuid, record.image_ref.as_deref()) {
                anyhow::bail!("Firecracker teardown for {uuid} was not confirmed before start");
            }
            record.pid = 0;
            record
                .save(self.runner_dir())
                .with_context(|| format!("save exited runner record for {uuid} before start"))?;
        } else if socket_present {
            // A live socket without a record is an untracked survivor. Its health
            // reply provides the PID used by definitive shutdown; never spawn a
            // second runner beside it.
            self.shutdown_runner_definitive(uuid, 0).await?;
            if !kill_fc_child_for_uuid(&self.shared().data_dir, uuid, None) {
                anyhow::bail!("Firecracker teardown for {uuid} was not confirmed before start");
            }
        }

        // No live runner. Spawn one DETACHED (it persists its own record). The
        // runtime is fixed to Firecracker, so the override is not threaded in.
        tracing::info!(
            uuid,
            branch = "cold-spawn",
            "start_app: no live runner — spawning detached runner"
        );
        let (handle, _child) = spawn_runner(&spec, self.runner_dir())
            .await
            .with_context(|| format!("spawn runner for {uuid}"))?;

        // Wait until the runner answers its control socket. We intentionally
        // drop `_child` after this — the runner is detached (its own session
        // leader), so letting the handle go does NOT kill it; the monitor loop
        // tracks it by pid + socket from here on.
        let client = ControlClient::new(&handle.control_sock);
        wait_healthy(&client, START_HEALTHY_TIMEOUT)
            .await
            .with_context(|| format!("runner for {uuid} never became healthy"))?;
        tracing::info!(
            uuid,
            pid = handle.pid,
            "start_app: runner became healthy (cold-spawn complete)"
        );

        Ok(AppSummary {
            uuid: uuid.to_owned(),
            app_ula: app_ula.to_string(),
            state: AppState::Running,
            restart_status: restart_status_str(RestartStatus::Running),
            restart_count: 0,
            next_retry_at: 0,
            last_exit_at: 0,
            requested_runtime: runtime_override.map(str::to_owned),
        })
    }

    /// Stop `uuid`: shut its runner down (the runner exits, KEEPING its on-disk
    /// artifacts + docker image for a fast restart) and MARK the record stopped
    /// so the monitor does not respawn it. Idempotent — a missing runner is a
    /// no-op.
    ///
    /// The record is PRESERVED (not deleted): its
    /// `image_ref`/`manifest_toml`/`extra_env`/`runner_join_token` are kept so a
    /// later respawn/reset/deploy still has the deploy artifact. Deleting the
    /// record here (the old behavior) bricked a later respawn — the runner died
    /// "fetch app … not found at this version/object" because the `image_ref` was
    /// gone. The full record DELETE now lives only in
    /// [`purge_app`](Self::purge_app), the real teardown.
    ///
    /// # Errors
    /// Returns an error if the UUID is malformed, durable state cannot be loaded
    /// or saved, or the runner survives the bounded graceful + SIGKILL teardown.
    pub async fn stop_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;

        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;
        let mut record = self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid} before stop"))?;
        let recorded_pid = record.as_ref().map_or(0, |record| record.pid);
        let image_ref = record.as_ref().and_then(|record| record.image_ref.clone());

        // Persist operator intent before touching the process. Keep the PID until
        // exit is definitive: it is the fallback handle when the socket is absent
        // during image pull or shutdown.
        if let Some(record) = record.as_mut() {
            record.stopped = true;
            record
                .save(self.runner_dir())
                .with_context(|| format!("persist stopped intent for {uuid}"))?;
        }

        self.shutdown_runner_definitive(uuid, recorded_pid).await?;

        // Reap any FC child the runner left behind (the FC process is not a
        // child of the supervisor — it busy-spins until reaped). Mirrors
        // purge_app's reap so a stopped app does not leak a 100%-CPU FC orphan.
        // Pass the captured `image_ref` so the scoped FC's `uuid:reff` scope is
        // also stopped (F1) — killing the pidfile wrapper pid alone leaks it.
        if !kill_fc_child_for_uuid(&self.shared().data_dir, uuid, image_ref.as_deref()) {
            anyhow::bail!("Firecracker teardown for stopped app {uuid} was not confirmed");
        }

        if let Some(record) = record.as_mut() {
            record.pid = 0;
            record
                .save(self.runner_dir())
                .with_context(|| format!("persist exited stopped runner for {uuid}"))?;
        }
        Ok(())
    }

    /// Reap a LIVE runner so a following COLD spawn of the SAME `uuid` boots clean.
    ///
    /// Used ONLY by the force-cold-on-env-change deploy path (#108): a warm swap
    /// cannot re-bake a changed `/init` env, so the running runner must exit and
    /// its firecracker child be reaped BEFORE the cold spawn re-derives the
    /// identical `uuid:reff` tap / api-socket (which would otherwise collide with
    /// the still-live VM). Unlike [`stop_app`] this does NOT mark the record
    /// stopped — the cold spawn immediately follows and writes a fresh
    /// `stopped: false` record.
    ///
    /// Failure is fatal to the deploy: spawning while the old process or socket
    /// survives would create two runners for one UUID. The caller already holds
    /// the per-UUID lifecycle lock, so the monitor cannot race the gap.
    async fn reap_runner_for_cold_respawn(&self, record: &RunnerHandle) -> Result<()> {
        self.shutdown_runner_definitive(&record.uuid, record.pid)
            .await?;
        if !kill_fc_child_for_uuid(
            &self.shared().data_dir,
            &record.uuid,
            record.image_ref.as_deref(),
        ) {
            anyhow::bail!(
                "Firecracker teardown for {} was not confirmed before cold respawn",
                record.uuid
            );
        }
        let mut exited = record.clone();
        exited.pid = 0;
        exited.save(self.runner_dir()).with_context(|| {
            format!(
                "persist exited runner before cold deploy for {}",
                record.uuid
            )
        })
    }

    /// Purge `uuid`: tell the runner to clear its on-disk cache + remove its
    /// docker image, then shut it down, forget its record, and reclaim the cache
    /// from the orchestrator side too (belt-and-suspenders). Idempotent.
    ///
    /// # Errors
    /// Returns an error if durable intent cannot be loaded/saved/deleted, the
    /// old runner survives teardown, or authoritative cache cleanup is incomplete.
    pub async fn purge_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;

        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;
        let mut record = self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid} before purge"))?;
        let recorded_pid = record.as_ref().map_or(0, |record| record.pid);
        let image_ref = record.as_ref().and_then(|record| record.image_ref.clone());

        // Durable stop intent comes first. If any later cleanup fails, the
        // retained record prevents monitor resurrection and makes purge retryable.
        if let Some(record) = record.as_mut() {
            record.stopped = true;
            record
                .save(self.runner_dir())
                .with_context(|| format!("persist stopped intent for {uuid} before purge"))?;
        }

        let client = self.client_for(uuid);
        // Purge clears the runner's cache + docker image (the runner stays up),
        // then Shutdown exits it. Both are best-effort.
        match client.purge().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to Purge"),
            Err(e) => tracing::warn!(uuid, error = %e, "Purge failed (runner may be gone)"),
        }
        self.shutdown_runner_definitive(uuid, recorded_pid).await?;

        // FIX C: reap any orphaned FC child. When a dev session is stopped the
        // runner exits (Shutdown), but the firecracker process it spawned is NOT
        // a child of the supervisor — it was spawned by the runner and gets
        // reparented to PID 1 where it busy-spins at 100% CPU. The monitor
        // already does this reap on its kill-before-respawn path; purge_app
        // did not, leaving FC orphans alive until the next monitor tick found a
        // dead runner. Call the same helper here, best-effort. The pre-forget
        // `image_ref` lets it stop the scoped FC's `uuid:reff` scope too (F1).
        if !kill_fc_child_for_uuid(&self.shared().data_dir, uuid, image_ref.as_deref()) {
            anyhow::bail!("Firecracker teardown for purged app {uuid} was not confirmed");
        }

        if let Some(record) = record.as_mut() {
            record.pid = 0;
            record
                .save(self.runner_dir())
                .with_context(|| format!("persist exited runner for {uuid} before purge"))?;
        }

        // Reclaim the on-disk cache from our side too: the runner's own purge
        // already cleared it, but if the runner was unreachable we still want a
        // clean disk. `purge_cache` is idempotent (missing dir = success).
        let fetcher =
            crate::fetcher::S3Fetcher::new(&self.shared().s3_base_url, &self.shared().data_dir);
        fetcher
            .purge_cache(uuid)
            .await
            .with_context(|| format!("purge cache for {uuid}"))?;

        // Release network ownership before deleting the durable runner record.
        // If allocator persistence fails, preserve the record so purge remains
        // retryable instead of losing the last ownership handle.
        let tap_subnet = std::env::var("SUPERVISOR_FC_TAP_SUBNET")
            .unwrap_or_else(|_| crate::config::DEFAULT_FC_TAP_SUBNET.to_owned());
        let released = crate::firecracker::link_allocator::LinkSlotAllocator::new(
            &self.shared().data_dir,
            &tap_subnet,
        )
        .release_uuid(uuid)
        .with_context(|| format!("release Firecracker link assignments for {uuid}"))?;
        tracing::info!(uuid, released, "purge released FC link assignments");
        // Remove the small per-uuid auxiliary artifacts (meshkey, logs, socket,
        // legacy graveyard entries) that previously leaked on disk forever.
        // Best-effort by design: the record deletion below stays the hard gate.
        self.remove_app_artifacts(uuid);
        // Delete only after the old runner, cache, and link allocation are gone.
        self.forget_record(uuid)
            .with_context(|| format!("delete runner record for {uuid} after purge"))?;
        Ok(())
    }

    /// Refresh `uuid`'s warm-LSP snapshot IN-PLACE (the `Cmd::Snapshot` path).
    ///
    /// Sends `Cmd::Snapshot` to the runner, which pauses the live workspace VM,
    /// writes a fresh `/snapshot/create` to the per-uuid cache dir, and resumes
    /// the guest (it keeps serving). This is the §12 POST-INDEX refresh: the node
    /// calls it AFTER the code-service reports `index_status == Ready`, so the
    /// captured RAM holds a WARM LSP index. The VM is left RUNNING on any error
    /// (the runner always `ensure_resumed`s).
    ///
    /// # Errors
    /// - `uuid` is malformed (caller can 400).
    /// - The control round-trip failed, OR the runner replied [`Reply::Err`]
    ///   (the snapshot create did not land). Either way the VM stays up; the
    ///   caller may retry. A successful snapshot returns `Ok(())`.
    pub async fn snapshot_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;
        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;
        match self.client_for(uuid).snapshot().await? {
            Reply::Ok => {
                tracing::info!(uuid, "Cmd::Snapshot: warm snapshot refreshed");
                Ok(())
            }
            Reply::Err { message } => {
                // The runner ran the create but it failed (VM still serving).
                // Surface it so the node can retry; do NOT treat as fatal.
                anyhow::bail!("snapshot for {uuid} failed: {message}")
            }
            other => anyhow::bail!("unexpected reply to Snapshot for {uuid}: {other:?}"),
        }
    }

    /// Snapshot every app the orchestrator has a runner record for, with each
    /// one's live state from a control-socket health probe.
    ///
    /// # Errors
    /// Returns an error only if the runner directory cannot be listed.
    pub async fn app_summaries(&self) -> Result<Vec<AppSummary>> {
        let records = RunnerHandle::list(self.runner_dir())?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut out = Vec::with_capacity(records.len());
        for rec in records {
            let state = if ControlClient::new(&rec.control_sock).health().await.is_ok() {
                AppState::Running
            } else {
                AppState::Stopped
            };
            let rs =
                crate::orchestrator::restart::status(rec.restart, BackoffParams::default(), now);
            let restart_status = restart_status_str(rs);
            out.push(AppSummary {
                uuid: rec.uuid,
                app_ula: rec.app_ula,
                state,
                restart_status,
                restart_count: rec.restart.consecutive_failures,
                next_retry_at: rec.restart.next_retry_at,
                last_exit_at: rec.restart.last_exit_at,
                // Read path: no override travels on a snapshot.
                requested_runtime: None,
            });
        }
        Ok(out)
    }

    /// The summary for a single `uuid` IF the orchestrator has a record for it
    /// (i.e. a runner was started for it), else `None`. State comes from a
    /// health probe.
    ///
    /// # Errors
    /// Returns an error only if `uuid` is malformed.
    pub async fn app_summary(&self, uuid: &str) -> Result<Option<AppSummary>> {
        let _ = self.app_ula_for(uuid)?;
        let Some(rec) = self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid}"))?
        else {
            return Ok(None);
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let state = if ControlClient::new(&rec.control_sock).health().await.is_ok() {
            AppState::Running
        } else {
            AppState::Stopped
        };
        let rs = crate::orchestrator::restart::status(rec.restart, BackoffParams::default(), now);
        let restart_status = restart_status_str(rs);
        Ok(Some(AppSummary {
            uuid: rec.uuid,
            app_ula: rec.app_ula,
            state,
            restart_status,
            restart_count: rec.restart.consecutive_failures,
            next_retry_at: rec.restart.next_retry_at,
            last_exit_at: rec.restart.last_exit_at,
            // Read path: no override travels on a snapshot.
            requested_runtime: None,
        }))
    }

    /// Send a live runner the `Deploy` control message, retrying transient
    /// control-transport failures.
    ///
    /// Returns `Ok(())` once the runner replies `Reply::Ok` (the swap landed).
    /// The TRANSPORT-failure class — a connect/write/read error or the build-length
    /// round-trip timeout, surfaced as `Err(e)` ("deploy control message failed")
    /// — is retried up to [`SWAP_MAX_ATTEMPTS`] with [`SWAP_RETRY_BACKOFF`]
    /// between tries: this is the symptom a momentarily-wedged or briefly-busy
    /// runner socket produces (and the residue of the deploy-race), and re-sending
    /// is safe because every runner-side deploy is a zero-downtime swap that keeps
    /// the OLD VM serving on failure, while the runner's same-ref guard makes a
    /// re-send after a lost reply an idempotent no-op.
    ///
    /// A runner `Reply::Err` is a deliberate verdict (the build failed or the new
    /// VM never became healthy; mesh-pull transients are already exhausted by the
    /// runner's own oras retries) — it is NOT retried, just surfaced. `Ok(other)`
    /// is a protocol violation, also not retried.
    ///
    /// On any returned `Err` the caller MUST NOT advance `image_ref`, so the app
    /// stays on its last-known-good build. The error is logged loudly with the
    /// dropped ref so a re-deploy is obvious.
    async fn swap_with_retry(&self, uuid: &str, reff: &str) -> Result<()> {
        for attempt in 1..=SWAP_MAX_ATTEMPTS {
            match self.client_for(uuid).deploy(reff).await {
                Ok(Reply::Ok) => return Ok(()),
                Ok(Reply::Err { message }) => {
                    tracing::warn!(
                        uuid,
                        reff,
                        %message,
                        "deploy swap rejected by runner; app stays on its last-known-good build — re-deploy to retry"
                    );
                    return Err(anyhow::anyhow!("runner deploy failed: {message}"));
                }
                Ok(other) => {
                    return Err(anyhow::anyhow!("unexpected reply to Deploy: {other:?}"));
                }
                Err(e) => {
                    if attempt >= SWAP_MAX_ATTEMPTS {
                        tracing::warn!(
                            uuid,
                            reff,
                            attempt,
                            error = %e,
                            "deploy swap failed after {SWAP_MAX_ATTEMPTS} attempts (control transport); app stays on its last-known-good build — re-deploy to retry"
                        );
                        return Err(e.context("deploy control message failed"));
                    }
                    tracing::warn!(
                        uuid,
                        reff,
                        attempt,
                        error = %e,
                        "deploy control transport failed; retrying after backoff"
                    );
                    tokio::time::sleep(SWAP_RETRY_BACKOFF).await;
                }
            }
        }
        // Unreachable: every match arm returns, and the final attempt's transport
        // arm returns rather than looping. Kept for exhaustiveness.
        unreachable!("swap_with_retry returns inside the loop on the final attempt")
    }

    /// Persist the record after a successful warm swap. If persistence fails,
    /// shut the swapped runner down before releasing the lifecycle lock so no
    /// live runtime disagrees with the previous atomic durable record.
    async fn persist_warm_deploy(
        &self,
        record: &RunnerHandle,
        previous_image_ref: Option<&str>,
        reff: &str,
    ) -> Result<()> {
        let Err(save_error) = record.save(self.runner_dir()) else {
            return Ok(());
        };
        tracing::error!(
            uuid = %record.uuid,
            reff,
            error = %save_error,
            "warm deploy landed but durable record save failed; shutting down runner fail-closed"
        );
        let shutdown_result = self
            .shutdown_runner_definitive(&record.uuid, record.pid)
            .await;
        let mut cleanup_complete =
            kill_fc_child_for_uuid(&self.shared().data_dir, &record.uuid, Some(reff));
        if let Some(previous) = previous_image_ref {
            cleanup_complete &=
                kill_fc_child_for_uuid(&self.shared().data_dir, &record.uuid, Some(previous));
        }
        if let Err(shutdown_error) = shutdown_result {
            anyhow::bail!(
                "save runner record for {} after deploy: {save_error}; fail-closed runner shutdown also failed: {shutdown_error}",
                record.uuid
            );
        }
        if !cleanup_complete {
            anyhow::bail!(
                "save runner record for {} after deploy: {save_error}; Firecracker cleanup was not confirmed",
                record.uuid
            );
        }
        Err(anyhow::Error::new(save_error).context(format!(
            "save runner record for {} after deploy",
            record.uuid
        )))
    }

    /// Deploy `reff` to `uuid`: if a runner is live send it a `Deploy` control
    /// message (zero-downtime swap); if there is no live runner, spawn one
    /// pinned to `reff`. The deployed ref is persisted so a future supervisor
    /// restart respawns the runner on the same version.
    ///
    /// `runtime_override` is accepted for HTTP back-compat and echoed back in the
    /// response summary's `requested_runtime`, but it no longer selects a runtime:
    /// a by-ref deploy is always the Firecracker pull source, so the override is
    /// NOT threaded into the control `Deploy` message or the spawned runner.
    ///
    /// `net` carries the Phase-2 tenant-network scoping (slug + node-minted
    /// scoped runner token). It is threaded into a COLD spawn only: a live
    /// zero-downtime swap does NOT re-key the running runner's mesh peer (the
    /// runner already holds its scoped identity), so network scoping takes effect
    /// when a fresh runner is spawned. [`DeployNetwork::default`] (both `None`)
    /// keeps today's unscoped behavior.
    ///
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the control-socket deploy fails (runner returned `Reply::Err`);
    /// - spawning a cold runner fails or it never becomes healthy.
    #[allow(clippy::too_many_arguments)]
    pub async fn deploy_app(
        &self,
        uuid: &str,
        reff: &str,
        runtime_override: Option<&str>,
        manifest_toml: Option<&str>,
        net: DeployNetwork,
        extra_env: Option<&std::collections::HashMap<String, String>>,
        egress_allow: Option<&[String]>,
    ) -> Result<AppSummary> {
        let app_ula = self.app_ula_for(uuid)?;

        // Serialize concurrent deploys of the SAME app. A single push commonly
        // fans out into two deploys (commit_repo_edit's redeploy + the GitHub-App
        // webhook's redeploy of the same commit); without this lock both reach the
        // runner's control socket at once and race the in-flight Firecracker swap
        // → one returns "deploy control message failed" (500) + the surviving
        // artifact is non-deterministic. The second caller queues here until the
        // first finishes its swap; DIFFERENT apps contend on different locks and
        // still deploy in parallel. Held across every await below for the whole
        // deploy (control round-trip and any cold spawn + health wait).
        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;

        // Mark this uuid as deploy-in-flight for the WHOLE deploy. The RAII guard
        // clears it on every exit path and protects the record-less shared build
        // VM from the Linux orphan sweep. Per-app monitor reconciliation is
        // already excluded by the lifecycle lock above.
        let _deploy_guard = self.begin_deploy(uuid);

        // Load the persisted record up front: needed both to compare the LIVE
        // env-hash against this deploy's requested env (the force-cold gate below)
        // and, on the warm path, to persist the new ref/env after the swap.
        let existing = self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid} before deploy"))?;

        // Force-cold-on-env-change (#108). A live runner's `/init`-baked env is
        // FIXED at spawn: a warm zero-downtime swap rebuilds the VM with the
        // runner's SPAWN-TIME env and CANNOT apply a newly-supplied env/cap set
        // (a workspace `add_repo`'s new clone cap, a rotated secret). Worse, the
        // same-digest guard turns such a swap into a "digest already live" no-op
        // — the change is silently dropped and the guest never re-runs `/init`
        // (its broker never clones the new repo). So compare the #106 fingerprint
        // of THIS deploy's env against what the live runtime was built with (the
        // persisted record's env); if they DIFFER, a warm swap is wrong — force
        // the COLD respawn path so the new env is baked into a fresh `/init` (the
        // cold spawn's snapshot warm-restore is ALSO env-gated, so the guest
        // genuinely re-runs `/init` + boot-clone). A `None` deploy env means
        // "keep the existing env" → never force-cold. A first deploy (no record)
        // has no live runtime to differ from → also never force-cold.
        let env_changed = match (extra_env, existing.as_ref()) {
            (Some(_), Some(rec)) => {
                crate::runner::build::firecracker::effective_env_hash(extra_env)
                    != crate::runner::build::firecracker::effective_env_hash(rec.extra_env.as_ref())
            }
            _ => false,
        };
        // WHY the fingerprint differs — the exact component (env key / cap-file
        // name / ROTATED cap value), so a forced cold respawn is one-grep
        // self-explaining. Key names only; env/cap VALUES never reach the log.
        let env_change_reason = if env_changed {
            crate::runner::build::rootfs_variants::describe_env_change(
                existing.as_ref().and_then(|r| r.extra_env.as_ref()),
                extra_env,
            )
        } else {
            String::new()
        };

        // Managed-manifest patch semantics (see `manifest_retention`). A deploy
        // body without a `manifest_toml` KEEPS the record's — omitting it is a
        // nudge, not a request to erase the app's runtime config. Erasing it
        // used to un-stateful the app on the next spawn, silently: `running`,
        // healthy, writing to a rootfs the following respawn discards.
        let persisted_manifest = existing.as_ref().and_then(|r| r.manifest_toml.clone());
        let manifest_toml =
            manifest_retention::retained(manifest_toml, persisted_manifest.as_deref());
        // And an EXPLICIT manifest that takes a live data disk away is refused
        // rather than applied — losing a disk must never be the quiet path.
        if let Some(regression) =
            manifest_retention::stateful_regression(persisted_manifest.as_deref(), manifest_toml)
        {
            tracing::error!(
                uuid,
                reff,
                previous_mount = regression.previous_mount.as_deref().unwrap_or("<unset>"),
                next_stateful = regression.next_stateful,
                "deploy: refused — the supplied tabbify.toml would strip this app's persistent data disk"
            );
            return Err(anyhow::Error::new(regression));
        }

        let live = self.runner_health_for(uuid).await?.is_some();
        let socket_present = live
            || self
                .client_for(uuid)
                .socket_reachable(COLD_REAP_POLL_INTERVAL)
                .await
                .with_context(|| format!("probe runner socket for {uuid} before deploy"))?;
        let warm_eligible =
            live && existing.as_ref().is_some_and(|record| !record.stopped) && !env_changed;
        // Branch decision trace: which of the three deploy paths this deploy took
        // and WHY (live? env changed vs the running runtime? which component?).
        // The blind spot was "did this deploy warm-swap or cold-respawn?" — now it
        // is one grep.
        tracing::info!(
            uuid,
            reff,
            live,
            socket_present,
            env_changed,
            env_change_reason = %env_change_reason,
            branch = if warm_eligible {
                "warm-swap"
            } else if live && env_changed {
                "cold-respawn (env changed on live runner)"
            } else if socket_present {
                "cold-respawn (stopped or untracked live runner)"
            } else {
                "cold-spawn (no live runner)"
            },
            "deploy: branch selected"
        );
        if warm_eligible {
            // Live runner, env UNCHANGED: send the Deploy message, retrying
            // transient transport failures. On a persistent failure this returns
            // Err BEFORE the persist block below — so `image_ref` is left at its
            // current value and the app stays on its last-known-good build (never
            // stranded on a half-deployed / broken image; the failed swap kept the
            // OLD VM serving). The error names the dropped ref so a re-deploy is
            // obvious.
            self.swap_with_retry(uuid, reff).await?;

            // Persist the new ref so a future respawn comes up on this version.
            let mut record =
                existing.ok_or_else(|| anyhow::anyhow!("no runner record found for {uuid}"))?;
            let previous_image_ref = record.image_ref.clone();
            record.image_ref = Some(reff.to_owned());
            // Persist this deploy's managed `tabbify.toml` so the durable record
            // always reflects the LATEST deploy's runtime config. Without this a
            // warm zero-downtime swap (the live runner keeps serving) would leave
            // the OLD toml on disk, and a later crash-respawn would re-derive
            // STALE `[runtime]`/`[routes]`. Already resolved against the record
            // above, so a body that carried no toml keeps the persisted one.
            record.manifest_toml = manifest_toml.map(str::to_owned);
            // Phase-2: if this deploy supplied a tenant network, persist it so a
            // future RESPAWN rejoins scoped (the live runner itself is not
            // re-keyed — it keeps its current mesh identity). A `None` network
            // leaves the record's existing scoping untouched (back-compat).
            if net.network.is_some() {
                record.network = net.network.clone();
            }
            // Phase-2: if this deploy supplied a scoped join token, persist it
            // so a future RESPAWN re-joins the validating coordinator with the
            // SAME token (the token is long-lived, 1-year TTL). A `None` token
            // in the deploy body is a "nudge" re-deploy that must NOT destroy
            // a previously-persisted token — keep the existing value so the
            // runner's respawn path always has a valid token.
            if net.runner_join_token.is_some() {
                record.runner_join_token = net.runner_join_token.clone();
            }
            // Persist this deploy's extra env so the durable record always
            // reflects the LATEST deploy's baked-in vars. A `Some` extra_env
            // replaces the persisted map; a `None` here means the deploy body
            // carried no extra env and we keep the previously-persisted map so
            // a token-less nudge re-deploy does not wipe the runner's env vars.
            if extra_env.is_some() {
                record.extra_env = extra_env.cloned();
            }
            // Persist this deploy's egress allow-list so the durable record always
            // reflects the LATEST deploy's posture. A `Some` replaces the persisted
            // list; a `None` here means the deploy body carried no allow-list and
            // we keep the previously-persisted value (a token-less nudge re-deploy
            // must not silently widen egress back to unrestricted). NB: the LIVE
            // runner keeps its spawn-time `RUNNER_EGRESS_ALLOW` posture — a rule
            // change applies on the NEXT COLD (re)deploy, exactly like extra_env.
            if egress_allow.is_some() {
                record.egress_allow = egress_allow.map(<[String]>::to_vec);
            }
            self.persist_warm_deploy(&record, previous_image_ref.as_deref(), reff)
                .await?;

            Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
                restart_status: restart_status_str(RestartStatus::Running),
                restart_count: 0,
                next_retry_at: 0,
                last_exit_at: 0,
                requested_runtime: runtime_override.map(str::to_owned),
            })
        } else {
            if live {
                // env_changed but a runner IS live (add_repo's prior `stop_app`
                // hadn't fully exited, or a monitor tick respawned it in the gap
                // before this deploy took its lifecycle lock). A warm swap
                // can't apply the new `/init` env, so REAP the live runner first:
                // the cold spawn below re-derives the IDENTICAL `uuid:reff` tap /
                // api-socket, which must be free, or the new VM collides with the
                // still-live one ("socket never appeared"). Held under the
                // lifecycle lock so the monitor cannot respawn it mid-reap.
                tracing::info!(
                    uuid,
                    reff,
                    "deploy: env/cap set changed vs the live runtime — forcing a cold respawn (a warm swap cannot re-bake /init env)"
                );
            }
            if let Some(record) = existing.as_ref() {
                self.reap_runner_for_cold_respawn(record).await?;
            } else if socket_present {
                // A socket without a record is an untracked survivor. Reap it
                // before creating the first durable runner for this UUID.
                self.shutdown_runner_definitive(uuid, 0).await?;
                if !kill_fc_child_for_uuid(&self.shared().data_dir, uuid, None) {
                    anyhow::bail!(
                        "Firecracker teardown for untracked runner {uuid} was not confirmed"
                    );
                }
            }
            // No live runner (never started, or just reaped for an env change):
            // spawn one pinned to reff. The runtime is fixed to Firecracker, so
            // the override is not threaded into the spec.
            let spec = self.spawn_spec_for_deploy(
                uuid,
                existing.as_ref(),
                reff,
                manifest_toml,
                &net,
                extra_env,
                egress_allow,
            );
            let (handle, _child) = spawn_runner(&spec, self.runner_dir())
                .await
                .with_context(|| format!("spawn runner for {uuid}"))?;

            let client = ControlClient::new(&handle.control_sock);
            // Cold-spawn health gate (option B). A terminal CrashLoop verdict is
            // surfaced as `ColdStartUnhealthy` WITHOUT `.context` so the handler
            // can downcast it to a distinct 503 (an app crash-loop is the app's
            // own fault, not a platform fault) — this is what flips the node's
            // async `deploy_status` off eternal "pending". The monitor keeps
            // respawning the runner in the background (self-heal preserved).
            if let ColdStart::CrashLoop(gate_reason) =
                wait_cold_serving(&client, handle.pid, START_HEALTHY_TIMEOUT).await
            {
                // P1-2 (reason propagation): the gate's own reason is terse
                // ("runner process exited before serving") because by the time it
                // detects the crash the runner is already dead — its control
                // socket answers nothing. The PRECISE, actionable diagnostic
                // (app not LISTENING on its EXPOSE port / daemonized PID 1 /
                // missing CAP_NET_ADMIN) was printed by the runner's cold-boot
                // `firecracker::wait_until_ready` to its stderr, which lands in
                // the per-app runner log. Recover it from the log tail and fold
                // it into the verdict so the `ColdStartUnhealthy` → 503 → node
                // `deploy_status` chain surfaces WHY the guest never served, not
                // just that it didn't. Best-effort: no marker in the tail ⇒ keep
                // the terse gate reason unchanged (backward compatible).
                let boot_diag = self
                    .runner_log_tail(uuid, 40)
                    .await
                    .as_deref()
                    .and_then(boot_failure_diagnostic);
                let reason = match boot_diag {
                    Some(diag) => format!("{gate_reason}; app-boot diagnostic: {diag}"),
                    None => gate_reason,
                };
                tracing::warn!(
                    uuid,
                    reff,
                    pid = handle.pid,
                    %reason,
                    "deploy: cold-spawn gate returned CrashLoop → 503 ColdStartUnhealthy (monitor keeps respawning in the background)"
                );
                return Err(anyhow::Error::new(ColdStartUnhealthy(format!(
                    "runner for {uuid} never became healthy: {reason}"
                ))));
            }
            tracing::info!(
                uuid,
                reff,
                pid = handle.pid,
                "deploy: cold-spawn healthy (app_health=serving)"
            );

            Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
                restart_status: restart_status_str(RestartStatus::Running),
                restart_count: 0,
                next_retry_at: 0,
                last_exit_at: 0,
                requested_runtime: runtime_override.map(str::to_owned),
            })
        }
    }

    /// Reset `uuid`'s crash-loop / backoff state and retry immediately.
    ///
    /// This is the `systemctl reset-failed` analog: it zeroes the
    /// `consecutive_failures` counter and clears `next_retry_at` so that a dead
    /// runner becomes immediately eligible for a respawn. Unlike [`purge_app`] it
    /// does NOT delete the artifact cache — the runner's cached image is kept for
    /// a fast cold start.
    ///
    /// After the state is cleared the record is persisted and
    /// the already-locked reconcile path is called so the
    /// respawn fires NOW rather than waiting for the next monitor tick.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `uuid` is not a valid UUID;
    /// - no on-disk record exists for `uuid` (i.e. the app was never started).
    pub async fn reset_app(&self, uuid: &str) -> Result<AppSummary> {
        let _ = self.app_ula_for(uuid)?;

        let lifecycle_lock = self.lifecycle_lock_for(uuid);
        let _serialize = lifecycle_lock.lock().await;

        // Load the record — a missing record means "never started" → 404.
        let mut record = self
            .load_runner_record(uuid)
            .with_context(|| format!("load runner record for {uuid}"))?
            .ok_or_else(|| anyhow::anyhow!("no runner record found for {uuid}"))?;

        // Clear the crash-loop / backoff state so the runner is immediately
        // eligible for a respawn (next_retry_at is zeroed by reset()).
        record.restart = restart::reset(record.restart);
        record.crash_looped = false;
        record.stopped = false;

        record
            .save(self.runner_dir())
            .with_context(|| format!("save runner record for {uuid} after reset"))?;

        // Fire an immediate reconcile: if the runner is dead it will be
        // respawned right now; if it is alive it is adopted untouched. Calling
        // the locked internal entry point avoids trying to acquire our own lock.
        let outcome = self.reconcile_record_locked(&record).await;
        if matches!(
            outcome,
            crate::orchestrator::monitor::RecordOutcome::RespawnFailed
                | crate::orchestrator::monitor::RecordOutcome::Missing
                | crate::orchestrator::monitor::RecordOutcome::Busy
        ) {
            anyhow::bail!("reset could not reconcile runner for {uuid}: {outcome:?}");
        }

        // Reconciliation may have replaced the PID/restart state. Reload before
        // constructing the response rather than returning the pre-reconcile copy.
        let record = self
            .load_runner_record(uuid)?
            .ok_or_else(|| anyhow::anyhow!("runner record disappeared during reset for {uuid}"))?;

        // Re-derive the summary from the (now-clean) on-disk record.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let state = if self.client_for(uuid).health().await.is_ok() {
            AppState::Running
        } else {
            AppState::Stopped
        };
        let rs =
            crate::orchestrator::restart::status(record.restart, BackoffParams::default(), now);
        Ok(AppSummary {
            uuid: record.uuid,
            app_ula: record.app_ula,
            state,
            restart_status: restart_status_str(rs),
            restart_count: record.restart.consecutive_failures,
            next_retry_at: record.restart.next_retry_at,
            last_exit_at: record.restart.last_exit_at,
            // Reset is a read/respawn path with no override.
            requested_runtime: None,
        })
    }

    /// Last `lines` lines of the per-app runner log (the spawned runner's
    /// stdout/stderr land there — see `spawn.rs::open_runner_log`). Best-effort
    /// diagnostics for spawn failures: a missing, unreadable, or EMPTY file
    /// returns `None` (spawn pre-creates the log before exec'ing the runner, so
    /// a failed exec leaves an empty file — an empty tail is noise, not signal).
    pub async fn runner_log_tail(&self, uuid: &str, lines: usize) -> Option<String> {
        let path = crate::orchestrator::spawn::runner_log_path(&self.shared().data_dir, uuid);
        read_last_lines(&path, lines)
            .await
            .ok()
            .filter(|t| !t.trim().is_empty())
    }

    /// Gracefully shut down a runner, then use bounded SIGKILL fallback. Success
    /// means both every known runner PID and the control-socket listener are gone.
    pub(crate) async fn shutdown_runner_definitive(
        &self,
        uuid: &str,
        recorded_pid: u32,
    ) -> Result<()> {
        let client = self.client_for(uuid);
        let mut known_pids = HashSet::new();
        let mut uncertain_pid = None;

        // A legacy stopped record may already have pid=0 while its runner still
        // answers. Recover the live PID from the identity-bearing health reply.
        if let Ok(Reply::Health { app_uuid, pid, .. }) =
            client.health_with_timeout(Duration::from_secs(1)).await
        {
            if app_uuid != uuid {
                anyhow::bail!(
                    "control socket identity mismatch for {uuid}: runner reported {app_uuid}"
                );
            }
            if pid != 0 {
                known_pids.insert(pid);
            }
        }

        match runner_pid_identity(recorded_pid, uuid) {
            RunnerPidIdentity::Matches => {
                known_pids.insert(recorded_pid);
            }
            RunnerPidIdentity::Unknown => uncertain_pid = Some(recorded_pid),
            RunnerPidIdentity::Mismatch => tracing::warn!(
                uuid,
                pid = recorded_pid,
                "recorded PID was reused by another process; excluding it from runner teardown"
            ),
            RunnerPidIdentity::Gone => {}
        }
        known_pids.extend(runner_pids_for_uuid(uuid));

        match client.shutdown().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to runner Shutdown"),
            Err(error) => tracing::debug!(
                uuid,
                error = %error,
                "runner Shutdown round-trip failed; checking process/socket before fallback"
            ),
        }

        if wait_runner_exit(
            &client,
            uuid,
            &known_pids,
            uncertain_pid,
            COLD_REAP_MAX_WAIT,
        )
        .await
        {
            return Ok(());
        }

        known_pids.extend(runner_pids_for_uuid(uuid));
        let live_pids: Vec<u32> = known_pids
            .iter()
            .copied()
            .chain(uncertain_pid)
            .filter(|pid| runner_identity_allows_sigkill(runner_pid_identity(*pid, uuid)))
            .collect();
        if live_pids.is_empty() {
            tracing::warn!(
                uuid,
                "runner socket survived graceful shutdown but exposed no killable PID"
            );
        } else {
            tracing::warn!(
                uuid,
                pids = ?live_pids,
                "runner survived graceful shutdown; applying SIGKILL fallback"
            );
            for pid in live_pids {
                // Revalidate immediately before every SIGKILL. Any PID can exit
                // and be reused while graceful shutdown is waiting, including a
                // PID learned from the earlier health reply.
                let identity = runner_pid_identity(pid, uuid);
                if runner_identity_allows_sigkill(identity) {
                    kill_pid(pid);
                } else {
                    tracing::warn!(
                        uuid,
                        pid,
                        ?identity,
                        "runner PID changed before SIGKILL; refusing to kill"
                    );
                }
            }
        }

        if wait_runner_exit(
            &client,
            uuid,
            &known_pids,
            uncertain_pid,
            COLD_REAP_MAX_WAIT,
        )
        .await
        {
            return Ok(());
        }
        let still_alive: Vec<u32> = known_pids
            .iter()
            .copied()
            .filter(|pid| runner_pid_may_still_match(*pid, uuid))
            .collect();
        let socket_reachable = client
            .socket_reachable(COLD_REAP_POLL_INTERVAL)
            .await
            .unwrap_or(true);
        anyhow::bail!(
            "runner for {uuid} survived definitive shutdown: live_pids={still_alive:?}, matching_pids={:?}, uncertain_pid={uncertain_pid:?}, socket_reachable={socket_reachable}",
            runner_pids_for_uuid(uuid)
        )
    }

    /// Remove `uuid`'s on-disk runner record. A missing file is success; every
    /// other deletion error is returned to the lifecycle caller.
    fn forget_record(&self, uuid: &str) -> std::io::Result<()> {
        let path = crate::orchestrator::handle::record_path(self.runner_dir(), uuid);
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Best-effort removal of every per-uuid AUXILIARY artifact once a purge is
    /// definitive. The runner RECORD (`runners/<uuid>.json`) is deliberately NOT
    /// touched here — its removal stays [`forget_record`](Self::forget_record)'s
    /// job, a hard error that keeps a failed purge retryable. Everything below
    /// is small leftover state that previously leaked on disk forever:
    /// - `runners/<uuid>.meshkey` — the runner's persistent WireGuard keypair
    /// - `runners/<uuid>.log` (+ rotated `.log.1`) — captured runner stdout/stderr
    /// - `runners/<uuid>.sock` — the control socket, if the dead runner left it
    /// - `fc/<uuid>.console.log` — FC serial-console capture
    /// - `build/<uuid>.log` + `build/<uuid>.progress.log` — build output logs
    /// - `runners.stale/<uuid>.*` — legacy graveyard entries (see
    ///   [`sweep_stale_runner_graveyard`](Self::sweep_stale_runner_graveyard))
    ///
    /// The FC pidfile is NOT listed: `kill_fc_child_for_uuid` already consumed
    /// it (purge bails earlier if FC teardown was not confirmed). A missing file
    /// is success; a real deletion error is logged and NEVER fails the purge.
    fn remove_app_artifacts(&self, uuid: &str) {
        let data_dir = &self.shared().data_dir;
        let runner_log = crate::orchestrator::spawn::runner_log_path(data_dir, uuid);
        let rotated_log = crate::orchestrator::spawn::rotated_log_path(&runner_log);
        let paths = [
            crate::runner::serve::runner_keypair_path(data_dir, uuid),
            rotated_log,
            runner_log,
            self.control_sock_for(uuid),
            crate::firecracker::pidfile::console_log_path(data_dir, uuid),
            crate::orchestrator::build::build_log_path(data_dir, uuid),
            crate::orchestrator::build::build_progress_log_path(data_dir, uuid),
        ];
        for path in &paths {
            remove_artifact_file(path, uuid);
        }
        remove_stale_graveyard_entries(data_dir, uuid);
    }

    /// One-shot startup sweep of the LEGACY `runners.stale/` graveyard.
    ///
    /// Nothing in the current codebase creates or reads `runners.stale/` — it is
    /// a leftover from an old supervisor generation / manual data-dir migration
    /// whose entries were never GC'd. Purge now removes per-uuid entries
    /// ([`remove_app_artifacts`](Self::remove_app_artifacts)), but entries
    /// orphaned BEFORE that fix would stay forever, so sweep them once per boot.
    /// Fail-closed: only files whose `<uuid>.`-prefix has NO runner record under
    /// `runners/` are removed; everything else is kept. Strictly bounded to
    /// `runners.stale/` — it never touches `runners/` itself. Best-effort: any
    /// error is logged and never fails startup.
    pub fn sweep_stale_runner_graveyard(&self) {
        let stale_dir = self.shared().data_dir.join(LEGACY_STALE_RUNNERS_DIR);
        let entries = match std::fs::read_dir(&stale_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => {
                tracing::warn!(path = %stale_dir.display(), error = %error, "cannot read legacy runner graveyard; skipping sweep");
                return;
            }
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            // Entries are `<uuid>.<ext>`; anything without that shape is kept.
            let Some((uuid, _ext)) = name.split_once('.') else {
                continue;
            };
            if uuid.is_empty()
                || crate::orchestrator::handle::record_path(self.runner_dir(), uuid).exists()
            {
                // A live record means the uuid is still known — keep its cruft
                // (purge will remove it definitively later).
                continue;
            }
            remove_artifact_file(&entry.path(), uuid);
        }
    }
}

/// Name of the legacy runner graveyard directory under `data_dir`. No current
/// code writes it; it exists only on hosts migrated from older generations.
const LEGACY_STALE_RUNNERS_DIR: &str = "runners.stale";

/// Remove one auxiliary per-app artifact file, best-effort: missing = success
/// (debug), removed = info, anything else = warn. Never returns an error — an
/// auxiliary file must never fail a purge.
fn remove_artifact_file(path: &std::path::Path, uuid: &str) {
    match std::fs::remove_file(path) {
        Ok(()) => tracing::info!(uuid, path = %path.display(), "removed per-app artifact"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(uuid, path = %path.display(), "per-app artifact already absent");
        }
        Err(error) => {
            tracing::warn!(uuid, path = %path.display(), error = %error, "failed to remove per-app artifact (continuing)");
        }
    }
}

/// Remove `uuid`'s files from the legacy `runners.stale/` graveyard (all
/// `<uuid>.*` entries). A missing directory is success. Prefix-matching on
/// `<uuid>.` is exact — `uuid` was already validated by the purge entry point,
/// so it cannot alias a different uuid's files.
fn remove_stale_graveyard_entries(data_dir: &std::path::Path, uuid: &str) {
    let stale_dir = data_dir.join(LEGACY_STALE_RUNNERS_DIR);
    let entries = match std::fs::read_dir(&stale_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(uuid, path = %stale_dir.display(), error = %error, "cannot read legacy runner graveyard (continuing)");
            return;
        }
    };
    let prefix = format!("{uuid}.");
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with(&prefix) {
            remove_artifact_file(&entry.path(), uuid);
        }
    }
}

const fn runner_identity_allows_sigkill(identity: RunnerPidIdentity) -> bool {
    matches!(identity, RunnerPidIdentity::Matches)
}

fn runner_pid_may_still_match(pid: u32, uuid: &str) -> bool {
    matches!(
        runner_pid_identity(pid, uuid),
        RunnerPidIdentity::Matches | RunnerPidIdentity::Unknown
    )
}

/// Map a [`RestartStatus`] to its snake_case wire string.
///
/// Mirrors the `#[serde(rename_all = "snake_case")]` derivation on `RestartStatus`
/// so callers that need an owned `String` do not have to serialize the whole enum.
fn restart_status_str(rs: RestartStatus) -> String {
    match rs {
        RestartStatus::Running => "running",
        RestartStatus::Backoff => "backoff",
        RestartStatus::CrashLoop => "crashloop",
    }
    .to_owned()
}

/// Poll `client.health()` until it succeeds or `timeout` elapses. Returns the
/// final [`Reply`] on success, or an error describing the last failure.
async fn wait_healthy(client: &ControlClient, timeout: Duration) -> Result<Reply> {
    let deadline = std::time::Instant::now() + timeout;
    // Poll faster than the monitor interval so a freshly-spawned runner is seen
    // healthy quickly; the cap keeps a doomed start from busy-looping.
    let poll = (MONITOR_INTERVAL / 100).max(Duration::from_millis(50));
    let mut last_err = None;
    while std::time::Instant::now() < deadline {
        match client.health().await {
            Ok(reply) => return Ok(reply),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(poll).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "control socket never became healthy within {timeout:?}: {last_err:?}"
    ))
}

/// Wait until all known PIDs have exited and the control socket no longer has a
/// listener. Reachability probe errors fail closed and are treated as live.
async fn wait_runner_exit(
    client: &ControlClient,
    uuid: &str,
    known_pids: &HashSet<u32>,
    uncertain_pid: Option<u32>,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let process_alive = known_pids
            .iter()
            .any(|pid| runner_pid_may_still_match(*pid, uuid))
            || uncertain_pid.is_some_and(|pid| runner_pid_may_still_match(pid, uuid))
            || !runner_pids_for_uuid(uuid).is_empty();
        let socket_reachable = client
            .socket_reachable(COLD_REAP_POLL_INTERVAL)
            .await
            .unwrap_or(true);
        if !process_alive && !socket_reachable {
            return true;
        }
        tokio::time::sleep(COLD_REAP_POLL_INTERVAL).await;
    }
    let process_alive = known_pids
        .iter()
        .any(|pid| runner_pid_may_still_match(*pid, uuid))
        || uncertain_pid.is_some_and(|pid| runner_pid_may_still_match(pid, uuid))
        || !runner_pids_for_uuid(uuid).is_empty();
    let socket_reachable = client
        .socket_reachable(COLD_REAP_POLL_INTERVAL)
        .await
        .unwrap_or(true);
    !process_alive && !socket_reachable
}

/// Cold-spawn readiness verdict (option B). `Serving` = the runner reports
/// `app_health = "serving"`. `CrashLoop` = a TERMINAL failure: the runner
/// process exited before serving (unambiguous crash — the common wrong-port /
/// PID-1-exits case, caught in ~30s), or the deadline elapsed on an
/// alive-but-never-serving runner.
enum ColdStart {
    Serving,
    CrashLoop(String),
}

/// Bounded cold-spawn health gate. Unlike [`wait_healthy`], this checks the REAL
/// app-level health (`app_health == "serving"`), so a reachable-socket-but-app-
/// down runner does NOT count as ready; and it resolves a terminal `CrashLoop`
/// the instant the runner PROCESS dies (via `runner_is_alive` — the exact
/// liveness probe the monitor uses), so a wrong-port cold start fails fast
/// (~30s) instead of polling a dead socket for the full 360s timeout. A
/// genuinely slow-but-ALIVE cold boot keeps its process alive and is polled
/// through to `Serving` — the death path never mislabels it. Does NOT touch the
/// monitor: the background respawn ladder is untouched.
async fn wait_cold_serving(client: &ControlClient, pid: u32, timeout: Duration) -> ColdStart {
    let deadline = std::time::Instant::now() + timeout;
    let poll = (MONITOR_INTERVAL / 100).max(Duration::from_millis(50));
    let mut last_reason: Option<String> = None;
    let mut polls: u32 = 0;
    while std::time::Instant::now() < deadline {
        polls += 1;
        match client.health().await {
            Ok(Reply::Health {
                app_health,
                app_health_reason,
                ..
            }) => {
                // Per-poll app-level health so a "never became ready" verdict is
                // backed by the exact health values seen along the way (e.g. a
                // guest stuck at `booting`/`unhealthy` vs one that never answered).
                tracing::debug!(
                    pid,
                    poll = polls,
                    %app_health,
                    app_health_reason = app_health_reason.as_deref().unwrap_or(""),
                    "cold-spawn gate: health poll"
                );
                if app_health == "serving" {
                    tracing::info!(
                        pid,
                        polls,
                        "cold-spawn gate verdict: Serving (app_health=serving)"
                    );
                    return ColdStart::Serving;
                }
                last_reason =
                    app_health_reason.or_else(|| Some(format!("app_health={app_health}")));
            }
            Ok(Reply::Err { message }) => {
                tracing::debug!(pid, poll = polls, %message, "cold-spawn gate: runner Err reply");
                last_reason = Some(message);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(pid, poll = polls, error = %e, "cold-spawn gate: control socket unreachable this poll");
                last_reason = Some(format!("control socket unreachable: {e}"));
            }
        }
        // Unambiguous crash: the runner process is gone before it ever served.
        if !crate::orchestrator::monitor::runner_is_alive(pid) {
            let reason = format!(
                "runner process exited before serving (last: {})",
                last_reason.as_deref().unwrap_or("no health reply")
            );
            tracing::warn!(
                pid,
                polls,
                verdict = "crashloop",
                cause = "process-death",
                %reason,
                "cold-spawn gate verdict: CrashLoop — runner process died before serving"
            );
            return ColdStart::CrashLoop(reason);
        }
        tokio::time::sleep(poll).await;
    }
    let reason = format!(
        "never reached app_health=serving within {timeout:?} (last: {})",
        last_reason.as_deref().unwrap_or("no health reply")
    );
    tracing::warn!(
        pid,
        polls,
        verdict = "crashloop",
        cause = "deadline",
        timeout_secs = timeout.as_secs(),
        %reason,
        "cold-spawn gate verdict: CrashLoop — deadline elapsed, runner alive but never served"
    );
    ColdStart::CrashLoop(reason)
}

/// Extract the cold-boot readiness diagnostic from a runner-log `tail` (P1-2).
///
/// The runner's `firecracker::wait_until_ready` prints a precise, actionable
/// message on a readiness timeout — the app not LISTENING on its EXPOSE port, a
/// daemonized (non-foreground) PID 1, or a missing `CAP_NET_ADMIN` — first via a
/// `tracing::error!` then again as the `bail!` chain the failing runner's
/// `main()` writes to stderr. Both land in the per-app runner log. The
/// cold-spawn gate only ever sees the terse "process exited before serving"
/// (the runner is already dead), so this recovers the real reason from the log.
///
/// Returns the LAST line matching a known readiness-failure marker (the newest
/// boot attempt wins on a respawn ladder), trimmed. `None` when the tail holds
/// no such marker — the caller then keeps the terse gate reason unchanged.
fn boot_failure_diagnostic(tail: &str) -> Option<String> {
    // Markers emitted by `wait_until_ready` (firecracker/linux.rs), matched
    // case-insensitively so both the `tracing::error!` guidance and the `bail!`
    // sentence are caught regardless of casing.
    const MARKERS: [&str; 3] = ["never became ready", "not listening", "cap_net_admin"];
    let line = tail.lines().rev().find(|line| {
        let lower = line.to_ascii_lowercase();
        MARKERS.iter().any(|m| lower.contains(m))
    })?;
    let trimmed = line.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// Bounded tail: reads at most the last `CHUNK` bytes of `path`, then keeps the
/// final `lines` lines. Runner logs are size-capped (rotated at ~50 MB by
/// `spawn::rotate_if_oversized`) but can still be tens of MB, so this must NEVER
/// slurp the whole file (an error-path heap spike on a large log). The seek can
/// land mid-UTF-8-codepoint, so the chunk decodes lossily rather than erroring.
///
/// NOTE: the tail surfaces whatever the runner printed — keep this API
/// mesh-internal; do not propagate to external callers unredacted.
pub(crate) async fn read_last_lines(
    path: &std::path::Path,
    lines: usize,
) -> std::io::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    const CHUNK: u64 = 8192;
    let mut f = tokio::fs::File::open(path).await?;
    let len = f.metadata().await?.len();
    f.seek(std::io::SeekFrom::Start(len.saturating_sub(CHUNK)))
        .await?;
    let mut buf = Vec::with_capacity(CHUNK as usize);
    // `take` keeps the read bounded even if the (append-active) log grows
    // between the metadata call and the read.
    f.take(CHUNK).read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf);
    let tail: Vec<&str> = text.lines().rev().take(lines).collect();
    Ok(tail.into_iter().rev().collect::<Vec<_>>().join("\n"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use super::*;
    use crate::orchestrator::{SharedRunnerConfig, monitor::runner_is_alive};

    const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";
    const APP_ULA: &str = "fd5a:1f02:44a5:240b:121a::1";

    fn orch(runner_dir: PathBuf) -> Orchestrator {
        Orchestrator::new(
            SharedRunnerConfig {
                runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
                s3_base_url: "http://s3.invalid".to_owned(),
                data_dir: PathBuf::from("/var/lib/tabbify/data"),
                parent: None,
                no_mesh: true,
                relay_url: None,
                relay_only: false,
            },
            runner_dir,
        )
    }

    /// The cold-spawn health wait must cover a cold pull over the relay-only WAN
    /// — otherwise a dev-session create false-fails + orphans the VM (#67).
    #[test]
    fn start_healthy_timeout_covers_a_cold_wan_pull() {
        assert!(
            START_HEALTHY_TIMEOUT >= std::time::Duration::from_secs(300),
            "cold-spawn health wait must outlast a cold WAN pull"
        );
    }

    #[test]
    fn sigkill_requires_fresh_exact_runner_identity() {
        assert!(runner_identity_allows_sigkill(RunnerPidIdentity::Matches));
        assert!(!runner_identity_allows_sigkill(RunnerPidIdentity::Gone));
        assert!(!runner_identity_allows_sigkill(RunnerPidIdentity::Mismatch));
        assert!(!runner_identity_allows_sigkill(RunnerPidIdentity::Unknown));
    }

    #[test]
    fn state_wire_strings() {
        assert_eq!(AppState::Running.as_str(), "running");
        assert_eq!(AppState::Stopped.as_str(), "stopped");
    }

    #[test]
    fn control_sock_is_uuid_dot_sock_under_runner_dir() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        assert_eq!(
            o.control_sock_for(APP_UUID),
            PathBuf::from("/run/tabbify/runners/0191e7c2-1111-7222-8333-444455556666.sock")
        );
    }

    #[test]
    fn app_ula_matches_derive() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        assert_eq!(o.app_ula_for(APP_UUID).unwrap().to_string(), APP_ULA);
        assert!(o.app_ula_for("not-a-uuid").is_err());
    }

    #[test]
    fn spawn_spec_carries_derived_sock_and_shared_fields() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        let spec = o.spawn_spec_for_uuid(APP_UUID);
        assert_eq!(spec.uuid, APP_UUID);
        assert_eq!(spec.control_sock, o.control_sock_for(APP_UUID));
        assert_eq!(spec.s3_base_url, "http://s3.invalid");
        assert!(spec.no_mesh);
        assert!(spec.parent.is_none());
    }

    #[test]
    fn start_spec_preserves_existing_deploy_context() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut seeded = seed_deployed_record(dir.path(), APP_UUID);
        seeded.pid = 0;
        seeded.stopped = true;
        seeded.save(dir.path()).unwrap();

        let (spec, existing) = o.spawn_spec_for_start(APP_UUID).unwrap();

        assert!(existing.is_some_and(|record| record.stopped));
        assert_eq!(spec.image_ref, seeded.image_ref);
        assert_eq!(spec.manifest_toml, seeded.manifest_toml);
        assert_eq!(spec.network, seeded.network);
        assert_eq!(spec.runner_join_token, seeded.runner_join_token);
        assert_eq!(spec.extra_env, seeded.extra_env);
        assert_eq!(spec.egress_allow, seeded.egress_allow);
    }

    #[test]
    fn start_spec_without_record_uses_fresh_s3_defaults() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());

        let (spec, existing) = o.spawn_spec_for_start(APP_UUID).unwrap();

        assert!(existing.is_none());
        assert!(spec.image_ref.is_none());
        assert!(spec.manifest_toml.is_none());
        assert!(spec.network.is_none());
        assert!(spec.runner_join_token.is_none());
        assert!(spec.extra_env.is_none());
        assert!(spec.egress_allow.is_none());
    }

    #[test]
    fn start_rejects_record_with_mismatched_identity() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut record = seed_deployed_record(dir.path(), APP_UUID);
        record.uuid = "0191e7c2-2222-7222-8333-444455556666".to_owned();
        std::fs::write(
            dir.path().join(format!("{APP_UUID}.json")),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();

        let error = o.spawn_spec_for_start(APP_UUID).unwrap_err();

        assert!(format!("{error:#}").contains("identity mismatch"));
    }

    #[test]
    fn start_fails_closed_on_unparseable_durable_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        std::fs::write(dir.path().join(format!("{APP_UUID}.json")), b"not-json").unwrap();

        let error = o.spawn_spec_for_start(APP_UUID).unwrap_err();

        assert!(error.to_string().contains("load runner record"));
    }

    #[test]
    fn cold_deploy_omitted_context_keeps_durable_fields() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let existing = seed_deployed_record(dir.path(), APP_UUID);

        let spec = o.spawn_spec_for_deploy(
            APP_UUID,
            Some(&existing),
            "registry/app:new",
            None,
            &DeployNetwork::default(),
            None,
            None,
        );

        assert_eq!(spec.image_ref.as_deref(), Some("registry/app:new"));
        assert_eq!(spec.network, existing.network);
        assert_eq!(spec.runner_join_token, existing.runner_join_token);
        assert_eq!(spec.extra_env, existing.extra_env);
        assert_eq!(spec.egress_allow, existing.egress_allow);
        assert_eq!(
            spec.manifest_toml, existing.manifest_toml,
            "an omitted manifest must keep the record's — clearing it here is what \
             silently stripped [runtime].stateful and sent the app back to an \
             ephemeral rootfs"
        );
    }

    /// A malformed uuid is a clear error on the read paths (so the API can 400)
    /// without touching any socket.
    #[tokio::test]
    async fn app_state_rejects_bad_uuid() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        assert!(o.app_state("not-a-uuid").await.is_err());
    }

    // ── restart_status_str ────────────────────────────────────────────────────

    #[test]
    fn restart_status_str_running() {
        assert_eq!(restart_status_str(RestartStatus::Running), "running");
    }

    #[test]
    fn restart_status_str_backoff() {
        assert_eq!(restart_status_str(RestartStatus::Backoff), "backoff");
    }

    #[test]
    fn restart_status_str_crashloop() {
        assert_eq!(restart_status_str(RestartStatus::CrashLoop), "crashloop");
    }

    // ── AppSummary restart fields ─────────────────────────────────────────────

    /// Construct a RunnerHandle with `consecutive_failures = 5`, build an
    /// AppSummary from the record fields (mirroring what app_summaries does),
    /// and assert restart_status == "crashloop" and restart_count == 5.
    #[test]
    fn app_summary_fields_crashloop() {
        use tempfile::TempDir;

        use crate::orchestrator::restart::{BackoffParams, RestartState, status};

        let dir = TempDir::new().unwrap();

        let restart = RestartState {
            consecutive_failures: 5,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_000_160,
            last_healthy_at: 0,
        };

        // Persist a record so we can load it back (mirrors real data path).
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 1,
            control_sock: PathBuf::from("/tmp/test.sock"),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart,
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();
        let loaded = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();

        // Reproduce what app_summaries/app_summary does.
        let now = 1_700_001_000u64;
        let rs = status(loaded.restart, BackoffParams::default(), now);
        let summary = AppSummary {
            uuid: loaded.uuid.clone(),
            app_ula: loaded.app_ula.clone(),
            state: AppState::Stopped,
            restart_status: restart_status_str(rs),
            restart_count: loaded.restart.consecutive_failures,
            next_retry_at: loaded.restart.next_retry_at,
            last_exit_at: loaded.restart.last_exit_at,
            requested_runtime: None,
        };

        assert_eq!(summary.restart_status, "crashloop");
        assert_eq!(summary.restart_count, 5);
        assert_eq!(summary.next_retry_at, 1_700_000_160);
    }

    /// A fresh (never-failed) record produces restart_status == "running" and
    /// restart_count == 0.
    #[test]
    fn app_summary_fields_default_is_running() {
        use crate::orchestrator::restart::{BackoffParams, RestartState, status};

        let restart = RestartState::default();
        let now = 1_700_001_000u64;
        let rs = status(restart, BackoffParams::default(), now);
        let restart_status = restart_status_str(rs);

        assert_eq!(restart_status, "running");
        assert_eq!(restart.consecutive_failures, 0);
        assert_eq!(restart.next_retry_at, 0);
    }

    // ── reset_app state clearing ──────────────────────────────────────────────

    /// reset_app clears the crash-loop state on the persisted record: after the
    /// call, loading the record shows consecutive_failures == 0 and
    /// next_retry_at == 0, so should_respawn is true for any `now`.
    ///
    /// The reconcile_record call inside reset_app will try (and fail) to spawn
    /// a real runner process — that failure is tolerated (logged, not
    /// propagated). We verify the state-clearing effect on the persisted record,
    /// which is the core guarantee of reset.
    #[tokio::test]
    async fn reset_app_persists_new_backoff_when_immediate_respawn_fails() {
        use tempfile::TempDir;

        use crate::orchestrator::restart::RestartState;

        let dir = TempDir::new().unwrap();
        let o = orch(dir.path().to_path_buf());

        // Write a record with a crash-loop restart state.
        let crashed_restart = RestartState {
            consecutive_failures: 5,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_000_160,
            last_healthy_at: 0,
        };
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 99_999_999, // deliberately non-existent pid
            control_sock: dir.path().join(format!("{APP_UUID}.sock")),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: crashed_restart,
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let result = o.reset_app(APP_UUID).await;
        assert!(result.is_err(), "failed immediate respawn must fail reset");

        // Reset clears the old streak, then the failed immediate spawn is a new
        // failure and must be durable for the next monitor tick.
        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must still exist after reset");
        assert!(
            updated.restart.consecutive_failures == 1 && updated.restart.next_retry_at > 0,
            "spawn failure after reset must persist a fresh backoff state: {:?}",
            updated.restart
        );
    }

    /// reset_app returns an error for an unknown uuid (no on-disk record), so
    /// the HTTP handler can return 404.
    #[tokio::test]
    async fn reset_app_errors_for_unknown_uuid() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let o = orch(dir.path().to_path_buf());

        let result = o.reset_app(APP_UUID).await;
        assert!(
            result.is_err(),
            "reset_app for an unknown uuid must return an error"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no runner record found"),
            "error message must mention 'no runner record found'"
        );
    }

    /// reset_app rejects malformed uuids before touching any on-disk state.
    #[tokio::test]
    async fn reset_app_rejects_bad_uuid() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        assert!(
            o.reset_app("not-a-uuid").await.is_err(),
            "malformed uuid must be rejected"
        );
    }

    #[tokio::test]
    async fn reset_waits_for_lifecycle_lock_and_reconciles_without_deadlock() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut record = seed_deployed_record(dir.path(), APP_UUID);
        record.pid = 0;
        record.stopped = true;
        record.save(dir.path()).unwrap();

        let lock = o.lifecycle_lock_for(APP_UUID);
        let guard = lock.lock().await;
        assert_eq!(
            o.reconcile_record(&record).await,
            crate::orchestrator::monitor::RecordOutcome::Busy,
            "monitor must try-lock and skip while reset/lifecycle work owns the UUID"
        );

        let reset_orchestrator = o.clone();
        let reset = tokio::spawn(async move { reset_orchestrator.reset_app(APP_UUID).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !reset.is_finished(),
            "reset must wait for the lifecycle lock"
        );
        drop(guard);

        let result = tokio::time::timeout(Duration::from_secs(2), reset)
            .await
            .expect("reset must not deadlock after acquiring its lock")
            .unwrap();
        assert!(
            result.is_err(),
            "reset must report the immediate respawn failure: {result:?}"
        );
    }

    // ── deploy_app ────────────────────────────────────────────────────────────

    /// deploy_app rejects malformed uuids before touching any socket.
    #[tokio::test]
    async fn deploy_app_rejects_bad_uuid() {
        let o = orch(PathBuf::from("/run/tabbify/runners"));
        assert!(
            o.deploy_app(
                "not-a-uuid",
                "reg:5000/a/b:sha",
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await
            .is_err(),
            "malformed uuid must be rejected"
        );
    }

    /// deploy_app: when no live runner exists AND no runner binary is available
    /// the spawn fails fast with an error (cold-path failure path test).
    #[tokio::test]
    async fn deploy_app_cold_path_spawn_fails_cleanly() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let o = orch(dir.path().to_path_buf());

        // No runner binary → spawn will fail → deploy returns Err.
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/a/b:sha",
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "deploy_app must fail when the runner binary is missing"
        );
    }

    /// deploy_app: after a Deploy control command succeeds (simulated via a live
    /// fake unix-socket server that returns Reply::Ok), the persisted RunnerHandle
    /// must have image_ref updated to the deployed ref.
    ///
    /// This test wires up a minimal fake control server on a real Unix socket so
    /// the orchestrator's deploy_app round-trips through ControlClient::deploy.
    #[tokio::test]
    async fn deploy_app_live_path_persists_image_ref() {
        use std::time::Duration;

        use tempfile::TempDir;
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));

        // Spawn a fake control server: answers Deploy{...} with Reply::Ok,
        // Health with Reply::Ok (for the liveness check), then exits.
        let sock_path_srv = sock_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path_srv).unwrap();
            // Handle a few connections (health probe + deploy command).
            for _ in 0..5 {
                match tokio::time::timeout(Duration::from_secs(2), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        let _ = reader.read_line(&mut line).await;
                        let command: crate::control_proto::Cmd =
                            serde_json::from_str(line.trim()).unwrap();
                        let reply = if matches!(command, crate::control_proto::Cmd::Health) {
                            Reply::Health {
                                state: "running".to_owned(),
                                app_ula: APP_ULA.to_owned(),
                                app_uuid: APP_UUID.to_owned(),
                                pid: 12345,
                                image_ref: None,
                                app_health: "serving".to_owned(),
                                app_health_reason: None,
                            }
                        } else {
                            Reply::Ok
                        };
                        let mut payload = serde_json::to_vec(&reply).unwrap();
                        payload.push(b'\n');
                        let _ = reader.into_inner().write_all(&payload).await;
                    }
                    _ => break,
                }
            }
        });

        // Give the fake server a moment to bind.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Persist a runner record so the live-path can load/mutate/save it.
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let reff = "[fd5a::1]:5000/acme/app:sha256abc";
        let result = o
            .deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await;

        assert!(result.is_ok(), "deploy_app must succeed: {result:?}");
        let summary = result.unwrap();
        assert_eq!(summary.uuid, APP_UUID);
        assert_eq!(summary.state, AppState::Running);

        // The persisted handle must carry the deployed ref.
        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must still exist after deploy");
        assert_eq!(
            updated.image_ref.as_deref(),
            Some(reff),
            "image_ref must be persisted after a live deploy"
        );
    }

    #[tokio::test]
    async fn warm_deploy_save_failure_shuts_runner_down_fail_closed() {
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let mut record = seed_deployed_record(dir.path(), APP_UUID);
        record.pid = 0;
        let record_path = crate::orchestrator::handle::record_path(dir.path(), APP_UUID);
        std::fs::remove_file(&record_path).unwrap();
        std::fs::create_dir(&record_path).unwrap();

        let socket = record.control_sock.clone();
        let server_socket = socket.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&server_socket).unwrap();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    continue;
                }
                let command: crate::control_proto::Cmd = serde_json::from_str(line.trim()).unwrap();
                let shutdown = matches!(command, crate::control_proto::Cmd::Shutdown);
                let reply = if shutdown {
                    Reply::Ok
                } else {
                    Reply::Health {
                        state: "running".to_owned(),
                        app_ula: APP_ULA.to_owned(),
                        app_uuid: APP_UUID.to_owned(),
                        pid: 0,
                        image_ref: Some("registry/app:new".to_owned()),
                        app_health: "serving".to_owned(),
                        app_health_reason: None,
                    }
                };
                let mut payload = serde_json::to_vec(&reply).unwrap();
                payload.push(b'\n');
                reader.into_inner().write_all(&payload).await.unwrap();
                if shutdown {
                    break;
                }
            }
            drop(listener);
            let _ = std::fs::remove_file(server_socket);
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        record.image_ref = Some("registry/app:new".to_owned());
        let error = o
            .persist_warm_deploy(&record, Some("registry/app:old"), "registry/app:new")
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("save runner record"));
        assert!(
            !ControlClient::new(&socket)
                .socket_reachable(Duration::from_millis(100))
                .await
                .unwrap(),
            "save failure compensation must leave no live runner socket"
        );
    }

    /// Spawn a fake runner control server on `sock_path` that answers Health
    /// with an identity-bearing snapshot and mutations with `Ok`, then UNLINKS
    /// the socket so any further connect
    /// fails fast — so a force-cold reap's post-shutdown health probe returns
    /// promptly instead of waiting out the whole reap timeout.
    fn spawn_fake_ok_server(sock_path: PathBuf, max_conns: usize) {
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path).unwrap();
            for _ in 0..max_conns {
                match tokio::time::timeout(Duration::from_secs(2), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        let _ = reader.read_line(&mut line).await;
                        let command: crate::control_proto::Cmd =
                            serde_json::from_str(line.trim()).unwrap();
                        let reply = if matches!(command, crate::control_proto::Cmd::Health) {
                            Reply::Health {
                                state: "running".to_owned(),
                                app_ula: APP_ULA.to_owned(),
                                app_uuid: APP_UUID.to_owned(),
                                pid: 12345,
                                image_ref: None,
                                app_health: "serving".to_owned(),
                                app_health_reason: None,
                            }
                        } else {
                            Reply::Ok
                        };
                        let mut payload = serde_json::to_vec(&reply).unwrap();
                        payload.push(b'\n');
                        let _ = reader.into_inner().write_all(&payload).await;
                    }
                    _ => break,
                }
            }
            // Unbind + unlink so a later connect gets ENOENT (fast reap exit).
            let _ = std::fs::remove_file(&sock_path);
        });
    }

    /// Build a live-runner record whose control socket is `sock_path`, image is
    /// `reff`, and baked env is `extra_env`.
    fn live_record(
        sock_path: &std::path::Path,
        reff: &str,
        extra_env: Option<HashMap<String, String>>,
    ) -> RunnerHandle {
        RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.to_path_buf(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: Some(reff.to_owned()),
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        }
    }

    /// (b) A live runner + the SAME env-hash as the record does NOT force a cold
    /// respawn — it takes the warm zero-downtime swap (the existing
    /// no-op-preserving behavior). Proven by the deploy SUCCEEDING through the
    /// fake live socket (a forced cold path would try to spawn the missing runner
    /// binary and Err).
    #[tokio::test]
    async fn deploy_app_same_env_takes_warm_swap() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        spawn_fake_ok_server(sock_path.clone(), 5);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let reff = "[fd5a::1]:5000/acme/app:sha256abc";
        let env = HashMap::from([("K".to_owned(), "v".to_owned())]);
        live_record(&sock_path, reff, Some(env.clone()))
            .save(dir.path())
            .unwrap();

        let o = orch(dir.path().to_path_buf());
        // Same reff (⇒ same digest) AND same env ⇒ warm swap, not cold.
        let result = o
            .deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                Some(&env),
                None,
            )
            .await;
        assert!(
            result.is_ok(),
            "same-env deploy must warm-swap (succeed), not force a cold respawn: {result:?}"
        );
        let updated = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();
        assert_eq!(
            updated.image_ref.as_deref(),
            Some(reff),
            "the warm path must persist the ref"
        );
    }

    /// (a) A live runner + a DIFFERENT env-hash must NOT warm-swap (a warm swap
    /// can't re-bake `/init`, so the same-digest no-op would silently drop the
    /// change). It force-COLD-respawns: reap the live runner, then spawn a fresh
    /// one. With no runner binary the cold spawn Errs — which is exactly how we
    /// prove the deploy diverted AWAY from the (would-succeed) warm swap.
    #[tokio::test]
    async fn deploy_app_changed_env_forces_cold_respawn() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        // health probe + Shutdown = 2 conns, then the socket is unlinked.
        spawn_fake_ok_server(sock_path.clone(), 2);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let reff = "[fd5a::1]:5000/acme/app:sha256abc";
        let old_env = HashMap::from([("K".to_owned(), "OLD".to_owned())]);
        live_record(&sock_path, reff, Some(old_env))
            .save(dir.path())
            .unwrap();

        let o = orch(dir.path().to_path_buf());
        let new_env = HashMap::from([("K".to_owned(), "NEW".to_owned())]);
        // SAME reff (same digest), DIFFERENT env ⇒ forced cold respawn ⇒ the
        // (binary-less) spawn Errs.
        let result = o
            .deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                Some(&new_env),
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "a changed-env deploy must force a cold respawn (which Errs with no runner binary), not a warm swap"
        );
    }

    /// (c) `add_repo`'s respawn: appending a repo cap to `CAP_FILES_ENV` changes
    /// the env-hash even though the image is unchanged, so deploy_app (the path
    /// `add_workspace_repo` drives) force-COLD-boots rather than warm-swapping the
    /// stale env — the ONLY way the guest's broker re-runs its boot-clone for the
    /// new repo (#108). The forced cold spawn Errs without a runner binary,
    /// proving the divert.
    #[tokio::test]
    async fn deploy_app_add_repo_cap_change_forces_cold_respawn() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        spawn_fake_ok_server(sock_path.clone(), 2);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let reff = "[fd5a::1]:5000/acme/ws:sha256deadbeef";
        let cap_key = crate::api::CAP_FILES_ENV;
        // Create-time env: one repo cap.
        let before = HashMap::from([
            ("TABBIFY_WORKSPACE_UUID".to_owned(), APP_UUID.to_owned()),
            (
                cap_key.to_owned(),
                r#"{"apartami.url":"http://g/a"}"#.to_owned(),
            ),
        ]);
        live_record(&sock_path, reff, Some(before))
            .save(dir.path())
            .unwrap();

        let o = orch(dir.path().to_path_buf());
        // add_repo merges a NEW `tetris.url` cap into CAP_FILES_ENV (same image).
        let after = HashMap::from([
            ("TABBIFY_WORKSPACE_UUID".to_owned(), APP_UUID.to_owned()),
            (
                cap_key.to_owned(),
                r#"{"apartami.url":"http://g/a","tetris.url":"http://g/t"}"#.to_owned(),
            ),
        ]);
        let result = o
            .deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                Some(&after),
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "add_repo's new cap must force a cold respawn (Errs with no runner binary), not a stale warm swap"
        );
    }

    /// `snapshot_app` round-trips `Cmd::Snapshot` to the runner and maps a
    /// `Reply::Ok` to `Ok(())` (the §12 post-index warm-snapshot path). A fake
    /// control server captures the request line to prove the right command went
    /// out and answers `ok`.
    #[tokio::test]
    async fn snapshot_app_round_trips_ok_and_sends_snapshot_cmd() {
        use std::{sync::Arc, time::Duration};

        use tempfile::TempDir;
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
            sync::Mutex,
        };

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_srv = captured.clone();
        let sock_path_srv = sock_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path_srv).unwrap();
            if let Ok(Ok((stream, _))) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
            {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                let _ = reader.read_line(&mut line).await;
                *captured_srv.lock().await = line;
                let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let o = orch(dir.path().to_path_buf());
        o.snapshot_app(APP_UUID)
            .await
            .expect("snapshot must succeed");
        assert!(
            captured.lock().await.contains("\"cmd\":\"snapshot\""),
            "snapshot_app must send Cmd::Snapshot, got: {}",
            captured.lock().await
        );
    }

    /// A runner `Reply::Err` (the snapshot create did not land — VM still
    /// serving) surfaces as an `Err` so the node can retry, carrying the runner's
    /// message.
    #[tokio::test]
    async fn snapshot_app_surfaces_runner_err() {
        use std::time::Duration;

        use tempfile::TempDir;
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        let sock_path_srv = sock_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path_srv).unwrap();
            if let Ok(Ok((stream, _))) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
            {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                let _ = reader.read_line(&mut line).await;
                let _ = reader
                    .into_inner()
                    .write_all(
                        b"{\"reply\":\"err\",\"message\":\"create did not produce files\"}\n",
                    )
                    .await;
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let o = orch(dir.path().to_path_buf());
        let err = o.snapshot_app(APP_UUID).await.unwrap_err();
        assert!(
            err.to_string().contains("create did not produce files"),
            "the runner's snapshot error must surface: {err}"
        );
    }

    /// A malformed uuid is rejected before any control I/O (so the API layer can
    /// 400 it) — mirrors `stop_app`/`purge_app`'s up-front uuid validation.
    #[tokio::test]
    async fn snapshot_app_rejects_bad_uuid() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch(dir.path().to_path_buf());
        assert!(o.snapshot_app("not-a-uuid").await.is_err());
    }

    #[tokio::test]
    async fn snapshot_waits_for_the_app_lifecycle_lock() {
        let dir = tempfile::TempDir::new().unwrap();
        let orchestrator = orch(dir.path().to_path_buf());
        let lock = orchestrator.lifecycle_lock_for(APP_UUID);
        let guard = lock.lock().await;
        let snapshot_orchestrator = orchestrator.clone();
        let snapshot =
            tokio::spawn(async move { snapshot_orchestrator.snapshot_app(APP_UUID).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!snapshot.is_finished());
        drop(guard);

        let result = tokio::time::timeout(Duration::from_secs(2), snapshot)
            .await
            .expect("snapshot must proceed after lifecycle lock release")
            .unwrap();
        assert!(result.is_err(), "fixture has no runner socket");
    }

    /// Two concurrent `deploy_app` calls for the SAME uuid must be SERIALIZED.
    ///
    /// The bug this guards: a single push fans out into two deploys (e.g.
    /// `commit_repo_edit`'s redeploy + the GitHub-App webhook's redeploy of the
    /// same commit). Without a per-uuid lock both reach the runner's control
    /// socket at once and race the in-flight FC swap → one returns
    /// `Err("deploy control message failed")` (supervisor 500) and the final
    /// artifact is non-deterministic.
    ///
    /// We prove serialization by counting the MAX number of simultaneously-open
    /// control connections. Each `deploy_app` opens two (a health probe then the
    /// Deploy), and the fake server holds every connection open for 80ms while
    /// accepting new ones concurrently. Serialized deploys never overlap on the
    /// socket → max concurrency 1; a racing implementation reaches 2.
    #[tokio::test]
    async fn deploy_app_serializes_concurrent_same_uuid() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Duration,
        };

        use tempfile::TempDir;
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));

        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let sock_path_srv = sock_path.clone();
        let active_srv = Arc::clone(&active);
        let max_srv = Arc::clone(&max_seen);
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path_srv).unwrap();
            // 2 deploys × (health + deploy) = 4 connections; accept a few extra.
            for _ in 0..8 {
                match tokio::time::timeout(Duration::from_secs(3), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let active = Arc::clone(&active_srv);
                        let max_seen = Arc::clone(&max_srv);
                        // Handle each connection on its own task so the server
                        // never serializes — overlap must come (or not) from the
                        // client side under test.
                        tokio::spawn(async move {
                            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                            max_seen.fetch_max(now, Ordering::SeqCst);
                            let mut reader = BufReader::new(stream);
                            let mut line = String::new();
                            let _ = reader.read_line(&mut line).await;
                            tokio::time::sleep(Duration::from_millis(80)).await;
                            let command: crate::control_proto::Cmd =
                                serde_json::from_str(line.trim()).unwrap();
                            let reply = if matches!(command, crate::control_proto::Cmd::Health) {
                                Reply::Health {
                                    state: "running".to_owned(),
                                    app_ula: APP_ULA.to_owned(),
                                    app_uuid: APP_UUID.to_owned(),
                                    pid: 12345,
                                    image_ref: None,
                                    app_health: "serving".to_owned(),
                                    app_health_reason: None,
                                }
                            } else {
                                Reply::Ok
                            };
                            let mut payload = serde_json::to_vec(&reply).unwrap();
                            payload.push(b'\n');
                            let _ = reader.into_inner().write_all(&payload).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                    _ => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let reff = "[fd5a::1]:5000/acme/app:sha256abc";

        // Fire both deploys for the SAME uuid concurrently.
        let o1 = o.clone();
        let o2 = o.clone();
        let h1 = tokio::spawn(async move {
            o1.deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            o2.deploy_app(
                APP_UUID,
                reff,
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await
        });
        let (r1, r2) = tokio::join!(h1, h2);
        assert!(r1.unwrap().is_ok(), "first concurrent deploy must succeed");
        assert!(r2.unwrap().is_ok(), "second concurrent deploy must succeed");

        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "control socket saw overlapping deploys for one uuid → not serialized"
        );
    }

    /// Helper: a fake control server that replies per a script of per-connection
    /// behaviors. Each accepted connection consumes the next `Behavior`:
    /// `Ok` → `{"reply":"ok"}`; `Err` → a runner `Reply::Err`; `DropNoReply` →
    /// accept then close WITHOUT replying (the client's read hits EOF → a
    /// deserialize/transport error → the orchestrator's `Err(e)` "deploy control
    /// message failed" path). Behaviors past the end of the script default to
    /// `Ok` (so the health probe + tail connections always answer).
    #[derive(Clone, Copy)]
    enum Behavior {
        Ok,
        Err,
        DropNoReply,
    }

    async fn scripted_control_server(sock_path: std::path::PathBuf, script: Vec<Behavior>) {
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
            time::Duration,
        };
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path).unwrap();
            let mut idx = 0usize;
            // Accept generously: 1 health probe + up to SWAP_MAX_ATTEMPTS deploys
            // + a trailing reload, plus slack.
            for _ in 0..12 {
                match tokio::time::timeout(Duration::from_secs(3), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let behavior = script.get(idx).copied().unwrap_or(Behavior::Ok);
                        idx += 1;
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        let _ = reader.read_line(&mut line).await;
                        let command: crate::control_proto::Cmd =
                            serde_json::from_str(line.trim()).unwrap();
                        match behavior {
                            Behavior::DropNoReply => {
                                // Close without writing a reply → client sees EOF.
                                drop(reader);
                            }
                            Behavior::Err => {
                                let _ = reader
                                    .into_inner()
                                    .write_all(b"{\"reply\":\"err\",\"message\":\"boom\"}\n")
                                    .await;
                            }
                            Behavior::Ok => {
                                let reply = if matches!(command, crate::control_proto::Cmd::Health)
                                {
                                    Reply::Health {
                                        state: "running".to_owned(),
                                        app_ula: APP_ULA.to_owned(),
                                        app_uuid: APP_UUID.to_owned(),
                                        pid: 12345,
                                        image_ref: None,
                                        app_health: "serving".to_owned(),
                                        app_health_reason: None,
                                    }
                                } else {
                                    Reply::Ok
                                };
                                let mut payload = serde_json::to_vec(&reply).unwrap();
                                payload.push(b'\n');
                                let _ = reader.into_inner().write_all(&payload).await;
                            }
                        }
                    }
                    _ => break,
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    /// Helper: build + persist a minimal live-runner record pointing at `sock`,
    /// with a pre-existing `image_ref` so tests can assert it is (not) advanced.
    fn save_live_record(dir: &std::path::Path, sock: &std::path::Path, old_ref: Option<&str>) {
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock.to_path_buf(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: old_ref.map(str::to_owned),
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir).unwrap();
    }

    /// A TRANSIENT control-transport failure on the first swap attempt (the
    /// runner's socket momentarily wedged — the "deploy control message failed"
    /// symptom) must be RETRIED, and a subsequent successful swap makes the whole
    /// deploy succeed + persist the new image_ref. Pre-fix (single send, no retry)
    /// this returned Err on the first failure.
    #[tokio::test]
    async fn deploy_app_retries_transient_transport_failure_then_succeeds() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        // conn 1 = health probe (Ok); conn 2 = first deploy (transport drop);
        // conn 3 = retried deploy (Ok).
        scripted_control_server(
            sock_path.clone(),
            vec![Behavior::Ok, Behavior::DropNoReply, Behavior::Ok],
        )
        .await;
        save_live_record(dir.path(), &sock_path, Some("[fd5a::1]:5000/acme/app:OLD"));

        let o = orch(dir.path().to_path_buf());
        let new_ref = "[fd5a::1]:5000/acme/app:NEW";
        let result = o
            .deploy_app(
                APP_UUID,
                new_ref,
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await;

        assert!(
            result.is_ok(),
            "deploy must succeed after retrying the transient transport failure: {result:?}"
        );
        let updated = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();
        assert_eq!(
            updated.image_ref.as_deref(),
            Some(new_ref),
            "image_ref must advance to the new ref after the retried swap succeeds"
        );
    }

    /// A runner `Reply::Err` (build failed / new VM never healthy) is NOT
    /// retried and must leave `image_ref` at its OLD value — the app stays on its
    /// last-known-good build and is never stranded on the half-deployed image.
    #[tokio::test]
    async fn deploy_app_keeps_old_image_ref_when_swap_fails() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        // conn 1 = health (Ok); conn 2 = deploy → Reply::Err.
        scripted_control_server(sock_path.clone(), vec![Behavior::Ok, Behavior::Err]).await;
        let old_ref = "[fd5a::1]:5000/acme/app:OLD";
        save_live_record(dir.path(), &sock_path, Some(old_ref));

        let o = orch(dir.path().to_path_buf());
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:NEW",
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await;

        assert!(result.is_err(), "a runner Reply::Err must fail the deploy");
        let updated = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();
        assert_eq!(
            updated.image_ref.as_deref(),
            Some(old_ref),
            "image_ref must stay at the OLD build after a failed swap (last-known-good)"
        );
    }

    /// A persistently-failing control transport gives up after
    /// [`SWAP_MAX_ATTEMPTS`] and leaves `image_ref` untouched (last-known-good).
    #[tokio::test]
    async fn deploy_app_gives_up_after_max_transport_attempts() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        // conn 1 = health (Ok); then every deploy attempt drops without replying.
        let mut script = vec![Behavior::Ok];
        for _ in 0..SWAP_MAX_ATTEMPTS {
            script.push(Behavior::DropNoReply);
        }
        scripted_control_server(sock_path.clone(), script).await;
        let old_ref = "[fd5a::1]:5000/acme/app:OLD";
        save_live_record(dir.path(), &sock_path, Some(old_ref));

        let o = orch(dir.path().to_path_buf());
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:NEW",
                None,
                None,
                DeployNetwork::default(),
                None,
                None,
            )
            .await;

        assert!(
            result.is_err(),
            "deploy must fail after exhausting transport retries"
        );
        let updated = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();
        assert_eq!(
            updated.image_ref.as_deref(),
            Some(old_ref),
            "image_ref must stay at the OLD build after exhausted retries (last-known-good)"
        );
    }

    /// Helper: spin up a fake control-server on `sock_path` that answers
    /// `Reply::Ok` to the first `n` messages, then returns a tempdir for
    /// cleanup.
    async fn fake_control_server(sock_path: std::path::PathBuf, n: usize) {
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
            time::Duration,
        };
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path).unwrap();
            for _ in 0..n {
                match tokio::time::timeout(Duration::from_secs(2), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        let _ = reader.read_line(&mut line).await;
                        let command: crate::control_proto::Cmd =
                            serde_json::from_str(line.trim()).unwrap();
                        let reply = if matches!(command, crate::control_proto::Cmd::Health) {
                            Reply::Health {
                                state: "running".to_owned(),
                                app_ula: APP_ULA.to_owned(),
                                app_uuid: APP_UUID.to_owned(),
                                pid: 12345,
                                image_ref: None,
                                app_health: "serving".to_owned(),
                                app_health_reason: None,
                            }
                        } else {
                            Reply::Ok
                        };
                        let mut payload = serde_json::to_vec(&reply).unwrap();
                        payload.push(b'\n');
                        let _ = reader.into_inner().write_all(&payload).await;
                    }
                    _ => break,
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    /// Phase-2: a live deploy that carries a tenant `network` AND a
    /// `runner_join_token` persists BOTH on the record so a future RESPAWN
    /// rejoins the validating coordinator with the same long-lived token
    /// instead of 401ing.  Also verifies `spawn_spec_for` picks up the token.
    #[tokio::test]
    async fn deploy_app_live_path_persists_network_and_token() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        fake_control_server(sock_path.clone(), 5).await;

        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let net = DeployNetwork {
            network: Some("n_jpegxik72nng".to_owned()),
            runner_join_token: Some("scoped-runner-jwt".to_owned()),
        };
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256abc",
                None,
                None,
                net,
                None,
                None,
            )
            .await;
        assert!(result.is_ok(), "deploy_app must succeed: {result:?}");

        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must exist after deploy");
        assert_eq!(
            updated.network.as_deref(),
            Some("n_jpegxik72nng"),
            "network must be persisted on a live network-scoped deploy"
        );
        // The on-disk record must carry the long-lived join token so a respawn
        // re-joins the validating coordinator instead of getting 401.
        assert_eq!(
            updated.runner_join_token.as_deref(),
            Some("scoped-runner-jwt"),
            "runner_join_token must be persisted on a live deploy so a respawn re-joins"
        );
        // spawn_spec_for must carry the token so the respawn uses it.
        let spec = o.shared().spawn_spec_for(&updated);
        assert_eq!(
            spec.runner_join_token.as_deref(),
            Some("scoped-runner-jwt"),
            "spawn_spec_for must carry the persisted token to the respawned runner"
        );
    }

    /// Phase-2: a live "nudge" re-deploy with NO token in the body must NOT
    /// overwrite a previously-persisted token — the existing token must be
    /// kept so the runner's respawn path always has a valid join token.
    #[tokio::test]
    async fn deploy_app_live_path_keeps_existing_token_when_body_has_none() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));
        fake_control_server(sock_path.clone(), 5).await;

        // Seed a record that already has a persisted token from a prior deploy.
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: Some("n_jpegxik72nng".to_owned()),
            runner_join_token: Some("previously-persisted-jwt".to_owned()),
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        // Nudge re-deploy: body carries no token (runner is already live and
        // the operator just wants to push a new image ref).
        let net = DeployNetwork {
            network: None,
            runner_join_token: None,
        };
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256new",
                None,
                None,
                net,
                None,
                None,
            )
            .await;
        assert!(result.is_ok(), "nudge deploy_app must succeed: {result:?}");

        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must exist after nudge deploy");
        // The previously-persisted token must NOT have been wiped.
        assert_eq!(
            updated.runner_join_token.as_deref(),
            Some("previously-persisted-jwt"),
            "a nudge deploy with no token must keep the existing persisted token"
        );
    }

    /// A live zero-downtime swap (the runner is already warm) that carries a NEW
    /// managed `tabbify.toml` must write it back to the durable record, so a later
    /// crash-respawn re-derives the LATEST runtime — not the stale toml the record
    /// had before this redeploy. We seed a record with an OLD toml, redeploy with a
    /// NEW one over a live (fake) runner, then assert (a) the saved record carries
    /// the NEW toml and (b) a `spawn_spec_for` respawn yields the NEW toml.
    #[tokio::test]
    async fn deploy_app_live_swap_refreshes_record_manifest_toml() {
        use std::time::Duration;

        use tempfile::TempDir;
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join(format!("{APP_UUID}.sock"));

        // Fake control server: Reply::Ok to health probe + deploy.
        let sock_path_srv = sock_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&sock_path_srv).unwrap();
            for _ in 0..5 {
                match tokio::time::timeout(Duration::from_secs(2), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        let _ = reader.read_line(&mut line).await;
                        let command: crate::control_proto::Cmd =
                            serde_json::from_str(line.trim()).unwrap();
                        let reply = if matches!(command, crate::control_proto::Cmd::Health) {
                            Reply::Health {
                                state: "running".to_owned(),
                                app_ula: APP_ULA.to_owned(),
                                app_uuid: APP_UUID.to_owned(),
                                pid: 12345,
                                image_ref: None,
                                app_health: "serving".to_owned(),
                                app_health_reason: None,
                            }
                        } else {
                            Reply::Ok
                        };
                        let mut payload = serde_json::to_vec(&reply).unwrap();
                        payload.push(b'\n');
                        let _ = reader.into_inner().write_all(&payload).await;
                    }
                    _ => break,
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Seed a record carrying the OLD managed toml (memory_mb = 256).
        const OLD_TOML: &str =
            "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\n[runtime]\nmemory_mb = 256\n";
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: Some(OLD_TOML.to_owned()),
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        // Redeploy over the LIVE runner with a NEW toml (memory_mb = 2048).
        const NEW_TOML: &str =
            "[app]\nname = \"x\"\n[build]\nkind = \"docker\"\n[runtime]\nmemory_mb = 2048\n";
        let o = orch(dir.path().to_path_buf());
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256abc",
                None,
                Some(NEW_TOML),
                DeployNetwork::default(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok(), "live-swap deploy must succeed: {result:?}");

        // (a) The saved record now carries the NEW toml, not the old one.
        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must exist after deploy");
        assert_eq!(
            updated.manifest_toml.as_deref(),
            Some(NEW_TOML),
            "live-swap must refresh the record's manifest_toml to the latest deploy's"
        );

        // (b) A respawn-from-record reconstructs a spec carrying the NEW toml, so
        // a crash-respawn re-derives the latest runtime (not the stale 256 MiB).
        let cfg = SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        };
        let spec = cfg.spawn_spec_for(&updated);
        assert_eq!(
            spec.manifest_toml.as_deref(),
            Some(NEW_TOML),
            "a respawn must reuse the refreshed (new) toml, not the stale one"
        );
    }

    /// A managed toml declaring a persistent data disk, as a connect-repo app
    /// with `stateful` carries it. This string is the ONLY place the app's
    /// persistence intent lives — there is no S3 manifest for such an app.
    const STATEFUL_TOML: &str = "[app]\nname = \"forge\"\n[build]\nkind = \"docker\"\n\
                                 [runtime]\nstateful = true\ndata_mount = \"/var/lib/forge\"\n";

    /// Seed a live-runner record for `uuid` carrying `toml`, with its control
    /// socket under `dir`.
    fn seed_record_with_manifest(
        dir: &std::path::Path,
        toml: Option<&str>,
    ) -> (RunnerHandle, PathBuf) {
        let sock_path = dir.join(format!("{APP_UUID}.sock"));
        let rec = RunnerHandle {
            uuid: APP_UUID.to_owned(),
            pid: 12345,
            control_sock: sock_path.clone(),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: toml.map(str::to_owned),
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(dir).unwrap();
        (rec, sock_path)
    }

    /// A re-deploy that carries NO managed toml — the MCP deploy path hardcodes
    /// `manifest_toml: None`, and a plain "redeploy this image" nudge sends none
    /// either — must KEEP the app's persisted manifest.
    ///
    /// Clearing it left the record with no `[runtime].stateful`, so the next
    /// spawn attached no data disk: the app came up `running` and healthy while
    /// writing to a rootfs the respawn after it discards. The only visible
    /// symptom was a MISSING `PUT /drives/data` line in the boot log.
    #[tokio::test]
    async fn deploy_app_without_a_manifest_keeps_the_persisted_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_rec, sock_path) = seed_record_with_manifest(dir.path(), Some(STATEFUL_TOML));
        fake_control_server(sock_path, 5).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let o = orch(dir.path().to_path_buf());
        let result = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256new",
                None,
                None, // the nudge: no managed toml in the body
                DeployNetwork::default(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok(), "nudge re-deploy must succeed: {result:?}");

        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must exist after deploy");
        assert_eq!(
            updated.manifest_toml.as_deref(),
            Some(STATEFUL_TOML),
            "a deploy that carried no manifest must not erase the app's persistence intent"
        );
        assert_eq!(
            updated.image_ref.as_deref(),
            Some("[fd5a::1]:5000/acme/app:sha256new"),
            "the deploy must still apply the new image"
        );
    }

    /// An EXPLICIT manifest that drops `[runtime].stateful` is refused, not
    /// applied. Losing a data disk destroys the app's state on its next
    /// respawn, so it must be a loud failure rather than a quiet success.
    #[tokio::test]
    async fn deploy_app_refuses_a_manifest_that_drops_the_data_disk() {
        let dir = tempfile::TempDir::new().unwrap();
        seed_record_with_manifest(dir.path(), Some(STATEFUL_TOML));

        const EPHEMERAL_TOML: &str =
            "[app]\nname = \"forge\"\n[build]\nkind = \"docker\"\n[runtime]\nmemory_mb = 2048\n";
        let o = orch(dir.path().to_path_buf());
        let error = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256new",
                None,
                Some(EPHEMERAL_TOML),
                DeployNetwork::default(),
                None,
                None,
            )
            .await
            .expect_err("dropping a live data disk must be refused");

        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("/var/lib/forge") && rendered.contains("ephemeral rootfs"),
            "the refusal must name the disk at risk and what would happen: {rendered}"
        );
        let after = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must survive a refused deploy");
        assert_eq!(
            after.manifest_toml.as_deref(),
            Some(STATEFUL_TOML),
            "a refused deploy must leave the record untouched"
        );
    }

    /// The refusal is a TYPED error so the API can attribute it to the caller's
    /// config (409) instead of reporting a platform fault (500) — the same
    /// misattribution the clone-failure work removed from the build path.
    #[tokio::test]
    async fn a_refused_stateful_deploy_is_downcastable() {
        let dir = tempfile::TempDir::new().unwrap();
        seed_record_with_manifest(dir.path(), Some(STATEFUL_TOML));

        let o = orch(dir.path().to_path_buf());
        let error = o
            .deploy_app(
                APP_UUID,
                "[fd5a::1]:5000/acme/app:sha256new",
                None,
                Some("[runtime]\nmemory_mb = 512\n"),
                DeployNetwork::default(),
                None,
                None,
            )
            .await
            .expect_err("must be refused");

        assert!(
            error
                .downcast_ref::<crate::orchestrator::manifest_retention::StatefulRegression>()
                .is_some(),
            "the API layer maps this variant to a 4xx; an untyped error would 500"
        );
    }

    // ── boot_failure_diagnostic (P1-2 reason propagation) ─────────────────────

    /// The readiness diagnostic printed by the runner's cold boot is recovered
    /// from a log tail so the deploy verdict carries WHY, not just THAT, it died.
    #[test]
    fn boot_failure_diagnostic_extracts_readiness_line() {
        let tail = "2026-07-09T00:00:00Z  INFO tabbify_runner: starting tabbify-runner\n\
             2026-07-09T00:00:30Z ERROR fc boot: guest app never became ready (readiness timeout)\n\
             Error: guest app at http://172.31.0.2:80 never became ready: the readiness probe got \
             no response. Most likely your app is not listening on that exact port";
        let diag = boot_failure_diagnostic(tail).expect("marker present");
        assert!(
            diag.contains("never became ready"),
            "must surface the readiness diagnostic; got: {diag}"
        );
        // The LAST matching line wins (newest boot attempt): the anyhow chain,
        // not the earlier tracing line.
        assert!(
            diag.starts_with("Error: guest app at"),
            "the most-recent matching line should be returned; got: {diag}"
        );
    }

    /// The `not LISTENING` / `CAP_NET_ADMIN` markers are matched case-insensitively.
    #[test]
    fn boot_failure_diagnostic_matches_case_insensitively() {
        let net = "boot: tap setup EPERM — the guest has no network (needs CAP_NET_ADMIN)";
        assert_eq!(boot_failure_diagnostic(net).as_deref(), Some(net));
    }

    /// A tail with no readiness marker yields `None` so the terse gate reason is
    /// kept unchanged (backward compatible).
    #[test]
    fn boot_failure_diagnostic_none_without_marker() {
        let tail = "just some unrelated log line\nanother benign line\ndone";
        assert!(boot_failure_diagnostic(tail).is_none());
    }

    // ── read_last_lines (bounded tail) ────────────────────────────────────────

    /// A log far larger than the 8KB chunk: the tail must still surface a
    /// marker that sits within the last few lines — and never slurp the file.
    #[tokio::test]
    async fn read_last_lines_bounded_finds_marker_in_large_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.log");
        // ~40KB of filler lines, then the marker among the last 5 lines.
        let mut content = String::new();
        for i in 0..1000 {
            content.push_str(&format!("filler line {i} padded to make it long\n"));
        }
        content.push_str("almost there 1\n");
        content.push_str("FATAL: tap device failed\n");
        content.push_str("almost there 2\n");
        std::fs::write(&path, &content).unwrap();
        assert!(content.len() > 8192, "fixture must exceed the chunk size");

        let tail = read_last_lines(&path, 5).await.unwrap();
        assert!(
            tail.contains("FATAL: tap device failed"),
            "bounded tail must surface the marker; got: {tail}"
        );
    }

    /// Multi-byte UTF-8 around the seek boundary must not error: the chunk
    /// decodes lossily, so a split codepoint degrades to U+FFFD instead of
    /// failing the whole tail.
    #[tokio::test]
    async fn read_last_lines_survives_utf8_split_at_seek_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("utf8.log");
        // Fill the file with multi-byte chars ("я" is 2 bytes) so the seek to
        // len-8192 is overwhelmingly likely to land mid-codepoint, then end
        // with an ASCII marker line.
        let mut content = "я".repeat(10_000); // 20_000 bytes
        content.push_str("\nFATAL: marker\n");
        std::fs::write(&path, &content).unwrap();

        let tail = read_last_lines(&path, 3)
            .await
            .expect("a mid-codepoint seek must not error");
        assert!(
            tail.contains("FATAL: marker"),
            "tail must contain the marker; got: {tail}"
        );
    }

    /// A short file (smaller than the chunk) tails from byte 0 unharmed.
    #[tokio::test]
    async fn read_last_lines_short_file_returns_all_requested_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.log");
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap();

        let tail = read_last_lines(&path, 2).await.unwrap();
        assert_eq!(tail, "two\nthree");
    }

    // ── purge_app reaps the FC child (FIX C) ──────────────────────────────────

    /// A helper that constructs an `Orchestrator` whose `data_dir` is the
    /// supplied tempdir (so `kill_fc_child_for_uuid` finds pidfiles there).
    fn orch_with_data_dir(
        runner_dir: std::path::PathBuf,
        data_dir: std::path::PathBuf,
    ) -> Orchestrator {
        Orchestrator::new(
            SharedRunnerConfig {
                runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
                s3_base_url: "http://s3.invalid".to_owned(),
                data_dir,
                parent: None,
                no_mesh: true,
                relay_url: None,
                relay_only: false,
            },
            runner_dir,
        )
    }

    /// `purge_app` must reap the FC child recorded in the per-uuid pidfile.
    ///
    /// Mirrors the existing monitor test `reconcile_kills_fc_child_via_pidfile_when_runner_dead`:
    /// spin up a real `sleep` child (the stand-in FC orphan), write its pid to
    /// the pidfile, call `purge_app`, then assert the pidfile is removed AND
    /// the child is dead.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn purge_app_reaps_fc_child_via_pidfile() {
        use std::{fs, os::unix::fs::PermissionsExt as _};

        use crate::firecracker::pidfile;

        let dir = tempfile::TempDir::new().unwrap();
        let uuid = "0191e7c2-beef-7222-8333-444455556666";

        // Keep a shell process alive with the same identity-bearing argv as a
        // real Firecracker child. The script path supplies the exact binary
        // basename while its ignored arguments supply the expected API socket.
        let fake_fc = dir.path().join("firecracker");
        fs::write(&fake_fc, b"#!/bin/sh\nwhile :; do sleep 1; done\n").unwrap();
        fs::set_permissions(&fake_fc, fs::Permissions::from_mode(0o700)).unwrap();
        let api_sock = crate::firecracker::fc_api_sock_for_key(uuid);
        let mut fc_orphan = std::process::Command::new(&fake_fc)
            .args(["--api-sock", &api_sock])
            .spawn()
            .expect("spawn fake firecracker child");
        let fc_pid = fc_orphan.id();

        // Write the pidfile as the runner would after spawning firecracker.
        pidfile::write(dir.path(), uuid, fc_pid);
        assert!(
            pidfile::path(dir.path(), uuid).exists(),
            "pidfile must be written before purge"
        );

        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());

        // purge_app will fail the control-socket calls (no real runner), but
        // the FC reap + record forget must still happen regardless.
        let _ = o.purge_app(uuid).await;

        // The pidfile must have been consumed.
        assert!(
            !pidfile::path(dir.path(), uuid).exists(),
            "purge_app must remove the FC pidfile"
        );

        // The FC orphan must be dead. Use waitpid(WNOHANG) (like the monitor's
        // runner_is_alive) — it reaps the zombie AND detects exit, so it returns
        // a non-zero value as soon as the child has exited (even if it is still a
        // zombie). SIGKILL is near-instant; poll for up to 100 ms.
        let mut fc_alive_after = true;
        for _ in 0..10 {
            let r = unsafe {
                libc::waitpid(fc_pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG)
            };
            if r != 0 {
                // Non-zero: either the pid was reaped (positive) or ECHILD (negative)
                // — either way the child is gone.
                fc_alive_after = false;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        #[cfg(target_os = "linux")]
        assert!(
            fc_alive_after,
            "purge must not kill an unrelated reused pidfile PID"
        );
        #[cfg(not(target_os = "linux"))]
        assert!(
            !fc_alive_after,
            "non-Linux fake FC process should be cleaned up for local tests"
        );
        if fc_alive_after {
            let _ = fc_orphan.kill();
        }
        let _ = fc_orphan.wait();
    }

    #[tokio::test]
    async fn purge_surfaces_incomplete_cache_cleanup() {
        let dir = tempfile::TempDir::new().unwrap();
        let apps = dir.path().join("apps");
        std::fs::create_dir(&apps).unwrap();
        // `purge_cache` expects this path to be a directory. A file makes its
        // remove_dir_all fail deterministically rather than silently succeeding.
        std::fs::write(apps.join(APP_UUID), b"not a cache directory").unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());

        let error = o.purge_app(APP_UUID).await.unwrap_err();

        assert!(format!("{error:#}").contains("purge cache"));
        assert!(
            apps.join(APP_UUID).exists(),
            "incomplete cache must remain visible"
        );
    }

    #[test]
    fn record_deletion_failure_is_returned() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let record_path = crate::orchestrator::handle::record_path(dir.path(), APP_UUID);
        std::fs::create_dir(&record_path).unwrap();
        std::fs::write(record_path.join("still-present"), b"x").unwrap();

        assert!(
            o.forget_record(APP_UUID).is_err(),
            "non-NotFound deletion failures must propagate"
        );
    }

    // ── purge removes ALL per-uuid auxiliary artifacts ────────────────────────

    /// Every auxiliary artifact `remove_app_artifacts` covers, created on disk
    /// for `uuid` (paths derived through the same canonical helpers).
    fn seed_artifacts(data_dir: &std::path::Path, uuid: &str) -> Vec<PathBuf> {
        let runner_log = crate::orchestrator::spawn::runner_log_path(data_dir, uuid);
        let paths = vec![
            crate::runner::serve::runner_keypair_path(data_dir, uuid),
            crate::orchestrator::spawn::rotated_log_path(&runner_log),
            runner_log,
            data_dir.join("runners").join(format!("{uuid}.sock")),
            crate::firecracker::pidfile::console_log_path(data_dir, uuid),
            crate::orchestrator::build::build_log_path(data_dir, uuid),
            crate::orchestrator::build::build_progress_log_path(data_dir, uuid),
            data_dir
                .join(LEGACY_STALE_RUNNERS_DIR)
                .join(format!("{uuid}.json")),
            data_dir
                .join(LEGACY_STALE_RUNNERS_DIR)
                .join(format!("{uuid}.sock")),
        ];
        for path in &paths {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"x").unwrap();
        }
        paths
    }

    #[test]
    fn remove_app_artifacts_removes_all_per_uuid_files_and_only_them() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let runner_dir = data_dir.join("runners");
        std::fs::create_dir_all(&runner_dir).unwrap();
        let o = orch_with_data_dir(runner_dir.clone(), data_dir.clone());

        let other_uuid = "0191e7c2-aaaa-7222-8333-444455556666";
        let purged = seed_artifacts(&data_dir, APP_UUID);
        let kept = seed_artifacts(&data_dir, other_uuid);
        // The runner RECORD must stay: forget_record owns it (hard error path).
        let record = crate::orchestrator::handle::record_path(&runner_dir, APP_UUID);
        std::fs::write(&record, b"{}").unwrap();

        o.remove_app_artifacts(APP_UUID);

        for path in &purged {
            assert!(
                !path.exists(),
                "artifact must be removed: {}",
                path.display()
            );
        }
        for path in &kept {
            assert!(
                path.exists(),
                "different uuid's artifact must survive: {}",
                path.display()
            );
        }
        assert!(
            record.exists(),
            "remove_app_artifacts must NOT touch the runner record"
        );
    }

    #[test]
    fn remove_app_artifacts_is_noop_when_nothing_exists() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let o = orch_with_data_dir(data_dir.join("runners"), data_dir.clone());

        // Nothing on disk at all (not even the dirs): must not panic or create.
        o.remove_app_artifacts(APP_UUID);

        assert!(!data_dir.join("runners").exists());
        assert!(!data_dir.join(LEGACY_STALE_RUNNERS_DIR).exists());
    }

    #[tokio::test]
    async fn purge_app_removes_per_uuid_artifacts() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let runner_dir = data_dir.join("runners");
        std::fs::create_dir_all(&runner_dir).unwrap();
        let o = orch_with_data_dir(runner_dir, data_dir.clone());
        let artifacts = seed_artifacts(&data_dir, APP_UUID);
        // The definitive shutdown probes the control socket; a seeded REGULAR
        // file reads as "possibly alive" (non-ECONNREFUSED connect error) and
        // correctly bails the purge. A real dead socket cannot be bound here
        // (macOS 104-byte sun_path limit under tempdirs), so run the purge with
        // the socket absent — its removal is covered by the unit test above.
        let sock = o.control_sock_for(APP_UUID);
        std::fs::remove_file(&sock).unwrap();
        let artifacts: Vec<_> = artifacts.into_iter().filter(|p| *p != sock).collect();

        o.purge_app(APP_UUID).await.unwrap();

        for path in &artifacts {
            assert!(
                !path.exists(),
                "purge must remove artifact: {}",
                path.display()
            );
        }
    }

    // ── legacy runners.stale graveyard sweep ──────────────────────────────────

    #[test]
    fn graveyard_sweep_removes_recordless_entries_and_keeps_live_ones() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let runner_dir = data_dir.join("runners");
        std::fs::create_dir_all(&runner_dir).unwrap();
        let o = orch_with_data_dir(runner_dir.clone(), data_dir.clone());

        let dead_uuid = "0191e7c2-dead-7222-8333-444455556666";
        let live_uuid = APP_UUID;
        let stale_dir = data_dir.join(LEGACY_STALE_RUNNERS_DIR);
        std::fs::create_dir_all(&stale_dir).unwrap();
        for uuid in [dead_uuid, live_uuid] {
            std::fs::write(stale_dir.join(format!("{uuid}.json")), b"{}").unwrap();
            std::fs::write(stale_dir.join(format!("{uuid}.sock")), b"").unwrap();
        }
        // Only `live_uuid` has a runner record → its graveyard files are kept.
        std::fs::write(
            crate::orchestrator::handle::record_path(&runner_dir, live_uuid),
            b"{}",
        )
        .unwrap();

        o.sweep_stale_runner_graveyard();

        assert!(!stale_dir.join(format!("{dead_uuid}.json")).exists());
        assert!(!stale_dir.join(format!("{dead_uuid}.sock")).exists());
        assert!(stale_dir.join(format!("{live_uuid}.json")).exists());
        assert!(stale_dir.join(format!("{live_uuid}.sock")).exists());
    }

    #[test]
    fn graveyard_sweep_is_noop_without_the_dir() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let o = orch_with_data_dir(data_dir.join("runners"), data_dir.clone());

        o.sweep_stale_runner_graveyard();

        assert!(!data_dir.join(LEGACY_STALE_RUNNERS_DIR).exists());
    }

    // ── stop_app preserves the deploy artifact (FIX (c)) ──────────────────────

    /// Build a runner record carrying a deployed `image_ref` + managed
    /// `manifest_toml` + `extra_env`, persisted under `runner_dir`.
    fn seed_deployed_record(runner_dir: &std::path::Path, uuid: &str) -> RunnerHandle {
        let rec = RunnerHandle {
            uuid: uuid.to_owned(),
            pid: 4242,
            control_sock: runner_dir.join(format!("{uuid}.sock")),
            app_ula: APP_ULA.to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: Some("[fd5a::1]:5000/acme/app@sha256:deadbeef".to_owned()),
            requested_runtime: None,
            network: Some("n_jpegxik72nng".to_owned()),
            runner_join_token: Some("jwt.runner.token".to_owned()),
            manifest_toml: Some("[app]\nname = \"x\"\n[runtime]\nmemory_mb = 2048\n".to_owned()),
            extra_env: Some(
                [("PORT".to_owned(), "9000".to_owned())]
                    .into_iter()
                    .collect(),
            ),
            egress_allow: Some(vec!["api.telegram.org".to_owned()]),
            crash_looped: false,
            wg_listen_port: None,
            stopped: false,
        };
        rec.save(runner_dir).unwrap();
        rec
    }

    /// `stop_app` must PRESERVE the record (image_ref/manifest_toml/extra_env/
    /// runner_join_token) and only MARK it stopped — it must NOT delete the
    /// record (the old behavior bricked a later respawn with no image_ref).
    #[tokio::test]
    async fn stop_app_preserves_record_and_marks_stopped() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let seeded = seed_deployed_record(dir.path(), APP_UUID);

        // No live runner (no socket server) → Shutdown round-trip fails, but the
        // record must still be marked stopped + preserved.
        o.stop_app(APP_UUID).await.unwrap();

        let after = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("stop_app must NOT delete the record — it must be preserved");
        assert!(after.stopped, "stop_app must mark the record stopped");
        assert_eq!(after.pid, 0, "stop_app must clear the live pid");
        // The deploy artifact must survive so a later respawn/reset/deploy works.
        assert_eq!(
            after.image_ref, seeded.image_ref,
            "image_ref must be preserved"
        );
        assert_eq!(
            after.manifest_toml, seeded.manifest_toml,
            "manifest_toml must be preserved"
        );
        assert_eq!(
            after.extra_env, seeded.extra_env,
            "extra_env must be preserved"
        );
        assert_eq!(
            after.egress_allow, seeded.egress_allow,
            "egress_allow must be preserved so a respawn re-applies the same egress posture"
        );
        assert_eq!(
            after.runner_join_token, seeded.runner_join_token,
            "runner_join_token must be preserved"
        );
    }

    #[tokio::test]
    async fn stop_propagates_durable_record_load_failure() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        std::fs::write(
            crate::orchestrator::handle::record_path(dir.path(), APP_UUID),
            b"not-json",
        )
        .unwrap();

        let error = o.stop_app(APP_UUID).await.unwrap_err();

        assert!(format!("{error:#}").contains("load runner record"));
    }

    #[tokio::test]
    async fn start_reaps_stopped_survivor_before_attempting_spawn() {
        use tokio::{
            io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
            net::UnixListener,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let mut survivor = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn surviving old runner stand-in");
        let survivor_pid = survivor.id();
        let socket = dir.path().join(format!("{APP_UUID}.sock"));
        let socket_server = socket.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_server).unwrap();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    continue;
                }
                let command: crate::control_proto::Cmd = serde_json::from_str(line.trim()).unwrap();
                let shutdown = matches!(command, crate::control_proto::Cmd::Shutdown);
                let reply = if shutdown {
                    kill_pid(survivor_pid);
                    Reply::Ok
                } else {
                    Reply::Health {
                        state: "running".to_owned(),
                        app_ula: APP_ULA.to_owned(),
                        app_uuid: APP_UUID.to_owned(),
                        pid: survivor_pid,
                        image_ref: None,
                        app_health: "serving".to_owned(),
                        app_health_reason: None,
                    }
                };
                let mut payload = serde_json::to_vec(&reply).unwrap();
                payload.push(b'\n');
                reader.into_inner().write_all(&payload).await.unwrap();
                if shutdown {
                    break;
                }
            }
            drop(listener);
            let _ = std::fs::remove_file(socket_server);
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut record = seed_deployed_record(dir.path(), APP_UUID);
        record.pid = survivor_pid;
        record.stopped = true;
        record.save(dir.path()).unwrap();

        // The configured runner binary does not exist, so Start fails immediately
        // after teardown. The invariant under test is that the old runner is gone
        // before that spawn is attempted.
        assert!(o.start_app(APP_UUID, None).await.is_err());
        for _ in 0..20 {
            if !runner_is_alive(survivor_pid) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !runner_is_alive(survivor_pid),
            "Start must not leave the stopped survivor beside a replacement attempt"
        );
        let _ = survivor.wait();
        let after = o.load_runner_record(APP_UUID).unwrap().unwrap();
        assert_eq!(after.pid, 0, "exited survivor PID must be durably cleared");
        assert!(after.stopped, "failed Start must retain stopped intent");
    }

    /// A `stopped` record must NOT be respawned by the monitor's reconcile (it is
    /// treated like a parked runner until the app is brought back up).
    #[tokio::test]
    async fn monitor_does_not_respawn_stopped_record() {
        use crate::orchestrator::monitor::RecordOutcome;

        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut rec = seed_deployed_record(dir.path(), APP_UUID);
        rec.stopped = true;
        rec.pid = 0;
        rec.save(dir.path()).unwrap();

        let outcome = o.reconcile_record(&rec).await;
        assert_eq!(
            outcome,
            RecordOutcome::CrashLooped,
            "a stopped record must be skipped (not respawned) by reconcile"
        );
    }

    /// `reset_app` must clear the `stopped` flag so a stopped app becomes
    /// respawn-eligible again (the preserved image_ref drives the cold start).
    #[tokio::test]
    async fn reset_app_clears_stopped_flag() {
        let dir = tempfile::TempDir::new().unwrap();
        let o = orch_with_data_dir(dir.path().to_path_buf(), dir.path().to_path_buf());
        let mut rec = seed_deployed_record(dir.path(), APP_UUID);
        rec.stopped = true;
        rec.pid = 0;
        rec.save(dir.path()).unwrap();

        // reset_app's reconcile will try (and fail) to spawn a real runner — that
        // is tolerated; we assert the persisted record has stopped cleared.
        let _ = o.reset_app(APP_UUID).await;

        let after = RunnerHandle::load(dir.path(), APP_UUID).unwrap().unwrap();
        assert!(
            !after.stopped,
            "reset_app must clear the stopped flag so the app can come back"
        );
        // The deploy artifact is still intact for the cold start.
        assert!(after.image_ref.is_some(), "image_ref must survive reset");
    }
}
