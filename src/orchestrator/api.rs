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

use std::{net::Ipv6Addr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::{
    app_ula::derive_app_ula,
    control_proto::Reply,
    orchestrator::{
        MONITOR_INTERVAL, Orchestrator,
        client::ControlClient,
        handle::RunnerHandle,
        monitor::{kill_fc_child_for_uuid, kill_pid},
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
/// the 5s round-trip timeout — the "deploy control message failed" symptom a
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
/// proceeds to the cold spawn. Bounded so a wedged socket can't hang the deploy;
/// the cold spawn re-derives the SAME `uuid:reff` tap/socket, so the old runner
/// should be gone first, but proceeding after the bound is safe (the cold-boot
/// path reconciles any stale FC via the per-uuid pidfile).
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
    /// `TABBIFY_RUNNER_JOIN_TOKEN`. NOT persisted (short-lived).
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

    /// A control client for `uuid`'s runner socket.
    #[must_use]
    fn client_for(&self, uuid: &str) -> ControlClient {
        ControlClient::new(self.control_sock_for(uuid))
    }

    /// Live state of `uuid`: `Running` iff its runner answers a health probe.
    ///
    /// # Errors
    /// Returns an error only if `uuid` is not a valid UUID (so a caller can
    /// 400). A missing/dead runner is reported as `Stopped`, not an error.
    pub async fn app_state(&self, uuid: &str) -> Result<AppState> {
        // Validate the uuid up front so a malformed id is a clear error.
        let _ = self.app_ula_for(uuid)?;
        Ok(if self.client_for(uuid).health().await.is_ok() {
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

        // Idempotent: a live runner answering its socket → return it untouched.
        if self.client_for(uuid).health().await.is_ok() {
            tracing::info!(uuid, branch = "idempotent-live", "start_app: live runner already healthy — returning existing summary (no spawn)");
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

        // No live runner. Spawn one DETACHED (it persists its own record). The
        // runtime is fixed to Firecracker, so the override is not threaded in.
        tracing::info!(uuid, branch = "cold-spawn", "start_app: no live runner — spawning detached runner");
        let spec = self.spawn_spec_for_uuid(uuid);
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
        tracing::info!(uuid, pid = handle.pid, "start_app: runner became healthy (cold-spawn complete)");

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
    /// Returns an error only if `uuid` is malformed. A `Shutdown` round-trip
    /// failure (e.g. the runner already gone) is logged + tolerated; the record
    /// is marked stopped regardless.
    pub async fn stop_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;

        // Mark the record stopped FIRST so a concurrent monitor tick cannot
        // respawn the runner we are about to ask to exit. Preserve the deploy
        // artifact (image_ref/manifest_toml/extra_env/runner_join_token); only
        // the live identity (pid) is cleared so the monitor never adopts a stale
        // live pid for a stopped app. A missing record is a no-op (already gone).
        // F1 (audit #93): capture the deployed `image_ref` so the FC reap below
        // can `systemctl stop` the right CPU scope (`uuid:reff`) — the pidfile pid
        // is only the `systemd-run` wrapper, so the scoped FC leaks without it.
        let mut image_ref: Option<String> = None;
        let mut live_pid: u32 = 0;
        match RunnerHandle::load(self.runner_dir(), uuid) {
            Ok(Some(mut record)) => {
                image_ref = record.image_ref.clone();
                // Capture the live pid BEFORE zeroing it: if the Shutdown
                // round-trip below cannot reach the runner, this is the only
                // handle left to force-kill it.
                live_pid = record.pid;
                record.stopped = true;
                // Clear the live pid: pid 0 reads as DEAD to the monitor's
                // liveness probe, and the `stopped` gate short-circuits the
                // respawn anyway. (control_sock is left so a fast restart reuses
                // the deterministic path.)
                record.pid = 0;
                if let Err(e) = record.save(self.runner_dir()) {
                    tracing::warn!(uuid, error = %e, "failed to persist stopped runner record");
                }
            }
            Ok(None) => {
                tracing::debug!(uuid, "stop_app: no runner record (already stopped/never started)");
            }
            Err(e) => {
                tracing::warn!(uuid, error = %e, "stop_app: could not load runner record");
            }
        }

        // Ask the runner to exit. Best-effort: if it is already gone the marked
        // record still prevents a respawn.
        match self.client_for(uuid).shutdown().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to Shutdown"),
            Err(e) => {
                tracing::warn!(uuid, error = %e, "Shutdown failed (runner may be gone)");
                // A runner still mid-IMAGE-PULL has no control socket yet, so
                // the Shutdown can never reach it — and the monitor's
                // stopped-record gate skips reaping, so the surviving runner
                // keeps (re)spawning `oras` pulls for a STOPPED app
                // indefinitely (observed live on MSI, 2026-07-04: stop_app
                // returned 200 yet fresh pulls appeared seconds later).
                // Force-kill the captured pid: the record is already marked
                // stopped, so nothing respawns it, and the pull reap in
                // `kill_fc_child_for_uuid` below sweeps its `oras` children.
                // `live_pid == 0` = no live pid was recorded — nothing to kill.
                if live_pid != 0 {
                    tracing::warn!(
                        uuid,
                        pid = live_pid,
                        "stop_app: force-killing unreachable runner (no control socket — likely mid-pull)"
                    );
                    kill_pid(live_pid);
                }
            }
        }

        // Reap any FC child the runner left behind (the FC process is not a
        // child of the supervisor — it busy-spins until reaped). Mirrors
        // purge_app's reap so a stopped app does not leak a 100%-CPU FC orphan.
        // Pass the captured `image_ref` so the scoped FC's `uuid:reff` scope is
        // also stopped (F1) — killing the pidfile wrapper pid alone leaks it.
        kill_fc_child_for_uuid(&self.shared().data_dir, uuid, image_ref.as_deref());
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
    /// Best-effort throughout; bounded wait for the control socket to go dead.
    /// MUST be called under the [`begin_deploy`](Orchestrator::begin_deploy) guard
    /// so the monitor does not respawn the runner mid-reap.
    async fn reap_runner_for_cold_respawn(&self, uuid: &str, image_ref: Option<&str>) {
        // Ask the runner to exit (best-effort — it may already be mid-shutdown
        // from add_repo's prior `stop_app`, or unreachable mid-pull).
        match self.client_for(uuid).shutdown().await {
            Ok(_) => {}
            Err(e) => tracing::debug!(
                uuid,
                error = %e,
                "force-cold reap: Shutdown round-trip failed (runner may already be exiting)"
            ),
        }
        // Reap the firecracker child the runner leaves behind (it is not a
        // supervisor child — it busy-spins until reaped), stopping the scoped
        // `uuid:reff` FC too (same primitive the monitor uses before a respawn).
        kill_fc_child_for_uuid(&self.shared().data_dir, uuid, image_ref);
        // Wait (bounded) for the control socket to stop answering so the cold
        // spawn does not race a still-live socket / pidfile on the same path.
        let deadline = std::time::Instant::now() + COLD_REAP_MAX_WAIT;
        while std::time::Instant::now() < deadline {
            if self.client_for(uuid).health().await.is_err() {
                return;
            }
            tokio::time::sleep(COLD_REAP_POLL_INTERVAL).await;
        }
        tracing::warn!(
            uuid,
            "force-cold reap: runner still answered its socket after the wait; proceeding with cold spawn (cold boot reconciles any stale FC via the pidfile)"
        );
    }

    /// Purge `uuid`: tell the runner to clear its on-disk cache + remove its
    /// docker image, then shut it down, forget its record, and reclaim the cache
    /// from the orchestrator side too (belt-and-suspenders). Idempotent.
    ///
    /// # Errors
    /// Returns an error only if `uuid` is malformed. Control-socket failures are
    /// logged + tolerated; the record + cache are removed regardless.
    pub async fn purge_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;

        // F1 (audit #93): read the deployed `image_ref` BEFORE `forget_record`
        // deletes the record, so the FC reap below can `systemctl stop` the
        // scoped FC's `uuid:reff` scope (the pidfile pid is only the
        // `systemd-run` wrapper — stopping it alone leaks the scoped firecracker).
        let image_ref: Option<String> = RunnerHandle::load(self.runner_dir(), uuid)
            .ok()
            .flatten()
            .and_then(|r| r.image_ref);

        let client = self.client_for(uuid);
        // Purge clears the runner's cache + docker image (the runner stays up),
        // then Shutdown exits it. Both are best-effort.
        match client.purge().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to Purge"),
            Err(e) => tracing::warn!(uuid, error = %e, "Purge failed (runner may be gone)"),
        }
        match client.shutdown().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to Shutdown after Purge"),
            Err(e) => {
                tracing::warn!(uuid, error = %e, "Shutdown after Purge failed (runner may be gone)");
            }
        }

        // Forget the record so the monitor does not respawn it.
        self.forget_record(uuid);

        // FIX C: reap any orphaned FC child. When a dev session is stopped the
        // runner exits (Shutdown), but the firecracker process it spawned is NOT
        // a child of the supervisor — it was spawned by the runner and gets
        // reparented to PID 1 where it busy-spins at 100% CPU. The monitor
        // already does this reap on its kill-before-respawn path; purge_app
        // did not, leaving FC orphans alive until the next monitor tick found a
        // dead runner. Call the same helper here, best-effort. The pre-forget
        // `image_ref` lets it stop the scoped FC's `uuid:reff` scope too (F1).
        kill_fc_child_for_uuid(&self.shared().data_dir, uuid, image_ref.as_deref());

        // Reclaim the on-disk cache from our side too: the runner's own purge
        // already cleared it, but if the runner was unreachable we still want a
        // clean disk. `purge_cache` is idempotent (missing dir = success).
        let fetcher =
            crate::fetcher::S3Fetcher::new(&self.shared().s3_base_url, &self.shared().data_dir);
        if let Err(e) = fetcher.purge_cache(uuid).await {
            tracing::warn!(uuid, error = %e, "orchestrator-side purge_cache failed (continuing)");
        }
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
        let Some(rec) = RunnerHandle::load(self.runner_dir(), uuid)
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
    /// The TRANSPORT-failure class — a connect/write/read error or the 5s
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
        let deploy_lock = self.deploy_lock_for(uuid);
        let _serialize = deploy_lock.lock().await;

        // Mark this uuid as deploy-in-flight for the WHOLE deploy. The RAII guard
        // clears it on every exit path (early `?`, error, success). While it is
        // held the monitor's reconcile defers killing + respawning this runner —
        // the runner can be briefly busy/unresponsive on its control socket while
        // it builds the new VM + performs the swap, and a reap mid-deploy would
        // abort the swap and orphan the half-built VM.
        let _deploy_guard = self.begin_deploy(uuid);

        // Load the persisted record up front: needed both to compare the LIVE
        // env-hash against this deploy's requested env (the force-cold gate below)
        // and, on the warm path, to persist the new ref/env after the swap.
        let existing = RunnerHandle::load(self.runner_dir(), uuid)
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
                    != crate::runner::build::firecracker::effective_env_hash(
                        rec.extra_env.as_ref(),
                    )
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

        let live = self.client_for(uuid).health().await.is_ok();
        // Branch decision trace: which of the three deploy paths this deploy took
        // and WHY (live? env changed vs the running runtime? which component?).
        // The blind spot was "did this deploy warm-swap or cold-respawn?" — now it
        // is one grep.
        tracing::info!(
            uuid,
            reff,
            live,
            env_changed,
            env_change_reason = %env_change_reason,
            branch = if live && !env_changed {
                "warm-swap"
            } else if live {
                "cold-respawn (env changed on live runner)"
            } else {
                "cold-spawn (no live runner)"
            },
            "deploy: branch selected"
        );
        if live && !env_changed {
            // Live runner, env UNCHANGED: send the Deploy message, retrying
            // transient transport failures. On a persistent failure this returns
            // Err BEFORE the persist block below — so `image_ref` is left at its
            // current value and the app stays on its last-known-good build (never
            // stranded on a half-deployed / broken image; the failed swap kept the
            // OLD VM serving). The error names the dropped ref so a re-deploy is
            // obvious.
            self.swap_with_retry(uuid, reff).await?;

            // Persist the new ref so a future respawn comes up on this version.
            let mut record = existing
                .ok_or_else(|| anyhow::anyhow!("no runner record found for {uuid}"))?;
            record.image_ref = Some(reff.to_owned());
            // Persist this deploy's managed `tabbify.toml` so the durable record
            // always reflects the LATEST deploy's runtime config. Without this a
            // warm zero-downtime swap (the live runner keeps serving) would leave
            // the OLD toml on disk, and a later crash-respawn would re-derive
            // STALE `[runtime]`/`[routes]`. `None` clears it (a deploy with no
            // managed config), mirroring the cold-spawn branch.
            // Unlike `runner_join_token` and `extra_env`, a `None` here is
            // intentional clearance, not a nudge.
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
            record
                .save(self.runner_dir())
                .with_context(|| format!("save runner record for {uuid} after deploy"))?;

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
                // before this deploy took its `begin_deploy` guard). A warm swap
                // can't apply the new `/init` env, so REAP the live runner first:
                // the cold spawn below re-derives the IDENTICAL `uuid:reff` tap /
                // api-socket, which must be free, or the new VM collides with the
                // still-live one ("socket never appeared"). Held under the
                // `begin_deploy` guard so the monitor won't respawn it mid-reap.
                tracing::info!(
                    uuid,
                    reff,
                    "deploy: env/cap set changed vs the live runtime — forcing a cold respawn (a warm swap cannot re-bake /init env)"
                );
                self.reap_runner_for_cold_respawn(
                    uuid,
                    existing.as_ref().and_then(|r| r.image_ref.as_deref()),
                )
                .await;
            }
            // No live runner (never started, or just reaped for an env change):
            // spawn one pinned to reff. The runtime is fixed to Firecracker, so
            // the override is not threaded into the spec.
            let mut spec = self.spawn_spec_for_uuid(uuid);
            spec.image_ref = Some(reff.to_owned());
            // Apply the managed `tabbify.toml` (when supplied) so a BUILD-pipeline
            // app's `[runtime]`/`[routes]` drive its synthesized manifest. `None`
            // keeps the hardcoded FC defaults.
            spec.manifest_toml = manifest_toml.map(str::to_owned);
            // Phase-2: scope the cold spawn to the tenant network. Both the
            // `--network` slug and the scoped join token are PERSISTED on the
            // record (the token travels to the runner via env, never the arg
            // list) so a future respawn re-joins the validating coordinator
            // with the same long-lived token instead of 401ing. Both `None`
            // keeps the unscoped spawn. The live path above applies the same
            // Some-replaces/None-keeps policy; the only way to explicitly
            // CLEAR a persisted token is purge + fresh deploy.
            spec.network = net.network.clone();
            spec.runner_join_token = net.runner_join_token.clone();
            // Bake the deploy-time extra env into the guest via the runner.
            // Persisted on the record so a RESPAWN re-bakes the same vars.
            spec.extra_env = extra_env.cloned();
            // Thread the egress allow-list into the cold spawn so the runner
            // installs host-side egress-filter rules at boot. Persisted on the
            // record (via `RunnerHandle::from`) so a RESPAWN re-applies it. `None`
            // keeps today's unrestricted egress.
            spec.egress_allow = egress_allow.map(<[String]>::to_vec);
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
    /// [`reconcile_record`](crate::orchestrator::monitor) is called so the
    /// respawn fires NOW rather than waiting for the next monitor tick.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `uuid` is not a valid UUID;
    /// - no on-disk record exists for `uuid` (i.e. the app was never started).
    pub async fn reset_app(&self, uuid: &str) -> Result<AppSummary> {
        let _ = self.app_ula_for(uuid)?;

        // Load the record — a missing record means "never started" → 404.
        let mut record = RunnerHandle::load(self.runner_dir(), uuid)
            .with_context(|| format!("load runner record for {uuid}"))?
            .ok_or_else(|| anyhow::anyhow!("no runner record found for {uuid}"))?;

        // Clear the crash-loop / backoff state so the runner is immediately
        // eligible for a respawn (next_retry_at is zeroed by reset()).
        record.restart = restart::reset(record.restart);
        // Also clear the circuit-breaker park flag so the monitor will resume
        // respawning the runner.
        record.crash_looped = false;
        // Clear the operator-stopped flag too: `reset` is an explicit "bring it
        // back" — a stopped app must become respawn-eligible again (the preserved
        // image_ref/manifest_toml/extra_env drive the cold start).
        record.stopped = false;

        // Persist the cleared state BEFORE triggering reconcile so a concurrent
        // monitor tick also sees the clean state.
        record
            .save(self.runner_dir())
            .with_context(|| format!("save runner record for {uuid} after reset"))?;

        // Fire an immediate reconcile: if the runner is dead it will be
        // respawned right now; if it is alive it is adopted untouched.
        self.reconcile_record(&record).await;

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

    /// Remove `uuid`'s on-disk runner record (best-effort; a missing file is
    /// success). After this the monitor will not respawn the runner.
    fn forget_record(&self, uuid: &str) {
        let path = crate::orchestrator::handle::record_path(self.runner_dir(), uuid);
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(uuid, path = %path.display(), error = %e, "failed to remove runner record");
            }
        }
    }
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
pub(crate) async fn read_last_lines(path: &std::path::Path, lines: usize) -> std::io::Result<String> {
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
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::orchestrator::SharedRunnerConfig;

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
    async fn reset_app_clears_restart_state_on_disk() {
        use tempfile::TempDir;

        use crate::orchestrator::restart::{RestartState, should_respawn};

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
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        // reset_app should clear the state regardless of reconcile outcome.
        let _ = o.reset_app(APP_UUID).await;

        // Load the record back and assert the crash state is cleared.
        let updated = RunnerHandle::load(dir.path(), APP_UUID)
            .unwrap()
            .expect("record must still exist after reset");
        assert_eq!(
            updated.restart,
            RestartState::default(),
            "reset must zero the restart state on disk"
        );
        // should_respawn must be true for any `now` after the reset.
        assert!(
            should_respawn(updated.restart, 1_700_001_000),
            "after reset, runner must be immediately eligible for respawn"
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
                        // Reply Ok to everything (health probe or deploy).
                        let reply = r#"{"reply":"ok"}"#;
                        let _ = reader
                            .into_inner()
                            .write_all(format!("{reply}\n").as_bytes())
                            .await;
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
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let reff = "[fd5a::1]:5000/acme/app:sha256abc";
        let result = o
            .deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), None, None)
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

    /// Spawn a fake runner control server on `sock_path` that answers the first
    /// `max_conns` connections with `{"reply":"ok"}` (a health probe, a Deploy /
    /// Shutdown round-trip, …), then UNLINKS the socket so any further connect
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
                        let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
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
    fn live_record(sock_path: &std::path::Path, reff: &str, extra_env: Option<HashMap<String, String>>) -> RunnerHandle {
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
            .deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), Some(&env), None)
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
            .deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), Some(&new_env), None)
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
            (cap_key.to_owned(), r#"{"apartami.url":"http://g/a"}"#.to_owned()),
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
            .deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), Some(&after), None)
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
        o.snapshot_app(APP_UUID).await.expect("snapshot must succeed");
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
                    .write_all(b"{\"reply\":\"err\",\"message\":\"create did not produce files\"}\n")
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
                atomic::{AtomicUsize, Ordering},
                Arc,
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
                            let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
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
            stopped: false,
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let reff = "[fd5a::1]:5000/acme/app:sha256abc";

        // Fire both deploys for the SAME uuid concurrently.
        let o1 = o.clone();
        let o2 = o.clone();
        let h1 = tokio::spawn(async move {
            o1.deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), None, None)
                .await
        });
        let h2 = tokio::spawn(async move {
            o2.deploy_app(APP_UUID, reff, None, None, DeployNetwork::default(), None, None)
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
                                let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
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
            .deploy_app(APP_UUID, new_ref, None, None, DeployNetwork::default(), None, None)
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
                        let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
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
                        let _ = reader.into_inner().write_all(b"{\"reply\":\"ok\"}\n").await;
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
    #[tokio::test]
    async fn purge_app_reaps_fc_child_via_pidfile() {
        use crate::firecracker::pidfile;

        let dir = tempfile::TempDir::new().unwrap();
        let uuid = "0191e7c2-beef-7222-8333-444455556666";

        // Spawn a real "FC orphan" child we can safely kill.
        let mut fc_orphan = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep child");
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
        let _ = fc_orphan.wait(); // best-effort cleanup
        assert!(
            !fc_alive_after,
            "FC orphan (pid {fc_pid}) must be killed by purge_app"
        );
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
        assert_eq!(after.image_ref, seeded.image_ref, "image_ref must be preserved");
        assert_eq!(
            after.manifest_toml, seeded.manifest_toml,
            "manifest_toml must be preserved"
        );
        assert_eq!(after.extra_env, seeded.extra_env, "extra_env must be preserved");
        assert_eq!(
            after.egress_allow, seeded.egress_allow,
            "egress_allow must be preserved so a respawn re-applies the same egress posture"
        );
        assert_eq!(
            after.runner_join_token, seeded.runner_join_token,
            "runner_join_token must be preserved"
        );
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
