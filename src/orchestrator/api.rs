//! Control-API-facing lifecycle operations on the runner fleet (Task 2.6).
//!
//! These are the orchestrator methods the axum control API
//! ([`crate::api`]) drives instead of the old in-process `AppRegistry`:
//!
//! - [`start_app`](Orchestrator::start_app) — if no live runner exists for the
//!   uuid, spawn one DETACHED and wait until its control socket is healthy;
//!   idempotent if one is already running.
//! - [`stop_app`](Orchestrator::stop_app) — `Shutdown` the runner (it exits,
//!   KEEPING its on-disk artifacts + docker image for a fast restart) and forget
//!   its record.
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
/// boots the microVM, and waits for in-guest readiness — comfortably exceeding
/// the previous 30s gate, which killed slow-but-healthy firecracker runners
/// before they came up. 180s covers that worst case while still failing a
/// genuinely-doomed start in bounded time.
const START_HEALTHY_TIMEOUT: Duration = Duration::from_secs(180);

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
            return Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
                restart_status: restart_status_str(RestartStatus::Running),
                restart_count: 0,
                next_retry_at: 0,
                requested_runtime: runtime_override.map(str::to_owned),
            });
        }

        // No live runner. Spawn one DETACHED (it persists its own record). The
        // runtime is fixed to Firecracker, so the override is not threaded in.
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

        Ok(AppSummary {
            uuid: uuid.to_owned(),
            app_ula: app_ula.to_string(),
            state: AppState::Running,
            restart_status: restart_status_str(RestartStatus::Running),
            restart_count: 0,
            next_retry_at: 0,
            requested_runtime: runtime_override.map(str::to_owned),
        })
    }

    /// Stop `uuid`: shut its runner down (the runner exits, KEEPING its on-disk
    /// artifacts + docker image for a fast restart) and forget its record so the
    /// monitor does not respawn it. Idempotent — a missing runner is a no-op.
    ///
    /// # Errors
    /// Returns an error only if `uuid` is malformed. A `Shutdown` round-trip
    /// failure (e.g. the runner already gone) is logged + tolerated; the record
    /// is removed regardless.
    pub async fn stop_app(&self, uuid: &str) -> Result<()> {
        let _ = self.app_ula_for(uuid)?;

        // Ask the runner to exit. Best-effort: if it is already gone the record
        // removal below still cleans up.
        match self.client_for(uuid).shutdown().await {
            Ok(Reply::Ok) => {}
            Ok(other) => tracing::warn!(uuid, ?other, "unexpected reply to Shutdown"),
            Err(e) => tracing::warn!(uuid, error = %e, "Shutdown failed (runner may be gone)"),
        }

        // Forget the record FIRST so a concurrent monitor tick cannot respawn
        // the runner we just asked to exit.
        self.forget_record(uuid);
        Ok(())
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
            // Read path: no override travels on a snapshot.
            requested_runtime: None,
        }))
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
    pub async fn deploy_app(
        &self,
        uuid: &str,
        reff: &str,
        runtime_override: Option<&str>,
        manifest_toml: Option<&str>,
        net: DeployNetwork,
    ) -> Result<AppSummary> {
        let app_ula = self.app_ula_for(uuid)?;

        // Mark this uuid as deploy-in-flight for the WHOLE deploy. The RAII guard
        // clears it on every exit path (early `?`, error, success). While it is
        // held the monitor's reconcile defers killing + respawning this runner —
        // the runner can be briefly busy/unresponsive on its control socket while
        // it builds the new VM + performs the swap, and a reap mid-deploy would
        // abort the swap and orphan the half-built VM.
        let _deploy_guard = self.begin_deploy(uuid);

        if self.client_for(uuid).health().await.is_ok() {
            // Live runner: send the Deploy message.
            match self.client_for(uuid).deploy(reff).await {
                Ok(Reply::Ok) => {}
                Ok(Reply::Err { message }) => {
                    return Err(anyhow::anyhow!("runner deploy failed: {message}"));
                }
                Ok(other) => {
                    return Err(anyhow::anyhow!("unexpected reply to Deploy: {other:?}"));
                }
                Err(e) => return Err(e.context("deploy control message failed")),
            }

            // Persist the new ref so a future respawn comes up on this version.
            let mut record = RunnerHandle::load(self.runner_dir(), uuid)
                .with_context(|| format!("load runner record for {uuid} after deploy"))?
                .ok_or_else(|| anyhow::anyhow!("no runner record found for {uuid}"))?;
            record.image_ref = Some(reff.to_owned());
            // Persist this deploy's managed `tabbify.toml` so the durable record
            // always reflects the LATEST deploy's runtime config. Without this a
            // warm zero-downtime swap (the live runner keeps serving) would leave
            // the OLD toml on disk, and a later crash-respawn would re-derive
            // STALE `[runtime]`/`[routes]`. `None` clears it (a deploy with no
            // managed config), mirroring the cold-spawn branch.
            record.manifest_toml = manifest_toml.map(str::to_owned);
            // Phase-2: if this deploy supplied a tenant network, persist it so a
            // future RESPAWN rejoins scoped (the live runner itself is not
            // re-keyed — it keeps its current mesh identity). A `None` network
            // leaves the record's existing scoping untouched (back-compat).
            if net.network.is_some() {
                record.network = net.network.clone();
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
                requested_runtime: runtime_override.map(str::to_owned),
            })
        } else {
            // No live runner: spawn one pinned to reff. The runtime is fixed to
            // Firecracker, so the override is not threaded into the spec.
            let mut spec = self.spawn_spec_for_uuid(uuid);
            spec.image_ref = Some(reff.to_owned());
            // Apply the managed `tabbify.toml` (when supplied) so a BUILD-pipeline
            // app's `[runtime]`/`[routes]` drive its synthesized manifest. `None`
            // keeps the hardcoded FC defaults.
            spec.manifest_toml = manifest_toml.map(str::to_owned);
            // Phase-2: scope the cold spawn to the tenant network. `--network`
            // (persisted for respawn) + the scoped node-join token (via env,
            // NOT persisted). Both `None` keeps the unscoped spawn.
            spec.network = net.network.clone();
            spec.runner_join_token = net.runner_join_token.clone();
            let (handle, _child) = spawn_runner(&spec, self.runner_dir())
                .await
                .with_context(|| format!("spawn runner for {uuid}"))?;

            let client = ControlClient::new(&handle.control_sock);
            wait_healthy(&client, START_HEALTHY_TIMEOUT)
                .await
                .with_context(|| format!("runner for {uuid} never became healthy"))?;

            Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
                restart_status: restart_status_str(RestartStatus::Running),
                restart_count: 0,
                next_retry_at: 0,
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

/// Bounded tail: reads at most the last `CHUNK` bytes of `path`, then keeps the
/// final `lines` lines. Runner logs are never rotated — a crash-looping runner
/// grows them unbounded, so this must NEVER slurp the whole file (an
/// error-path heap spike on a multi-hundred-MB log). The seek can land
/// mid-UTF-8-codepoint, so the chunk decodes lossily rather than erroring.
///
/// NOTE: the tail surfaces whatever the runner printed — keep this API
/// mesh-internal; do not propagate to external callers unredacted.
async fn read_last_lines(path: &std::path::Path, lines: usize) -> std::io::Result<String> {
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
                DeployNetwork::default()
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
        };
        rec.save(dir.path()).unwrap();

        let o = orch(dir.path().to_path_buf());
        let reff = "[fd5a::1]:5000/acme/app:sha256abc";
        let result = o
            .deploy_app(APP_UUID, reff, None, None, DeployNetwork::default())
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

    /// Phase-2: a live deploy that carries a tenant `network` persists it on the
    /// record (so a future RESPAWN rejoins scoped). The scoped token is NOT
    /// persisted — it is short-lived and minted per deploy by the node.
    #[tokio::test]
    async fn deploy_app_live_path_persists_network_not_token() {
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
        // The on-disk record must never carry the short-lived join token.
        let raw = std::fs::read_to_string(crate::orchestrator::handle::record_path(
            dir.path(),
            APP_UUID,
        ))
        .unwrap();
        assert!(
            !raw.contains("scoped-runner-jwt"),
            "the join token must NEVER be persisted to disk; record: {raw}"
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
}
