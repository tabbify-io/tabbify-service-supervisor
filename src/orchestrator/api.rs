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

use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::app_ula::derive_app_ula;
use crate::control_proto::Reply;
use crate::orchestrator::client::ControlClient;
use crate::orchestrator::handle::RunnerHandle;
use crate::orchestrator::spawn::{SpawnSpec, spawn_runner};
use crate::orchestrator::{MONITOR_INTERVAL, Orchestrator};

/// How long [`Orchestrator::start_app`] waits for a freshly-spawned runner to
/// answer its control socket before giving up. A cold start fetches the
/// artifact from S3 and (for docker) builds an image, so this is generous.
const START_HEALTHY_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// # Errors
    /// - `uuid` is not a valid UUID;
    /// - the runner process fails to spawn (binary missing / record write);
    /// - the runner never becomes healthy within [`START_HEALTHY_TIMEOUT`].
    pub async fn start_app(&self, uuid: &str) -> Result<AppSummary> {
        let app_ula = self.app_ula_for(uuid)?;

        // Idempotent: a live runner answering its socket → return it untouched.
        if self.client_for(uuid).health().await.is_ok() {
            return Ok(AppSummary {
                uuid: uuid.to_owned(),
                app_ula: app_ula.to_string(),
                state: AppState::Running,
            });
        }

        // No live runner. Spawn one DETACHED (it persists its own record).
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
        let mut out = Vec::with_capacity(records.len());
        for rec in records {
            let state = if ControlClient::new(&rec.control_sock).health().await.is_ok() {
                AppState::Running
            } else {
                AppState::Stopped
            };
            out.push(AppSummary {
                uuid: rec.uuid,
                app_ula: rec.app_ula,
                state,
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
        let state = if ControlClient::new(&rec.control_sock).health().await.is_ok() {
            AppState::Running
        } else {
            AppState::Stopped
        };
        Ok(Some(AppSummary {
            uuid: rec.uuid,
            app_ula: rec.app_ula,
            state,
        }))
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
}
