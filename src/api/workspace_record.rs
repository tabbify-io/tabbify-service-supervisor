//! Durable workspace sidecars — the on-disk delta that lets a per-user workspace
//! survive a supervisor restart / OTA.
//!
//! A workspace differs from a dev-session in three ways the sidecar must carry:
//! - identity is the STABLE `workspace_uuid = Uuidv5(WORKSPACE_NS, user_id)`
//!   (frozen contract), not an ephemeral session id — so a restart re-keys to the
//!   SAME workspace;
//! - it has N repos, each with its OWN git-proxy cap (multi-cap), so the sidecar
//!   persists a `Vec<WorkspaceCap>` and re-registers ALL of them on readopt;
//! - the `user_id` is persisted so a future per-caller-identity path can map an
//!   MCP caller back to its workspace.
//!
//! As with the dev-session sidecar (#63), the git-proxy TOKENS are NOT persisted
//! (short-lived, node-minted): readopt re-registers each cap with an
//! already-expired placeholder token, and the node's standing token sweep mints
//! fresh ones — ZERO node change.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::api::{GitSessionEntry, GitSessions};
use crate::orchestrator::handle::RunnerHandle;

/// `extra_env` key that marks a [`RunnerHandle`] as a WORKSPACE FC (distinct
/// from the dev-session `TABBIFY_GIT_REMOTE` marker). Set by `create_workspace`
/// (`workspaces.rs`); read here to confirm a record's VM is still alive.
pub const WORKSPACE_MARKER_ENV: &str = "TABBIFY_WORKSPACE_UUID";

/// Sub-directory of the runner dir holding workspace sidecars
/// (`<workspace_uuid>.json`). A SUBDIR so `RunnerHandle::list`'s `*.json` scan of
/// the runner dir skips it (a directory has no `json` extension), and DISTINCT
/// from the dev-sessions subdir so the two registries never collide.
const WORKSPACES_SUBDIR: &str = "workspaces";

/// One repo's git-proxy access inside a workspace: the cap token + the real
/// upstream clone URL to re-register on readopt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceCap {
    /// Git-proxy capability (64 hex).
    pub cap: String,
    /// Real upstream clone URL (`https://github.com/owner/repo.git`).
    pub repo_url: String,
}

/// The durable per-workspace delta. Keyed on disk by `workspace_uuid`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    /// STABLE per-user workspace id (`Uuidv5(WORKSPACE_NS, user_id)`). Also the
    /// FC `app_uuid` AND the on-disk record key (1:1 with the workspace VM).
    pub workspace_uuid: String,
    /// Canonical internal account id this workspace belongs to.
    pub user_id: String,
    /// One cap per repo (multi-cap). Re-registered en masse on readopt.
    pub caps: Vec<WorkspaceCap>,
    /// Branch checked out per repo (parallel to `caps` by index), for ops.
    pub branches: Vec<String>,
    /// Creation time, wall-clock unix seconds.
    pub created_at_unix: u64,
    /// Last-activity time, wall-clock unix seconds.
    pub last_activity_unix: u64,
}

/// The directory holding workspace sidecars, given the runner dir.
#[must_use]
pub fn workspaces_dir(runner_dir: &Path) -> PathBuf {
    runner_dir.join(WORKSPACES_SUBDIR)
}

fn record_path(runner_dir: &Path, workspace_uuid: &str) -> PathBuf {
    workspaces_dir(runner_dir).join(format!("{workspace_uuid}.json"))
}

impl WorkspaceRecord {
    /// Write as `<runner_dir>/workspaces/<workspace_uuid>.json`.
    ///
    /// # Errors
    /// [`io::Error`] if the dir cannot be created or the write fails.
    pub fn save(&self, runner_dir: &Path) -> io::Result<()> {
        let dir = workspaces_dir(runner_dir);
        fs::create_dir_all(&dir)?;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(dir.join(format!("{}.json", self.workspace_uuid)), json)
    }

    /// Load the sidecar for `workspace_uuid`. `Ok(None)` if absent.
    ///
    /// # Errors
    /// [`io::Error`] if the file exists but cannot be read/parsed.
    pub fn load(runner_dir: &Path, workspace_uuid: &str) -> io::Result<Option<Self>> {
        match fs::read_to_string(record_path(runner_dir, workspace_uuid)) {
            Ok(json) => Ok(Some(
                serde_json::from_str(&json)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Remove the sidecar for `workspace_uuid` (idempotent; missing is Ok).
    ///
    /// # Errors
    /// [`io::Error`] only for a real removal failure (not NotFound).
    pub fn remove(runner_dir: &Path, workspace_uuid: &str) -> io::Result<()> {
        match fs::remove_file(record_path(runner_dir, workspace_uuid)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// List all workspace sidecars; empty when the dir is absent; bad files are
    /// skipped with a warning (one bad record never blocks re-adoption).
    ///
    /// # Errors
    /// [`io::Error`] only if the dir exists but cannot be read.
    pub fn list(runner_dir: &Path) -> io::Result<Vec<Self>> {
        let dir = workspaces_dir(runner_dir);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            match fs::read_to_string(&path) {
                Ok(json) => match serde_json::from_str::<Self>(&json) {
                    Ok(r) => records.push(r),
                    Err(e) => tracing::warn!(path = %path.display(), err = %e, "skipping unparseable workspace record"),
                },
                Err(e) => tracing::warn!(path = %path.display(), err = %e, "skipping unreadable workspace record"),
            }
        }
        Ok(records)
    }
}

/// Outcome of a startup workspace re-adopt pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReadoptWorkspaceSummary {
    /// Workspaces re-registered (all their caps re-inserted).
    pub adopted: usize,
    /// Orphan sidecars GC'd (no live workspace VM).
    pub gc: usize,
}

/// Re-adopt persisted workspaces on supervisor startup.
///
/// For every on-disk [`WorkspaceRecord`] whose workspace VM is still alive (a
/// [`RunnerHandle`] carrying [`WORKSPACE_MARKER_ENV`], keyed by the
/// `workspace_uuid` == the runner uuid), re-register EVERY cap into
/// `git_sessions` with an already-expired placeholder token (the node's token
/// sweep restores them). A record whose VM is gone is GC'd.
///
/// Infallible: all I/O errors are logged and treated as "nothing to adopt".
pub fn readopt_workspaces(
    runner_dir: &Path,
    git_sessions: &GitSessions,
) -> ReadoptWorkspaceSummary {
    let mut summary = ReadoptWorkspaceSummary::default();

    let workspace_runner_uuids: HashSet<String> = RunnerHandle::list(runner_dir)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "workspace re-adopt: cannot list runner records");
            Vec::new()
        })
        .into_iter()
        .filter(|h| {
            h.extra_env
                .as_ref()
                .is_some_and(|e| e.contains_key(WORKSPACE_MARKER_ENV))
        })
        .map(|h| h.uuid)
        .collect();

    let records = WorkspaceRecord::list(runner_dir).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "workspace re-adopt: cannot list workspace records");
        Vec::new()
    });

    for rec in records {
        if !workspace_runner_uuids.contains(&rec.workspace_uuid) {
            if let Err(e) = WorkspaceRecord::remove(runner_dir, &rec.workspace_uuid) {
                tracing::warn!(workspace_uuid = %rec.workspace_uuid, error = %e, "workspace re-adopt: failed to GC orphan record");
            }
            summary.gc += 1;
            continue;
        }
        // Re-register ALL caps (multi-repo) with expired placeholder tokens.
        for c in &rec.caps {
            git_sessions.register(
                c.cap.clone(),
                GitSessionEntry {
                    upstream_url: c.repo_url.clone(),
                    token: String::new(),
                    expires_at: Instant::now(),
                },
            );
        }
        summary.adopted += 1;
    }

    tracing::info!(
        adopted = summary.adopted,
        gc = summary.gc,
        "workspace re-adopt pass complete"
    );
    summary
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;
    use crate::api::dev_session_record::now_unix;

    fn workspace_runner(uuid: &str, with_marker: bool) -> RunnerHandle {
        let extra_env = with_marker.then(|| {
            let mut m = HashMap::new();
            m.insert(WORKSPACE_MARKER_ENV.to_owned(), uuid.to_owned());
            m
        });
        RunnerHandle {
            uuid: uuid.to_owned(),
            pid: 4242,
            control_sock: PathBuf::from("/run/tabbify/x.sock"),
            app_ula: "fd5a:1f02::1".to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env,
            crash_looped: false,
            stopped: false,
        }
    }

    fn record(ws_uuid: &str, caps: &[(&str, &str)]) -> WorkspaceRecord {
        WorkspaceRecord {
            workspace_uuid: ws_uuid.to_owned(),
            user_id: "user-1".to_owned(),
            caps: caps
                .iter()
                .map(|(cap, url)| WorkspaceCap {
                    cap: (*cap).to_owned(),
                    repo_url: (*url).to_owned(),
                })
                .collect(),
            branches: caps.iter().map(|_| "main".to_owned()).collect(),
            created_at_unix: now_unix().saturating_sub(100),
            last_activity_unix: now_unix().saturating_sub(10),
        }
    }

    #[test]
    fn record_roundtrips_n_caps_through_disk() {
        let dir = TempDir::new().unwrap();
        record(
            "ws-1",
            &[("capA", "https://github.com/a/a.git"), ("capB", "https://github.com/a/b.git")],
        )
        .save(dir.path())
        .unwrap();
        let listed = WorkspaceRecord::list(dir.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].caps.len(), 2, "both repo caps must persist");
        assert_eq!(listed[0].caps[1].cap, "capB");
        assert_eq!(listed[0].user_id, "user-1");
    }

    #[test]
    fn list_empty_when_dir_absent() {
        let dir = TempDir::new().unwrap();
        assert!(WorkspaceRecord::list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = TempDir::new().unwrap();
        record("ws-1", &[("c", "u")]).save(dir.path()).unwrap();
        WorkspaceRecord::remove(dir.path(), "ws-1").unwrap();
        WorkspaceRecord::remove(dir.path(), "ws-1").unwrap();
        assert!(WorkspaceRecord::list(dir.path()).unwrap().is_empty());
    }

    /// Re-adopt: a live workspace VM + its sidecar → ALL caps re-registered so
    /// the node's token sweep can restore each (the multi-cap keystone).
    #[test]
    fn readopt_reregisters_all_caps_for_live_workspace() {
        let dir = TempDir::new().unwrap();
        workspace_runner("ws-1", true).save(dir.path()).unwrap();
        record(
            "ws-1",
            &[("capA", "https://github.com/a/a.git"), ("capB", "https://github.com/a/b.git")],
        )
        .save(dir.path())
        .unwrap();

        let git = GitSessions::default();
        let summary = readopt_workspaces(dir.path(), &git);

        assert_eq!(summary.adopted, 1);
        assert_eq!(summary.gc, 0);
        let caps = git.registered_caps();
        assert!(caps.contains(&"capA".to_owned()));
        assert!(caps.contains(&"capB".to_owned()), "every repo cap must be re-registered");
        assert!(
            git.refresh_token(
                "capA",
                "fresh".to_owned(),
                Instant::now() + std::time::Duration::from_secs(3600)
            ),
            "node token sweep must be able to restore each re-adopted cap"
        );
    }

    /// A sidecar with no matching live runner is GC'd, not re-adopted.
    #[test]
    fn readopt_gcs_orphan_workspace_record() {
        let dir = TempDir::new().unwrap();
        record("ws-gone", &[("c", "u")]).save(dir.path()).unwrap();

        let git = GitSessions::default();
        let summary = readopt_workspaces(dir.path(), &git);

        assert_eq!(summary.adopted, 0);
        assert_eq!(summary.gc, 1);
        assert!(WorkspaceRecord::list(dir.path()).unwrap().is_empty());
        assert!(git.registered_caps().is_empty());
    }
}
