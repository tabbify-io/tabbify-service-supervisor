//! Durable dev-session sidecars — the minimal on-disk delta that lets a dev
//! session survive a supervisor restart / OTA.
//!
//! The dev-VM runner itself already survives a restart (`setsid`-detached +
//! systemd `KillMode=process`) and so does its on-disk [`RunnerHandle`]. What is
//! LOST on restart is the dev-session LAYER: [`DevSessionRegistry`] and
//! [`GitSessions`] are both in-memory only, so a running dev-VM becomes orphaned
//! — invisible to `GET /v1/dev-sessions`, and `git push` 403s ("unknown or
//! expired git session"). [`RunnerHandle`] carries `app_uuid` + `branch` + the
//! `cap` (embedded in `extra_env[TABBIFY_GIT_REMOTE]`), but NOT the `session_id`
//! or the real upstream `repo_url`. Those two, plus the wall-clock timestamps,
//! are exactly what this sidecar persists.
//!
//! The git-proxy TOKEN is deliberately NOT persisted: it is a ~1 h GitHub App
//! installation token minted by the NODE (the supervisor structurally cannot
//! mint it), so writing it to disk would be a plaintext short-lived secret that
//! is stale within the hour anyway. After [`readopt_dev_sessions`] re-registers
//! the cap with an already-expired placeholder token, the node's standing token
//! sweep lists the re-adopted session and POSTs a fresh token — restoring push
//! within one sweep interval, with ZERO node-side change.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::dev_sessions::DevSession;
use super::ssh_jump::start_dev_ssh_jump;
use crate::api::{DevSessionRegistry, GitSessionEntry, GitSessions};
use crate::orchestrator::handle::RunnerHandle;

/// `extra_env` key that marks a [`RunnerHandle`] as a dev-FC. Set by
/// `create_dev_session` (`dev_sessions.rs`) and read by the FC launch
/// (`runner/build/firecracker.rs`); kept in sync as a plain literal there.
const DEV_MARKER_ENV: &str = "TABBIFY_GIT_REMOTE";

/// Sub-directory of the runner dir holding the sidecars (`<app_uuid>.json`). A
/// SUBDIR (not a sibling file) so [`RunnerHandle::list`]'s `*.json` scan of the
/// runner dir skips it (a directory has no `json` extension) — no collision.
const DEV_SESSIONS_SUBDIR: &str = "dev-sessions";

/// Current wall-clock time as unix seconds (the serializable form of the
/// in-memory `Instant` timestamps).
#[must_use]
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The durable per-session delta: everything needed to re-insert a dev session
/// into the in-memory registries on startup that is NOT already in
/// [`RunnerHandle`]. Keyed on disk by `app_uuid` (1:1 with the dev-FC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevSessionRecord {
    /// Session identifier (UUID v7, string form) — addressable by node/ops.
    pub session_id: String,
    /// The dev-FC app uuid (= the matching [`RunnerHandle::uuid`] + record key).
    pub app_uuid: String,
    /// Git-proxy capability (64 hex). Stored explicitly rather than parsed out of
    /// `extra_env[TABBIFY_GIT_REMOTE]` to avoid coupling to that URL format.
    pub cap: String,
    /// Real upstream clone URL (`https://github.com/owner/repo.git`) — the
    /// `GitSessionEntry.upstream_url` to re-register. NOT the proxied URL.
    pub repo_url: String,
    /// Branch checked out at `/workspace`.
    pub branch: String,
    /// Creation time, wall-clock unix seconds (`Instant` is not serializable).
    pub created_at_unix: u64,
    /// Last-activity time, wall-clock unix seconds; bumped on token refresh.
    pub last_activity_unix: u64,
    /// The SSH-jump listener port (`[my_ula]:<port>`) bound for this session, so
    /// a restart can RE-BIND the same port and keep the node's cached jump
    /// address valid. `#[serde(default)]` ⇒ a record written before this field
    /// existed decodes as `None` (re-adoption then binds a fresh port; the node
    /// re-lists to learn it).
    #[serde(default)]
    pub ssh_jump_port: Option<u16>,
}

/// The directory holding dev-session sidecars, given the runner dir.
#[must_use]
pub fn dev_sessions_dir(runner_dir: &Path) -> PathBuf {
    runner_dir.join(DEV_SESSIONS_SUBDIR)
}

fn record_path(runner_dir: &Path, app_uuid: &str) -> PathBuf {
    dev_sessions_dir(runner_dir).join(format!("{app_uuid}.json"))
}

impl DevSessionRecord {
    /// Write this record as `<runner_dir>/dev-sessions/<app_uuid>.json`, creating
    /// the directory if needed.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the directory cannot be created or the write
    /// fails.
    pub fn save(&self, runner_dir: &Path) -> io::Result<()> {
        let dir = dev_sessions_dir(runner_dir);
        let path = dir.join(format!("{}.json", self.app_uuid));
        super::atomic_record::save_json(&dir, &path, self)
    }

    /// Load the sidecar for `app_uuid`. `Ok(None)` if there is no record.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the file exists but cannot be read/parsed.
    pub fn load(runner_dir: &Path, app_uuid: &str) -> io::Result<Option<Self>> {
        match fs::read_to_string(record_path(runner_dir, app_uuid)) {
            Ok(json) => Ok(Some(
                serde_json::from_str(&json)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Remove the sidecar for `app_uuid`. A missing file is NOT an error
    /// (idempotent teardown).
    ///
    /// # Errors
    /// Returns an [`io::Error`] only for a real removal failure (not NotFound).
    pub fn remove(runner_dir: &Path, app_uuid: &str) -> io::Result<()> {
        match fs::remove_file(record_path(runner_dir, app_uuid)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// List all dev-session sidecars. Empty (not an error) when the dir is
    /// absent; unparseable/unreadable files are skipped with a warning, mirroring
    /// [`RunnerHandle::list`] — a single bad record never blocks re-adoption.
    ///
    /// # Errors
    /// Returns an [`io::Error`] only if the directory exists but cannot be read.
    pub fn list(runner_dir: &Path) -> io::Result<Vec<Self>> {
        let dir = dev_sessions_dir(runner_dir);
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
                    Err(e) => {
                        tracing::warn!(path = %path.display(), err = %e, "skipping unparseable dev-session record");
                    }
                },
                Err(e) => {
                    tracing::warn!(path = %path.display(), err = %e, "skipping unreadable dev-session record");
                }
            }
        }
        Ok(records)
    }
}

/// Outcome of a startup dev-session re-adopt pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReadoptDevSummary {
    /// Sessions re-inserted into the in-memory registries.
    pub adopted: usize,
    /// Orphan sidecars garbage-collected (no live dev-FC runner).
    pub gc: usize,
}

/// Convert persisted unix-seconds back to an approximate `Instant`. Panic-free:
/// `Instant - Duration` is `checked_sub`bed and falls back to `now` on underflow
/// (the value only drives the cosmetic `idle_secs`/`created_age` — reaping is
/// effectively disabled, so an approximation is fine).
pub(crate) fn unix_to_instant(unix_secs: u64) -> Instant {
    let now = Instant::now();
    let elapsed = now_unix().saturating_sub(unix_secs);
    now.checked_sub(Duration::from_secs(elapsed)).unwrap_or(now)
}

/// Re-adopt persisted dev-sessions on supervisor startup.
///
/// For every on-disk [`DevSessionRecord`] whose dev-FC runner is still alive (a
/// [`RunnerHandle`] carrying the `TABBIFY_GIT_REMOTE` marker), re-insert the
/// [`DevSession`] into `dev_sessions` and re-register its `cap` into
/// `git_sessions` with an ALREADY-EXPIRED placeholder token. The session
/// reappears in `GET /v1/dev-sessions` immediately; the node's standing token
/// sweep then mints + POSTs a fresh git token (≤ one sweep interval), restoring
/// `git push`. The cap MUST be re-registered (even with a dud token) because the
/// node's `refresh_token` is a no-op on an unknown cap.
///
/// It ALSO re-establishes the per-session SSH TCP jump (the in-memory listener
/// died with the old process): using the live runner's `image_ref` + the
/// `tap_subnet` to re-derive the dev-FC `guest_ip`, and the persisted
/// `ssh_jump_port` to re-bind the SAME port when free (so a node's cached jump
/// address keeps working) — else a fresh port, which the node re-learns on its
/// next list. Skipped when `my_ula` is `None` (no mesh) or the guest_ip cannot
/// be derived; the session still adopts (the node then uses the direct path).
///
/// Records whose runner is gone (purged) are skipped and their orphan sidecar is
/// GC'd, so no phantom session ever appears for a dead `app_uuid`.
///
/// Infallible: all I/O errors are logged and treated as "nothing to adopt" so a
/// transient FS hiccup never blocks startup.
pub async fn readopt_dev_sessions(
    runner_dir: &Path,
    dev_sessions: &DevSessionRegistry,
    git_sessions: &GitSessions,
    tap_subnet: &str,
    my_ula: Option<IpAddr>,
) -> ReadoptDevSummary {
    let mut summary = ReadoptDevSummary::default();

    // Map live dev-FC runner uuid → its `image_ref` (needed to re-derive the tap
    // `guest_ip` the SSH jump dials). A runner with the dev marker but no
    // image_ref still adopts; only its jump is skipped.
    let dev_runners: HashMap<String, Option<String>> = RunnerHandle::list(runner_dir)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "dev re-adopt: cannot list runner records");
            Vec::new()
        })
        .into_iter()
        .filter(|h| {
            h.extra_env
                .as_ref()
                .is_some_and(|e| e.contains_key(DEV_MARKER_ENV))
        })
        .map(|h| (h.uuid, h.image_ref))
        .collect();

    let records = DevSessionRecord::list(runner_dir).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "dev re-adopt: cannot list dev-session records");
        Vec::new()
    });

    for rec in records {
        let Some(image_ref) = dev_runners.get(&rec.app_uuid) else {
            // The dev-VM is gone (purged) — GC the orphan sidecar so no phantom
            // session surfaces for a dead app_uuid (and the node sweep does not
            // waste a token mint on it).
            if let Err(e) = DevSessionRecord::remove(runner_dir, &rec.app_uuid) {
                tracing::warn!(app_uuid = %rec.app_uuid, error = %e, "dev re-adopt: failed to GC orphan record");
            }
            summary.gc += 1;
            continue;
        };

        // Re-register the cap with an already-expired placeholder token so the
        // node's refresh_token (a no-op on an unknown cap) can later restore it.
        git_sessions.register(
            rec.cap.clone(),
            GitSessionEntry {
                upstream_url: rec.repo_url.clone(),
                token: String::new(),
                expires_at: Instant::now(),
            },
        );

        // Re-establish the SSH jump (best-effort; `None` ⇒ node uses the direct
        // path). Re-bind the persisted port when possible so a node's cached
        // address survives this restart.
        let ssh_jump = match (my_ula, image_ref.as_deref()) {
            (Some(ula), Some(reff)) => {
                start_dev_ssh_jump(ula, &rec.app_uuid, reff, tap_subnet, rec.ssh_jump_port).await
            }
            _ => None,
        };

        dev_sessions.insert(DevSession {
            session_id: rec.session_id.clone(),
            app_uuid: rec.app_uuid.clone(),
            cap: rec.cap.clone(),
            created_at: unix_to_instant(rec.created_at_unix),
            last_activity: unix_to_instant(rec.last_activity_unix),
            repo_url: rec.repo_url.clone(),
            branch: rec.branch.clone(),
            ssh_jump,
        });
        summary.adopted += 1;
    }

    tracing::info!(
        adopted = summary.adopted,
        gc = summary.gc,
        "dev-session re-adopt pass complete"
    );
    summary
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;

    fn dev_runner(uuid: &str, with_marker: bool) -> RunnerHandle {
        let extra_env = with_marker.then(|| {
            let mut m = HashMap::new();
            m.insert(
                DEV_MARKER_ENV.to_owned(),
                format!("http://172.31.0.1:8788/git/cap-{uuid}"),
            );
            m.insert("TABBIFY_GIT_BRANCH".to_owned(), "main".to_owned());
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
            egress_allow: None,
            crash_looped: false,
            stopped: false,
        }
    }

    fn record(session_id: &str, app_uuid: &str, cap: &str) -> DevSessionRecord {
        DevSessionRecord {
            session_id: session_id.to_owned(),
            app_uuid: app_uuid.to_owned(),
            cap: cap.to_owned(),
            repo_url: "https://github.com/acme/app.git".to_owned(),
            branch: "main".to_owned(),
            created_at_unix: now_unix().saturating_sub(100),
            last_activity_unix: now_unix().saturating_sub(10),
            ssh_jump_port: None,
        }
    }

    #[test]
    fn record_roundtrips_through_disk() {
        let dir = TempDir::new().unwrap();
        record("sess-1", "app-1", "capabc").save(dir.path()).unwrap();
        let listed = DevSessionRecord::list(dir.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, "sess-1");
        assert_eq!(listed[0].cap, "capabc");
        assert_eq!(listed[0].repo_url, "https://github.com/acme/app.git");
    }

    #[test]
    fn list_empty_when_dir_absent() {
        let dir = TempDir::new().unwrap();
        assert!(DevSessionRecord::list(dir.path()).unwrap().is_empty());
    }

    /// `ssh_jump_port` round-trips through disk, AND a record written before the
    /// field existed (JSON without the key) decodes as `None` (serde default) —
    /// so an OTA over old sidecars never fails to parse.
    #[test]
    fn ssh_jump_port_round_trips_and_defaults() {
        let dir = TempDir::new().unwrap();
        let mut rec = record("sess-j", "app-j", "cap-j");
        rec.ssh_jump_port = Some(54321);
        rec.save(dir.path()).unwrap();
        let loaded = DevSessionRecord::list(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].ssh_jump_port, Some(54321));

        // Back-compat: a sidecar JSON with no `ssh_jump_port` key decodes as None.
        let legacy = r#"{
            "session_id": "old", "app_uuid": "app-old", "cap": "c",
            "repo_url": "https://github.com/acme/app.git", "branch": "main",
            "created_at_unix": 1, "last_activity_unix": 2
        }"#;
        let decoded: DevSessionRecord = serde_json::from_str(legacy).unwrap();
        assert_eq!(decoded.ssh_jump_port, None, "pre-field record must default to None");
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = TempDir::new().unwrap();
        record("s", "app-1", "c").save(dir.path()).unwrap();
        DevSessionRecord::remove(dir.path(), "app-1").unwrap();
        DevSessionRecord::remove(dir.path(), "app-1").unwrap(); // second time: no error
        assert!(DevSessionRecord::list(dir.path()).unwrap().is_empty());
    }

    /// Re-adopt: a live dev-FC runner + its sidecar → the session is re-inserted
    /// AND the cap is re-registered so the node's token refresh will succeed
    /// (the keystone that restores `git push`).
    #[tokio::test]
    async fn readopt_reinserts_session_and_registers_cap() {
        let dir = TempDir::new().unwrap();
        dev_runner("app-1", true).save(dir.path()).unwrap();
        record("sess-1", "app-1", "cap-1").save(dir.path()).unwrap();

        let dev = DevSessionRegistry::default();
        let git = GitSessions::default();
        // `my_ula = None` ⇒ no SSH jump is bound (keeps this test off the network
        // + cross-platform); the adopt/cap-register path is what's under test.
        let summary = readopt_dev_sessions(dir.path(), &dev, &git, "172.31.0.0/16", None).await;

        assert_eq!(summary.adopted, 1);
        assert_eq!(summary.gc, 0);
        assert_eq!(
            dev.lookup("sess-1"),
            Some(("app-1".to_owned(), "cap-1".to_owned())),
            "session must reappear addressable by session_id"
        );
        assert!(git.registered_caps().contains(&"cap-1".to_owned()));
        assert!(
            git.refresh_token(
                "cap-1",
                "fresh".to_owned(),
                Instant::now() + Duration::from_secs(3600)
            ),
            "node token sweep must be able to restore the token on the re-adopted cap"
        );
    }

    /// A sidecar with no matching live runner (the VM was purged) is GC'd, not
    /// re-adopted — no phantom session for a dead app_uuid.
    #[tokio::test]
    async fn readopt_gcs_orphan_record_with_no_runner() {
        let dir = TempDir::new().unwrap();
        record("sess-x", "app-gone", "cap-x")
            .save(dir.path())
            .unwrap();

        let dev = DevSessionRegistry::default();
        let git = GitSessions::default();
        let summary = readopt_dev_sessions(dir.path(), &dev, &git, "172.31.0.0/16", None).await;

        assert_eq!(summary.adopted, 0);
        assert_eq!(summary.gc, 1);
        assert!(dev.lookup("sess-x").is_none());
        assert!(
            DevSessionRecord::list(dir.path()).unwrap().is_empty(),
            "orphan sidecar must be removed"
        );
    }

    /// A normal (non-dev) runner with no marker + no sidecar is ignored entirely.
    #[tokio::test]
    async fn readopt_ignores_non_dev_runners() {
        let dir = TempDir::new().unwrap();
        dev_runner("app-normal", false).save(dir.path()).unwrap();

        let dev = DevSessionRegistry::default();
        let git = GitSessions::default();
        let summary = readopt_dev_sessions(dir.path(), &dev, &git, "172.31.0.0/16", None).await;

        assert_eq!(summary.adopted, 0);
        assert_eq!(summary.gc, 0);
        assert!(git.registered_caps().is_empty());
    }
}
