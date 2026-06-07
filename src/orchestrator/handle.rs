//! On-disk record for a spawned `tabbify-runner` process.
//!
//! [`RunnerHandle`] describes a live runner (its UUID, OS PID, control-socket
//! path, app ULA, and parent supervisor ULA). One JSON file per runner is
//! written to a `runner_dir` (e.g. `/var/lib/tabbify/runners/<uuid>.json`) so a
//! restarted supervisor can rediscover its living runners (Task 2.5).

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// Bookkeeping record for a single spawned runner process.
///
/// Serializes to/from JSON for on-disk persistence. No spawning or control
/// logic lives here — those are Tasks 2.2 and 2.3.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunnerHandle {
    /// UUID of the app this runner hosts (string form).
    pub uuid: String,
    /// OS process ID of the runner process.
    pub pid: u32,
    /// Path to the runner's Unix-domain control socket.
    pub control_sock: PathBuf,
    /// Deterministic per-app ULA (`fd5a:…`) the runner is bound on.
    pub app_ula: String,
    /// ULA of the parent supervisor that spawned this runner (`None` for a
    /// standalone runner launched outside of a supervisor).
    pub parent: Option<String>,
    /// Unix timestamp (seconds) at which this runner was last spawned.
    ///
    /// Used by the monitor to implement a post-spawn grace period: a runner
    /// that is alive but whose control socket is not yet healthy is treated as
    /// "still starting" for [`SPAWN_GRACE`](crate::orchestrator::monitor::SPAWN_GRACE)
    /// seconds after spawn, preventing a duplicate from being created.
    ///
    /// `#[serde(default)]` ensures old records (before this field existed)
    /// decode correctly — they get `0`, which is treated as "old enough that
    /// the grace window has long expired".
    #[serde(default)]
    pub spawned_at: u64,
    /// Per-runner restart / backoff state persisted across supervisor restarts.
    ///
    /// `#[serde(default)]` ensures old on-disk records (written before this
    /// field existed) still deserialize correctly — they get
    /// [`RestartState::default()`], which is the clean "never failed" sentinel.
    #[serde(default)]
    pub restart: crate::orchestrator::restart::RestartState,
    /// The OCI image ref of the last successful deploy (`Deploy{reff}`), if any.
    ///
    /// Persisted so a supervisor-driven respawn comes back on the SAME deployed
    /// version: the orchestrator passes it to the runner as `--image-ref` and
    /// the runner applies it to the manifest's `registry_ref` before building
    /// the initial runtime. `None` (the default) = build from the S3 manifest as
    /// usual.
    ///
    /// `#[serde(default)]` keeps old on-disk records (written before this field
    /// existed) loading — they get `None`, i.e. today's behavior. Setting this
    /// on a successful deploy is the orchestrator's job (P2.4); here it only
    /// exists, persists/loads, and flows into the spawn args.
    #[serde(default)]
    pub image_ref: Option<String>,
    /// INERT, back-compat-only. Once the D4 wire string (e.g. `"firecracker"` /
    /// `"docker"`) the deploy/start requested, persisted so a respawn could
    /// rebuild the same runtime.
    ///
    /// The platform now serves a SINGLE runtime (generic Firecracker), so the
    /// runtime is no longer selectable: this field is no longer written (new
    /// records get `None`) and is NEVER read for dispatch. It is retained ONLY so
    /// that old on-disk records that still carry a `requested_runtime` (including
    /// a now-unknown value like `"docker"`) keep deserializing without error.
    ///
    /// `#[serde(default)]` keeps even older records (written before this field
    /// existed) loading — they also get `None`. NOTE: do NOT add
    /// `deny_unknown_fields` to this struct, or old records would fail to load.
    #[serde(default)]
    pub requested_runtime: Option<String>,
    /// Tenant network slug this runner is scoped to (Phase-2 contract). Set on a
    /// network-scoped deploy and forwarded to a RESPAWN as `--network <slug>` so
    /// the runner rejoins the same tenant network. `None` (the default) = an
    /// unscoped runner (today's behavior). The scoped JOIN TOKEN is NOT persisted
    /// (short-lived, minted per deploy by the node); a respawn rejoins via the
    /// runner's sticky per-uuid keypair.
    ///
    /// `#[serde(default)]` keeps old on-disk records (written before this field
    /// existed) loading — they get `None`.
    #[serde(default)]
    pub network: Option<String>,
    /// Scoped mesh join-token (JWT) minted by the node for this runner's
    /// tenant network. PERSISTED so a supervisor-driven RESPAWN re-joins the
    /// validating coordinator with the SAME token instead of 401ing. The token
    /// is long-lived (node mints a 1-year TTL), so it outlives idle-outs/crashes
    /// for the app's whole life. `None` for unscoped runners (no tenant network)
    /// and for records written before this field existed (serde default).
    #[serde(default)]
    pub runner_join_token: Option<String>,
}

/// Returns the path at which `uuid`'s record is stored inside `dir`.
///
/// ```
/// # use std::path::Path;
/// # use tabbify_supervisor::orchestrator::handle::record_path;
/// let p = record_path(Path::new("/var/lib/tabbify/runners"), "abc-123");
/// assert_eq!(p, Path::new("/var/lib/tabbify/runners/abc-123.json"));
/// ```
#[must_use]
pub fn record_path(dir: &Path, uuid: &str) -> PathBuf {
    dir.join(format!("{uuid}.json"))
}

impl RunnerHandle {
    /// Serialize this handle to its JSON record file inside `runner_dir`.
    ///
    /// The parent directory must already exist.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if serialization or the write fails.
    pub fn save(&self, runner_dir: &Path) -> io::Result<()> {
        let path = record_path(runner_dir, &self.uuid);
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(path, json)
    }

    /// Load a runner handle from its JSON record file inside `runner_dir`.
    ///
    /// Returns `Ok(None)` if no record file exists for `uuid`.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the file exists but cannot be read or
    /// parsed.
    pub fn load(runner_dir: &Path, uuid: &str) -> io::Result<Option<Self>> {
        let path = record_path(runner_dir, uuid);
        match fs::read_to_string(&path) {
            Ok(json) => {
                let handle: Self = serde_json::from_str(&json)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(handle))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// List all runner handles recorded in `runner_dir`.
    ///
    /// Files that cannot be parsed are silently skipped with a warning log so a
    /// single corrupt record does not block re-adoption of healthy runners.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the directory cannot be read. Returns an
    /// empty `Vec` (not an error) if the directory does not exist yet.
    pub fn list(runner_dir: &Path) -> io::Result<Vec<Self>> {
        if !runner_dir.exists() {
            return Ok(Vec::new());
        }
        let mut handles = Vec::new();
        for entry in fs::read_dir(runner_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            match fs::read_to_string(&path) {
                Ok(json) => match serde_json::from_str::<Self>(&json) {
                    Ok(h) => handles.push(h),
                    Err(e) => {
                        tracing::warn!(path = %path.display(), err = %e, "skipping unparseable runner record");
                    }
                },
                Err(e) => {
                    tracing::warn!(path = %path.display(), err = %e, "skipping unreadable runner record");
                }
            }
        }
        Ok(handles)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    fn sample_handle() -> RunnerHandle {
        RunnerHandle {
            uuid: "0191e7c2-1111-7222-8333-444455556666".to_owned(),
            pid: 12345,
            control_sock: PathBuf::from("/var/run/tabbify/runners/0191e7c2.sock"),
            app_ula: "fd5a:1f02:44a5:240b:121a::1".to_owned(),
            parent: Some("fd5a:1f00:0:3::1".to_owned()),
            spawned_at: 1_700_000_000,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
        }
    }

    fn sample_handle_no_parent() -> RunnerHandle {
        RunnerHandle {
            uuid: "0191e7c2-2222-7222-8333-444455556666".to_owned(),
            pid: 99,
            control_sock: PathBuf::from("/tmp/runner.sock"),
            app_ula: "fd5a:1f02:dead:beef:cafe::1".to_owned(),
            parent: None,
            spawned_at: 0,
            restart: Default::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
        }
    }

    // ── record_path ──────────────────────────────────────────────────────────

    #[test]
    fn record_path_returns_uuid_dot_json_under_dir() {
        let p = record_path(
            Path::new("/var/lib/tabbify/runners"),
            "0191e7c2-1111-7222-8333-444455556666",
        );
        assert_eq!(
            p,
            PathBuf::from("/var/lib/tabbify/runners/0191e7c2-1111-7222-8333-444455556666.json")
        );
    }

    // ── JSON round-trip ──────────────────────────────────────────────────────

    #[test]
    fn round_trip_with_parent() {
        let h = sample_handle();
        let json = serde_json::to_string(&h).unwrap();
        let back: RunnerHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn round_trip_no_parent() {
        let h = sample_handle_no_parent();
        let json = serde_json::to_string(&h).unwrap();
        let back: RunnerHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
        assert!(back.parent.is_none());
    }

    /// Old records written before `spawned_at` was added must still deserialize:
    /// the missing field defaults to `0` (treated as "long past the grace window").
    #[test]
    fn spawned_at_defaults_to_zero_for_old_records() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert_eq!(h.spawned_at, 0, "missing spawned_at must default to 0");
    }

    // ── restart field ────────────────────────────────────────────────────────

    /// A handle with a non-default `restart` state must round-trip through
    /// `save` → `load` with all fields intact.
    #[test]
    fn restart_state_round_trips_via_save_load() {
        use crate::orchestrator::restart::RestartState;

        let dir = TempDir::new().unwrap();
        let mut h = sample_handle();
        h.restart = RestartState {
            consecutive_failures: 3,
            last_exit_at: 1_700_001_000,
            next_retry_at: 1_700_001_040,
            last_healthy_at: 1_700_000_900,
        };
        h.save(dir.path()).unwrap();

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(
            loaded.restart, h.restart,
            "restart state must survive save/load"
        );
    }

    // ── image_ref field ──────────────────────────────────────────────────────

    /// A handle with a non-default `image_ref` must round-trip through
    /// `save` → `load` with the ref intact (so a respawn comes back on the same
    /// deployed version).
    #[test]
    fn image_ref_round_trips_via_save_load() {
        let dir = TempDir::new().unwrap();
        let mut h = sample_handle();
        h.image_ref = Some("[fd5a:1f02::1]:5000/acme/app:sha256abc".to_owned());
        h.save(dir.path()).unwrap();

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(
            loaded.image_ref.as_deref(),
            Some("[fd5a:1f02::1]:5000/acme/app:sha256abc"),
            "image_ref must survive save/load"
        );
        assert_eq!(loaded, h);
    }

    /// JSON written before the `image_ref` field was added (no `"image_ref"`
    /// key) must deserialize with `image_ref = None` so old records still load.
    #[test]
    fn image_ref_defaults_to_none_for_old_records() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert!(
            h.image_ref.is_none(),
            "missing image_ref key must deserialize as None"
        );
    }

    // ── requested_runtime field ───────────────────────────────────────────────

    /// A handle with a non-default `requested_runtime` must round-trip through
    /// `save` → `load` with the runtime intact (so a respawn rebuilds the same
    /// runtime the deploy requested).
    #[test]
    fn requested_runtime_round_trips_via_save_load() {
        let dir = TempDir::new().unwrap();
        let mut h = sample_handle();
        h.requested_runtime = Some("firecracker".to_owned());
        h.save(dir.path()).unwrap();

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(
            loaded.requested_runtime.as_deref(),
            Some("firecracker"),
            "requested_runtime must survive save/load"
        );
        assert_eq!(loaded, h);
    }

    /// JSON written before the `requested_runtime` field was added (no
    /// `"requested_runtime"` key) must deserialize with
    /// `requested_runtime = None` so old records still load.
    #[test]
    fn requested_runtime_defaults_to_none_for_old_records() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000,
            "image_ref": null
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert!(
            h.requested_runtime.is_none(),
            "missing requested_runtime key must deserialize as None"
        );
    }

    /// An OLD on-disk record that still carries a now-unknown
    /// `requested_runtime` (e.g. `"docker"`, written before the runtime was
    /// fixed to Firecracker) MUST still deserialize without error — the field is
    /// inert/back-compat only and is never read for dispatch. (No
    /// `deny_unknown_fields`; the value is preserved on read, just ignored.)
    #[test]
    fn requested_runtime_loads_legacy_docker_value() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000,
            "image_ref": null,
            "requested_runtime": "docker"
        }"#;
        let h: RunnerHandle = serde_json::from_str(json)
            .expect("an old record with requested_runtime=docker must still load");
        assert_eq!(
            h.requested_runtime.as_deref(),
            Some("docker"),
            "the legacy value is read but inert (never used for dispatch)"
        );
    }

    // ── network field (Phase-2) ───────────────────────────────────────────────

    /// A handle with a tenant `network` slug must round-trip through save → load
    /// so a respawn rejoins the same network (`--network <slug>`).
    #[test]
    fn network_round_trips_via_save_load() {
        let dir = TempDir::new().unwrap();
        let mut h = sample_handle();
        h.network = Some("n_jpegxik72nng".to_owned());
        h.save(dir.path()).unwrap();

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(
            loaded.network.as_deref(),
            Some("n_jpegxik72nng"),
            "network must survive save/load for respawn scoping"
        );
        assert_eq!(loaded, h);
    }

    /// JSON written before the `network` field was added (no `"network"` key)
    /// must deserialize with `network = None` so old records still load.
    #[test]
    fn network_defaults_to_none_for_old_records() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000,
            "image_ref": null
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert!(
            h.network.is_none(),
            "missing network key must deserialize as None"
        );
    }

    // ── runner_join_token field ───────────────────────────────────────────────

    /// A handle with a persisted `runner_join_token` must round-trip through
    /// save → load so a supervisor-driven RESPAWN re-joins the validating
    /// coordinator with the SAME token (the long-lived token outlives the
    /// runner's idle-outs/crashes) instead of 401ing.
    #[test]
    fn runner_join_token_round_trips_via_save_load() {
        let dir = TempDir::new().unwrap();
        let mut h = sample_handle();
        h.runner_join_token = Some("jwt.runner.token".to_owned());
        h.save(dir.path()).unwrap();

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(
            loaded.runner_join_token.as_deref(),
            Some("jwt.runner.token"),
            "runner_join_token must survive save/load so a respawn re-joins"
        );
        assert_eq!(loaded, h);
    }

    /// JSON written before the `runner_join_token` field was added (no
    /// `"runner_join_token"` key) must deserialize with
    /// `runner_join_token = None` so old records still load.
    #[test]
    fn runner_join_token_defaults_to_none_for_old_records() {
        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000,
            "image_ref": null,
            "network": "n_jpegxik72nng"
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert!(
            h.runner_join_token.is_none(),
            "missing runner_join_token key must deserialize as None"
        );
    }

    /// JSON written before the `restart` field was added (no `"restart"` key)
    /// must deserialize with `RestartState::default()` so old records are not
    /// rejected.
    #[test]
    fn restart_defaults_to_clean_state_for_old_records() {
        use crate::orchestrator::restart::RestartState;

        let json = r#"{
            "uuid": "0191e7c2-1111-7222-8333-444455556666",
            "pid": 12345,
            "control_sock": "/var/run/tabbify/runners/0191e7c2.sock",
            "app_ula": "fd5a:1f02:44a5:240b:121a::1",
            "parent": null,
            "spawned_at": 1700000000
        }"#;
        let h: RunnerHandle = serde_json::from_str(json).unwrap();
        assert_eq!(
            h.restart,
            RestartState::default(),
            "missing restart key must deserialize as RestartState::default()"
        );
    }

    // ── save / load ──────────────────────────────────────────────────────────

    #[test]
    fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let h = sample_handle();
        h.save(dir.path()).unwrap();

        let expected_path = record_path(dir.path(), &h.uuid);
        assert!(expected_path.exists(), "record file must be written");

        let loaded = RunnerHandle::load(dir.path(), &h.uuid)
            .unwrap()
            .expect("record must be present");
        assert_eq!(h, loaded);
    }

    #[test]
    fn load_returns_none_for_missing_uuid() {
        let dir = TempDir::new().unwrap();
        let result = RunnerHandle::load(dir.path(), "does-not-exist").unwrap();
        assert!(
            result.is_none(),
            "missing uuid must return None, not an error"
        );
    }

    // ── list ─────────────────────────────────────────────────────────────────

    #[test]
    fn list_returns_empty_when_dir_missing() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("no-such-subdir");
        let handles = RunnerHandle::list(&missing).unwrap();
        assert!(handles.is_empty());
    }

    #[test]
    fn list_returns_all_saved_handles() {
        let dir = TempDir::new().unwrap();
        let h1 = sample_handle();
        let h2 = sample_handle_no_parent();
        h1.save(dir.path()).unwrap();
        h2.save(dir.path()).unwrap();

        let mut handles = RunnerHandle::list(dir.path()).unwrap();
        handles.sort_by(|a, b| a.uuid.cmp(&b.uuid));
        assert_eq!(handles.len(), 2);
        // Both originals should be recoverable (order-independent match).
        assert!(handles.contains(&h1));
        assert!(handles.contains(&h2));
    }

    #[test]
    fn list_skips_non_json_files() {
        let dir = TempDir::new().unwrap();
        let h = sample_handle();
        h.save(dir.path()).unwrap();
        // Write a non-JSON file alongside — should be ignored.
        fs::write(dir.path().join("README.txt"), "ignore me").unwrap();

        let handles = RunnerHandle::list(dir.path()).unwrap();
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0], h);
    }
}
