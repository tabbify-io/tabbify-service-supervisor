//! Spawn a per-app `tabbify-runner` process DETACHED + persist its record
//! (Task 2.2).
//!
//! The supervisor orchestrator runs one `tabbify-runner` process per app. The
//! whole resilience story depends on that runner being DETACHED from the
//! supervisor: if `supervisord` is SIGKILLed (or simply restarted), the runner
//! and its workload must keep running and stay reachable on their control
//! socket so the supervisor can re-adopt them later (Task 2.5).
//!
//! # Detach mechanics (Unix)
//! We use `Command::pre_exec` (tokio's inherent method, mirroring
//! [`std::os::unix::process::CommandExt::pre_exec`]) to call `libc::setsid()` in
//! the child between `fork` and `exec`. `setsid` makes the child a new
//! **session leader** in a **new process group**, so:
//! - signals delivered to the supervisor's process group (e.g. a `Ctrl-C` /
//!   `SIGINT` to the foreground group, or a group-wide `SIGTERM`) do NOT reach
//!   the runner; and
//! - the runner is not in the supervisor's job-control session, so it survives
//!   the supervisor's death.
//!
//! We deliberately do **not** set `kill_on_drop`. Both `std` and `tokio`
//! default it to `false`, but we are explicit about it here because dropping
//! the returned [`Child`] handle MUST NOT kill the runner — that is the very
//! property the integration test verifies.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::app_ula::derive_app_ula;
use crate::orchestrator::handle::RunnerHandle;

/// Filename of the per-app runner binary, resolved next to the current
/// executable in production (both binaries ship side-by-side from one cargo
/// build / one container image).
const RUNNER_BIN_NAME: &str = "tabbify-runner";

/// Everything [`spawn_runner`] needs to launch one runner process.
///
/// Decoupled from the full clap [`crate::runner::RunnerConfig`] so callers
/// (and the integration test) construct it directly; the binary path is
/// injectable so the test can point at the cargo-built `tabbify-runner`
/// (`env!("CARGO_BIN_EXE_tabbify-runner")`).
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// Path to the `tabbify-runner` binary to exec.
    pub runner_bin: PathBuf,
    /// UUID of the app this runner hosts (string form).
    pub uuid: String,
    /// Unix-domain control socket the runner binds (the orchestrator's channel
    /// to the runner).
    pub control_sock: PathBuf,
    /// S3 base URL for anonymous artifact fetch (a wiremock URI in tests).
    pub s3_base_url: String,
    /// Local data dir the runner caches artifacts under.
    pub data_dir: PathBuf,
    /// ULA of this (parent) supervisor, forwarded to the runner so it can report
    /// up the topology. `None` for a standalone runner.
    pub parent: Option<String>,
    /// Skip mesh join; bind plain loopback. Used for local runs / tests without
    /// root + TUN.
    pub no_mesh: bool,
    /// OCI image ref of the last successful deploy, forwarded to the runner as
    /// `--image-ref <ref>` so a respawn comes up on the deployed version (the
    /// runner applies it to the manifest's `registry_ref`). `None` (the default)
    /// = build from the S3 manifest as usual. Set from
    /// [`RunnerHandle::image_ref`](crate::orchestrator::handle::RunnerHandle) on
    /// a respawn; `None` on a fresh spawn.
    pub image_ref: Option<String>,
    /// Runtime override (D4 wire string) forwarded to the runner as
    /// `--runtime-override <wire>`, so a cold spawn builds the requested runtime
    /// instead of the manifest default (D10). `None` = manifest default.
    pub runtime_override: Option<String>,
}

/// Resolve the production `tabbify-runner` path: the binary sitting next to the
/// currently-running executable (the supervisor + runner ship together).
///
/// Falls back to the bare binary name (resolved via `$PATH` at exec time) if
/// the current executable's directory cannot be determined.
#[must_use]
pub fn default_runner_bin() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(RUNNER_BIN_NAME)))
        .unwrap_or_else(|| PathBuf::from(RUNNER_BIN_NAME))
}

/// Build the runner's argv from a [`SpawnSpec`].
///
/// In `--no-mesh` mode the runner binds an ephemeral loopback address for its
/// app listener (`--bind 127.0.0.1:0`); the orchestrator talks to it over the
/// control socket regardless, so the serve bind addr is the runner's own
/// concern. In mesh mode the runner derives + binds its app-ULA itself, so no
/// `--bind` is passed.
fn build_args(spec: &SpawnSpec) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--uuid".into(),
        spec.uuid.as_str().into(),
        "--control-sock".into(),
        spec.control_sock.clone().into_os_string(),
        "--s3-base-url".into(),
        spec.s3_base_url.as_str().into(),
        "--data-dir".into(),
        spec.data_dir.clone().into_os_string(),
    ];
    if spec.no_mesh {
        args.push("--no-mesh".into());
        // Ephemeral loopback for the app listener; the control socket is the
        // orchestrator's channel.
        args.push("--bind".into());
        args.push("127.0.0.1:0".into());
    }
    if let Some(parent) = &spec.parent {
        args.push("--parent".into());
        args.push(parent.as_str().into());
    }
    // Forward the deployed image ref so a respawn comes up on that version.
    if let Some(image_ref) = &spec.image_ref {
        args.push("--image-ref".into());
        args.push(image_ref.as_str().into());
    }
    // Forward the runtime override so a cold spawn builds the requested runtime
    // instead of the manifest default (D10).
    if let Some(rt) = &spec.runtime_override {
        args.push("--runtime-override".into());
        args.push(rt.as_str().into());
    }
    args
}

/// Spawn a `tabbify-runner` process DETACHED, persist its [`RunnerHandle`] to
/// `<runner_dir>/<uuid>.json`, and return the handle plus the [`Child`].
///
/// The returned [`Child`] is the orchestrator's handle to *track* the process
/// (e.g. await its exit in the health monitor, Task 2.4) — but because the
/// child is its own session leader (via `setsid`) and `kill_on_drop` is left
/// `false`, **dropping the handle does NOT kill the runner**.
///
/// The parent directory of `control_sock` must exist (the runner binds the
/// socket there). `runner_dir` must exist (the record is written there).
///
/// # Errors
/// - the `uuid` is not a valid UUID;
/// - the process fails to spawn (binary missing / not executable);
/// - persisting the [`RunnerHandle`] record fails.
pub async fn spawn_runner(spec: &SpawnSpec, runner_dir: &Path) -> Result<(RunnerHandle, Child)> {
    let parsed_uuid = Uuid::parse_str(&spec.uuid)
        .with_context(|| format!("invalid app uuid: {:?}", spec.uuid))?;
    let app_ula = derive_app_ula(parsed_uuid);

    let mut cmd = Command::new(&spec.runner_bin);
    cmd.args(build_args(spec));

    // Detach: become a new session leader so the runner is not in the
    // supervisor's process group and survives the supervisor's death / signals.
    //
    // SAFETY: `pre_exec` runs in the forked child after `fork(2)` and before
    // `exec(2)`. In that window only async-signal-safe work is permitted (no
    // allocation, no locks). `setsid(2)` is async-signal-safe and is the entire
    // body of the closure. It can only fail with `EPERM` when the caller is
    // already a process-group leader — which the child (a fresh fork) never is —
    // so in practice it never fails here; we still surface any error as an
    // `io::Error` so the spawn fails loudly rather than silently un-detached.
    unsafe {
        cmd.pre_exec(|| {
            // SAFETY: async-signal-safe libc call; see the block comment above.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    // Detach stdio. stdin is always null (the runner reads no input). stdout +
    // stderr are redirected to a per-app append log under
    // `<data_dir>/runners/<uuid>.log` so a crashed / detached runner's output is
    // not lost (it would otherwise go to /dev/null). Logging is BEST-EFFORT: if
    // the file cannot be opened we fall back to `Stdio::null()` so an app start
    // never fails just because we could not open its log.
    cmd.stdin(Stdio::null());
    match open_runner_log(&spec.data_dir, &spec.uuid) {
        Ok(file) => {
            // The child needs an independent fd for stderr; `try_clone` dups the
            // underlying OS handle (separate offset, same append semantics).
            match file.try_clone() {
                Ok(file_err) => {
                    cmd.stdout(Stdio::from(file)).stderr(Stdio::from(file_err));
                    // `cmd` now owns both fds (they are dup'd into the child on
                    // spawn); nothing else holds the parent handles.
                }
                Err(e) => {
                    tracing::warn!(
                        uuid = %spec.uuid,
                        error = %e,
                        "could not clone runner log fd; runner stdio -> /dev/null"
                    );
                    cmd.stdout(Stdio::null()).stderr(Stdio::null());
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                uuid = %spec.uuid,
                error = %e,
                "could not open runner log file; runner stdio -> /dev/null"
            );
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
    }

    // NOTE: we intentionally leave `kill_on_drop` at its default (`false`).
    // Dropping the returned `Child` must NOT kill the detached runner.

    let child = cmd
        .spawn()
        .with_context(|| format!("spawn runner binary {:?}", spec.runner_bin))?;

    let pid = child
        .id()
        .context("spawned runner has no pid (already exited?)")?;

    let spawned_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let handle = RunnerHandle {
        uuid: spec.uuid.clone(),
        pid,
        control_sock: spec.control_sock.clone(),
        app_ula: app_ula.to_string(),
        parent: spec.parent.clone(),
        spawned_at,
        restart: Default::default(),
        // Carry the deployed ref through so a future respawn-from-record keeps
        // the same version. `None` on a fresh spawn = today's behavior.
        image_ref: spec.image_ref.clone(),
        // Persist the requested runtime override so a respawn-from-record
        // rebuilds the SAME runtime (the only source of truth for a ref-deploy).
        requested_runtime: spec.runtime_override.clone(),
    };

    handle
        .save(runner_dir)
        .with_context(|| format!("persist runner record for {}", spec.uuid))?;

    tracing::info!(
        uuid = %spec.uuid,
        pid,
        %app_ula,
        control_sock = %spec.control_sock.display(),
        "spawned detached runner"
    );

    Ok((handle, child))
}

/// Open (creating as needed) the per-app runner log at
/// `<data_dir>/runners/<uuid>.log` for APPEND, returning the file handle.
///
/// Creates the `runners/` subdirectory if missing. The caller redirects the
/// detached runner's stdout + stderr into this file (cloning the handle for the
/// second fd), so each (re)spawn's output is retained across restarts rather
/// than discarded to `/dev/null`. Append mode means a respawn does not clobber
/// the previous run's log.
fn open_runner_log(data_dir: &Path, uuid: &str) -> std::io::Result<std::fs::File> {
    let log_dir = data_dir.join("runners");
    std::fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join(format!("{uuid}.log"));
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn spec() -> SpawnSpec {
        SpawnSpec {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            uuid: "0191e7c2-1111-7222-8333-444455556666".to_owned(),
            control_sock: PathBuf::from("/run/tabbify/runners/x.sock"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: Some("fd5a:1f00:1::1".to_owned()),
            no_mesh: true,
            image_ref: None,
            runtime_override: None,
        }
    }

    /// argv carries the core flags the runner needs, in the form clap expects.
    #[test]
    fn build_args_includes_core_flags() {
        let args = build_args(&spec());
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        // Each flag is present and immediately followed by its value.
        for (flag, value) in [
            ("--uuid", "0191e7c2-1111-7222-8333-444455556666"),
            ("--control-sock", "/run/tabbify/runners/x.sock"),
            ("--s3-base-url", "http://s3.invalid"),
            ("--data-dir", "/var/lib/tabbify/data"),
            ("--parent", "fd5a:1f00:1::1"),
            ("--bind", "127.0.0.1:0"),
        ] {
            let idx = joined
                .iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("missing flag {flag} in {joined:?}"));
            assert_eq!(
                joined.get(idx + 1).map(String::as_str),
                Some(value),
                "flag {flag} should be followed by {value}"
            );
        }
        assert!(joined.iter().any(|a| a == "--no-mesh"), "got: {joined:?}");
    }

    /// In mesh mode no loopback `--bind`/`--no-mesh` is passed (the runner binds
    /// its own ULA); `--parent` is omitted when there is no parent.
    #[test]
    fn build_args_mesh_mode_has_no_bind_or_parent() {
        let mut s = spec();
        s.no_mesh = false;
        s.parent = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(!joined.iter().any(|a| a == "--no-mesh"), "got: {joined:?}");
        assert!(!joined.iter().any(|a| a == "--bind"), "got: {joined:?}");
        assert!(!joined.iter().any(|a| a == "--parent"), "got: {joined:?}");
    }

    /// When `image_ref` is set, the runner argv carries `--image-ref <ref>` so a
    /// respawn comes up on the deployed version.
    #[test]
    fn build_args_includes_image_ref_when_present() {
        let mut s = spec();
        s.image_ref = Some("[fd5a::1]:5000/a/b:sha".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let idx = joined
            .iter()
            .position(|a| a == "--image-ref")
            .unwrap_or_else(|| panic!("missing --image-ref in {joined:?}"));
        assert_eq!(
            joined.get(idx + 1).map(String::as_str),
            Some("[fd5a::1]:5000/a/b:sha"),
            "--image-ref must be followed by the ref"
        );
    }

    /// When `image_ref` is `None`, no `--image-ref` arg is emitted (today's
    /// default behavior is unchanged).
    #[test]
    fn build_args_omits_image_ref_when_none() {
        let mut s = spec();
        s.image_ref = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--image-ref"),
            "no --image-ref when None; got: {joined:?}"
        );
    }

    /// runtime_override = Some emits `--runtime-override <wire>`.
    #[test]
    fn build_args_includes_runtime_override_when_present() {
        let mut s = spec();
        s.runtime_override = Some("docker".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let idx = joined
            .iter()
            .position(|a| a == "--runtime-override")
            .unwrap_or_else(|| panic!("missing --runtime-override in {joined:?}"));
        assert_eq!(
            joined.get(idx + 1).map(String::as_str),
            Some("docker"),
            "--runtime-override must be followed by the wire string"
        );
    }

    /// runtime_override = None emits no `--runtime-override`.
    #[test]
    fn build_args_omits_runtime_override_when_none() {
        let mut s = spec();
        s.runtime_override = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--runtime-override"),
            "no --runtime-override when None; got: {joined:?}"
        );
    }

    /// Write an executable shell wrapper that prints `out_text` to stdout and
    /// `err_text` to stderr, then exits 0. Returns its path. Used by the
    /// log-capture tests below: pointing `SpawnSpec.runner_bin` at it lets us
    /// drive the real detached spawn without a real `tabbify-runner`.
    fn write_echo_wrapper(dir: &Path, name: &str, out_text: &str, err_text: &str) -> PathBuf {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let wrapper = dir.join(name);
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            // `"$@"` is ignored; we only care about the captured stdio.
            writeln!(
                f,
                "#!/bin/sh\nprintf '%s\\n' '{out_text}'\nprintf '%s\\n' '{err_text}' 1>&2\n"
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();
        wrapper
    }

    /// A spawned runner's stdout AND stderr are captured to
    /// `<data_dir>/runners/<uuid>.log`.
    #[tokio::test]
    async fn spawn_runner_captures_stdout_and_stderr_to_log_file() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(
            dir.path(),
            "echo-runner.sh",
            "RUNNER_STDOUT_LINE",
            "RUNNER_STDERR_LINE",
        );

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-aaaa-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        // Await the wrapper so it has written and flushed before we read the log.
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path)
            .unwrap_or_else(|e| panic!("runner log {log_path:?} should exist: {e}"));
        assert!(
            contents.contains("RUNNER_STDOUT_LINE"),
            "log must contain captured stdout; got: {contents:?}"
        );
        assert!(
            contents.contains("RUNNER_STDERR_LINE"),
            "log must contain captured stderr; got: {contents:?}"
        );
    }

    /// A second spawn for the same uuid APPENDS to the existing log (both runs
    /// retained), it does not truncate.
    #[tokio::test]
    async fn spawn_runner_appends_to_existing_log() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper_a = write_echo_wrapper(dir.path(), "a.sh", "FIRST_RUN_OUT", "FIRST_RUN_ERR");
        let wrapper_b = write_echo_wrapper(dir.path(), "b.sh", "SECOND_RUN_OUT", "SECOND_RUN_ERR");

        let mut s = spec();
        s.uuid = "0191e7c2-bbbb-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        std::fs::create_dir_all(&s.data_dir).unwrap();

        s.runner_bin = wrapper_a;
        let (_h1, mut c1) = spawn_runner(&s, dir.path()).await.unwrap();
        c1.wait().await.unwrap();

        s.runner_bin = wrapper_b;
        let (_h2, mut c2) = spawn_runner(&s, dir.path()).await.unwrap();
        c2.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        for needle in [
            "FIRST_RUN_OUT",
            "FIRST_RUN_ERR",
            "SECOND_RUN_OUT",
            "SECOND_RUN_ERR",
        ] {
            assert!(
                contents.contains(needle),
                "appended log must retain {needle}; got: {contents:?}"
            );
        }
    }

    /// If the log file cannot be opened, the spawn still succeeds (logging is
    /// best-effort and must never block an app from starting). Here `data_dir`
    /// is a FILE, so `runners/` cannot be created under it.
    #[tokio::test]
    async fn spawn_runner_falls_back_to_null_when_log_open_fails() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(dir.path(), "echo.sh", "OUT", "ERR");

        // data_dir points at an existing regular file → create_dir_all fails.
        let bogus_data = dir.path().join("not-a-dir");
        std::fs::write(&bogus_data, b"i am a file").unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-cccc-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = bogus_data;

        let (_handle, mut child) = spawn_runner(&s, dir.path())
            .await
            .expect("spawn must succeed even when log dir cannot be created");
        child.wait().await.unwrap();
    }

    /// The prod binary path resolves next to the current executable (not the
    /// bare name), so a relative `cwd` can't break runner discovery.
    #[test]
    fn default_runner_bin_is_next_to_current_exe() {
        let resolved = default_runner_bin();
        assert_eq!(
            resolved.file_name().and_then(|n| n.to_str()),
            Some(RUNNER_BIN_NAME)
        );
        // When the current exe is resolvable the path is absolute (has a parent
        // dir); we don't assert the exact dir (varies by test runner layout).
        if std::env::current_exe().is_ok() {
            assert!(
                resolved.parent().is_some(),
                "resolved runner path should sit in a directory: {resolved:?}"
            );
        }
    }
}
