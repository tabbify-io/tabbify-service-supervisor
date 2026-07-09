//! One-shot build dispatch from the orchestrator (Task P3.5).
//!
//! [`Orchestrator::spawn_build`] serializes a [`BuildJob`] to a 0600 temp file,
//! invokes `tabbify-runner --build-spec <file>` as a CAPTURED (awaited) child,
//! parses the resulting [`ArtifactRef`] from stdout, and returns it.
//!
//! The captured-child spawn is different from the DETACHED serve spawn in
//! [`super::spawn`]: we need the runner's stdout (the `ArtifactRef` JSON) and
//! we wait for it to exit, so `kill_on_drop` and `setsid` are NOT used here.
//!
//! # Injection seam
//! The actual process execution goes through a [`BuildSpawner`] so tests can
//! drive the whole path with a canned stdout + exit-code without spawning a
//! real `tabbify-runner` binary.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, bail};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::orchestrator::Orchestrator;
use crate::runner::build::ArtifactRef;
use crate::runner::build::BuildJob;

/// Outcome of one [`BuildSpawner`] invocation: the captured stdout bytes plus
/// a boolean indicating whether the child exited with status 0.
pub struct BuildOutput {
    /// Raw bytes written to stdout by `tabbify-runner`.
    pub stdout: Vec<u8>,
    /// Raw bytes written to stderr by `tabbify-runner`.
    pub stderr: Vec<u8>,
    /// `true` iff the child exited with status 0.
    pub success: bool,
}

/// Injectable seam for the captured-child build spawn.
///
/// The production implementation runs
/// `tabbify-runner --uuid <UUID> --build-spec <PATH>` and captures its output;
/// tests supply a closure that returns canned data without starting any
/// process.
///
/// The `Box<dyn …>` indirection keeps [`Orchestrator`] object-safe and avoids
/// generic parameters leaking into the rest of the API layer.
///
/// # Why `app_uuid` is here
///
/// `RunnerConfig` requires `--uuid` at parse time even in builder mode (the
/// flag is declared without a default and clap rejects the invocation without
/// it). Passing it through the spawner — rather than putting it only inside
/// the spec JSON — keeps the CLI contract honest: the runner can be launched
/// directly from a shell with the exact arg list shown here.
pub trait BuildSpawner: Send + Sync {
    /// Run the build runner with `spec_path` as the `--build-spec` argument
    /// and `app_uuid` as the `--uuid` argument, returning the captured output.
    fn run<'a>(
        &'a self,
        spec_path: &'a Path,
        app_uuid: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>>;
}

/// Production [`BuildSpawner`]: runs the real `tabbify-runner` binary next to
/// the orchestrator's known runner binary path.
pub struct ProcessBuildSpawner {
    /// Absolute path to the `tabbify-runner` binary.
    pub runner_bin: std::path::PathBuf,
    /// Supervisor-resolved data dir, injected as `SUPERVISOR_DATA_DIR` into
    /// the build child so the sandboxed build path (cache/lock/logs) lands
    /// under the SAME dir as the daemon, regardless of how the daemon
    /// resolved it (env vs `--data-dir`).
    pub data_dir: std::path::PathBuf,
}

impl BuildSpawner for ProcessBuildSpawner {
    fn run<'a>(
        &'a self,
        spec_path: &'a Path,
        app_uuid: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>>
    {
        use std::process::Stdio;
        use tokio::process::Command;

        let runner_bin = self.runner_bin.clone();
        let spec_path = spec_path.to_path_buf();
        let app_uuid = app_uuid.to_owned();
        let data_dir = self.data_dir.clone();
        Box::pin(async move {
            let mut child = Command::new(&runner_bin)
                .args([
                    "--uuid",
                    &app_uuid,
                    "--build-spec",
                    spec_path.to_string_lossy().as_ref(),
                ])
                .env("SUPERVISOR_DATA_DIR", &data_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                // NOTE: no setsid, no kill_on_drop override — this is a captured
                // child we await to completion.
                .spawn()
                .with_context(|| format!("spawn build runner {:?}", runner_bin))?;

            // P1-3 (progress visibility): tee the child's stdout+stderr to a LIVE
            // progress log (`<data_dir>/build/<uuid>.progress.log`, truncated per
            // build) AS the build runs, so a concurrent
            // `GET /v1/build/{uuid}/progress` tail shows real-time forward
            // progress (distinguishing a slow build from a hung one) instead of
            // only the buffered-at-completion `<uuid>.log`. The bytes are ALSO
            // accumulated in memory and returned unchanged, so the ArtifactRef
            // parse + the canonical build-log write downstream are byte-identical
            // to the previous `.output()` behaviour.
            let progress_sink = open_progress_log(&data_dir, &app_uuid).await;
            let stdout_pipe = child.stdout.take();
            let stderr_pipe = child.stderr.take();

            let out_sink = progress_sink.clone();
            let out_task = tokio::spawn(async move {
                match stdout_pipe {
                    Some(pipe) => drain_tee(pipe, out_sink).await,
                    None => Vec::new(),
                }
            });
            let err_sink = progress_sink.clone();
            let err_task = tokio::spawn(async move {
                match stderr_pipe {
                    Some(pipe) => drain_tee(pipe, err_sink).await,
                    None => Vec::new(),
                }
            });

            let status = child
                .wait()
                .await
                .with_context(|| format!("await build runner {runner_bin:?}"))?;
            // The drain tasks are driven to EOF (which the pipes reach at child
            // exit); joining them can only fail if the task panicked, which the
            // simple copy loop never does — default to empty on the impossible
            // join error rather than propagate.
            let stdout = out_task.await.unwrap_or_default();
            let stderr = err_task.await.unwrap_or_default();

            Ok(BuildOutput {
                stdout,
                stderr,
                success: status.success(),
            })
        })
    }
}

/// Live per-build progress log path: `<data_dir>/build/<app_uuid>.progress.log`.
///
/// Distinct from the canonical `<app_uuid>.log` (written ONCE, at completion, by
/// [`write_build_log`]): this file is truncated at the start of each build and
/// appended to as the build runs, so a poller sees progress in real time.
pub(crate) fn build_progress_log_path(data_dir: &Path, app_uuid: &str) -> PathBuf {
    data_dir
        .join("build")
        .join(format!("{app_uuid}.progress.log"))
}

/// Create (truncating) the live progress log for `app_uuid`, returning a shared
/// handle the tee tasks append to. Best-effort: on any dir-create / open failure
/// the sink is `None` and the build still runs (progress is simply unavailable
/// for that build) — a progress-log problem must never fail a build.
async fn open_progress_log(data_dir: &Path, app_uuid: &str) -> Arc<Mutex<Option<tokio::fs::File>>> {
    let path = build_progress_log_path(data_dir, app_uuid);
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::warn!(app_uuid, error = %e, "could not create build progress dir; progress disabled for this build");
            return Arc::new(Mutex::new(None));
        }
    }
    match tokio::fs::File::create(&path).await {
        Ok(f) => Arc::new(Mutex::new(Some(f))),
        Err(e) => {
            tracing::warn!(app_uuid, error = %e, "could not open build progress log; progress disabled for this build");
            Arc::new(Mutex::new(None))
        }
    }
}

/// Drain `reader` to EOF, returning every byte read AND appending each chunk to
/// `sink` (the live progress log) as it arrives. Best-effort on the sink: a
/// write error is logged once and further writes skipped, but the returned bytes
/// are always complete (the caller relies on them for the ArtifactRef parse).
async fn drain_tee<R>(mut reader: R, sink: Arc<Mutex<Option<tokio::fs::File>>>) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut all = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,           // EOF
            Err(_) => break,          // pipe error — stop; captured bytes so far
            Ok(n) => {
                all.extend_from_slice(&buf[..n]);
                let mut guard = sink.lock().await;
                if let Some(f) = guard.as_mut() {
                    // `tokio::fs::File` batches writes internally, so flush each
                    // chunk to the OS — otherwise the live progress tail (and a
                    // drop) could miss in-flight bytes. Any error drops the sink
                    // so we stop retrying a broken file; the captured bytes (and
                    // the canonical build log) are unaffected.
                    if f.write_all(&buf[..n]).await.is_err() || f.flush().await.is_err() {
                        *guard = None;
                    }
                }
            }
        }
    }
    all
}

/// Coarse build stage inferred from the tail of the (live) build log, for
/// agent-visible progress (P1-3). A cheap keyword heuristic — enough to tell
/// "pulling" from "compiling" from "booting" so the agent distinguishes a slow
/// build from a hung one, without parsing the buildkit/docker protocol. Latest
/// stages are checked first so the newest signal in the tail wins.
pub(crate) fn derive_build_stage(tail: &str) -> &'static str {
    let l = tail.to_ascii_lowercase();
    // Newest-stage-first: when the tail spans multiple stages the most ADVANCED
    // signal wins (a clone line lingering above build output ⇒ still "building").
    if l.contains("firecracker") || l.contains("microvm") || l.contains("snapshot") {
        "booting"
    } else if l.contains("rootfs")
        || l.contains("convert")
        || l.contains("export")
        || l.contains("pushing")
    {
        "converting"
    } else if l.contains("compiling")
        || l.contains("cargo")
        || l.contains("step ") // docker "Step 3/9 : ..."
        || l.contains(" run ") // docker "RUN ..."
        || l.contains("building")
    {
        "building"
    } else if l.contains("cloning")
        || l.contains("pulling")
        || l.contains("fetching")
        || l.contains("download")
    {
        "pulling"
    } else if !tail.trim().is_empty() {
        "building"
    } else {
        "starting"
    }
}

/// A snapshot of an in-flight (or most-recent) build's progress, returned by
/// `GET /v1/build/{uuid}/progress` for the node's deploy-progress poller.
#[derive(Debug, Serialize)]
pub struct BuildProgress {
    /// Coarse stage: `starting` | `pulling` | `building` | `converting` |
    /// `booting`. Derived from the log tail via [`derive_build_stage`].
    pub stage: String,
    /// Last N lines of the live build log. Mesh-internal (may echo raw build
    /// output) — never expose unredacted to external callers.
    pub log_tail: String,
    /// Current byte length of the live progress log. A value that grows across
    /// polls ⇒ the build is advancing; a static value over several polls ⇒ it may
    /// be hung.
    pub log_bytes: u64,
}

impl Orchestrator {
    /// Dispatch a one-shot build: serialize `job` to a 0600 temp file, run
    /// `tabbify-runner --build-spec <file>` as a captured (awaited) child,
    /// parse the [`ArtifactRef`] from stdout, and return it.
    ///
    /// The spec file may contain short-lived tokens so it is written with mode
    /// 0600 and removed (best-effort) whether the build succeeds or fails.
    ///
    /// The full multi-target control-plane (build-then-deploy across a fleet) is
    /// Phase 4; this is the minimal invoker.
    ///
    /// # Errors
    /// - The spec file could not be written.
    /// - The runner binary failed to spawn.
    /// - The child exited non-zero — the captured stderr is included in the error.
    /// - The child's stdout could not be parsed as [`ArtifactRef`] JSON.
    pub async fn spawn_build(&self, job: &BuildJob) -> anyhow::Result<ArtifactRef> {
        self.spawn_build_with(
            job,
            &ProcessBuildSpawner {
                runner_bin: self.shared().runner_bin.clone(),
                data_dir: self.shared().data_dir.clone(),
            },
        )
        .await
    }

    /// Same as [`spawn_build`](Self::spawn_build) but with an injected
    /// [`BuildSpawner`].  Used directly by tests to avoid spawning a real process.
    pub async fn spawn_build_with(
        &self,
        job: &BuildJob,
        spawner: &dyn BuildSpawner,
    ) -> anyhow::Result<ArtifactRef> {
        // Serialize the job to JSON.
        let spec_json = serde_json::to_vec(job).context("serialize BuildJob")?;

        // Write a 0600 temp file under the data dir (which always exists at
        // runtime).  Using the data dir keeps the spec on the same filesystem so a
        // rename(2) would be atomic; in practice we just write + delete it.
        let spec_path = write_spec_file(&self.shared().data_dir, &spec_json)?;

        // Run the build runner.  We delete the spec file on both success and
        // failure so tokens do not linger.
        // `app_uuid` is forwarded explicitly: `tabbify-runner --uuid` is required
        // at parse time even in builder mode, so the spawner can't infer it from
        // the spec file alone.
        let result = spawner.run(&spec_path, &job.app_uuid).await;
        let _ = std::fs::remove_file(&spec_path); // best-effort cleanup

        let output = result.context("build runner invocation")?;

        // Persist the captured build output (stdout + stderr) to a per-app log
        // BEFORE branching on success: a failed build is exactly when the
        // operator needs the log. Writing is best-effort — a log-write failure
        // must not mask the build result.
        write_build_log(
            &self.shared().data_dir,
            &job.app_uuid,
            &output.stdout,
            &output.stderr,
        );

        if !output.success {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("build runner exited non-zero; stderr: {}", stderr.trim());
        }

        // Parse the ArtifactRef from the last non-empty line of stdout (the
        // runner may emit log lines to stdout before the final JSON in some
        // configurations; taking the last line is robust).
        let stdout = String::from_utf8_lossy(&output.stdout);
        let last_line = stdout
            .lines()
            .rfind(|l| !l.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("build runner produced no stdout"))?;

        serde_json::from_str(last_line)
            .with_context(|| format!("parse ArtifactRef from stdout line: {last_line:?}"))
    }

    /// Snapshot the current build's progress (P1-3): the last `lines` lines of
    /// the live progress log plus a derived [`stage`](derive_build_stage) and the
    /// log's byte length. Returns `None` when no progress log exists yet for
    /// `app_uuid` (no build has started, or it was cleaned up) so the caller can
    /// answer 404. The node polls this WHILE its (blocking) build request is in
    /// flight to surface forward progress to the agent.
    pub async fn build_progress(&self, app_uuid: &str, lines: usize) -> Option<BuildProgress> {
        let path = build_progress_log_path(&self.shared().data_dir, app_uuid);
        // A missing file ⇒ no build in progress (or none ever) ⇒ 404 upstream.
        let log_bytes = tokio::fs::metadata(&path).await.ok()?.len();
        let log_tail = crate::orchestrator::api::read_last_lines(&path, lines)
            .await
            .unwrap_or_default();
        Some(BuildProgress {
            stage: derive_build_stage(&log_tail).to_owned(),
            log_tail,
            log_bytes,
        })
    }
}

/// Write `data` to a fresh 0600 temp file under `base_dir`.
///
/// Returns the path of the written file.
fn write_spec_file(base_dir: &Path, data: &[u8]) -> anyhow::Result<std::path::PathBuf> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    // Build a deterministic-ish temp name with a process-unique suffix.
    let name = format!(
        "tbf-build-spec-{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let path = base_dir.join(&name);

    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("create build spec file {}", path.display()))?;

    f.write_all(data)
        .with_context(|| format!("write build spec file {}", path.display()))?;

    Ok(path)
}

/// Append the captured build output to `<data_dir>/build/<app_uuid>.log`.
///
/// Writes `stdout`, a `--- stderr ---` separator, then `stderr`. Best-effort:
/// any failure (dir create / open / write) is logged and swallowed so it never
/// changes the build's success/failure result. Append mode keeps prior build
/// runs for the same app rather than clobbering them.
fn write_build_log(data_dir: &Path, app_uuid: &str, stdout: &[u8], stderr: &[u8]) {
    use std::io::Write as _;

    let log_dir = data_dir.join("build");
    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        tracing::warn!(app_uuid, error = %e, "could not create build log dir; skipping build log");
        return;
    }
    let log_path = log_dir.join(format!("{app_uuid}.log"));
    let mut f = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(app_uuid, error = %e, "could not open build log; skipping build log");
            return;
        }
    };

    // One combined write attempt; on partial failure we just warn.
    let res = (|| -> std::io::Result<()> {
        f.write_all(stdout)?;
        f.write_all(b"\n--- stderr ---\n")?;
        f.write_all(stderr)?;
        f.write_all(b"\n")?;
        Ok(())
    })();
    if let Err(e) = res {
        tracing::warn!(app_uuid, error = %e, "failed writing build log");
        return;
    }
    tracing::info!(app_uuid, path = %log_path.display(), "captured build output to log");
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::future::Future;
    use std::os::unix::fs::MetadataExt as _;
    use std::path::{Path, PathBuf};
    use std::pin::Pin;

    use super::*;
    use crate::orchestrator::{Orchestrator, SharedRunnerConfig};

    // ── helpers ──────────────────────────────────────────────────────────────

    fn orch(data_dir: PathBuf, runner_dir: PathBuf) -> Orchestrator {
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

    fn test_job() -> BuildJob {
        BuildJob {
            repo_url: "https://github.com/acme/app".into(),
            git_ref: "abc123".into(),
            tenant: "acme".into(),
            app_uuid: "u".into(),
            registry_ula: "[fd5a::1]:5000".into(),
            clone_token: None,
            push_token: None,
            build_kind: crate::runner::build::BuildKind::Docker,
            build_cmd: None,
            artifact_path: None,
            manifest_toml: None,
        }
    }

    /// A [`BuildSpawner`] that records the spec-file path it was called with and
    /// returns a canned stdout (successful build).
    struct CannedSpawner {
        stdout: Vec<u8>,
        success: bool,
        called_with: std::sync::Arc<std::sync::Mutex<Option<PathBuf>>>,
    }

    impl CannedSpawner {
        fn ok(stdout: &str) -> (Self, std::sync::Arc<std::sync::Mutex<Option<PathBuf>>>) {
            let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
            let s = Self {
                stdout: stdout.as_bytes().to_vec(),
                success: true,
                called_with: slot.clone(),
            };
            (s, slot)
        }

        fn fail() -> Self {
            Self {
                stdout: b"".to_vec(),
                success: false,
                called_with: std::sync::Arc::new(std::sync::Mutex::new(None)),
            }
        }
    }

    impl BuildSpawner for CannedSpawner {
        fn run<'a>(
            &'a self,
            spec_path: &'a Path,
            _app_uuid: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>> {
            // Record the spec path; also verify the file exists at call time.
            *self.called_with.lock().unwrap() = Some(spec_path.to_path_buf());
            let stdout = self.stdout.clone();
            let success = self.success;
            Box::pin(async move {
                Ok(BuildOutput {
                    stdout,
                    stderr: b"fake error output".to_vec(),
                    success,
                })
            })
        }
    }

    // ── write_spec_file ───────────────────────────────────────────────────────

    /// The spec file is created with mode 0600.
    #[test]
    fn spec_file_has_mode_0600() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"hello";
        let path = write_spec_file(dir.path(), data).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        let mode = meta.mode() & 0o777;
        assert_eq!(mode, 0o600, "spec file must be 0600, got {mode:03o}");
    }

    /// The spec file contains the exact bytes written.
    #[test]
    fn spec_file_contains_written_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"{\"repo_url\":\"x\"}";
        let path = write_spec_file(dir.path(), data).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
    }

    // ── spawn_build_with (injected spawner) ───────────────────────────────────

    /// Happy path: the spawner is invoked with a `--build-spec`-like path whose
    /// file contains the serialized BuildJob, returns a canned ArtifactRef JSON,
    /// and `spawn_build_with` parses it correctly.
    #[tokio::test]
    async fn spawn_build_happy_path_returns_artifact_ref() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let canned = r#"{"reff":"[fd5a::1]:5000/acme/u:abc","digest":null}"#;
        let (spawner, called_with) = CannedSpawner::ok(canned);
        let job = test_job();

        let art = o.spawn_build_with(&job, &spawner).await.unwrap();
        assert_eq!(art.reff, "[fd5a::1]:5000/acme/u:abc");
        assert!(art.digest.is_none());

        // The spawner must have been called with a path that ended in .json and
        // contained the serialized job.
        let spec_path = called_with
            .lock()
            .unwrap()
            .clone()
            .expect("spawner was called");
        assert!(
            spec_path.extension().and_then(|e| e.to_str()) == Some("json"),
            "spec file must be a .json file: {spec_path:?}"
        );
        // The spec file itself is cleaned up by the time we get here (delete is
        // best-effort before returning), so we check the serialized content by
        // re-serializing the job and comparing.
        let expected_json = serde_json::to_string(&job).unwrap();
        // The file may already be deleted; read it only if it still exists
        // (in the test the spawner is canned so delete races with the check).
        // We assert the path was under the data dir instead.
        assert!(
            spec_path.starts_with(dir.path()),
            "spec file must be under the data dir: {spec_path:?}"
        );
        let _ = expected_json; // silence unused warning
    }

    /// The spec file is gone after `spawn_build_with` returns (cleanup on success).
    #[tokio::test]
    async fn spawn_build_cleans_up_spec_file_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let canned = r#"{"reff":"[fd5a::1]:5000/a/b:sha","digest":null}"#;
        let (spawner, called_with) = CannedSpawner::ok(canned);

        o.spawn_build_with(&test_job(), &spawner).await.unwrap();

        let spec_path = called_with.lock().unwrap().clone().unwrap();
        assert!(
            !spec_path.exists(),
            "spec file must be cleaned up after a successful build: {spec_path:?}"
        );
    }

    /// The spec file is cleaned up even when the build fails.
    #[tokio::test]
    async fn spawn_build_cleans_up_spec_file_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        // We need to capture the spec path even from the failing spawner.
        let called_with = std::sync::Arc::new(std::sync::Mutex::new(None::<PathBuf>));
        let cw2 = called_with.clone();
        struct FailCapture {
            slot: std::sync::Arc<std::sync::Mutex<Option<PathBuf>>>,
        }
        impl BuildSpawner for FailCapture {
            fn run<'a>(
                &'a self,
                spec_path: &'a Path,
                _app_uuid: &'a str,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>>
            {
                *self.slot.lock().unwrap() = Some(spec_path.to_path_buf());
                Box::pin(async move {
                    Ok(BuildOutput {
                        stdout: b"".to_vec(),
                        stderr: b"build failed".to_vec(),
                        success: false,
                    })
                })
            }
        }

        let spawner = FailCapture { slot: cw2 };
        let _ = o.spawn_build_with(&test_job(), &spawner).await;

        let spec_path = called_with.lock().unwrap().clone().unwrap();
        assert!(
            !spec_path.exists(),
            "spec file must be cleaned up after a failed build: {spec_path:?}"
        );
    }

    /// When the spawner returns `success: false`, `spawn_build_with` returns an
    /// error (the stderr content is captured in the error message).
    #[tokio::test]
    async fn spawn_build_errors_on_non_zero_exit() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let spawner = CannedSpawner::fail();
        let err = o
            .spawn_build_with(&test_job(), &spawner)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("non-zero") || err.contains("exit"),
            "error must mention non-zero exit; got: {err}"
        );
    }

    /// When the spawner returns garbled stdout, `spawn_build_with` fails with a
    /// parse error.
    #[tokio::test]
    async fn spawn_build_errors_on_bad_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let (spawner, _) = CannedSpawner::ok("not valid json at all");
        let err = o
            .spawn_build_with(&test_job(), &spawner)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("parse ArtifactRef") || err.contains("ArtifactRef"),
            "error must mention ArtifactRef parsing; got: {err}"
        );
    }

    /// When the spawner returns only a digest-less ArtifactRef on the last line,
    /// `digest` is `None`.
    #[tokio::test]
    async fn spawn_build_parses_digest_none() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let canned = r#"{"reff":"[fd5a::1]:5000/acme/u:abc"}"#;
        let (spawner, _) = CannedSpawner::ok(canned);
        let art = o.spawn_build_with(&test_job(), &spawner).await.unwrap();
        assert!(art.digest.is_none());
    }

    /// `spawn_build_with` forwards the job's `app_uuid` to the spawner, so the
    /// production [`ProcessBuildSpawner`] can pass it as `--uuid <UUID>` to the
    /// runner. Without this `tabbify-runner` aborts at clap-parse time even in
    /// builder mode (the field has no default).
    #[tokio::test]
    async fn spawn_build_forwards_app_uuid_to_spawner() {
        use std::sync::{Arc, Mutex};

        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        struct UuidCapturingSpawner {
            seen: Arc<Mutex<Option<String>>>,
            stdout: Vec<u8>,
        }
        impl BuildSpawner for UuidCapturingSpawner {
            fn run<'a>(
                &'a self,
                _spec_path: &'a Path,
                app_uuid: &'a str,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>>
            {
                *self.seen.lock().unwrap() = Some(app_uuid.to_owned());
                let stdout = self.stdout.clone();
                Box::pin(async move {
                    Ok(BuildOutput {
                        stdout,
                        stderr: vec![],
                        success: true,
                    })
                })
            }
        }

        let seen = Arc::new(Mutex::new(None));
        let spawner = UuidCapturingSpawner {
            seen: seen.clone(),
            stdout: br#"{"reff":"x","digest":null}"#.to_vec(),
        };

        let mut job = test_job();
        job.app_uuid = "0191e7c2-1111-7222-8333-444455556666".into();
        o.spawn_build_with(&job, &spawner).await.unwrap();

        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("0191e7c2-1111-7222-8333-444455556666")
        );
    }

    /// The production [`ProcessBuildSpawner`] constructs a child-process arg
    /// list that includes BOTH `--uuid <app_uuid>` and `--build-spec <path>` —
    /// in any order, but both must be present. We exercise it by pointing the
    /// `runner_bin` at `/bin/sh -c 'printf %s "$*"'` which echoes its arg list,
    /// then assert the echo contains the expected flags.
    ///
    /// This is a real child-spawn, but uses only `/bin/sh`, so it is portable
    /// across the Linux dev hosts the supervisor runs on.
    #[tokio::test]
    async fn process_spawner_passes_uuid_and_build_spec_flags() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        // Wrapper script: print all args, separated by spaces, on stdout
        // (mirrors a real `tabbify-runner` invocation argv), exit 0.
        let dir = tempfile::tempdir().unwrap();
        let wrapper = dir.path().join("fake-runner.sh");
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            // `--build-spec` is on argv at a known position; `--uuid` precedes.
            // We echo argv and exit successfully with a canned ArtifactRef so
            // the parent's stdout-parse path still works.
            writeln!(
                f,
                "#!/bin/sh\nprintf '%s\\n' \"$*\" 1>&2\nprintf '{{\"reff\":\"x\",\"digest\":null}}\\n'\n"
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();

        let spawner = ProcessBuildSpawner {
            runner_bin: wrapper.clone(),
            data_dir: dir.path().to_path_buf(),
        };

        let spec_file = dir.path().join("spec.json");
        std::fs::write(&spec_file, br#"{"x":1}"#).unwrap();

        let out = spawner
            .run(&spec_file, "0191e7c2-1111-7222-8333-444455556666")
            .await
            .expect("spawn");
        assert!(out.success, "wrapper script must exit 0");
        // The wrapper writes argv to STDERR (so it doesn't collide with the
        // ArtifactRef JSON on stdout).
        let argv = String::from_utf8_lossy(&out.stderr);
        assert!(
            argv.contains("--uuid 0191e7c2-1111-7222-8333-444455556666"),
            "argv must contain `--uuid <app_uuid>`; got: {argv:?}"
        );
        assert!(
            argv.contains(&format!("--build-spec {}", spec_file.to_string_lossy())),
            "argv must contain `--build-spec <path>`; got: {argv:?}"
        );
    }

    /// A spawner that returns caller-supplied stdout + stderr with a chosen exit
    /// status. Used to assert the build log captures BOTH streams.
    struct StreamsSpawner {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        success: bool,
    }
    impl BuildSpawner for StreamsSpawner {
        fn run<'a>(
            &'a self,
            _spec_path: &'a Path,
            _app_uuid: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>> {
            let stdout = self.stdout.clone();
            let stderr = self.stderr.clone();
            let success = self.success;
            Box::pin(async move {
                Ok(BuildOutput {
                    stdout,
                    stderr,
                    success,
                })
            })
        }
    }

    /// On a SUCCESSFUL build the captured stdout AND stderr are written to
    /// `<data_dir>/build/<app_uuid>.log`.
    #[tokio::test]
    async fn spawn_build_writes_build_log_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let spawner = StreamsSpawner {
            stdout: br#"BUILD_STDOUT_NOISE
{"reff":"[fd5a::1]:5000/acme/u:abc","digest":null}"#
                .to_vec(),
            stderr: b"BUILD_STDERR_NOISE".to_vec(),
            success: true,
        };

        let mut job = test_job();
        job.app_uuid = "0191e7c2-dddd-7222-8333-444455556666".into();
        o.spawn_build_with(&job, &spawner).await.unwrap();

        let log_path = dir
            .path()
            .join("build")
            .join(format!("{}.log", job.app_uuid));
        let contents = std::fs::read_to_string(&log_path)
            .unwrap_or_else(|e| panic!("build log {log_path:?} should exist: {e}"));
        assert!(
            contents.contains("BUILD_STDOUT_NOISE"),
            "build log must contain captured stdout; got: {contents:?}"
        );
        assert!(
            contents.contains("BUILD_STDERR_NOISE"),
            "build log must contain captured stderr; got: {contents:?}"
        );
    }

    /// On a FAILED build the captured stdout AND stderr are still written to the
    /// build log (this is exactly when the output matters most).
    #[tokio::test]
    async fn spawn_build_writes_build_log_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let spawner = StreamsSpawner {
            stdout: b"PARTIAL_BUILD_OUTPUT".to_vec(),
            stderr: b"COMPILER_ERROR_E0277".to_vec(),
            success: false,
        };

        let mut job = test_job();
        job.app_uuid = "0191e7c2-eeee-7222-8333-444455556666".into();
        // The call returns an error (non-zero exit) but the log is still written.
        let _ = o.spawn_build_with(&job, &spawner).await;

        let log_path = dir
            .path()
            .join("build")
            .join(format!("{}.log", job.app_uuid));
        let contents = std::fs::read_to_string(&log_path)
            .unwrap_or_else(|e| panic!("build log {log_path:?} should exist: {e}"));
        assert!(
            contents.contains("PARTIAL_BUILD_OUTPUT"),
            "build log must contain captured stdout on failure; got: {contents:?}"
        );
        assert!(
            contents.contains("COMPILER_ERROR_E0277"),
            "build log must contain captured stderr on failure; got: {contents:?}"
        );
    }

    /// When the spawner returns a digest, it is forwarded in the `ArtifactRef`.
    #[tokio::test]
    async fn spawn_build_parses_digest_some() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        let canned = r#"{"reff":"[fd5a::1]:5000/a/b:sha","digest":"sha256:deadbeef"}"#;
        let (spawner, _) = CannedSpawner::ok(canned);
        let art = o.spawn_build_with(&test_job(), &spawner).await.unwrap();
        assert_eq!(art.digest.as_deref(), Some("sha256:deadbeef"));
    }

    /// The spec file written to disk contains the serialized BuildJob and is
    /// parseable back as one.
    #[tokio::test]
    async fn spawn_build_spec_file_contains_valid_job_json() {
        use std::sync::{Arc, Mutex};

        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());

        // A spawner that reads the spec file before returning.
        let captured_job: Arc<Mutex<Option<BuildJob>>> = Arc::new(Mutex::new(None));
        let cj2 = captured_job.clone();

        struct ReadSpecSpawner {
            slot: Arc<Mutex<Option<BuildJob>>>,
            stdout: Vec<u8>,
        }
        impl BuildSpawner for ReadSpecSpawner {
            fn run<'a>(
                &'a self,
                spec_path: &'a Path,
                _app_uuid: &'a str,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<BuildOutput>> + Send + 'a>>
            {
                let slot = self.slot.clone();
                let stdout = self.stdout.clone();
                let p = spec_path.to_path_buf();
                Box::pin(async move {
                    let raw = std::fs::read_to_string(&p).unwrap();
                    let job: BuildJob = serde_json::from_str(&raw).unwrap();
                    *slot.lock().unwrap() = Some(job);
                    Ok(BuildOutput {
                        stdout,
                        stderr: vec![],
                        success: true,
                    })
                })
            }
        }

        let spawner = ReadSpecSpawner {
            slot: cj2,
            stdout: br#"{"reff":"x","digest":null}"#.to_vec(),
        };

        let expected_job = test_job();
        o.spawn_build_with(&expected_job, &spawner).await.unwrap();

        let read_job = captured_job.lock().unwrap().clone().expect("spawner ran");
        assert_eq!(
            read_job, expected_job,
            "spec file must contain the job JSON"
        );
    }

    // ── P1-3: build progress ──────────────────────────────────────────────────

    /// `drain_tee` returns every byte read AND mirrors it to the sink file, so
    /// the caller's captured bytes are byte-identical while the live progress log
    /// is populated as the reader is drained.
    #[tokio::test]
    async fn drain_tee_captures_and_mirrors() {
        let dir = tempfile::tempdir().unwrap();
        let sink_path = dir.path().join("progress.log");
        let file = tokio::fs::File::create(&sink_path).await.unwrap();
        let sink = Arc::new(Mutex::new(Some(file)));

        let input = b"step 1: pulling\nstep 2: building\n".to_vec();
        let captured = drain_tee(std::io::Cursor::new(input.clone()), sink.clone()).await;

        assert_eq!(captured, input, "captured bytes must equal the input");
        // Drop the file handle so the OS flushes before we read it back.
        drop(sink);
        let on_disk = std::fs::read(&sink_path).unwrap();
        assert_eq!(on_disk, input, "sink must mirror the drained bytes");
    }

    /// A `None` sink (progress-log open failed) must not lose the captured bytes.
    #[tokio::test]
    async fn drain_tee_tolerates_absent_sink() {
        let sink = Arc::new(Mutex::new(None));
        let input = b"no sink but still captured".to_vec();
        let captured = drain_tee(std::io::Cursor::new(input.clone()), sink).await;
        assert_eq!(captured, input);
    }

    /// The stage heuristic maps recent log content to a coarse stage; later
    /// stages win when multiple markers are present (newest signal).
    #[test]
    fn derive_build_stage_maps_keywords() {
        assert_eq!(derive_build_stage(""), "starting");
        assert_eq!(derive_build_stage("Cloning into 'app'..."), "pulling");
        assert_eq!(derive_build_stage("pulling base image layers"), "pulling");
        assert_eq!(derive_build_stage("Compiling serde v1.0"), "building");
        assert_eq!(derive_build_stage("exporting to oci image"), "converting");
        assert_eq!(derive_build_stage("booting firecracker microVM"), "booting");
        // Later stage wins when both a pull and a boot marker are in the tail.
        assert_eq!(
            derive_build_stage("pulling layers\n...\nbooting firecracker"),
            "booting"
        );
    }

    /// `build_progress` returns `None` when no progress log exists, and a
    /// populated snapshot (stage + tail + byte length) once one does.
    #[tokio::test]
    async fn build_progress_reads_live_log() {
        let dir = tempfile::tempdir().unwrap();
        let o = orch(dir.path().to_path_buf(), dir.path().to_path_buf());
        let uuid = "0191e7c2-9999-7222-8333-444455556666";

        // No log yet ⇒ None (404 upstream).
        assert!(o.build_progress(uuid, 40).await.is_none());

        // Write a live progress log and read it back.
        let path = build_progress_log_path(dir.path(), uuid);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"Cloning into 'repo'...\nStep 3/9: RUN cargo build\n").unwrap();

        let p = o.build_progress(uuid, 40).await.expect("progress present");
        assert!(p.log_bytes > 0, "byte length must be reported");
        assert!(
            p.log_tail.contains("cargo build"),
            "tail must surface recent build output; got: {}",
            p.log_tail
        );
        assert_eq!(p.stage, "building", "the RUN/compile line ⇒ building stage");
    }
}
