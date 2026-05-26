//! Integration test for the orchestrator's detached runner spawn (Task 2.2).
//!
//! This is THE correctness test for the per-app-runner resilience story: the
//! supervisor orchestrator spawns a REAL `tabbify-runner` process DETACHED, so
//! the runner (and its workload) outlives the supervisor. The test proves
//! detach directly: after dropping the spawner's child handle the runner must
//! STILL answer on its control socket — it is its own session leader, not a
//! child that dies when the parent lets go.
//!
//! Mirrors the harness in `tests/integration.rs` / `tests/runner_integration.rs`:
//! a wiremock S3 serves the committed `hello.wasm` fixture; the runner is
//! launched in `--no-mesh` loopback mode (no TUN / root needed); the test talks
//! to the runner over its unix-domain control socket using [`Cmd`] / [`Reply`].

use std::path::Path;
use std::time::Duration;

use tabbify_supervisor::control_proto::{Cmd, Reply};
use tabbify_supervisor::orchestrator::handle::record_path;
use tabbify_supervisor::orchestrator::spawn::{SpawnSpec, spawn_runner};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The app UUID used in all fixture mocks (matches the other integration tests).
const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

/// The deterministic per-app ULA for `APP_UUID` (golden value, see `app_ula`).
const APP_ULA: &str = "fd5a:1f02:44a5:240b:121a::1";

/// The committed pure-proxy fixture (compiled wasi:http/proxy component).
const HELLO_WASM: &[u8] = include_bytes!("fixtures/hello.wasm");

/// `wasm-http` manifest for the fixture (mirrors the one in `integration.rs`).
const ON_REQUEST_MANIFEST: &str = r#"
[app]
name        = "hello-tabbify"
kind        = "headless"
description = "fixture"

[lifecycle]
mode             = "on_request"
idle_timeout_sec = 300

[runtime]
type             = "wasm-http"
entry            = "app.wasm"
fuel_per_request = 1000000000
memory_mb        = 64

[routes]
dynamic_prefixes = ["/"]
"#;

/// Stand up a wiremock S3 serving `latest`, `manifest.toml`, and `app.wasm`
/// for `APP_UUID` at version 1 with the given manifest body.
async fn mock_s3(manifest: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/app.wasm")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(HELLO_WASM.to_vec()))
        .mount(&server)
        .await;
    server
}

/// Connect to the control socket, send one [`Cmd`], read back one [`Reply`].
///
/// Returns `Err` if the socket is not (yet) connectable or the round-trip fails
/// — callers use this both to poll for readiness and to issue real commands.
async fn ctrl_try(sock: &Path, cmd: &Cmd) -> std::io::Result<Reply> {
    let mut stream = UnixStream::connect(sock).await?;
    let line = serde_json::to_string(cmd).expect("serialize cmd") + "\n";
    stream.write_all(line.as_bytes()).await?;
    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    serde_json::from_str(buf.trim())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Poll the control socket with `Health` until it answers or `timeout` elapses.
async fn wait_health(sock: &Path, timeout: Duration) -> Reply {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_err = None;
    while std::time::Instant::now() < deadline {
        match ctrl_try(sock, &Cmd::Health).await {
            Ok(reply) => return reply,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("runner control socket never became reachable: {last_err:?}");
}

/// Force-kill `pid` (best-effort, no-op if already gone). Used as the test's
/// last-resort cleanup so a panic before the graceful `Shutdown` can never leak
/// a real detached process. Signal delivery is synchronous and needs no async
/// runtime, so it is safe to run from a `Drop` guard.
fn force_kill(pid: u32) {
    // SAFETY: `kill(2)` is a standard POSIX syscall; SIGKILL to a (possibly
    // already-dead) pid is harmless. PID reuse is not a concern in this short
    // test window.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

/// THE detach test: spawn a real `tabbify-runner` detached, prove it is
/// reachable, DROP the spawner's child handle, and prove it is STILL reachable
/// (it is its own session leader, so letting go of the child does not kill it).
#[tokio::test]
async fn spawn_runner_detaches_persists_record_and_survives_handle_drop() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    let spec = SpawnSpec {
        // The cargo-built `tabbify-runner` binary (set for integration tests).
        runner_bin: env!("CARGO_BIN_EXE_tabbify-runner").into(),
        uuid: APP_UUID.to_owned(),
        control_sock: sock_path.clone(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.path().to_path_buf(),
        parent: Some("fd5a:1f00:1::1".to_owned()),
        no_mesh: true,
    };

    // Spawn DETACHED. We keep the child handle only so we can drop it below.
    let (handle, child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    // Cleanup guard: whatever happens (incl. panic), do not leak the detached
    // process. SIGKILL on the recorded pid is synchronous, so it's safe in Drop.
    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    // --- The handle record is persisted at <runner_dir>/<uuid>.json ---
    let rec_path = record_path(runner_dir.path(), APP_UUID);
    assert!(
        rec_path.exists(),
        "runner handle record must be persisted at {}",
        rec_path.display()
    );
    assert_eq!(handle.uuid, APP_UUID);
    assert_eq!(handle.control_sock, sock_path);
    assert_eq!(handle.app_ula, APP_ULA, "handle must carry the derived ULA");
    assert_eq!(handle.parent.as_deref(), Some("fd5a:1f00:1::1"));
    assert!(handle.pid > 0, "handle must record the child pid");

    // The on-disk record round-trips back to the same handle.
    let loaded = tabbify_supervisor::orchestrator::RunnerHandle::load(runner_dir.path(), APP_UUID)
        .expect("load record")
        .expect("record present");
    assert_eq!(
        loaded, handle,
        "persisted record must match returned handle"
    );

    // --- The runner becomes reachable on its control socket ---
    let reply = wait_health(&sock_path, Duration::from_secs(20)).await;
    match reply {
        Reply::Health {
            state,
            app_uuid,
            app_ula,
            pid,
        } => {
            assert_eq!(state, "running", "runner should be running after spawn");
            assert_eq!(app_uuid, APP_UUID);
            assert_eq!(app_ula, APP_ULA);
            assert_eq!(
                pid, handle.pid,
                "runner-reported pid must equal the spawned child's pid"
            );
        }
        other => panic!("expected Health reply, got: {other:?}"),
    }

    // --- THE DETACH PROOF: drop the spawner's child handle, then assert the
    // runner is STILL reachable. If the runner were a non-detached child it
    // would be reaped / killed when its parent handle goes away; because it
    // called setsid() it is its own session leader and keeps running. ---
    drop(child);

    // Give any (incorrect) kill-on-drop a chance to take effect before probing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let reply = ctrl_try(&sock_path, &Cmd::Health)
        .await
        .expect("runner must STILL answer after the child handle is dropped (detach)");
    match reply {
        Reply::Health {
            state, app_uuid, ..
        } => {
            assert_eq!(state, "running", "detached runner should still be running");
            assert_eq!(app_uuid, APP_UUID);
        }
        other => panic!("expected Health after detach, got: {other:?}"),
    }

    // --- Clean up: shut the runner down explicitly (also covered by _guard). ---
    let reply = ctrl_try(&sock_path, &Cmd::Shutdown)
        .await
        .expect("shutdown reachable");
    assert!(matches!(reply, Reply::Ok), "expected Ok from Shutdown");

    // The process exits ~100ms after replying to Shutdown; the socket then dies.
    let gone = wait_unreachable(&sock_path, Duration::from_secs(5)).await;
    assert!(gone, "runner should become unreachable after Shutdown");
}

/// Poll until the control socket stops answering (the process exited).
async fn wait_unreachable(sock: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if ctrl_try(sock, &Cmd::Health).await.is_err() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Minimal RAII scope guard (avoids adding a dev-dependency for one use). Runs
/// `f` when the returned value is dropped — including on panic/unwind.
fn scopeguard(f: impl FnOnce()) -> impl Drop {
    struct Guard<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for Guard<F> {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                f();
            }
        }
    }
    Guard(Some(f))
}
