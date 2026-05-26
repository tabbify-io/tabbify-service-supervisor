//! Integration tests for the orchestrator — detached runner spawn (Task 2.2),
//! the control client (Task 2.3), and the monitor loop (Task 2.4).
//!
//! The Task 2.2 test (`spawn_runner_detaches_…`) is refactored to use the new
//! [`ControlClient`] everywhere instead of the hand-rolled socket helpers.
//! The Task 2.3 tests drive `health`, `stop`, and `purge` against a real spawned
//! runner through the client. The Task 2.4 test spawns a real runner, `kill -9`s
//! it, runs one monitor `tick`, and proves the orchestrator respawned it.

use std::path::Path;
use std::time::Duration;

use tabbify_supervisor::app_ula::derive_app_ula;
use tabbify_supervisor::control_proto::Reply;
use tabbify_supervisor::orchestrator::client::ControlClient;
use tabbify_supervisor::orchestrator::handle::{RunnerHandle, record_path};
use tabbify_supervisor::orchestrator::spawn::{SpawnSpec, spawn_runner};
use tabbify_supervisor::orchestrator::{Orchestrator, SharedRunnerConfig};
use uuid::Uuid;
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

/// Mount `latest`, `manifest.toml`, and `app.wasm` routes for `uuid` (version 1)
/// onto an already-running mock S3 server.
async fn mount_app(server: &MockServer, uuid: &str, manifest: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/apps/{uuid}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{uuid}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{uuid}/v1/app.wasm")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(HELLO_WASM.to_vec()))
        .mount(server)
        .await;
}

/// Stand up a wiremock S3 serving `latest`, `manifest.toml`, and `app.wasm`
/// for `APP_UUID` at version 1 with the given manifest body.
async fn mock_s3(manifest: &str) -> MockServer {
    let server = MockServer::start().await;
    mount_app(&server, APP_UUID, manifest).await;
    server
}

/// Build a standard [`SpawnSpec`] pointing at the cargo-built `tabbify-runner`
/// and the given wiremock S3.
fn make_spec(s3: &MockServer, sock_path: &Path, data_dir: &Path) -> SpawnSpec {
    SpawnSpec {
        runner_bin: env!("CARGO_BIN_EXE_tabbify-runner").into(),
        uuid: APP_UUID.to_owned(),
        control_sock: sock_path.to_path_buf(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.to_path_buf(),
        parent: Some("fd5a:1f00:1::1".to_owned()),
        no_mesh: true,
    }
}

/// Force-kill `pid` (best-effort, no-op if already gone). Used as the test's
/// last-resort cleanup so a panic before a graceful `Shutdown` can never leak
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

/// Wait until `client.health()` returns successfully or `timeout` elapses.
async fn wait_health(client: &ControlClient, timeout: Duration) -> Reply {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_err = None;
    while std::time::Instant::now() < deadline {
        match client.health().await {
            Ok(reply) => return reply,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("runner control socket never became reachable: {last_err:?}");
}

/// Wait until the control socket stops answering (the process exited).
async fn wait_unreachable(client: &ControlClient, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if client.ping().await.is_err() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

// ── Task 2.2: detach test (refactored to use ControlClient) ─────────────────

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

    let spec = make_spec(&s3, &sock_path, data_dir.path());

    // Spawn DETACHED. We keep the child handle only so we can drop it below.
    let (handle, child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    // Cleanup guard: whatever happens (incl. panic), do not leak the detached
    // process. SIGKILL on the recorded pid is synchronous, so it's safe in Drop.
    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    let client = ControlClient::new(&sock_path);

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
    let reply = wait_health(&client, Duration::from_secs(20)).await;
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

    let reply = client
        .health()
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
    let reply = client.shutdown().await.expect("shutdown reachable");
    assert!(matches!(reply, Reply::Ok), "expected Ok from Shutdown");

    // The process exits ~100ms after replying to Shutdown; the socket then dies.
    let gone = wait_unreachable(&client, Duration::from_secs(5)).await;
    assert!(gone, "runner should become unreachable after Shutdown");
}

// ── Task 2.3: ControlClient tests ───────────────────────────────────────────

/// `ControlClient::health` returns `Reply::Health { state: "running", … }` on
/// a freshly spawned runner.
#[tokio::test]
async fn client_health_reports_running() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    let spec = make_spec(&s3, &sock_path, data_dir.path());
    let (handle, _child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    let client = ControlClient::new(&sock_path);
    let reply = wait_health(&client, Duration::from_secs(20)).await;

    match reply {
        Reply::Health {
            state,
            app_uuid,
            app_ula,
            pid,
        } => {
            assert_eq!(state, "running");
            assert_eq!(app_uuid, APP_UUID);
            assert_eq!(app_ula, APP_ULA);
            assert_eq!(pid, handle.pid);
        }
        other => panic!("expected Health, got {other:?}"),
    }

    client.shutdown().await.ok();
}

/// `ControlClient::stop` makes a follow-up `health` report `state: "stopped"`.
#[tokio::test]
async fn client_stop_then_health_shows_stopped() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    let spec = make_spec(&s3, &sock_path, data_dir.path());
    let (handle, _child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    let client = ControlClient::new(&sock_path);

    // Wait until running.
    wait_health(&client, Duration::from_secs(20)).await;

    // Issue Stop.
    let stop_reply = client.stop().await.expect("stop command");
    assert!(matches!(stop_reply, Reply::Ok), "Stop should reply Ok");

    // Follow-up Health should show "stopped".
    let health = client.health().await.expect("health after stop");
    match health {
        Reply::Health { state, .. } => {
            assert_eq!(state, "stopped", "runner app listener should be stopped");
        }
        other => panic!("expected Health after stop, got {other:?}"),
    }

    // Shut down the runner process cleanly.
    client.shutdown().await.ok();
    wait_unreachable(&client, Duration::from_secs(5)).await;
}

/// `ControlClient::purge` causes the runner to exit (socket becomes unreachable)
/// and the on-disk artifact cache is cleared.
#[tokio::test]
async fn client_purge_exits_runner_and_clears_cache() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    let spec = make_spec(&s3, &sock_path, data_dir.path());
    let (handle, _child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    let client = ControlClient::new(&sock_path);

    // Wait until running (this also causes the runner to fetch+cache the artifact).
    wait_health(&client, Duration::from_secs(20)).await;

    // Issue Purge — the runner stops + clears cache + replies Ok.
    let purge_reply = client.purge().await.expect("purge command");
    assert!(matches!(purge_reply, Reply::Ok), "Purge should reply Ok");

    // After purge the runner's app listener is stopped; the runner process is
    // still alive (Purge does NOT exit the process — only Shutdown does that).
    // A follow-up Health should show "stopped".
    let health = client.health().await.expect("health after purge");
    match health {
        Reply::Health { state, .. } => {
            assert_eq!(
                state, "stopped",
                "runner app listener must be stopped after purge"
            );
        }
        other => panic!("expected Health after purge, got {other:?}"),
    }

    // Shut down the runner process cleanly.
    client.shutdown().await.ok();
    wait_unreachable(&client, Duration::from_secs(5)).await;
}

/// `ControlClient::ping` returns `Reply::Pong`.
#[tokio::test]
async fn client_ping_returns_pong() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    let spec = make_spec(&s3, &sock_path, data_dir.path());
    let (handle, _child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");

    let cleanup_pid = handle.pid;
    let _guard = scopeguard(move || force_kill(cleanup_pid));

    let client = ControlClient::new(&sock_path);

    // Wait until the runner is up first.
    wait_health(&client, Duration::from_secs(20)).await;

    let pong = client.ping().await.expect("ping");
    assert!(matches!(pong, Reply::Pong), "expected Pong, got {pong:?}");

    client.shutdown().await.ok();
    wait_unreachable(&client, Duration::from_secs(5)).await;
}

/// A dead socket returns a clear `Err`, not a hang.
#[tokio::test]
async fn client_dead_socket_returns_err() {
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let dead_sock = sock_dir.path().join("does-not-exist.sock");

    let client = ControlClient::new(&dead_sock);
    let result = client.health().await;
    assert!(
        result.is_err(),
        "dead socket must return Err, got: {result:?}"
    );
}

// ── Task 2.4: monitor loop + restart dead runners ───────────────────────────

/// Build the [`SharedRunnerConfig`] mirroring [`make_spec`]'s shared fields, so
/// the monitor reconstructs the exact same `SpawnSpec` from a record.
fn make_shared(s3: &MockServer, data_dir: &Path) -> SharedRunnerConfig {
    SharedRunnerConfig {
        runner_bin: env!("CARGO_BIN_EXE_tabbify-runner").into(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.to_path_buf(),
        parent: Some("fd5a:1f00:1::1".to_owned()),
        no_mesh: true,
    }
}

/// Wait until the runner record for `uuid` reports a pid OTHER than `old_pid`
/// (i.e. the monitor respawned it). Returns the new handle.
async fn wait_pid_changed(
    runner_dir: &Path,
    uuid: &str,
    old_pid: u32,
    timeout: Duration,
) -> RunnerHandle {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(h)) = RunnerHandle::load(runner_dir, uuid) {
            if h.pid != old_pid {
                return h;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("runner record pid never changed from {old_pid} (no respawn)");
}

/// THE self-healing test: spawn a real runner, `kill -9` it (simulate a crash),
/// run ONE monitor tick → the orchestrator detects it dead and RESPAWNS it: a
/// NEW process (different pid) on the SAME uuid + app_ula, reachable again on
/// its control socket. The updated record reflects the new pid.
#[tokio::test]
async fn monitor_tick_respawns_dead_runner() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join(format!("{APP_UUID}.sock"));

    // Spawn the original runner via the existing harness (same shared fields as
    // the SharedRunnerConfig below, so the monitor reconstructs an equal spec).
    let spec = make_spec(&s3, &sock_path, data_dir.path());
    let (handle, child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner");
    let original_pid = handle.pid;

    // Cleanup guard: kill BOTH the original and whatever the monitor respawns
    // (the respawned pid is recorded in the on-disk handle).
    let runner_dir_path = runner_dir.path().to_path_buf();
    let uuid = APP_UUID.to_owned();
    let _guard = scopeguard(move || {
        force_kill(original_pid);
        if let Ok(Some(h)) = RunnerHandle::load(&runner_dir_path, &uuid) {
            if h.pid != original_pid {
                force_kill(h.pid);
            }
        }
    });

    let client = ControlClient::new(&sock_path);

    // The original runner becomes reachable.
    let reply = wait_health(&client, Duration::from_secs(20)).await;
    assert!(
        matches!(reply, Reply::Health { ref state, .. } if state == "running"),
        "original runner should be running, got {reply:?}"
    );

    // --- SIMULATE A CRASH: kill -9 the runner and wait for it to disappear. ---
    // Drop the spawner's child handle first (the orchestrator does not retain
    // the `Child`; it monitors by pid + socket only). Then SIGKILL the pid.
    drop(child);
    force_kill(original_pid);
    let gone = wait_unreachable(&client, Duration::from_secs(10)).await;
    assert!(gone, "killed runner should become unreachable");

    // --- ONE MONITOR TICK → detect dead + respawn. ---
    let orch = Orchestrator::new(
        make_shared(&s3, data_dir.path()),
        runner_dir.path().to_path_buf(),
    );
    orch.tick().await.expect("monitor tick");

    // The record now reflects a NEW pid (different process).
    let new_handle = wait_pid_changed(
        runner_dir.path(),
        APP_UUID,
        original_pid,
        Duration::from_secs(10),
    )
    .await;
    assert_ne!(
        new_handle.pid, original_pid,
        "respawned runner must have a different pid"
    );
    assert_eq!(new_handle.uuid, APP_UUID, "uuid must be preserved");
    assert_eq!(
        new_handle.app_ula, APP_ULA,
        "respawned runner must keep the same deterministic app_ula"
    );
    assert_eq!(
        new_handle.control_sock, sock_path,
        "respawned runner must reuse the same control socket"
    );
    assert_eq!(
        new_handle.parent.as_deref(),
        Some("fd5a:1f00:1::1"),
        "respawned runner must keep the same parent"
    );

    // The respawned runner is reachable again on the SAME control socket, and
    // reports the new pid.
    let reply = wait_health(&client, Duration::from_secs(20)).await;
    match reply {
        Reply::Health {
            state,
            app_uuid,
            app_ula,
            pid,
        } => {
            assert_eq!(state, "running", "respawned runner should be running");
            assert_eq!(app_uuid, APP_UUID);
            assert_eq!(app_ula, APP_ULA);
            assert_eq!(
                pid, new_handle.pid,
                "runner-reported pid must equal the respawned record's pid"
            );
            assert_ne!(pid, original_pid, "must be a genuinely new process");
        }
        other => panic!("expected Health after respawn, got {other:?}"),
    }

    // Clean up: shut the respawned runner down.
    client.shutdown().await.ok();
    wait_unreachable(&client, Duration::from_secs(5)).await;
}

// ── Task 2.5: re-adopt living runners on supervisor restart ──────────────────

/// A second app UUID, used for the runner whose record is pre-seeded dead so
/// re-adopt must respawn it (distinct from the living `APP_UUID` runner).
const DEAD_UUID: &str = "0191e7c2-2222-7222-8333-444455556666";

/// THE re-adopt test: model "supervisor A died, supervisor B started".
///
/// Orchestrator A (here: the spawn harness) launches a real detached runner and
/// it becomes reachable. Then a FRESH Orchestrator B is constructed over the
/// SAME `runner_dir` — B never spawned anything, the on-disk records are its
/// only knowledge of the fleet. Additionally, a record with a DEAD pid is
/// pre-seeded.
///
/// `B.readopt()` must:
/// - ADOPT the living runner — its record pid is UNCHANGED and it is still the
///   same reachable process (re-spawning a healthy runner would kill+recreate
///   it, defeating the whole crash-survival property);
/// - RESPAWN the dead runner — a new pid, reachable on its socket.
#[tokio::test]
async fn readopt_adopts_living_runner_and_respawns_dead_one() {
    // One mock S3 serving BOTH apps.
    let s3 = MockServer::start().await;
    mount_app(&s3, APP_UUID, ON_REQUEST_MANIFEST).await;
    mount_app(&s3, DEAD_UUID, ON_REQUEST_MANIFEST).await;

    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let sock_dir = tempfile::tempdir().expect("sock dir");

    let live_sock = sock_dir.path().join(format!("{APP_UUID}.sock"));
    let dead_sock = sock_dir.path().join(format!("{DEAD_UUID}.sock"));

    // --- Orchestrator A: spawn the LIVING runner (real, detached). ---
    let spec = make_spec(&s3, &live_sock, data_dir.path());
    let (live_handle, live_child) = spawn_runner(&spec, runner_dir.path())
        .await
        .expect("spawn_runner (living)");
    let live_pid = live_handle.pid;

    // Cleanup guard: SIGKILL the living runner, the dead runner's bogus pid is
    // never alive, and whatever B respawns for DEAD_UUID (read from the record).
    let runner_dir_path = runner_dir.path().to_path_buf();
    let _guard = scopeguard(move || {
        force_kill(live_pid);
        if let Ok(Some(h)) = RunnerHandle::load(&runner_dir_path, DEAD_UUID) {
            force_kill(h.pid);
        }
    });

    let live_client = ControlClient::new(&live_sock);
    let dead_client = ControlClient::new(&dead_sock);

    // The living runner is reachable.
    let reply = wait_health(&live_client, Duration::from_secs(20)).await;
    assert!(
        matches!(reply, Reply::Health { ref state, .. } if state == "running"),
        "living runner should be running, got {reply:?}"
    );

    // --- Pre-seed a DEAD record: a bogus pid that is not a live process. ---
    // Drop the spawner's child for the living runner (B does not retain a Child;
    // it reconciles purely from the on-disk records + liveness probes).
    drop(live_child);
    let dead_handle = RunnerHandle {
        uuid: DEAD_UUID.to_owned(),
        // A pid that is essentially never alive in the test window. `readopt`
        // must treat the process as dead and respawn.
        pid: 2,
        control_sock: dead_sock.clone(),
        app_ula: derive_app_ula(Uuid::parse_str(DEAD_UUID).unwrap()).to_string(),
        parent: Some("fd5a:1f00:1::1".to_owned()),
    };
    dead_handle
        .save(runner_dir.path())
        .expect("seed dead record");
    let dead_seed_pid = dead_handle.pid;

    // --- Orchestrator B: FRESH, over the SAME runner_dir. Never spawned. ---
    let orch_b = Orchestrator::new(
        make_shared(&s3, data_dir.path()),
        runner_dir.path().to_path_buf(),
    );
    let summary = orch_b.readopt().await.expect("readopt");

    // --- The living runner is ADOPTED: pid UNCHANGED, still reachable. ---
    assert!(
        summary.adopted.contains(&APP_UUID.to_owned()),
        "living runner must be in the adopted set, got {summary:?}"
    );
    assert!(
        !summary.respawned.contains(&APP_UUID.to_owned()),
        "living runner must NOT be respawned, got {summary:?}"
    );
    let live_after = RunnerHandle::load(runner_dir.path(), APP_UUID)
        .expect("load living record")
        .expect("living record present");
    assert_eq!(
        live_after.pid, live_pid,
        "ADOPTED living runner must keep its ORIGINAL pid (not respawned)"
    );
    // And it is still the same reachable process reporting that same pid.
    let reply = live_client.health().await.expect("living runner reachable");
    match reply {
        Reply::Health { state, pid, .. } => {
            assert_eq!(state, "running", "adopted runner should still be running");
            assert_eq!(pid, live_pid, "adopted runner reports its original pid");
        }
        other => panic!("expected Health from adopted runner, got {other:?}"),
    }

    // --- The dead runner is RESPAWNED: new pid, reachable. ---
    assert!(
        summary.respawned.contains(&DEAD_UUID.to_owned()),
        "dead runner must be in the respawned set, got {summary:?}"
    );
    assert!(
        !summary.adopted.contains(&DEAD_UUID.to_owned()),
        "dead runner must NOT be adopted, got {summary:?}"
    );
    let dead_after = wait_pid_changed(
        runner_dir.path(),
        DEAD_UUID,
        dead_seed_pid,
        Duration::from_secs(10),
    )
    .await;
    assert_ne!(
        dead_after.pid, dead_seed_pid,
        "respawned dead runner must have a NEW pid"
    );
    assert_eq!(dead_after.uuid, DEAD_UUID, "uuid preserved across respawn");

    // The respawned runner is reachable on its own control socket.
    let reply = wait_health(&dead_client, Duration::from_secs(20)).await;
    match reply {
        Reply::Health {
            state,
            app_uuid,
            pid,
            ..
        } => {
            assert_eq!(state, "running", "respawned runner should be running");
            assert_eq!(app_uuid, DEAD_UUID);
            assert_eq!(
                pid, dead_after.pid,
                "runner-reported pid must equal the respawned record's pid"
            );
        }
        other => panic!("expected Health from respawned runner, got {other:?}"),
    }

    // --- Clean up both runners. ---
    live_client.shutdown().await.ok();
    dead_client.shutdown().await.ok();
    wait_unreachable(&live_client, Duration::from_secs(5)).await;
    wait_unreachable(&dead_client, Duration::from_secs(5)).await;
}
