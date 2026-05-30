//! SU-6 regression: a (simulated) supervisor self-update restart must ADOPT the
//! living runner fleet untouched — same pid, same record, same runner_dir.
//!
//! This pins spec invariant #2: the self-update swap touches ONLY the binary
//! symlink + VERSION ledger, never `runner_dir` / `data_dir` / mesh-identity.
//! A brand-new orchestrator over the same `runner_dir` (the binary swapped, the
//! data did not) must re-discover and adopt the living runner, leaving its pid
//! undisturbed. If this ever fails, the swap is mutating runner state.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::time::Duration;

use tabbify_supervisor::orchestrator::client::ControlClient;
use tabbify_supervisor::orchestrator::handle::RunnerHandle;
use tabbify_supervisor::orchestrator::spawn::{SpawnSpec, spawn_runner};
use tabbify_supervisor::orchestrator::{Orchestrator, SharedRunnerConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

/// The committed pure-proxy fixture (compiled wasi:http/proxy component).
const HELLO_WASM: &[u8] = include_bytes!("fixtures/hello.wasm");

/// `wasm-http` `on_request` manifest: the runner fetches it at startup, then
/// parks idle on its control socket (no eager workload), so its pid is stable
/// across the simulated restart.
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

/// Stand up a wiremock S3 serving the manifest + wasm for `APP_UUID` so the
/// runner starts cleanly (`RunnerServe::start` eagerly fetches the manifest).
async fn mock_s3() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(ON_REQUEST_MANIFEST))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/app.wasm")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(HELLO_WASM.to_vec()))
        .mount(&server)
        .await;
    server
}

/// Poll the runner's control socket until it answers a health probe (or the
/// deadline passes). A fixed sleep is racy under parallel test load — the
/// detached runner may not have bound its socket yet — so we wait for the
/// process to be genuinely reachable before simulating the restart.
async fn wait_until_reachable(sock: &std::path::Path, timeout: Duration) {
    let client = ControlClient::new(sock);
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if client.health().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("runner control socket {sock:?} never became reachable");
}

#[tokio::test]
async fn readopt_does_not_disturb_living_runner_across_simulated_restart() {
    let s3 = mock_s3().await;
    let tmp = tempfile::tempdir().unwrap();
    let runner_dir = tmp.path().join("runners");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&runner_dir).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();

    // Spawn a long-lived detached runner via the spawn seam directly. The
    // `on_request` wasm app parks idle on its control socket after fetching its
    // manifest, so the pid stays stable across the simulated restart.
    let control_sock = runner_dir.join("x.sock");
    let spec = SpawnSpec {
        runner_bin: PathBuf::from(env!("CARGO_BIN_EXE_tabbify-runner")),
        uuid: APP_UUID.to_owned(),
        control_sock: control_sock.clone(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.clone(),
        parent: None,
        no_mesh: true,
        image_ref: None,
        runtime_override: None,
    };
    let (handle, _child) = spawn_runner(&spec, &runner_dir).await.unwrap();
    let original_pid = handle.pid;

    // Wait until the detached runner is genuinely up and parked on its control
    // socket before simulating the restart (a fixed sleep is racy under parallel
    // test load).
    wait_until_reachable(&control_sock, Duration::from_secs(20)).await;

    // Simulate the self-update restart: a BRAND-NEW orchestrator over the SAME
    // runner_dir + data_dir (the binary swapped, the data did not).
    let orch = Orchestrator::new(
        SharedRunnerConfig {
            runner_bin: PathBuf::from(env!("CARGO_BIN_EXE_tabbify-runner")),
            s3_base_url: s3.uri(),
            data_dir,
            parent: None,
            no_mesh: true,
        },
        runner_dir.clone(),
    );
    let summary = orch.readopt().await.unwrap();

    assert!(
        summary.adopted.contains(&APP_UUID.to_owned()),
        "living runner must be ADOPTED, not respawned: {summary:?}"
    );
    assert!(
        !summary.respawned.contains(&APP_UUID.to_owned()),
        "living runner must NOT be respawned: {summary:?}"
    );

    // The record's pid is undisturbed by the restart.
    let rec = RunnerHandle::load(&runner_dir, APP_UUID)
        .unwrap()
        .expect("record present");
    assert_eq!(
        rec.pid, original_pid,
        "runner pid must survive the supervisor restart"
    );

    // Clean up the detached process.
    unsafe {
        libc::kill(original_pid as libc::pid_t, libc::SIGKILL);
    }
}
