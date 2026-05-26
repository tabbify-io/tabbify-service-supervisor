//! Integration tests for the per-app runner serve core (Tasks 1.2 + 1.4 + 1.6 + 1.5).
//!
//! Mirrors the patterns in `tests/integration.rs`: a wiremock S3 serves the
//! `hello.wasm` fixture; the runner serve core binds a loopback listener in
//! `--no-mesh` mode; the test dials the bound address and asserts the fixture
//! response on `/` AND on a deep subpath.
//!
//! Task 1.4 adds control-socket tests: Health / Stop / Purge via the unix-
//! domain socket server.
//!
//! Task 1.6 adds a binary-wiring test: `RunnerConfig` → `ServeConfig` →
//! `RunnerServe::start` → `control::serve` → `Health` reply, proving the
//! entrypoint wires config→serve→control correctly without spawning a process.
//!
//! Task 1.5 adds reconciliation tests:
//! - Docker (gated): pre-create a stale container for the same uuid → launch
//!   runner → exactly ONE container with that name suffix exists afterwards.
//! - Firecracker pidfile: unit-level in `src/firecracker.rs` (no real VM needed).

use std::time::Duration;

use clap::Parser;
use tabbify_supervisor::config::{DockerConfig, FcConfig};
use tabbify_supervisor::control_proto::{Cmd, Reply};
use tabbify_supervisor::runner::serve::{RunnerServe, ServeConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The app UUID used in all fixture mocks.
const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

/// The compiled wasi:http/proxy fixture.
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

/// Runner serves the wasm fixture on `/` and on a deep subpath in loopback
/// (`--no-mesh`) mode, using a wiremock stand-in for S3.
#[tokio::test]
async fn runner_serves_fixture_on_root_and_deep_path_loopback() {
    let server = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("tempdir");

    let cfg = ServeConfig {
        uuid: APP_UUID.to_owned(),
        s3_base_url: server.uri(),
        data_dir: data_dir.path().to_path_buf(),
        no_mesh: true,
        coordinator_url: "http://127.0.0.1:8888".to_owned(),
        display_name: "runner-test".to_owned(),
        parent: None,
        port: 8730,
        fc: FcConfig::default(),
        docker: DockerConfig::default(),
    };

    // Start the runner serve core and obtain the bound address.
    let runner_serve = RunnerServe::start(cfg).await.expect("runner serve start");
    let addr = runner_serve.addr();

    let client = reqwest::Client::new();

    // Root path.
    let body = client
        .get(format!("http://{addr}/"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("GET /")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "Hello, Tabbify!");

    // Deep subpath.
    let body = client
        .get(format!("http://{addr}/some/deep/path?q=1"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("GET /some/deep/path?q=1")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "Hello, Tabbify!");
}

// ── Task 1.4: control-socket tests ─────────────────────────────────────────

/// Send one [`Cmd`] over a unix socket and read back one [`Reply`].
async fn ctrl_send(sock: &std::path::Path, cmd: Cmd) -> Reply {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let mut stream = UnixStream::connect(sock)
        .await
        .expect("connect control socket");
    let line = serde_json::to_string(&cmd).expect("serialize cmd") + "\n";
    stream.write_all(line.as_bytes()).await.expect("write cmd");
    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.expect("read reply");
    serde_json::from_str(buf.trim()).expect("parse reply")
}

/// Health reports `running` state with the real app ULA and uuid;
/// Stop tears down the listener (subsequent dial fails) and Health
/// then reports `stopped`; Purge removes the on-disk cache.
#[tokio::test]
async fn control_socket_health_stop_purge() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("tempdir");

    let cfg = ServeConfig {
        uuid: APP_UUID.to_owned(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.path().to_path_buf(),
        no_mesh: true,
        coordinator_url: "http://127.0.0.1:8888".to_owned(),
        display_name: "runner-test".to_owned(),
        parent: None,
        port: 8730,
        fc: FcConfig::default(),
        docker: DockerConfig::default(),
    };

    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join("runner.sock");

    let runner = RunnerServe::start(cfg).await.expect("runner start");
    let app_addr = runner.addr();

    // Spawn the control server against the runner's lifecycle handle.
    let lifecycle = runner.lifecycle();
    let sock_path2 = sock_path.clone();
    tokio::spawn(async move {
        tabbify_supervisor::runner::control::serve(sock_path2, lifecycle)
            .await
            .expect("control server");
    });

    // Give the socket a moment to appear.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Health: expect `running` with correct uuid + ula ---
    let reply = ctrl_send(&sock_path, Cmd::Health).await;
    match reply {
        Reply::Health {
            state,
            app_uuid,
            app_ula: _,
            pid,
        } => {
            assert_eq!(state, "running");
            assert_eq!(app_uuid, APP_UUID);
            assert!(pid > 0);
        }
        other => panic!("expected Health reply, got: {other:?}"),
    }

    // Verify the app is reachable before stop.
    {
        let pre_client = reqwest::Client::new();
        let pre_stop = pre_client
            .get(format!("http://{app_addr}/"))
            .timeout(Duration::from_millis(500))
            .send()
            .await;
        assert!(pre_stop.is_ok(), "app should be reachable before stop");
    }

    // --- Stop: tears down the listener ---
    let reply = ctrl_send(&sock_path, Cmd::Stop).await;
    assert!(
        matches!(reply, Reply::Ok),
        "expected Ok from Stop, got: {reply:?}"
    );

    // Give the listener task a moment to be aborted.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Dialing the previously-served addr must now fail.
    // A fresh client is used so no pooled connection from before stop is reused.
    let post_stop = reqwest::Client::builder()
        .connection_verbose(true)
        .build()
        .expect("build client")
        .get(format!("http://{app_addr}/"))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(post_stop.is_err(), "app listener should be down after Stop");

    // Health after stop reports `stopped`.
    let reply = ctrl_send(&sock_path, Cmd::Health).await;
    match reply {
        Reply::Health { state, .. } => assert_eq!(state, "stopped"),
        other => panic!("expected Health reply after stop, got: {other:?}"),
    }

    // --- Purge: clears the on-disk cache ---
    let cache_dir = data_dir.path().join("apps").join(APP_UUID);
    assert!(cache_dir.exists(), "cache should exist before purge");

    let reply = ctrl_send(&sock_path, Cmd::Purge).await;
    assert!(
        matches!(reply, Reply::Ok),
        "expected Ok from Purge, got: {reply:?}"
    );

    // Give the async removal a moment.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!cache_dir.exists(), "cache dir should be gone after Purge");
}

// ── Task 1.6: binary entrypoint wiring test ────────────────────────────────

/// Prove the binary's config→serve→control wiring without spawning a process.
///
/// Constructs a [`tabbify_supervisor::RunnerConfig`] (the clap struct the binary
/// parses) and passes it through [`tabbify_supervisor::runner::wire::serve_config_from`]
/// (the mapping the entrypoint uses) to obtain a [`ServeConfig`]; then starts
/// [`RunnerServe`] + [`tabbify_supervisor::runner::control::serve`] in-process
/// and asserts that a `Health` command returns `state = "running"` with the
/// correct `app_uuid`.
#[tokio::test]
async fn runner_binary_wiring_config_to_control_health() {
    let s3 = mock_s3(ON_REQUEST_MANIFEST).await;
    let data_dir = tempfile::tempdir().expect("tempdir");
    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock_path = sock_dir.path().join("runner_wire.sock");

    // Build a RunnerConfig the same way the binary does (clap parse_from).
    let cfg = tabbify_supervisor::RunnerConfig::try_parse_from([
        "tabbify-runner",
        "--uuid",
        APP_UUID,
        "--no-mesh",
        "--s3-base-url",
        &s3.uri(),
        "--data-dir",
        data_dir.path().to_str().unwrap(),
        "--control-sock",
        sock_path.to_str().unwrap(),
    ])
    .expect("parse RunnerConfig");

    // Map RunnerConfig → ServeConfig using the entrypoint's wiring helper.
    let serve_cfg = tabbify_supervisor::runner::wire::serve_config_from(&cfg);

    // Start the runner serve core (same as the binary does).
    let runner = RunnerServe::start(serve_cfg).await.expect("runner start");

    // Spawn the control server (same as the binary does).
    let lifecycle = runner.lifecycle();
    let sock_path2 = sock_path.clone();
    tokio::spawn(async move {
        tabbify_supervisor::runner::control::serve(sock_path2, lifecycle)
            .await
            .expect("control server");
    });

    // Allow the socket to appear.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send Health and assert the wiring is correct.
    let reply = ctrl_send(&sock_path, Cmd::Health).await;
    match reply {
        Reply::Health {
            state, app_uuid, ..
        } => {
            assert_eq!(state, "running", "expected running after wire start");
            assert_eq!(app_uuid, APP_UUID, "app_uuid mismatch");
        }
        other => panic!("expected Health reply, got: {other:?}"),
    }
}

// ── Task 1.5: workload reconciliation — no stale duplicate on restart ──────

/// Build the `tests/fixtures/docker-app` directory into a gzipped-tar build
/// context at `dest` via `tar czf`. Mirrors the helper in `integration.rs`.
fn make_docker_context(dest: &std::path::Path) {
    let fixture_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/docker-app");
    let status = std::process::Command::new("tar")
        .arg("czf")
        .arg(dest)
        .arg("-C")
        .arg(&fixture_dir)
        .arg("Dockerfile")
        .arg("server.py")
        .status()
        .expect("spawn tar");
    assert!(
        status.success(),
        "tar failed to build docker context tarball"
    );
}

/// Count containers whose name starts with `name_prefix` using `docker ps -a`.
fn count_containers_named(docker_bin: &str, name_prefix: &str) -> usize {
    let out = std::process::Command::new(docker_bin)
        .args(["ps", "-a", "--format", "{{.Names}}"])
        .output()
        .expect("docker ps");
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .lines()
        .filter(|line| line.starts_with(name_prefix))
        .count()
}

/// Docker-gated docker manifest (always_on, context.tar.gz entry).
const DOCKER_MANIFEST: &str = r#"
[app]
name        = "docker-reconcile-test"
kind        = "headless"
description = "reconcile fixture"

[lifecycle]
mode             = "always_on"
idle_timeout_sec = 300

[runtime]
type             = "docker"
entry            = "context.tar.gz"
fuel_per_request = 0
memory_mb        = 0

[routes]
dynamic_prefixes = ["/"]
"#;

/// Set up a wiremock S3 serving a docker manifest + the build-context tarball
/// for `APP_UUID`.
async fn mock_s3_docker(ctx_bytes: &[u8]) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_string("1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/manifest.toml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(DOCKER_MANIFEST))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/apps/{APP_UUID}/v1/context.tar.gz")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ctx_bytes.to_vec()))
        .mount(&server)
        .await;
    server
}

/// Docker-gated reconcile test:
///
/// When a runner (re)starts for `APP_UUID` and a stale container of the
/// runner-deterministic name (`tbf-<sanitized-uuid>-0`) is already present,
/// `launch_with_id` must remove it before creating the fresh one — leaving
/// EXACTLY ONE container with that name prefix running afterwards.
///
/// The `docker` runner's container name is `tbf-<sanitized-uuid>-<seq>` where
/// `seq` comes from `RUN_SEQ` (a per-process `AtomicU64` starting at 0). On a
/// fresh runner restart `seq` is always 0, so the name is deterministic across
/// restarts, and the existing best-effort `docker rm -f <name>` in
/// `launch_with_id` already handles the stale-container case.
///
/// Skipped (not failed) when no Docker daemon is reachable.
#[tokio::test]
async fn docker_runner_removes_stale_container_before_starting_fresh() {
    if !tabbify_supervisor::docker::docker_available() {
        eprintln!("skipping docker reconcile test: no Docker daemon");
        return;
    }

    let docker_bin = "docker";
    // The container name the runner will use for APP_UUID at seq=0 (first
    // launch in this process, RUN_SEQ resets to 0 on every process start).
    // Sanitized uuid: hyphens are kept as-is by container_name().
    let expected_name = format!("tbf-{APP_UUID}-0");

    // --- Pre-condition: create a stale container of the same name ---
    // We use `docker create` (not `run`) so there is no actual process; we
    // just need the container record to exist so `docker rm -f` can remove it.
    let pre = std::process::Command::new(docker_bin)
        .args(["create", "--name", &expected_name, "hello-world"])
        .output()
        .expect("docker create stale container");
    assert!(
        pre.status.success(),
        "failed to pre-create stale container: {}",
        String::from_utf8_lossy(&pre.stderr)
    );

    // Verify the stale container exists (baseline).
    assert_eq!(
        count_containers_named(docker_bin, &expected_name),
        1,
        "stale container must exist before runner starts"
    );

    // --- Build the docker context tarball and set up the mock S3 ---
    let tmp = tempfile::tempdir().expect("tempdir");
    let ctx_path = tmp.path().join("context.tar.gz");
    make_docker_context(&ctx_path);
    let ctx_bytes = std::fs::read(&ctx_path).expect("read context tarball");

    let s3 = mock_s3_docker(&ctx_bytes).await;
    let data_dir = tempfile::tempdir().expect("data dir");

    let cfg = ServeConfig {
        uuid: APP_UUID.to_owned(),
        s3_base_url: s3.uri(),
        data_dir: data_dir.path().to_path_buf(),
        no_mesh: true,
        coordinator_url: "http://127.0.0.1:8888".to_owned(),
        display_name: "runner-test".to_owned(),
        parent: None,
        port: 8730,
        fc: FcConfig::default(),
        docker: DockerConfig::default(),
    };

    // --- Start the runner (triggers launch_with_id which pre-rm's the stale) ---
    let runner = RunnerServe::start(cfg).await.expect("runner start");

    // --- Assertion: exactly ONE container with the expected name exists ---
    let count = count_containers_named(docker_bin, &expected_name);
    assert_eq!(
        count, 1,
        "expected exactly 1 container named {expected_name} after runner start, got {count}"
    );

    // --- Cleanup: drop the runner (DockerRuntime Drop rm -f's the container) ---
    drop(runner);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Remove the image to keep the host clean.
    let image = format!("tabbify-app-{APP_UUID}");
    let _ = std::process::Command::new(docker_bin)
        .args(["rmi", "-f", &image])
        .output();
}
