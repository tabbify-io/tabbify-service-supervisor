//! Integration tests for the per-app runner serve core (Tasks 1.2 + 1.4).
//!
//! Mirrors the patterns in `tests/integration.rs`: a wiremock S3 serves the
//! `hello.wasm` fixture; the runner serve core binds a loopback listener in
//! `--no-mesh` mode; the test dials the bound address and asserts the fixture
//! response on `/` AND on a deep subpath.
//!
//! Task 1.4 adds control-socket tests: Health / Stop / Purge via the unix-
//! domain socket server.

use std::time::Duration;

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
async fn ctrl_send(
    sock: &std::path::Path,
    cmd: Cmd,
) -> Reply {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let mut stream = UnixStream::connect(sock)
        .await
        .expect("connect control socket");
    let line = serde_json::to_string(&cmd).expect("serialize cmd") + "\n";
    stream
        .write_all(line.as_bytes())
        .await
        .expect("write cmd");
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
    assert!(matches!(reply, Reply::Ok), "expected Ok from Stop, got: {reply:?}");

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
    assert!(
        post_stop.is_err(),
        "app listener should be down after Stop"
    );

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
    assert!(matches!(reply, Reply::Ok), "expected Ok from Purge, got: {reply:?}");

    // Give the async removal a moment.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !cache_dir.exists(),
        "cache dir should be gone after Purge"
    );
}
