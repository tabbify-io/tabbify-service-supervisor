//! Integration tests for the per-app runner serve core (Task 1.2).
//!
//! Mirrors the patterns in `tests/integration.rs`: a wiremock S3 serves the
//! `hello.wasm` fixture; the runner serve core binds a loopback listener in
//! `--no-mesh` mode; the test dials the bound address and asserts the fixture
//! response on `/` AND on a deep subpath.

use std::time::Duration;

use tabbify_supervisor::config::{DockerConfig, FcConfig};
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
