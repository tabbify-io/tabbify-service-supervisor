//! Integration tests for the per-app runner serve core (Task 1.5: workload
//! reconciliation).
//!
//! The in-process WASM runtime was removed, so the WASM-served runner tests
//! (loopback serve, control socket, binary wiring) — which relied on the
//! committed WASM fixture as a hermetic app — were removed with it.
//! What remains is the docker-gated reconciliation test: it serves a docker
//! manifest from a wiremock S3 and exercises stale-container removal on restart.

use std::time::Duration;

use tabbify_supervisor::config::{DockerConfig, FcConfig};
use tabbify_supervisor::runner::serve::{RunnerServe, ServeConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The app UUID used in all fixture mocks.
const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";

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
        image_ref: None,
        runtime_override: None,
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
