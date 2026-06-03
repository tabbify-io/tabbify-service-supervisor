//! Focused integration test for the `/openapi.json` endpoint on the supervisor
//! CONTROL router.
//!
//! Asserts that the LIVE router (not just `ApiDoc::openapi()` from a unit test)
//! serves a valid OpenAPI document with:
//! * `info.title == "tabbify-supervisor"`;
//! * every public HTTP handler documented under `paths`;
//! * customer-impactful endpoints (`POST /v1/build`, `GET /v1/apps`,
//!   `POST /v1/apps/{uuid}/deploy`) carry realistic examples;
//! * the Swagger UI route is reachable.
//!
//! Mirrors the reference pattern in `tabbify-service-node/tests/openapi_served.rs`
//! adapted to the supervisor's internal (no-auth) surface.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tabbify_supervisor::api::{SupervisorState, router};
use tabbify_supervisor::fetcher::S3Fetcher;
use tabbify_supervisor::orchestrator::{Orchestrator, SharedRunnerConfig};
use tower::ServiceExt;

/// Build a minimal no-mesh supervisor state over temp dirs. The `s3.invalid`
/// base URL is never dialed — `/openapi.json` is a static document built from
/// the `ApiDoc` derive.
fn make_state() -> (SupervisorState, tempfile::TempDir, tempfile::TempDir) {
    let data_dir = tempfile::tempdir().expect("data dir");
    let runner_dir = tempfile::tempdir().expect("runner dir");
    let shared = SharedRunnerConfig {
        runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
        s3_base_url: "http://s3.invalid".to_owned(),
        data_dir: data_dir.path().to_path_buf(),
        parent: None,
        no_mesh: true,
        relay_url: None,
    };
    let orchestrator = Orchestrator::new(shared, runner_dir.path().to_path_buf());
    let fetcher = S3Fetcher::new("http://s3.invalid", data_dir.path());
    let state = SupervisorState::new(
        orchestrator,
        fetcher,
        "test-supervisor".to_owned(),
        "fd5a:1f00:1::1".to_owned(),
    );
    (state, data_dir, runner_dir)
}

/// Drive one GET through the router and return (status, body bytes).
async fn fetch(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    (status, body)
}

#[tokio::test]
async fn openapi_json_served() {
    let (state, _data_dir, _runner_dir) = make_state();
    let app = router(state);

    let (status, body) = fetch(&app, "/openapi.json").await;
    assert_eq!(status, StatusCode::OK, "/openapi.json must be 200");

    let spec: Value =
        serde_json::from_slice(&body).expect("openapi.json must be valid JSON");

    // --- info block carries the service title -------------------------------
    assert_eq!(
        spec["info"]["title"].as_str(),
        Some("tabbify-supervisor"),
        "info.title must identify the service"
    );

    // --- every documented path is listed ------------------------------------
    let paths = spec["paths"]
        .as_object()
        .expect("paths must be an object");
    for expected in [
        "/health",
        "/v1/apps",
        "/v1/apps/{uuid}",
        "/v1/apps/{uuid}/start",
        "/v1/apps/{uuid}/stop",
        "/v1/apps/{uuid}/purge",
        "/v1/apps/{uuid}/reset",
        "/v1/apps/{uuid}/deploy",
        "/v1/build",
    ] {
        assert!(
            paths.contains_key(expected),
            "openapi.json missing path {expected}"
        );
    }

    // --- supervisor is internal: NO bearer scheme registered ----------------
    // (Unlike the node, the supervisor CONTROL API is peer-ULA bound and not
    // customer-facing — no Bearer scheme is configured.)
    let components = spec["components"].as_object();
    if let Some(components) = components {
        if let Some(schemes) = components.get("securitySchemes") {
            let schemes = schemes.as_object().expect("securitySchemes is an object");
            assert!(
                schemes.is_empty(),
                "supervisor must NOT register any security schemes (internal API)"
            );
        }
    }

    // --- customer-impactful endpoints carry response examples ---------------
    // `GET /v1/apps` → 200 response example present.
    let apps_200 = &paths["/v1/apps"]["get"]["responses"]["200"];
    let apps_example = &apps_200["content"]["application/json"]["example"];
    assert!(
        apps_example.is_object() || apps_example.is_array(),
        "/v1/apps 200 must carry a response example"
    );

    // `POST /v1/build` → request body example AND 200 response example present.
    let build = &paths["/v1/build"]["post"];
    assert!(
        build["requestBody"]["content"]["application/json"]["example"].is_object(),
        "/v1/build must carry a request body example"
    );
    assert!(
        build["responses"]["200"]["content"]["application/json"]["example"].is_object(),
        "/v1/build 200 must carry a response example"
    );

    // `POST /v1/apps/{uuid}/deploy` → request body + 200 response examples.
    let deploy = &paths["/v1/apps/{uuid}/deploy"]["post"];
    assert!(
        deploy["requestBody"]["content"]["application/json"]["example"].is_object(),
        "/v1/apps/{{uuid}}/deploy must carry a request body example"
    );
    assert!(
        deploy["responses"]["200"]["content"]["application/json"]["example"].is_object(),
        "/v1/apps/{{uuid}}/deploy 200 must carry a response example"
    );
}

#[tokio::test]
async fn swagger_ui_index_served() {
    let (state, _data_dir, _runner_dir) = make_state();
    let app = router(state);

    // SwaggerUi serves an HTML index at /swagger-ui (the redirect target is
    // /swagger-ui/index.html). Either 200 or a 301/302 redirect is acceptable —
    // what matters is that the route is wired (not 404 / 405).
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/swagger-ui/index.html")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status.is_redirection(),
        "/swagger-ui/index.html must be 2xx or 3xx, got {status}"
    );
}
