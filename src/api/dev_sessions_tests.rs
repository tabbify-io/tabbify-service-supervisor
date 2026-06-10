//! Tests for [`super`] (dev_sessions) — cap generation, registry, idle reaper.
//! Out-of-line module (`#[path]` in dev_sessions.rs) per the <500-line rule;
//! the HTTP-handler tests live in `crate::api::tests` (router oneshot harness).

use std::time::Duration;

use super::*;

// ── generate_cap ──────────────────────────────────────────────────────────────

#[test]
fn generate_cap_is_64_hex_chars() {
    let cap = generate_cap("sess-1", "app-1");
    assert_eq!(cap.len(), 64, "cap must be 64 hex chars; got {}", cap.len());
    assert!(
        cap.chars().all(|c| c.is_ascii_hexdigit()),
        "cap must be hex"
    );
}

#[test]
fn generate_cap_is_different_for_different_inputs() {
    let a = generate_cap("sess-a", "app-1");
    let b = generate_cap("sess-b", "app-1");
    assert_ne!(a, b, "distinct session ids must produce distinct caps");
}

#[test]
fn generate_cap_calls_are_non_deterministic() {
    // Two calls with the SAME inputs must still produce different caps
    // (each call mixes in fresh OS-CSPRNG v4 salts).
    let a = generate_cap("same-sess", "same-app");
    let b = generate_cap("same-sess", "same-app");
    assert_ne!(
        a, b,
        "repeated calls must not be deterministic (salt randomness)"
    );
}

// ── DevSessionRegistry ────────────────────────────────────────────────────────

fn make_session(id: &str, app: &str, cap: &str) -> DevSession {
    let now = Instant::now();
    DevSession {
        session_id: id.to_owned(),
        app_uuid: app.to_owned(),
        cap: cap.to_owned(),
        created_at: now,
        last_activity: now,
    }
}

#[test]
fn registry_insert_and_lookup() {
    let reg = DevSessionRegistry::default();
    reg.insert(make_session("s1", "a1", "cap1"));
    let result = reg.lookup("s1");
    assert!(result.is_some());
    let (app, cap) = result.unwrap();
    assert_eq!(app, "a1");
    assert_eq!(cap, "cap1");
}

#[test]
fn registry_remove_clears_entry() {
    let reg = DevSessionRegistry::default();
    reg.insert(make_session("s2", "a2", "cap2"));
    let removed = reg.remove("s2");
    assert!(removed.is_some());
    assert!(
        reg.lookup("s2").is_none(),
        "removed session must not be found"
    );
    assert_eq!(reg.len(), 0);
}

#[test]
fn registry_remove_unknown_returns_none() {
    let reg = DevSessionRegistry::default();
    assert!(reg.remove("no-such").is_none());
}

#[test]
fn registry_bump_activity_on_known_session() {
    let reg = DevSessionRegistry::default();
    reg.insert(make_session("s3", "a3", "cap3"));
    assert!(
        reg.bump_activity("s3"),
        "bump on known session must return true"
    );
}

#[test]
fn registry_bump_activity_on_unknown_returns_false() {
    let reg = DevSessionRegistry::default();
    assert!(!reg.bump_activity("no-such"));
}

#[test]
fn registry_snapshot_lists_all_sessions() {
    let reg = DevSessionRegistry::default();
    reg.insert(make_session("s4", "a4", "cap4"));
    reg.insert(make_session("s5", "a5", "cap5"));
    let snap = reg.snapshot();
    assert_eq!(snap.len(), 2);
}

// ── sweep_expired — unit-testable with short TTLs ─────────────────────────────

use std::path::PathBuf;

fn make_state_for_sweep() -> Arc<crate::api::SupervisorState> {
    let runner_dir = PathBuf::from("/tmp/dev-session-sweep-runners");
    let data_dir = PathBuf::from("/tmp/dev-session-sweep-data");
    let orchestrator = crate::orchestrator::Orchestrator::new(
        crate::orchestrator::SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: data_dir.clone(),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        },
        runner_dir,
    );
    let fetcher = crate::fetcher::S3Fetcher::new("http://s3.invalid", data_dir);
    Arc::new(crate::api::SupervisorState::new(
        orchestrator,
        fetcher,
        "test-supervisor".to_owned(),
        "::1".to_owned(),
    ))
}

fn insert_session_aged(
    state: &Arc<crate::api::SupervisorState>,
    id: &str,
    app: &str,
    cap: &str,
    age: Duration,
    idle: Duration,
) {
    let now = Instant::now();
    let session = DevSession {
        session_id: id.to_owned(),
        app_uuid: app.to_owned(),
        cap: cap.to_owned(),
        created_at: now - age,
        last_activity: now - idle,
    };
    state.dev_sessions.insert(session);
    // Also register a git cap so revoke has something to operate on.
    state.git_sessions.register(
        cap.to_owned(),
        GitSessionEntry {
            upstream_url: "http://upstream.invalid".to_owned(),
            token: "tok".to_owned(),
            expires_at: Instant::now() + Duration::from_secs(3600),
        },
    );
}

/// Idle-expired session is purged; fresh session survives.
#[tokio::test]
async fn sweep_removes_idle_expired_session() {
    let state = make_state_for_sweep();
    // idle = 2 min > short idle TTL of 1 min
    insert_session_aged(
        &state,
        "idle-sess",
        "app-idle",
        "cap-idle",
        Duration::from_secs(10),
        Duration::from_secs(120),
    );
    // fresh session: age = 5s, idle = 1s — well within any TTL
    insert_session_aged(
        &state,
        "fresh-sess",
        "app-fresh",
        "cap-fresh",
        Duration::from_secs(5),
        Duration::from_secs(1),
    );

    let purged = sweep_expired(&state, Duration::from_secs(60), Duration::from_secs(3600)).await;

    assert_eq!(purged, vec!["idle-sess".to_owned()]);
    assert!(
        state.dev_sessions.lookup("idle-sess").is_none(),
        "idle session must be removed"
    );
    assert!(
        state.dev_sessions.lookup("fresh-sess").is_some(),
        "fresh session must survive"
    );
}

/// Max-TTL-expired session is purged even if recently active.
#[tokio::test]
async fn sweep_removes_max_ttl_expired_session() {
    let state = make_state_for_sweep();
    // age = 2 min > short max TTL of 1 min; idle = 5s (still active)
    insert_session_aged(
        &state,
        "old-sess",
        "app-old",
        "cap-old",
        Duration::from_secs(120),
        Duration::from_secs(5),
    );

    let purged = sweep_expired(&state, Duration::from_secs(3600), Duration::from_secs(60)).await;

    assert_eq!(purged, vec!["old-sess".to_owned()]);
    assert!(state.dev_sessions.lookup("old-sess").is_none());
}

/// Both sessions expired: both purged.
#[tokio::test]
async fn sweep_removes_both_expired() {
    let state = make_state_for_sweep();
    insert_session_aged(
        &state,
        "e1",
        "app-e1",
        "cap-e1",
        Duration::from_secs(10),
        Duration::from_secs(120),
    );
    insert_session_aged(
        &state,
        "e2",
        "app-e2",
        "cap-e2",
        Duration::from_secs(10),
        Duration::from_secs(120),
    );

    let mut purged =
        sweep_expired(&state, Duration::from_secs(60), Duration::from_secs(3600)).await;
    purged.sort();

    assert_eq!(purged, vec!["e1".to_owned(), "e2".to_owned()]);
    assert_eq!(state.dev_sessions.len(), 0);
}

/// No expired sessions: nothing purged.
#[tokio::test]
async fn sweep_keeps_fresh_sessions() {
    let state = make_state_for_sweep();
    insert_session_aged(
        &state,
        "f1",
        "app-f1",
        "cap-f1",
        Duration::from_secs(5),
        Duration::from_secs(1),
    );
    insert_session_aged(
        &state,
        "f2",
        "app-f2",
        "cap-f2",
        Duration::from_secs(5),
        Duration::from_secs(1),
    );

    let purged = sweep_expired(&state, Duration::from_secs(3600), Duration::from_secs(7200)).await;

    assert!(purged.is_empty(), "no sessions must be purged");
    assert_eq!(state.dev_sessions.len(), 2);
}

/// After sweep, the git cap is revoked (proxy returns 403).
#[tokio::test]
async fn sweep_revokes_git_cap() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt as _;

    let state = make_state_for_sweep();
    insert_session_aged(
        &state,
        "cap-sess",
        "app-cap",
        "cap-to-revoke",
        Duration::from_secs(10),
        Duration::from_secs(120),
    );

    sweep_expired(&state, Duration::from_secs(60), Duration::from_secs(3600)).await;

    assert!(state.dev_sessions.lookup("cap-sess").is_none());

    // The revoked cap must 403 at the git proxy (lookup happens BEFORE any
    // upstream contact, so this never touches the network).
    let app = crate::api::router((*state).clone());
    let req = Request::builder()
        .method("GET")
        .uri("/git/cap-to-revoke/info/refs?service=git-upload-pack")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "swept session's cap must be revoked at the proxy"
    );
}
