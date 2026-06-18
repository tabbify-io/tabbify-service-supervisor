//! Tests for [`super`] (dev_sessions) — cap generation, registry, idle reaper,
//! and B2: git_remote host_ip derivation correctness.
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
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
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
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
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

/// The max-ttl reaper also scrubs the on-disk dev-session sidecar (the third
/// teardown path, alongside delete + async-deploy-failure) so a later restart
/// cannot resurrect a reaped session.
#[tokio::test]
async fn sweep_removes_dev_session_record_sidecar() {
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let runner_dir = tmp.path().join("runners");
    std::fs::create_dir_all(&runner_dir).unwrap();
    let orchestrator = crate::orchestrator::Orchestrator::new(
        crate::orchestrator::SharedRunnerConfig {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: tmp.path().to_path_buf(),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        },
        runner_dir.clone(),
    );
    let fetcher = crate::fetcher::S3Fetcher::new("http://s3.invalid", tmp.path().to_path_buf());
    let state = Arc::new(crate::api::SupervisorState::new(
        orchestrator,
        fetcher,
        "test-supervisor".to_owned(),
        "::1".to_owned(),
    ));

    insert_session_aged(
        &state,
        "old-sess",
        "app-old",
        "cap-old",
        Duration::from_secs(120),
        Duration::from_secs(5),
    );
    crate::api::DevSessionRecord {
        session_id: "old-sess".to_owned(),
        app_uuid: "app-old".to_owned(),
        cap: "cap-old".to_owned(),
        repo_url: "https://github.com/acme/app.git".to_owned(),
        branch: "main".to_owned(),
        created_at_unix: crate::api::now_unix(),
        last_activity_unix: crate::api::now_unix(),
    }
    .save(&runner_dir)
    .unwrap();
    assert_eq!(
        crate::api::DevSessionRecord::list(&runner_dir).unwrap().len(),
        1
    );

    // max-ttl 60s < age 120s → reaped.
    let purged = sweep_expired(&state, Duration::from_secs(3600), Duration::from_secs(60)).await;
    assert_eq!(purged, vec!["old-sess".to_owned()]);
    assert!(
        crate::api::DevSessionRecord::list(&runner_dir)
            .unwrap()
            .is_empty(),
        "reaper must scrub the on-disk sidecar"
    );
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

// ── B2: git_remote host_ip correctness ───────────────────────────────────────

/// B2 (Linux-only): `derive_dev_fc_host_ip(uuid, image_ref, subnet)` must equal
/// the `host_ip` that `fc_identity_for_key("uuid:image_ref")` + `derive_link_ips`
/// produces — i.e. the same IP the FC launch will assign to the tap's host side,
/// which is what the guest sees as its default gateway.
///
/// `vm_key = format!("{uuid}:{reff}")` in the FC launch path (linux.rs); `reff`
/// is `runtime.registry_ref` = the OCI `image_ref` for a dev session. We hash
/// the SAME composite key.
///
/// This is the load-bearing correctness test: if `git_remote` points at the
/// wrong IP, the guest's `git clone` fails with a connection error.
#[cfg(target_os = "linux")]
#[test]
fn git_remote_host_ip_matches_fc_launch_host_ip() {
    use crate::api::GIT_PROXY_IPV4_PORT;
    use crate::config::DEFAULT_FC_TAP_SUBNET;
    use crate::firecracker::linux::{derive_link_ips, fc_identity_for_key};

    let app_uuid = "cc4bfba2-17a9-512d-b6f4-43f69114be65";
    let image_ref = "[fd5a::1]:5000/tabbify/devbox:latest";
    let subnet = DEFAULT_FC_TAP_SUBNET;
    // vm_key as constructed by launch_with_uuid (linux.rs ~315).
    let vm_key = format!("{app_uuid}:{image_ref}");

    // What the FC launch will derive.
    let (_, link_idx) = fc_identity_for_key(&vm_key);
    let (expected_host_ip, _) = derive_link_ips(subnet, link_idx).unwrap();

    // What derive_dev_fc_host_ip produces (used by create_dev_session).
    let derived = super::derive_dev_fc_host_ip(app_uuid, image_ref, subnet);

    assert_eq!(
        derived,
        expected_host_ip.to_string(),
        "git_remote host_ip must equal the FC tap's host_ip (the guest's default gateway)"
    );

    // PIN: the equality assert above recomputes the same chain on both sides, so
    // a key-format / derivation change would shift both together and pass
    // silently. Pin the LITERAL host_ip for this fixed (uuid, image_ref, default
    // subnet) so any such change fails here. Computed once from the real blake3
    // derivation: vm_key = "cc4bfba2-…:[fd5a::1]:5000/tabbify/devbox:latest" →
    // link_idx 394 → host 172.31.0.0 + 394*4 + 1 = 172.31.6.41.
    assert_eq!(
        derived, "172.31.6.41",
        "host_ip for the pinned (uuid, image_ref, 172.31.0.0/16) must be the baked literal; \
         a change here means the key-format or /30 derivation moved"
    );

    // The full git_remote URL must carry the correct host and port.
    let cap = "testcap1234";
    let git_remote = format!("http://{derived}:{GIT_PROXY_IPV4_PORT}/git/{cap}");
    assert!(
        git_remote.starts_with(&format!("http://{}:", expected_host_ip)),
        "git_remote must start with http://<host_ip>:<port>; got: {git_remote}"
    );
    assert!(
        git_remote.contains(&format!(":{GIT_PROXY_IPV4_PORT}/")),
        "git_remote must contain the IPv4 proxy port; got: {git_remote}"
    );
}

/// B2-cross-app: two distinct app UUIDs + same image_ref must produce distinct
/// host IPs (each gets its own /30 tap).
#[cfg(target_os = "linux")]
#[test]
fn git_remote_host_ip_distinct_for_distinct_uuids() {
    use crate::config::DEFAULT_FC_TAP_SUBNET;

    let uuid_a = "cc4bfba2-17a9-512d-b6f4-43f69114be65";
    let uuid_b = "78a254d8-77ab-5e0b-ac55-c95e0ce7f0c3";
    let image_ref = "[fd5a::1]:5000/tabbify/devbox:latest";
    let subnet = DEFAULT_FC_TAP_SUBNET;

    let ip_a = super::derive_dev_fc_host_ip(uuid_a, image_ref, subnet);
    let ip_b = super::derive_dev_fc_host_ip(uuid_b, image_ref, subnet);

    assert_ne!(
        ip_a, ip_b,
        "distinct app UUIDs must produce distinct host IPs (each gets its own /30)"
    );
}

/// B2-determinism: calling `derive_dev_fc_host_ip` twice with the same inputs
/// returns the same IP (stable identity, just like fc_identity_for_key).
#[cfg(target_os = "linux")]
#[test]
fn git_remote_host_ip_is_stable_per_uuid() {
    use crate::config::DEFAULT_FC_TAP_SUBNET;

    let uuid = "0191e7c2-1111-7222-8333-444455556666";
    let image_ref = "[fd5a::1]:5000/tabbify/devbox:latest";
    let subnet = DEFAULT_FC_TAP_SUBNET;

    let ip1 = super::derive_dev_fc_host_ip(uuid, image_ref, subnet);
    let ip2 = super::derive_dev_fc_host_ip(uuid, image_ref, subnet);
    assert_eq!(
        ip1, ip2,
        "host_ip derivation must be deterministic for the same inputs"
    );
}
