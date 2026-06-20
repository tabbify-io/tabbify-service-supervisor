//! Unit tests for the workspace lifecycle handlers + registry. Split out of
//! `workspaces.rs` to keep that file under the 500-line house limit.
#![allow(clippy::unwrap_used)]

    use super::*;

    /// The stable workspace_uuid is a pure fn of user_id (frozen contract): same
    /// user → same uuid, distinct users → distinct. This is the re-key keystone.
    #[test]
    fn workspace_uuid_is_stable_per_user() {
        assert_eq!(
            workspace_uuid("acct_A").to_string(),
            workspace_uuid("acct_A").to_string(),
            "same user must re-key to the same workspace"
        );
        assert_ne!(
            workspace_uuid("acct_A").to_string(),
            workspace_uuid("acct_B").to_string(),
        );
    }

    /// The registry stores N caps for one workspace and returns them all.
    #[test]
    fn registry_holds_n_caps() {
        let reg = WorkspaceRegistry::default();
        reg.insert(Workspace {
            workspace_uuid: "ws-1".to_owned(),
            user_id: "u".to_owned(),
            caps: vec!["capA".to_owned(), "capB".to_owned()],
            created_at: Instant::now(),
            last_activity: Instant::now(),
        });
        let caps = reg.caps_of("ws-1").unwrap();
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&"capB".to_owned()));
        // remove returns the workspace; a second remove is None.
        assert!(reg.remove("ws-1").is_some());
        assert!(reg.remove("ws-1").is_none());
        assert_eq!(reg.len(), 0);
    }

    /// WORKSPACE_MAX_TTL is effectively infinite (spec §3 re-key #3): far beyond
    /// the dev-session 7d, so the safety reaper never reclaims a workspace.
    #[test]
    fn workspace_max_ttl_is_effectively_infinite() {
        assert!(
            WORKSPACE_MAX_TTL > Duration::from_secs(7 * 24 * 60 * 60),
            "workspace TTL must exceed the dev-session 7d ceiling"
        );
        assert!(
            WORKSPACE_MAX_TTL >= Duration::from_secs(10 * 365 * 24 * 60 * 60),
            "workspace TTL must be effectively infinite (≥10y)"
        );
    }

    /// The cap-file stem is sanitized + traversal-safe: it strips `.git`, keeps
    /// only safe chars, and never escapes the caps dir or empties (§12 S1).
    #[test]
    fn cap_repo_basename_is_safe_and_strips_git() {
        assert_eq!(cap_repo_basename("https://github.com/acme/app.git"), "app");
        assert_eq!(cap_repo_basename("https://github.com/acme/My-Repo"), "My-Repo");
        // A traversal attempt can never produce a path separator.
        assert!(!cap_repo_basename("../../etc/passwd").contains('/'));
        // Trailing slash + empty segment still yields a usable, non-empty stem.
        assert!(!cap_repo_basename("https://x/").is_empty());
    }
