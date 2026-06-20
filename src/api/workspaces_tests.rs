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

    /// §12 S6: `insert_authkeys_cap` writes the cap into the cap-file map under
    /// the reserved `authkeys.cap` name AND returns the SAME token, so the
    /// handler's `WorkspaceCreated.authkeys_cap` matches the value the runner
    /// writes 0600 broker-uid into the FC. The token must be unguessable (64 hex
    /// chars) and fresh on each create (no static secret).
    #[test]
    fn insert_authkeys_cap_writes_and_returns_matching_token() {
        let mut cap_files = serde_json::Map::new();
        let token = insert_authkeys_cap("ws-uuid-1", &mut cap_files);
        // Returned token == the value written into the cap-file map (so node's
        // bearer token matches the broker's cap-file).
        assert_eq!(
            cap_files.get(AUTHKEYS_CAP_FILE).and_then(|v| v.as_str()),
            Some(token.as_str()),
            "returned cap must equal the cap-file value the broker validates against"
        );
        // 64-hex blake3 token (unguessable, not a static/derivable secret).
        assert_eq!(token.len(), 64, "authkeys cap must be a 64-hex token");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        // Fresh per create — a second call yields a DIFFERENT token (CSPRNG salt).
        let mut cap_files2 = serde_json::Map::new();
        let token2 = insert_authkeys_cap("ws-uuid-1", &mut cap_files2);
        assert_ne!(token, token2, "each workspace-create mints a fresh cap");
    }
