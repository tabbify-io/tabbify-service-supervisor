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

    /// `insert_forge_env` injects EXACTLY the two keys the in-FC broker's
    /// `ForgeCfg::from_env` requires — and omits each when `None` (an older node
    /// / no forge), so forge ops report an honest "forge not configured" instead
    /// of dialing a bogus endpoint. Neither key is snapshot-forbidden (they are
    /// non-secret; the creds ride the cap-file channel).
    #[test]
    fn insert_forge_env_injects_url_and_org_and_omits_when_none() {
        // No forge-proxy configured (gateway None) ⇒ the node URL passes through
        // unchanged (today's behavior).
        let mut env: HashMap<String, String> = HashMap::new();
        insert_forge_env(
            &mut env,
            &Some("http://[fd5a:1f02::1]:8730".to_owned()),
            &Some("t_acme".to_owned()),
            None,
        );
        assert_eq!(
            env.get("TABBIFY_FORGE_URL").unwrap(),
            "http://[fd5a:1f02::1]:8730"
        );
        assert_eq!(env.get("TABBIFY_FORGE_ORG").unwrap(), "t_acme");
        // The pair must survive the snapshot env-safety guard (it is baked into
        // /init and frozen into the workspace's Full snapshot by design).
        for key in crate::firecracker::snapshot_decision::snapshot_forbidden_env_keys() {
            assert!(!env.contains_key(*key), "{key} must not be injected");
        }

        // None ⇒ omitted (never an empty-string env the broker would misread).
        let mut empty: HashMap<String, String> = HashMap::new();
        insert_forge_env(&mut empty, &None, &None, None);
        assert!(!empty.contains_key("TABBIFY_FORGE_URL"));
        assert!(!empty.contains_key("TABBIFY_FORGE_ORG"));
    }

    /// FORGE-PROXY REWRITE: when the host-side forge-proxy is enabled the caller
    /// passes the guest's tap-gateway proxy URL, which REPLACES the node's raw v6
    /// mesh ULA in `TABBIFY_FORGE_URL` (the IPv4-only FC cannot route the ULA) —
    /// while `TABBIFY_FORGE_ORG` is passed through UNTOUCHED.
    #[test]
    fn insert_forge_env_rewrites_url_to_gateway_and_keeps_org() {
        let mut env: HashMap<String, String> = HashMap::new();
        let gateway = crate::api::forge_proxy_gateway_url("172.31.14.61");
        insert_forge_env(
            &mut env,
            // The RAW v6 ULA the node supplies — must NOT reach the guest.
            &Some("http://[fd5a:1f02:e3ca:25c7:1171::1]:8730".to_owned()),
            &Some("t_acme".to_owned()),
            Some(gateway.as_str()),
        );
        assert_eq!(
            env.get("TABBIFY_FORGE_URL").unwrap(),
            "http://172.31.14.61:8789",
            "the guest URL must be the tap-gateway proxy, not the raw v6 ULA",
        );
        assert_eq!(
            env.get("TABBIFY_FORGE_ORG").unwrap(),
            "t_acme",
            "the org slug must be passed through untouched (only the URL is rewritten)",
        );
    }

    /// Guard: a gateway is only meaningful WITH a node forge_url. No forge_url ⇒
    /// no `TABBIFY_FORGE_URL` even when a gateway is passed (no forge for this
    /// workspace → honest "unconfigured").
    #[test]
    fn insert_forge_env_no_url_means_no_key_even_with_gateway() {
        let mut env: HashMap<String, String> = HashMap::new();
        let gateway = crate::api::forge_proxy_gateway_url("172.31.14.61");
        insert_forge_env(&mut env, &None, &Some("t_acme".to_owned()), Some(&gateway));
        assert!(!env.contains_key("TABBIFY_FORGE_URL"));
        assert_eq!(env.get("TABBIFY_FORGE_ORG").unwrap(), "t_acme");
    }

    /// Rollout-order safety: a create body WITHOUT the forge_url/forge_org keys
    /// (an older node) deserializes with both `None`, and one WITH them carries
    /// the values — plain serde `#[serde(default)]`, no deny_unknown_fields.
    #[test]
    fn create_body_forge_fields_are_optional_and_carried() {
        let legacy: CreateWorkspaceBody = serde_json::from_value(serde_json::json!({
            "user_id": "acct_a",
            "image_ref": "reg/ws:latest",
            "repos": [],
            "authorized_key": "ssh-ed25519 AAAA node",
        }))
        .unwrap();
        assert!(legacy.forge_url.is_none());
        assert!(legacy.forge_org.is_none());

        let with_forge: CreateWorkspaceBody = serde_json::from_value(serde_json::json!({
            "user_id": "acct_a",
            "image_ref": "reg/ws:latest",
            "repos": [],
            "authorized_key": "ssh-ed25519 AAAA node",
            "forge_url": "http://[fd5a:1f02::1]:8730",
            "forge_org": "t_acme",
        }))
        .unwrap();
        assert_eq!(
            with_forge.forge_url.as_deref(),
            Some("http://[fd5a:1f02::1]:8730")
        );
        assert_eq!(with_forge.forge_org.as_deref(), Some("t_acme"));
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

    /// `append_cap` adds a repo's cap to the live in-mem registry WITHOUT dropping
    /// the existing caps (additive, mirrors the durable record append). A missing
    /// workspace is a no-op `false`.
    #[test]
    fn append_cap_is_additive_and_noops_on_missing() {
        let reg = WorkspaceRegistry::default();
        reg.insert(Workspace {
            workspace_uuid: "ws-1".to_owned(),
            user_id: "u".to_owned(),
            caps: vec!["capA".to_owned()],
            created_at: Instant::now(),
            last_activity: Instant::now(),
        });
        assert!(reg.append_cap("ws-1", "capB".to_owned()));
        let caps = reg.caps_of("ws-1").unwrap();
        assert_eq!(caps.len(), 2, "append must keep the existing cap");
        assert!(caps.contains(&"capA".to_owned()));
        assert!(caps.contains(&"capB".to_owned()));
        // Unknown workspace → false, no panic.
        assert!(!reg.append_cap("ws-missing", "capX".to_owned()));
    }

    /// `merge_cap_into_env` MERGES a new `<repo>.url` entry into the persisted
    /// `CAP_FILES_ENV` JSON map, preserving every existing cap-file (the
    /// authkeys/forge-admin/other-repo entries the respawn must re-bake). A
    /// record with no prior CAP_FILES_ENV starts a fresh single-entry map; all
    /// other env keys (marker, user-id, authorized-key) are untouched.
    #[test]
    fn merge_cap_into_env_preserves_existing_cap_files() {
        // Existing env: marker + a CAP_FILES_ENV holding repo-A + authkeys.
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert(crate::api::WORKSPACE_MARKER_ENV.to_owned(), "ws-1".to_owned());
        env.insert("TABBIFY_USER_ID".to_owned(), "acct_a".to_owned());
        env.insert(
            CAP_FILES_ENV.to_owned(),
            serde_json::json!({
                "app.url": "http://h:8788/git/capA",
                "authkeys.cap": "deadbeef",
            })
            .to_string(),
        );

        merge_cap_into_env(&mut env, "extra.url", "http://h:8788/git/capB");

        // Non-cap env keys are untouched.
        assert_eq!(env.get(crate::api::WORKSPACE_MARKER_ENV).unwrap(), "ws-1");
        assert_eq!(env.get("TABBIFY_USER_ID").unwrap(), "acct_a");
        // The cap-file map now has BOTH repos + the preserved authkeys cap.
        let cap_files: serde_json::Value =
            serde_json::from_str(env.get(CAP_FILES_ENV).unwrap()).unwrap();
        assert_eq!(cap_files["app.url"], "http://h:8788/git/capA");
        assert_eq!(cap_files["extra.url"], "http://h:8788/git/capB");
        assert_eq!(
            cap_files["authkeys.cap"], "deadbeef",
            "the authkeys cap must survive the merge"
        );
    }

    /// With no prior `CAP_FILES_ENV`, the merge creates a single-entry map.
    #[test]
    fn merge_cap_into_env_starts_fresh_map_when_absent() {
        let mut env: HashMap<String, String> = HashMap::new();
        merge_cap_into_env(&mut env, "extra.url", "http://h:8788/git/capB");
        let cap_files: serde_json::Value =
            serde_json::from_str(env.get(CAP_FILES_ENV).unwrap()).unwrap();
        assert_eq!(cap_files["extra.url"], "http://h:8788/git/capB");
        assert_eq!(
            cap_files.as_object().unwrap().len(),
            1,
            "a fresh map holds exactly the new entry"
        );
    }
