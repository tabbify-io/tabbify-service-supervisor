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
        // A configured forge bakes the MANDATORY gateway (never the raw ULA) plus
        // the org slug.
        let mut env: HashMap<String, String> = HashMap::new();
        let gateway = crate::api::forge_proxy_gateway_url("172.31.14.61");
        insert_forge_env(
            &mut env,
            &Some("http://[fd5a:1f02::1]:8730".to_owned()),
            &Some("t_acme".to_owned()),
            Some(gateway.as_str()),
        )
        .expect("with a gateway it must succeed");
        assert_eq!(env.get("TABBIFY_FORGE_URL").unwrap(), "http://172.31.14.61:8789");
        assert_eq!(env.get("TABBIFY_FORGE_ORG").unwrap(), "t_acme");
        // The pair must survive the snapshot env-safety guard (it is baked into
        // /init and frozen into the workspace's Full snapshot by design).
        for key in crate::firecracker::snapshot_decision::snapshot_forbidden_env_keys() {
            assert!(!env.contains_key(*key), "{key} must not be injected");
        }

        // None ⇒ omitted (never an empty-string env the broker would misread).
        let mut empty: HashMap<String, String> = HashMap::new();
        insert_forge_env(&mut empty, &None, &None, None).expect("no forge = ok");
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
        )
        .expect("with a gateway it must succeed");
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
        insert_forge_env(&mut env, &None, &Some("t_acme".to_owned()), Some(&gateway))
            .expect("no forge_url = ok, org still set");
        assert!(!env.contains_key("TABBIFY_FORGE_URL"));
        assert_eq!(env.get("TABBIFY_FORGE_ORG").unwrap(), "t_acme");
    }

    /// Always-gateway guard: even when the node supplies a RAW v6 ULA, the baked
    /// `TABBIFY_FORGE_URL` is the host-side proxy gateway — the raw ULA must
    /// NEVER reach an IPv4-only FC (the exact #107 bug).
    #[test]
    fn insert_forge_env_always_uses_gateway_never_raw_ula() {
        let mut env = std::collections::HashMap::new();
        insert_forge_env(
            &mut env,
            &Some("http://[fd5a:1f02:e3ca:25c7:1171::1]:8730".to_owned()), // raw node ULA
            &Some("t_org".to_owned()),
            Some("http://172.31.14.61:8789"),
        )
        .expect("with a gateway it must succeed");
        assert_eq!(
            env.get("TABBIFY_FORGE_URL").unwrap(),
            "http://172.31.14.61:8789"
        );
        // The regression guard: the raw v6 ULA must NEVER be baked.
        assert!(!env.get("TABBIFY_FORGE_URL").unwrap().contains("fd5a:1f02"));
        assert_eq!(env.get("TABBIFY_FORGE_ORG").unwrap(), "t_org");
    }

    /// A forge is configured but the mandatory gateway is absent: refuse loudly
    /// (`Err(MissingGateway)`) rather than silently baking the raw ULA. Nothing
    /// is written to the env on the error path.
    #[test]
    fn insert_forge_env_errors_when_forge_set_but_no_gateway() {
        let mut env = std::collections::HashMap::new();
        let r = insert_forge_env(
            &mut env,
            &Some("http://[fd5a::1]:8730".to_owned()),
            &None,
            None,
        );
        assert!(matches!(r, Err(ForgeEnvError::MissingGateway)));
        assert!(!env.contains_key("TABBIFY_FORGE_URL")); // nothing baked
    }

    /// No forge configured at all ⇒ Ok with no key baked (an older node / a
    /// workspace with no forge → honest "unconfigured").
    #[test]
    fn insert_forge_env_noop_when_no_forge_configured() {
        let mut env = std::collections::HashMap::new();
        insert_forge_env(&mut env, &None, &None, None).expect("no forge = ok, no key");
        assert!(!env.contains_key("TABBIFY_FORGE_URL"));
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

    /// BACKFILL: `merge_cap_into_env` ALSO merges the reserved `forge-admin.token`
    /// entry (the add_repo forge-creds backfill), ADDING it to a workspace whose
    /// prior `CAP_FILES_ENV` had none while PRESERVING every repo `<stem>.url` +
    /// the authkeys cap. This is what lets the cold respawn re-bake the forge
    /// cap-file for a workspace provisioned before its forge org existed.
    #[test]
    fn merge_cap_into_env_adds_forge_admin_token_preserving_repos() {
        // Prior env: two repo caps + authkeys, but NO forge-admin.token (the bug).
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert(
            CAP_FILES_ENV.to_owned(),
            serde_json::json!({
                "app.url": "http://h:8788/git/capA",
                "tetris.url": "http://h:8788/git/capB",
                "authkeys.cap": "deadbeef",
            })
            .to_string(),
        );

        let creds = r#"{"org_slug":"t_a","owner_user":"t_a-bot","admin_token":"adm"}"#;
        merge_cap_into_env(&mut env, "forge-admin.token", creds);

        let cap_files: serde_json::Value =
            serde_json::from_str(env.get(CAP_FILES_ENV).unwrap()).unwrap();
        // The forge-admin token was ADDED (backfill) ...
        assert_eq!(
            cap_files["forge-admin.token"], creds,
            "the forge-admin creds must be merged in verbatim"
        );
        // ... while every prior repo cap + the authkeys cap survive untouched.
        assert_eq!(cap_files["app.url"], "http://h:8788/git/capA");
        assert_eq!(cap_files["tetris.url"], "http://h:8788/git/capB");
        assert_eq!(cap_files["authkeys.cap"], "deadbeef");
        assert_eq!(
            cap_files.as_object().unwrap().len(),
            4,
            "exactly: app.url + tetris.url + authkeys.cap + forge-admin.token"
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

    /// BUG-1 regression: `create_workspace` rebuilds `cap_files` FROM SCRATCH every
    /// call, so a repo added via `add_workspace_repo` (its `<repo>.url` lives ONLY
    /// in the prior persisted env, not in this request) would be CLOBBERED and the
    /// runner's cold re-bake would drop its clone. `preserve_prior_repo_caps` must
    /// carry every prior `*.url` entry forward, WITHOUT overwriting a fresh same-stem
    /// entry the request just produced, and WITHOUT resurrecting the re-minted
    /// authkeys/forge secrets.
    #[test]
    fn preserve_prior_repo_caps_carries_add_repo_url_alongside_fresh_caps() {
        // Freshly-built cap_files for THIS create: one request repo + freshly-minted
        // authkeys/forge secrets (exactly what create_workspace builds before the
        // preservation step).
        let mut cap_files = serde_json::Map::new();
        cap_files.insert(
            "app.url".to_owned(),
            serde_json::Value::String("http://h/git/FRESH_A".to_owned()),
        );
        cap_files.insert(
            AUTHKEYS_CAP_FILE.to_owned(),
            serde_json::Value::String("fresh-authkeys".to_owned()),
        );
        cap_files.insert(
            "forge-admin.token".to_owned(),
            serde_json::Value::String("fresh-forge".to_owned()),
        );

        // Prior persisted runner env: the SAME app repo (STALE cap), a tetris repo
        // added later via add_repo, and a STALE authkeys secret.
        let mut prior: HashMap<String, String> = HashMap::new();
        prior.insert(crate::api::WORKSPACE_MARKER_ENV.to_owned(), "ws-1".to_owned());
        prior.insert(
            CAP_FILES_ENV.to_owned(),
            serde_json::json!({
                "app.url": "http://h/git/STALE_A",
                "tetris.url": "http://h/git/TETRIS",
                "authkeys.cap": "stale-authkeys",
            })
            .to_string(),
        );

        preserve_prior_repo_caps(&mut cap_files, Some(&prior));

        // The add_repo URL is carried forward — the whole point of the fix.
        assert_eq!(
            cap_files["tetris.url"], "http://h/git/TETRIS",
            "add_repo cap-URL must survive a create re-provision"
        );
        // The request's fresh same-stem entry WINS over the stale prior.
        assert_eq!(
            cap_files["app.url"], "http://h/git/FRESH_A",
            "the request's fresh repo cap must win over the prior stale one"
        );
        // The re-minted secrets are NEVER resurrected from the prior.
        assert_eq!(
            cap_files[AUTHKEYS_CAP_FILE], "fresh-authkeys",
            "authkeys stays freshly minted (never carried from the prior)"
        );
        assert_eq!(
            cap_files["forge-admin.token"], "fresh-forge",
            "forge-admin token stays freshly minted (never carried from the prior)"
        );
        assert_eq!(
            cap_files.len(),
            4,
            "exactly: app.url + tetris.url + authkeys.cap + forge-admin.token"
        );
    }

    /// No prior record / no `CAP_FILES_ENV` ⇒ preservation is a no-op (create
    /// behaves exactly as before the fix).
    #[test]
    fn preserve_prior_repo_caps_noop_without_prior() {
        let mut cap_files = serde_json::Map::new();
        cap_files.insert(
            "app.url".to_owned(),
            serde_json::Value::String("http://h/git/A".to_owned()),
        );
        preserve_prior_repo_caps(&mut cap_files, None);
        assert_eq!(cap_files.len(), 1);
        assert_eq!(cap_files["app.url"], "http://h/git/A");

        // A prior env WITHOUT a CAP_FILES_ENV key is also a no-op.
        let mut prior: HashMap<String, String> = HashMap::new();
        prior.insert("TABBIFY_USER_ID".to_owned(), "acct".to_owned());
        preserve_prior_repo_caps(&mut cap_files, Some(&prior));
        assert_eq!(cap_files.len(), 1);
    }

    /// A malformed prior `CAP_FILES_ENV` (not a JSON object) carries NOTHING —
    /// corruption is never propagated into the freshly-built map.
    #[test]
    fn preserve_prior_repo_caps_ignores_malformed_prior() {
        let mut cap_files = serde_json::Map::new();
        cap_files.insert(
            "app.url".to_owned(),
            serde_json::Value::String("http://h/git/A".to_owned()),
        );
        let mut prior: HashMap<String, String> = HashMap::new();
        prior.insert(CAP_FILES_ENV.to_owned(), "not-json".to_owned());
        preserve_prior_repo_caps(&mut cap_files, Some(&prior));
        assert_eq!(cap_files.len(), 1, "a malformed prior contributes nothing");
    }

    /// BUG-1 COLD-safety: `create_workspace` rebuilds `record_caps`/`branches` from
    /// the request, so a repo added via `add_workspace_repo` (its cap row lives ONLY
    /// in the prior durable record) would be dropped — and a COLD readopt would then
    /// re-register only the request caps, ORPHANING the preserved `<repo>.url`.
    /// `preserve_prior_record_caps` must carry the prior non-request cap+branch rows
    /// forward, with the fresh request cap winning on a repo_url collision, keeping
    /// caps/branches index-parallel.
    #[test]
    fn preserve_prior_record_caps_carries_add_repo_rows() {
        // Fresh request record: one repo "app" with a FRESH cap.
        let mut record_caps = vec![crate::api::WorkspaceCap {
            cap: "fresh-app-cap".to_owned(),
            repo_url: "https://github.com/acme/app.git".to_owned(),
        }];
        let mut branches = vec!["main".to_owned()];

        // Prior durable record: the SAME app repo (STALE cap) + a tetris repo added
        // later via add_repo (branch "dev").
        let prior = crate::api::WorkspaceRecord {
            workspace_uuid: "ws-1".to_owned(),
            user_id: "acct".to_owned(),
            caps: vec![
                crate::api::WorkspaceCap {
                    cap: "stale-app-cap".to_owned(),
                    repo_url: "https://github.com/acme/app.git".to_owned(),
                },
                crate::api::WorkspaceCap {
                    cap: "tetris-cap".to_owned(),
                    repo_url: "https://forge/acme/tetris.git".to_owned(),
                },
            ],
            branches: vec!["main".to_owned(), "dev".to_owned()],
            created_at_unix: 0,
            last_activity_unix: 0,
        };

        preserve_prior_record_caps(&mut record_caps, &mut branches, Some(&prior));

        // The add_repo cap row is carried forward — the cold-safety fix.
        assert!(
            record_caps
                .iter()
                .any(|c| c.cap == "tetris-cap" && c.repo_url == "https://forge/acme/tetris.git"),
            "the add_repo WorkspaceCap must survive in the durable record"
        );
        // The request's fresh app cap WINS; the stale prior one is NOT carried.
        assert_eq!(
            record_caps
                .iter()
                .filter(|c| c.repo_url == "https://github.com/acme/app.git")
                .count(),
            1,
            "no duplicate row for a repo the request already carries"
        );
        assert!(record_caps.iter().any(|c| c.cap == "fresh-app-cap"));
        assert!(
            !record_caps.iter().any(|c| c.cap == "stale-app-cap"),
            "the request cap wins over the prior stale one"
        );
        // caps/branches stay parallel-by-index; tetris carried its "dev" branch.
        assert_eq!(
            record_caps.len(),
            branches.len(),
            "caps/branches stay index-parallel"
        );
        let tetris_idx = record_caps
            .iter()
            .position(|c| c.cap == "tetris-cap")
            .unwrap();
        assert_eq!(
            branches[tetris_idx], "dev",
            "the carried cap keeps its parallel branch"
        );
    }

    /// No prior record ⇒ record preservation is a no-op (create unchanged).
    #[test]
    fn preserve_prior_record_caps_noop_without_prior() {
        let mut record_caps = vec![crate::api::WorkspaceCap {
            cap: "c".to_owned(),
            repo_url: "u".to_owned(),
        }];
        let mut branches = vec!["main".to_owned()];
        preserve_prior_record_caps(&mut record_caps, &mut branches, None);
        assert_eq!(record_caps.len(), 1);
        assert_eq!(branches.len(), 1);
    }
