//! Cap-value STABILITY across workspace ensures (the cost half of the
//! stale-caps fix).
//!
//! The rootfs cache key fingerprints cap VALUES (so a rotated secret re-bakes —
//! see `rootfs_env_fingerprint`'s invariant). That is only affordable because
//! an ensure that rotates NOTHING keeps every cap value byte-stable: this module
//! reuses the prior generation's tokens instead of re-minting per ensure.
//! Re-minting would turn every `workspace_ensure` into a fingerprint change →
//! a full ~2.3 GB rootfs rebuild → the latency/disk regression this host has
//! already been burned by.
//!
//! What is reused vs. minted:
//! - **repo caps** (`<stem>.url` git-proxy tokens): reused from the durable
//!   [`WorkspaceRecord`] by EXACT `repo_url` match (never by file-stem, which
//!   can collide across forges). The reused token is re-registered in
//!   `GitSessions` with the request's FRESH provider token/TTL, so reuse never
//!   extends a dead upstream credential.
//! - **`authkeys.cap`**: reused from the prior persisted runner env. It gates
//!   node→guest `:8732` add-key calls for THIS workspace only and lives exactly
//!   as long as the workspace; a per-ensure rotation added no security (nothing
//!   ever revoked the old one — the re-bake IS the rotation) and broke caching.
//! - **`forge-admin.token`**: NOT handled here — always request-supplied (auth
//!   is its source of truth), so it changes exactly when auth truly rotates it.

use std::collections::HashMap;

use crate::api::workspace_record::WorkspaceRecord;
use crate::api::workspaces::{AUTHKEYS_CAP_FILE, CAP_FILES_ENV};

/// Is `s` shaped like a `generate_cap` token (64 lowercase hex chars)? Reuse is
/// gated on this so a corrupt/legacy record can never smuggle an arbitrary
/// string back into the git-proxy session table or the baked cap-file.
#[must_use]
pub fn is_cap_token(s: &str) -> bool {
    s.len() == 64
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

/// The prior generation's git-proxy cap token for EXACTLY `repo_url`, from the
/// durable record. `None` (first ensure for this repo / no record / token not
/// cap-shaped) ⇒ the caller mints fresh.
#[must_use]
pub fn prior_repo_cap(prior_record: Option<&WorkspaceRecord>, repo_url: &str) -> Option<String> {
    prior_record?
        .caps
        .iter()
        .find(|c| c.repo_url == repo_url && is_cap_token(&c.cap))
        .map(|c| c.cap.clone())
}

/// The prior generation's `authkeys.cap` value, from the previously-persisted
/// runner env's [`CAP_FILES_ENV`] JSON map. `None` (no prior env / malformed
/// map / value not cap-shaped) ⇒ the caller mints fresh.
#[must_use]
pub fn prior_authkeys_cap(prior_extra_env: Option<&HashMap<String, String>>) -> Option<String> {
    let json = prior_extra_env?.get(CAP_FILES_ENV)?;
    let map: serde_json::Value = serde_json::from_str(json).ok()?;
    let value = map.as_object()?.get(AUTHKEYS_CAP_FILE)?.as_str()?;
    is_cap_token(value).then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::workspace_record::WorkspaceCap;

    const CAP_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CAP_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn record(caps: Vec<(&str, &str)>) -> WorkspaceRecord {
        WorkspaceRecord {
            workspace_uuid: "ws-1".to_owned(),
            user_id: "acct_1".to_owned(),
            caps: caps
                .into_iter()
                .map(|(cap, url)| WorkspaceCap {
                    cap: cap.to_owned(),
                    repo_url: url.to_owned(),
                })
                .collect(),
            branches: vec![],
            created_at_unix: 0,
            last_activity_unix: 0,
        }
    }

    /// STABILITY CORE: the same repo_url on a re-ensure reuses the prior cap
    /// token — the baked `<stem>.url` value stays byte-stable, the rootfs
    /// fingerprint does not churn, and the ensure stays a cache HIT.
    #[test]
    fn same_repo_url_reuses_prior_cap() {
        let rec = record(vec![(CAP_A, "https://forge/t_a/app.git")]);
        assert_eq!(
            prior_repo_cap(Some(&rec), "https://forge/t_a/app.git").as_deref(),
            Some(CAP_A)
        );
    }

    /// Match is by EXACT repo_url — a same-stem repo on a different forge/org
    /// must NOT inherit another repo's cap (a stem-match would hijack the other
    /// repo's git-proxy session on re-registration).
    #[test]
    fn different_repo_url_same_stem_mints_fresh() {
        let rec = record(vec![(CAP_A, "https://forge/t_a/app.git")]);
        assert_eq!(
            prior_repo_cap(Some(&rec), "https://forge/t_OTHER/app.git"),
            None
        );
    }

    /// No prior record / a non-cap-shaped stored token ⇒ mint fresh (never
    /// propagate corruption into GitSessions or the baked cap-file).
    #[test]
    fn missing_or_malformed_prior_cap_mints_fresh() {
        assert_eq!(prior_repo_cap(None, "https://forge/t_a/app.git"), None);
        let corrupt = record(vec![("not-a-cap-token", "https://forge/t_a/app.git")]);
        assert_eq!(
            prior_repo_cap(Some(&corrupt), "https://forge/t_a/app.git"),
            None
        );
        let upper = record(vec![(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "https://forge/t_a/app.git",
        )]);
        assert_eq!(
            prior_repo_cap(Some(&upper), "https://forge/t_a/app.git"),
            None
        );
    }

    /// STABILITY CORE: `authkeys.cap` survives across ensures via the persisted
    /// runner env, so the baked value (and thus the rootfs fingerprint) is
    /// stable for an ensure that rotates nothing.
    #[test]
    fn authkeys_cap_reused_from_prior_env() {
        let env = HashMap::from([(
            CAP_FILES_ENV.to_owned(),
            serde_json::json!({ AUTHKEYS_CAP_FILE: CAP_B }).to_string(),
        )]);
        assert_eq!(prior_authkeys_cap(Some(&env)).as_deref(), Some(CAP_B));
    }

    /// No prior env / no CAP_FILES_ENV / malformed JSON / non-cap value ⇒ mint.
    #[test]
    fn authkeys_cap_minted_when_prior_absent_or_malformed() {
        assert_eq!(prior_authkeys_cap(None), None);
        assert_eq!(prior_authkeys_cap(Some(&HashMap::new())), None);
        let malformed = HashMap::from([(CAP_FILES_ENV.to_owned(), "not json".to_owned())]);
        assert_eq!(prior_authkeys_cap(Some(&malformed)), None);
        let not_cap = HashMap::from([(
            CAP_FILES_ENV.to_owned(),
            serde_json::json!({ AUTHKEYS_CAP_FILE: "short" }).to_string(),
        )]);
        assert_eq!(prior_authkeys_cap(Some(&not_cap)), None);
    }

    /// The token-shape gate itself.
    #[test]
    fn cap_token_shape_is_64_lowercase_hex() {
        assert!(is_cap_token(CAP_A));
        assert!(!is_cap_token(""));
        assert!(!is_cap_token(&CAP_A[..63]));
        assert!(!is_cap_token(&format!("{}Z", &CAP_A[..63])));
        assert!(!is_cap_token(&CAP_A.to_uppercase()));
    }
}
