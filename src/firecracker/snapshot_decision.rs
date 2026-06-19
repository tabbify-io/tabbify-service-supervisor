//! Pure, host-agnostic snapshot DECISION logic — the single source of truth for
//! "when does the cold boot create a snapshot?" and "what must be scrubbed
//! before a Full snapshot freezes RAM?".
//!
//! These functions are platform-independent (NO `cfg(target_os = "linux")`) so
//! they are UNIT-TESTABLE ON macOS — the rest of the firecracker snapshot path
//! lives behind the Linux gate and is only compile-checked under musl. The
//! Linux `cold_boot` calls [`should_snapshot_on_cold_boot`] instead of an inline
//! boolean so the policy is testable without a real VM/KVM (spec §8: "Snapshot-
//! decision функции мак-тестируемы").

/// Should the cold boot create a warm-start snapshot?
///
/// Policy (spec §3 re-key #4 + §6 «Контракт-правки» + §12 snapshot-timing):
/// - `files_present` — a snapshot already exists on disk. A plain restart finds
///   it present and must NOT re-snapshot (refresh is ONLY via `Cmd::Snapshot`).
/// - `suppressed` — the `.no-snapshot` marker. dev-sessions set it (async clone
///   races the boot snapshot, #58/#68). WORKSPACES ALSO set it (Task 9), but for
///   a DIFFERENT reason: `cold_boot`'s `wait_until_ready()` only waits for the
///   :8080 readiness shim, NOT for rust-analyzer to finish indexing (minutes), so
///   snapshotting here would freeze a COLD index into RAM and warm-restore that
///   cold index on every restart. The workspace's warm snapshot is taken LATER,
///   only by `Cmd::Snapshot` after the code-service signals «indexed && idle» —
///   and `FirecrackerRuntime::snapshot()` deliberately bypasses this marker
///   (suppress gates cold_boot ONLY).
///
/// Snapshot ⇔ no snapshot yet AND not suppressed. This is the cold-boot gate; it
/// deliberately does NOT depend on whether this is a workspace vs a dev-FC — the
/// suppression marker is the only differentiator, written by whichever spawn path
/// (dev-session OR workspace) needs to defer the snapshot.
#[must_use]
pub fn should_snapshot_on_cold_boot(files_present: bool, suppressed: bool) -> bool {
    !files_present && !suppressed
}

/// Environment-variable KEYS that MUST NOT be present in the guest's snapshotted
/// RAM/fs for a workspace (spec §4: "Снапшот = Full ... любой cap/токен/секрет в
/// env/RAM/fs на момент снапшота переживёт в каждый тёплый restore").
///
/// A workspace's git-proxy cap URL + any broker credential are injected OUTSIDE
/// the env channel — into a 0600 non-env file `/run/tabbify/caps/<repo>.url`
/// (§4/§12 S1, Task 9) — so these keys must simply be ABSENT from the boot
/// `extra_env`. [`extra_env_is_snapshot_safe`] enforces that **in the RUNNER
/// process** (`run_firecracker_build`, Task 9 — the point where `RUNNER_EXTRA_ENV`
/// is re-baked into the rootfs `/init`, i.e. where a leak would actually be
/// frozen), NOT in the API handler (which never re-bakes env and so cannot catch
/// a leak introduced by the runner). A regression (a cap leaking back into env)
/// fails the spawn loudly instead of being baked into every warm restore.
/// Returned as a slice so the caller can log exactly which key offended.
#[must_use]
pub const fn snapshot_forbidden_env_keys() -> &'static [&'static str] {
    &[
        // The dev-session git remote carried the cap in the URL — a workspace
        // must never bake it into env (the broker holds caps off-env).
        "TABBIFY_GIT_REMOTE",
        // Any raw provider/broker token.
        "TABBIFY_GIT_TOKEN",
        "TABBIFY_BROKER_TOKEN",
    ]
}

/// Is `extra_env` safe to freeze into a Full snapshot? `true` iff NONE of the
/// [`snapshot_forbidden_env_keys`] is present. The offending key (if any) is
/// returned so the caller can log it WITHOUT logging its value.
///
/// # Errors
/// Returns the forbidden key that was found (a `'static str` from the constant
/// list, so logging it never leaks a credential value).
pub fn extra_env_is_snapshot_safe<'a, I, K>(keys: I) -> Result<(), &'static str>
where
    I: IntoIterator<Item = &'a K>,
    K: AsRef<str> + 'a,
{
    let forbidden = snapshot_forbidden_env_keys();
    for k in keys {
        if let Some(hit) = forbidden.iter().find(|f| **f == k.as_ref()) {
            return Err(hit);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_boot_snapshots_only_when_no_snapshot_and_not_suppressed() {
        // Regular app, fresh: no files, not suppressed → snapshot on cold boot.
        assert!(should_snapshot_on_cold_boot(false, false));
        // Plain restart: snapshot present → do NOT re-snapshot.
        assert!(!should_snapshot_on_cold_boot(true, false));
        // Suppressed (dev-session OR workspace): cold boot NEVER snapshots — the
        // workspace's warm snapshot comes only from Cmd::Snapshot (post-index).
        assert!(!should_snapshot_on_cold_boot(false, true));
        assert!(!should_snapshot_on_cold_boot(true, true));
    }

    #[test]
    fn clean_workspace_env_is_snapshot_safe() {
        let env = ["TABBIFY_USER_ID", "WORKSPACE_UUID", "PATH"];
        assert_eq!(extra_env_is_snapshot_safe(env.iter()), Ok(()));
    }

    #[test]
    fn env_with_a_cap_url_is_rejected_naming_the_key() {
        let env = ["WORKSPACE_UUID", "TABBIFY_GIT_REMOTE"];
        assert_eq!(
            extra_env_is_snapshot_safe(env.iter()),
            Err("TABBIFY_GIT_REMOTE"),
            "a cap URL in env must be rejected so it never freezes into RAM"
        );
    }

    #[test]
    fn env_with_a_broker_token_is_rejected() {
        let env = ["TABBIFY_BROKER_TOKEN"];
        assert_eq!(extra_env_is_snapshot_safe(env.iter()), Err("TABBIFY_BROKER_TOKEN"));
    }
}
