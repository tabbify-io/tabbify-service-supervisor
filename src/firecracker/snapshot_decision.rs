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
/// - `stateful` — this app owns a LIVE persistent data disk (`/dev/vdb`: a SQLite
///   DB + git repos, from `[runtime].stateful`). A cold-boot RAM snapshot would
///   freeze stale RAM and, on a later warm restore, resurrect it OVER the live
///   disk = corruption. So a stateful app must NEVER be RAM-snapshotted — this is
///   a HARD suppression (never re-taken via `Cmd::Snapshot` either), distinct from
///   the `suppressed` TIMING-deferral used by dev-sessions/workspaces.
///
/// Stateful short-circuits FIRST: a stateful app never snapshots regardless of
/// `files_present`/`suppressed`. Otherwise snapshot ⇔ no snapshot yet AND not
/// suppressed. This is the cold-boot gate; for a non-stateful app it deliberately
/// does NOT depend on whether this is a workspace vs a dev-FC — the suppression
/// marker is the only differentiator, written by whichever spawn path (dev-session
/// OR workspace) needs to defer the snapshot.
#[must_use]
pub fn should_snapshot_on_cold_boot(files_present: bool, suppressed: bool, stateful: bool) -> bool {
    if stateful {
        return false;
    }
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

/// What the launch path must do about snapshots for a STATEFUL app, decided
/// BEFORE the per-uuid snapshot companions are even consulted.
///
/// [`should_snapshot_on_cold_boot`] guarantees a stateful app never TAKES a
/// snapshot — but that alone leaves a trap: an app that ran NON-stateful first
/// and had `[runtime].stateful` flipped later still owns a LEGACY `snap.mem`
/// whose digest+env companions match, so the warm-vs-cold decision would
/// happily restore it. That restore rolls guest RAM (and the rootfs-resident
/// state the guest flushed since) back to the snapshot moment ON TOP of the
/// now-live `/dev/vdb` data disk — the exact silent-state-loss this flag
/// exists to prevent. So the gate is unconditional:
///
/// - `block_warm_restore` — a stateful app NEVER warm-restores, regardless of
///   how viable the on-disk snapshot looks (digest/env/link all matching).
/// - `clear_legacy_snapshot` — any snapshot files present for a stateful app
///   are by definition pre-flip leftovers; delete them so nothing can ever
///   consume them (and so `files_present` stops muddying later decisions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatefulLaunchGate {
    /// Warm restore is forbidden for this launch (the app is stateful).
    pub block_warm_restore: bool,
    /// A legacy (pre-`stateful`-flip) snapshot exists and must be deleted.
    pub clear_legacy_snapshot: bool,
}

/// Compute the [`StatefulLaunchGate`] for a launch. Pure so the trap ("flip
/// `stateful` while a matching legacy snapshot is on disk") is unit-testable
/// on macOS; the Linux launch path consumes the verdict.
#[must_use]
pub const fn stateful_launch_gate(
    stateful: bool,
    snapshot_files_present: bool,
) -> StatefulLaunchGate {
    StatefulLaunchGate {
        block_warm_restore: stateful,
        clear_legacy_snapshot: stateful && snapshot_files_present,
    }
}

/// The in-guest broker route the runner POSTs to RIGHT BEFORE pausing a workspace
/// VM for a Full snapshot (GAP#4). The broker's `:8732` control listener serves it
/// (broker-uid) and drops ALL in-RAM creds + removes the tmpfs cred files, so the
/// paused VM the snapshot freezes carries no live git/forge token. The runner
/// reaches it host-side at `http://<guest_ip>:8732<PATH>`.
pub const PRE_SNAPSHOT_SCRUB_PATH: &str = "/v1/pre-snapshot-scrub";

/// Must the pre-snapshot scrub run before THIS VM's warm snapshot?
///
/// Only a WORKSPACE holds provider creds (the per-repo git cap-URLs + the
/// forge-admin token) in the broker's RAM + tmpfs, and only a workspace takes a
/// Full (RAM-freezing) warm snapshot via `Cmd::Snapshot`. A non-workspace FC has
/// no broker / no creds → nothing to scrub, and dialing `:8732` would just refuse.
/// So the scrub is gated on `is_workspace`. When it returns `true`, a scrub
/// FAILURE must ABORT the snapshot (never freeze a held secret); when `false`, the
/// snapshot proceeds with no scrub. Pure so it is unit-testable on macOS.
#[must_use]
pub const fn must_scrub_before_snapshot(is_workspace: bool) -> bool {
    is_workspace
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_required_only_for_workspaces() {
        assert!(
            must_scrub_before_snapshot(true),
            "a workspace MUST scrub creds before a Full snapshot"
        );
        assert!(
            !must_scrub_before_snapshot(false),
            "a non-workspace FC has no creds to scrub"
        );
    }

    #[test]
    fn scrub_path_is_the_broker_route() {
        assert_eq!(PRE_SNAPSHOT_SCRUB_PATH, "/v1/pre-snapshot-scrub");
    }

    #[test]
    fn cold_boot_snapshots_only_when_no_snapshot_and_not_suppressed() {
        // Regular app, fresh: no files, not suppressed → snapshot on cold boot.
        assert!(should_snapshot_on_cold_boot(false, false, false));
        // Plain restart: snapshot present → do NOT re-snapshot.
        assert!(!should_snapshot_on_cold_boot(true, false, false));
        // Suppressed (dev-session OR workspace): cold boot NEVER snapshots — the
        // workspace's warm snapshot comes only from Cmd::Snapshot (post-index).
        assert!(!should_snapshot_on_cold_boot(false, true, false));
        assert!(!should_snapshot_on_cold_boot(true, true, false));
    }

    #[test]
    fn stateful_app_never_snapshots_on_cold_boot() {
        // A stateful app owns a LIVE persistent disk (`/dev/vdb`: SQLite DB + git
        // repos). A cold-boot RAM snapshot would freeze stale RAM and warm-restore
        // it OVER the live disk = corruption. So it must NEVER snapshot — the
        // stateful short-circuit wins over EVERY files_present/suppressed combo,
        // exactly as a workspace defers its cold snapshot.
        assert!(!should_snapshot_on_cold_boot(false, false, true));
        assert!(!should_snapshot_on_cold_boot(true, false, true));
        assert!(!should_snapshot_on_cold_boot(false, true, true));
        assert!(!should_snapshot_on_cold_boot(true, true, true));
    }

    #[test]
    fn flipping_stateful_with_a_legacy_snapshot_present_must_not_warm_restore() {
        // THE TRAP: an app ran non-stateful, took its cold-boot snapshot, then
        // the operator flips `[runtime].stateful = true`. The legacy snapshot's
        // digest+env companions still MATCH, so without this gate the launch
        // would warm-restore it — rolling guest state back over the live
        // `/dev/vdb`. The gate must both forbid the restore AND delete the
        // legacy files.
        let gate = stateful_launch_gate(true, true);
        assert!(
            gate.block_warm_restore,
            "a stateful app must NEVER warm-restore"
        );
        assert!(
            gate.clear_legacy_snapshot,
            "the pre-flip snapshot must be deleted, not just skipped"
        );
    }

    #[test]
    fn stateful_without_a_snapshot_still_blocks_warm_and_clears_nothing() {
        let gate = stateful_launch_gate(true, false);
        assert!(gate.block_warm_restore);
        assert!(
            !gate.clear_legacy_snapshot,
            "nothing on disk, nothing to delete"
        );
    }

    #[test]
    fn non_stateful_launch_is_unaffected_by_the_gate() {
        // The historical warm-restore path must stay byte-identical for every
        // non-stateful app, snapshot present or not.
        for files_present in [false, true] {
            let gate = stateful_launch_gate(false, files_present);
            assert!(!gate.block_warm_restore);
            assert!(!gate.clear_legacy_snapshot);
        }
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
        assert_eq!(
            extra_env_is_snapshot_safe(env.iter()),
            Err("TABBIFY_BROKER_TOKEN")
        );
    }
}
