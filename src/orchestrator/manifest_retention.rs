//! Pure decisions about what a deploy does to the durable record's managed
//! `tabbify.toml` — the ONLY carrier of `[runtime].stateful` / `data_mount` for
//! a connect-repo app.
//!
//! # Why this is its own module
//!
//! `manifest_toml` used to be the one deploy field with REPLACEMENT semantics
//! (`None` clears) while every sibling — `runner_join_token`, `extra_env`,
//! `egress_allow` — used PATCH semantics (`None` keeps). Same `Option` type,
//! opposite meaning, adjacent lines. A nudge re-deploy that omitted the toml
//! (the MCP deploy path hardcodes `manifest_toml: None`) therefore erased the
//! app's persistence intent, and the NEXT spawn came up ephemeral: running,
//! healthy, writing to a rootfs that the following respawn throws away. The
//! only symptom was the ABSENCE of a `PUT /drives/data` line in the boot log.
//!
//! So the rules here are:
//!
//! 1. `None` KEEPS the persisted manifest ([`retained`]). Clearing is done by
//!    purging the app and deploying fresh, exactly like the network fields.
//! 2. An explicit manifest that DROPS a live data disk is refused, loudly
//!    ([`stateful_regression`]) — silently un-statefuling an app destroys data
//!    on its next respawn, so it must never be the quiet path.

use crate::build::managed_persistence;

/// The manifest a deploy should persist and spawn with: the incoming one when
/// the body carried one, otherwise the record's existing one.
///
/// Patch semantics, matching every other optional deploy field.
#[must_use]
pub fn retained<'a>(incoming: Option<&'a str>, persisted: Option<&'a str>) -> Option<&'a str> {
    incoming.or(persisted)
}

/// A deploy that would take a live persistent data disk away from an app.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatefulRegression {
    /// Where the app's data disk mounts today (`None` = declared stateful with
    /// no mount, which the launch path already rejects).
    pub previous_mount: Option<String>,
    /// What the incoming manifest would leave it at.
    pub next_mount: Option<String>,
    /// Whether the incoming manifest keeps `stateful` at all.
    pub next_stateful: bool,
}

impl std::fmt::Display for StatefulRegression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let previous = self.previous_mount.as_deref().unwrap_or("<unset>");
        if self.next_stateful {
            write!(
                f,
                "refusing deploy: this app's persistent data disk is mounted at \
                 {previous}, but the supplied tabbify.toml moves it to {}. The \
                 existing data stays at the old path and the app would start \
                 against an empty disk. Keep [runtime].data_mount unchanged, or \
                 migrate the data and purge the app first.",
                self.next_mount.as_deref().unwrap_or("<unset>")
            )
        } else {
            write!(
                f,
                "refusing deploy: this app is stateful (data disk mounted at \
                 {previous}), but the supplied tabbify.toml drops \
                 [runtime].stateful. The app would boot on an ephemeral rootfs \
                 and lose everything on its next respawn. Restore \
                 [runtime].stateful, or purge the app first if the data is \
                 genuinely disposable."
            )
        }
    }
}

impl std::error::Error for StatefulRegression {}

/// Would applying `effective` to an app whose record holds `persisted` strip its
/// persistent disk?
///
/// Fires on the two transitions that silently orphan data:
/// - `stateful` true → false (the disk stops being attached at all);
/// - `data_mount` moved while stateful (the disk attaches, but the app looks for
///   its data at a path that was never written to).
///
/// Never fires when the app was not stateful to begin with — GAINING a disk is
/// always safe — and, because [`retained`] runs first, never fires for a deploy
/// that simply omitted the toml.
#[must_use]
pub fn stateful_regression(
    persisted: Option<&str>,
    effective: Option<&str>,
) -> Option<StatefulRegression> {
    let before = managed_persistence(persisted);
    if !before.stateful {
        return None;
    }
    let after = managed_persistence(effective);
    if after.stateful && after.data_mount == before.data_mount {
        return None;
    }
    Some(StatefulRegression {
        previous_mount: before.data_mount,
        next_mount: after.data_mount,
        next_stateful: after.stateful,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATEFUL: &str = "[runtime]\nstateful = true\ndata_mount = \"/var/lib/forge\"\n";
    const EPHEMERAL: &str = "[runtime]\nport = 8730\n";

    #[test]
    fn absent_incoming_manifest_keeps_the_persisted_one() {
        assert_eq!(
            retained(None, Some(STATEFUL)),
            Some(STATEFUL),
            "a nudge re-deploy must not erase the app's runtime config"
        );
    }

    #[test]
    fn supplied_manifest_replaces_the_persisted_one() {
        assert_eq!(retained(Some(EPHEMERAL), Some(STATEFUL)), Some(EPHEMERAL));
    }

    #[test]
    fn first_deploy_has_nothing_to_keep() {
        assert_eq!(retained(None, None), None);
        assert_eq!(retained(Some(STATEFUL), None), Some(STATEFUL));
    }

    #[test]
    fn dropping_stateful_is_a_regression() {
        let reg = stateful_regression(Some(STATEFUL), Some(EPHEMERAL))
            .expect("stateful true -> false must be refused");
        assert_eq!(reg.previous_mount.as_deref(), Some("/var/lib/forge"));
        assert!(!reg.next_stateful);
        assert!(
            reg.to_string().contains("ephemeral rootfs"),
            "the error must say what happens to the data: {reg}"
        );
    }

    #[test]
    fn clearing_the_manifest_entirely_is_a_regression() {
        // Only reachable when a caller bypasses `retained`; pinned so the guard
        // does not quietly depend on retention having run first.
        assert!(stateful_regression(Some(STATEFUL), None).is_some());
    }

    #[test]
    fn an_unparseable_manifest_is_a_regression() {
        // A toml that stops parsing loses the disk exactly like one that drops
        // the flag — `fetched_from_ref` falls back to the ephemeral defaults.
        let broken = "[runtime\nstateful = true\n";
        assert!(
            stateful_regression(Some(STATEFUL), Some(broken)).is_some(),
            "a manifest that no longer parses must not silently un-stateful the app"
        );
    }

    #[test]
    fn moving_the_data_mount_is_a_regression() {
        let moved = "[runtime]\nstateful = true\ndata_mount = \"/srv/forge\"\n";
        let reg = stateful_regression(Some(STATEFUL), Some(moved))
            .expect("relocating the mount orphans the data at the old path");
        assert!(reg.next_stateful);
        assert_eq!(reg.next_mount.as_deref(), Some("/srv/forge"));
    }

    #[test]
    fn an_unchanged_stateful_manifest_is_not_a_regression() {
        assert_eq!(stateful_regression(Some(STATEFUL), Some(STATEFUL)), None);
    }

    #[test]
    fn gaining_a_data_disk_is_not_a_regression() {
        assert_eq!(stateful_regression(Some(EPHEMERAL), Some(STATEFUL)), None);
        assert_eq!(stateful_regression(None, Some(STATEFUL)), None);
    }

    #[test]
    fn an_ephemeral_app_may_still_change_its_manifest_freely() {
        assert_eq!(stateful_regression(Some(EPHEMERAL), None), None);
        assert_eq!(stateful_regression(None, None), None);
    }
}
