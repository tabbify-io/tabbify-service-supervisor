//! Production [`CommandSink`] — turns Track-C verbs into supervisor process
//! effects: `RestartJoiner` triggers a unit restart (the cleanest "drop +
//! rebuild the joiner" on a host daemon — a fresh process re-reads
//! `TABBIFY_MESH_RELAY_ONLY` from the unit env, so the relay floor is preserved
//! by construction); `RebootHost` is a guarded `systemctl reboot`.

use std::path::{Path, PathBuf};
use std::process::Command;

use mesh_joiner::coordinator::command_exec::CommandSink;

use super::reboot_guard::RebootGuard;

/// Supervisor command sink. Owns the reboot loop-guard path.
pub struct SupervisorCommandSink {
    reboot_guard: RebootGuard,
    /// The systemd unit to restart for `RestartJoiner`.
    unit: String,
}

impl SupervisorCommandSink {
    /// Build a sink with the reboot-history sidecar under `data_dir`.
    #[must_use]
    pub fn new(data_dir: &Path, unit: impl Into<String>) -> Self {
        Self {
            reboot_guard: RebootGuard::new(reboot_history_path(data_dir)),
            unit: unit.into(),
        }
    }
}

/// Path of the reboot-history sidecar.
#[must_use]
pub fn reboot_history_path(data_dir: &Path) -> PathBuf {
    data_dir.join("reboot-guard.json")
}

impl CommandSink for SupervisorCommandSink {
    fn restart_joiner(&self) {
        // A process restart is the cleanest joiner rebuild on a host daemon:
        // fresh register, fresh boringtun Tunns, fresh relay-WS — and it
        // re-reads TABBIFY_MESH_RELAY_ONLY from the unit env, so `relay_only`
        // is preserved automatically (spec §7 invariant). `KillMode=process`
        // (nixos unit) keeps detached runners alive across the restart.
        tracing::warn!(unit = %self.unit, "Track C: RestartJoiner → systemctl restart");
        let _ = Command::new("systemctl")
            .arg("restart")
            .arg(&self.unit)
            .status();
    }

    fn reboot_host(&self) {
        if !self.reboot_guard.try_reboot_now() {
            tracing::error!("Track C: RebootHost refused by loop-guard (parked for a human)");
            return;
        }
        tracing::warn!("Track C: RebootHost → systemctl reboot (guard slot consumed)");
        let _ = Command::new("systemctl").arg("reboot").status();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// The sink wires its reboot guard under `data_dir`; a fourth reboot in the
    /// window is refused (no actual reboot runs in the test — we exercise the
    /// guard path that gates the `systemctl reboot` call).
    #[test]
    fn reboot_guard_parks_after_limit() {
        let dir = TempDir::new().unwrap();
        let guard = RebootGuard::new(reboot_history_path(dir.path()));
        let t = 42;
        assert!(guard.try_reboot(t));
        assert!(guard.try_reboot(t));
        assert!(guard.try_reboot(t));
        assert!(!guard.try_reboot(t), "sink's guard must park the 4th reboot");
    }
}
