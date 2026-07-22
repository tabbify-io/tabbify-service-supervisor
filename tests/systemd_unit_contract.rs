//! The shipped systemd units must keep the detached runners ALIVE across a
//! supervisord restart.
//!
//! App runners are `setsid`-detached so an OTA self-update does not take the
//! tenant apps down with it. `setsid` escapes the process GROUP but NOT the
//! CGROUP, so this only holds while the unit sets `KillMode=process`: under
//! systemd's default (`control-group`) a `systemctl restart` SIGKILLs every
//! runner and its Firecracker child, the new supervisord adopts nothing, and the
//! whole fleet cold-respawns.
//!
//! That is not hypothetical. `deploy/tabbify-supervisor.service` carried the
//! setting and documented exactly this hazard, but `scripts/install.sh` — which
//! writes the unit that actually runs on the dedicated host — drifted without
//! it. On 2026-07-22 three self-updates each bounced every tenant app
//! (`adopted=0 respawned=17`). These tests pin BOTH writers so the two cannot
//! diverge again silently.

/// Every place that emits the supervisor unit, as (label, file contents).
fn unit_writers() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "deploy/tabbify-supervisor.service",
            include_str!("../deploy/tabbify-supervisor.service"),
        ),
        ("scripts/install.sh", include_str!("../scripts/install.sh")),
    ]
}

/// `KillMode=process` — without it a restart kills the detached runners.
#[test]
fn every_unit_writer_sets_killmode_process() {
    for (label, body) in unit_writers() {
        assert!(
            body.contains("KillMode=process"),
            "{label} must set KillMode=process, else a supervisord restart SIGKILLs \
             every detached runner (the whole tenant fleet) with it"
        );
    }
}

/// `Delegate=yes` — the unit owns its cgroup subtree, so systemd does not manage
/// the detached runners' cgroup placement out from under the supervisor.
#[test]
fn every_unit_writer_sets_delegate() {
    for (label, body) in unit_writers() {
        assert!(
            body.contains("Delegate=yes"),
            "{label} must set Delegate=yes so the detached runners keep their own \
             cgroup subtree across a restart"
        );
    }
}

/// Neither writer may re-introduce the lethal default explicitly.
#[test]
fn no_unit_writer_sets_killmode_control_group() {
    for (label, body) in unit_writers() {
        for line in body.lines() {
            let stripped = line.trim_start_matches('#').trim();
            assert!(
                stripped != "KillMode=control-group",
                "{label} sets KillMode=control-group — that kills every runner on restart"
            );
        }
    }
}
