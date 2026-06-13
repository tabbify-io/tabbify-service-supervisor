//! Out-of-band candidate probe (spec §4): the candidate binary runs under the
//! `--check` entrypoint and self-evaluates a local readiness gate before exit.
//!
//! # Why the candidate probe does NOT join the production mesh
//!
//! The gate answers ONE question: *is the freshly-staged binary good enough to
//! swap to?* — i.e. does it boot, bind its control listener, and serve its
//! control surface. That is exactly what a bad binary breaks (a panic on boot,
//! a broken router, a missing dependency, a bad migration of the control
//! layer). It is INDEPENDENT of whether a throwaway identity can acquire a TUN
//! and join the live coordinator.
//!
//! An earlier design required the candidate to perform a FULL production mesh
//! join ([`crate::mesh::MeshMembership::join`] → `Joiner::join`) with a fresh
//! transient identity. That was the root cause of the self-update gate failing
//! on every production node:
//!   1. The host-integrated join opens + addresses a brand-new TUN device and
//!      installs ip6tables trust rules — operations that need `CAP_NET_ADMIN` /
//!      root, which the out-of-band `--check` child does not reliably hold.
//!   2. It registers a second, throwaway peer (fresh keypair → fresh ULA → a
//!      brand-new stable TUN name) against the LIVE coordinator, contending
//!      with the running supervisor's host integration and polluting the roster.
//!   3. None of that says anything about whether the NEW BINARY is healthy.
//!
//! The mesh fabric is never hot-swapped anyway (the in-process `Tunn` /
//! `SessionTable` are not serialisable): a self-update only ever exercises the
//! mesh by a FULL PROCESS RESTART. The post-swap self-watchdog
//! ([`crate::selfupdate::confirm::live_local_observe`]) — the part that actually
//! decides confirm-vs-rollback after the real swap — already validates health
//! over LOCAL HTTP, not a mesh join. The candidate gate is brought into line:
//! it runs the candidate `--no-mesh` and checks local readiness only.

/// The three gate signals collected from a running candidate.
#[derive(Debug, Clone, Copy)]
pub struct GateInputs {
    /// The candidate binary launched and bound its local control listener (it
    /// did NOT crash on boot / fail to bind). This is the decoupled stand-in for
    /// the old "joined the mesh" signal: a bad binary cannot reach this point.
    pub launched: bool,
    /// `GET /health` on the candidate returned 200.
    pub health_200: bool,
    /// `GET /v1/about` (a DISTINCT liveness route/handler) returned 200.
    pub pong: bool,
    /// Seconds elapsed gathering the three signals.
    pub elapsed_secs: u64,
}

/// Result of the 3-part gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// All three parts passed within the timeout — safe to swap.
    Pass,
    /// At least one part failed (or the timeout elapsed). Carries a reason.
    Fail(String),
}

/// The args the production probe spawns the candidate with — the SINGLE source
/// of truth for the candidate-probe contract.
///
/// Crucially this includes `--no-mesh`: the probe is a LOCAL readiness check of
/// the new binary, NOT a production mesh join (see the module docs). It still
/// passes the transient identity path so the candidate honours the
/// "never the sticky identity" contract for any code that consults it, and pins
/// a loopback ephemeral control bind so the candidate never contends for the
/// production ULA / bind.
///
/// Returned as owned `String`s so the production probe and a unit test build the
/// exact same argv.
#[must_use]
pub fn candidate_probe_args(transient_identity: &std::path::Path) -> Vec<String> {
    vec![
        "--check".to_owned(),
        "--candidate-identity-path".to_owned(),
        transient_identity.display().to_string(),
        // Decouple the gate from a production mesh join: verify the new binary
        // boots + serves locally, do NOT grab a TUN / join the live coordinator
        // (which needs root + contends with the live peer). This is THE fix for
        // the self-update gate failing on every production node.
        "--no-mesh".to_owned(),
    ]
}

/// Pure 3-part gate (spec §4, decoupled from a production mesh join): the binary
/// launched + `/health` 200 + a distinct liveness route 200, all within
/// `timeout_secs`. No I/O — the caller gathers the signals.
#[must_use]
pub fn evaluate_gate(i: GateInputs, timeout_secs: u64) -> ProbeOutcome {
    if i.elapsed_secs > timeout_secs {
        return ProbeOutcome::Fail(format!("gate timed out after {}s", i.elapsed_secs));
    }
    if !i.launched {
        return ProbeOutcome::Fail("candidate binary did not launch / bind".into());
    }
    if !i.health_200 {
        return ProbeOutcome::Fail("candidate /health not 200".into());
    }
    if !i.pong {
        return ProbeOutcome::Fail("candidate liveness route not 200".into());
    }
    ProbeOutcome::Pass
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn pass_inputs() -> GateInputs {
        GateInputs {
            launched: true,
            health_200: true,
            pong: true,
            elapsed_secs: 12,
        }
    }

    #[test]
    fn gate_passes_when_all_three_within_timeout() {
        assert_eq!(evaluate_gate(pass_inputs(), 60), ProbeOutcome::Pass);
    }

    #[test]
    fn gate_fails_if_any_part_missing() {
        for mutate in [
            (|g: &mut GateInputs| g.launched = false) as fn(&mut GateInputs),
            |g: &mut GateInputs| g.health_200 = false,
            |g: &mut GateInputs| g.pong = false,
        ] {
            let mut g = pass_inputs();
            mutate(&mut g);
            assert!(
                matches!(evaluate_gate(g, 60), ProbeOutcome::Fail(_)),
                "missing a gate part must fail"
            );
        }
    }

    #[test]
    fn gate_fails_on_timeout_even_if_all_parts_eventually_true() {
        let g = GateInputs {
            elapsed_secs: 61,
            ..pass_inputs()
        };
        assert!(matches!(evaluate_gate(g, 60), ProbeOutcome::Fail(_)));
    }

    /// The candidate probe must spawn the candidate with `--no-mesh` so the gate
    /// is a LOCAL readiness check, never a production mesh join (the root-cause
    /// fix: a fresh transient identity could not acquire a TUN / join the live
    /// coordinator without root, failing the gate on every node). It still
    /// carries `--check` + the transient identity path.
    #[test]
    fn candidate_probe_args_decouple_from_the_production_mesh() {
        let args =
            candidate_probe_args(std::path::Path::new("/opt/tabbify/candidate-identity.json"));
        assert!(
            args.contains(&"--no-mesh".to_owned()),
            "candidate probe MUST run --no-mesh so it never joins the live mesh: {args:?}"
        );
        assert!(
            args.contains(&"--check".to_owned()),
            "must be a candidate probe"
        );
        assert!(
            args.contains(&"--candidate-identity-path".to_owned())
                && args.contains(&"/opt/tabbify/candidate-identity.json".to_owned()),
            "must pass the transient identity path: {args:?}"
        );
    }
}
