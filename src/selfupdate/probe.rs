//! Out-of-band candidate probe (spec §4): the candidate binary runs under the
//! `--check` entrypoint with a TRANSIENT mesh identity (separate identity path,
//! alternate bind, OS-ephemeral WG port); this module holds the pure 3-part
//! health gate it self-evaluates before exit.

/// The three gate signals collected from a running candidate.
#[derive(Debug, Clone, Copy)]
pub struct GateInputs {
    /// The candidate joined the mesh (got an ULA).
    pub joined_mesh: bool,
    /// `GET /health` on the candidate returned 200.
    pub health_200: bool,
    /// Control `Cmd::Ping` returned `Reply::Pong`.
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

/// Pure 3-part gate (spec §4): joined mesh + /health 200 + Ping→Pong, all
/// within `timeout_secs`. No I/O — the caller gathers the signals.
#[must_use]
pub fn evaluate_gate(i: GateInputs, timeout_secs: u64) -> ProbeOutcome {
    if i.elapsed_secs > timeout_secs {
        return ProbeOutcome::Fail(format!("gate timed out after {}s", i.elapsed_secs));
    }
    if !i.joined_mesh {
        return ProbeOutcome::Fail("candidate did not join mesh".into());
    }
    if !i.health_200 {
        return ProbeOutcome::Fail("candidate /health not 200".into());
    }
    if !i.pong {
        return ProbeOutcome::Fail("candidate control Ping had no Pong".into());
    }
    ProbeOutcome::Pass
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn pass_inputs() -> GateInputs {
        GateInputs {
            joined_mesh: true,
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
            (|g: &mut GateInputs| g.joined_mesh = false) as fn(&mut GateInputs),
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
}
