//! Out-of-band candidate probe (spec §4): launch the candidate binary with a
//! TRANSIENT mesh identity (separate identity path, alternate bind, OS-ephemeral
//! WG port) + the `--check` entrypoint, then evaluate the 3-part health gate.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;

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

/// Inputs for launching the candidate process out-of-band.
#[derive(Debug, Clone)]
pub struct CandidateSpec {
    /// Path to the candidate `supervisord` binary in `releases/v<VER>/`.
    pub binary: PathBuf,
    /// Transient identity file — NOT the sticky `mesh-identity.json`.
    pub candidate_identity_path: PathBuf,
    /// Alternate bind addr (loopback ephemeral) for the candidate's control API.
    pub bind: SocketAddr,
    /// Coordinator URL (same mesh, transient peer).
    pub coordinator_url: String,
    /// Data dir (read-only consult; candidate must not mutate runner_dir).
    pub data_dir: PathBuf,
}

/// argv for the candidate launch: `--check` + transient identity + alt bind.
/// The OS-ephemeral WG port is the joiner default (`listen_port: None`), so no
/// explicit port flag is needed; the candidate never claims the sticky ULA.
#[must_use]
pub fn candidate_args(spec: &CandidateSpec) -> Vec<OsString> {
    vec![
        "--check".into(),
        "--candidate-identity-path".into(),
        spec.candidate_identity_path.clone().into_os_string(),
        "--bind".into(),
        spec.bind.to_string().into(),
        "--coordinator-url".into(),
        spec.coordinator_url.as_str().into(),
        "--data-dir".into(),
        spec.data_dir.clone().into_os_string(),
    ]
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

    /// The candidate launches OUT-OF-BAND: a transient identity path, an
    /// alternate bind addr, an ephemeral WG port, and the --check entrypoint —
    /// and it must NOT reuse the sticky mesh-identity.json.
    #[test]
    fn candidate_args_use_transient_identity_and_check_mode() {
        let spec = CandidateSpec {
            binary: "/opt/tabbify/releases/v9.9.9/supervisord".into(),
            candidate_identity_path: "/opt/tabbify/candidate-identity.json".into(),
            bind: "[::1]:0".parse().unwrap(),
            coordinator_url: "http://127.0.0.1:8888".into(),
            data_dir: "/var/lib/tabbify/data".into(),
        };
        let args: Vec<String> = candidate_args(&spec)
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(args.iter().any(|a| a == "--check"), "got: {args:?}");
        assert!(
            args.iter().any(|a| a == "--candidate-identity-path"),
            "got: {args:?}"
        );
        assert!(
            args.iter()
                .any(|a| a == "/opt/tabbify/candidate-identity.json"),
            "got: {args:?}"
        );
        assert!(args.iter().any(|a| a == "--bind"), "got: {args:?}");
        // Must NOT point the candidate at the sticky identity file.
        assert!(
            !args.iter().any(|a| a.ends_with("mesh-identity.json")),
            "got: {args:?}"
        );
    }
}
