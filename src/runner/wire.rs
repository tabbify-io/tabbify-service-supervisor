//! Mapping layer between [`RunnerConfig`] (the clap struct) and the serve +
//! control parameters that the binary and tests consume.
//!
//! Extracted from the binary entrypoint so integration tests can exercise the
//! same config → serve → control wiring path without spawning a subprocess.

use crate::runner::{config::RunnerConfig, serve::ServeConfig};

/// Map a parsed [`RunnerConfig`] to the [`ServeConfig`] that
/// [`crate::runner::serve::RunnerServe::start`] accepts.
///
/// The mapping is straightforward: every `ServeConfig` field comes directly
/// from a `RunnerConfig` field. The helper exists so both the binary and
/// integration tests call the same path instead of duplicating the struct
/// construction.
#[must_use]
pub fn serve_config_from(cfg: &RunnerConfig) -> ServeConfig {
    ServeConfig {
        uuid: cfg.uuid.to_string(),
        s3_base_url: cfg.s3_base_url.clone(),
        data_dir: cfg.data_dir.clone(),
        no_mesh: cfg.no_mesh,
        coordinator_url: cfg.coordinator_url.clone(),
        relay_url: cfg.relay_url.clone(),
        relay_only: cfg.relay_only,
        // Use the served app's UUID as the mesh display name so the runner is
        // identifiable in the coordinator roster.
        display_name: format!("tabbify-runner:{}", cfg.uuid),
        parent: cfg.parent.clone(),
        port: cfg.port,
        fc: cfg.firecracker.clone(),
        docker: cfg.docker.clone(),
        image_ref: cfg.image_ref.clone(),
        // Phase-2: thread the tenant network slug (`--network`) + the scoped
        // node-join token (from `TABBIFY_RUNNER_JOIN_TOKEN`, resolved in
        // `RunnerConfig::parse_with_env`) into the serve config so the runner's
        // mesh join is scoped to its tenant network.
        network: cfg.network.clone(),
        runner_join_token: cfg.runner_join_token.clone(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use clap::Parser as _;

    use super::*;

    fn parse(args: &[&str]) -> RunnerConfig {
        let mut full = vec![
            "tabbify-runner",
            "--uuid",
            "0191e7c2-1111-7222-8333-444455556666",
        ];
        full.extend_from_slice(args);
        RunnerConfig::try_parse_from(full).unwrap()
    }

    /// `serve_config_from` carries the parsed `relay_only` through to the
    /// `ServeConfig` so the runner's mesh join declares no reachable direct
    /// endpoint when the supervisor forwarded `--mesh-relay-only`.
    #[test]
    fn serve_config_from_carries_relay_only_true() {
        let cfg = parse(&["--mesh-relay-only"]);
        let serve = serve_config_from(&cfg);
        assert!(
            serve.relay_only,
            "serve_config_from must carry relay_only=true through"
        );
    }

    /// Absent `--mesh-relay-only`, `serve_config_from` leaves `relay_only` false
    /// (the runner keeps direct + hole-punch traversal).
    #[test]
    fn serve_config_from_relay_only_defaults_false() {
        let cfg = parse(&[]);
        let serve = serve_config_from(&cfg);
        assert!(!serve.relay_only);
    }
}
