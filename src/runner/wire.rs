//! Mapping layer between [`RunnerConfig`] (the clap struct) and the serve +
//! control parameters that the binary and tests consume.
//!
//! Extracted from the binary entrypoint so integration tests can exercise the
//! same config → serve → control wiring path without spawning a subprocess.

use crate::runner::{
    config::{RunnerConfig, parse_extra_env},
    serve::ServeConfig,
};

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
        // The supervisor-allocated WireGuard port (`--mesh-listen-port`), carried
        // through to the joiner so co-resident runners never share one.
        wg_listen_port: cfg.listen_port,
        // Use the served app's UUID as the mesh display name so the runner is
        // identifiable in the coordinator roster.
        display_name: format!("tabbify-runner:{}", cfg.uuid),
        parent: cfg.parent.clone(),
        port: cfg.port,
        fc: cfg.firecracker.clone(),
        docker: cfg.docker.clone(),
        image_ref: cfg.image_ref.clone(),
        // The managed `tabbify.toml` (from `RUNNER_MANIFEST_TOML`): drives the
        // synthesized `[runtime]`/`[routes]` on the BUILD-pipeline path.
        manifest_toml: cfg.manifest_toml.clone(),
        // Phase-2: thread the tenant network slug (`--network`) + the scoped
        // node-join token (from `TABBIFY_RUNNER_JOIN_TOKEN`, resolved in
        // `RunnerConfig::parse_with_env`) into the serve config so the runner's
        // mesh join is scoped to its tenant network.
        network: cfg.network.clone(),
        runner_join_token: cfg.runner_join_token.clone(),
        // Decode the `RUNNER_EXTRA_ENV` JSON string into the typed map so the
        // build pipeline can merge deploy-time entries into the guest `/init`.
        // A missing/blank/malformed value becomes `None` (safe fallback).
        extra_env: parse_extra_env(cfg.extra_env_json.as_deref()),
        // Decode the `RUNNER_EGRESS_ALLOW` JSON array (Track 7) into the typed
        // allow-list threaded host-side to `setup_guest_nat`. A missing/blank/
        // malformed value becomes `None` (safe fallback = unrestricted egress).
        egress_allow: cfg
            .egress_allow_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok()),
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

    /// `serve_config_from` carries the managed `tabbify.toml` through so the
    /// runner's `resolve_app` can apply `[runtime]`/`[routes]` on the
    /// BUILD-pipeline path.
    #[test]
    fn serve_config_from_carries_manifest_toml() {
        let cfg = parse(&["--manifest-toml", "[app]\nname = \"x\"\n"]);
        let serve = serve_config_from(&cfg);
        assert_eq!(
            serve.manifest_toml.as_deref(),
            Some("[app]\nname = \"x\"\n")
        );
    }

    /// Absent the managed toml, `serve_config_from` leaves `manifest_toml` None
    /// (today's hardcoded-default behavior).
    #[test]
    fn serve_config_from_manifest_toml_defaults_none() {
        let cfg = parse(&[]);
        let serve = serve_config_from(&cfg);
        assert!(serve.manifest_toml.is_none());
    }

    /// `serve_config_from` decodes the `RUNNER_EXTRA_ENV` JSON (here via the
    /// equivalent `--extra-env-json` flag) into the typed `extra_env` map so the
    /// build pipeline can bake deploy-time vars into the guest `/init`.
    #[test]
    fn serve_config_from_carries_extra_env() {
        let cfg = parse(&["--extra-env-json", r#"{"SSH_KEY":"ssh-ed25519 AAAA"}"#]);
        let serve = serve_config_from(&cfg);
        assert_eq!(
            serve
                .extra_env
                .as_ref()
                .and_then(|m| m.get("SSH_KEY"))
                .map(String::as_str),
            Some("ssh-ed25519 AAAA"),
            "serve_config_from must decode RUNNER_EXTRA_ENV JSON into extra_env"
        );
    }

    /// PINNED CONTRACT: a malformed `RUNNER_EXTRA_ENV` decodes to `None` (with a
    /// warning) — broken JSON must never wedge the runner; the guest just gets
    /// the OCI image's own env. Exercises the REAL `parse_extra_env` path.
    #[test]
    fn serve_config_from_malformed_extra_env_is_none() {
        let cfg = parse(&["--extra-env-json", "{not-json"]);
        let serve = serve_config_from(&cfg);
        assert!(
            serve.extra_env.is_none(),
            "malformed RUNNER_EXTRA_ENV must decode to None, not panic/abort"
        );
    }

    /// Absent extra env, `serve_config_from` leaves `extra_env` None (a normal
    /// deploy: the guest gets exactly the OCI image's env).
    #[test]
    fn serve_config_from_extra_env_defaults_none() {
        let cfg = parse(&[]);
        let serve = serve_config_from(&cfg);
        assert!(serve.extra_env.is_none());
    }

    /// Track 7: `serve_config_from` decodes the `RUNNER_EGRESS_ALLOW` JSON array
    /// (here via the equivalent `--egress-allow-json` flag) into the typed
    /// allow-list threaded host-side to `setup_guest_nat`.
    #[test]
    fn serve_config_from_carries_egress_allow() {
        let cfg = parse(&[
            "--egress-allow-json",
            r#"["api.telegram.org","10.0.0.0/24"]"#,
        ]);
        let serve = serve_config_from(&cfg);
        assert_eq!(
            serve.egress_allow.as_deref(),
            Some(&["api.telegram.org".to_owned(), "10.0.0.0/24".to_owned()][..]),
            "serve_config_from must decode RUNNER_EGRESS_ALLOW JSON into egress_allow"
        );
    }

    /// Track 7 PINNED CONTRACT: a malformed `RUNNER_EGRESS_ALLOW` decodes to
    /// `None` (safe fallback = unrestricted egress) — broken JSON must never
    /// wedge the runner nor silently sever its egress.
    #[test]
    fn serve_config_from_malformed_egress_allow_is_none() {
        let cfg = parse(&["--egress-allow-json", "[not-json"]);
        let serve = serve_config_from(&cfg);
        assert!(
            serve.egress_allow.is_none(),
            "malformed RUNNER_EGRESS_ALLOW must decode to None, not panic/abort"
        );
    }

    /// Absent an allow-list, `serve_config_from` leaves `egress_allow` None (a
    /// normal deploy: today's unrestricted egress, no regression).
    #[test]
    fn serve_config_from_egress_allow_defaults_none() {
        let cfg = parse(&[]);
        let serve = serve_config_from(&cfg);
        assert!(serve.egress_allow.is_none());
    }
}
