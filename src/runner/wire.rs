//! Mapping layer between [`RunnerConfig`] (the clap struct) and the serve +
//! control parameters that the binary and tests consume.
//!
//! Extracted from the binary entrypoint so integration tests can exercise the
//! same config → serve → control wiring path without spawning a subprocess.

use crate::runner::config::RunnerConfig;
use crate::runner::serve::ServeConfig;

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
        // Use the served app's UUID as the mesh display name so the runner is
        // identifiable in the coordinator roster.
        display_name: format!("tabbify-runner:{}", cfg.uuid),
        parent: cfg.parent.clone(),
        port: cfg.port,
        fc: cfg.firecracker.clone(),
        docker: cfg.docker.clone(),
        image_ref: cfg.image_ref.clone(),
        runtime_override: cfg.runtime_override.clone(),
    }
}
