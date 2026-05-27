//! Per-app runner module — hosts exactly one app instance.

pub mod active;
pub mod build;
pub mod config;
pub mod control;
pub mod serve;
pub mod wire;

pub use config::RunnerConfig;
