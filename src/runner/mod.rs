//! Per-app runner module — hosts exactly one app instance.

pub mod config;
pub mod control;
pub mod serve;

pub use config::RunnerConfig;
