//! Supervisor orchestrator — spawns, monitors, and re-adopts per-app runner
//! processes.
//!
//! # Phase 2 tasks
//! - Task 2.1 [`handle`] — [`RunnerHandle`] bookkeeping type + on-disk record.
//! - Task 2.2 [`spawn`] — spawn a detached runner process + persist its record.
//! - Task 2.3 [`client`] — control-socket client.
//! - Task 2.4 — health-monitor loop.
//! - Task 2.5 — re-adopt runners on supervisor restart.
//! - Task 2.6 — API rewire.

pub mod client;
pub mod handle;
pub mod spawn;

pub use client::ControlClient;
pub use handle::RunnerHandle;
pub use spawn::{SpawnSpec, spawn_runner};
