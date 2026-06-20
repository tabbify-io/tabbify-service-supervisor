//! Supervisor-side Track-C plumbing: the reboot loop-guard (shared with B2) and
//! the production `CommandSink` that turns mesh verbs into process effects.

pub mod reboot_guard;
pub mod sink;
