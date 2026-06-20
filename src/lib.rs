#![cfg_attr(not(test), warn(missing_docs))]

//! `tabbify-service-supervisor` — app-layer supervisor for the Tabbify mesh.
//!
//! Joins the WireGuard mesh as a `supervisor`-tagged peer, fetches apps from S3
//! by UUID, runs them per a TOML lifecycle, and serves them over the mesh on
//! `[my_ula]:8730` (contract §5).
//!
//! # Layers
//! - [`config`] — configuration (env + clap).
//! - [`manifest`] — vendored `manifest.toml` schema (contract §3).
//! - [`app_ula`] — vendored deterministic app-ULA (contract §4).
//! - [`app_runtime`] — the [`app_runtime::AppRuntime`] seam (re-exported from
//!   [`runtime`]) plus the deploy-time runtime-selection enum.
//! - [`firecracker`] — an [`app_runtime::AppRuntime`]: a KVM-gated Firecracker
//!   microVM runtime (real on Linux, stub elsewhere).
//! - [`docker`] — an [`app_runtime::AppRuntime`]: a cross-platform Docker
//!   container runtime that builds the app image from source on the supervisor.
//! - [`build_backend`] — swappable OCI-image build backends:
//!   [`build_backend::BuildBackend`] trait + [`build_backend::HostDockerBackend`]
//!   (runs `docker build` on the host daemon; fc-sandbox backend is a follow-up).
//! - [`git`] — secure `git clone` helper: injects `GIT_ASKPASS` so the token
//!   never appears in process argv.
//! - [`fetcher`] — anonymous S3 artifact fetch + local cache (contract §2).
//! - [`host`] — per-app-ULA hosting: one listener per app on its own ULA, used
//!   by the per-app [`runner`] (contract §5, Component 3).
//! - [`orchestrator`] — spawns / monitors / re-adopts the per-app runner fleet
//!   and drives the control-API lifecycle (start / stop / purge / list).
//! - [`runner`] — the per-app `tabbify-runner`: hosts exactly one app on its
//!   own ULA + a unix-socket control plane.
//! - [`mesh`] — mesh join wiring (contract §5).
//! - [`api`] — axum control HTTP API (contract §5).
//! - [`selfupdate`] — health-gated self-update engine: versioned fetch +
//!   sha256 verify, out-of-band probe, atomic symlink swap, watchdog rollback.
//! - [`readiness`] — `sd_notify(READY=1)` for the `Type=notify` systemd unit,
//!   emitted once after bind + mesh-join; best-effort no-op off systemd.

pub mod api;
pub mod app_runtime;
pub mod app_ula;
pub mod build;
pub mod build_backend;
pub mod capability_tags;
pub mod config;
pub mod control_proto;
pub mod docker;
pub mod fetcher;
pub mod firecracker;
pub mod git;
pub mod host;
pub mod manifest;
pub mod mesh;
pub mod mesh_command;
pub mod openapi;
pub mod oras;
pub mod orchestrator;
pub mod readiness;
pub mod runner;
pub mod runtime;
pub mod selfupdate;
pub mod skopeo;
pub mod tcp_forward;
pub mod tool_exec;
pub mod unified_manifest;
pub mod version;

pub use config::Config;
pub use runner::RunnerConfig;
