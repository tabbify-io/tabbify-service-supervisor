#![cfg_attr(not(test), warn(missing_docs))]

//! `tabbify-service-supervisor` — app-layer supervisor for the Tabbify mesh.
//!
//! Joins the WireGuard mesh as a `supervisor`-tagged peer, fetches WASM apps
//! from S3 by UUID, runs them per a TOML lifecycle, and serves them over the
//! mesh on `[my_ula]:8730` (contract §5).
//!
//! # Layers
//! - [`config`] — configuration (env + clap).
//! - [`manifest`] — vendored `manifest.toml` schema (contract §3).
//! - [`app_ula`] — vendored deterministic app-ULA (contract §4).
//! - [`runtime`] — minimal wasmtime `wasi:http/proxy` runtime (contract §8).
//! - [`fetcher`] — anonymous S3 artifact fetch + local cache (contract §2).
//! - [`host`] — per-app-ULA hosting: one listener per app on its own ULA
//!   (contract §5, Component 3).
//! - [`registry`] — app registry + lifecycle state machine (contract §5).
//! - [`mesh`] — mesh join wiring (contract §5).
//! - [`api`] — axum control HTTP API (contract §5).

pub mod api;
pub mod app_ula;
pub mod config;
pub mod fetcher;
pub mod host;
pub mod manifest;
pub mod mesh;
pub mod registry;
pub mod runtime;

pub use config::Config;
