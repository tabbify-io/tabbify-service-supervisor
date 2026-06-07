//! Spawn a per-app `tabbify-runner` process DETACHED + persist its record
//! (Task 2.2).
//!
//! The supervisor orchestrator runs one `tabbify-runner` process per app. The
//! whole resilience story depends on that runner being DETACHED from the
//! supervisor: if `supervisord` is SIGKILLed (or simply restarted), the runner
//! and its workload must keep running and stay reachable on their control
//! socket so the supervisor can re-adopt them later (Task 2.5).
//!
//! # Detach mechanics (Unix)
//! We use `Command::pre_exec` (tokio's inherent method, mirroring
//! [`std::os::unix::process::CommandExt::pre_exec`]) to call `libc::setsid()` in
//! the child between `fork` and `exec`. `setsid` makes the child a new
//! **session leader** in a **new process group**, so:
//! - signals delivered to the supervisor's process group (e.g. a `Ctrl-C` /
//!   `SIGINT` to the foreground group, or a group-wide `SIGTERM`) do NOT reach
//!   the runner; and
//! - the runner is not in the supervisor's job-control session, so it survives
//!   the supervisor's death.
//!
//! We deliberately do **not** set `kill_on_drop`. Both `std` and `tokio`
//! default it to `false`, but we are explicit about it here because dropping
//! the returned [`Child`] handle MUST NOT kill the runner — that is the very
//! property the integration test verifies.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result};
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::{app_ula::derive_app_ula, orchestrator::handle::RunnerHandle};

/// Filename of the per-app runner binary, resolved next to the current
/// executable in production (both binaries ship side-by-side from one cargo
/// build / one container image).
const RUNNER_BIN_NAME: &str = "tabbify-runner";

/// Everything [`spawn_runner`] needs to launch one runner process.
///
/// Decoupled from the full clap [`crate::runner::RunnerConfig`] so callers
/// (and the integration test) construct it directly; the binary path is
/// injectable so the test can point at the cargo-built `tabbify-runner`
/// (`env!("CARGO_BIN_EXE_tabbify-runner")`).
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// Path to the `tabbify-runner` binary to exec.
    pub runner_bin: PathBuf,
    /// UUID of the app this runner hosts (string form).
    pub uuid: String,
    /// Unix-domain control socket the runner binds (the orchestrator's channel
    /// to the runner).
    pub control_sock: PathBuf,
    /// S3 base URL for anonymous artifact fetch (a wiremock URI in tests).
    pub s3_base_url: String,
    /// Local data dir the runner caches artifacts under.
    pub data_dir: PathBuf,
    /// ULA of this (parent) supervisor, forwarded to the runner so it can report
    /// up the topology. `None` for a standalone runner.
    pub parent: Option<String>,
    /// Skip mesh join; bind plain loopback. Used for local runs / tests without
    /// root + TUN.
    pub no_mesh: bool,
    /// Explicit DERP-style mesh relay endpoint (`TABBIFY_MESH_RELAY_URL`),
    /// forwarded to the runner as `--mesh-relay-url <url>` when `Some` so the
    /// runner routes its OWN mesh-join relay over the same `wss://` endpoint as
    /// the supervisor (the corporate-firewall escape hatch). `None` (the
    /// default) lets the runner derive the relay from the coordinator URL.
    pub relay_url: Option<String>,
    /// Relay-only declaration (`TABBIFY_MESH_RELAY_ONLY`), forwarded to the runner
    /// as the bare `--mesh-relay-only` flag when `true` so the runner's OWN mesh
    /// join tells the coordinator it has no reachable direct endpoint (it shares
    /// the host's NAT/firewall with the supervisor) — the coordinator then
    /// suppresses the runner's direct endpoint + hole-punch directives so its WG
    /// handshake converges over the relay. `false` (the default) keeps direct +
    /// hole-punch traversal.
    pub relay_only: bool,
    /// OCI image ref of the last successful deploy, forwarded to the runner as
    /// `--image-ref <ref>` so a respawn comes up on the deployed version (the
    /// runner applies it to the manifest's `registry_ref`). `None` (the default)
    /// = build from the S3 manifest as usual. Set from
    /// [`RunnerHandle::image_ref`](crate::orchestrator::handle::RunnerHandle) on
    /// a respawn; `None` on a fresh spawn.
    pub image_ref: Option<String>,
    /// Tenant network slug (Phase-2 contract). Forwarded to the runner as
    /// `--network <slug>` so it joins the mesh scoped to this network and the
    /// coordinator stamps it `network=<slug>`, `tags=["tag:net-<slug>"]`.
    /// Persisted in the runner record so a respawn rejoins the same network.
    /// `None` (the default) = today's unscoped behavior.
    pub network: Option<String>,
    /// Scoped node-minted node-join JWT for THIS app's runner (Phase-2). Passed
    /// to the runner via the `TABBIFY_RUNNER_JOIN_TOKEN` environment variable
    /// (NOT a CLI arg — it is a credential, kept off the process arg list / ps
    /// output) so a validating coordinator authenticates the runner's register.
    /// NOT persisted to disk (it is short-lived + minted per deploy); `None` on
    /// a respawn means the runner rejoins via its sticky per-uuid keypair.
    /// `None` (the default) keeps the current tokenless behavior.
    pub runner_join_token: Option<String>,
    /// The Tabbify-MANAGED `tabbify.toml` (raw TOML) for a connect-repo deploy.
    /// Passed to the runner via the `RUNNER_MANIFEST_TOML` environment variable
    /// (an env, not an arg: the toml is multi-line and would clutter `ps`) so a
    /// BUILD-pipeline app's `[runtime]`/`[routes]` drive its synthesized
    /// manifest. NOT persisted to the runner record (it is re-supplied per
    /// deploy). `None` (the default) keeps the hardcoded FC defaults.
    pub manifest_toml: Option<String>,
}

/// Resolve the production `tabbify-runner` path: the binary sitting next to the
/// currently-running executable (the supervisor + runner ship together).
///
/// Falls back to the bare binary name (resolved via `$PATH` at exec time) if
/// the current executable's directory cannot be determined.
#[must_use]
pub fn default_runner_bin() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(RUNNER_BIN_NAME)))
        .unwrap_or_else(|| PathBuf::from(RUNNER_BIN_NAME))
}

/// Build the runner's argv from a [`SpawnSpec`].
///
/// In `--no-mesh` mode the runner binds an ephemeral loopback address for its
/// app listener (`--bind 127.0.0.1:0`); the orchestrator talks to it over the
/// control socket regardless, so the serve bind addr is the runner's own
/// concern. In mesh mode the runner derives + binds its app-ULA itself, so no
/// `--bind` is passed.
fn build_args(spec: &SpawnSpec) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--uuid".into(),
        spec.uuid.as_str().into(),
        "--control-sock".into(),
        spec.control_sock.clone().into_os_string(),
        "--s3-base-url".into(),
        spec.s3_base_url.as_str().into(),
        "--data-dir".into(),
        spec.data_dir.clone().into_os_string(),
    ];
    if spec.no_mesh {
        args.push("--no-mesh".into());
        // Ephemeral loopback for the app listener; the control socket is the
        // orchestrator's channel.
        args.push("--bind".into());
        args.push("127.0.0.1:0".into());
    }
    if let Some(parent) = &spec.parent {
        args.push("--parent".into());
        args.push(parent.as_str().into());
    }
    // Forward the explicit relay endpoint so the runner routes its OWN mesh-join
    // relay over the same `wss://` url as the supervisor (corporate firewall).
    // Omitted when `None` so the runner derives the relay from the coordinator
    // URL as before. The runner ALSO reads `TABBIFY_MESH_RELAY_URL` via clap
    // `env=`, so an inherited env is a safety net — but the explicit arg is
    // authoritative.
    if let Some(relay_url) = &spec.relay_url {
        args.push("--mesh-relay-url".into());
        args.push(relay_url.as_str().into());
    }
    // Forward the relay-only declaration so the runner tells the coordinator it
    // has no reachable direct endpoint (the coordinator then suppresses its
    // direct endpoint + hole-punch directives). A bare flag (no value), pushed
    // ONLY when true; omitted when false keeps the runner's direct + hole-punch
    // traversal. The runner ALSO reads `TABBIFY_MESH_RELAY_ONLY` via clap `env=`,
    // so an inherited env is a safety net — but the explicit flag is the
    // authoritative pass-through from the supervisor.
    if spec.relay_only {
        args.push("--mesh-relay-only".into());
    }
    // Forward the deployed image ref so a respawn comes up on that version.
    if let Some(image_ref) = &spec.image_ref {
        args.push("--image-ref".into());
        args.push(image_ref.as_str().into());
    }
    // Phase-2: forward the tenant network slug so the runner joins the mesh
    // scoped to `tag:net-<slug>`. Omitted when `None` (today's unscoped join).
    // The scoped join TOKEN travels via the `TABBIFY_RUNNER_JOIN_TOKEN` env
    // (set in `spawn_runner`), never on the arg list — it is a credential.
    if let Some(network) = &spec.network {
        args.push("--network".into());
        args.push(network.as_str().into());
    }
    args
}

/// Spawn a `tabbify-runner` process DETACHED, persist its [`RunnerHandle`] to
/// `<runner_dir>/<uuid>.json`, and return the handle plus the [`Child`].
///
/// The returned [`Child`] is the orchestrator's handle to *track* the process
/// (e.g. await its exit in the health monitor, Task 2.4) — but because the
/// child is its own session leader (via `setsid`) and `kill_on_drop` is left
/// `false`, **dropping the handle does NOT kill the runner**.
///
/// The parent directory of `control_sock` must exist (the runner binds the
/// socket there). `runner_dir` must exist (the record is written there).
///
/// # Errors
/// - the `uuid` is not a valid UUID;
/// - the process fails to spawn (binary missing / not executable);
/// - persisting the [`RunnerHandle`] record fails.
pub async fn spawn_runner(spec: &SpawnSpec, runner_dir: &Path) -> Result<(RunnerHandle, Child)> {
    let parsed_uuid = Uuid::parse_str(&spec.uuid)
        .with_context(|| format!("invalid app uuid: {:?}", spec.uuid))?;
    let app_ula = derive_app_ula(parsed_uuid);

    let mut cmd = Command::new(&spec.runner_bin);
    cmd.args(build_args(spec));

    // Phase-2: pass the scoped node-join token to the runner via the environment
    // (NOT the arg list, so the credential never lands in `ps`/process args).
    // The runner reads it via clap `env = "TABBIFY_RUNNER_JOIN_TOKEN"`. When
    // `None`, EXPLICITLY clear the var on the child so an ambient token in the
    // supervisor's own env never leaks into an unscoped runner — today's
    // tokenless behavior is preserved.
    match &spec.runner_join_token {
        Some(token) => {
            cmd.env(crate::runner::config::RUNNER_JOIN_TOKEN_ENV, token);
        }
        None => {
            cmd.env_remove(crate::runner::config::RUNNER_JOIN_TOKEN_ENV);
        }
    }

    // The managed `tabbify.toml` travels via the `RUNNER_MANIFEST_TOML` env (an
    // env, not an arg: the toml is multi-line and would clutter `ps`). When
    // `None`, clear it on the child so an ambient value never leaks into a deploy
    // that has no managed config — today's hardcoded-default behavior is kept.
    match &spec.manifest_toml {
        Some(t) => {
            cmd.env("RUNNER_MANIFEST_TOML", t);
        }
        None => {
            cmd.env_remove("RUNNER_MANIFEST_TOML");
        }
    }

    // Detach: become a new session leader so the runner is not in the
    // supervisor's process group and survives the supervisor's death / signals.
    //
    // SAFETY: `pre_exec` runs in the forked child after `fork(2)` and before
    // `exec(2)`. In that window only async-signal-safe work is permitted (no
    // allocation, no locks). `setsid(2)` is async-signal-safe and is the entire
    // body of the closure. It can only fail with `EPERM` when the caller is
    // already a process-group leader — which the child (a fresh fork) never is —
    // so in practice it never fails here; we still surface any error as an
    // `io::Error` so the spawn fails loudly rather than silently un-detached.
    unsafe {
        cmd.pre_exec(|| {
            // SAFETY: async-signal-safe libc call; see the block comment above.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    // Detach stdio. stdin is always null (the runner reads no input). stdout +
    // stderr are redirected to a per-app append log under
    // `<data_dir>/runners/<uuid>.log` so a crashed / detached runner's output is
    // not lost (it would otherwise go to /dev/null). Logging is BEST-EFFORT: if
    // the file cannot be opened we fall back to `Stdio::null()` so an app start
    // never fails just because we could not open its log.
    cmd.stdin(Stdio::null());
    match open_runner_log(&spec.data_dir, &spec.uuid) {
        Ok(file) => {
            // The child needs an independent fd for stderr; `try_clone` dups the
            // underlying OS handle (separate offset, same append semantics).
            match file.try_clone() {
                Ok(file_err) => {
                    cmd.stdout(Stdio::from(file)).stderr(Stdio::from(file_err));
                    // `cmd` now owns both fds (they are dup'd into the child on
                    // spawn); nothing else holds the parent handles.
                }
                Err(e) => {
                    tracing::warn!(
                        uuid = %spec.uuid,
                        error = %e,
                        "could not clone runner log fd; runner stdio -> /dev/null"
                    );
                    cmd.stdout(Stdio::null()).stderr(Stdio::null());
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                uuid = %spec.uuid,
                error = %e,
                "could not open runner log file; runner stdio -> /dev/null"
            );
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
    }

    // NOTE: we intentionally leave `kill_on_drop` at its default (`false`).
    // Dropping the returned `Child` must NOT kill the detached runner.

    let child = cmd
        .spawn()
        .with_context(|| format!("spawn runner binary {:?}", spec.runner_bin))?;

    let pid = child
        .id()
        .context("spawned runner has no pid (already exited?)")?;

    let spawned_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let handle = RunnerHandle {
        uuid: spec.uuid.clone(),
        pid,
        control_sock: spec.control_sock.clone(),
        app_ula: app_ula.to_string(),
        parent: spec.parent.clone(),
        spawned_at,
        restart: Default::default(),
        // Carry the deployed ref through so a future respawn-from-record keeps
        // the same version. `None` on a fresh spawn = today's behavior.
        image_ref: spec.image_ref.clone(),
        // The runtime is no longer selectable — every app builds as Firecracker —
        // so nothing is threaded here. The field is retained on RunnerHandle only
        // so old on-disk records (which may carry a `requested_runtime`) still
        // deserialize; it is inert and never read for dispatch.
        requested_runtime: None,
        // Persist the tenant network slug so a respawn rejoins the same network
        // (`--network <slug>`). `None` on a fresh/unscoped spawn = today's
        // behavior.
        network: spec.network.clone(),
        // Persist the scoped join token so a supervisor-driven RESPAWN re-joins
        // the validating coordinator with the SAME token (the token is
        // long-lived, a 1-year TTL minted by the node, so it outlives the
        // runner's idle-outs/crashes). `None` on an unscoped spawn.
        runner_join_token: spec.runner_join_token.clone(),
    };

    handle
        .save(runner_dir)
        .with_context(|| format!("persist runner record for {}", spec.uuid))?;

    tracing::info!(
        uuid = %spec.uuid,
        pid,
        %app_ula,
        control_sock = %spec.control_sock.display(),
        "spawned detached runner"
    );

    Ok((handle, child))
}

/// Open (creating as needed) the per-app runner log at
/// `<data_dir>/runners/<uuid>.log` for APPEND, returning the file handle.
///
/// Creates the `runners/` subdirectory if missing. The caller redirects the
/// detached runner's stdout + stderr into this file (cloning the handle for the
/// second fd), so each (re)spawn's output is retained across restarts rather
/// than discarded to `/dev/null`. Append mode means a respawn does not clobber
/// the previous run's log.
fn open_runner_log(data_dir: &Path, uuid: &str) -> std::io::Result<std::fs::File> {
    let log_dir = data_dir.join("runners");
    std::fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join(format!("{uuid}.log"));
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn spec() -> SpawnSpec {
        SpawnSpec {
            runner_bin: PathBuf::from("/opt/tabbify/tabbify-runner"),
            uuid: "0191e7c2-1111-7222-8333-444455556666".to_owned(),
            control_sock: PathBuf::from("/run/tabbify/runners/x.sock"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: Some("fd5a:1f00:1::1".to_owned()),
            no_mesh: true,
            relay_url: None,
            relay_only: false,
            image_ref: None,
            manifest_toml: None,
            network: None,
            runner_join_token: None,
        }
    }

    /// argv carries the core flags the runner needs, in the form clap expects.
    #[test]
    fn build_args_includes_core_flags() {
        let args = build_args(&spec());
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        // Each flag is present and immediately followed by its value.
        for (flag, value) in [
            ("--uuid", "0191e7c2-1111-7222-8333-444455556666"),
            ("--control-sock", "/run/tabbify/runners/x.sock"),
            ("--s3-base-url", "http://s3.invalid"),
            ("--data-dir", "/var/lib/tabbify/data"),
            ("--parent", "fd5a:1f00:1::1"),
            ("--bind", "127.0.0.1:0"),
        ] {
            let idx = joined
                .iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("missing flag {flag} in {joined:?}"));
            assert_eq!(
                joined.get(idx + 1).map(String::as_str),
                Some(value),
                "flag {flag} should be followed by {value}"
            );
        }
        assert!(joined.iter().any(|a| a == "--no-mesh"), "got: {joined:?}");
    }

    /// In mesh mode no loopback `--bind`/`--no-mesh` is passed (the runner binds
    /// its own ULA); `--parent` is omitted when there is no parent.
    #[test]
    fn build_args_mesh_mode_has_no_bind_or_parent() {
        let mut s = spec();
        s.no_mesh = false;
        s.parent = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(!joined.iter().any(|a| a == "--no-mesh"), "got: {joined:?}");
        assert!(!joined.iter().any(|a| a == "--bind"), "got: {joined:?}");
        assert!(!joined.iter().any(|a| a == "--parent"), "got: {joined:?}");
    }

    /// When `image_ref` is set, the runner argv carries `--image-ref <ref>` so a
    /// respawn comes up on the deployed version.
    #[test]
    fn build_args_includes_image_ref_when_present() {
        let mut s = spec();
        s.image_ref = Some("[fd5a::1]:5000/a/b:sha".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let idx = joined
            .iter()
            .position(|a| a == "--image-ref")
            .unwrap_or_else(|| panic!("missing --image-ref in {joined:?}"));
        assert_eq!(
            joined.get(idx + 1).map(String::as_str),
            Some("[fd5a::1]:5000/a/b:sha"),
            "--image-ref must be followed by the ref"
        );
    }

    /// When `image_ref` is `None`, no `--image-ref` arg is emitted (today's
    /// default behavior is unchanged).
    #[test]
    fn build_args_omits_image_ref_when_none() {
        let mut s = spec();
        s.image_ref = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--image-ref"),
            "no --image-ref when None; got: {joined:?}"
        );
    }

    /// When `relay_url` is set, the runner argv carries `--mesh-relay-url <url>`
    /// so the runner routes its OWN mesh-join relay over the same `wss://`
    /// endpoint as the supervisor (the corporate-firewall escape hatch).
    #[test]
    fn build_args_includes_relay_url_when_present() {
        let mut s = spec();
        s.relay_url = Some("wss://relay.tabbify.io/v1/mesh/relay".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let idx = joined
            .iter()
            .position(|a| a == "--mesh-relay-url")
            .unwrap_or_else(|| panic!("missing --mesh-relay-url in {joined:?}"));
        assert_eq!(
            joined.get(idx + 1).map(String::as_str),
            Some("wss://relay.tabbify.io/v1/mesh/relay"),
            "--mesh-relay-url must be followed by the relay endpoint"
        );
    }

    /// When `relay_url` is `None`, no `--mesh-relay-url` arg is emitted (the
    /// runner derives the relay from the coordinator URL — today's default).
    #[test]
    fn build_args_omits_relay_url_when_none() {
        let mut s = spec();
        s.relay_url = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--mesh-relay-url"),
            "no --mesh-relay-url when None; got: {joined:?}"
        );
    }

    /// When `relay_only` is true, the runner argv carries the BARE
    /// `--mesh-relay-only` flag (no value) so the runner declares no reachable
    /// direct endpoint and its WG handshake converges over the relay.
    #[test]
    fn build_args_includes_relay_only_when_true() {
        let mut s = spec();
        s.relay_only = true;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            joined.iter().any(|a| a == "--mesh-relay-only"),
            "expected the bare --mesh-relay-only flag; got: {joined:?}"
        );
    }

    /// When `relay_only` is false (the default), no `--mesh-relay-only` arg is
    /// emitted — the runner keeps direct + hole-punch traversal.
    #[test]
    fn build_args_omits_relay_only_when_false() {
        let mut s = spec();
        s.relay_only = false;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--mesh-relay-only"),
            "no --mesh-relay-only when false; got: {joined:?}"
        );
    }

    /// Phase-2: when `network` is set, the runner argv carries `--network
    /// <slug>` so the runner joins the mesh scoped to `tag:net-<slug>`.
    #[test]
    fn build_args_includes_network_when_present() {
        let mut s = spec();
        s.network = Some("n_jpegxik72nng".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let idx = joined
            .iter()
            .position(|a| a == "--network")
            .unwrap_or_else(|| panic!("missing --network in {joined:?}"));
        assert_eq!(
            joined.get(idx + 1).map(String::as_str),
            Some("n_jpegxik72nng"),
            "--network must be followed by the network slug"
        );
    }

    /// When `network` is `None`, no `--network` arg is emitted (today's unscoped
    /// join is unchanged).
    #[test]
    fn build_args_omits_network_when_none() {
        let mut s = spec();
        s.network = None;
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--network"),
            "no --network when None; got: {joined:?}"
        );
    }

    /// The scoped node-join token is a CREDENTIAL: it MUST NOT appear on the
    /// runner's arg list (it travels via the `TABBIFY_RUNNER_JOIN_TOKEN` env so
    /// it never lands in `ps` output). Even with a token set, `build_args`
    /// carries neither the token value nor a token flag.
    #[test]
    fn build_args_never_carries_join_token() {
        let mut s = spec();
        s.runner_join_token = Some("super-secret-jwt".to_owned());
        s.network = Some("n_jpegxik72nng".to_owned());
        let args = build_args(&s);
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a.contains("super-secret-jwt")),
            "the join token must never appear on the arg list; got: {joined:?}"
        );
        assert!(
            !joined.iter().any(|a| a == "--runner-join-token"),
            "no --runner-join-token flag (token rides the env); got: {joined:?}"
        );
    }

    /// The runtime is no longer selectable per app — every app builds as
    /// Firecracker — so the runner argv never carries a `--runtime-override`
    /// flag (the flag and the threading were removed).
    #[test]
    fn build_args_never_emits_runtime_override() {
        let args = build_args(&spec());
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            !joined.iter().any(|a| a == "--runtime-override"),
            "runtime is fixed to firecracker; no --runtime-override must be emitted; got: {joined:?}"
        );
    }

    /// Write an executable shell wrapper that prints `out_text` to stdout and
    /// `err_text` to stderr, then exits 0. Returns its path. Used by the
    /// log-capture tests below: pointing `SpawnSpec.runner_bin` at it lets us
    /// drive the real detached spawn without a real `tabbify-runner`.
    fn write_echo_wrapper(dir: &Path, name: &str, out_text: &str, err_text: &str) -> PathBuf {
        use std::{io::Write as _, os::unix::fs::PermissionsExt as _};

        let wrapper = dir.join(name);
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            // `"$@"` is ignored; we only care about the captured stdio.
            writeln!(
                f,
                "#!/bin/sh\nprintf '%s\\n' '{out_text}'\nprintf '%s\\n' '{err_text}' 1>&2\n"
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();
        wrapper
    }

    /// A spawned runner's stdout AND stderr are captured to
    /// `<data_dir>/runners/<uuid>.log`.
    #[tokio::test]
    async fn spawn_runner_captures_stdout_and_stderr_to_log_file() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(
            dir.path(),
            "echo-runner.sh",
            "RUNNER_STDOUT_LINE",
            "RUNNER_STDERR_LINE",
        );

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-aaaa-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        // Await the wrapper so it has written and flushed before we read the log.
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path)
            .unwrap_or_else(|e| panic!("runner log {log_path:?} should exist: {e}"));
        assert!(
            contents.contains("RUNNER_STDOUT_LINE"),
            "log must contain captured stdout; got: {contents:?}"
        );
        assert!(
            contents.contains("RUNNER_STDERR_LINE"),
            "log must contain captured stderr; got: {contents:?}"
        );
    }

    /// Phase-2: the scoped node-join token is passed to the spawned runner via
    /// the `TABBIFY_RUNNER_JOIN_TOKEN` env (a credential kept off the arg list).
    /// We point the runner at a wrapper that prints that env var, then assert the
    /// value lands in the captured log.
    #[tokio::test]
    async fn spawn_runner_passes_join_token_via_env() {
        use std::{io::Write as _, os::unix::fs::PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        // A wrapper that echoes the token env var to stdout.
        let wrapper = dir.path().join("env-echo.sh");
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            writeln!(
                f,
                "#!/bin/sh\nprintf 'TOKEN=[%s]\\n' \"${{{env}}}\"\n",
                env = crate::runner::config::RUNNER_JOIN_TOKEN_ENV
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-dddd-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.runner_join_token = Some("scoped-runner-jwt".to_owned());
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("TOKEN=[scoped-runner-jwt]"),
            "the runner must receive the join token via TABBIFY_RUNNER_JOIN_TOKEN; got: {contents:?}"
        );
    }

    /// The spec's `manifest_toml` is passed to the spawned runner via the
    /// `RUNNER_MANIFEST_TOML` env so a connect-repo deploy's `[runtime]`/`[routes]`
    /// reach the runner's synthesized manifest. We point the runner at a wrapper
    /// that echoes that env var and assert the toml lands in the captured log.
    #[tokio::test]
    async fn spawn_runner_passes_manifest_toml_via_env() {
        use std::{io::Write as _, os::unix::fs::PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        let wrapper = dir.path().join("toml-echo.sh");
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            writeln!(
                f,
                "#!/bin/sh\nprintf 'TOML=[%s]\\n' \"${{RUNNER_MANIFEST_TOML}}\"\n"
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-2020-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.manifest_toml = Some("[app]\nname = \"sized\"\n".to_owned());
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("name = \"sized\""),
            "the runner must receive the managed toml via RUNNER_MANIFEST_TOML; got: {contents:?}"
        );
    }

    /// With no managed toml, `RUNNER_MANIFEST_TOML` is unset on the child (today's
    /// hardcoded-default behavior): the wrapper sees an empty value.
    #[tokio::test]
    async fn spawn_runner_omits_manifest_toml_env_when_none() {
        use std::{io::Write as _, os::unix::fs::PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        let wrapper = dir.path().join("toml-echo.sh");
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            writeln!(
                f,
                "#!/bin/sh\nprintf 'TOML=[%s]\\n' \"${{RUNNER_MANIFEST_TOML}}\"\n"
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-3030-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.manifest_toml = None;
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("TOML=[]"),
            "no managed toml → RUNNER_MANIFEST_TOML must be unset; got: {contents:?}"
        );
    }

    /// The spec's `runner_join_token` is PERSISTED into the saved
    /// [`RunnerHandle`] so a supervisor-driven RESPAWN can re-join the validating
    /// coordinator with the SAME token (instead of 401ing). We assert both the
    /// returned handle and the on-disk record carry the token.
    #[tokio::test]
    async fn spawn_runner_persists_join_token_into_handle() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(dir.path(), "noop.sh", "OUT", "ERR");

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-ffff-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.network = Some("n_jpegxik72nng".to_owned());
        s.runner_join_token = Some("jwt.runner.token".to_owned());
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        assert_eq!(
            handle.runner_join_token.as_deref(),
            Some("jwt.runner.token"),
            "the returned handle must carry the spec's join token"
        );
        let loaded = RunnerHandle::load(dir.path(), &s.uuid).unwrap().unwrap();
        assert_eq!(
            loaded.runner_join_token.as_deref(),
            Some("jwt.runner.token"),
            "the saved record must persist the join token for a respawn"
        );
    }

    /// A spawn with no token leaves the saved handle's `runner_join_token` as
    /// `None` (an unscoped runner — today's behavior).
    #[tokio::test]
    async fn spawn_runner_handle_token_none_when_unscoped() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(dir.path(), "noop.sh", "OUT", "ERR");

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-1010-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.runner_join_token = None;
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        assert!(
            handle.runner_join_token.is_none(),
            "an unscoped spawn must leave the handle's join token None"
        );
    }

    /// With no token, the env var is unset for the runner (today's behavior):
    /// the wrapper sees an empty value.
    #[tokio::test]
    async fn spawn_runner_omits_join_token_env_when_none() {
        use std::{io::Write as _, os::unix::fs::PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        let wrapper = dir.path().join("env-echo.sh");
        {
            let mut f = std::fs::File::create(&wrapper).unwrap();
            writeln!(
                f,
                "#!/bin/sh\nprintf 'TOKEN=[%s]\\n' \"${{{env}}}\"\n",
                env = crate::runner::config::RUNNER_JOIN_TOKEN_ENV
            )
            .unwrap();
        }
        let mut perm = std::fs::metadata(&wrapper).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrapper, perm).unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-eeee-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        s.runner_join_token = None;
        std::fs::create_dir_all(&s.data_dir).unwrap();

        let (_handle, mut child) = spawn_runner(&s, dir.path()).await.unwrap();
        child.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("TOKEN=[]"),
            "no token env must be set when runner_join_token is None; got: {contents:?}"
        );
    }

    /// A second spawn for the same uuid APPENDS to the existing log (both runs
    /// retained), it does not truncate.
    #[tokio::test]
    async fn spawn_runner_appends_to_existing_log() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper_a = write_echo_wrapper(dir.path(), "a.sh", "FIRST_RUN_OUT", "FIRST_RUN_ERR");
        let wrapper_b = write_echo_wrapper(dir.path(), "b.sh", "SECOND_RUN_OUT", "SECOND_RUN_ERR");

        let mut s = spec();
        s.uuid = "0191e7c2-bbbb-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = dir.path().join("data");
        std::fs::create_dir_all(&s.data_dir).unwrap();

        s.runner_bin = wrapper_a;
        let (_h1, mut c1) = spawn_runner(&s, dir.path()).await.unwrap();
        c1.wait().await.unwrap();

        s.runner_bin = wrapper_b;
        let (_h2, mut c2) = spawn_runner(&s, dir.path()).await.unwrap();
        c2.wait().await.unwrap();

        let log_path = s.data_dir.join("runners").join(format!("{}.log", s.uuid));
        let contents = std::fs::read_to_string(&log_path).unwrap();
        for needle in [
            "FIRST_RUN_OUT",
            "FIRST_RUN_ERR",
            "SECOND_RUN_OUT",
            "SECOND_RUN_ERR",
        ] {
            assert!(
                contents.contains(needle),
                "appended log must retain {needle}; got: {contents:?}"
            );
        }
    }

    /// If the log file cannot be opened, the spawn still succeeds (logging is
    /// best-effort and must never block an app from starting). Here `data_dir`
    /// is a FILE, so `runners/` cannot be created under it.
    #[tokio::test]
    async fn spawn_runner_falls_back_to_null_when_log_open_fails() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = write_echo_wrapper(dir.path(), "echo.sh", "OUT", "ERR");

        // data_dir points at an existing regular file → create_dir_all fails.
        let bogus_data = dir.path().join("not-a-dir");
        std::fs::write(&bogus_data, b"i am a file").unwrap();

        let mut s = spec();
        s.runner_bin = wrapper;
        s.uuid = "0191e7c2-cccc-7222-8333-444455556666".to_owned();
        s.control_sock = dir.path().join("x.sock");
        s.data_dir = bogus_data;

        let (_handle, mut child) = spawn_runner(&s, dir.path())
            .await
            .expect("spawn must succeed even when log dir cannot be created");
        child.wait().await.unwrap();
    }

    /// The prod binary path resolves next to the current executable (not the
    /// bare name), so a relative `cwd` can't break runner discovery.
    #[test]
    fn default_runner_bin_is_next_to_current_exe() {
        let resolved = default_runner_bin();
        assert_eq!(
            resolved.file_name().and_then(|n| n.to_str()),
            Some(RUNNER_BIN_NAME)
        );
        // When the current exe is resolvable the path is absolute (has a parent
        // dir); we don't assert the exact dir (varies by test runner layout).
        if std::env::current_exe().is_ok() {
            assert!(
                resolved.parent().is_some(),
                "resolved runner path should sit in a directory: {resolved:?}"
            );
        }
    }
}
