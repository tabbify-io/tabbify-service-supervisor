//! F1 — per-FC CPU cap + kill-able, supervisor-independent scope.
//!
//! Each Firecracker child is launched inside its OWN transient systemd
//! `--scope` under a shared `tabbify-fc.slice`, instead of a bare child of the
//! runner. This buys two compounding properties (FC-resource-audit #93, R1):
//!
//! 1. **A hard per-guest CPU ceiling** (`CPUQuota`) + relative weight
//!    (`CPUWeight`) enforced by the kernel cgroup, so a runaway or busy guest
//!    can never take a whole box; the `tabbify-fc.slice` aggregate ceiling
//!    (declared in the NixOS module) always leaves the supervisor's own mesh
//!    data-plane CPU headroom.
//! 2. **A tracked, kill-able cgroup handle that SURVIVES a dead supervisor.**
//!    The scope is a systemd unit named deterministically from the app uuid /
//!    build id, so `systemctl stop tabbify-fc-<uuid>.scope` is an independent,
//!    race-free reaper primitive (no PID-reuse hazard) that works even when the
//!    supervisor is gone — directly attacking the "supervisor crash-loop leaves
//!    orphaned spinning FCs" cascade.
//!
//! This module is **pure + cross-platform** (no Linux-only imports, no I/O): it
//! only BUILDS the `systemd-run` argv and the scope name, and DECIDES whether a
//! scope wrapper applies. The actual spawn (and the bare-spawn fallback off
//! systemd / on macOS / in tests) lives in the Linux runtime + build VM. Keeping
//! the logic here means the argv + fallback decision are unit-tested on the
//! macOS CI host exactly as they run on a NixOS worker.
//!
//! The module is consumed only from the `#[cfg(target_os = "linux")]` runtime,
//! so on macOS the compiler sees the non-test functions as unused.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

/// The shared slice every Firecracker scope lives under. Declared (with an
/// aggregate `CPUQuota` ceiling that reserves host headroom for the supervisor)
/// in the NixOS module; the supervisor only NAMES it here when wrapping a spawn.
pub const FC_SLICE: &str = "tabbify-fc.slice";

/// CPU-bound knobs for the per-FC systemd scope. Defaults bound ONE serving
/// guest to ~1 core and a build guest to ~2 cores; the AGGREGATE ceiling that
/// reserves host headroom is the slice's own `CPUQuota` (NixOS, not here), so a
/// single guest's quota and the box-wide cap are decoupled. All values are
/// operator-tunable (env / CLI on [`crate::config::FcConfig`]) — NOT hardcoded
/// magic — because the right numbers are a capacity-planning call on the sole
/// worker (#93 owner-decision flag).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuScopeCfg {
    /// Per-SERVING-guest `CPUQuota` percent. `100` == one full core. Bounds a
    /// runaway/busy 1-vCPU serving app to a single core.
    pub serving_quota_pct: u32,
    /// Per-BUILD-VM `CPUQuota` percent. Builds are 2-vCPU and compile-heavy, so
    /// the default is higher (`200` == two cores) — but still capped so a wedged
    /// build can't take the whole box.
    pub build_quota_pct: u32,
    /// `CPUWeight` (cgroup-v2 relative share, 1..=10000, default systemd 100).
    /// Lower than the supervisor's own (default 100) so under contention the
    /// supervisor + mesh data-plane win the scheduler — the box stays steerable
    /// and never goes "dark" under guest load.
    pub weight: u32,
}

impl Default for CpuScopeCfg {
    fn default() -> Self {
        Self {
            // 1 core per serving guest (serving FCs are 1 vCPU by default).
            serving_quota_pct: 100,
            // 2 cores per build VM (build VMs are 2 vCPU; compile-heavy).
            build_quota_pct: 200,
            // Below the supervisor's default 100 so it out-prioritises guests
            // under contention (the box must never starve its own mesh plane).
            weight: 80,
        }
    }
}

/// Which kind of FC is being launched — selects the quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FcKind {
    /// A serving app microVM (per-uuid identity).
    Serving,
    /// The ephemeral build VM (fixed `fc-bld0` identity).
    Build,
}

impl FcKind {
    fn quota_pct(self, cfg: &CpuScopeCfg) -> u32 {
        match self {
            FcKind::Serving => cfg.serving_quota_pct,
            FcKind::Build => cfg.build_quota_pct,
        }
    }
}

/// Sanitize an id into a systemd-unit-name-safe token: lower-case, only
/// `[a-z0-9-]`, every other byte → `-`. Mirrors [`super::pidfile`]'s sanitizer
/// so the scope name and the pidfile name derive identically from the uuid.
fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Deterministic scope-unit name for `id` (an app uuid, or `build-<seq>`).
/// Format: `tabbify-fc-<sanitized_id>.scope`. This is the kill handle:
/// `systemctl stop <this>` tears the guest down even with the supervisor dead.
#[must_use]
pub fn scope_name(id: &str) -> String {
    format!("tabbify-fc-{}.scope", sanitize(id))
}

/// Build the `systemd-run` argv that wraps `fc_bin <fc_args...>` in a transient
/// CPU-capped scope under [`FC_SLICE`].
///
/// Shape (man systemd-run): `systemd-run --scope --collect
/// --unit=<scope> --slice=tabbify-fc.slice -p CPUQuota=<n>% -p CPUWeight=<w>
/// -- <fc_bin> <fc_args...>`.
///
/// * `--scope` — run as a transient SCOPE (the command stays our direct child,
///   foreground/synchronous), NOT a forking service, so the existing
///   `Child`/`waitpid`-based exit-detection keeps working; systemd just adds the
///   cgroup + quota around it.
/// * `--collect` — garbage-collect the unit even if it fails, so a crashed guest
///   doesn't leave a `failed` scope lingering in `systemctl`.
/// * `--unit=<scope>` — the deterministic, uuid-derived name → the kill handle.
///
/// Returned as an owned `Vec<String>` (argv[0] = `systemd-run`); the caller
/// feeds argv[0] to `Command::new` and the rest as args. Pure — no spawn, no I/O.
#[must_use]
pub fn systemd_run_argv(
    scope: &str,
    cfg: &CpuScopeCfg,
    kind: FcKind,
    fc_bin: &str,
    fc_args: &[String],
) -> Vec<String> {
    let mut argv = vec![
        "systemd-run".to_owned(),
        "--scope".to_owned(),
        "--collect".to_owned(),
        format!("--unit={scope}"),
        format!("--slice={FC_SLICE}"),
        "-p".to_owned(),
        format!("CPUQuota={}%", kind.quota_pct(cfg)),
        "-p".to_owned(),
        format!("CPUWeight={}", cfg.weight),
        "--".to_owned(),
        fc_bin.to_owned(),
    ];
    argv.extend(fc_args.iter().cloned());
    argv
}

/// Should we wrap the FC spawn in a systemd scope on THIS host?
///
/// `true` only when BOTH a usable `systemd-run` is present AND the host is
/// actually running systemd as PID 1 (a transient unit needs a live manager to
/// register with). Off-systemd (macOS dev host, a plain container, CI) → `false`
/// → the caller bare-spawns firecracker directly, preserving the legacy
/// `Child`-as-firecracker lifecycle so unit/integration tests stay host-agnostic.
///
/// The probe is injected so this decision is unit-testable without a real
/// systemd: production wires [`host_has_systemd`].
#[must_use]
pub fn should_wrap(have_systemd_run: bool, systemd_is_pid1: bool) -> bool {
    have_systemd_run && systemd_is_pid1
}

/// Production host-probe for [`should_wrap`]: is this box running systemd as the
/// init system? Checks `/run/systemd/system` — the canonical, documented marker
/// (`sd_booted(3)`). Cheap, no fork. Off-Linux always `false`.
#[must_use]
pub fn host_has_systemd() -> bool {
    cfg!(target_os = "linux") && std::path::Path::new("/run/systemd/system").is_dir()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn scope_name_is_deterministic_and_sanitized() {
        assert_eq!(
            scope_name("0191e7c2-1111-7222-8333-444455556666"),
            "tabbify-fc-0191e7c2-1111-7222-8333-444455556666.scope"
        );
        // Uppercase + slashes/colons sanitized exactly like the pidfile.
        assert_eq!(scope_name("My/App:v2"), "tabbify-fc-my-app-v2.scope");
        // The build identity gets its own deterministic scope.
        assert_eq!(scope_name("build-65534"), "tabbify-fc-build-65534.scope");
    }

    #[test]
    fn argv_wraps_fc_under_slice_with_serving_quota() {
        let cfg = CpuScopeCfg::default();
        let scope = scope_name("uuid-a");
        let fc_args = vec![
            "--api-sock".to_owned(),
            "/tmp/firecracker-fc-tap0.sock".to_owned(),
        ];
        let argv = systemd_run_argv(&scope, &cfg, FcKind::Serving, "firecracker", &fc_args);

        assert_eq!(argv[0], "systemd-run");
        assert!(argv.contains(&"--scope".to_owned()));
        assert!(argv.contains(&"--collect".to_owned()));
        assert!(argv.contains(&"--unit=tabbify-fc-uuid-a.scope".to_owned()));
        assert!(argv.contains(&"--slice=tabbify-fc.slice".to_owned()));
        // Serving quota = 100% (one core).
        assert!(argv.contains(&"CPUQuota=100%".to_owned()));
        assert!(argv.contains(&"CPUWeight=80".to_owned()));

        // The `--` separator precedes the firecracker binary + its verbatim args,
        // and nothing reorders/loses the FC argv.
        let sep = argv.iter().position(|a| a == "--").unwrap();
        assert_eq!(argv[sep + 1], "firecracker");
        assert_eq!(argv[sep + 2], "--api-sock");
        assert_eq!(argv[sep + 3], "/tmp/firecracker-fc-tap0.sock");
        // Everything FC-specific is AFTER the separator (no FC arg leaks into the
        // systemd-run option block where it'd be misparsed).
        assert!(!argv[..sep].iter().any(|a| a == "--api-sock"));
    }

    #[test]
    fn build_kind_uses_the_build_quota() {
        let cfg = CpuScopeCfg::default();
        let argv = systemd_run_argv(
            &scope_name("build-65534"),
            &cfg,
            FcKind::Build,
            "firecracker",
            &["--api-sock".to_owned(), "/tmp/x.sock".to_owned()],
        );
        // Build VMs get the higher (2-core) quota, never the serving one.
        assert!(argv.contains(&"CPUQuota=200%".to_owned()));
        assert!(!argv.contains(&"CPUQuota=100%".to_owned()));
    }

    #[test]
    fn quotas_track_the_configured_values_not_magic_constants() {
        let cfg = CpuScopeCfg {
            serving_quota_pct: 150,
            build_quota_pct: 400,
            weight: 33,
        };
        let serving =
            systemd_run_argv(&scope_name("u"), &cfg, FcKind::Serving, "fc", &[]);
        assert!(serving.contains(&"CPUQuota=150%".to_owned()));
        assert!(serving.contains(&"CPUWeight=33".to_owned()));
        let build = systemd_run_argv(&scope_name("b"), &cfg, FcKind::Build, "fc", &[]);
        assert!(build.contains(&"CPUQuota=400%".to_owned()));
    }

    #[test]
    fn should_wrap_only_when_systemd_run_and_pid1_systemd() {
        assert!(should_wrap(true, true));
        // Missing either half → bare spawn (off-systemd dev/CI/macOS).
        assert!(!should_wrap(false, true));
        assert!(!should_wrap(true, false));
        assert!(!should_wrap(false, false));
    }

    #[test]
    fn default_cfg_bounds_serving_to_one_core_and_build_to_two() {
        let cfg = CpuScopeCfg::default();
        assert_eq!(cfg.serving_quota_pct, 100, "1 serving guest ⇒ ~1 core cap");
        assert_eq!(cfg.build_quota_pct, 200, "1 build VM ⇒ ~2 core cap");
        // Weight below the supervisor's default 100 so the box stays steerable.
        assert!(cfg.weight < 100);
    }
}
