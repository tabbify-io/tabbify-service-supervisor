//! Linux microVM runtime: owns a firecracker child process + tap, configures
//! it via its unix-socket REST API, and proxies HTTP into the guest.

#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use http::Request;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

use super::pidfile;
use super::protocol::{
    boot_source_body, instance_start_body, machine_config_body, network_iface_body, pause_body,
    proxy_request, read_http_status, resolve_port, resolve_vcpus, resume_body, rootfs_drive_body,
    snapshot_create_body, snapshot_load_body,
};
use super::snapshot;
use super::{FcConfig, kvm_available};
use crate::manifest::Runtime;
use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

/// How long to wait for the guest app's HTTP server to come up after boot.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-poll HTTP timeout for the guest-app readiness probe. Each GET attempt is
/// bounded by this; the overall budget is [`READY_TIMEOUT`].
const READY_POLL: Duration = Duration::from_millis(250);
/// Readiness-probe backoff: first sleep between polls.
const READY_BACKOFF_START: Duration = Duration::from_millis(50);
/// Readiness-probe backoff: cap on the sleep between polls.
const READY_BACKOFF_CAP: Duration = Duration::from_secs(2);
/// How long to wait for one firecracker API call over the unix socket.
const API_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to allow `PUT /snapshot/create` — the guest's ENTIRE RAM is copied
/// to disk, which on a busy worker far exceeds the 5s [`API_TIMEOUT`]. With the
/// old 5s budget the create timed out, left the VM PAUSED, and the recovery
/// resume then also timed out (FC still flushing) → the new runtime never became
/// healthy → the deploy swap aborted (TAB-10). Give the memory write room.
const SNAPSHOT_CREATE_TIMEOUT: Duration = Duration::from_secs(180);
/// Total budget for [`FirecrackerRuntime::ensure_resumed`] to confirm the VM is
/// running after a snapshot attempt. The snapshot PAUSES the VM; cold_boot MUST
/// NOT return it paused (a paused VM fails the swap health-gate). Retry the
/// resume on this budget so a momentarily-busy FC (still flushing the snapshot)
/// is waited out rather than stranding the deploy.
const RESUME_ENSURE_TIMEOUT: Duration = Duration::from_secs(60);
/// Delay between resume attempts in [`FirecrackerRuntime::ensure_resumed`].
const RESUME_ENSURE_POLL: Duration = Duration::from_secs(2);

/// Env flag enabling live-boot debugging: when truthy, the firecracker child's
/// stdout+stderr (including the guest serial console, `console=ttyS0`) are
/// appended to `<data_dir>/fc/<uuid>.console.log` instead of discarded, so a
/// guest kernel panic / boot failure can be inspected post-mortem.
const FC_DEBUG_ENV: &str = "SUPERVISOR_FC_DEBUG";

/// Read [`FC_DEBUG_ENV`] and decide whether console capture is on. Truthy
/// values: `1`, `true`, `yes`, `on` (case-insensitive). Anything else (incl.
/// unset) keeps the default silent (`/dev/null`) behavior.
fn fc_debug_enabled() -> bool {
    std::env::var(FC_DEBUG_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Decide the firecracker child's `(stdout, stderr)` redirection.
///
/// Default (debug flag off, or no console path available): both `/dev/null`
/// — byte-for-byte identical to the historical behavior. When debugging is
/// enabled AND a `console_log` path is supplied, the file is opened in append
/// mode (its parent dir created best-effort) and both streams are pointed at
/// it so the serial console is captured. If the file can't be opened we log a
/// warning and fall back to `/dev/null` (never fail the boot over logging).
pub(crate) fn console_stdio(console_log: Option<&Path>) -> (Stdio, Stdio) {
    let null = || (Stdio::null(), Stdio::null());
    if !fc_debug_enabled() {
        return null();
    }
    let Some(path) = console_log else {
        return null();
    };
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(path = %path.display(), error = %e, "fc debug: cannot create console-log dir; logging to /dev/null");
            return null();
        }
    }
    let open = || OpenOptions::new().create(true).append(true).open(path);
    match (open(), open()) {
        (Ok(out), Ok(err)) => {
            tracing::info!(console_log = %path.display(), "fc debug: capturing microVM stdout+stderr (serial console)");
            (Stdio::from(out), Stdio::from(err))
        }
        (out, err) => {
            let e = out.err().or(err.err());
            tracing::warn!(path = %path.display(), error = ?e, "fc debug: cannot open console log; logging to /dev/null");
            null()
        }
    }
}

/// `/30` link slots reserved for SERVING VMs in the tap `/16`. The build VM
/// occupies the very top slot ([`super::build_vm::BUILD_SEQ`] = `0xFFFF/4 - 1`
/// = 16382), so serving VMs hash into `[0, SERVING_LINK_SLOTS)` and never land
/// on the build VM's `/30`.
const SERVING_LINK_SLOTS: u32 = 16_382;

/// Deterministic firecracker host identity from a `key` — replaces the old
/// process-global `VM_SEQ` counter. The key is the app `uuid` on a cold start
/// and `"uuid:reff"` on a deploy, so the new microVM of a zero-downtime swap
/// gets a DIFFERENT tap than the old one it replaces (different `reff`) yet is
/// still unique per app (the `uuid` is in the key).
///
/// The previous `VM_SEQ` was a `static AtomicU32::new(0)` LIVING IN THE RUNNER
/// PROCESS. Since every app runs in its OWN `tabbify-runner` process, the
/// counter started at 0 in each → **every app booted its microVM on `fc-tap0`
/// / `/tmp/firecracker-fc-tap0.sock`**, so two concurrent apps (and a runner's
/// own respawns vs. orphaned firecrackers) collided on the same tap device +
/// api-socket. A new boot's `ip link del fc-tap0` then ripped the tap out from
/// under a live microVM → unhealthy → monitor crash-loop. (`app_ula` was
/// already per-uuid, so only the host-side plumbing collided.)
///
/// Deriving the identity from the key (same `blake3` source as
/// [`crate::app_ula`]) makes it STABLE and collision-free in practice:
/// - `tap_name`: `fc-<48-bit blake3 hex>` — 15 chars, the IFNAMSIZ limit; also
///   names the `/tmp/firecracker-<tap>.sock` api socket. 48 bits ⇒ two distinct
///   keys effectively never share a tap device or socket.
/// - `link_idx`: `hash % SERVING_LINK_SLOTS` — the `/30` + MAC index fed to
///   [`derive_link_ips`] / [`derive_guest_mac`]. Only 14 bits (the `/16` holds
///   16384 `/30`s), so a same-`link_idx` (different-`tap_name`) clash is rare
///   and surfaces as a hard `ip addr add` duplicate-address error rather than
///   silent corruption.
pub(crate) fn fc_identity_for_key(key: &str) -> (String, u32) {
    let digest = blake3::hash(key.as_bytes());
    let b = digest.as_bytes();
    let hash48: u64 = (u64::from(b[0]) << 40)
        | (u64::from(b[1]) << 32)
        | (u64::from(b[2]) << 24)
        | (u64::from(b[3]) << 16)
        | (u64::from(b[4]) << 8)
        | u64::from(b[5]);
    // Tap name comes from the CROSS-PLATFORM single source of truth so the F2.2
    // orphan sweep reconstructs the exact same `/tmp/firecracker-<tap>.sock` as
    // the spawn path (a drift here would make the sweep mis-correlate a LIVE FC).
    let tap_name = super::fc_tap_name_for_key(key);
    debug_assert_eq!(tap_name, format!("fc-{hash48:012x}"));
    let link_idx = u32::try_from(hash48 % u64::from(SERVING_LINK_SLOTS)).unwrap_or(0);
    (tap_name, link_idx)
}

/// Probe type: given a `host:port` string returns `true` iff the guest app is
/// reachable. Production `health()` does a real HTTP GET to `guest_base`; this
/// injectable seam lets unit tests fake the result so no real microVM is
/// needed.
#[cfg(test)]
type TcpProbe = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// A booted Firecracker microVM hosting one app. Owns the firecracker child
/// process + the host tap device; [`Drop`] tears both down.
pub struct FirecrackerRuntime {
    /// The firecracker child. `Option` so [`Drop`] can take it and spawn a
    /// kill+reap (so no `<defunct>` zombie lingers). `Some` while alive.
    child: Option<Child>,
    tap_name: String,
    api_sock: PathBuf,
    /// F1 (audit #93): the transient systemd SCOPE name the firecracker child
    /// runs in (`tabbify-fc-<uuid>.scope`), when this host wrapped the spawn in
    /// `systemd-run --scope` for a host-enforced CPU cap. `None` off-systemd
    /// (macOS dev / CI / a plain container) where the child was bare-spawned and
    /// `child` IS firecracker directly. When `Some`, `child` is the `systemd-run`
    /// FOREGROUND wrapper (synchronous → it lives exactly as long as the FC), and
    /// teardown must `systemctl stop` the scope (killing the wrapper PID alone
    /// would orphan the FC INTO the now-supervisor-independent scope).
    fc_scope: Option<String>,
    /// `http://<guest_ip>:<app_port>` — the base the proxy targets.
    guest_base: String,
    /// The guest's IPv4 address on its /30 tap. Used by the L4 SSH forwarder
    /// (`guest_ssh_addr`) to expose `guest_ip:2222` via `[app_ula]:2222`.
    guest_ip: Ipv4Addr,
    client: reqwest::Client,
    /// The per-uuid snapshot cache dir (`<data_dir>/apps/<uuid>/cache`) this VM
    /// writes its snapshot to. `None` for the bare `launch` entry (tests /
    /// single-VM callers) and the build VM — those never snapshot. `Some` on the
    /// production `launch_with_uuid` path so `AppRuntime::snapshot()` (the
    /// `Cmd::Snapshot` refresh) knows where to put `snap.vmstate` / `snap.mem`.
    snapshot_cache_dir: Option<PathBuf>,
    /// The OCI image ref this VM booted from, stamped into `.snapshot_ref` after
    /// a successful `snapshot()` so a later warm restore is invalidated when the
    /// deployed image changes (cache is keyed by uuid only — see
    /// `snapshot::ref_matches`). `None` mirrors `snapshot_cache_dir`.
    image_ref: Option<String>,
    /// The #106 env/cap fingerprint this VM's `/init` was baked with, stamped into
    /// `.snapshot_env` alongside `.snapshot_ref` after a successful `snapshot()`.
    /// A later warm restore is invalidated when the effective env/cap set changes
    /// even though the image did NOT — e.g. a workspace `add_repo` adds a clone
    /// cap (same image, new `/init`), so the guest MUST cold-boot to re-run its
    /// broker's boot-clone (#108; see `snapshot::restore_matches`). `None` mirrors
    /// `snapshot_cache_dir` (the bare `launch` / build VM never restores).
    env_hash: Option<String>,
    /// Is this a WORKSPACE VM (vs a regular app / dev-FC)? Set true only on the
    /// workspace spawn path (detected via the workspace marker env). Gates the
    /// GAP#4 pre-snapshot scrub: a workspace holds provider creds in the broker's
    /// RAM + tmpfs and takes a Full (RAM-freezing) snapshot, so `snapshot()` MUST
    /// drop those creds via the in-guest broker before pausing — and ABORT the
    /// snapshot if the scrub fails. A non-workspace FC has no broker / no creds.
    is_workspace: bool,
    /// Test-only injectable reachability probe. Production leaves this `None`
    /// and `health()` does a real HTTP GET to `guest_base`; tests substitute a
    /// closure via [`Self::with_probe_for_test`] so no real microVM is needed.
    #[cfg(test)]
    probe: Option<TcpProbe>,
    /// Test-only override for the pre-snapshot scrub target base URL (e.g.
    /// `http://127.0.0.1:<ephemeral>`). Production leaves this `None` and
    /// `pre_snapshot_scrub` dials the real `guest_ip:8732`; a test points it at a
    /// local stub server so the fail-closed semantics are exercised without a VM.
    #[cfg(test)]
    scrub_base_override: Option<String>,
}

impl FirecrackerRuntime {
    /// Boot `rootfs` as a microVM and wait for its app HTTP server.
    ///
    /// Steps (design §4): KVM guard → allocate tap + /30 → spawn
    /// `firecracker --api-sock` → configure via the unix-socket REST API
    /// (machine-config, boot-source, rootfs drive, eth0 tap, InstanceStart)
    /// → poll the guest app until ready.
    ///
    /// # Errors
    /// `!kvm_available()`, tap setup failure, firecracker spawn failure, any
    /// REST configuration call failing, or the guest app not answering
    /// within [`READY_TIMEOUT`].
    pub async fn launch(rootfs: &Path, rt: &Runtime, cfg: &FcConfig) -> Result<Self> {
        // No per-app uuid context here (the bare entry, used by tests + simple
        // single-VM callers): a fixed identity is fine. The per-uuid production
        // path is `launch_with_uuid`.
        // The bare entry carries no deploy egress allow-list → `None` keeps the
        // legacy unrestricted egress. Never a workspace (no creds to scrub). No
        // cache dir ⇒ never snapshots ⇒ no env fingerprint to stamp (`None`).
        Self::cold_boot(
            rootfs,
            rt,
            cfg,
            None,
            None,
            "fc-launch-default",
            None,
            None,
            false,
            None,
        )
        .await
    }

    /// Cold-boot a microVM. `cache_dir` — when `Some`, a snapshot is taken
    /// after first boot (best-effort) and stored there for future warm starts.
    /// `console_log` — when `Some` and `SUPERVISOR_FC_DEBUG` is truthy, the
    /// firecracker child's stdout+stderr are appended there; otherwise both go
    /// to `/dev/null` (the default, unchanged behavior).
    /// `image_ref` — when `Some` AND a snapshot is created, the ref is recorded
    /// in `<cache_dir>/.snapshot_ref` so a later warm restore can be invalidated
    /// if the deployed image_ref changes (the snapshot cache is keyed by UUID
    /// only — see [`snapshot::ref_matches`]).
    /// `env_hash` — when `Some` AND a snapshot is created, the #106 env/cap
    /// fingerprint is recorded in `<cache_dir>/.snapshot_env` alongside the ref so
    /// a later warm restore is ALSO invalidated when the env/cap set changes on
    /// an unchanged image (a workspace `add_repo`; see [`snapshot::restore_matches`]).
    #[allow(clippy::too_many_arguments)]
    async fn cold_boot(
        rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        cache_dir: Option<&Path>,
        console_log: Option<&Path>,
        vm_key: &str,
        image_ref: Option<&str>,
        egress_allow: Option<&[String]>,
        is_workspace: bool,
        env_hash: Option<&str>,
    ) -> Result<Self> {
        if !kvm_available() {
            bail!("firecracker runtime requires Linux + /dev/kvm (/dev/kvm not R/W-openable)");
        }
        if !rootfs.is_file() {
            bail!("firecracker rootfs not found at {}", rootfs.display());
        }

        // Host identity (tap/api-sock/link/MAC) from the `uuid:reff` key —
        // STABLE per (app, version), collision-free across concurrent apps,
        // respawns, AND the old/new VMs of a zero-downtime swap (see
        // `fc_identity_for_key`).
        let (tap_name, link_idx) = fc_identity_for_key(vm_key);
        let (host_ip, guest_ip) = derive_link_ips(&cfg.tap_subnet, link_idx)
            .with_context(|| format!("derive /30 from subnet {}", cfg.tap_subnet))?;
        let guest_mac = derive_guest_mac(link_idx);
        let guest_base = format!("http://{guest_ip}:{}", resolve_port(rt, cfg));
        tracing::debug!(
            link_idx,
            %host_ip,
            %guest_ip,
            %tap_name,
            %guest_mac,
            %guest_base,
            "fc cold boot: derived /30 link + tap + guest MAC"
        );

        // Host tap + /30 link (design §4.2). Best-effort cleanup of a stale
        // tap of the same name first (ignore failure — it may not exist).
        let _ = run_ip(&["link", "del", &tap_name]).await;
        setup_tap(&tap_name, host_ip).await.map_err(|e| {
            tracing::error!(
                %tap_name, %host_ip, error = %e,
                "fc cold boot: tap setup failed (need CAP_NET_ADMIN/root for `ip tuntap`; EPERM ⇒ run supervisor with the net-admin capability)"
            );
            e
        })?;
        tracing::debug!(%tap_name, %host_ip, "fc cold boot: tap up");

        // Guest egress (best-effort): forward + SNAT the tap subnet so the
        // in-VM supervisor can reach the public mesh coordinator/relay. When an
        // egress allow-list is supplied (Track 7), `setup_guest_nat` installs
        // deny-by-default + allowed-host rules instead of the blanket-ACCEPT.
        setup_guest_nat(&tap_name, &cfg.tap_subnet, egress_allow).await;

        // Spawn firecracker with its API socket. Clean any stale socket.
        let api_sock = PathBuf::from(format!("/tmp/firecracker-{tap_name}.sock"));
        let _ = std::fs::remove_file(&api_sock);
        let (stdout, stderr) = console_stdio(console_log);
        // F1 (audit #93): wrap in a per-FC CPU-capped systemd scope. The scope
        // id is `vm_key` (`uuid:reff`) — STABLE per (app, version) and distinct
        // from the old/new VMs of a zero-downtime swap, exactly like the tap, so
        // both coexist with their own scope. Off-systemd → bare spawn (scope=None).
        let fc_args = vec![
            "--api-sock".to_owned(),
            api_sock.to_string_lossy().into_owned(),
        ];
        let (child, fc_scope) = spawn_firecracker(
            cfg,
            super::cpu_scope::FcKind::Serving,
            vm_key,
            &fc_args,
            Stdio::null(),
            stdout,
            stderr,
        )?;

        let me = Self {
            child: Some(child),
            tap_name,
            api_sock: api_sock.clone(),
            fc_scope,
            guest_base,
            guest_ip,
            client: reqwest::Client::new(),
            snapshot_cache_dir: cache_dir.map(Path::to_path_buf),
            image_ref: image_ref.map(str::to_owned),
            env_hash: env_hash.map(str::to_owned),
            is_workspace,
            #[cfg(test)]
            probe: None,
            #[cfg(test)]
            scrub_base_override: None,
        };

        // Configure + boot the VM, then wait for the guest app. On any
        // failure `me` drops → child killed + tap deleted.
        me.configure_and_boot(rootfs, rt, cfg, &guest_ip, &host_ip, &guest_mac)
            .await?;
        me.wait_until_ready().await?;

        // Best-effort snapshot after first boot. A failure is logged and
        // does NOT fail the launch — the VM continues to serve cold. On a
        // successful create, record the image_ref the snapshot was taken from so
        // a later warm restore is invalidated when the deployed image changes
        // (snapshot cache is keyed by UUID only — see `snapshot::ref_matches`).
        // `is_suppressed`: dev-sessions mark their cache dir `.no-snapshot`
        // because the guest `/init` clones `/workspace` ASYNC — a snapshot here
        // (right after the app port answers) would freeze a pre-/mid-clone
        // rootfs, and a later warm-restore would resurrect an EMPTY /workspace.
        // Suppressed ⇒ never snapshot ⇒ every (re)launch cold-boots + re-clones.
        if let Some(dir) = cache_dir {
            if super::snapshot_decision::should_snapshot_on_cold_boot(
                snapshot::files_present(dir),
                snapshot::is_suppressed(dir),
            ) {
                me.try_create_snapshot(dir).await;
                if let (Some(reff), true) = (image_ref, snapshot::files_present(dir)) {
                    snapshot::write_ref(dir, reff);
                    // Stamp the env/cap fingerprint alongside the ref so a later
                    // restore is gated on BOTH (a workspace `add_repo` changes the
                    // env, not the image — #108). Paired with the ref write so a
                    // restore never sees one companion without the other.
                    if let Some(eh) = env_hash {
                        snapshot::write_env(dir, eh);
                    }
                }
            }
        }

        Ok(me)
    }

    /// Launch a microVM for `uuid` running image `reff`, in one of two modes.
    ///
    /// `is_swap == false` (COLD START — first boot / monitor respawn):
    /// 1. Kill any stale VM recorded in the per-uuid pidfile.
    /// 2. Warm-restore from the per-uuid snapshot if present, else cold boot.
    /// 3. A cold boot writes a snapshot so later restarts are warm.
    ///
    /// `is_swap == true` (DEPLOY — zero-downtime swap to a NEW image):
    /// 1. Do NOT reconcile-kill: the OLD microVM keeps serving until
    ///    `perform_swap` flips to this new one and drains it.
    /// 2. Clear the (old-image) snapshot and COLD-boot `reff`; the host tap is
    ///    `uuid:reff`-derived, distinct from the old VM's, so they coexist.
    ///
    /// # Notes
    /// Snapshots are host-kernel + CPU-template specific. This supervisor
    /// creates and consumes them on the same host, so they are always
    /// compatible. Cross-host snapshot reuse is NOT supported.
    ///
    /// # Errors
    /// See [`Self::launch`].
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_with_uuid(
        rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        uuid: &str,
        reff: &str,
        data_dir: &Path,
        is_swap: bool,
        egress_allow: Option<&[String]>,
        is_workspace: bool,
        env_hash: &str,
    ) -> Result<Self> {
        // Per-app paths (pidfile / snapshot cache / console) are keyed on the
        // UUID, NOT the version. The firecracker HOST identity (tap / api-sock /
        // /30 link), by contrast, is keyed on `uuid:reff` so a deploy's NEW
        // microVM and the OLD one it replaces get DISTINCT taps and can coexist
        // during the zero-downtime swap.
        let vm_key = format!("{uuid}:{reff}");
        let cache_dir = data_dir.join("apps").join(uuid).join("cache");
        // Per-app console log: <data_dir>/fc/<uuid>.console.log (only written
        // when SUPERVISOR_FC_DEBUG is truthy — see `console_stdio`).
        let console_log = pidfile::console_log_path(data_dir, uuid);

        let vm = if is_swap {
            // DEPLOY / SWAP: the OLD microVM stays LIVE — `perform_swap` health-
            // gates the new one and only then flips + drains the old. So we must
            // NOT reconcile-kill the old VM here (doing so dropped it mid-deploy,
            // the serve loop saw it die, and the runner exited before the flip).
            // The snapshot is per-uuid ⇒ it belongs to the OLD image; clear it
            // and COLD-boot the NEW image (a warm restore would resurrect the
            // old image). The cold boot writes a fresh snapshot for the new
            // image, so later restarts come up on the deployed version.
            snapshot::clear(&cache_dir);
            Self::cold_boot(
                rootfs,
                rt,
                cfg,
                Some(&cache_dir),
                Some(&console_log),
                &vm_key,
                Some(reff),
                egress_allow,
                is_workspace,
                Some(env_hash),
            )
            .await?
        } else {
            // COLD START (first boot / monitor respawn): reconcile a stale VM
            // left by a crashed predecessor, then warm-restore if a snapshot
            // exists AND it was taken from the SAME image (else cold boot, which
            // re-creates a fresh snapshot for the current image).
            if let Some(stale_pid) = pidfile::take(data_dir, uuid) {
                pidfile::kill_stale_if_alive(stale_pid, pidfile::process_is_alive);
            }
            // Invalidate the snapshot when the image_ref OR the #106 env/cap
            // fingerprint has changed: the cache is keyed by UUID only, so a
            // respawn after a redeploy of a NEW image — OR a workspace `add_repo`
            // that changed the `/init`-baked cap set on the SAME image (#108) —
            // would otherwise warm-restore the STALE guest (and the broker's
            // boot-clone would never run for the new repo). `restore_matches` is
            // safe-by-default (any mismatch / missing companion / read error →
            // cold boot, which re-runs `/init` + re-stamps fresh companions).
            if snapshot::restore_matches(&cache_dir, reff, env_hash) {
                match Self::launch_from_snapshot(
                    &cache_dir,
                    cfg,
                    Some(&console_log),
                    &vm_key,
                    resolve_port(rt, cfg),
                    data_dir,
                    uuid,
                    is_workspace,
                )
                .await
                {
                    Ok(warm_vm) => {
                        tracing::info!(uuid, "warm start from snapshot");
                        warm_vm
                    }
                    Err(e) => {
                        tracing::warn!(uuid, error = %e, "snapshot load failed; cold boot");
                        Self::cold_boot(
                            rootfs,
                            rt,
                            cfg,
                            Some(&cache_dir),
                            Some(&console_log),
                            &vm_key,
                            Some(reff),
                            egress_allow,
                            is_workspace,
                            Some(env_hash),
                        )
                        .await?
                    }
                }
            } else {
                // No usable snapshot for THIS image+env (absent, or taken from a
                // different image_ref / env-cap set): cold boot. If a now-stale
                // snapshot is on disk, clear it first so the fresh cold-boot
                // snapshot + its `.snapshot_ref`/`.snapshot_env` replace it cleanly.
                if snapshot::files_present(&cache_dir) {
                    tracing::info!(
                        uuid,
                        "snapshot present but image_ref or env/cap set changed; clearing + cold boot"
                    );
                    snapshot::clear(&cache_dir);
                }
                Self::cold_boot(
                    rootfs,
                    rt,
                    cfg,
                    Some(&cache_dir),
                    Some(&console_log),
                    &vm_key,
                    Some(reff),
                    egress_allow,
                    is_workspace,
                    Some(env_hash),
                )
                .await?
            }
        };

        // Record the active VM's pid for cold-start reconciliation.
        if let Some(pid) = vm.child.as_ref().and_then(|c| c.id()) {
            pidfile::write(data_dir, uuid, pid);
        }
        Ok(vm)
    }

    /// Restore a previously-snapshotted VM from `cache_dir`.
    ///
    /// Flow: `setup_tap` (per-uuid tap/IP) → spawn `firecracker --api-sock` →
    /// `PUT /snapshot/load` (resume_vm=true) → `wait_until_ready` (should be ~ms
    /// not seconds).
    ///
    /// The machine-config / boot-source / drives / network-interfaces sequence
    /// from `configure_and_boot` is intentionally SKIPPED here — the snapshot
    /// embeds all that state. The only networking re-wiring needed is the host
    /// tap; the guest MAC and IP are baked into the snapshot — and because the
    /// tap/IP are now derived from the SAME `uuid` as the cold boot that took
    /// the snapshot, the re-wired host link matches the baked guest exactly
    /// (the old `VM_SEQ` counter could hand the restore a DIFFERENT /30 than the
    /// snapshot was taken with).
    ///
    /// # Errors
    /// Tap setup failure, firecracker spawn failure, snapshot load API failure,
    /// or the guest app failing to answer within [`READY_TIMEOUT`].
    #[allow(clippy::too_many_arguments)]
    async fn launch_from_snapshot(
        cache_dir: &Path,
        cfg: &FcConfig,
        console_log: Option<&Path>,
        vm_key: &str,
        app_port: u16,
        data_dir: &Path,
        uuid: &str,
        is_workspace: bool,
    ) -> Result<Self> {
        if !kvm_available() {
            bail!("firecracker runtime requires Linux + /dev/kvm (/dev/kvm not R/W-openable)");
        }

        // Reap a stale FC left by a PRIOR restore of the same uuid BEFORE
        // spawning the new one. Without this, every warm restore spawned a fresh
        // firecracker without killing the predecessor → FC processes piled up
        // (observed 3 FCs for 1 runner). The cold-boot path already does this
        // via the per-uuid pidfile; mirror the SAME pidfile mutual-exclusion
        // semantics here so only one respawn runs at a time. (The active pid is
        // re-written by `launch_with_uuid` after this returns.)
        if let Some(stale_pid) = pidfile::take(data_dir, uuid) {
            pidfile::kill_stale_if_alive(stale_pid, pidfile::process_is_alive);
        }

        let vmstate = snapshot::vmstate_path(cache_dir);
        let mem = snapshot::mem_path(cache_dir);

        let vmstate_str = vmstate
            .to_str()
            .ok_or_else(|| anyhow!("snapshot vmstate path is not valid UTF-8"))?;
        let mem_str = mem
            .to_str()
            .ok_or_else(|| anyhow!("snapshot mem path is not valid UTF-8"))?;

        // `uuid:reff` tap + /30 — MATCHES the cold boot that took the snapshot
        // (same key ⇒ same tap/IP as the baked-in guest networking).
        let (tap_name, link_idx) = fc_identity_for_key(vm_key);
        let (host_ip, guest_ip) = derive_link_ips(&cfg.tap_subnet, link_idx)
            .with_context(|| format!("derive /30 from subnet {}", cfg.tap_subnet))?;
        let guest_base = format!("http://{guest_ip}:{app_port}");
        tracing::debug!(
            link_idx,
            %host_ip,
            %guest_ip,
            %tap_name,
            %guest_base,
            "fc warm start: derived /30 link + tap for snapshot restore"
        );

        let _ = run_ip(&["link", "del", &tap_name]).await;
        setup_tap(&tap_name, host_ip).await.map_err(|e| {
            tracing::error!(
                %tap_name, %host_ip, error = %e,
                "fc warm start: tap setup failed (need CAP_NET_ADMIN/root for `ip tuntap`; EPERM ⇒ run supervisor with the net-admin capability)"
            );
            e
        })?;
        tracing::debug!(%tap_name, %host_ip, "fc warm start: tap up");

        let api_sock = PathBuf::from(format!("/tmp/firecracker-{tap_name}.sock"));
        let _ = std::fs::remove_file(&api_sock);
        let (stdout, stderr) = console_stdio(console_log);
        // F1 (audit #93): the warm-restore FC gets the SAME per-FC CPU-capped
        // scope as the cold boot (scope id = `vm_key`), so a restored guest is
        // bounded + kill-able identically. Off-systemd → bare spawn (scope=None).
        let fc_args = vec![
            "--api-sock".to_owned(),
            api_sock.to_string_lossy().into_owned(),
        ];
        let (child, fc_scope) = spawn_firecracker(
            cfg,
            super::cpu_scope::FcKind::Serving,
            vm_key,
            &fc_args,
            Stdio::null(),
            stdout,
            stderr,
        )?;

        let me = Self {
            child: Some(child),
            tap_name,
            api_sock: api_sock.clone(),
            fc_scope,
            guest_base,
            guest_ip,
            client: reqwest::Client::new(),
            snapshot_cache_dir: Some(cache_dir.to_path_buf()),
            // Carry the ref the existing snapshot was stamped with so a later
            // Cmd::Snapshot re-stamps the same ref (best-effort read; None on
            // any error → the snapshot() path simply skips the ref write).
            image_ref: std::fs::read_to_string(snapshot::ref_path(cache_dir)).ok(),
            // Likewise carry the env/cap fingerprint the snapshot was stamped with
            // so a Cmd::Snapshot re-stamp keeps `.snapshot_env` in sync (the guest
            // we restored has the SAME env). Best-effort read; `None` → the
            // snapshot() path skips the env write (restore then falls back to cold).
            env_hash: std::fs::read_to_string(snapshot::env_path(cache_dir)).ok(),
            is_workspace,
            #[cfg(test)]
            probe: None,
            #[cfg(test)]
            scrub_base_override: None,
        };

        // Wait for the API socket, then load the snapshot (resume_vm=true).
        // This replaces the full configure_and_boot sequence.
        wait_for_socket(&me.api_sock).await?;
        tracing::debug!(path = "/snapshot/load", "fc warm start: PUT snapshot load");
        me.api_put(
            "/snapshot/load",
            &snapshot_load_body(vmstate_str, mem_str, true),
        )
        .await
        .context("PUT /snapshot/load")?;

        // After a snapshot load the guest resumes immediately; wait_until_ready
        // should return within milliseconds (app was already initialised when
        // the snapshot was taken).
        me.wait_until_ready().await?;
        Ok(me)
    }

    /// Push the full firecracker REST configuration, then start the VM.
    async fn configure_and_boot(
        &self,
        rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        guest_ip: &Ipv4Addr,
        host_ip: &Ipv4Addr,
        guest_mac: &str,
    ) -> Result<()> {
        // The API socket appears asynchronously after spawn; wait for it.
        wait_for_socket(&self.api_sock).await?;

        let kernel = rt.kernel.clone().unwrap_or_else(|| cfg.kernel.clone());
        let rootfs_str = rootfs
            .to_str()
            .ok_or_else(|| anyhow!("rootfs path is not valid UTF-8"))?;

        self.api_put(
            "/machine-config",
            &machine_config_body(resolve_vcpus(rt, cfg), rt.memory_mb),
        )
        .await
        .context("PUT /machine-config")?;
        self.api_put(
            "/boot-source",
            &boot_source_body(&kernel, &guest_ip.to_string(), &host_ip.to_string()),
        )
        .await
        .context("PUT /boot-source")?;
        self.api_put("/drives/rootfs", &rootfs_drive_body(rootfs_str, false))
            .await
            .context("PUT /drives/rootfs")?;
        self.api_put(
            "/network-interfaces/eth0",
            &network_iface_body(&self.tap_name, guest_mac),
        )
        .await
        .context("PUT /network-interfaces/eth0")?;
        self.api_put("/actions", &instance_start_body())
            .await
            .context("PUT /actions InstanceStart")?;
        Ok(())
    }

    /// One firecracker REST `PUT` over the API unix socket. Hand-rolled
    /// HTTP/1.1: firecracker speaks plain HTTP/1.1 with `Content-Length`
    /// bodies and replies `204 No Content` (or 200) on success, an error
    /// JSON otherwise.
    async fn api_put(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        api_put_sock(&self.api_sock, path, body).await
    }

    /// One firecracker REST `PATCH` over the API unix socket.
    ///
    /// Used for VM state transitions (`/vm`): pause before snapshot, resume
    /// after. The Firecracker API uses PATCH (not PUT) for state changes.
    async fn api_patch(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        let payload = serde_json::to_vec(body)?;
        let status =
            tokio::time::timeout(API_TIMEOUT, unix_http_patch(&self.api_sock, path, &payload))
                .await
                .map_err(|_| anyhow!("firecracker API timed out on PATCH {path}"))??;
        tracing::debug!(verb = "PATCH", path, status, "fc API call");
        if !(200..300).contains(&status) {
            bail!("firecracker API PATCH {path} returned HTTP {status}");
        }
        Ok(())
    }

    /// Take a snapshot of a running VM into `cache_dir/snap.vmstate` +
    /// `cache_dir/snap.mem`. Called once after the first cold boot.
    ///
    /// Flow: PATCH /vm Paused → PUT /snapshot/create → PATCH /vm Resumed.
    ///
    /// This is best-effort: any error is logged and the VM continues serving
    /// cold (the caller must NOT fail the launch on snapshot errors).
    async fn try_create_snapshot(&self, cache_dir: &std::path::Path) {
        if let Err(e) = self.create_snapshot_inner(cache_dir).await {
            tracing::warn!(
                cache_dir = %cache_dir.display(),
                error = %e,
                "snapshot create failed (best-effort; VM continues serving cold)"
            );
        }
        // ALWAYS confirm the VM is running afterward. `create_snapshot_inner`
        // PAUSES the VM; on ANY failure (or a memory write that outran the inner
        // resume) it can be left paused — which fails the deploy swap
        // health-gate (TAB-10 strand). Guarantee it is resumed so cold_boot NEVER
        // returns a paused VM due to a snapshot hiccup.
        self.ensure_resumed().await;
    }

    /// Best-effort GUARANTEE that the VM is running (not paused). Resuming an
    /// already-running VM is treated as success (FC answers 4xx "not paused" —
    /// it IS running). Retries a momentarily-busy FC (still flushing a snapshot)
    /// up to [`RESUME_ENSURE_TIMEOUT`]; a persistently-unresponsive FC is logged
    /// and left to the swap health-gate to reject.
    async fn ensure_resumed(&self) {
        let deadline = tokio::time::Instant::now() + RESUME_ENSURE_TIMEOUT;
        loop {
            match self.api_patch("/vm", &resume_body()).await {
                Ok(()) => return,
                // A 4xx means the VM is not in a resumable (paused) state — i.e.
                // it is already running. Done.
                Err(e) if e.to_string().contains("returned HTTP 4") => return,
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(
                            error = %e,
                            "ensure_resumed: VM may be stranded paused (FC unresponsive)"
                        );
                        return;
                    }
                    tokio::time::sleep(RESUME_ENSURE_POLL).await;
                }
            }
        }
    }

    async fn create_snapshot_inner(&self, cache_dir: &std::path::Path) -> Result<()> {
        // Ensure the cache directory exists so snapshot files can be written.
        std::fs::create_dir_all(cache_dir)
            .with_context(|| format!("create snapshot cache dir {}", cache_dir.display()))?;

        let vmstate = snapshot::vmstate_path(cache_dir);
        let mem = snapshot::mem_path(cache_dir);

        let vmstate_str = vmstate
            .to_str()
            .ok_or_else(|| anyhow!("snapshot vmstate path is not valid UTF-8"))?;
        let mem_str = mem
            .to_str()
            .ok_or_else(|| anyhow!("snapshot mem path is not valid UTF-8"))?;

        self.api_patch("/vm", &pause_body())
            .await
            .context("PATCH /vm Paused before snapshot")?;
        // The memory write can far exceed the 5s API_TIMEOUT — use the snapshot
        // budget so it is not killed mid-write (which would strand the paused VM).
        api_put_sock_with_timeout(
            &self.api_sock,
            "/snapshot/create",
            &snapshot_create_body(vmstate_str, mem_str),
            SNAPSHOT_CREATE_TIMEOUT,
        )
        .await
        .context("PUT /snapshot/create")?;
        self.api_patch("/vm", &resume_body())
            .await
            .context("PATCH /vm Resumed after snapshot")?;

        tracing::info!(
            vmstate = %vmstate.display(),
            mem = %mem.display(),
            "snapshot created; subsequent launches will warm-start"
        );
        Ok(())
    }

    /// GAP#4 — make the (still-running) workspace VM snapshot-safe BEFORE we pause
    /// it: POST the broker's host-reachable `:8732` scrub route so it drops its
    /// in-RAM creds + removes the tmpfs cred files. Only workspaces hold creds and
    /// take a Full snapshot, so this is gated on `is_workspace`
    /// ([`super::snapshot_decision::must_scrub_before_snapshot`]); a non-workspace
    /// FC has no broker on `:8732` and skips.
    ///
    /// FAIL-CLOSED: for a workspace, ANY scrub failure (broker unreachable, a
    /// non-2xx, or a transport error) returns `Err` so the caller ABORTS the
    /// snapshot — we NEVER freeze a held secret into a warm restore. The broker
    /// must be live (the scrub is a real socket round-trip inside the guest), so a
    /// connect refusal on a WORKSPACE means the broker died/never came up → abort
    /// rather than silently snapshot creds.
    ///
    /// # Errors
    /// The workspace broker scrub did not return 2xx (so the snapshot must not run).
    async fn pre_snapshot_scrub(&self) -> Result<()> {
        if !super::snapshot_decision::must_scrub_before_snapshot(self.is_workspace) {
            return Ok(());
        }
        let base = {
            #[cfg(test)]
            {
                self.scrub_base_override.clone().unwrap_or_else(|| {
                    format!(
                        "http://{}:{}",
                        self.guest_ip,
                        crate::tcp_forward::GUEST_BROKER_CTRL_PORT
                    )
                })
            }
            #[cfg(not(test))]
            {
                format!(
                    "http://{}:{}",
                    self.guest_ip,
                    crate::tcp_forward::GUEST_BROKER_CTRL_PORT
                )
            }
        };
        let url = format!("{base}{}", super::snapshot_decision::PRE_SNAPSHOT_SCRUB_PATH);
        let resp = self
            .client
            .post(&url)
            .timeout(API_TIMEOUT)
            .send()
            .await
            .with_context(|| {
                format!("pre-snapshot scrub POST to {url} failed (workspace broker unreachable)")
            })?;
        let status = resp.status();
        if !status.is_success() {
            bail!("pre-snapshot scrub returned HTTP {status}; aborting snapshot (would freeze a held secret)");
        }
        tracing::info!(
            guest_ip = %self.guest_ip,
            "pre-snapshot scrub OK: broker dropped in-RAM creds + cred files before snapshot"
        );
        Ok(())
    }

    /// Poll the guest app's HTTP server until it answers (any status) or
    /// [`READY_TIMEOUT`] elapses.
    async fn wait_until_ready(&self) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + READY_TIMEOUT;
        let mut attempt: u32 = 0;
        // Exponential backoff between polls (50ms → 100 → 250 → 500 → … capped
        // at 2s): a healthy guest is detected sooner than the old fixed 250ms,
        // while a slow one is still waited for within the same READY_TIMEOUT
        // budget. The per-poll HTTP timeout stays READY_POLL.
        let mut backoff = READY_BACKOFF_START;
        loop {
            attempt += 1;
            match self
                .client
                .get(&self.guest_base)
                .timeout(READY_POLL)
                .send()
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        guest_base = %self.guest_base,
                        attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        "fc boot: guest app answered; ready"
                    );
                    return Ok(());
                }
                Err(e) if tokio::time::Instant::now() < deadline => {
                    // Previously a silent loop — log each failed poll so a
                    // slow/never-booting guest is visible in the boot trace.
                    tracing::debug!(
                        guest_base = %self.guest_base,
                        attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = %e,
                        "fc boot: guest app not ready yet; retrying"
                    );
                    // Don't oversleep past the deadline — clamp the final sleep
                    // so the overall budget stays ~READY_TIMEOUT.
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(backoff.min(remaining)).await;
                    backoff = (backoff * 2).min(READY_BACKOFF_CAP);
                }
                Err(e) => {
                    tracing::error!(
                        guest_base = %self.guest_base,
                        attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = %e,
                        "fc boot: guest app never became ready (timeout); if tap setup hit EPERM the guest has no network — run the supervisor with CAP_NET_ADMIN/root"
                    );
                    bail!("guest app at {} never became ready: {e}", self.guest_base)
                }
            }
        }
    }

    /// Build a `FirecrackerRuntime` with an injectable reachability probe for
    /// unit tests. `guest_base` is `http://<guest_ip>:<port>` (the proxy target
    /// base); `probe` is the reachability check that `health()` will call
    /// instead of the real HTTP GET.
    ///
    /// There is no live child or tap here — `child` is `None` and `tap_name`
    /// is a sentinel — so `health()` can be exercised without a real microVM.
    /// This constructor is `#[cfg(test)]`-only so it never surfaces in
    /// production code.
    #[cfg(test)]
    pub fn with_probe_for_test(guest_base: &str, probe: TcpProbe) -> Self {
        Self {
            child: None,
            tap_name: "fc-tap-test".to_owned(),
            api_sock: PathBuf::from("/tmp/firecracker-test.sock"),
            fc_scope: None,
            guest_base: guest_base.to_owned(),
            guest_ip: Ipv4Addr::new(169, 254, 0, 2),
            client: reqwest::Client::new(),
            snapshot_cache_dir: None,
            image_ref: None,
            env_hash: None,
            is_workspace: false,
            probe: Some(probe),
            scrub_base_override: None,
        }
    }

    /// Test-only: build a runtime whose `pre_snapshot_scrub` targets `scrub_base`
    /// (e.g. a local stub server) and whose `is_workspace` flag is set, so the
    /// GAP#4 fail-closed scrub path is exercised without a real microVM. There is
    /// no live child/tap (mirrors [`Self::with_probe_for_test`]).
    #[cfg(test)]
    pub fn with_scrub_target_for_test(scrub_base: &str, is_workspace: bool) -> Self {
        let mut me = Self::with_probe_for_test("http://169.254.0.2:8080", std::sync::Arc::new(|_| true));
        me.is_workspace = is_workspace;
        me.scrub_base_override = Some(scrub_base.to_owned());
        me
    }
}

impl AppRuntime for FirecrackerRuntime {
    fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a> {
        // Delegate to the VM-independent proxy core (tested via wiremock).
        Box::pin(proxy_request(&self.client, &self.guest_base, request))
    }

    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        // Test seam: when an injectable probe is present (set only by
        // `with_probe_for_test`), use it instead of a real HTTP GET so health
        // can be exercised without a live microVM. The probe receives the
        // `host:port` (with the `http://` scheme stripped).
        #[cfg(test)]
        if let Some(probe) = self.probe.clone() {
            let hp = self.guest_base.trim_start_matches("http://").to_owned();
            return Box::pin(async move {
                if (probe)(&hp) {
                    RuntimeHealth::Serving
                } else {
                    RuntimeHealth::Unavailable(format!("guest {hp} unreachable (probe)"))
                }
            });
        }

        // The app is healthy iff its guest HTTP server answers (any status).
        Box::pin(async move {
            match self
                .client
                .get(&self.guest_base)
                .timeout(API_TIMEOUT)
                .send()
                .await
            {
                Ok(_) => RuntimeHealth::Serving,
                Err(e) => RuntimeHealth::Unavailable(format!(
                    "guest {} unreachable: {e}",
                    self.guest_base
                )),
            }
        })
    }

    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
        // The firecracker child is OUR child, so a dead VM becomes a ZOMBIE
        // until we reap it. `kill(pid, 0)` (`process_is_alive`) reports a
        // zombie as "alive" forever — the live Lima test proved that hangs
        // fail-fast — so we poll with `waitpid(WNOHANG)`, which REAPS the
        // zombie AND detects the exit. Polling keeps this `&self` (tokio
        // `Child::wait` needs `&mut`, which `Drop` owns). When the VM exits
        // the app is gone → the runner exits → L2 respawns it.
        let pid = self.child.as_ref().and_then(|c| c.id());
        Box::pin(async move {
            match pid {
                None => std::future::pending().await,
                Some(pid) => loop {
                    // SAFETY: waitpid is a POSIX syscall; WNOHANG is
                    // non-blocking. Returns the pid (and reaps) once it has
                    // exited, 0 while still running, <0 (ECHILD) if already
                    // reaped — any non-zero result means the child is gone.
                    let r = unsafe {
                        libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG)
                    };
                    if r != 0 {
                        return ExitReason::Died(format!("firecracker child pid {pid} exited"));
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                },
            }
        })
    }

    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
        // Graceful teardown: kill the VM child (if still alive) + delete the tap.
        //
        // IMPORTANT — the SIGKILL is issued SYNCHRONOUSLY in the method body
        // (before `Box::pin`), NOT inside the async block. This guarantees the
        // firecracker process is killed at method-call time even if the caller
        // drops the returned future before polling it (e.g. a detached drain task
        // that is abandoned when the runner calls `process::exit`). The tap
        // teardown is still async (it shells `ip link del`) and lives inside the
        // returned future; if the future is dropped the tap may leak, but the
        // critical goal — no orphaned spinning FC process — is already met by the
        // synchronous kill above. `Drop` repeats both as a safety net (idempotent).
        let pid = self.child.as_ref().and_then(|c| c.id());
        if let Some(pid) = pid {
            tracing::info!(pid, tap = %self.tap_name, "fc shutdown: killing FC child");
            pidfile::kill_stale_if_alive(pid, pidfile::process_is_alive);
        }
        // F1 (audit #93): when the FC runs in a systemd scope, killing the
        // `systemd-run` wrapper PID above is NOT sufficient — systemd owns the
        // scope, so the firecracker process survives INTO the
        // supervisor-independent scope. `systemctl stop` the scope to SIGKILL the
        // FC in its cgroup + GC the unit. Synchronous-ish (detached), idempotent.
        if let Some(scope) = self.fc_scope.as_deref() {
            tracing::info!(scope, tap = %self.tap_name, "fc shutdown: stopping CPU-capped scope");
            stop_fc_scope(scope);
        }
        let tap = self.tap_name.clone();
        Box::pin(async move {
            if let Err(e) = run_ip(&["link", "del", &tap]).await {
                tracing::debug!(%tap, error = %e, "shutdown: ip link del tap (may already be gone)");
            }
        })
    }

    fn guest_ssh_addr(&self) -> Option<std::net::SocketAddr> {
        // The guest's sshd listens on GUEST_SSH_PORT inside the VM (on its
        // eth0, IPv4-only). The host reaches it at guest_ip:<port> via the /30
        // tap.
        Some(std::net::SocketAddr::new(
            std::net::IpAddr::V4(self.guest_ip),
            crate::tcp_forward::GUEST_SSH_PORT,
        ))
    }

    fn guest_code_addr(&self) -> Option<std::net::SocketAddr> {
        // The workspace code-service listens on CODE_SERVICE_PORT (8731) inside
        // the VM (IPv4 eth0). The host reaches it at guest_ip:8731 via the /30
        // tap; the runner forwards [app_ula]:8731 → here. Every FC guest exposes
        // the port; non-workspace images simply have nothing listening, so a dial
        // refuses — harmless (the forwarder only matters for workspaces).
        Some(std::net::SocketAddr::new(
            std::net::IpAddr::V4(self.guest_ip),
            tabbify_workspace_contract::CODE_SERVICE_PORT,
        ))
    }

    fn guest_broker_ctrl_addr(&self) -> Option<std::net::SocketAddr> {
        // The workspace broker serves its token-gated add-key endpoint on
        // GUEST_BROKER_CTRL_PORT (8732) inside the VM (IPv4 eth0). The host
        // reaches it at guest_ip:8732 via the /30 tap; the runner forwards
        // [app_ula]:8732 → here so node can POST the laptop pubkey with its
        // bearer cap (§12 S6). Non-workspace images have nothing listening → a
        // dial refuses (harmless; the forwarder only matters for workspaces).
        Some(std::net::SocketAddr::new(
            std::net::IpAddr::V4(self.guest_ip),
            crate::tcp_forward::GUEST_BROKER_CTRL_PORT,
        ))
    }

    fn snapshot<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let Some(cache_dir) = self.snapshot_cache_dir.clone() else {
                // No cache dir (bare launch / build VM): nothing to refresh.
                return Ok(());
            };
            // GAP#4 (spec §4): a workspace's warm snapshot is a FULL snapshot — it
            // freezes ALL guest RAM + fs. Before we PAUSE, the in-guest broker MUST
            // drop its in-RAM creds (the per-repo git cap-URLs + the forge-admin
            // token) and remove the tmpfs cred files, or the snapshot would freeze
            // a live token into EVERY warm restore (a rotated/expired token, AND a
            // secret-in-snapshot leak). A scrub FAILURE ABORTS the snapshot — we
            // never freeze a held secret. This runs while the VM is still RUNNING
            // (the broker socket must be live to scrub) so it precedes the pause.
            self.pre_snapshot_scrub().await?;
            // §12 snapshot-timing: this is the explicit POST-INDEX refresh. We do
            // NOT check `snapshot::is_suppressed(&cache_dir)` — a workspace cache
            // dir CARRIES the `.no-snapshot` marker (Task 9) so cold_boot never
            // freezes a COLD index; `Cmd::Snapshot` must write the FIRST warm
            // snapshot OVER that marker. Suppress gates cold_boot only.
            //
            // Reuse the v1.4.67 live-VM primitive: pause → PUT /snapshot/create
            // (SNAPSHOT_CREATE_TIMEOUT, room for the multi-GB RAM write) →
            // resume. `try_create_snapshot` ALWAYS `ensure_resumed`s afterward,
            // so the VM is left RUNNING even if the create itself failed — the
            // workspace never strands paused. We re-check `files_present` to
            // know whether the create actually landed before stamping the ref.
            self.try_create_snapshot(&cache_dir).await;
            if snapshot::files_present(&cache_dir) {
                if let Some(reff) = self.image_ref.as_deref() {
                    snapshot::write_ref(&cache_dir, reff);
                }
                // Stamp the env/cap fingerprint alongside the ref (#108). A
                // workspace's warm snapshot is created HERE (post-index), NOT in
                // `cold_boot` (which is `.no-snapshot`-suppressed for workspaces),
                // so without this the workspace's warm snapshot would carry a ref
                // but no `.snapshot_env` and `restore_matches` would ALWAYS cold
                // boot it — defeating the warm-LSP index. With it, an UNCHANGED
                // workspace warm-restores; only an `add_repo` (changed env) cold-boots.
                if let Some(eh) = self.env_hash.as_deref() {
                    snapshot::write_env(&cache_dir, eh);
                }
                tracing::info!(
                    cache_dir = %cache_dir.display(),
                    "Cmd::Snapshot: live-VM warm snapshot refreshed (post-index)"
                );
                Ok(())
            } else {
                // create did not land (logged inside try_create_snapshot); the
                // VM is still serving (ensure_resumed ran) — report the failure
                // so the node can retry, but the workspace stays up.
                anyhow::bail!(
                    "Cmd::Snapshot: snapshot create did not produce files at {}",
                    cache_dir.display()
                )
            }
        })
    }
}

impl Drop for FirecrackerRuntime {
    fn drop(&mut self) {
        // Kill AND REAP the firecracker child. `start_kill` alone SIGKILLs but
        // leaves a zombie until tokio's orphan reaper runs (seconds later);
        // spawning `kill()` (SIGKILL + `wait`) reaps it immediately so no
        // `<defunct>` lingers in the process table. We're normally inside the
        // tokio runtime here; if not (e.g. a sync drop in a test), fall back
        // to a best-effort non-reaping kill. The tap delete stays synchronous
        // (quick) so it happens even if there's no runtime to spawn on.
        if let Some(mut child) = self.child.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        let _ = child.kill().await;
                    });
                }
                Err(_) => {
                    let _ = child.start_kill();
                }
            }
        }
        // F1 (audit #93): tear down the systemd scope so the FC inside it is
        // SIGKILLed in its cgroup (killing the `systemd-run` wrapper child above
        // would otherwise orphan the FC into the supervisor-independent scope).
        // Safety net mirroring `shutdown`; idempotent if already stopped.
        if let Some(scope) = self.fc_scope.as_deref() {
            stop_fc_scope(scope);
        }
        let _ = std::fs::remove_file(&self.api_sock);
        let tap = self.tap_name.clone();
        match std::process::Command::new("ip")
            .args(["link", "del", &tap])
            .status()
        {
            Ok(s) if s.success() => {}
            Ok(s) => tracing::warn!(%tap, code = ?s.code(), "ip link del tap nonzero exit"),
            Err(e) => tracing::warn!(%tap, error = %e, "ip link del tap failed"),
        }
    }
}

/// Create the host tap device and assign it the /30 host IP.
pub(crate) async fn setup_tap(tap_name: &str, host_ip: Ipv4Addr) -> Result<()> {
    run_ip(&["tuntap", "add", tap_name, "mode", "tap"])
        .await
        .with_context(|| format!("ip tuntap add {tap_name}"))?;
    run_ip(&["addr", "add", &format!("{host_ip}/30"), "dev", tap_name])
        .await
        .with_context(|| format!("ip addr add {host_ip}/30 dev {tap_name}"))?;
    run_ip(&["link", "set", tap_name, "up"])
        .await
        .with_context(|| format!("ip link set {tap_name} up"))?;
    Ok(())
}

/// F1 (audit #93) — spawn the firecracker child, wrapped in a per-FC,
/// CPU-capped, supervisor-independent systemd SCOPE when this host runs systemd.
///
/// On a systemd host the child is launched as
/// `systemd-run --scope --collect --unit=tabbify-fc-<id>.scope
/// --slice=tabbify-fc.slice -p CPUQuota=<n>% -p CPUWeight=<w> -- firecracker
/// --api-sock <sock>`. `--scope` keeps firecracker as our DIRECT, FOREGROUND
/// child (so the existing `Child`/`waitpid` exit-detection is unchanged — the
/// `systemd-run` process lives exactly as long as the FC) while systemd wraps a
/// cgroup with the CPU cap + the named kill handle. Off-systemd (macOS dev / CI
/// / plain container) it falls back to a BARE `firecracker` spawn, preserving
/// the legacy "child IS firecracker" lifecycle so tests stay host-agnostic.
///
/// Returns `(child, scope)` where `scope` is `Some(name)` iff the spawn was
/// wrapped — the caller stores it so [`stop_fc_scope`] can tear the guest down
/// even with the supervisor dead.
///
/// `scope_id` is the per-FC identity (an app uuid, or `build-<seq>`); `fc_args`
/// is the verbatim firecracker argv tail (`["--api-sock", "<sock>"]`).
fn spawn_firecracker(
    cfg: &FcConfig,
    kind: super::cpu_scope::FcKind,
    scope_id: &str,
    fc_args: &[String],
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
) -> Result<(Child, Option<String>)> {
    use super::cpu_scope;

    let wrap = cpu_scope::should_wrap(systemd_run_available(), cpu_scope::host_has_systemd());
    if wrap {
        let scope = cpu_scope::scope_name(scope_id);
        let argv =
            cpu_scope::systemd_run_argv(&scope, &cfg.cpu_scope_cfg(), kind, &cfg.bin, fc_args);
        // argv[0] == "systemd-run"; the rest are its args.
        let child = Command::new(&argv[0])
            .args(&argv[1..])
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| {
                format!(
                    "spawn firecracker under systemd scope {scope} (systemd-run wrapping {})",
                    cfg.bin
                )
            })?;
        tracing::debug!(
            scope = %scope,
            quota_serving = cfg.cpu_quota_serving_pct,
            quota_build = cfg.cpu_quota_build_pct,
            weight = cfg.cpu_weight,
            "fc spawn: wrapped in CPU-capped systemd scope (F1)"
        );
        Ok((child, Some(scope)))
    } else {
        let child = Command::new(&cfg.bin)
            .args(fc_args)
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| format!("spawn firecracker binary {:?} (no systemd scope)", cfg.bin))?;
        Ok((child, None))
    }
}

/// F1 — build-VM spawn wrapper: [`spawn_firecracker`] with the BUILD kind/quota.
/// Exposed `pub(crate)` so [`super::build_vm`] can route its FC spawn through the
/// SAME CPU-scope path (the build VM is the hottest, 2-vCPU guest). `stdin` is
/// always `null` for builds (no interactive input).
pub(crate) fn spawn_build_firecracker(
    cfg: &FcConfig,
    scope_id: &str,
    fc_args: &[String],
    stdout: Stdio,
    stderr: Stdio,
) -> Result<(Child, Option<String>)> {
    spawn_firecracker(
        cfg,
        super::cpu_scope::FcKind::Build,
        scope_id,
        fc_args,
        Stdio::null(),
        stdout,
        stderr,
    )
}

/// Is a usable `systemd-run` on `$PATH`? Cheap existence check (no fork): scans
/// the `PATH` dirs for an executable `systemd-run`. Pairs with
/// [`super::cpu_scope::host_has_systemd`] in [`spawn_firecracker`]'s wrap gate.
fn systemd_run_available() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| {
        let candidate = dir.join("systemd-run");
        candidate.is_file()
            || std::fs::metadata(&candidate)
                .map(|m| m.is_file())
                .unwrap_or(false)
    })
}

/// F1 — stop the transient FC scope `scope` (`systemctl stop <scope>`).
///
/// This is the supervisor-INDEPENDENT teardown: systemd owns the scope, so a
/// `systemctl stop` SIGTERM→SIGKILLs every process in the scope's cgroup
/// (the firecracker child) and garbage-collects the unit, even if the
/// `systemd-run` wrapper PID was already reaped/killed. Best-effort + fire-and-
/// forget (spawned detached) so teardown never blocks on systemd; a missing
/// scope (already gone) is a harmless non-zero exit. Idempotent.
pub(crate) fn stop_fc_scope(scope: &str) {
    let scope = scope.to_owned();
    let spawn = std::process::Command::new("systemctl")
        .args(["stop", &scope])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    match spawn {
        Ok(mut child) => {
            // Reap asynchronously if we're on a tokio runtime; else detach. We
            // never block the caller (shutdown/Drop) on systemd.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => {
            tracing::debug!(scope = %scope, error = %e, "systemctl stop fc scope failed to spawn (systemd absent?)");
        }
    }
}

/// #17 — delete the host tap device `tap` (`ip link del <tap>`), SYNC + best-
/// effort. This is the network half of the orphan teardown: when a runner dies
/// abnormally its [`Drop`] (which deletes the tap, see the `Drop for
/// FirecrackerRuntime` impl above) never runs, so the reaped FC's tap leaks. The
/// atomic per-uuid teardown (`kill_fc_child_for_uuid`) reconstructs the tap
/// name(s) the SAME way the spawn keyed them and calls this for each. Mirrors the
/// `Drop` impl's `ip link del` exactly (same sync `std::process::Command`), so it
/// stays callable from the sync reap path with no async-coloring. Deleting a tap
/// that was never created (or already gone) is a harmless non-zero exit, so this
/// is idempotent + safe to call on every reap, including just before a respawn
/// (the fresh boot re-creates the tap via `setup_tap`). A missing `ip` binary is
/// logged at debug and ignored (off-Linux / minimal hosts).
pub(crate) fn delete_fc_tap(tap: &str) {
    match std::process::Command::new("ip")
        .args(["link", "del", tap])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => {}
        // Non-zero exit = the tap was already gone (the common, healthy case on a
        // respawn where the runner's Drop already removed it). Debug, not warn.
        Ok(s) => tracing::debug!(%tap, code = ?s.code(), "ip link del tap nonzero exit (already gone?)"),
        Err(e) => tracing::debug!(%tap, error = %e, "ip link del tap failed to spawn (ip absent?)"),
    }
}

/// Run an `ip ...` command, erroring on a non-zero exit.
pub(crate) async fn run_ip(args: &[&str]) -> Result<()> {
    let out = Command::new("ip")
        .args(args)
        .output()
        .await
        .with_context(|| format!("spawn ip {}", args.join(" ")))?;
    if !out.status.success() {
        bail!(
            "ip {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// Give cold-booted guests internet egress so the in-VM tabbify-supervisor can
/// reach the public mesh coordinator (and the relay / WG peers): enable IPv4
/// forwarding and SNAT the guest tap subnet out the host's default-route uplink.
///
/// `egress_allow` (Track 7 network ACL) selects the FORWARD policy:
/// - `None`/empty ⇒ LEGACY unrestricted egress: a blanket `FORWARD -i tap -o
///   uplink -j ACCEPT` (+ the established-conn return rule). Today's behavior;
///   no regression for apps without an allow-list.
/// - `Some(non-empty)` ⇒ ENFORCED: deny-by-default forward out the uplink, with
///   per-resolved-IP ACCEPTs for the allowed hosts plus an always-allow for the
///   default-route gateway (so the in-VM supervisor keeps its mesh uplink). The
///   legacy blanket-ACCEPT for this tap is removed FIRST (open→restricted
///   transition) so it cannot win the chain over the per-IP+DROP rules.
///
/// MASQUERADE is installed in BOTH modes (allowed flows must still be SNAT'd).
///
/// BEST-EFFORT and idempotent: a failure here must not abort a boot — it only
/// costs the guest its egress (the VM still runs and answers the :8080 probe),
/// so we log loudly and continue. The host tap link itself (`setup_tap`) is
/// what the readiness probe needs; this only matters for guest→internet.
pub(crate) async fn setup_guest_nat(
    tap_name: &str,
    tap_subnet: &str,
    egress_allow: Option<&[String]>,
) {
    // Enable forwarding (no-op if already 1; warn but continue on EACCES).
    if let Err(e) = tokio::fs::write("/proc/sys/net/ipv4/ip_forward", b"1\n").await {
        tracing::warn!(error = %e, "fc nat: could not enable net.ipv4.ip_forward");
    }
    let Some(uplink) = default_route_dev().await else {
        tracing::warn!("fc nat: no default-route uplink found; guest egress disabled");
        return;
    };

    // MASQUERADE is needed in BOTH modes (allowed flows must still be SNAT'd out
    // the uplink). Idempotent (`-C ... || -A ...`).
    let masq_check: Vec<&str> = vec![
        "-t", "nat", "-C", "POSTROUTING", "-s", tap_subnet, "-o", &uplink, "-j", "MASQUERADE",
    ];
    let masq_add: Vec<&str> = vec![
        "-t", "nat", "-A", "POSTROUTING", "-s", tap_subnet, "-o", &uplink, "-j", "MASQUERADE",
    ];
    if let Err(e) = ensure_iptables(&masq_check, &masq_add).await {
        tracing::warn!(error = %e, "fc nat: masquerade rule add failed");
    }

    match egress_allow.filter(|a| !a.is_empty()) {
        // ── ENFORCED allow-list ────────────────────────────────────────────
        Some(allow) => {
            // TRANSITION open→restricted: a previous (re)deploy of this SAME tap
            // may have installed the legacy BLANKET `-I FORWARD 1 -i tap -o uplink
            // -j ACCEPT` at the head of the chain. Left in place it WINS over our
            // per-IP ACCEPT + catch-all DROP (broader + earlier). So FIRST remove
            // the legacy blanket rules for this tap (idempotent `-D`; a no-op when
            // absent). `teardown_guest_nat` deletes exactly those.
            let _ = teardown_guest_nat(tap_name).await;

            // Resolve hosts → IP literals (DNS-pinning at install). Always-allow
            // the default-route gateway so the in-VM supervisor keeps its mesh
            // uplink even under the catch-all DROP. The git-proxy needs NO rule
            // here: it listens on the HOST tap-gateway IP (inside `tap_subnet`),
            // so guest→git-proxy traffic is delivered locally on the tap and is
            // NEVER `-o <uplink>` — the FORWARD allow/DROP rules (all `-o uplink`)
            // never match it.
            let resolved = crate::firecracker::egress_filter::resolve_hosts(allow).await;
            let always: Vec<String> = default_route_gateway().await.into_iter().collect();
            let rules = crate::firecracker::egress_filter::egress_rules(
                tap_name, &uplink, &resolved, &always,
            );
            for (check, add) in &rules {
                let c: Vec<&str> = check.iter().map(String::as_str).collect();
                let a: Vec<&str> = add.iter().map(String::as_str).collect();
                if let Err(e) = ensure_iptables(&c, &a).await {
                    tracing::warn!(error = %e, "fc nat: egress allow-rule add failed");
                }
            }
            tracing::info!(
                %tap_name, uplink = %uplink, allowed = resolved.len(), always = always.len(),
                "fc nat: egress allow-list ENFORCED (deny-by-default + allowed hosts)"
            );
        }
        // ── LEGACY unrestricted (unchanged behavior) ───────────────────────
        None => {
            // Idempotent (`-C ... || -I ...`). FORWARD rules are *inserted* at the
            // head so they precede any docker-installed DROP/jump.
            let rules: [(Vec<&str>, Vec<&str>); 2] = [
                (
                    vec!["-C", "FORWARD", "-i", tap_name, "-o", &uplink, "-j", "ACCEPT"],
                    vec!["-I", "FORWARD", "1", "-i", tap_name, "-o", &uplink, "-j", "ACCEPT"],
                ),
                (
                    vec![
                        "-C", "FORWARD", "-i", &uplink, "-o", tap_name, "-m", "state", "--state",
                        "RELATED,ESTABLISHED", "-j", "ACCEPT",
                    ],
                    vec![
                        "-I", "FORWARD", "1", "-i", &uplink, "-o", tap_name, "-m", "state",
                        "--state", "RELATED,ESTABLISHED", "-j", "ACCEPT",
                    ],
                ),
            ];
            for (check, add) in &rules {
                if let Err(e) = ensure_iptables(check, add).await {
                    tracing::warn!(error = %e, "fc nat: iptables rule add failed; guest egress may be blocked");
                }
            }
            tracing::info!(%tap_name, uplink = %uplink, subnet = %tap_subnet, "fc nat: guest egress enabled (unrestricted)");
        }
    }
}

/// The default-route GATEWAY IP — always-allowed so the in-VM supervisor keeps
/// its uplink path even under a strict egress allow-list. VERIFIED: `run_ip`
/// returns `Result<()>` (no stdout capture), so we cannot reuse it here; we shell
/// `ip` directly exactly as `default_route_dev` does and parse with the pure
/// helper below. `None` when there is no default route or `ip` is unavailable.
async fn default_route_gateway() -> Option<String> {
    let out = Command::new("ip")
        .args(["route", "show", "default"])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_default_gateway(&String::from_utf8_lossy(&out.stdout))
}

/// Parse the `via <gw>` token out of `ip route show default`. Pure + isolated for
/// unit-testing (mirrors the existing `parse_default_dev`).
fn parse_default_gateway(route_output: &str) -> Option<String> {
    let toks: Vec<&str> = route_output.split_whitespace().collect();
    toks.iter()
        .position(|t| *t == "via")
        .and_then(|i| toks.get(i + 1))
        .map(|g| (*g).to_owned())
}

/// Install iptables rules so an IPv4 tap-gateway proxy `port` is reachable only
/// from the FC tap subnet, not from the WiFi uplink. Shared by the git-proxy
/// (`:8788`, see [`setup_git_proxy_firewall`]) and the forge-proxy (`:8789`, see
/// [`setup_forge_proxy_firewall`]) — the two differ ONLY in the port + the
/// `label` used in log lines; the security posture is identical. `label` is a
/// short human tag (`"git proxy"` / `"forge proxy"`).
///
/// Rules (all best-effort — failure is logged, never fatal):
/// 1. INPUT ACCEPT from `tap_subnet` to `proxy_port` (guests → proxy).
/// 2. INPUT DROP from uplink to `proxy_port` (depth-in-defence; the primary
///    guard is the git-proxy cap / the mesh ACL, but this closes WiFi exposure).
///
/// Rules are idempotent (`-C ... || -I ...`) and come from the pure
/// [`crate::firecracker::proxy_firewall::proxy_ipv4_firewall_rules`] builder.
/// Called ONCE per proxy at startup from `main.rs` — not per-VM, because the port
/// and subnet are host-global.
///
/// SAFETY ORDERING: if the tap-subnet ACCEPT cannot be installed, we DO NOT
/// install the uplink DROP — a DROP at INPUT position 1 without the preceding
/// ACCEPT would also drop tap traffic and silently break the guest.
///
/// TODO: no teardown for these INPUT rules (idempotent `-C` guards dupes;
/// revisit if port/subnet become dynamic). Matches the NAT teardown honesty.
///
/// FOLLOW-UP: consider restricting further with `--src-range` on the /30
/// subnet; for now the /16 is narrow enough for a home/lab host.
async fn setup_proxy_ipv4_firewall(label: &str, tap_subnet: &str, proxy_port: u16) {
    use crate::firecracker::proxy_firewall::proxy_ipv4_firewall_rules;

    let port_str = proxy_port.to_string();
    let Some(uplink) = default_route_dev().await else {
        tracing::warn!(
            proxy = %label,
            "proxy firewall: no default-route uplink found; WiFi DROP rule skipped"
        );
        // Still try to install the ACCEPT rule for the tap subnet (uplink unused).
        let rules = proxy_ipv4_firewall_rules(tap_subnet, "", &port_str);
        if let Err(e) = ensure_iptables(&rules.accept_check, &rules.accept_add).await {
            tracing::warn!(proxy = %label, error = %e, "proxy firewall: INPUT ACCEPT for tap subnet failed");
        }
        return;
    };

    let rules = proxy_ipv4_firewall_rules(tap_subnet, &uplink, &port_str);

    // ACCEPT from tap subnet first (inserted at head so it precedes the DROP).
    if let Err(e) = ensure_iptables(&rules.accept_check, &rules.accept_add).await {
        // CRITICAL: do NOT install the uplink DROP without the ACCEPT guard in
        // place — a position-1 DROP would also drop tap traffic and silently
        // break the guest. Bail (the primary guard is still in place).
        tracing::warn!(proxy = %label, error = %e, "proxy firewall: INPUT ACCEPT for tap subnet failed; skipping uplink DROP to avoid blocking guests");
        return;
    }

    // DROP inbound on the uplink interface to the proxy port. Inserted after the
    // ACCEPT so tap traffic is still allowed.
    if let Err(e) = ensure_iptables(&rules.drop_check, &rules.drop_add).await {
        tracing::warn!(
            proxy = %label,
            error = %e,
            uplink = %uplink,
            port = proxy_port,
            "proxy firewall: DROP on uplink failed; port is WiFi-reachable (primary guard still applies)"
        );
    } else {
        tracing::info!(
            proxy = %label,
            uplink = %uplink,
            subnet = %tap_subnet,
            port = proxy_port,
            "proxy firewall: ACCEPT from tap subnet + DROP on uplink installed"
        );
    }
}

/// Install the git-proxy (`:8788`) IPv4 tap-gateway firewall — see
/// [`setup_proxy_ipv4_firewall`]. The 256-bit git capability is the primary
/// guard; this is depth-in-defence.
pub async fn setup_git_proxy_firewall(tap_subnet: &str, git_proxy_port: u16) {
    setup_proxy_ipv4_firewall("git proxy", tap_subnet, git_proxy_port).await;
}

/// Install the forge-proxy (`:8789`) IPv4 tap-gateway firewall — see
/// [`setup_proxy_ipv4_firewall`]. Mirrors the git-proxy firewall exactly; the
/// mesh ACL (the forge answers only permitted peers) is the primary guard, this
/// closes the WiFi-uplink exposure of the L4 forward.
pub async fn setup_forge_proxy_firewall(tap_subnet: &str, forge_proxy_port: u16) {
    setup_proxy_ipv4_firewall("forge proxy", tap_subnet, forge_proxy_port).await;
}

/// Best-effort teardown of the two tap-keyed `FORWARD ACCEPT` rules
/// [`setup_guest_nat`] installed for `tap_name` (the shared subnet
/// MASQUERADE rule is left — all VMs share it). Without this, every
/// cold-boot/build leaks two FORWARD rules (tap names come from a
/// never-reused monotonic seq). Errors are ignored: the rule may already be
/// gone, and a leaked rule on a now-dead tap is inert.
pub(crate) async fn teardown_guest_nat(tap_name: &str) -> Result<()> {
    let Some(uplink) = default_route_dev().await else {
        return Ok(());
    };
    let dirs: [[&str; 8]; 2] = [
        [
            "-D", "FORWARD", "-i", tap_name, "-o", &uplink, "-j", "ACCEPT",
        ],
        [
            "-D", "FORWARD", "-o", &uplink, "-i", tap_name, "-j", "ACCEPT",
        ],
    ];
    for d in dirs {
        let _ = Command::new("iptables").args(d).output().await;
    }
    Ok(())
}

/// Ensure an iptables rule exists: run the `-C` (check) form; if absent, run
/// the `-A`/`-I` (add) form. Errors only if the *add* itself fails — a missing
/// rule (check returns non-zero) is the normal "needs adding" path.
async fn ensure_iptables(check: &[&str], add: &[&str]) -> Result<()> {
    let present = Command::new("iptables")
        .args(check)
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false);
    if present {
        return Ok(());
    }
    let out = Command::new("iptables")
        .args(add)
        .output()
        .await
        .with_context(|| format!("spawn iptables {}", add.join(" ")))?;
    if !out.status.success() {
        bail!(
            "iptables {} failed: {}",
            add.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// The interface name of the host's default IPv4 route, e.g. `eth0` from
/// `default via 10.0.0.1 dev eth0 proto dhcp ...`. `None` if there is no
/// default route or `ip` is unavailable.
async fn default_route_dev() -> Option<String> {
    let out = Command::new("ip")
        .args(["route", "show", "default"])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_default_dev(&String::from_utf8_lossy(&out.stdout))
}

/// Parse the `dev <iface>` token out of `ip route show default` output. Pure +
/// isolated for unit-testing.
fn parse_default_dev(route_output: &str) -> Option<String> {
    let toks: Vec<&str> = route_output.split_whitespace().collect();
    toks.iter()
        .position(|t| *t == "dev")
        .and_then(|i| toks.get(i + 1))
        .map(|d| (*d).to_owned())
}

/// Derive a (host_ip, guest_ip) /30 pair for VM index `seq` out of
/// `subnet`. We carve sequential /30s: VM `n` gets hosts
/// `base + 4n + 1` (host) and `base + 4n + 2` (guest).
pub(crate) fn derive_link_ips(subnet: &str, seq: u32) -> Result<(Ipv4Addr, Ipv4Addr)> {
    let base = subnet
        .split('/')
        .next()
        .and_then(|s| s.parse::<Ipv4Addr>().ok())
        .ok_or_else(|| anyhow!("invalid tap subnet {subnet:?}"))?;
    let base_u32 = u32::from(base);
    let host = base_u32
        .checked_add(seq * 4 + 1)
        .ok_or_else(|| anyhow!("tap subnet exhausted at seq {seq}"))?;
    let guest = host + 1;
    Ok((Ipv4Addr::from(host), Ipv4Addr::from(guest)))
}

/// Deterministic locally-administered guest MAC from the VM index.
pub(crate) fn derive_guest_mac(seq: u32) -> String {
    let b = seq.to_le_bytes();
    // 02:xx → locally administered, unicast.
    format!("02:FC:{:02X}:{:02X}:{:02X}:{:02X}", b[0], b[1], b[2], b[3])
}

/// How long to wait for firecracker to create its API socket after spawn.
///
/// A raw spawn creates the socket in ~50ms, but a socket created DURING a deploy
/// swap races with the oras image pull (over the mesh relay) + the OCI→ext4
/// conversion + any concurrently-booting guests — all of which can push the
/// fork/exec and socket creation well past a few seconds under I/O/CPU pressure.
/// A tight 5s window turned that transient slowness into a hard "socket never
/// appeared" deploy failure (and a kill→respawn loop), even though the very same
/// image boots fine moments later under calmer load. The poll exits the instant
/// the socket appears, so a generous ceiling costs nothing on the happy path.
const SOCKET_WAIT: Duration = Duration::from_secs(30);

/// Wait (bounded by [`SOCKET_WAIT`]) for firecracker to create its API socket.
async fn wait_for_socket(sock: &Path) -> Result<()> {
    wait_for_socket_within(sock, SOCKET_WAIT).await
}

/// [`wait_for_socket`] with an explicit timeout — split out so the bring-up
/// tolerance is unit-testable without a real 30s wait.
async fn wait_for_socket_within(sock: &Path, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while !sock.exists() {
        if tokio::time::Instant::now() >= deadline {
            bail!("firecracker API socket {} never appeared", sock.display());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// One firecracker REST `PUT` against an explicit API socket — usable by
/// callers that don't hold a [`FirecrackerRuntime`] (the build-VM boots its
/// own short-lived machine). The instance method delegates here.
pub(crate) async fn api_put_sock(
    api_sock: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<()> {
    api_put_sock_with_timeout(api_sock, path, body, API_TIMEOUT).await
}

/// Like [`api_put_sock`] but with a caller-chosen timeout. `PUT /snapshot/create`
/// copies the guest RAM to disk and needs [`SNAPSHOT_CREATE_TIMEOUT`], not the
/// 5s [`API_TIMEOUT`] (TAB-10).
pub(crate) async fn api_put_sock_with_timeout(
    api_sock: &Path,
    path: &str,
    body: &serde_json::Value,
    timeout: Duration,
) -> Result<()> {
    let payload = serde_json::to_vec(body)?;
    let status = tokio::time::timeout(timeout, unix_http_put(api_sock, path, &payload))
        .await
        .map_err(|_| anyhow!("firecracker API timed out on PUT {path}"))??;
    tracing::debug!(verb = "PUT", path, status, "fc API call");
    if !(200..300).contains(&status) {
        bail!("firecracker API PUT {path} returned HTTP {status}");
    }
    Ok(())
}

/// Hand-rolled HTTP/1.1 `PUT` over a unix socket: write the request, read
/// the status line, return the status code. Bodies are small JSON; we send
/// `Content-Length` and read just enough of the reply to learn the status
/// (firecracker responds 204/200 on success).
async fn unix_http_put(sock: &Path, path: &str, body: &[u8]) -> Result<u16> {
    unix_http_verb(sock, "PUT", path, body).await
}

/// Hand-rolled HTTP/1.1 `PATCH` over a unix socket. Same framing as PUT;
/// used for the `/vm` state transitions (pause/resume) which the Firecracker
/// API exposes as PATCH, not PUT.
async fn unix_http_patch(sock: &Path, path: &str, body: &[u8]) -> Result<u16> {
    unix_http_verb(sock, "PATCH", path, body).await
}

/// Shared HTTP/1.1 request writer for PUT and PATCH. Both methods carry a
/// JSON body and expect a 2xx status; the difference is just the verb string.
async fn unix_http_verb(sock: &Path, verb: &str, path: &str, body: &[u8]) -> Result<u16> {
    let mut stream = UnixStream::connect(sock)
        .await
        .with_context(|| format!("connect firecracker socket {}", sock.display()))?;

    let head = format!(
        "{verb} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;

    // Firecracker's API server uses keep-alive and ignores `Connection:
    // close`, so it doesn't close the socket; read just the response head
    // (NOT to EOF, which would hang) and parse its status.
    read_http_status(&mut stream).await
}

#[cfg(test)]
#[path = "linux_tests.rs"]
mod tests;
