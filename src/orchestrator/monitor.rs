//! Per-record reconcile: keep every recorded runner alive (Task 2.4 + 2.5 + 2.7).
//!
//! The orchestrator has no in-memory fleet table — the [`RunnerHandle`] records
//! on disk ARE its source of truth. The whole self-healing / crash-survival
//! story reduces to ONE per-record decision, [`reconcile_record`]:
//!
//! 1. **process** — is `handle.pid` still a live process
//!    ([`process_is_alive`])?
//! 2. **grace window** — was the runner spawned very recently (within
//!    [`SPAWN_GRACE`])? A just-spawned process that hasn't yet bound its
//!    control socket MUST be left alone to avoid creating a duplicate.
//! 3. **control socket** — does the runner answer
//!    [`ControlClient::health`] within a short timeout?
//!
//! The decision matrix (Task 2.7):
//!
//! | pid alive | within grace | socket healthy | action |
//! |-----------|-------------|----------------|--------|
//! | no        | any         | any            | respawn (no kill — pid already gone) |
//! | yes       | yes         | any            | **adopt** (socket not required yet) |
//! | yes       | no          | yes            | adopt |
//! | yes       | no          | no             | **kill** old pid, then respawn |
//!
//! The last row is the "kill-before-respawn" fix: a hung runner past the grace
//! window is killed with `SIGKILL` before the replacement is spawned, so the
//! old process is never orphaned.
//!
//! ## Testability
//!
//! The core decision logic lives in [`decide_pid_grace`] (pure, synchronous,
//! injectable clock + pid-liveness), which returns a [`PidDecision`]. The
//! async `reconcile_record` calls it first, then checks the socket only when
//! `PidDecision::CheckSocket` instructs it to, so the two orthogonal
//! concerns (pid/grace vs. socket) are each independently unit-testable.
//! Integration-level socket behavior is covered by the existing 2.4/2.5 tests.

use std::{path::Path, time::Duration};

use crate::{
    firecracker::pidfile,
    orchestrator::{
        Orchestrator,
        client::ControlClient,
        handle::RunnerHandle,
        restart::{BackoffParams, RestartState, on_exit, on_healthy, should_respawn},
        spawn::spawn_runner,
    },
};

/// Liveness probe for runner processes: returns `true` iff `pid` is a live,
/// non-zombie process.
///
/// Uses `waitpid(pid, WNOHANG)` first: if the process has already exited (even
/// as a zombie waiting for the parent to reap it), `waitpid` reaps it and we
/// report dead. This prevents the grace-window logic from falsely adopting a
/// just-killed runner whose zombie entry is still in the process table (a `kill
/// -0` on a zombie returns 0 / "alive" on POSIX, which would fool the grace check).
///
/// Falls back to `kill(pid, 0)` for non-child processes (e.g. pre-existing
/// runners discovered via readopt after a supervisor restart — those are NOT
/// children of the current supervisor process, so `waitpid` returns `ECHILD`).
fn runner_is_alive(pid: u32) -> bool {
    // pid 0 has process-GROUP semantics for waitpid(2)/kill(2): `waitpid(0)`
    // waits on the caller's own process group and `kill(0, 0)` probes it —
    // both would report pid 0 as "alive" (it is OUR group). A corrupted
    // record/pidfile carrying pid 0 must read as DEAD, never as alive, or the
    // hung-socket path would `kill_pid(0)` = SIGKILL the supervisor's own
    // process group.
    if pid == 0 {
        return false;
    }
    // SAFETY: waitpid is a POSIX syscall. WNOHANG makes it non-blocking: it
    // returns 0 if the child has not yet changed state, or `pid` if it has
    // (exited/stopped). ECHILD (-1 with errno ECHILD) means `pid` is not a
    // child of this process, in which case we fall through to kill(pid,0).
    let wait_ret =
        unsafe { libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) };
    if wait_ret == pid as libc::pid_t {
        // waitpid reaped the zombie — process has definitely exited.
        return false;
    }
    if wait_ret == 0 {
        // waitpid returned immediately: child exists and hasn't changed state
        // (i.e. it is still running). Report alive.
        return true;
    }
    // wait_ret < 0: ECHILD (not our child) or some other error. Fall back to
    // the kill(0) existence check — this is the readopt case where we inherit
    // a runner that was spawned by a previous supervisor instance.
    // SAFETY: kill(pid, 0) — POSIX existence check, sends no signal.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// How long after a spawn to treat an alive-but-unhealthy-socket runner as
/// "still starting" (adopt without requiring socket health). This prevents the
/// monitor from respawning a just-spawned runner before its control socket has
/// had time to bind.
pub const SPAWN_GRACE: Duration = Duration::from_secs(10);

/// Outcome of reconciling a single runner record against its live process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    /// The runner is alive — left running untouched (pid unchanged).
    Adopted,
    /// The runner was dead and a replacement process was spawned.
    Respawned,
    /// The runner was dead but spawning a replacement failed (logged, skipped).
    RespawnFailed,
    /// The runner is dead but its backoff window has not yet elapsed — no
    /// respawn this tick; the monitor will retry on the next pass.
    Backoff,
    /// The runner has exceeded the crash-loop threshold and has been parked —
    /// no further respawns until a new deploy clears the `crash_looped` flag.
    CrashLooped,
}

/// Result of the pure backoff gate check.
///
/// This is what [`backoff_action`] returns. It answers "is this the right
/// moment to fire a respawn?" without touching any I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackoffAction {
    /// The backoff window has elapsed (or no failure has been recorded yet) —
    /// a respawn attempt is warranted.
    RespawnNow,
    /// The backoff window has not yet elapsed — skip this tick and wait.
    Wait,
}

/// Pure backoff gate: wraps [`should_respawn`] in the monitor's vocabulary.
///
/// # Parameters
/// - `restart` — the current per-runner restart state.
/// - `now` — current unix timestamp in seconds (injected for determinism).
pub(crate) fn backoff_action(restart: RestartState, now: u64) -> BackoffAction {
    if should_respawn(restart, now) {
        BackoffAction::RespawnNow
    } else {
        BackoffAction::Wait
    }
}

/// Result of the pure pid+grace decision step (first half of reconciliation).
///
/// This is what [`decide_pid_grace`] returns. It answers "given the pid liveness
/// and the spawn age, what do I need next?" without touching the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PidDecision {
    /// Pid is dead — spawn a replacement immediately (no kill needed).
    RespawnDead,
    /// Pid is alive and within the grace window — adopt unconditionally.
    /// (Do NOT check the socket: the runner may not have bound it yet.)
    AdoptInGrace,
    /// Pid is alive and past the grace window — check the socket next.
    CheckSocket,
}

/// Pure, synchronous decision: given pid liveness and spawn age, return what
/// to do next. All inputs are injected so the function is deterministic in
/// unit tests.
///
/// # Parameters
/// - `pid` — the recorded pid.
/// - `spawned_at` — unix-seconds timestamp of the last spawn (`0` = absent →
///   treated as age = `now_secs`, i.e. always past grace).
/// - `now_secs` — current unix timestamp in seconds (injected clock).
/// - `is_pid_alive` — probe returning `true` iff the pid is a live process.
pub(crate) fn decide_pid_grace(
    pid: u32,
    spawned_at: u64,
    now_secs: u64,
    is_pid_alive: impl Fn(u32) -> bool,
) -> PidDecision {
    if !is_pid_alive(pid) {
        return PidDecision::RespawnDead;
    }
    let age_secs = now_secs.saturating_sub(spawned_at);
    if age_secs < SPAWN_GRACE.as_secs() {
        PidDecision::AdoptInGrace
    } else {
        PidDecision::CheckSocket
    }
}

/// After this many consecutive failed respawns with no healthy window, the
/// monitor parks the runner (stops respawning it) and sets `crash_looped =
/// true` on its record. A new deploy clears the flag; a supervisor restart
/// respects the flag (parked runners stay parked until re-deployed).
///
/// 10 attempts covers the full backoff ladder — the 9 waits before the 10th
/// (parking) failure are 10+20+40+80+160 s, then 300 s × 4 = 1510 s ≈ 25 min,
/// closer to ~30 min with monitor-tick granularity and per-attempt boot time —
/// so a transient coordinator outage is survived before the breaker trips.
///
/// Also covers spawn-error loops (`RespawnFailed`), where the count advances
/// per tick past the backoff window rather than per raw spawn attempt.
pub const CRASH_LOOP_PARK_THRESHOLD: u32 = 10;

/// STALL window (seconds of ZERO byte-progress) after which an in-flight image
/// pull is considered wedged and the runner is reaped via the normal path.
///
/// The deferral is PROGRESS-based, not age-based: a pull whose `.pull` layout
/// dir keeps GROWING is deferred indefinitely — a 400+ MB base image over a
/// few-Mbit home link legitimately needs 20+ minutes, and the old fixed
/// 600-seconds-since-spawn cap killed exactly those pulls mid-flight. Each kill
/// orphaned the live `oras` child (reparented to PID 1, still downloading) and
/// the respawned runner started ANOTHER from-scratch pull beside it — piling up
/// concurrent pulls that divided the link until none could ever finish, while
/// the saturated link black-holed the host's whole mesh control plane (the MSI
/// outage of 2026-07-03). Progress ⇒ defer; a full window with zero new bytes ⇒
/// genuinely wedged ⇒ reap (and the reap now also kills the pull process — see
/// [`kill_fc_child_for_uuid`]).
pub const PULL_GRACE_SECS: u64 = 600;

/// Send `SIGKILL` to `pid`. Best-effort: logs on failure (e.g. permission
/// error or already-reaped pid).
fn kill_pid(pid: u32) {
    // pid 0 means "the caller's own process group" to kill(2): SIGKILLing it
    // would take down the supervisor (and, in tests, the test binary + cargo +
    // shell). A corrupted record/pidfile with pid 0 must be a no-op here.
    if pid == 0 {
        tracing::warn!("refusing to SIGKILL pid 0 (own process group) — corrupted record?");
        return;
    }
    // SAFETY: `libc::kill` is a standard POSIX syscall. SIGKILL to a
    // (possibly dead) pid is harmless — ESRCH is simply logged.
    let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(pid, error = %err, "SIGKILL to hung runner failed (may already be gone)");
    }
}

/// The systemd-scope names that, post-F1, own the firecracker process for an
/// app — so a per-uuid reap must `systemctl stop` ALL of them to actually kill
/// the scoped FC (not merely the `systemd-run` wrapper PID in the pidfile).
///
/// The FC spawn keys its CPU scope on the SAME `vm_key` as the tap/socket
/// (`fc_identity_for_key`): a deploy boots under `uuid:reff` and a cold start
/// under `uuid` (`firecracker/linux.rs` `cold_boot` / `launch_with_uuid`). We
/// can't know from the call site which key a given live FC used, so we return
/// BOTH candidate scope names when an `image_ref` is known (deploy-key first,
/// then the defensive cold-start key), or just the cold-start key when it isn't.
/// `stop_fc_scope` is idempotent and a no-op for a non-existent scope, so
/// stopping a name that was never used is harmless. Pure (testable).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn fc_scope_names_for_uuid(uuid: &str, image_ref: Option<&str>) -> Vec<String> {
    let mut names = Vec::with_capacity(2);
    // Deploy key (`uuid:reff`) — the production serving identity. First so the
    // most-likely-live scope is torn down first.
    if let Some(reff) = image_ref {
        names.push(crate::firecracker::cpu_scope::scope_name(&format!(
            "{uuid}:{reff}"
        )));
    }
    // Cold-start key (`uuid`) — the bare cold-boot identity; also defensive when
    // a record carries a stale/absent reff.
    names.push(crate::firecracker::cpu_scope::scope_name(uuid));
    names
}

/// #17 — the host TAP name(s) a live FC for `uuid` may own, reconstructed from
/// the SAME keys as the scope/socket ([`fc_scope_names_for_uuid`] /
/// [`crate::firecracker::fc_tap_name_for_key`]): a deploy boots its tap under
/// `uuid:reff` and a cold start under `uuid`. The call site can't know which key
/// a given leaked FC used, so we return BOTH candidate tap names when an
/// `image_ref` is known (deploy-key first), or just the cold-start key when it
/// isn't. `delete_fc_tap` is idempotent + a no-op for a non-existent tap, so
/// deleting a name that was never used is harmless. Pure (testable on macOS).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn fc_tap_names_for_uuid(uuid: &str, image_ref: Option<&str>) -> Vec<String> {
    let mut names = Vec::with_capacity(2);
    if let Some(reff) = image_ref {
        names.push(crate::firecracker::fc_tap_name_for_key(&format!(
            "{uuid}:{reff}"
        )));
    }
    names.push(crate::firecracker::fc_tap_name_for_key(uuid));
    names
}

/// Kill the Firecracker child process for `uuid` by reading its pidfile from
/// `data_dir`, AND tear down its CPU-capped systemd scope(s). Best-effort: logs
/// if the pidfile is absent or the kill fails.
///
/// Mechanism: the runner writes `<data_dir>/tabbify-fc-<uuid>.pid` after
/// spawning the firecracker child (via [`crate::firecracker::pidfile::write`]).
/// When the runner is killed by the monitor the firecracker child is NOT
/// automatically reaped — it was spawned by the runner (not the supervisor)
/// and `setsid` is NOT called on the FC child, so it gets reparented to PID 1
/// and spins forever at 100% CPU. Reading + consuming the pidfile here lets
/// the supervisor kill the orphan before it spawns a fresh runner.
///
/// F1 (audit #93) — CRITICAL completeness fix: on a systemd host the FC now runs
/// inside a `systemd-run --scope`, so the pidfile records the `systemd-run`
/// WRAPPER pid, not firecracker. SIGKILLing that wrapper does NOT propagate into
/// the scope cgroup — the firecracker reparents to PID 1 and the scope leaks (a
/// capped-but-live "hot furnace" the F2 sweep can't catch, because a same-version
/// respawn boots an FC on the IDENTICAL socket). So after killing the pidfile pid
/// we ALSO `systemctl stop` the reconstructed scope name(s) (deploy key
/// `uuid:reff` + cold-start key `uuid` — see [`fc_scope_names_for_uuid`]).
/// `stop_fc_scope` is idempotent + a no-op off-systemd, so this is safe on every
/// host. `image_ref` is the record's deployed ref (the caller passes it from the
/// `RunnerHandle` / loaded record); `None` falls back to the cold-start scope.
///
/// The pidfile is consumed by this call (removed from disk) so the fresh
/// runner's own cold-start reconciliation does not re-kill the NEW FC.
///
/// #17 — ATOMIC per-uuid FC teardown. Historically the orphan cleanup was split
/// across several steps that did not all run together (pidfile-pid kill, the F1
/// scope `systemctl stop`, the F2 record-less sweep, and — only on `purge_app` —
/// the rootfs/cache purge), so a reaped FC could leave its scope, its host TAP,
/// or its NAT rules behind. This function is now THE single place that tears down
/// the in-host footprint of one app's FC, in one shot:
///
///   1. SIGKILL the pidfile pid (the `systemd-run` wrapper, or the bare FC).
///   2. `systemctl stop` the reconstructed CPU scope(s) (F1) — kills the real FC
///      inside the cgroup when (1) only hit the wrapper.
///   3. `ip link del` the reconstructed host TAP(s) (#17) — the runner's `Drop`
///      deletes the tap on a graceful exit, but on an abnormal death (the orphan
///      case this function exists for) Drop never runs, so the tap leaks. Doing
///      it here means the FC process AND its network device die together.
///
/// Steps 2+3 reconstruct the deploy-key (`uuid:reff`) + cold-start (`uuid`)
/// identities the SAME way the spawn keyed them (see [`fc_scope_names_for_uuid`]
/// / [`fc_tap_names_for_uuid`]); every primitive is idempotent + a no-op for a
/// non-existent scope/tap, so this is safe to call before a respawn (the fresh
/// boot re-creates its tap via `setup_tap`) and on a record-less orphan.
///
/// The app's MESH REGISTRATION needs no explicit withdrawal here: in the FC model
/// each app IS its own mesh peer — the runner joins claiming
/// `requested_ula = derive_app_ula(uuid)` via `AppHost::mesh_self` (no host-side
/// `host_app_ula` advertisement to unhost). When the runner process dies its mesh
/// peer / TUN identity dies with it and the coordinator ages the peer out, so
/// killing the FC + its runner already removes the registration. (The on-disk
/// runner RECORD is cleared by the caller: `forget_record` on purge, or kept on a
/// respawn/stop by design.)
///
/// The on-disk ROOTFS/cache is INTENTIONALLY not purged here: the respawn, park,
/// and stop paths all reuse it (deleting it would force a re-pull or brick a
/// restart). The real destructive cache reclaim stays in `purge_app`, which calls
/// this for the process/scope/tap teardown and then `purge_cache` for the rootfs.
///
/// Exposed as `pub(crate)` so `purge_app` / `stop_app` reuse the same reaping
/// logic (FIX C: purge must also reap the FC orphan).
pub(crate) fn kill_fc_child_for_uuid(data_dir: &Path, uuid: &str, image_ref: Option<&str>) {
    // Reap any in-flight/orphaned image pull FIRST: a SIGKILLed runner does not
    // take its `oras` child with it (it reparents to PID 1 and keeps
    // downloading), and a respawn would start a SECOND from-scratch pull beside
    // it — on a slow link the accumulated orphans divide the bandwidth until no
    // pull can finish while saturating the host's mesh (the 2026-07-03 MSI
    // outage). Every per-uuid teardown (park / respawn / stop / purge / hung
    // reap) funnels through here, so this is the single choke point.
    kill_pull_procs_for_uuid(data_dir, uuid);
    if let Some(fc_pid) = pidfile::take(data_dir, uuid) {
        tracing::info!(
            uuid,
            fc_pid,
            "killing orphaned FC child (pidfile pid) before runner respawn"
        );
        pidfile::kill_stale_if_alive(fc_pid, pidfile::process_is_alive);
    }
    // F1 — also stop the CPU-capped scope(s): on systemd the pidfile pid above is
    // only the `systemd-run` wrapper; the firecracker lives inside the scope and
    // survives the wrapper's death. `stop_fc_scope` SIGKILLs everything in the
    // scope cgroup (the real FC) and GCs the unit; idempotent + no-op off-systemd.
    // #17 — and `ip link del` the host tap(s): on an abnormal runner death the
    // runner's Drop never deletes the tap, so it leaks alongside the FC. Both
    // primitives live in the Linux-only runtime (scopes + taps only exist on a
    // Linux host), so the teardown is `cfg`-gated; the pure name reconstructions
    // ([`fc_scope_names_for_uuid`] / [`fc_tap_names_for_uuid`]) stay
    // cross-platform tested.
    #[cfg(target_os = "linux")]
    {
        for scope in fc_scope_names_for_uuid(uuid, image_ref) {
            crate::firecracker::linux::stop_fc_scope(&scope);
        }
        for tap in fc_tap_names_for_uuid(uuid, image_ref) {
            crate::firecracker::linux::delete_fc_tap(&tap);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = image_ref; // used only by the Linux scope/tap-teardown above
}

/// The path token that a live `oras` image pull for `uuid` carries in its
/// argv (`oras copy … --to-oci-layout <data_dir>/apps/<uuid>/fc/.pull/oci`).
/// Pure (testable); used by [`pull_in_progress`] to recognise the pull process.
fn pull_path_needle(data_dir: &Path, uuid: &str) -> Option<String> {
    data_dir
        .join("apps")
        .join(uuid)
        .join("fc")
        .join(".pull")
        .to_str()
        .map(str::to_owned)
}

/// `true` when a process's `/proc/<pid>/cmdline` (NUL-separated argv) references
/// `needle`. The pull path is a single argv token (no embedded NUL), so a plain
/// substring search over the lossy-decoded bytes is correct. Pure (testable).
fn cmdline_matches_pull(cmdline: &[u8], needle: &str) -> bool {
    !needle.is_empty() && String::from_utf8_lossy(cmdline).contains(needle)
}

/// `true` if an `oras` image pull is STILL running for `uuid` — i.e. a live
/// process's cmdline references this runner's `.pull` OCI-layout path. The
/// monitor uses this to AVOID reaping a runner whose control socket is merely
/// "not up yet because it is still pulling its image" over the slow relay (vs
/// genuinely hung). Linux-only (scans `/proc`); returns `false` elsewhere,
/// which is fine — FC runners only run on Linux.
fn pull_in_progress(data_dir: &Path, uuid: &str) -> bool {
    !pull_pids_for_uuid(data_dir, uuid).is_empty()
}

/// The pids of every live process whose cmdline references `uuid`'s `.pull`
/// OCI-layout path — i.e. the in-flight `oras copy` for this runner (plus any
/// orphan a prior runner death left behind). Linux-only (`/proc` scan); empty
/// elsewhere. Never includes the supervisor itself (its argv carries no
/// per-uuid pull path).
fn pull_pids_for_uuid(data_dir: &Path, uuid: &str) -> Vec<u32> {
    let Some(needle) = pull_path_needle(data_dir, uuid) else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut pids = Vec::new();
    for entry in entries.flatten() {
        // Only numeric `/proc/<pid>` entries carry a `cmdline`.
        let Some(pid) = entry
            .file_name()
            .to_str()
            .filter(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        if let Ok(bytes) = std::fs::read(entry.path().join("cmdline")) {
            if cmdline_matches_pull(&bytes, &needle) {
                pids.push(pid);
            }
        }
    }
    pids
}

/// SIGKILL every live pull process for `uuid` (the in-flight `oras copy` and
/// any orphan a prior runner death reparented to PID 1). Returns the count
/// killed.
///
/// A SIGKILLed runner does NOT take its `oras` child with it: the child
/// reparents to PID 1 and keeps downloading at full tilt for tens of minutes —
/// and the respawned runner then starts ANOTHER from-scratch pull beside it.
/// On a slow home link the accumulated orphans divide the bandwidth until no
/// pull can ever finish, and the saturated link black-holes the host's whole
/// mesh control plane (the MSI outage of 2026-07-03: three concurrent 420 MB
/// pulls of the same digest). Every per-uuid teardown must therefore reap the
/// pull processes alongside the FC child.
fn kill_pull_procs_for_uuid(data_dir: &Path, uuid: &str) -> usize {
    let pids = pull_pids_for_uuid(data_dir, uuid);
    for &pid in &pids {
        tracing::info!(
            uuid,
            pid,
            "killing in-flight/orphaned image pull before teardown/respawn"
        );
        kill_pid(pid);
    }
    pids.len()
}

/// Byte-progress snapshot of one runner's in-flight image pull: how many bytes
/// its `.pull` layout dir held when last observed, and the monotonic second at
/// which that number last GREW. Drives the progress-based reap deferral
/// ([`pull_stall_update`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullProgress {
    /// Bytes under `<data_dir>/apps/<uuid>/fc/.pull` at the last observation.
    pub bytes: u64,
    /// `now_secs` of the last observation at which `bytes` CHANGED (or the
    /// first observation). The stall clock: `now - last_progress_secs`
    /// compares against [`PULL_GRACE_SECS`].
    pub last_progress_secs: u64,
}

/// Pure progress-vs-stall decision for one monitor tick (testable).
///
/// Given the previous snapshot (if any), the layout dir's CURRENT byte count,
/// and the tick time, returns the snapshot to store plus `defer`:
/// * first observation ⇒ start the stall clock, defer;
/// * `cur_bytes` CHANGED (grew: blobs landing; shrank: the stage dir was wiped
///   for a fresh attempt, or a stale snapshot lingering from a PRIOR pull
///   episode meets a from-scratch re-pull) ⇒ activity — restamp the clock,
///   defer. Progress defers INDEFINITELY by design: however slow the link, a
///   growing pull converges, while killing it forces a from-scratch re-pull
///   that may never converge (the livelock this replaces). Restamp-on-change
///   cannot be gamed into infinite deferral by kill→re-pull cycling — that
///   cycling advances the restart ladder and trips the
///   [`CRASH_LOOP_PARK_THRESHOLD`] breaker like any other respawn loop;
/// * byte count UNCHANGED within [`PULL_GRACE_SECS`] of the last change ⇒
///   still defer (blob data arrives in bursts; a quiet spell is not a wedge);
/// * unchanged for a full window ⇒ genuinely wedged ⇒ `defer = false` (reap).
#[must_use]
pub const fn pull_stall_update(
    prev: Option<PullProgress>,
    cur_bytes: u64,
    now_secs: u64,
    stall_secs: u64,
) -> (PullProgress, bool) {
    match prev {
        Some(p) if cur_bytes == p.bytes => {
            (p, now_secs.saturating_sub(p.last_progress_secs) < stall_secs)
        }
        // First observation, or the byte count MOVED since the last one.
        _ => (
            PullProgress {
                bytes: cur_bytes,
                last_progress_secs: now_secs,
            },
            true,
        ),
    }
}

/// Total bytes under `dir`, recursively. `0` for a missing/unreadable dir —
/// indistinguishable from an empty pull, which is exactly the conservative
/// reading the stall clock wants (no bytes ⇒ no progress). Symlinks are not
/// followed (the pull layout contains none; not following is the safe default).
fn dir_size_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total = total.saturating_add(dir_size_bytes(&entry.path()));
        } else if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

/// The dir whose growth measures pull progress: the runner's whole `.pull`
/// staging area (`<data_dir>/apps/<uuid>/fc/.pull` — the same token
/// [`pull_path_needle`] matches in the puller's argv), so blobs land in it
/// whatever the layout sub-dir is named.
fn pull_stage_dir(data_dir: &Path, uuid: &str) -> std::path::PathBuf {
    data_dir.join("apps").join(uuid).join("fc").join(".pull")
}

// ── F2.2 (audit #93) — record-less FC orphan sweep ───────────────────────────
//
// During a supervisor crash-loop nothing reaps the FCs left by a prior runner
// death (the reaper IS the supervisor), and there is no per-uuid record/pidfile
// for an FC whose record was already forgotten — or for the BUILD VM, which
// (now) has a fixed pidfile but no runner record at all. Such an FC reparents to
// PID 1 and spins/holds RAM until reboot. This sweep is the INDEPENDENT backstop:
// scan `/proc` for tabbify firecracker processes, and SIGKILL any that are
// PROVABLY orphaned. It is SAFETY-CRITICAL — a false positive kills a serving
// app — so every gate fails toward NOT killing.

/// The deterministic build-VM api-socket (`fc-bld0` is the fixed build tap).
/// Uses the CROSS-PLATFORM build-tap const so this pure fn (and its tests) stay
/// macOS-testable (the Linux-only `build_vm` module re-exports the same value).
//
// The sweep that calls these pure fns is `#[cfg(target_os = "linux")]` (it scans
// `/proc`), so on a non-Linux build they have no non-test caller — but they ARE
// exercised by the cross-platform unit tests below, hence allow-dead-code only
// off Linux (mirrors `pidfile.rs` / `protocol.rs`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn build_fc_api_sock() -> String {
    format!(
        "/tmp/firecracker-{}.sock",
        crate::firecracker::BUILD_TAP_NAME
    )
}

/// Extract the `--api-sock <path>` value from a process's NUL-separated
/// `/proc/<pid>/cmdline` argv, IF this looks like a tabbify firecracker process.
///
/// Recognises BOTH spawn shapes (F1, audit #93):
/// - bare:  `firecracker\0--api-sock\0/tmp/firecracker-<tap>.sock\0`
/// - scoped: `systemd-run\0--scope\0…\0--\0firecracker\0--api-sock\0/tmp/…\0`
///
/// Returns `Some(sock)` only when the argv contains the `firecracker` binary
/// token AND an `--api-sock` whose value is under `/tmp/firecracker-….sock` (the
/// deterministic tabbify socket prefix). Returns `None` for anything else — a
/// non-FC process, a malformed cmdline, or a firecracker we don't recognise — so
/// an unrecognised process is NEVER a kill candidate. Pure (testable).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_tabbify_fc_api_sock(cmdline: &[u8]) -> Option<String> {
    // Split the NUL-separated argv into UTF-8-lossy tokens.
    let argv: Vec<String> = cmdline
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    if argv.is_empty() {
        return None;
    }
    // Must invoke the firecracker binary (the basename of SOME argv token equals
    // `firecracker`). Covers both the bare `firecracker` argv[0] and the scoped
    // `systemd-run … -- firecracker …` form.
    let invokes_fc = argv.iter().any(|tok| {
        std::path::Path::new(tok)
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|base| base == "firecracker")
    });
    if !invokes_fc {
        return None;
    }
    // Find the `--api-sock <value>` pair and require the deterministic tabbify
    // socket prefix `/tmp/firecracker-….sock` (never matches an unrelated FC).
    let pos = argv.iter().position(|t| t == "--api-sock")?;
    let sock = argv.get(pos + 1)?;
    if sock.starts_with("/tmp/firecracker-") && sock.ends_with(".sock") {
        Some(sock.clone())
    } else {
        None
    }
}

/// Build the set of api-sockets belonging to LIVE FCs that the sweep must NEVER
/// kill, from the on-disk runner records.
///
/// For each record this adds BOTH reconstructible identities (the spawn path
/// keys the FC tap on `uuid` for a cold start and on `uuid:reff` for a deploy —
/// see `fc_identity_for_key`), so a live serving FC is always covered whichever
/// key its tap was derived from. The BUILD socket is added unconditionally when
/// a build may be in flight (`build_maybe_running`) — the build VM has no record.
///
/// NOTE — this set is INTENTIONALLY over-inclusive (it errs toward protecting
/// more sockets): the swap's OLD VM (`uuid:old_reff`, whose reff the record no
/// longer carries) is NOT reconstructible here, which is exactly why the sweep
/// ALSO requires the orphan's parent to be PID 1 — a mid-swap old VM still has a
/// live runner parent, so it is excluded by the parent gate, not this set.
/// Pure (testable).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn live_fc_socket_set(records: &[RunnerHandle], build_maybe_running: bool) -> std::collections::HashSet<String> {
    let mut set = std::collections::HashSet::new();
    for r in records {
        // Cold-start key = uuid.
        set.insert(crate::firecracker::fc_api_sock_for_key(&r.uuid));
        // Deploy key = uuid:reff (the current deployed version, if any).
        if let Some(reff) = r.image_ref.as_deref() {
            set.insert(crate::firecracker::fc_api_sock_for_key(&format!(
                "{}:{}",
                r.uuid, reff
            )));
        }
    }
    if build_maybe_running {
        set.insert(build_fc_api_sock());
    }
    set
}

/// THE safety-critical kill decision for ONE discovered firecracker process.
///
/// Returns `true` (REAP) only when ALL of these provable-orphan conditions hold;
/// any uncertainty (a `None`, an unreadable field) returns `false` (DO NOT KILL):
///
/// 1. `api_sock` — the process WAS recognised as a tabbify FC with a
///    `/tmp/firecracker-…sock` (the caller passes `Some` only then).
/// 2. `parent_is_pid1 == true` — the FC has been REPARENTED to init, i.e. the
///    runner (or its `systemd-run` wrapper) that spawned it is DEAD. A LIVE FC —
///    serving OR mid-swap OR scoped — always has a living parent (the runner, or
///    the foreground `systemd-run` child of the runner), so its parent is never
///    PID 1. This is the strongest orphan signal and the primary guard.
/// 3. `!live_sockets.contains(api_sock)` — its socket matches NO live runner
///    record (and no in-flight build). Belt-and-braces with (2).
///
/// Both (2) AND (3) are required: (2) alone could in theory misfire if a future
/// subreaper changed reparenting; (3) alone can't see the unreconstructible
/// mid-swap old VM. Requiring BOTH makes a false-positive kill of a live app
/// vanishingly unlikely — the audit's "fail toward NOT killing" bar. Pure.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn fc_orphan_should_reap(
    api_sock: &str,
    parent_is_pid1: bool,
    live_sockets: &std::collections::HashSet<String>,
) -> bool {
    parent_is_pid1 && !live_sockets.contains(api_sock)
}

/// Read `PPid:` from `/proc/<pid>/status`. `Some(ppid)` on success; `None` if the
/// file is gone/unreadable (the process exited mid-scan) — the caller treats
/// `None` as "uncertain ⇒ do not kill".
#[cfg(target_os = "linux")]
fn read_parent_pid(pid: u32) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("PPid:") {
            return rest.trim().parse::<u32>().ok();
        }
    }
    None
}

impl Orchestrator {
    /// F2.2 (audit #93) — INDEPENDENT record-less FC orphan sweep. Scans `/proc`
    /// for tabbify firecracker processes and SIGKILLs any PROVABLY orphaned one
    /// (reparented to PID 1 AND matching no live runner record / in-flight build).
    /// Called on `readopt` (startup, after a possible crash-loop) and on each
    /// monitor `tick`. Linux-only (`/proc`); a no-op elsewhere.
    ///
    /// Returns the count of orphans reaped (0 in the common healthy case). The
    /// kill gate ([`fc_orphan_should_reap`]) fails toward NOT killing.
    #[cfg(target_os = "linux")]
    pub(crate) fn sweep_orphan_fcs(&self) -> usize {
        let records = match RunnerHandle::list(&self.runner_dir) {
            Ok(r) => r,
            Err(e) => {
                // Can't enumerate records ⇒ can't build the live-socket safe-set
                // ⇒ DO NOT sweep (a sweep with an empty safe-set would nuke every
                // live FC). Fail toward not killing.
                tracing::warn!(error = %e, "orphan-FC sweep: cannot list runner records — skipping (fail-safe)");
                return 0;
            }
        };
        // A build may be running iff the build pidfile exists OR a deploy is in
        // flight for any uuid; protect the build socket in either case.
        let build_maybe_running = self.build_maybe_running();
        let live = live_fc_socket_set(&records, build_maybe_running);

        let Ok(entries) = std::fs::read_dir("/proc") else {
            return 0;
        };
        let mut reaped = 0usize;
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .filter(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
                continue;
            };
            let Some(api_sock) = parse_tabbify_fc_api_sock(&cmdline) else {
                continue; // not a recognised tabbify FC ⇒ never a candidate
            };
            // Parent gate: unreadable ⇒ uncertain ⇒ do not kill.
            let parent_is_pid1 = read_parent_pid(pid) == Some(1);
            if fc_orphan_should_reap(&api_sock, parent_is_pid1, &live) {
                tracing::warn!(
                    pid,
                    api_sock = %api_sock,
                    "orphan-FC sweep: reaping record-less firecracker orphan (parent=pid1, no live record)"
                );
                pidfile::kill_stale_if_alive(pid, pidfile::process_is_alive);
                reaped += 1;
            }
        }
        if reaped > 0 {
            tracing::info!(reaped, "orphan-FC sweep: reaped record-less FC orphans");
        }
        reaped
    }

    /// Is a build possibly running right now? `true` if the build pidfile exists
    /// (a build VM was spawned and hasn't been reaped) OR any uuid is mid-deploy
    /// (a build precedes the deploy). Conservative — when in doubt, protect the
    /// build socket from the sweep.
    #[cfg(target_os = "linux")]
    fn build_maybe_running(&self) -> bool {
        let build_pidfile =
            pidfile::path(&self.shared.data_dir, crate::firecracker::BUILD_SCOPE_ID_NAME);
        if build_pidfile.exists() {
            return true;
        }
        // Any in-flight deploy may be in its build phase (build VM not yet pidfiled).
        RunnerHandle::list(&self.runner_dir)
            .map(|recs| recs.iter().any(|r| self.is_deploying(&r.uuid)))
            .unwrap_or(true) // can't tell ⇒ assume yes (protect build socket)
    }

    /// Progress-based reap deferral for a runner whose control socket is not
    /// up because it is still PULLING its image (see [`PULL_GRACE_SECS`]).
    ///
    /// Returns `true` (defer the reap) while a pull process is live for `uuid`
    /// AND it has made byte-progress within the stall window; `false` when no
    /// pull is running (also clears the tracker entry) or the pull has stalled
    /// for a full window (genuinely wedged — the normal reap path then also
    /// kills the pull process via [`kill_fc_child_for_uuid`]).
    fn pull_deferral_active(&self, uuid: &str, now_secs: u64) -> bool {
        if !pull_in_progress(&self.shared.data_dir, uuid) {
            if let Ok(mut map) = self.pull_progress.lock() {
                map.remove(uuid);
            }
            return false;
        }
        let cur_bytes = dir_size_bytes(&pull_stage_dir(&self.shared.data_dir, uuid));
        let Ok(mut map) = self.pull_progress.lock() else {
            // Poisoned tracker (a panic elsewhere): fail toward DEFERRING — a
            // spurious extra tick of grace is harmless, killing a live pull
            // restarts a multi-minute download from scratch.
            return true;
        };
        let prev = map.get(uuid).copied();
        let (next, defer) = pull_stall_update(prev, cur_bytes, now_secs, PULL_GRACE_SECS);
        map.insert(uuid.to_owned(), next);
        defer
    }

    /// Run ONE monitor pass over every recorded runner: probe liveness and
    /// respawn any that are dead (adopt the living ones untouched).
    ///
    /// Returns the list of UUIDs that were respawned this pass (empty when every
    /// runner was healthy). A failure to spawn a replacement for one runner is
    /// logged and skipped — it must not abort the pass for the other runners, so
    /// the method itself only returns `Err` for an unrecoverable failure to even
    /// enumerate the records.
    ///
    /// # Errors
    /// Returns an [`anyhow::Error`] only if the runner directory cannot be
    /// listed. Per-runner respawn failures are logged, not propagated.
    pub async fn tick(&self) -> anyhow::Result<Vec<String>> {
        let records = RunnerHandle::list(&self.runner_dir)?;
        let mut respawned = Vec::new();

        for record in records {
            if self.reconcile_record(&record).await == RecordOutcome::Respawned {
                respawned.push(record.uuid);
            }
        }

        // F2.2 (audit #93): record-less FC orphan sweep AFTER the per-record
        // reconcile (so a just-respawned runner's fresh FC pidfile/record is in
        // place before the sweep reads the live-socket set). Independent of the
        // per-uuid reaper; catches FCs whose record was forgotten + the build VM.
        #[cfg(target_os = "linux")]
        self.sweep_orphan_fcs();

        Ok(respawned)
    }

    /// Reconcile ONE record: adopt it if its runner is alive, else respawn it.
    ///
    /// Implements the Task 2.7 decision matrix (grace window + kill-before-respawn).
    /// This is the single source of truth shared by [`tick`](Self::tick) and
    /// [`readopt`](Self::readopt). A respawn failure is logged and reported as
    /// [`RecordOutcome::RespawnFailed`] — never propagated.
    ///
    /// Backoff is gated via [`backoff_action`]: if the runner is dead but its
    /// next-retry window has not yet elapsed, the function returns
    /// [`RecordOutcome::Backoff`] without touching the process table.
    pub(crate) async fn reconcile_record(&self, record: &RunnerHandle) -> RecordOutcome {
        // Operator-stopped: `stop_app` shut the runner down but PRESERVED its
        // record (so the deploy artifact survives for a later respawn/reset).
        // A stopped record must NOT be respawned — treat it like a parked one
        // until the app is brought back up (a fresh spawn writes `stopped:
        // false`; `reset_app` clears it).
        if record.stopped {
            tracing::debug!(
                uuid = %record.uuid,
                "runner is operator-stopped — skipping respawn until restarted/reset/deployed"
            );
            return RecordOutcome::CrashLooped;
        }

        // Circuit breaker: if this runner is already parked (exceeded the
        // crash-loop threshold), do NOT respawn it until a new deploy writes a
        // fresh record with `crash_looped = false`.
        if record.crash_looped {
            tracing::debug!(
                uuid = %record.uuid,
                "runner is crash-looped (parked) — skipping respawn until re-deployed"
            );
            return RecordOutcome::CrashLooped;
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        match decide_pid_grace(record.pid, record.spawned_at, now_secs, runner_is_alive) {
            PidDecision::RespawnDead => {
                // Gate the actual respawn behind the backoff policy.
                if backoff_action(record.restart, now_secs) == BackoffAction::Wait {
                    tracing::debug!(
                        uuid = %record.uuid,
                        next_retry_at = record.restart.next_retry_at,
                        "runner dead but backoff window not elapsed — skipping this tick"
                    );
                    return RecordOutcome::Backoff;
                }

                let new_restart = on_exit(record.restart, BackoffParams::default(), now_secs);

                // Circuit breaker: park the runner after N consecutive failures.
                if new_restart.consecutive_failures >= CRASH_LOOP_PARK_THRESHOLD {
                    tracing::error!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        consecutive_failures = new_restart.consecutive_failures,
                        threshold = CRASH_LOOP_PARK_THRESHOLD,
                        "runner exceeded crash-loop threshold — parking (no further respawns until re-deployed)"
                    );
                    // Kill the FC orphan (best-effort) even when parking: the
                    // runner is dead and the FC child is spinning at 100% CPU.
                    kill_fc_child_for_uuid(
                        &self.shared.data_dir,
                        &record.uuid,
                        record.image_ref.as_deref(),
                    );
                    return self.park_runner(record, new_restart).await;
                }

                tracing::warn!(
                    uuid = %record.uuid,
                    pid = record.pid,
                    control_sock = %record.control_sock.display(),
                    "runner is dead — respawning"
                );
                // Kill any lingering FC child left by the dead runner before
                // spawning a fresh one so we do not accumulate orphaned VMs.
                kill_fc_child_for_uuid(
                    &self.shared.data_dir,
                    &record.uuid,
                    record.image_ref.as_deref(),
                );
                self.do_respawn(record, None, new_restart).await
            }

            PidDecision::AdoptInGrace => {
                tracing::debug!(
                    uuid = %record.uuid,
                    pid = record.pid,
                    "runner alive within grace window — adopted (socket check skipped)"
                );
                // Update healthy timestamp but do not reset failures yet — the
                // runner is still within its startup window.
                self.persist_healthy_if_changed(record, now_secs);
                RecordOutcome::Adopted
            }

            PidDecision::CheckSocket => {
                // Past grace window: require the socket to be healthy.
                let socket_ok = ControlClient::new(&record.control_sock)
                    .health()
                    .await
                    .is_ok();

                if socket_ok {
                    tracing::debug!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        "runner alive and socket healthy — adopted"
                    );
                    // Runner is healthy: advance state (may reset failure count
                    // once stable_secs have elapsed since last exit).
                    self.persist_healthy_if_changed(record, now_secs);
                    RecordOutcome::Adopted
                } else if self.is_deploying(&record.uuid) {
                    // A deploy is in flight for this uuid: the runner is alive but
                    // briefly busy/unresponsive on its control socket while it
                    // builds the new VM + swaps. Killing it now would abort the
                    // deploy and orphan the half-built VM. Defer the reap — the
                    // deploy guard clears once the deploy finishes, and the next
                    // tick re-evaluates with the runner responsive again.
                    tracing::info!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        "deploy in progress; deferring reap (runner busy mid-deploy)"
                    );
                    RecordOutcome::Adopted
                } else if self.pull_deferral_active(&record.uuid, now_secs) {
                    // The control socket is not up yet because the runner is
                    // STILL PULLING its image over the (slow, relay-only) mesh —
                    // not hung. Reaping now kills the in-flight `oras` pull; the
                    // replacement re-pulls from scratch and the runner never
                    // converges (an endless respawn loop on a slow link). Defer
                    // for as long as the pull keeps making BYTE-PROGRESS —
                    // however slow the link, a growing pull converges — and
                    // reap only once it has stalled for a full
                    // [`PULL_GRACE_SECS`] window (genuinely wedged; the reap
                    // then also kills the pull process so no orphan keeps
                    // eating the link).
                    tracing::info!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        age_secs = now_secs.saturating_sub(record.spawned_at),
                        "image pull in progress and making progress — deferring reap"
                    );
                    RecordOutcome::Adopted
                } else {
                    // Gate kill-then-respawn behind the backoff policy too.
                    if backoff_action(record.restart, now_secs) == BackoffAction::Wait {
                        tracing::debug!(
                            uuid = %record.uuid,
                            next_retry_at = record.restart.next_retry_at,
                            "hung runner socket unhealthy but backoff window not elapsed — skipping"
                        );
                        return RecordOutcome::Backoff;
                    }

                    let new_restart = on_exit(record.restart, BackoffParams::default(), now_secs);

                    // Circuit breaker: park after N consecutive failures.
                    if new_restart.consecutive_failures >= CRASH_LOOP_PARK_THRESHOLD {
                        tracing::error!(
                            uuid = %record.uuid,
                            pid = record.pid,
                            consecutive_failures = new_restart.consecutive_failures,
                            threshold = CRASH_LOOP_PARK_THRESHOLD,
                            "runner exceeded crash-loop threshold — parking (no further respawns until re-deployed)"
                        );
                        kill_pid(record.pid);
                        kill_fc_child_for_uuid(
                        &self.shared.data_dir,
                        &record.uuid,
                        record.image_ref.as_deref(),
                    );
                        return self.park_runner(record, new_restart).await;
                    }

                    tracing::warn!(
                        uuid = %record.uuid,
                        pid = record.pid,
                        control_sock = %record.control_sock.display(),
                        "runner alive but socket unhealthy past grace window — killing before respawn"
                    );
                    kill_pid(record.pid);
                    kill_fc_child_for_uuid(
                        &self.shared.data_dir,
                        &record.uuid,
                        record.image_ref.as_deref(),
                    );
                    self.do_respawn(record, Some(record.pid), new_restart).await
                }
            }
        }
    }

    /// Compute the `on_healthy` state for `record` and, if it differs from the
    /// current state, persist the updated record to disk. No-op when the state
    /// is already up-to-date (so a healthy steady-state runner costs only one
    /// cheap comparison per tick once it is stable).
    fn persist_healthy_if_changed(&self, record: &RunnerHandle, now_secs: u64) {
        let new_restart = on_healthy(record.restart, BackoffParams::default(), now_secs);
        if new_restart != record.restart {
            let mut updated = record.clone();
            updated.restart = new_restart;
            if let Err(e) = updated.save(&self.runner_dir) {
                tracing::warn!(
                    uuid = %record.uuid,
                    error = %e,
                    "failed to persist updated healthy restart state"
                );
            }
        }
    }

    /// Spawn a replacement runner, stamp the bumped `new_restart` state onto
    /// the freshly written record, and return the outcome.
    ///
    /// `killed_pid` is provided when the caller already sent SIGKILL to an old
    /// process (logged for traceability). `new_restart` is the result of
    /// [`on_exit`] computed by the caller (it is merged into the new record so
    /// consecutive-failure counts survive a supervisor restart).
    async fn do_respawn(
        &self,
        record: &RunnerHandle,
        killed_pid: Option<u32>,
        new_restart: RestartState,
    ) -> RecordOutcome {
        let spec = self.shared.spawn_spec_for(record);
        match spawn_runner(&spec, &self.runner_dir).await {
            Ok((mut new_handle, _child)) => {
                // Merge the bumped restart state into the fresh record before
                // persisting it, so the failure count is not lost on the next tick.
                new_handle.restart = new_restart;
                if let Err(e) = new_handle.save(&self.runner_dir) {
                    tracing::warn!(
                        uuid = %new_handle.uuid,
                        error = %e,
                        "failed to persist restart state after respawn (will recover on next tick)"
                    );
                }
                if let Some(old) = killed_pid {
                    tracing::info!(
                        uuid = %new_handle.uuid,
                        old_pid = old,
                        new_pid = new_handle.pid,
                        "killed hung runner and spawned replacement"
                    );
                } else {
                    tracing::info!(
                        uuid = %new_handle.uuid,
                        old_pid = record.pid,
                        new_pid = new_handle.pid,
                        "respawned dead runner"
                    );
                }
                RecordOutcome::Respawned
            }
            Err(e) => {
                tracing::error!(
                    uuid = %record.uuid,
                    error = %e,
                    "failed to respawn dead runner (will retry next tick)"
                );
                RecordOutcome::RespawnFailed
            }
        }
    }

    /// Mark a runner as crash-looped (parked): persist the updated record with
    /// `crash_looped = true` so neither this supervisor nor a restarted one
    /// will respawn it until a new deploy writes a fresh record.
    async fn park_runner(&self, record: &RunnerHandle, new_restart: RestartState) -> RecordOutcome {
        let mut parked = record.clone();
        parked.restart = new_restart;
        parked.crash_looped = true;
        if let Err(e) = parked.save(&self.runner_dir) {
            tracing::warn!(
                uuid = %record.uuid,
                error = %e,
                "failed to persist crash-looped state (runner will be parked in memory until next tick persists it)"
            );
        }
        RecordOutcome::CrashLooped
    }
}

// ── Unit tests for the pure pid+grace decision function ──────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use tempfile::TempDir;

    use super::*;
    use crate::orchestrator::{
        Orchestrator, SharedRunnerConfig, handle::RunnerHandle, restart::RestartState,
    };

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // ── image-pull-in-progress guard (don't reap a runner mid-pull) ───────────

    #[test]
    fn pull_path_needle_targets_the_runner_pull_dir() {
        let needle = pull_path_needle(Path::new("/opt/tabbify/data"), "abc-123")
            .expect("needle");
        assert_eq!(needle, "/opt/tabbify/data/apps/abc-123/fc/.pull");
    }

    #[test]
    fn cmdline_matches_pull_recognises_the_oras_pull_argv() {
        let needle = pull_path_needle(Path::new("/opt/tabbify/data"), "abc-123").unwrap();
        // Realistic /proc/<pid>/cmdline: NUL-separated argv of the live oras pull.
        let cmdline = b"oras\0copy\0--from-plain-http\0[fd5a::1]:5000/tabbify/x:tag\0--to-oci-layout\0/opt/tabbify/data/apps/abc-123/fc/.pull/oci\0";
        assert!(cmdline_matches_pull(cmdline, &needle), "pull argv must match");
        // A different uuid's pull must NOT match (no false-positive defer).
        let other = pull_path_needle(Path::new("/opt/tabbify/data"), "zzz-999").unwrap();
        assert!(!cmdline_matches_pull(cmdline, &other), "other uuid must not match");
        // An unrelated process (the FC itself) must NOT match.
        let fc = b"firecracker\0--api-sock\0/tmp/firecracker-fc-deadbeef.sock\0";
        assert!(!cmdline_matches_pull(fc, &needle), "unrelated proc must not match");
    }

    #[test]
    fn cmdline_matches_pull_is_false_for_empty_needle() {
        assert!(!cmdline_matches_pull(b"anything", ""));
    }

    // ── progress-based pull-stall clock (slow-link livelock fix) ──────────────

    /// First observation and every byte-count CHANGE defer and (re)stamp the
    /// stall clock — a growing pull on an arbitrarily slow link is never
    /// reaped (killing it forces a from-scratch re-pull that may never
    /// converge; the 2026-07-03 MSI livelock).
    #[test]
    fn pull_stall_update_defers_on_first_observation_and_growth() {
        // First observation: clock starts, defer.
        let (s1, defer) = pull_stall_update(None, 1_000, 100, PULL_GRACE_SECS);
        assert!(defer, "first observation must defer");
        assert_eq!(s1, PullProgress { bytes: 1_000, last_progress_secs: 100 });
        // Growth: restamp, defer — even if the previous stamp is ancient.
        let old = PullProgress { bytes: 1_000, last_progress_secs: 100 };
        let (s2, defer) =
            pull_stall_update(Some(old), 2_000, 100 + 10 * PULL_GRACE_SECS, PULL_GRACE_SECS);
        assert!(defer, "byte growth must defer regardless of elapsed time");
        assert_eq!(s2.bytes, 2_000);
        assert_eq!(s2.last_progress_secs, 100 + 10 * PULL_GRACE_SECS);
    }

    /// An UNCHANGED byte count within the stall window still defers (blobs
    /// arrive in bursts; a quiet spell is not a wedge) and keeps the ORIGINAL
    /// stamp so the window is not self-extending.
    #[test]
    fn pull_stall_update_defers_within_stall_window_without_growth() {
        let prev = PullProgress { bytes: 5_000, last_progress_secs: 100 };
        let (s, defer) =
            pull_stall_update(Some(prev), 5_000, 100 + PULL_GRACE_SECS - 1, PULL_GRACE_SECS);
        assert!(defer, "quiet spell inside the window must defer");
        assert_eq!(s, prev, "no-change must keep the original stamp (no self-extension)");
    }

    /// A full stall window with ZERO byte change is a genuinely wedged pull —
    /// `defer = false` hands the runner to the normal reap path (which also
    /// kills the pull process via `kill_fc_child_for_uuid`).
    #[test]
    fn pull_stall_update_reaps_after_full_stall_window() {
        let prev = PullProgress { bytes: 5_000, last_progress_secs: 100 };
        let (_, defer) =
            pull_stall_update(Some(prev), 5_000, 100 + PULL_GRACE_SECS, PULL_GRACE_SECS);
        assert!(!defer, "a full window of zero progress must stop deferring");
    }

    /// A SHRINK (stage dir wiped for a fresh attempt, or a stale snapshot from
    /// a PRIOR pull episode meeting a from-scratch re-pull) counts as activity:
    /// restamp + defer, so a fresh pull never inherits a long-expired clock and
    /// gets reaped on its very first tick.
    #[test]
    fn pull_stall_update_restamps_on_shrink_fresh_attempt() {
        let stale = PullProgress { bytes: 400_000_000, last_progress_secs: 100 };
        let (s, defer) =
            pull_stall_update(Some(stale), 64, 100 + 10 * PULL_GRACE_SECS, PULL_GRACE_SECS);
        assert!(defer, "a fresh from-scratch pull must get its own stall window");
        assert_eq!(s.bytes, 64);
        assert_eq!(s.last_progress_secs, 100 + 10 * PULL_GRACE_SECS);
    }

    /// `dir_size_bytes` sums files recursively and reads a missing dir as `0`
    /// (no bytes ⇒ no progress — the conservative reading the stall clock
    /// wants).
    #[test]
    fn dir_size_bytes_sums_nested_files_and_zero_for_missing() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("blobs/sha256")).unwrap();
        std::fs::write(tmp.path().join("index.json"), vec![0u8; 10]).unwrap();
        std::fs::write(tmp.path().join("blobs/sha256/aa"), vec![0u8; 90]).unwrap();
        assert_eq!(dir_size_bytes(tmp.path()), 100, "nested files must sum");
        assert_eq!(
            dir_size_bytes(&tmp.path().join("does-not-exist")),
            0,
            "missing dir reads as zero bytes"
        );
    }

    /// The dir measured for PROGRESS and the argv token matched for LIVENESS
    /// must be the same path — if they drift apart, the stall clock measures a
    /// dir no pull writes to and every slow pull reads as wedged again.
    #[test]
    fn pull_stage_dir_is_the_needle_path() {
        let data_dir = Path::new("/opt/tabbify/data");
        let needle = pull_path_needle(data_dir, "abc-123").unwrap();
        assert_eq!(
            pull_stage_dir(data_dir, "abc-123").to_str().unwrap(),
            needle,
            "progress dir and liveness needle must be the same path"
        );
    }

    // ── F2.2 record-less FC orphan sweep (audit #93) — SAFETY-CRITICAL gate ────

    #[test]
    fn parse_fc_sock_recognises_bare_firecracker_argv() {
        let cmdline = b"firecracker\0--api-sock\0/tmp/firecracker-fc-deadbeef0000.sock\0";
        assert_eq!(
            parse_tabbify_fc_api_sock(cmdline).as_deref(),
            Some("/tmp/firecracker-fc-deadbeef0000.sock")
        );
    }

    #[test]
    fn parse_fc_sock_recognises_systemd_run_scoped_argv() {
        // F1 scoped spawn: systemd-run --scope … -- firecracker --api-sock <sock>
        let cmdline = b"systemd-run\0--scope\0--collect\0--unit=tabbify-fc-x.scope\0--slice=tabbify-fc.slice\0-p\0CPUQuota=100%\0--\0firecracker\0--api-sock\0/tmp/firecracker-fc-abc123abc123.sock\0";
        assert_eq!(
            parse_tabbify_fc_api_sock(cmdline).as_deref(),
            Some("/tmp/firecracker-fc-abc123abc123.sock")
        );
    }

    #[test]
    fn parse_fc_sock_rejects_non_firecracker_and_foreign_sockets() {
        // Not firecracker at all (the oras pull) ⇒ None (never a kill candidate).
        let oras = b"oras\0copy\0--to-oci-layout\0/opt/tabbify/data/apps/u/fc/.pull/oci\0";
        assert!(parse_tabbify_fc_api_sock(oras).is_none());
        // firecracker but a NON-tabbify socket path ⇒ None (don't touch a
        // firecracker we didn't spawn / can't prove is ours).
        let foreign = b"firecracker\0--api-sock\0/run/other/fc.sock\0";
        assert!(parse_tabbify_fc_api_sock(foreign).is_none());
        // firecracker with no --api-sock ⇒ None.
        let nosock = b"firecracker\0--config-file\0/etc/fc.json\0";
        assert!(parse_tabbify_fc_api_sock(nosock).is_none());
        // Empty cmdline ⇒ None.
        assert!(parse_tabbify_fc_api_sock(b"").is_none());
    }

    fn rec(uuid: &str, image_ref: Option<&str>) -> RunnerHandle {
        RunnerHandle {
            uuid: uuid.to_owned(),
            pid: 1234,
            control_sock: PathBuf::from("/tmp/x.sock"),
            app_ula: "fd5a::1".to_owned(),
            parent: None,
            spawned_at: 0,
            restart: RestartState::default(),
            image_ref: image_ref.map(str::to_owned),
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            stopped: false,
        }
    }

    #[test]
    fn live_socket_set_covers_both_cold_and_deploy_keys() {
        let records = vec![rec("uuid-a", Some("ref-1")), rec("uuid-b", None)];
        let set = live_fc_socket_set(&records, false);
        // uuid-a covered by BOTH its cold-start key AND its deploy key.
        assert!(set.contains(&crate::firecracker::fc_api_sock_for_key("uuid-a")));
        assert!(set.contains(&crate::firecracker::fc_api_sock_for_key("uuid-a:ref-1")));
        // uuid-b (no deploy ref) covered by its cold-start key.
        assert!(set.contains(&crate::firecracker::fc_api_sock_for_key("uuid-b")));
        // Build socket absent when no build in flight.
        assert!(!set.contains(&build_fc_api_sock()));
    }

    #[test]
    fn live_socket_set_includes_build_socket_when_build_running() {
        let set = live_fc_socket_set(&[], true);
        assert!(set.contains(&build_fc_api_sock()));
    }

    #[test]
    fn reap_only_when_parent_pid1_and_socket_unknown() {
        let live: std::collections::HashSet<String> =
            [crate::firecracker::fc_api_sock_for_key("uuid-a")]
                .into_iter()
                .collect();
        let live_sock = crate::firecracker::fc_api_sock_for_key("uuid-a");
        let orphan_sock = crate::firecracker::fc_api_sock_for_key("uuid-GONE");

        // The provable orphan: reparented (ppid==1) AND no live record ⇒ REAP.
        assert!(fc_orphan_should_reap(&orphan_sock, true, &live));

        // A LIVE serving FC (socket in the safe-set) is NEVER killed, even if its
        // parent momentarily reads as pid 1.
        assert!(!fc_orphan_should_reap(&live_sock, true, &live));

        // An FC with a LIVING parent (not reparented) is NEVER killed, even if we
        // couldn't reconstruct its socket (the mid-swap old-VM case).
        assert!(!fc_orphan_should_reap(&orphan_sock, false, &live));

        // Both failing ⇒ never killed.
        assert!(!fc_orphan_should_reap(&live_sock, false, &live));
    }

    // ── F1 (audit #93) — scoped-FC reap completeness ──────────────────────────
    //
    // Post-F1 the FC runs inside a `systemd-run --scope` named on its `vm_key`,
    // and the pidfile records the WRAPPER pid (not firecracker). `kill_fc_child_
    // for_uuid` therefore MUST also `systemctl stop` the reconstructed scope(s),
    // or a same-version respawn leaks the old scoped FC (the F2 socket-identity
    // sweep can't catch it — old+new share the socket). These prove the pure name
    // reconstruction yields BOTH candidate scopes (deploy key + cold-start key).

    #[test]
    fn scope_names_cover_deploy_and_cold_start_keys() {
        let uuid = "0191e7c2-1111-7222-8333-444455556666";
        let reff = "[fd5a:1f02::1]:5000/acme/app:sha256abc";
        let names = fc_scope_names_for_uuid(uuid, Some(reff));

        // The DEPLOY-key scope (`uuid:reff`) is the production serving identity —
        // it MUST be present (this is the leak the review flagged), and FIRST.
        let deploy_scope =
            crate::firecracker::cpu_scope::scope_name(&format!("{uuid}:{reff}"));
        assert_eq!(
            names.first().map(String::as_str),
            Some(deploy_scope.as_str()),
            "deploy-key scope (uuid:reff) must be stopped first — it owns the live serving FC"
        );

        // The cold-start key (`uuid`) is also present (defensive: a cold boot /
        // stale-reff record uses it).
        let cold_scope = crate::firecracker::cpu_scope::scope_name(uuid);
        assert!(
            names.contains(&cold_scope),
            "cold-start-key scope (uuid) must also be stopped"
        );
        assert_eq!(names.len(), 2, "exactly the two candidate scopes, no more");
    }

    #[test]
    fn scope_names_fall_back_to_cold_start_when_no_image_ref() {
        let uuid = "0191e7c2-2222-7222-8333-444455556666";
        let names = fc_scope_names_for_uuid(uuid, None);
        // With no deployed ref we can only reconstruct the cold-start scope.
        assert_eq!(
            names,
            vec![crate::firecracker::cpu_scope::scope_name(uuid)],
            "no image_ref ⇒ stop only the cold-start-key scope"
        );
    }

    #[test]
    fn scope_names_match_the_spawn_path_scope_id() {
        // The reap MUST reconstruct the SAME scope name the spawn path
        // (`firecracker::cold_boot` → `spawn_firecracker(scope_id = vm_key)`)
        // registered, or `systemctl stop` would target a non-existent unit and
        // the scoped FC would leak. The spawn keys the serving scope on the
        // `uuid:reff` vm_key; assert the reaper's first candidate equals exactly
        // `cpu_scope::scope_name("{uuid}:{reff}")` — the spawn-side derivation.
        let uuid = "abc-123";
        let reff = "registry/x:sha-NEW";
        let vm_key = format!("{uuid}:{reff}");
        let names = fc_scope_names_for_uuid(uuid, Some(reff));
        assert_eq!(
            names.first().map(String::as_str),
            Some(crate::firecracker::cpu_scope::scope_name(&vm_key).as_str()),
            "reaper scope name must equal the spawn-path scope id (uuid:reff)"
        );
    }

    // ── #17 — tap-name reconstruction for the atomic teardown ─────────────────

    /// The atomic teardown deletes BOTH candidate taps (deploy key first, then
    /// the defensive cold-start key) so a reaped FC never leaks its host tap,
    /// whichever key its boot used.
    #[test]
    fn tap_names_cover_deploy_and_cold_start_keys() {
        let uuid = "0191e7c2-1111-7222-8333-444455556666";
        let reff = "[fd5a:1f02::1]:5000/acme/app:sha256abc";
        let names = fc_tap_names_for_uuid(uuid, Some(reff));

        let deploy_tap =
            crate::firecracker::fc_tap_name_for_key(&format!("{uuid}:{reff}"));
        assert_eq!(
            names.first().map(String::as_str),
            Some(deploy_tap.as_str()),
            "deploy-key tap (uuid:reff) must be deleted first — it owns the live serving FC"
        );
        let cold_tap = crate::firecracker::fc_tap_name_for_key(uuid);
        assert!(
            names.contains(&cold_tap),
            "cold-start-key tap (uuid) must also be deleted"
        );
        assert_eq!(names.len(), 2, "exactly the two candidate taps, no more");
    }

    /// No deployed ref ⇒ only the cold-start tap is reconstructible.
    #[test]
    fn tap_names_fall_back_to_cold_start_when_no_image_ref() {
        let uuid = "0191e7c2-2222-7222-8333-444455556666";
        let names = fc_tap_names_for_uuid(uuid, None);
        assert_eq!(
            names,
            vec![crate::firecracker::fc_tap_name_for_key(uuid)],
            "no image_ref ⇒ delete only the cold-start-key tap"
        );
    }

    /// The reaped tap name MUST equal the tap the spawn path keyed (the FC boots
    /// its tap on the same `uuid:reff` vm_key — see `firecracker::fc_tap_name_for_key`),
    /// or `ip link del` targets a phantom and the real tap leaks.
    #[test]
    fn tap_names_match_the_spawn_path_tap_key() {
        let uuid = "abc-123";
        let reff = "registry/x:sha-NEW";
        let vm_key = format!("{uuid}:{reff}");
        let names = fc_tap_names_for_uuid(uuid, Some(reff));
        assert_eq!(
            names.first().map(String::as_str),
            Some(crate::firecracker::fc_tap_name_for_key(&vm_key).as_str()),
            "reaper tap name must equal the spawn-path tap key (uuid:reff)"
        );
    }

    // ── pid dead ─────────────────────────────────────────────────────────────

    /// A dead pid → RespawnDead regardless of spawned_at.
    #[test]
    fn dead_pid_always_respawn_dead() {
        assert_eq!(
            decide_pid_grace(999, 0, now_secs(), |_| false),
            PidDecision::RespawnDead
        );
    }

    #[test]
    fn dead_pid_with_recent_spawn_still_respawn_dead() {
        let n = now_secs();
        assert_eq!(
            decide_pid_grace(999, n, n, |_| false),
            PidDecision::RespawnDead
        );
    }

    // ── within grace window ───────────────────────────────────────────────────

    /// Alive pid spawned NOW (age = 0) → AdoptInGrace (socket not checked).
    #[test]
    fn alive_within_grace_adopts_in_grace() {
        let n = now_secs();
        assert_eq!(
            decide_pid_grace(999, n, n, |_| true),
            PidDecision::AdoptInGrace,
            "runner spawned right now must be AdoptInGrace"
        );
    }

    /// Alive pid spawned SPAWN_GRACE - 1 seconds ago → still within grace.
    #[test]
    fn alive_just_within_grace_adopts_in_grace() {
        let grace = SPAWN_GRACE.as_secs();
        let n = 1_700_000_000u64;
        let spawned_at = n - (grace - 1);
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::AdoptInGrace
        );
    }

    // ── past grace window ─────────────────────────────────────────────────────

    /// Alive pid at EXACTLY the grace boundary (age == SPAWN_GRACE) → past grace
    /// → CheckSocket.
    #[test]
    fn alive_at_grace_boundary_checks_socket() {
        let grace = SPAWN_GRACE.as_secs();
        let n = 1_700_000_000u64;
        let spawned_at = n - grace; // age == SPAWN_GRACE → NOT within grace
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::CheckSocket
        );
    }

    /// Alive pid well past grace → CheckSocket.
    #[test]
    fn alive_past_grace_checks_socket() {
        let n = 1_700_000_000u64;
        let spawned_at = n - SPAWN_GRACE.as_secs() - 60;
        assert_eq!(
            decide_pid_grace(999, spawned_at, n, |_| true),
            PidDecision::CheckSocket
        );
    }

    /// `spawned_at = 0` (old record with no timestamp) → age = now_secs
    /// (huge number) → always past grace → CheckSocket.
    #[test]
    fn spawned_at_zero_is_past_grace() {
        let n = 1_700_000_000u64;
        assert_eq!(
            decide_pid_grace(999, 0, n, |_| true),
            PidDecision::CheckSocket,
            "spawned_at=0 must be treated as past the grace window"
        );
    }

    // ── probe call discipline ─────────────────────────────────────────────────

    /// The pid probe is called exactly once, and its result drives the decision.
    #[test]
    fn pid_probe_called_once() {
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let n = now_secs();
        let _ = decide_pid_grace(999, n, n, move |_| {
            cc.fetch_add(1, Ordering::SeqCst);
            true
        });
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "pid probe must be called exactly once"
        );
    }

    /// Dead pid: `decide_pid_grace` returns `RespawnDead` and the caller is not
    /// expected to make a socket call — verify the decision itself conveys this
    /// (no socket closure needed in the pure function).
    #[test]
    fn dead_pid_returns_respawn_dead_no_socket_needed() {
        // The pure function has no socket closure — the caller only calls the
        // socket when PidDecision::CheckSocket is returned. Asserting the
        // decision is sufficient.
        let result = decide_pid_grace(999, 0, now_secs(), |_| false);
        assert_eq!(result, PidDecision::RespawnDead);
        // Callers must not invoke the socket health check for RespawnDead.
        let probed = Arc::new(AtomicBool::new(false));
        // (Simulating the caller's logic: only check socket on CheckSocket)
        if result == PidDecision::CheckSocket {
            probed.store(true, Ordering::SeqCst);
        }
        assert!(
            !probed.load(Ordering::SeqCst),
            "socket must not be checked when pid is dead"
        );
    }

    /// Within grace: caller must NOT invoke socket check.
    #[test]
    fn within_grace_returns_adopt_in_grace_no_socket_needed() {
        let n = now_secs();
        let result = decide_pid_grace(999, n, n, |_| true);
        assert_eq!(result, PidDecision::AdoptInGrace);
        let probed = Arc::new(AtomicBool::new(false));
        if result == PidDecision::CheckSocket {
            probed.store(true, Ordering::SeqCst);
        }
        assert!(
            !probed.load(Ordering::SeqCst),
            "socket must not be checked within grace window"
        );
    }

    // ── backoff_action ────────────────────────────────────────────────────────

    /// Default (no failures, next_retry_at=0) → backoff window is already past
    /// → RespawnNow.
    #[test]
    fn backoff_action_default_state_is_respawn_now() {
        let state = RestartState::default();
        assert_eq!(
            backoff_action(state, 1_700_000_000),
            BackoffAction::RespawnNow,
            "a never-failed runner must always be RespawnNow"
        );
    }

    /// next_retry_at is in the FUTURE → Wait (reconcile_record must yield Backoff).
    #[test]
    fn backoff_action_future_retry_is_wait() {
        let state = RestartState {
            consecutive_failures: 2,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_000_030, // 30 s in the future
            last_healthy_at: 0,
        };
        let now = 1_700_000_010u64; // 10 s after exit, before retry window
        assert_eq!(
            backoff_action(state, now),
            BackoffAction::Wait,
            "next_retry_at in the future must be Wait"
        );
    }

    /// next_retry_at is in the PAST (window elapsed) → RespawnNow.
    #[test]
    fn backoff_action_past_retry_is_respawn_now() {
        let state = RestartState {
            consecutive_failures: 2,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_000_030,
            last_healthy_at: 0,
        };
        let now = 1_700_000_031u64; // 1 s past the retry window
        assert_eq!(
            backoff_action(state, now),
            BackoffAction::RespawnNow,
            "next_retry_at in the past must be RespawnNow"
        );
    }

    /// next_retry_at exactly equals now → RespawnNow (boundary is inclusive).
    #[test]
    fn backoff_action_at_retry_boundary_is_respawn_now() {
        let state = RestartState {
            consecutive_failures: 1,
            last_exit_at: 1_700_000_000,
            next_retry_at: 1_700_000_010,
            last_healthy_at: 0,
        };
        let now = 1_700_000_010u64;
        assert_eq!(
            backoff_action(state, now),
            BackoffAction::RespawnNow,
            "at the exact retry boundary, RespawnNow must fire"
        );
    }

    // ── deploy-in-flight defers the reap ─────────────────────────────────────

    fn shared_for_test() -> SharedRunnerConfig {
        SharedRunnerConfig {
            // A binary that does not exist → any respawn attempt fails, so an
            // outcome that is NOT `Adopted` would surface as `RespawnFailed`.
            runner_bin: PathBuf::from("/nonexistent/tabbify-runner"),
            s3_base_url: "http://s3.invalid".to_owned(),
            data_dir: PathBuf::from("/var/lib/tabbify/data"),
            parent: None,
            no_mesh: true,
            relay_url: None,
            relay_only: false,
        }
    }

    /// Spawn a real, long-lived child process (`sleep`) and return its pid. The
    /// pid is alive (so `runner_is_alive` reports true) and — crucially — it is
    /// safe for the reap path to SIGKILL it (it is NOT the test process). The
    /// child is harvested at the end of each test via the returned handle.
    fn spawn_sleep_child() -> std::process::Child {
        std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep child")
    }

    fn unhealthy_record(uuid: &str, pid: u32, dir: &std::path::Path) -> RunnerHandle {
        RunnerHandle {
            uuid: uuid.to_owned(),
            pid,
            // Non-existent socket → health probe fails (unhealthy).
            control_sock: dir.join("no-such.sock"),
            app_ula: "fd5a:1f02:44a5:240b:121a::1".to_owned(),
            parent: None,
            // spawned_at = 0 → treated as well past the grace window.
            spawned_at: 0,
            restart: RestartState::default(),
            image_ref: None,
            requested_runtime: None,
            network: None,
            runner_join_token: None,
            manifest_toml: None,
            extra_env: None,
            egress_allow: None,
            crash_looped: false,
            stopped: false,
        }
    }

    /// A runner whose pid is ALIVE (a real `sleep` child) but whose control
    /// socket is unhealthy, past the grace window, is normally killed +
    /// respawned. With a deploy in flight for its uuid, the monitor must instead
    /// DEFER the reap and report `Adopted` — the runner is left running so the
    /// in-flight deploy can finish (its pid is NOT killed).
    #[tokio::test]
    async fn reconcile_defers_reap_while_deploying() {
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let uuid = "0191e7c2-1111-7222-8333-444455556666";

        let mut child = spawn_sleep_child();
        let record = unhealthy_record(uuid, child.id(), dir.path());

        // Hold the deploy guard for the uuid across the reconcile.
        let _guard = orch.begin_deploy(uuid);
        let outcome = orch.reconcile_record(&record).await;
        assert_eq!(
            outcome,
            RecordOutcome::Adopted,
            "a runner with a deploy in flight must be adopted (reap deferred), not reaped"
        );
        // The child must still be alive — the defer must NOT have killed it.
        assert!(
            runner_is_alive(child.id()),
            "deferred reap must leave the runner pid alive"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    /// Without a deploy in flight, the SAME alive-pid/unhealthy-socket record is
    /// reaped: the kill-before-respawn path SIGKILLs the pid and the respawn is
    /// attempted (failing on the missing binary → `RespawnFailed`). This proves
    /// the defer above is caused by the guard, not by some other adopt path.
    #[tokio::test]
    async fn reconcile_reaps_when_not_deploying() {
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let uuid = "0191e7c2-1111-7222-8333-444455556666";

        let mut child = spawn_sleep_child();
        let child_pid = child.id();
        let record = unhealthy_record(uuid, child_pid, dir.path());

        // No deploy guard → the runner is reaped: SIGKILL then respawn (which
        // fails on the nonexistent binary → RespawnFailed).
        let outcome = orch.reconcile_record(&record).await;
        assert_eq!(
            outcome,
            RecordOutcome::RespawnFailed,
            "without a deploy in flight, an unhealthy runner is reaped (respawn attempted)"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    // ── FC child reaping via pidfile (Fix 2) ──────────────────────────────────

    /// When the monitor kills a dead runner, it reads the per-uuid FC pidfile
    /// and kills the orphaned firecracker child. Test: write a real pidfile for
    /// a live `sleep` child (simulating the FC orphan), let reconcile_record run
    /// on a DEAD runner pid, then assert the pidfile is consumed AND the sleep
    /// child is dead.
    #[tokio::test]
    async fn reconcile_kills_fc_child_via_pidfile_when_runner_dead() {
        use crate::firecracker::pidfile;

        let dir = TempDir::new().unwrap();
        let uuid = "0191e7c2-dead-7222-8333-444455556666";

        // Spin up a "FC orphan" — a real `sleep` child we can safely SIGKILL.
        let mut fc_orphan = spawn_sleep_child();
        let fc_pid = fc_orphan.id();

        // Write a pidfile as the runner would after spawning firecracker.
        pidfile::write(dir.path(), uuid, fc_pid);
        assert!(
            pidfile::path(dir.path(), uuid).exists(),
            "pidfile must be written before reconcile"
        );

        // Runner pid = 99_999_999 (dead / non-existent — the test convention;
        // NEVER 0, which kill(2) treats as the caller's own process group).
        // Shared config points data_dir at our tempdir so
        // kill_fc_child_for_uuid finds the pidfile there.
        let mut cfg = shared_for_test();
        cfg.data_dir = dir.path().to_path_buf();
        let orch = Orchestrator::new(cfg, dir.path().to_path_buf());

        let dead_record = unhealthy_record(uuid, 99_999_999, dir.path());
        let outcome = orch.reconcile_record(&dead_record).await;

        // The reconcile attempts a respawn (fails on the non-existent binary).
        assert_eq!(
            outcome,
            RecordOutcome::RespawnFailed,
            "reconcile of a dead runner must attempt a respawn"
        );

        // The pidfile must have been consumed (removed from disk).
        assert!(
            !pidfile::path(dir.path(), uuid).exists(),
            "pidfile must be removed after kill_fc_child_for_uuid"
        );

        // The FC orphan must be dead now.
        // Give the kernel a moment to process the SIGKILL.
        let fc_alive_after = runner_is_alive(fc_pid);
        let _ = fc_orphan.wait();
        assert!(
            !fc_alive_after,
            "FC orphan (pid {fc_pid}) must be killed by the monitor"
        );
    }

    // ── Crash-loop circuit breaker (Fix 3) ───────────────────────────────────

    /// A record that already has `crash_looped = true` must be returned as
    /// `CrashLooped` immediately — no respawn is attempted.
    #[tokio::test]
    async fn reconcile_skips_parked_runner() {
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let uuid = "0191e7c2-park-7222-8333-444455556666";

        let mut record = unhealthy_record(uuid, 99_999_999, dir.path());
        record.crash_looped = true;

        let outcome = orch.reconcile_record(&record).await;
        assert_eq!(
            outcome,
            RecordOutcome::CrashLooped,
            "a parked runner must return CrashLooped without attempting a respawn"
        );
    }

    /// After N consecutive failed respawns the monitor must park the runner:
    /// the on-disk record must have `crash_looped = true` and the outcome must
    /// be `CrashLooped`.
    #[tokio::test]
    async fn reconcile_parks_runner_at_threshold() {
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let uuid = "0191e7c2-thr-7222-8333-444455556666";

        // Build a RestartState with exactly CRASH_LOOP_PARK_THRESHOLD - 1
        // consecutive failures. The next call to on_exit will push it to the
        // threshold, which must trigger parking.
        let pre_threshold = RestartState {
            consecutive_failures: CRASH_LOOP_PARK_THRESHOLD - 1,
            last_exit_at: 0,
            next_retry_at: 0, // already elapsed → RespawnNow
            last_healthy_at: 0,
        };
        let mut record = unhealthy_record(uuid, 99_999_999, dir.path());
        record.restart = pre_threshold;
        record.save(dir.path()).unwrap();

        let outcome = orch.reconcile_record(&record).await;
        assert_eq!(
            outcome,
            RecordOutcome::CrashLooped,
            "hitting the park threshold must return CrashLooped"
        );

        // The on-disk record must be parked.
        let updated = crate::orchestrator::handle::RunnerHandle::load(dir.path(), uuid)
            .unwrap()
            .expect("record must still exist after parking");
        assert!(
            updated.crash_looped,
            "crash_looped must be true on the persisted record after parking"
        );
        assert_eq!(
            updated.restart.consecutive_failures, CRASH_LOOP_PARK_THRESHOLD,
            "failure count must be persisted at the threshold"
        );
    }

    /// A healthy observation RESETS the consecutive-failure counter, so the
    /// circuit breaker does NOT trip: a runner that heals before the threshold
    /// must not be parked.
    ///
    /// This is a pure logic test on `on_exit` / `on_healthy` — no async I/O.
    #[test]
    fn crash_loop_counter_resets_on_healthy() {
        use crate::orchestrator::restart::{BackoffParams, on_exit, on_healthy};

        let p = BackoffParams::default();
        let mut state = RestartState::default();
        let mut t = 1_000u64;

        // Simulate N-1 failures.
        for _ in 0..(CRASH_LOOP_PARK_THRESHOLD - 1) {
            state = on_exit(state, p, t);
            t += 10;
        }
        assert_eq!(
            state.consecutive_failures,
            CRASH_LOOP_PARK_THRESHOLD - 1,
            "should have N-1 failures before heal"
        );

        // Heal: advance time past stable_secs (60 s) so the counter resets.
        t += p.stable_secs + 1;
        state = on_healthy(state, p, t);
        assert_eq!(
            state.consecutive_failures, 0,
            "on_healthy after stable_secs must reset the failure counter"
        );

        // After the reset the next failure is only #1 — far from the threshold.
        state = on_exit(state, p, t);
        assert_eq!(state.consecutive_failures, 1);
        assert!(
            state.consecutive_failures < CRASH_LOOP_PARK_THRESHOLD,
            "a single failure after a heal must not reach the park threshold"
        );
    }

    /// A deploy on a parked runner (cold path: deploy_app writes a fresh
    /// record with crash_looped = false) must clear the park flag.
    ///
    /// This is tested indirectly: after parking, simulate a cold deploy by
    /// writing a fresh record with crash_looped = false, then assert reconcile
    /// returns something other than CrashLooped.
    #[tokio::test]
    async fn deploy_clears_crash_looped_flag() {
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let uuid = "0191e7c2-clr-7222-8333-444455556666";

        // Park the runner (write a crash_looped record).
        let mut parked = unhealthy_record(uuid, 99_999_999, dir.path());
        parked.crash_looped = true;
        parked.save(dir.path()).unwrap();

        // Confirm it is parked.
        let outcome_before = orch.reconcile_record(&parked).await;
        assert_eq!(outcome_before, RecordOutcome::CrashLooped);

        // Simulate a cold deploy: fresh record with crash_looped = false.
        let fresh = unhealthy_record(uuid, 99_999_999, dir.path()); // crash_looped defaults false
        fresh.save(dir.path()).unwrap();

        // Now reconcile must NOT return CrashLooped (it may fail the spawn, but
        // the parking gate must be cleared).
        let outcome_after = orch.reconcile_record(&fresh).await;
        assert_ne!(
            outcome_after,
            RecordOutcome::CrashLooped,
            "a fresh record (crash_looped=false) must NOT be parked"
        );
    }

    // ── pid 0 guard (regression) ─────────────────────────────────────────────

    /// pid 0 has process-GROUP semantics for kill(2)/waitpid(2) — `kill(0, …)`
    /// signals the CALLER'S OWN process group. A corrupted record/pidfile with
    /// pid 0 must (a) read as DEAD and (b) never be killed: without the guard,
    /// `runner_is_alive(0)` reported "alive" (kill(0,0) succeeds against our
    /// own group), reconcile took the hung-socket path, and `kill_pid(0)`
    /// SIGKILLed the test binary + cargo + shell.
    #[tokio::test]
    async fn pid_zero_is_dead_and_never_killed() {
        // (a) pid 0 must be reported dead.
        assert!(!runner_is_alive(0), "pid 0 must be reported dead");

        // (b) kill_pid(0) must be a no-op. Surviving this call IS the
        // assertion — without the guard it SIGKILLs our own process group.
        kill_pid(0);

        // (c) a reconcile on a pid-0 record must take the DEAD-pid path
        // (respawn attempted, no kill): RespawnFailed on the missing binary,
        // and the test process is still alive to observe it.
        let dir = TempDir::new().unwrap();
        let orch = Orchestrator::new(shared_for_test(), dir.path().to_path_buf());
        let record = unhealthy_record("0191e7c2-aaaa-7222-8333-444455556666", 0, dir.path());
        let outcome = orch.reconcile_record(&record).await;
        assert_eq!(
            outcome,
            RecordOutcome::RespawnFailed,
            "a pid-0 record must take the dead-pid respawn path"
        );
    }
}
