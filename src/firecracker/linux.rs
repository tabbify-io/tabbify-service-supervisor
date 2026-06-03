//! Linux microVM runtime: owns a firecracker child process + tap, configures
//! it via its unix-socket REST API, and proxies HTTP into the guest.

#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
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
    proxy_request, read_http_status, resume_body, rootfs_drive_body, snapshot_create_body,
    snapshot_load_body,
};
use super::snapshot;
use super::{FcConfig, kvm_available};
use crate::manifest::Runtime;
use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, ExitReason, RuntimeHealth};

/// How long to wait for the guest app's HTTP server to come up after boot.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for the guest app.
const READY_POLL: Duration = Duration::from_millis(250);
/// How long to wait for one firecracker API call over the unix socket.
const API_TIMEOUT: Duration = Duration::from_secs(5);

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
fn console_stdio(console_log: Option<&Path>) -> (Stdio, Stdio) {
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

/// Monotonic per-process counter → a unique tap name + /30 offset per VM, so
/// concurrently-hosted firecracker apps don't collide on tap devices/links.
static VM_SEQ: AtomicU32 = AtomicU32::new(0);

/// Probe type: given a `host:port` string returns `true` iff the guest app is
/// reachable. Production `health()` does a real HTTP GET to `guest_base`; this
/// injectable seam lets unit tests fake the result so no real microVM is
/// needed. Mirrors `DockerRuntime`'s `TcpProbe`.
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
    /// `http://<guest_ip>:<app_port>` — the base the proxy targets.
    guest_base: String,
    client: reqwest::Client,
    /// Test-only injectable reachability probe. Production leaves this `None`
    /// and `health()` does a real HTTP GET to `guest_base`; tests substitute a
    /// closure via [`Self::with_probe_for_test`] so no real microVM is needed.
    #[cfg(test)]
    probe: Option<TcpProbe>,
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
        Self::cold_boot(rootfs, rt, cfg, None, None).await
    }

    /// Cold-boot a microVM. `cache_dir` — when `Some`, a snapshot is taken
    /// after first boot (best-effort) and stored there for future warm starts.
    /// `console_log` — when `Some` and `SUPERVISOR_FC_DEBUG` is truthy, the
    /// firecracker child's stdout+stderr are appended there; otherwise both go
    /// to `/dev/null` (the default, unchanged behavior).
    async fn cold_boot(
        rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        cache_dir: Option<&Path>,
        console_log: Option<&Path>,
    ) -> Result<Self> {
        if !kvm_available() {
            bail!("firecracker runtime requires Linux + /dev/kvm (/dev/kvm not R/W-openable)");
        }
        if !rootfs.is_file() {
            bail!("firecracker rootfs not found at {}", rootfs.display());
        }

        let seq = VM_SEQ.fetch_add(1, Ordering::SeqCst);
        let (host_ip, guest_ip) = derive_link_ips(&cfg.tap_subnet, seq)
            .with_context(|| format!("derive /30 from subnet {}", cfg.tap_subnet))?;
        let tap_name = format!("fc-tap{seq}");
        let guest_mac = derive_guest_mac(seq);
        let guest_base = format!("http://{guest_ip}:{}", cfg.app_port);
        tracing::debug!(
            seq,
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
        // in-VM supervisor can reach the public mesh coordinator/relay.
        setup_guest_nat(&tap_name, &cfg.tap_subnet).await;

        // Spawn firecracker with its API socket. Clean any stale socket.
        let api_sock = PathBuf::from(format!("/tmp/firecracker-{tap_name}.sock"));
        let _ = std::fs::remove_file(&api_sock);
        let (stdout, stderr) = console_stdio(console_log);
        let child = Command::new(&cfg.bin)
            .arg("--api-sock")
            .arg(&api_sock)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| format!("spawn firecracker binary {:?}", cfg.bin))?;

        let me = Self {
            child: Some(child),
            tap_name,
            api_sock: api_sock.clone(),
            guest_base,
            client: reqwest::Client::new(),
            #[cfg(test)]
            probe: None,
        };

        // Configure + boot the VM, then wait for the guest app. On any
        // failure `me` drops → child killed + tap deleted.
        me.configure_and_boot(rootfs, rt, cfg, &guest_ip, &host_ip, &guest_mac)
            .await?;
        me.wait_until_ready().await?;

        // Best-effort snapshot after first boot. A failure is logged and
        // does NOT fail the launch — the VM continues to serve cold.
        if let Some(dir) = cache_dir {
            if !snapshot::files_present(dir) {
                me.try_create_snapshot(dir).await;
            }
        }

        Ok(me)
    }

    /// [`Self::launch`] with per-uuid pidfile reconciliation and snapshot
    /// warm-start.
    ///
    /// Decision flow:
    /// 1. Kill any stale VM recorded in `<data_dir>/tabbify-fc-<uuid>.pid`.
    /// 2. If `<data_dir>/apps/<uuid>/cache/snap.vmstate` + `snap.mem` both
    ///    exist → attempt a warm start via [`Self::launch_from_snapshot`].
    ///    If the load fails (corrupt snapshot, kernel mismatch, etc.) → fall
    ///    back to cold boot automatically.
    /// 3. On the first (cold) boot, a snapshot is created in the cache dir
    ///    after the guest app is ready. Subsequent restarts will be warm.
    ///
    /// # Notes
    /// Snapshots are host-kernel + CPU-template specific. This supervisor
    /// creates and consumes them on the same host, so they are always
    /// compatible. Cross-host snapshot reuse is NOT supported.
    ///
    /// # Errors
    /// See [`Self::launch`].
    pub async fn launch_with_uuid(
        rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        uuid: &str,
        data_dir: &Path,
    ) -> Result<Self> {
        // Reconcile: kill any stale VM for this uuid before spawning fresh.
        if let Some(stale_pid) = pidfile::take(data_dir, uuid) {
            pidfile::kill_stale_if_alive(stale_pid, pidfile::process_is_alive);
        }

        // Per-app snapshot cache: <data_dir>/apps/<uuid>/cache/
        // (mirrors the wasm .cwasm cache directory layout)
        let cache_dir = data_dir.join("apps").join(uuid).join("cache");
        // Per-app console log: <data_dir>/fc/<uuid>.console.log. Only written
        // when SUPERVISOR_FC_DEBUG is truthy (see `console_stdio`).
        let console_log = pidfile::console_log_path(data_dir, uuid);

        let vm = if snapshot::files_present(&cache_dir) {
            // Warm path: try to restore from a previously-taken snapshot.
            // Fall back to cold boot on any failure (corrupt files, kernel
            // mismatch, etc.) so the app always comes up eventually.
            match Self::launch_from_snapshot(&cache_dir, cfg, Some(&console_log)).await {
                Ok(warm_vm) => {
                    tracing::info!(uuid, "warm start from snapshot");
                    warm_vm
                }
                Err(e) => {
                    tracing::warn!(
                        uuid,
                        error = %e,
                        "snapshot load failed; falling back to cold boot"
                    );
                    Self::cold_boot(rootfs, rt, cfg, Some(&cache_dir), Some(&console_log)).await?
                }
            }
        } else {
            // Cold path: first boot — take a snapshot on the way out.
            Self::cold_boot(rootfs, rt, cfg, Some(&cache_dir), Some(&console_log)).await?
        };

        // Record the new child PID so a future restart can clean it up.
        if let Some(pid) = vm.child.as_ref().and_then(|c| c.id()) {
            pidfile::write(data_dir, uuid, pid);
        }
        Ok(vm)
    }

    /// Restore a previously-snapshotted VM from `cache_dir`.
    ///
    /// Flow: `setup_tap` (new tap/IP, derived by VM_SEQ) → spawn
    /// `firecracker --api-sock` → `PUT /snapshot/load` (resume_vm=true) →
    /// `wait_until_ready` (should be ~ms not seconds).
    ///
    /// The machine-config / boot-source / drives / network-interfaces sequence
    /// from `configure_and_boot` is intentionally SKIPPED here — the snapshot
    /// embeds all that state. The only networking re-wiring needed is the host
    /// tap; the guest MAC and IP are baked into the snapshot.
    ///
    /// # Errors
    /// Tap setup failure, firecracker spawn failure, snapshot load API failure,
    /// or the guest app failing to answer within [`READY_TIMEOUT`].
    async fn launch_from_snapshot(
        cache_dir: &Path,
        cfg: &FcConfig,
        console_log: Option<&Path>,
    ) -> Result<Self> {
        if !kvm_available() {
            bail!("firecracker runtime requires Linux + /dev/kvm (/dev/kvm not R/W-openable)");
        }

        let vmstate = snapshot::vmstate_path(cache_dir);
        let mem = snapshot::mem_path(cache_dir);

        let vmstate_str = vmstate
            .to_str()
            .ok_or_else(|| anyhow!("snapshot vmstate path is not valid UTF-8"))?;
        let mem_str = mem
            .to_str()
            .ok_or_else(|| anyhow!("snapshot mem path is not valid UTF-8"))?;

        // Allocate a fresh tap + /30 for this restored VM.
        let seq = VM_SEQ.fetch_add(1, Ordering::SeqCst);
        let (host_ip, guest_ip) = derive_link_ips(&cfg.tap_subnet, seq)
            .with_context(|| format!("derive /30 from subnet {}", cfg.tap_subnet))?;
        let tap_name = format!("fc-tap{seq}");
        let guest_base = format!("http://{guest_ip}:{}", cfg.app_port);
        tracing::debug!(
            seq,
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
        let child = Command::new(&cfg.bin)
            .arg("--api-sock")
            .arg(&api_sock)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| format!("spawn firecracker binary {:?}", cfg.bin))?;

        let me = Self {
            child: Some(child),
            tap_name,
            api_sock: api_sock.clone(),
            guest_base,
            client: reqwest::Client::new(),
            #[cfg(test)]
            probe: None,
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
            &machine_config_body(cfg.vcpus, rt.memory_mb),
        )
        .await
        .context("PUT /machine-config")?;
        self.api_put(
            "/boot-source",
            &boot_source_body(&kernel, &guest_ip.to_string(), &host_ip.to_string()),
        )
        .await
        .context("PUT /boot-source")?;
        self.api_put("/drives/rootfs", &rootfs_drive_body(rootfs_str))
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
        let payload = serde_json::to_vec(body)?;
        let status =
            tokio::time::timeout(API_TIMEOUT, unix_http_put(&self.api_sock, path, &payload))
                .await
                .map_err(|_| anyhow!("firecracker API timed out on PUT {path}"))??;
        tracing::debug!(verb = "PUT", path, status, "fc API call");
        if !(200..300).contains(&status) {
            bail!("firecracker API PUT {path} returned HTTP {status}");
        }
        Ok(())
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
            // Attempt to resume in case we paused but failed after that.
            if let Err(re) = self.api_patch("/vm", &resume_body()).await {
                tracing::warn!(error = %re, "PATCH /vm Resumed failed during snapshot error recovery");
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
        self.api_put(
            "/snapshot/create",
            &snapshot_create_body(vmstate_str, mem_str),
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

    /// Poll the guest app's HTTP server until it answers (any status) or
    /// [`READY_TIMEOUT`] elapses.
    async fn wait_until_ready(&self) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + READY_TIMEOUT;
        let mut attempt: u32 = 0;
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
                    tokio::time::sleep(READY_POLL).await;
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
    /// production code (mirrors `DockerRuntime::with_probe_for_test`).
    #[cfg(test)]
    pub fn with_probe_for_test(guest_base: &str, probe: TcpProbe) -> Self {
        Self {
            child: None,
            tap_name: "fc-tap-test".to_owned(),
            api_sock: PathBuf::from("/tmp/firecracker-test.sock"),
            guest_base: guest_base.to_owned(),
            client: reqwest::Client::new(),
            probe: Some(probe),
        }
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
        // `host:port` (with the `http://` scheme stripped), mirroring
        // `DockerRuntime::health`.
        #[cfg(test)]
        if let Some(probe) = self.probe.clone() {
            let hp = self
                .guest_base
                .trim_start_matches("http://")
                .to_owned();
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
        // Graceful teardown: kill the VM (if still alive) + delete the tap.
        // Idempotent — `Drop` repeats it as the safety net.
        let pid = self.child.as_ref().and_then(|c| c.id());
        let tap = self.tap_name.clone();
        Box::pin(async move {
            if let Some(pid) = pid {
                pidfile::kill_stale_if_alive(pid, pidfile::process_is_alive);
            }
            if let Err(e) = run_ip(&["link", "del", &tap]).await {
                tracing::debug!(%tap, error = %e, "shutdown: ip link del tap (may already be gone)");
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
async fn setup_tap(tap_name: &str, host_ip: Ipv4Addr) -> Result<()> {
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

/// Run an `ip ...` command, erroring on a non-zero exit.
async fn run_ip(args: &[&str]) -> Result<()> {
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
/// forwarding and SNAT the guest tap subnet out the host's default-route
/// uplink, plus explicit FORWARD ACCEPTs (a docker-managed FORWARD policy of
/// DROP would otherwise black-hole guest traffic even with masquerade present).
///
/// BEST-EFFORT and idempotent: a failure here must not abort a boot — it only
/// costs the guest its egress (the VM still runs and answers the :8080 probe),
/// so we log loudly and continue. The host tap link itself (`setup_tap`) is
/// what the readiness probe needs; this only matters for guest→internet.
async fn setup_guest_nat(tap_name: &str, tap_subnet: &str) {
    // Enable forwarding (no-op if already 1; warn but continue on EACCES).
    if let Err(e) = tokio::fs::write("/proc/sys/net/ipv4/ip_forward", b"1\n").await {
        tracing::warn!(error = %e, "fc nat: could not enable net.ipv4.ip_forward");
    }
    let Some(uplink) = default_route_dev().await else {
        tracing::warn!("fc nat: no default-route uplink found; guest egress disabled");
        return;
    };
    // Idempotent (`-C ... || -A/-I ...`). FORWARD rules are *inserted* at the
    // head so they precede any docker-installed DROP/jump.
    let rules: [(Vec<&str>, Vec<&str>); 3] = [
        (
            vec!["-t", "nat", "-C", "POSTROUTING", "-s", tap_subnet, "-o", &uplink, "-j", "MASQUERADE"],
            vec!["-t", "nat", "-A", "POSTROUTING", "-s", tap_subnet, "-o", &uplink, "-j", "MASQUERADE"],
        ),
        (
            vec!["-C", "FORWARD", "-i", tap_name, "-o", &uplink, "-j", "ACCEPT"],
            vec!["-I", "FORWARD", "1", "-i", tap_name, "-o", &uplink, "-j", "ACCEPT"],
        ),
        (
            vec!["-C", "FORWARD", "-i", &uplink, "-o", tap_name, "-m", "state", "--state", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
            vec!["-I", "FORWARD", "1", "-i", &uplink, "-o", tap_name, "-m", "state", "--state", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
        ),
    ];
    for (check, add) in &rules {
        if let Err(e) = ensure_iptables(check, add).await {
            tracing::warn!(error = %e, "fc nat: iptables rule add failed; guest egress may be blocked");
        }
    }
    tracing::info!(%tap_name, uplink = %uplink, subnet = %tap_subnet, "fc nat: guest egress enabled");
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
fn derive_link_ips(subnet: &str, seq: u32) -> Result<(Ipv4Addr, Ipv4Addr)> {
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
fn derive_guest_mac(seq: u32) -> String {
    let b = seq.to_le_bytes();
    // 02:xx → locally administered, unicast.
    format!("02:FC:{:02X}:{:02X}:{:02X}:{:02X}", b[0], b[1], b[2], b[3])
}

/// Wait (bounded) for firecracker to create its API socket after spawn.
async fn wait_for_socket(sock: &Path) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !sock.exists() {
        if tokio::time::Instant::now() >= deadline {
            bail!("firecracker API socket {} never appeared", sock.display());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
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
