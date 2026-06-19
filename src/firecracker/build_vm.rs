//! Ephemeral Firecracker BUILD VM (phase 2 of the build/run split).
//!
//! Boots a short-lived microVM from the buildkit TOOLCHAIN rootfs with three
//! block devices — rootfs (`/dev/vda`), a per-build SCRATCH disk carrying the
//! cloned source in and the built OCI image out (`/dev/vdb`), and a
//! persistent build CACHE disk (`/dev/vdc`) — then waits for the VM to exit.
//! The VM is the unit of isolation AND of lifecycle: untrusted build code
//! never touches the host docker daemon, the clone token never enters the
//! guest (the HOST clones; only source rides in), and `kill VM == cleanup`.
//!
//! Unlike the serving runtime ([`super::FirecrackerRuntime`]) there is no
//! readiness probe and no snapshotting: completion is the firecracker
//! process EXITING (the guest entrypoint powers off via sysrq when done),
//! bounded by a hard timeout after which the VM is killed.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

use super::linux::{
    api_put_sock, derive_link_ips, run_ip, setup_guest_nat, setup_tap, teardown_guest_nat,
};
use super::protocol::{
    aux_drive_body, boot_source_body, instance_start_body, machine_config_body, network_iface_body,
    rootfs_drive_body,
};
use crate::config::FcConfig;

/// Build VMs live in their OWN address/tap/MAC namespace, DISJOINT from the
/// serving runtime's `VM_SEQ`-derived `fc-tap<seq>` / `02:FC:…` / first-/30
/// space. Builds are serialized (one at a time per host) and ephemeral, so a
/// single fixed identity is enough — and critically, the build path must
/// NEVER name, address, or `ip link del` a device a serving app VM in the
/// SAME host netns could own (serving + build runners share one netns).
pub(crate) const BUILD_TAP: &str = "fc-bld0";
/// `02:FB:…` (locally-administered, distinct from serving's `02:FC:…`).
pub(crate) const BUILD_MAC: &str = "02:FB:00:00:00:01";
/// /30 carved from the TOP of the tap /16 (172.31.255.x), where the serving
/// `VM_SEQ` counter — climbing from .0.1 — realistically never reaches.
pub(crate) const BUILD_SEQ: u32 = 0xFFFF / 4 - 1;

/// Hard ceiling on one sandboxed build. A build that hasn't finished by then
/// is killed (the VM is the cleanup boundary — nothing leaks onto the host).
pub const BUILD_VM_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Build-VM sizing: builds are heavier than serving microVMs (compilers,
/// layer unpacking), so give them a fixed 2 vCPU / 2 GiB rather than the
/// app-manifest-driven sizing of the serving path.
const BUILD_VCPUS: u32 = 2;
const BUILD_MEM_MIB: u32 = 2048;

/// Everything the build VM needs on disk, prepared by the caller
/// (`runner::build::fc_sandbox`).
pub struct BuildVmSpec<'a> {
    /// Toolchain rootfs ext4 (buildkit + entrypoint), digest-cached.
    pub rootfs: &'a Path,
    /// Per-build scratch ext4: `src/` + `job.json` in, `out/oci.tar` +
    /// `result.json` out. Destroyed by the caller after read-back.
    pub scratch: &'a Path,
    /// Persistent build-cache ext4 (buildkit local cache). Survives VMs.
    pub cache: &'a Path,
    /// Console log path (always captured for builds — the guest build log's
    /// last resort when the scratch comes back unreadable).
    pub console_log: &'a Path,
    /// Firecracker runtime config (binary, kernel, tap subnet).
    pub cfg: &'a FcConfig,
}

/// Boot the build VM and wait for it to EXIT (success path: the guest
/// entrypoint syncs the scratch disk and powers off). Returns once the
/// firecracker process is gone; the caller then reads `result.json` from the
/// scratch disk. On timeout the VM is killed and an error returned.
///
/// # Errors
/// KVM/tap/spawn/API failures, or the timeout.
pub async fn run_build_vm(spec: &BuildVmSpec<'_>) -> Result<()> {
    if !super::kvm_available() {
        bail!("firecracker build sandbox requires Linux + /dev/kvm");
    }
    for (what, p) in [
        ("rootfs", spec.rootfs),
        ("scratch", spec.scratch),
        ("cache", spec.cache),
    ] {
        if !p.is_file() {
            bail!("build VM {what} image not found at {}", p.display());
        }
    }

    // Build-owned identity — never overlaps serving (see BUILD_* consts).
    let (host_ip, guest_ip) = derive_link_ips(&spec.cfg.tap_subnet, BUILD_SEQ)
        .with_context(|| format!("derive build /30 from subnet {}", spec.cfg.tap_subnet))?;
    let tap_name = BUILD_TAP;
    let guest_mac = BUILD_MAC;

    // Pre-clean ONLY our own build tap (a prior build's leak); never a
    // serving app's fc-tap*.
    let _ = run_ip(&["link", "del", tap_name]).await;
    setup_tap(tap_name, host_ip)
        .await
        .context("build VM tap setup (need CAP_NET_ADMIN/root)")?;
    // Egress NAT: the guest pulls BASE image layers (FROM …) over the tap.
    // No mesh access, no clone token — outbound internet only. The build VM is
    // NEVER egress-restricted (it clones + pulls base images from arbitrary
    // registries), so `None` keeps unrestricted egress (Track 7 ACL is for
    // RUNTIME app/workspace FCs, not the builder).
    setup_guest_nat(tap_name, &spec.cfg.tap_subnet, None).await;

    // Tear down our tap + NAT rules on EVERY exit (success/boot-error/
    // timeout-kill) so the host accrues nothing across builds.
    let cleanup = || async {
        let _ = teardown_guest_nat(tap_name).await;
        let _ = run_ip(&["link", "del", tap_name]).await;
    };

    let api_sock = PathBuf::from(format!("/tmp/firecracker-{tap_name}.sock"));
    let _ = std::fs::remove_file(&api_sock);
    // Builds ALWAYS capture the console (ephemeral, single-tenant — the
    // serving path's /dev/null-by-default rationale doesn't apply); it's the
    // last-resort build log if the scratch comes back unreadable.
    let (stdout, stderr) = build_console_stdio(spec.console_log);
    let mut child = match Command::new(&spec.cfg.bin)
        .arg("--api-sock")
        .arg(&api_sock)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            cleanup().await;
            return Err(e).with_context(|| format!("spawn {} for build VM", spec.cfg.bin));
        }
    };

    // Configure + boot. Any failure here kills the half-configured VM.
    let boot = async {
        wait_for_api_sock(&api_sock).await?;
        api_put_sock(
            &api_sock,
            "/machine-config",
            &machine_config_body(BUILD_VCPUS, BUILD_MEM_MIB),
        )
        .await
        .context("PUT /machine-config")?;
        api_put_sock(
            &api_sock,
            "/boot-source",
            &boot_source_body(
                &spec.cfg.kernel,
                &guest_ip.to_string(),
                &host_ip.to_string(),
            ),
        )
        .await
        .context("PUT /boot-source")?;
        api_put_sock(
            &api_sock,
            "/drives/rootfs",
            // READ-ONLY: the toolchain rootfs is shared + digest-cached
            // across builds; a writable mount would let one build corrupt
            // the cached image. The guest tmpfs-mounts its few write paths.
            &rootfs_drive_body(&spec.rootfs.to_string_lossy(), true),
        )
        .await
        .context("PUT /drives/rootfs")?;
        api_put_sock(
            &api_sock,
            "/drives/scratch",
            &aux_drive_body("scratch", &spec.scratch.to_string_lossy(), false),
        )
        .await
        .context("PUT /drives/scratch")?;
        api_put_sock(
            &api_sock,
            "/drives/cache",
            &aux_drive_body("cache", &spec.cache.to_string_lossy(), false),
        )
        .await
        .context("PUT /drives/cache")?;
        api_put_sock(
            &api_sock,
            "/network-interfaces/eth0",
            &network_iface_body(tap_name, guest_mac),
        )
        .await
        .context("PUT /network-interfaces/eth0")?;
        api_put_sock(&api_sock, "/actions", &instance_start_body())
            .await
            .context("PUT /actions InstanceStart")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let outcome = match boot {
        Err(e) => {
            let _ = child.kill().await;
            Err(e)
        }
        Ok(()) => {
            // The build runs to VM POWER-OFF; bound it hard. `child.wait()`
            // reaps the firecracker process (our own child — waitpid, never
            // kill(0)-style probing).
            match tokio::time::timeout(BUILD_VM_TIMEOUT, child.wait()).await {
                Ok(Ok(status)) => {
                    tracing::info!(%status, %tap_name, "build VM exited");
                    Ok(())
                }
                Ok(Err(e)) => Err(anyhow::anyhow!("wait on build VM: {e}")),
                Err(_) => {
                    tracing::warn!(%tap_name, "build VM timed out — killing");
                    let _ = child.kill().await;
                    Err(anyhow::anyhow!(
                        "build timed out after {}s (VM killed)",
                        BUILD_VM_TIMEOUT.as_secs()
                    ))
                }
            }
        }
    };

    // VM-scoped host state dies with the VM, success or not.
    let _ = std::fs::remove_file(&api_sock);
    cleanup().await;
    outcome
}

/// Console stdio for a build VM: ALWAYS capture to `console_log` (truncate
/// per build), falling back to `/dev/null` only if the file can't be opened.
fn build_console_stdio(console_log: &Path) -> (Stdio, Stdio) {
    if let Some(parent) = console_log.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::File::create(console_log) {
        Ok(f) => match f.try_clone() {
            Ok(f2) => (Stdio::from(f), Stdio::from(f2)),
            Err(_) => (Stdio::from(f), Stdio::null()),
        },
        Err(_) => (Stdio::null(), Stdio::null()),
    }
}

/// The firecracker API socket appears asynchronously after spawn; poll
/// briefly (mirrors the serving runtime's bring-up tolerance).
async fn wait_for_api_sock(api_sock: &Path) -> Result<()> {
    for _ in 0..50 {
        if api_sock.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "firecracker API socket never appeared at {}",
        api_sock.display()
    )
}
