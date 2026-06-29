//! SANDBOXED build path (phase 2 of the build/run split): run the docker
//! build inside an ephemeral Firecracker microVM instead of the host docker
//! daemon.
//!
//! Flow (host side):
//! 1. ensure the buildkit TOOLCHAIN rootfs (digest-cached conversion of the
//!    `tabbify/buildkit-toolchain` image from the mesh registry);
//! 2. stage a per-build SCRATCH ext4: the HOST-cloned source rides IN under
//!    `src/` (the clone token never enters the guest), `out/` rides the
//!    result OUT (`mkfs.ext4 -d` populates without a mount);
//! 3. ensure the persistent build-CACHE ext4 (buildkit local cache —
//!    survives VMs, so warm builds skip base layers + unchanged steps);
//! 4. boot the VM ([`crate::firecracker::build_vm`]) and wait for it to
//!    power off (hard timeout → kill);
//! 5. LOOPLESS read-back via `debugfs` (no CAP_SYS_ADMIN / loop module —
//!    the builder may be a host-net container): dump `result.json` +
//!    `out/oci.tar` out of the scratch ext4, check, extract for the push.
//!
//! Builds are SERIALIZED across ALL builder processes on the host via an
//! `flock` on `<data_dir>/build/cache.lock` — each build is its own one-shot
//! process, so the single shared cache disk must never attach to two VMs at
//! once. Per-tenant cache isolation is a follow-up (spec).

// On non-Linux hosts the whole execution path is compiled down to the
// `bail!` arm of `run_sandboxed_build`, leaving the helpers referenced only
// by tests — silence the lib-build dead-code noise there (Linux builds use
// everything).
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use super::firecracker::FcBuildRunner;

/// Scratch disk size. Big enough for source + the built image + logs of any
/// reasonable app; sparse, so unused space costs nothing.
const SCRATCH_MIB: u64 = 8 * 1024;

/// Persistent build-cache disk size (sparse).
const CACHE_MIB: u64 = 20 * 1024;

/// Repository path of the toolchain image inside the mesh registry. The full
/// ref is `<registry_ula>/<this>` — the registry host comes per-job, so no
/// address is baked here. Override the repo path via
/// `SUPERVISOR_BUILD_TOOLCHAIN` (e.g. to pin a digest while testing).
pub const TOOLCHAIN_REPO: &str = "tabbify/buildkit-toolchain:v1";

/// `true` when this supervisor should build inside Firecracker sandboxes.
///
/// Default: build inside Firecracker whenever KVM is available — the docker-less
/// path is the target, so a clean KVM host opts in automatically (no env, no
/// `skopeo` needed). Explicit `SUPERVISOR_FC_BUILD=false`/`0` forces the legacy
/// docker+skopeo path. KVM is required either way: a host without `/dev/kvm`
/// cannot run a build microVM, so it always falls back to the docker path.
#[must_use]
pub fn enabled() -> bool {
    let disabled = matches!(
        std::env::var("SUPERVISOR_FC_BUILD").as_deref(),
        Ok("false") | Ok("0")
    );
    !disabled && crate::firecracker::kvm_available()
}

/// The toolchain image ref for `registry_ula` (env-overridable repo path).
#[must_use]
pub fn toolchain_ref(registry_ula: &str) -> String {
    let repo =
        std::env::var("SUPERVISOR_BUILD_TOOLCHAIN").unwrap_or_else(|_| TOOLCHAIN_REPO.to_owned());
    format!("{registry_ula}/{repo}")
}

/// Upper bound on the cloned source tree before it is copied into the
/// scratch staging — an untrusted repo must not exhaust the host disk (or
/// overflow the scratch image). Well under [`SCRATCH_MIB`].
const MAX_SOURCE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Stage the scratch-disk content tree under `staging`:
/// `src/` (the host-cloned worktree), an empty `out/`, and a version-stamped
/// `job.json` (v2). The tree is then baked into the ext4 via `mkfs -d` — no mount.
///
/// `context` + `dockerfile` are the resolved `[build]` directives RELATIVE to
/// the clone root (`"."` / `"Dockerfile"` by default). They ride in via job.json
/// so the guest builder can honor a subdir Dockerfile/context: a v2 guest reads
/// them (falling back to the defaults when absent), while a legacy v1 guest
/// ignores job.json and always builds the default layout.
///
/// The source is UNTRUSTED (a freshly-cloned repo): its total real size is
/// checked against [`MAX_SOURCE_BYTES`] BEFORE any copy (host-disk-exhaustion
/// guard), and symlinks are recreated as-is (never followed) so a symlink to
/// `../../etc` cannot redirect a copy out of the staging tree.
///
/// # Errors
/// Source exceeds the budget, or an I/O failure copying the tree.
pub fn stage_scratch(staging: &Path, src: &Path, context: &str, dockerfile: &str) -> Result<()> {
    let total = tree_size(src)?;
    if total > MAX_SOURCE_BYTES {
        bail!("source tree is {total} bytes, over the {MAX_SOURCE_BYTES}-byte build limit");
    }
    let dst_src = staging.join("src");
    copy_tree(src, &dst_src).context("copy source into scratch staging")?;
    std::fs::create_dir_all(staging.join("out")).context("create scratch out/")?;
    // job.json v2: the build layout the guest honors. `context`/`dockerfile`
    // are RELATIVE to the cloned source (the guest prefixes `/scratch/src`). A
    // v2 guest reads them + falls back to the v1 defaults (`.` / `Dockerfile`)
    // when absent; a legacy v1 guest ignored job.json entirely.
    let job = serde_json::json!({
        "v": 2,
        "context": context,
        "dockerfile": dockerfile,
    });
    std::fs::write(staging.join("job.json"), format!("{job}\n")).context("write job.json")?;
    Ok(())
}

/// Sum the real on-disk size of a tree WITHOUT following symlinks (a symlink
/// counts as the byte length of its target path, never the pointed-at file).
fn tree_size(root: &Path) -> Result<u64> {
    let mut total = 0u64;
    let md = std::fs::symlink_metadata(root)?;
    if md.is_dir() {
        for entry in std::fs::read_dir(root)? {
            total = total.saturating_add(tree_size(&entry?.path())?);
            if total > MAX_SOURCE_BYTES {
                return Ok(total); // short-circuit; caller bails
            }
        }
    } else {
        total = total.saturating_add(md.len());
    }
    Ok(total)
}

/// Minimal recursive copy (no symlink following into the unknown: symlinks
/// are re-created as-is). The cloned worktree is small and host-trusted.
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst = to.join(entry.file_name());
        if ty.is_dir() {
            copy_tree(&entry.path(), &dst)?;
        } else if ty.is_symlink() {
            let target = std::fs::read_link(entry.path())?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &dst)?;
            #[cfg(not(unix))]
            let _ = target;
        } else {
            std::fs::copy(entry.path(), &dst)?;
        }
    }
    Ok(())
}

/// `mkfs.ext4` argv for creating `image` (pre-truncated by the caller),
/// optionally populated from `from_dir` (`-d`, rootless/loopless).
#[must_use]
pub fn mkfs_args(image: &str, from_dir: Option<&str>) -> Vec<String> {
    let mut args = vec!["-F".to_owned(), "-m".to_owned(), "0".to_owned()];
    if let Some(d) = from_dir {
        args.push("-d".to_owned());
        args.push(d.to_owned());
    }
    args.push(image.to_owned());
    args
}

/// Parse the guest's `result.json` (`{"ok":true}` / `{"ok":false}`); the
/// build log lives next to it for diagnostics.
///
/// # Errors
/// Missing/garbled file (VM died before writing it) or `ok=false`.
pub fn check_result(result_json: &str) -> Result<()> {
    let v: serde_json::Value =
        serde_json::from_str(result_json).context("parse build result.json")?;
    if v.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        Ok(())
    } else {
        bail!("guest build reported failure (see build log)")
    }
}

/// Create a sparse file of `size_mib` and run `mkfs.ext4` on it.
async fn make_ext4(
    image: &Path,
    size_mib: u64,
    from_dir: Option<&Path>,
    runner: &FcBuildRunner,
) -> Result<()> {
    if let Some(parent) = image.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::File::create(image)
        .with_context(|| format!("create disk image {}", image.display()))?;
    f.set_len(size_mib * 1024 * 1024)
        .context("size disk image")?;
    drop(f);
    let mut argv = vec!["mkfs.ext4".to_owned()];
    argv.extend(mkfs_args(
        &image.to_string_lossy(),
        from_dir
            .map(|d| d.to_string_lossy().into_owned())
            .as_deref(),
    ));
    run_host(&argv, runner).await
}

/// Run one host command through the injected build runner (argv[0] = binary;
/// the runner returns `(success, combined-output)`).
async fn run_host(argv: &[String], runner: &FcBuildRunner) -> Result<()> {
    let (ok, output) = (runner)(argv.to_vec()).await;
    if ok {
        Ok(())
    } else {
        bail!(
            "`{}` failed: {}",
            argv.join(" "),
            String::from_utf8_lossy(&output).trim()
        )
    }
}

/// Run the sandboxed build: source in `src` (already cloned by the host),
/// returns the extracted OCI LAYOUT directory (tagged `build`) ready for the
/// oras registry push.
///
/// Builds are serialized across ALL builder processes on the host via an
/// `flock` on `<data_dir>/build/cache.lock` — each build runs in its own
/// one-shot `tabbify-runner --build-spec` process, so an in-process mutex
/// would protect nothing; the single shared cache disk must never be
/// attached to two VMs at once.
///
/// # Errors
/// Toolchain/scratch/VM/result failures. Linux-only (callers gate on
/// [`enabled`]).
// Each parameter is a distinct build input threaded from `run_build` (identity
// + source + build layout + registry + on-disk locations + the injected host
// runner); bundling them into a struct would only obscure the one call site.
#[allow(clippy::too_many_arguments)]
pub async fn run_sandboxed_build(
    app_uuid: &str,
    src: &Path,
    context: &str,
    dockerfile: &str,
    registry_ula: &str,
    data_dir: &Path,
    workdir: &Path,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (
            app_uuid,
            src,
            context,
            dockerfile,
            registry_ula,
            data_dir,
            workdir,
            runner,
        );
        bail!("the firecracker build sandbox is Linux-only")
    }
    #[cfg(target_os = "linux")]
    {
        let fc_cfg = fc_config_from_env();
        let build_dir = data_dir.join("build");
        std::fs::create_dir_all(&build_dir)?;

        // 1. Toolchain rootfs (digest-cached).
        let toolchain = toolchain_ref(registry_ula);
        let rootfs =
            super::firecracker::ensure_toolchain_rootfs(&toolchain, data_dir, runner).await?;

        // 2. Scratch disk: staged tree → mkfs -d (untrusted source size-capped).
        //    `context`/`dockerfile` ride in via job.json v2 so the guest builds
        //    the right layout (subdir Dockerfile/context supported).
        let staging = workdir.join("scratch-staging");
        stage_scratch(&staging, src, context, dockerfile)?;
        let scratch = workdir.join("scratch.ext4");
        make_ext4(&scratch, SCRATCH_MIB, Some(&staging), runner).await?;

        // 3+4. CROSS-PROCESS lock around cache touch + boot. The flock guard
        //      lives in a blocking task; it is held for the whole VM lifetime.
        let cache = build_dir.join("cache.ext4");
        let lock_path = build_dir.join("cache.lock");
        let console_log = build_dir.join(format!("{app_uuid}.console.log"));

        let _guard = HostFileLock::acquire(&lock_path).await?;

        // Cache integrity: a build VM killed mid-write leaves the cache ext4
        // dirty. e2fsck -p replays the journal / auto-fixes; an UNFIXABLE
        // cache is quarantined (rebuilt fresh) rather than poisoning every
        // future build. Created on first use.
        if cache.is_file() {
            let (ok, _) = (runner)(vec![
                "e2fsck".to_owned(),
                "-p".to_owned(),
                "-f".to_owned(),
                cache.to_string_lossy().into_owned(),
            ])
            .await;
            if !ok {
                tracing::warn!("build cache failed e2fsck — recreating fresh");
                let _ = std::fs::remove_file(&cache);
            }
        }
        if !cache.is_file() {
            make_ext4(&cache, CACHE_MIB, None, runner).await?;
        }

        let spec = crate::firecracker::build_vm::BuildVmSpec {
            rootfs: &rootfs,
            scratch: &scratch,
            cache: &cache,
            console_log: &console_log,
            data_dir,
            cfg: &fc_cfg,
        };
        crate::firecracker::build_vm::run_build_vm(&spec).await?;

        // 5. Read back LOOPLESS via debugfs (no CAP_SYS_ADMIN / loop module —
        //    the builder may be a host-net container): dump result.json +
        //    out/oci.tar straight out of the scratch ext4, then check +
        //    extract on the host. Mirrors the loopless `mkfs -d` write side.
        let outcome = read_back_debugfs(&scratch, workdir, runner).await;

        drop(_guard);
        outcome
    }
}

/// Dump `path_in_ext4` out of `image` to `dest` via `debugfs -R "dump …"`
/// (no mount, no loop, no root-mount caps — just e2fsprogs).
#[cfg(target_os = "linux")]
async fn debugfs_dump(
    image: &Path,
    path_in_ext4: &str,
    dest: &Path,
    runner: &FcBuildRunner,
) -> Result<()> {
    run_host(
        &[
            "debugfs".to_owned(),
            "-R".to_owned(),
            format!("dump {path_in_ext4} {}", dest.to_string_lossy()),
            image.to_string_lossy().into_owned(),
        ],
        runner,
    )
    .await
    .with_context(|| format!("debugfs dump {path_in_ext4} from {}", image.display()))
}

/// Loopless read-back: dump `result.json` + `out/oci.tar` from the scratch
/// ext4, check the result, extract the OCI layout.
#[cfg(target_os = "linux")]
async fn read_back_debugfs(
    scratch: &Path,
    workdir: &Path,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    let result_path = workdir.join("result.json");
    debugfs_dump(scratch, "/result.json", &result_path, runner)
        .await
        .context("VM died before writing result.json? (see console log)")?;
    let result = std::fs::read_to_string(&result_path).context("read dumped result.json")?;

    if let Err(e) = check_result(&result) {
        // Surface the tail of the guest build log — THE actionable part.
        let log_path = workdir.join("build.log");
        let tail = if debugfs_dump(scratch, "/out/build.log", &log_path, runner)
            .await
            .is_ok()
        {
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            log.lines()
                .rev()
                .take(15)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            String::new()
        };
        bail!("{e}: {tail}");
    }

    let tarball = workdir.join("oci.tar");
    debugfs_dump(scratch, "/out/oci.tar", &tarball, runner)
        .await
        .context("dump built OCI tarball")?;
    let layout = workdir.join("oci-out");
    std::fs::create_dir_all(&layout)?;
    let f = std::fs::File::open(&tarball)
        .with_context(|| format!("open built OCI tarball {}", tarball.display()))?;
    tar::Archive::new(f)
        .unpack(&layout)
        .context("extract OCI layout tarball")?;
    // buildkit tags the OCI manifest with whatever `name=` it was given (and
    // may qualify it, e.g. `build:latest`), but the registry push reads the
    // layout by the fixed tag the push args use. Normalize the FIRST image
    // manifest's `org.opencontainers.image.ref.name` to that tag so the oras
    // `--from-oci-layout <dir>:build` always resolves.
    normalize_layout_tag(&layout)?;
    Ok(layout)
}

/// Force the first image manifest in `<layout>/index.json` to carry the
/// `ref.name` the push expects (`build` — see `skopeo::LAYOUT_TAG`), so the
/// host oras push finds it regardless of how buildkit named the build.
#[cfg(target_os = "linux")]
fn normalize_layout_tag(layout: &Path) -> Result<()> {
    let index_path = layout.join("index.json");
    let raw = std::fs::read_to_string(&index_path).context("read built OCI layout index.json")?;
    let mut index: serde_json::Value =
        serde_json::from_str(&raw).context("parse OCI layout index.json")?;
    let manifests = index
        .get_mut("manifests")
        .and_then(serde_json::Value::as_array_mut)
        .filter(|m| !m.is_empty())
        .context("OCI layout index.json has no manifests")?;
    let ann = manifests[0]
        .as_object_mut()
        .context("OCI manifest descriptor is not an object")?
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    ann.as_object_mut()
        .context("manifest annotations is not an object")?
        .insert(
            "org.opencontainers.image.ref.name".to_owned(),
            serde_json::Value::String("build".to_owned()),
        );
    std::fs::write(&index_path, serde_json::to_vec(&index)?)
        .context("rewrite OCI layout index.json with normalized tag")?;
    Ok(())
}

/// Cross-process exclusive advisory lock (`flock(LOCK_EX)`) on a lockfile,
/// held for the build's VM lifetime. The kernel keys flock on the inode, so
/// it serializes EVERY `tabbify-runner --build-spec` process on the host
/// contending for the single shared cache disk — which a process-local
/// mutex could not.
#[cfg(target_os = "linux")]
struct HostFileLock {
    file: std::fs::File,
}

#[cfg(target_os = "linux")]
impl HostFileLock {
    async fn acquire(path: &Path) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("open build lock {}", path.display()))?;
        let fd = file.as_raw_fd();
        // LOCK_EX blocks; run it on the blocking pool so we don't stall the
        // reactor while another build holds the lock.
        tokio::task::spawn_blocking(move || {
            // SAFETY: fd is a valid open file for the duration of this call.
            let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        })
        .await
        .context("flock task join")?
        .context("flock LOCK_EX on build lock")?;
        Ok(Self { file })
    }
}

#[cfg(target_os = "linux")]
impl Drop for HostFileLock {
    fn drop(&mut self) {
        use std::os::unix::io::AsRawFd;
        // SAFETY: self.file is open until after this unlock.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

/// FcConfig for the build VM from the same envs/defaults the daemon uses
/// (the one-shot build runner has no clap context).
#[cfg(target_os = "linux")]
fn fc_config_from_env() -> crate::config::FcConfig {
    crate::config::FcConfig {
        bin: std::env::var("SUPERVISOR_FC_BIN")
            .unwrap_or_else(|_| crate::config::DEFAULT_FC_BIN.to_owned()),
        kernel: std::env::var("SUPERVISOR_FC_KERNEL")
            .unwrap_or_else(|_| crate::config::DEFAULT_FC_KERNEL.to_owned()),
        vcpus: 2,
        tap_subnet: std::env::var("SUPERVISOR_FC_TAP_SUBNET")
            .unwrap_or_else(|_| crate::config::DEFAULT_FC_TAP_SUBNET.to_owned()),
        app_port: 8080,
        // F1 (audit #93): the one-shot build runner has no clap context, so read
        // the per-FC CPU-scope knobs from the SAME envs clap binds (the daemon
        // bakes them into the unit), falling back to the audited defaults. Bad /
        // missing values use the default so a build never fails to spawn over a
        // malformed knob.
        cpu_quota_serving_pct: env_u32("SUPERVISOR_FC_CPU_QUOTA_SERVING", 100),
        cpu_quota_build_pct: env_u32("SUPERVISOR_FC_CPU_QUOTA_BUILD", 200),
        cpu_weight: env_u32("SUPERVISOR_FC_CPU_WEIGHT", 80),
    }
}

/// Parse a `u32` from env `var`, falling back to `default` on absent/unparseable.
#[cfg(target_os = "linux")]
fn env_u32(var: &str, default: u32) -> u32 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The scratch staging tree must carry the source under `src/`, an empty
    /// `out/`, and a versioned `job.json` — the v2 guest contract carries the
    /// build layout (context + dockerfile) for the guest to honor.
    #[test]
    fn stage_scratch_lays_out_the_v2_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("clone");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(src.join("sub").join("a.txt"), "x").unwrap();

        let staging = tmp.path().join("staging");
        stage_scratch(&staging, &src, ".", "Dockerfile").unwrap();

        assert!(staging.join("src").join("Dockerfile").is_file());
        assert!(staging.join("src").join("sub").join("a.txt").is_file());
        assert!(staging.join("out").is_dir());
        let job = std::fs::read_to_string(staging.join("job.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&job).unwrap();
        assert_eq!(v["v"], 2, "got: {job}");
        assert_eq!(v["context"], ".");
        assert_eq!(v["dockerfile"], "Dockerfile");
    }

    /// A NON-default `[build]` layout (subdir context + subdir Dockerfile) is
    /// threaded VERBATIM into job.json v2 — the host no longer rejects it; the
    /// guest reads these RELATIVE paths and prefixes `/scratch/src`.
    #[test]
    fn stage_scratch_threads_custom_layout_into_job_json() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("clone");
        std::fs::create_dir_all(src.join("frontend")).unwrap();
        std::fs::write(src.join("frontend").join("Dockerfile"), "FROM scratch\n").unwrap();

        let staging = tmp.path().join("staging");
        stage_scratch(&staging, &src, "frontend", "frontend/Dockerfile").unwrap();

        let job = std::fs::read_to_string(staging.join("job.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&job).unwrap();
        assert_eq!(v["v"], 2);
        assert_eq!(v["context"], "frontend");
        assert_eq!(v["dockerfile"], "frontend/Dockerfile");
    }

    /// mkfs argv shape: force + no reserved blocks + optional populate dir,
    /// image last.
    #[test]
    fn mkfs_args_shape() {
        assert_eq!(
            mkfs_args("/w/scratch.ext4", Some("/w/staging")),
            vec!["-F", "-m", "0", "-d", "/w/staging", "/w/scratch.ext4"]
        );
        assert_eq!(
            mkfs_args("/d/cache.ext4", None),
            vec!["-F", "-m", "0", "/d/cache.ext4"]
        );
    }

    /// result.json contract: ok=true passes; ok=false / garbage / missing-ok
    /// all fail (the VM dying mid-build must never look like success).
    #[test]
    fn check_result_contract() {
        assert!(check_result("{\"ok\":true}").is_ok());
        assert!(check_result("{\"ok\":false}").is_err());
        assert!(check_result("{}").is_err());
        assert!(check_result("not json").is_err());
    }

    /// The toolchain ref composes the per-job registry host with the stable
    /// repo path — no registry address is baked into the binary.
    #[test]
    fn toolchain_ref_composes_registry_and_repo() {
        // NB: do not set SUPERVISOR_BUILD_TOOLCHAIN in the test env.
        let r = toolchain_ref("[fd5a:1f00:0:3::1]:5000");
        assert_eq!(r, "[fd5a:1f00:0:3::1]:5000/tabbify/buildkit-toolchain:v1");
    }

    /// `tree_size` must NOT follow symlinks (a symlink to a huge/forbidden
    /// path counts as its link-string length, never the target's size) — the
    /// host-disk-exhaustion + symlink-escape guard over an untrusted repo.
    #[test]
    fn tree_size_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a"), vec![0u8; 1000]).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc/passwd", root.join("link")).unwrap();
        let n = tree_size(&root).unwrap();
        // 1000 (real file) + small symlink path length, NOT the size of
        // /etc/passwd's target.
        assert!((1000..1100).contains(&n), "got {n}");
    }

    /// An oversized source tree is rejected BEFORE any copy (the staging dir
    /// stays empty).
    #[test]
    fn stage_scratch_rejects_oversized_source() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("clone");
        std::fs::create_dir_all(&src).unwrap();
        // A sparse file LARGER than the cap — `len()` reports the apparent
        // size without consuming disk.
        let f = std::fs::File::create(src.join("big")).unwrap();
        f.set_len(MAX_SOURCE_BYTES + 1).unwrap();
        let staging = tmp.path().join("staging");
        let err = stage_scratch(&staging, &src, ".", "Dockerfile")
            .unwrap_err()
            .to_string();
        assert!(err.contains("over the"), "got: {err}");
        assert!(!staging.join("src").exists(), "must not copy on reject");
    }

    /// The push reads the layout by a fixed tag, so the extracted layout's
    /// first manifest must be normalized to that tag regardless of how
    /// buildkit named the build (`build`, `build:latest`, or unnamed).
    #[cfg(target_os = "linux")]
    #[test]
    fn normalize_layout_tag_sets_ref_name() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = tmp.path().join("oci-out");
        std::fs::create_dir_all(&layout).unwrap();
        // buildkit-style index with a different ref.name.
        std::fs::write(
            layout.join("index.json"),
            r#"{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:abc","size":1,"annotations":{"org.opencontainers.image.ref.name":"build:latest"}}]}"#,
        )
        .unwrap();
        normalize_layout_tag(&layout).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(layout.join("index.json")).unwrap())
                .unwrap();
        assert_eq!(
            v["manifests"][0]["annotations"]["org.opencontainers.image.ref.name"],
            "build"
        );
    }

    /// An unnamed manifest gains the ref.name annotation.
    #[cfg(target_os = "linux")]
    #[test]
    fn normalize_layout_tag_adds_missing_annotation() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = tmp.path().join("oci-out");
        std::fs::create_dir_all(&layout).unwrap();
        std::fs::write(
            layout.join("index.json"),
            r#"{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:abc","size":1}]}"#,
        )
        .unwrap();
        normalize_layout_tag(&layout).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(layout.join("index.json")).unwrap())
                .unwrap();
        assert_eq!(
            v["manifests"][0]["annotations"]["org.opencontainers.image.ref.name"],
            "build"
        );
    }

    /// The build VM's tap / MAC / /30 identity must be DISJOINT from any the
    /// serving runtime's `VM_SEQ` realistically derives — a build must never
    /// name or address (or `ip link del`) a live serving app's device in the
    /// shared netns. (Linux-only: the build VM + `derive_link_ips` are.)
    #[cfg(target_os = "linux")]
    #[test]
    fn build_vm_identity_is_disjoint_from_serving() {
        use crate::firecracker::linux::derive_link_ips;
        // Serving climbs from seq 0; build is pinned at the top of the /16.
        let (serving_host, _) = derive_link_ips("172.31.0.0/16", 0).unwrap();
        let (build_host, _) =
            derive_link_ips("172.31.0.0/16", crate::firecracker::build_vm::BUILD_SEQ).unwrap();
        assert_ne!(serving_host, build_host);
        // The build tap/MAC use a distinct prefix/OUI byte from serving's
        // fc-tap* / 02:FC:*.
        assert!(crate::firecracker::build_vm::BUILD_TAP.starts_with("fc-bld"));
        assert!(crate::firecracker::build_vm::BUILD_MAC.starts_with("02:FB:"));
    }
}
