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
//! 5. loop-mount the scratch read-only, check `result.json`, extract the
//!    OCI tarball into a layout dir for the oras registry push.
//!
//! Builds are SERIALIZED per supervisor (one global lock): the cache disk
//! is a single ext4 that must not be attached to two VMs at once.
//! Per-tenant cache isolation is a follow-up (spec).

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

/// `true` when this supervisor should build inside Firecracker sandboxes:
/// explicit opt-in (`SUPERVISOR_FC_BUILD=true`) AND a usable KVM. The docker
/// path stays the default during the transition.
#[must_use]
pub fn enabled() -> bool {
    let opted = std::env::var("SUPERVISOR_FC_BUILD")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    opted && crate::firecracker::kvm_available()
}

/// The toolchain image ref for `registry_ula` (env-overridable repo path).
#[must_use]
pub fn toolchain_ref(registry_ula: &str) -> String {
    let repo = std::env::var("SUPERVISOR_BUILD_TOOLCHAIN")
        .unwrap_or_else(|_| TOOLCHAIN_REPO.to_owned());
    format!("{registry_ula}/{repo}")
}

/// Stage the scratch-disk content tree under `staging`:
/// `src/` (the host-cloned worktree), an empty `out/`, and a version-stamped
/// `job.json`. The tree is then baked into the ext4 via `mkfs -d` — no mount.
///
/// # Errors
/// I/O failures copying the source tree.
pub fn stage_scratch(staging: &Path, src: &Path) -> Result<()> {
    let dst_src = staging.join("src");
    copy_tree(src, &dst_src).context("copy source into scratch staging")?;
    std::fs::create_dir_all(staging.join("out")).context("create scratch out/")?;
    // Versioned for forward evolution; the v1 guest entrypoint is parameter-
    // free (fixed context/dockerfile/output paths).
    std::fs::write(staging.join("job.json"), "{\"v\":1}\n").context("write job.json")?;
    Ok(())
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
        from_dir.map(|d| d.to_string_lossy().into_owned()).as_deref(),
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

/// Builds are serialized per supervisor: the single cache disk must never be
/// attached to two running VMs.
static BUILD_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Run the sandboxed build: source in `src` (already cloned by the host),
/// returns the extracted OCI LAYOUT directory (tagged `build`) ready for the
/// oras registry push.
///
/// # Errors
/// Toolchain/scratch/VM/result failures. Linux-only (callers gate on
/// [`enabled`]).
pub async fn run_sandboxed_build(
    app_uuid: &str,
    src: &Path,
    registry_ula: &str,
    workdir: &Path,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (app_uuid, src, registry_ula, workdir, runner);
        bail!("the firecracker build sandbox is Linux-only")
    }
    #[cfg(target_os = "linux")]
    {
        let data_dir = std::env::var("SUPERVISOR_DATA_DIR")
            .unwrap_or_else(|_| "/var/lib/tabbify".to_owned());
        let data_dir = PathBuf::from(data_dir);
        let fc_cfg = fc_config_from_env();

        // 1. Toolchain rootfs (digest-cached).
        let toolchain = toolchain_ref(registry_ula);
        let rootfs =
            super::firecracker::ensure_toolchain_rootfs(&toolchain, &data_dir, runner).await?;

        // 2. Scratch disk: staged tree → mkfs -d.
        let staging = workdir.join("scratch-staging");
        stage_scratch(&staging, src)?;
        let scratch = workdir.join("scratch.ext4");
        make_ext4(&scratch, SCRATCH_MIB, Some(&staging), runner).await?;

        // 3. Persistent cache disk (created once).
        let cache = data_dir.join("build").join("cache.ext4");
        if !cache.is_file() {
            make_ext4(&cache, CACHE_MIB, None, runner).await?;
        }

        // 4. Boot + wait (serialized — single cache disk).
        let console_log = data_dir.join("build").join(format!("{app_uuid}.console.log"));
        if let Some(p) = console_log.parent() {
            std::fs::create_dir_all(p)?;
        }
        {
            let _guard = BUILD_LOCK.lock().await;
            let spec = crate::firecracker::build_vm::BuildVmSpec {
                rootfs: &rootfs,
                scratch: &scratch,
                cache: &cache,
                console_log: &console_log,
                cfg: &fc_cfg,
            };
            crate::firecracker::build_vm::run_build_vm(&spec).await?;
        }

        // 5. Read back: loop-mount the scratch RO, check the result, pull
        //    the OCI tarball out, extract it into a layout dir.
        let mnt = workdir.join("scratch-mnt");
        std::fs::create_dir_all(&mnt)?;
        run_host(
            &[
                "mount".to_owned(),
                "-o".to_owned(),
                "loop,ro".to_owned(),
                scratch.to_string_lossy().into_owned(),
                mnt.to_string_lossy().into_owned(),
            ],
            runner,
        )
        .await
        .context("loop-mount scratch for read-back")?;

        let outcome = read_back(&mnt, workdir);

        // Always unmount, success or not (the scratch file dies with workdir).
        let _ = run_host(
            &["umount".to_owned(), mnt.to_string_lossy().into_owned()],
            runner,
        )
        .await;

        outcome
    }
}

/// Mounted-scratch read-back: result check + OCI tar extraction (in-process
/// `tar` crate — the tarball is a plain OCI layout tree).
#[cfg(target_os = "linux")]
fn read_back(mnt: &Path, workdir: &Path) -> Result<PathBuf> {
    let result = std::fs::read_to_string(mnt.join("result.json")).with_context(|| {
        format!(
            "read result.json (VM died before finishing? console log + {} for clues)",
            mnt.join("out").join("build.log").display()
        )
    })?;
    if let Err(e) = check_result(&result) {
        // Surface the tail of the guest build log — THE actionable part.
        let log = std::fs::read_to_string(mnt.join("out").join("build.log")).unwrap_or_default();
        let tail: String = log
            .lines()
            .rev()
            .take(15)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        bail!("{e}: {tail}");
    }

    let tarball = mnt.join("out").join("oci.tar");
    let layout = workdir.join("oci-out");
    std::fs::create_dir_all(&layout)?;
    let f = std::fs::File::open(&tarball)
        .with_context(|| format!("open built OCI tarball {}", tarball.display()))?;
    tar::Archive::new(f)
        .unpack(&layout)
        .context("extract OCI layout tarball")?;
    Ok(layout)
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
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The scratch staging tree must carry the source under `src/`, an empty
    /// `out/`, and a versioned `job.json` — the v1 guest contract.
    #[test]
    fn stage_scratch_lays_out_the_v1_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("clone");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(src.join("sub").join("a.txt"), "x").unwrap();

        let staging = tmp.path().join("staging");
        stage_scratch(&staging, &src).unwrap();

        assert!(staging.join("src").join("Dockerfile").is_file());
        assert!(staging.join("src").join("sub").join("a.txt").is_file());
        assert!(staging.join("out").is_dir());
        let job = std::fs::read_to_string(staging.join("job.json")).unwrap();
        assert!(job.contains("\"v\":1"), "got: {job}");
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
}
