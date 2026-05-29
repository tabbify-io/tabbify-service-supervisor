//! Generic Firecracker runtime-build: convert an OCI image into a
//! `rootfs.ext4` + a minimal PID-1 init, then boot it via the existing
//! `FirecrackerRuntime` contract.
//!
//! This is a RUNTIME-build helper (OCI image → bootable rootfs), invoked from
//! [`crate::build::build_runtime`] — NOT the CI-build pipeline in the sibling
//! `docker.rs` / `wasm.rs` (clone → build → push). See plan 04.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};

use crate::runtime::BoxFut;

// The OCI→ext4 building blocks below get their first non-test caller when fc-5
// wires `run_firecracker_build` into the `"firecracker"` arm of
// `crate::build::build_runtime`. Until then they are exercised only by this
// module's unit tests, so each carries `#[allow(dead_code)]` to keep the build
// warning-clean (CI denies warnings via `cargo clippy --all-targets -- -D
// warnings`); the attribute drops out as the chain becomes live in fc-5.

/// External-command seam for the OCI→ext4 conversion (`docker pull`,
/// `docker inspect`, `docker export`, `tar`, `mkfs.ext4`). Receives the full
/// argv (first element = program) and returns `(exit_ok, stdout_bytes)`:
/// `exit_ok` is `true` iff the process exits 0, and `stdout_bytes` is the
/// captured STDOUT (empty for commands whose output we ignore, populated for
/// `docker inspect` whose JSON config we parse — see [`read_oci_config`]).
///
/// `docker::CommandRunner` returns only `bool`; we widen to also carry stdout
/// because `docker inspect` writes its OCI config to STDOUT (it has NO `-o`
/// flag). Unit tests inject a fake runner that returns canned stdout for
/// `inspect` and side-effects the rootfs file for `mkfs.ext4`.
#[allow(dead_code)] // first non-test caller arrives in fc-5
pub type FcBuildRunner =
    Arc<dyn Fn(Vec<String>) -> BoxFut<'static, (bool, Vec<u8>)> + Send + Sync>;

/// Name of the produced rootfs image inside the output dir.
#[allow(dead_code)] // first non-test caller arrives in fc-5
const ROOTFS_NAME: &str = "rootfs.ext4";

/// Convert a local OCI image (already pulled + tagged as `image_tag`) into a
/// bootable `rootfs.ext4` under `out_dir`, ROOTLESS and LOOPLESS.
///
/// ## OCI → ext4 contract (see fc-8 for the full risk write-up)
/// 1. `docker create <image_tag>` → a stopped container whose filesystem is the
///    image's merged layers.
/// 2. `docker export <cid>` → a flat tar of that filesystem, untarred into a
///    staging dir (no overlay, no daemon mounts needed at boot).
/// 3. `mkfs.ext4 -d <staging> <out_dir>/rootfs.ext4` — the `-d` flag populates a
///    fresh ext4 image from the staging dir's contents WITHOUT a loop device
///    and WITHOUT root (e2fsprogs ≥ 1.43). This is the crux of the rootless
///    path: no `mount`, no `losetup`, no `sudo`.
///
/// `size_mib` sizes the ext4 image; callers pad it over the unpacked size.
///
/// # Errors
/// A failing `docker create`/`export`, untar failure, or a failing `mkfs.ext4`.
#[allow(dead_code)] // first non-test caller arrives in fc-5
pub async fn build_rootfs_ext4(
    image_tag: &str,
    out_dir: &Path,
    size_mib: u32,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    // fc-1 is the no-init form: export → untar → mkfs with nothing injected.
    // The fc-5 init path calls the same primitive with `Some(init)`. Keeping a
    // single primitive means the export/tar/mkfs argv shape has ONE source of
    // truth (no drift between fc-1 and fc-5).
    build_rootfs_ext4_inner(image_tag, out_dir, size_mib, None, runner).await
}

/// Shared OCI→ext4 primitive — the SINGLE source of truth for the
/// export → untar → (optional init inject) → `mkfs.ext4 -d` argv sequence.
///
/// `init`:
/// - `None`  → fc-1 form (no PID-1 init written; raw image filesystem),
/// - `Some(s)` → fc-5 form: write `s` to `<staging>/init` (mode 0755) AFTER the
///   untar and BEFORE `mkfs.ext4` so the rendered PID-1 init is baked in.
///
/// Both [`build_rootfs_ext4`] (fc-1) and the fc-5 conversion call this; neither
/// re-inlines the argv, so the shape can never drift.
///
/// # Errors
/// A failing `docker export`, untar failure, init-write failure, or a failing
/// `mkfs.ext4`.
#[allow(dead_code)] // first non-test caller arrives in fc-5
async fn build_rootfs_ext4_inner(
    image_tag: &str,
    out_dir: &Path,
    size_mib: u32,
    init: Option<&str>,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(out_dir)
        .await
        .with_context(|| format!("create rootfs out dir {}", out_dir.display()))?;

    let staging = out_dir.join("stage");
    tokio::fs::create_dir_all(&staging)
        .await
        .with_context(|| format!("create staging dir {}", staging.display()))?;
    let tar_path = out_dir.join("fs.tar");

    // 1+2. `docker create` → `docker export <cid> -o <tar>`. We model both as a
    //      single `export` argv for the seam; production wiring (fc-5) supplies
    //      a runner that runs `docker create` then `docker export`. (`docker
    //      export` DOES support `-o <file>` — unlike `docker inspect`.)
    let (exported, _) = (runner)(vec![
        "docker".to_owned(),
        "export".to_owned(),
        image_tag.to_owned(),
        "-o".to_owned(),
        tar_path.to_string_lossy().into_owned(),
    ])
    .await;
    if !exported {
        bail!("docker export of image {image_tag} failed");
    }

    // 2b. Untar the exported filesystem into the staging dir (rootless).
    let (untarred, _) = (runner)(vec![
        "tar".to_owned(),
        "-xf".to_owned(),
        tar_path.to_string_lossy().into_owned(),
        "-C".to_owned(),
        staging.to_string_lossy().into_owned(),
    ])
    .await;
    if !untarred {
        bail!("untar of exported image {image_tag} failed");
    }

    // 2c. fc-5 only: inject the rendered PID-1 init into the staging dir, AFTER
    //     untar and BEFORE mkfs, so it is baked into the ext4 image.
    if let Some(script) = init {
        inject_init(&staging, script).await?;
    }

    // 3. Pre-size the backing image to `size_mib` MiB (sparse `set_len`, no
    //    loop device, no root), then `mkfs.ext4 -F -d <staging> <out>` formats
    //    the existing file in place. The fs-size positional is intentionally
    //    OMITTED so the OUTPUT path stays the final argv element: `mkfs.ext4`
    //    treats the first positional as the device and the *optional* trailing
    //    one as the fs size; a regular file pre-sized here makes the size
    //    positional redundant. Content-populated, rootless, loopless.
    let rootfs = out_dir.join(ROOTFS_NAME);
    {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&rootfs)
            .await
            .with_context(|| format!("create rootfs image {}", rootfs.display()))?;
        file.set_len(u64::from(size_mib) * 1024 * 1024)
            .await
            .with_context(|| format!("size rootfs image {} to {size_mib}MiB", rootfs.display()))?;
    }
    let (made, _) = (runner)(vec![
        "mkfs.ext4".to_owned(),
        "-F".to_owned(), // overwrite the pre-sized image without prompting
        "-d".to_owned(),
        staging.to_string_lossy().into_owned(),
        rootfs.to_string_lossy().into_owned(),
    ])
    .await;
    if !made {
        bail!("mkfs.ext4 -d for image {image_tag} failed");
    }
    if !rootfs.is_file() {
        bail!(
            "mkfs.ext4 reported success but {} is missing",
            rootfs.display()
        );
    }
    Ok(rootfs)
}

/// Write the rendered init to `<staging>/init` with mode 0755 so the kernel can
/// `init=/init` it as PID 1.
#[allow(dead_code)] // first non-test caller arrives in fc-5
async fn inject_init(staging: &Path, script: &str) -> Result<()> {
    let path = staging.join("init");
    tokio::fs::write(&path, script.as_bytes())
        .await
        .with_context(|| format!("write guest init {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mut perms = tokio::fs::metadata(&path).await?.permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(&path, perms).await?;
    }
    Ok(())
}

/// The exec-form entrypoint distilled from an OCI image config.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // first non-test caller arrives in fc-5
pub struct OciExec {
    /// `config.Entrypoint` — the program + leading args (PID 1).
    pub entrypoint: Vec<String>,
    /// `config.Cmd` — default args appended after the entrypoint.
    pub cmd: Vec<String>,
    /// `config.Env` — `KEY=VALUE` strings exported before exec.
    pub env: Vec<String>,
    /// `config.WorkingDir` — `cd`'d into before exec (`/` if empty).
    pub workdir: String,
}

/// How the image declares its process. Phase-1 supports EXEC-FORM only (D3).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // first non-test caller arrives in fc-5
pub enum Entrypoint {
    /// A concrete argv to `exec` as PID 1.
    Exec(OciExec),
    /// No usable exec-form argv (image relies on a shell). DEFERRED — render
    /// returns a clear error rather than guessing `/bin/sh -c`.
    ShellForm,
}

impl Entrypoint {
    /// Classify an OCI config into exec-form vs shell-form.
    ///
    /// Exec-form requires a non-empty `Entrypoint` OR `Cmd` (the program to run
    /// is then `entrypoint ++ cmd`). An image with NEITHER is shell-form
    /// (deferred): a bare `Cmd` that is meant for a shell base image can't be
    /// distinguished safely in Phase-1, so empty argv ⇒ ShellForm.
    #[must_use]
    #[allow(dead_code)] // first non-test caller arrives in fc-5
    pub fn from_oci(cfg: &oci_spec::image::ImageConfiguration) -> Self {
        let Some(inner) = cfg.config().as_ref() else {
            return Entrypoint::ShellForm;
        };
        let entrypoint = inner.entrypoint().clone().unwrap_or_default();
        let cmd = inner.cmd().clone().unwrap_or_default();
        if entrypoint.is_empty() && cmd.is_empty() {
            return Entrypoint::ShellForm;
        }
        Entrypoint::Exec(OciExec {
            entrypoint,
            cmd,
            env: inner.env().clone().unwrap_or_default(),
            workdir: {
                let wd = inner.working_dir().clone().unwrap_or_default();
                if wd.is_empty() { "/".to_owned() } else { wd }
            },
        })
    }
}

/// Render the guest PID-1 init script from an [`Entrypoint`].
///
/// The script (run as PID 1 by the kernel `init=/init` arg) mounts the pseudo
/// filesystems, verifies `eth0` (the kernel `ip=` boot-arg already configured
/// it to `172.31.0.2` per the existing `FirecrackerRuntime` contract), exports
/// the OCI env, cd's to the workdir, then `exec`s the entrypoint argv so the
/// app server becomes PID 1's successor.
///
/// # Errors
/// [`Entrypoint::ShellForm`] — shell-form entrypoints are not yet supported
/// (D3); the error message says so clearly.
#[allow(dead_code)] // first non-test caller arrives in fc-5
pub fn render_init(entry: &Entrypoint) -> Result<String> {
    let exec = match entry {
        Entrypoint::Exec(e) => e,
        Entrypoint::ShellForm => {
            bail!(
                "shell-form entrypoint not yet supported by the generic \
                 firecracker runtime (Phase-1 is EXEC-FORM only); set an \
                 explicit exec-form ENTRYPOINT/CMD in the image"
            );
        }
    };

    // Build the exec argv: entrypoint ++ cmd, space-joined verbatim (exec-form).
    let mut argv = exec.entrypoint.clone();
    argv.extend(exec.cmd.iter().cloned());
    let exec_line = argv.join(" ");

    let env_lines: String = exec
        .env
        .iter()
        .map(|kv| format!("export {kv}\n"))
        .collect();

    // POSIX sh init. `set -e` so a failed mount aborts loudly to the console.
    Ok(format!(
        "#!/bin/sh\n\
         set -e\n\
         mount -t proc proc /proc\n\
         mount -t sysfs sysfs /sys\n\
         mount -t devtmpfs devtmpfs /dev 2>/dev/null || mount -t tmpfs tmpfs /dev\n\
         # eth0 is configured by the kernel ip= boot-arg; verify it came up.\n\
         if [ ! -e /sys/class/net/eth0 ]; then\n\
         \techo 'tabbify-init: eth0 missing (kernel ip= boot-arg did not configure it)' >&2\n\
         fi\n\
         ip link show eth0 >/dev/null 2>&1 || true\n\
         {env_lines}\
         cd {workdir}\n\
         exec {exec_line}\n",
        workdir = exec.workdir,
    ))
}

#[cfg(test)]
#[path = "firecracker_tests.rs"]
mod tests;
