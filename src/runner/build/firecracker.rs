//! Generic Firecracker runtime-build: convert ANY OCI image into a bootable
//! `rootfs.ext4` + a minimal PID-1 init, then hand it to the existing
//! [`crate::firecracker::FirecrackerRuntime`] (guest `172.31.0.2:8080`, kernel
//! `/opt/tabbify/vmlinux`, per-uuid pidfile + warm-snapshot). Invoked from the
//! `"firecracker"` arm of [`crate::build::build_runtime`] — this is a
//! RUNTIME-build helper, NOT the CI-build pipeline in the sibling `docker.rs` /
//! `wasm.rs` (clone → build → push).
//!
//! ## OCI → ext4 contract (CURRENT — docker-based)
//! The implementation in this file shells out to the LOCAL docker daemon. A
//! docker-less variant is PLANNED (see the "PLANNED / TARGET (fc-dl)" section
//! below); it is NOT yet implemented, so the functions named there do not exist.
//! 1. PULL (spec §7 step 1): `run_firecracker_build` pulls the deployed image
//!    from the mesh OCI registry by its `registry_ref` and tags it locally as
//!    `tbf-img-<uuid>-v<N>`, reusing the existing docker-pull argv builders
//!    (`docker::protocol::pull_args` + `tag_args` — the same `docker pull` +
//!    `docker tag` sequence as `docker::push::pull_and_tag`; see
//!    `pull_and_tag_image`). This MUST happen before the OCI-config read and
//!    export, both of which hit the LOCAL daemon. Skipped iff the rootfs is
//!    already cached by digest.
//! 2. CONVERT (cached by IMMUTABLE digest under `<data_dir>/apps/<uuid>/fc/
//!    <digest>/rootfs.ext4`):
//!    a. Read the image's OCI config via `docker inspect --format
//!    '{{json .Config}}'` (`read_oci_config`) — captured from STDOUT (no `-o`
//!    flag exists); translate ENTRYPOINT/CMD/ENV/WORKDIR into a PID-1 `/init`
//!    script (EXEC-FORM only — D3, see [`render_init`]).
//!    b. `docker create` + `docker export` the image filesystem → a flat tar →
//!    untar into a staging dir (ROOTLESS); inject `/init` before mkfs.
//!    c. `mkfs.ext4 -d <staging> rootfs.ext4` — populates a fresh ext4 from the
//!    staging contents with NO loop device and NO root (e2fsprogs ≥ 1.43).
//! 3. BOOT: `FirecrackerRuntime::launch_with_uuid` with the converted rootfs.
//!    The kernel `ip=` boot-arg already configures `eth0`/`172.31.0.2`; the
//!    init only verifies it, then `exec`s the image entrypoint so the same
//!    image that runs under `runtime=docker` also runs under
//!    `runtime=firecracker`.
//!
//! ## PLANNED / TARGET (fc-dl): docker-less OCI → ext4 (NOT IMPLEMENTED)
//! The goal is to drop the docker-daemon dependency from the FC path so a
//! bare-metal FC node needs only `oras` + `tar` + `mkfs.ext4`. The functions
//! named here (`pull_oci_layout`, `read_oci_config_from_layout`,
//! `unpack_oci_layers`) DO NOT EXIST YET — this section is the target contract,
//! not a description of the code in this file. When implemented it replaces the
//! docker steps above:
//! 1. PULL: `pull_oci_layout` would pull the deployed image from the mesh OCI
//!    registry into `<out_dir>/oci` as a spec-compliant OCI LAYOUT via the
//!    `oras copy <ref> --to-oci-layout <dir> --from-plain-http` argv form
//!    (`--from-plain-http` is the SOURCE flag for the plain-HTTP mesh registry,
//!    NOT `--plain-http`). This is the form the fc-dl-1 step-0a probe PROVED:
//!    `oras pull -o <dir>` (the `crate::oras::oras_pull_args` argv) does NOT
//!    produce a layout for a normal container image — `oras` skips layers
//!    lacking an `org.opencontainers.image.title` annotation (all docker-built
//!    layers) and leaves the dir EMPTY. The `--to-oci-layout` form yields the
//!    full layout: `oci-layout` + `index.json` + `blobs/<alg>/<hex>` for
//!    manifest+config+layers. See the "fc-dl-1 probe outcome" section below for
//!    the recorded evidence.
//! 2. CONFIG: `read_oci_config_from_layout` would read `index.json` → first
//!    image manifest descriptor → manifest blob → config-blob under
//!    `blobs/<alg>/<hex>` → typed [`oci_spec::image::ImageConfiguration`]. No
//!    `docker inspect`. ENTRYPOINT/CMD/ENV/WORKDIR → exec-form `/init` (D3).
//! 3. UNPACK: `unpack_oci_layers` would iterate the manifest's layer
//!    descriptors in order, `tar -xf <blob> -C <staging>` per layer, then apply
//!    OCI WHITEOUTS (`.wh.<name>` file-deletes + `.wh..wh..opq` opaque-dir
//!    clears) after each layer. The diff-id ↔ layer-blob mapping is validated
//!    against `config.rootfs().diff_ids()`.
//! 4. EXT4: `mkfs.ext4 -d <staging> rootfs.ext4` (rootless, loopless,
//!    e2fsprogs ≥ 1.43), with the exec-form `/init` injected BEFORE mkfs (D3).
//! 5. CACHE: keyed by the IMMUTABLE image digest (fc-3) — unchanged.
//! 6. BOOT: `FirecrackerRuntime::launch_with_uuid` with the converted rootfs.
//!
//! ## fc-dl-1 probe outcome (RECORDED — `oras` 1.3.2, real registry)
//! Step 0a probed `oras` against a real registry (docker.io busybox). RESULT:
//! `oras pull -o <dir>` (the form behind [`crate::oras::oras_pull_args`]) does
//! NOT produce a layout for a normal container image — `oras` skips layers that
//! lack an `org.opencontainers.image.title` annotation (all docker-built image
//! layers) and leaves the output dir EMPTY, printing:
//! "Skipped pulling layers without file name ... Use 'oras copy ...
//! --to-oci-layout' to pull all layers." The WORKING form is therefore the
//! documented FALLBACK: `oras copy <ref> --to-oci-layout <dir>`, which on a
//! single-platform digest-pinned ref yields a clean layout whose
//! `index.json.manifests[0]` is an `application/vnd.oci.image.manifest.v1+json`
//! (plus `oci-layout` + `blobs/sha256/<hex>` for manifest+config+layers).
//! NOTE: for a multi-arch TAG, `oras copy --to-oci-layout` leaves a top-level
//! image-INDEX as `manifests[0]`, so the PLANNED `read_oci_config_from_layout`
//! must select the image-manifest descriptor (not blindly take the first). For
//! the plain-HTTP mesh registry `oras copy` uses `--from-plain-http` (the
//! SOURCE), NOT `--plain-http`. CONSEQUENCE for the PLANNED fc-dl-2: the
//! `pull_oci_layout` would use the `oras copy ... --to-oci-layout` argv form
//! (not `oras pull -o`) to obtain a real layout for container images.
//!
//! ## Risks (spec §7)
//! - **OCI-config → init translation.** ENTRYPOINT/CMD/ENV/WORKDIR are mapped
//!   to an exec-form `/init`. Shell-form entrypoints (images that rely on a
//!   base-image shell) are DEFERRED (D3): [`render_init`] returns a clear error
//!   rather than guessing `/bin/sh -c`. USER, HEALTHCHECK, and signal semantics
//!   are NOT yet honoured.
//! - **Conversion latency.** export + untar + mkfs is seconds-to-minutes for a
//!   large image. Mitigated by (a) the digest-keyed rootfs cache here (a
//!   redeploy of an unchanged image skips conversion entirely) and (b) the
//!   existing FirecrackerRuntime warm-snapshot path (subsequent boots restore
//!   from `snap.mem`).
//! - **Image size vs ext4 sizing.** `mkfs.ext4 -d` needs the image to fit the
//!   sized ext4; we size from `runtime.memory_mb` padded over the unpacked
//!   size. An under-sized image fails mkfs loudly (better than a silently
//!   truncated rootfs). A large image inflates both conversion time and the
//!   on-disk cache.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};

use crate::runtime::BoxFut;

/// External-command seam for the OCI→ext4 conversion (`docker pull`,
/// `docker inspect`, `docker export`, `tar`, `mkfs.ext4`). Receives the full
/// argv (first element = program) and returns `(exit_ok, stdout_bytes)`:
/// `exit_ok` is `true` iff the process exits 0, and `stdout_bytes` is the
/// captured STDOUT (empty for commands whose output we ignore, populated for
/// `docker inspect` whose JSON config we parse — see `read_oci_config`).
///
/// `docker::CommandRunner` returns only `bool`; we widen to also carry stdout
/// because `docker inspect` writes its OCI config to STDOUT (it has NO `-o`
/// flag). Unit tests inject a fake runner that returns canned stdout for
/// `inspect` and side-effects the rootfs file for `mkfs.ext4`.
pub type FcBuildRunner =
    Arc<dyn Fn(Vec<String>) -> BoxFut<'static, (bool, Vec<u8>)> + Send + Sync>;

/// Name of the produced rootfs image inside the output dir.
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
// The production path (`resolve_rootfs`, fc-5) calls `build_rootfs_ext4_inner`
// with `Some(&init)` directly; this no-init (`None`) wrapper is exercised only
// by the fc-1 unit tests, hence still `#[allow(dead_code)]`.
#[allow(dead_code)]
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

/// On-disk cache path for an app's converted rootfs, keyed by the IMMUTABLE
/// image digest (`sha256:…`) — NOT the tag. The same digest always maps to the
/// same path, so a redeploy of an unchanged image skips the (slow) OCI→ext4
/// conversion entirely. A new digest gets a fresh dir, never clobbering the old
/// rootfs (immutable-by-content).
///
/// Layout mirrors the wasm `.cwasm` / fc snapshot caches:
/// `<data_dir>/apps/<uuid>/fc/<digest-sanitized>/rootfs.ext4`.
/// The `:` in the digest is replaced with `-` so it's a single path segment.
#[must_use]
pub fn cached_rootfs_path(data_dir: &Path, uuid: &str, digest: &str) -> PathBuf {
    let sanitized = digest.replace(':', "-");
    data_dir
        .join("apps")
        .join(uuid)
        .join("fc")
        .join(sanitized)
        .join(ROOTFS_NAME)
}

/// Is the digest-keyed rootfs already converted + on disk?
#[must_use]
pub fn rootfs_is_cached(data_dir: &Path, uuid: &str, digest: &str) -> bool {
    cached_rootfs_path(data_dir, uuid, digest).is_file()
}

/// The exec-form entrypoint distilled from an OCI image config.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Resolve the bootable rootfs for an app: cache-hit by digest (fc-3) → return
/// the cached `rootfs.ext4`; cache-miss → parse the OCI config, render the
/// PID-1 init (fc-2), and convert image → `rootfs.ext4` (fc-1) at the
/// digest-keyed path. Extracted from [`run_firecracker_build`] so the
/// cache/convert decision is unit-testable without a VM boot.
///
/// # Errors
/// OCI-config parse failure, shell-form entrypoint (D3), or conversion failure.
pub async fn resolve_rootfs(
    uuid: &str,
    fetched: &crate::fetcher::FetchedApp,
    digest: &str,
    data_dir: &Path,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    let cached = cached_rootfs_path(data_dir, uuid, digest);
    if rootfs_is_cached(data_dir, uuid, digest) {
        tracing::info!(
            uuid,
            digest,
            "firecracker rootfs cache hit; skipping conversion"
        );
        return Ok(cached);
    }

    let out_dir = cached
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cached rootfs path has no parent"))?
        .to_path_buf();

    // Image tag the conversion reads from. The registry-pull done by
    // `run_firecracker_build` (spec §7 step 1 — same pull+tag argv as
    // `docker::push::pull_and_tag`) has left the image present locally under
    // this versioned tag.
    let image_tag = crate::docker::protocol::versioned_image_tag(uuid, fetched.version);

    // Read OCI config from `docker inspect` STDOUT (no -o, no temp file), then
    // render the PID-1 init from its entrypoint (fc-2). The conversion itself
    // (export → untar → inject_init → mkfs) is the SINGLE shared primitive
    // `build_rootfs_ext4_inner` (fc-1) — we just pass `Some(&init)` so the init
    // is baked in. No re-inlined argv here; the argv shape has one source of
    // truth in fc-1.
    let oci = read_oci_config(&image_tag, runner).await?;
    let entry = Entrypoint::from_oci(&oci);
    let init = render_init(&entry)?; // shell-form → clear error (D3)

    build_rootfs_ext4_inner(
        &image_tag,
        &out_dir,
        fetched.manifest.runtime.memory_mb,
        Some(&init),
        runner,
    )
    .await
}

/// `docker inspect --format '{{json .Config}}' <tag>` → typed
/// [`oci_spec::image::ImageConfiguration`]. `docker inspect` writes its JSON to
/// STDOUT and has NO `-o` flag, so we capture the runner's returned stdout bytes
/// and parse those directly — we do NOT pass `-o` and we do NOT read a file off
/// disk. Production shells `docker inspect` and pipes stdout; tests inject a
/// fake runner that returns the canned config JSON as its stdout.
///
/// `{{json .Config}}` prints ONLY the image's execution config object
/// (`Entrypoint`/`Cmd`/`Env`/`WorkingDir`, PascalCase) — NOT a full OCI image
/// configuration (it carries no `architecture`/`os`/`rootfs`). So we parse it
/// into [`oci_spec::image::Config`] and wrap it in a default
/// [`oci_spec::image::ImageConfiguration`], which is what [`Entrypoint::from_oci`]
/// reads its entrypoint from.
///
/// Requires the image to be present locally — callers MUST pull it first
/// (see [`run_firecracker_build`], spec §7 step 1).
async fn read_oci_config(
    image_tag: &str,
    runner: &FcBuildRunner,
) -> Result<oci_spec::image::ImageConfiguration> {
    let (ok, stdout) = (runner)(vec![
        "docker".to_owned(),
        "inspect".to_owned(),
        "--format".to_owned(),
        "{{json .Config}}".to_owned(),
        image_tag.to_owned(),
    ])
    .await;
    if !ok {
        bail!("docker inspect of image {image_tag} (OCI config) failed");
    }
    let text = std::str::from_utf8(&stdout)
        .with_context(|| format!("docker inspect {image_tag}: stdout is not UTF-8"))?;
    let config: oci_spec::image::Config =
        serde_json::from_str(text).with_context(|| "parse OCI image config (.Config)")?;
    let mut image_config = oci_spec::image::ImageConfiguration::default();
    image_config.set_config(Some(config));
    Ok(image_config)
}

/// Resolve `<layout>/index.json` → first image-manifest descriptor → manifest
/// blob → config blob (`blobs/<alg>/<hex>`) → typed
/// [`oci_spec::image::ImageConfiguration`]. DOCKER-LESS replacement for the old
/// `docker inspect --format '{{json .Config}}'` path.
///
/// # Errors
/// Missing/garbled `index.json`, no image manifest, or an unreadable blob.
#[allow(dead_code)] // wired into resolve_rootfs by fc-dl-6
fn read_oci_config_from_layout(layout: &Path) -> Result<oci_spec::image::ImageConfiguration> {
    let index = oci_spec::image::ImageIndex::from_file(layout.join("index.json"))
        .with_context(|| format!("read OCI index.json under {}", layout.display()))?;
    let man_desc = index
        .manifests()
        .iter()
        .find(|d| matches!(d.media_type(), oci_spec::image::MediaType::ImageManifest))
        .or_else(|| index.manifests().first())
        .ok_or_else(|| anyhow::anyhow!("OCI index.json has no image manifest descriptor"))?;
    let manifest = oci_spec::image::ImageManifest::from_file(blob_path(layout, man_desc.digest()))
        .context("read OCI image manifest blob")?;
    let cfg = oci_spec::image::ImageConfiguration::from_file(blob_path(
        layout,
        manifest.config().digest(),
    ))
    .context("read OCI image config blob")?;
    Ok(cfg)
}

/// `blobs/<alg>/<hex>` path for a content-addressed [`oci_spec::image::Digest`].
#[allow(dead_code)] // used by read_oci_config_from_layout + unpack_oci_layers (fc-dl-4)
fn blob_path(layout: &Path, digest: &oci_spec::image::Digest) -> PathBuf {
    layout
        .join("blobs")
        .join(digest.algorithm().as_ref())
        .join(digest.digest())
}

/// Production [`FcBuildRunner`]: spawns `argv[0] argv[1..]`, captures STDOUT,
/// and returns `(exit_ok, stdout_bytes)`. STDOUT capture is required because
/// `docker inspect` writes its OCI config JSON there (it has no `-o` flag);
/// callers that ignore stdout (export/tar/mkfs) just drop the second tuple
/// element. Used by the `"firecracker"` arm in [`crate::build`].
#[must_use]
pub fn production_fc_build_runner() -> FcBuildRunner {
    use tokio::process::Command;
    Arc::new(move |argv: Vec<String>| {
        let fut: BoxFut<'static, (bool, Vec<u8>)> = Box::pin(async move {
            let Some((prog, rest)) = argv.split_first() else {
                return (false, Vec::new());
            };
            match Command::new(prog)
                .args(rest)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .output()
                .await
            {
                Ok(out) => (out.status.success(), out.stdout),
                Err(_) => (false, Vec::new()),
            }
        });
        fut
    })
}

/// Pull the deployed OCI image from the mesh registry and alias it under the
/// supervisor's versioned local tag, the same `docker pull <reff>` +
/// `docker tag <reff> <vtag>` sequence as [`crate::docker::push::pull_and_tag`].
///
/// CRITICAL — argv contract. The [`FcBuildRunner`] (and its production form,
/// [`production_fc_build_runner`]) spawns `Command::new(argv[0]).args(argv[1..])`,
/// so the program MUST be argv[0]. The `protocol::{pull_args, tag_args}` builders
/// return argv WITHOUT the binary (`["pull", reff]` / `["tag", reff, vtag]`)
/// because they're consumed by the docker module's `production_command_runner`,
/// which bakes `docker` in via `Command::new(docker_bin).args(args)`. Feeding
/// those raw into the FC runner would spawn nonexistent `pull`/`tag`
/// executables. So we prepend `"docker"` here to honour the FC runner contract.
///
/// # Errors
/// A failing `docker pull` or `docker tag`.
async fn pull_and_tag(reff: &str, vtag: &str, runner: &FcBuildRunner) -> Result<()> {
    // `protocol::pull_args(reff)` = `["pull", reff]`; prepend the binary so the
    // FcBuildRunner spawns `docker pull <reff>` (program at argv[0]).
    let mut pull = vec!["docker".to_owned()];
    pull.extend(crate::docker::protocol::pull_args(reff));
    let (pulled, _) = (runner)(pull).await;
    if !pulled {
        bail!("docker pull of registry_ref {reff:?} from mesh registry failed");
    }

    // `protocol::tag_args(reff, vtag)` = `["tag", reff, vtag]`; prepend the
    // binary so the FcBuildRunner spawns `docker tag <reff> <vtag>`.
    let mut tag = vec!["docker".to_owned()];
    tag.extend(crate::docker::protocol::tag_args(reff, vtag));
    let (tagged, _) = (runner)(tag).await;
    if !tagged {
        bail!("docker tag {reff:?} -> {vtag} failed");
    }
    Ok(())
}

/// Pull the deployed OCI image from the mesh registry into `<out_dir>/oci` as a
/// spec-compliant OCI LAYOUT (`oci-layout` + `index.json` + `blobs/<alg>/<hex>`),
/// DOCKER-LESS, via the existing `oras` seam. Replaces the old `docker pull` +
/// `docker tag`.
///
/// CRITICAL — argv contract: [`FcBuildRunner`] spawns `Command::new(argv[0])`,
/// so the program MUST be argv[0]. `crate::oras::oras_copy_to_oci_layout_args`
/// returns argv WITHOUT the binary
/// (`["copy", "--from-plain-http", reff, "--to-oci-layout", dir]`), so we
/// prepend `"oras"`.
///
/// CRITICAL — why `oras copy --to-oci-layout`, NOT `oras pull -o`: the fc-dl-1
/// probe (recorded in this file's header) PROVED that `oras pull -o <dir>` (the
/// `crate::oras::oras_pull_args` argv) does NOT produce a layout for a normal
/// docker-built container image — `oras` skips every layer lacking an
/// `org.opencontainers.image.title` annotation and leaves `<dir>` EMPTY
/// (`"Skipped pulling layers without file name ... Use 'oras copy ...
/// --to-oci-layout'"`). An empty layout would silently break the downstream
/// `read_oci_config_from_layout` / `unpack_oci_layers`. The `oras copy
/// --to-oci-layout` form yields the full layout. For the plain-HTTP mesh
/// registry the SOURCE flag is `--from-plain-http`, NOT `--plain-http`.
///
/// Returns the layout directory (`<out_dir>/oci`) for the config-read + unpack.
///
/// # Errors
/// A failing `oras copy`.
#[allow(dead_code)] // wired into run_firecracker_build by fc-dl-6
async fn pull_oci_layout(reff: &str, out_dir: &Path, runner: &FcBuildRunner) -> Result<PathBuf> {
    let layout = out_dir.join("oci");
    tokio::fs::create_dir_all(&layout)
        .await
        .with_context(|| format!("create oci layout dir {}", layout.display()))?;
    let mut argv = vec!["oras".to_owned()];
    argv.extend(crate::oras::oras_copy_to_oci_layout_args(
        reff,
        &layout.to_string_lossy(),
    ));
    let (ok, _) = (runner)(argv).await;
    if !ok {
        bail!("oras copy of registry_ref {reff:?} into OCI layout failed");
    }
    Ok(layout)
}

/// Entry point for the `"firecracker"` arm of [`crate::build::build_runtime`]:
/// resolve (cache or convert) the rootfs, then boot it via the existing
/// `FirecrackerRuntime` contract (guest `172.31.0.2:8080`, kernel
/// `/opt/tabbify/vmlinux`, per-uuid pidfile + warm-snapshot).
///
/// # Errors
/// Conversion failure (see [`resolve_rootfs`]) or a VM launch failure.
pub async fn run_firecracker_build(
    uuid: &str,
    fetched: &crate::fetcher::FetchedApp,
    fc: &crate::config::FcConfig,
    data_dir: &Path,
    runner: &FcBuildRunner,
) -> Result<std::sync::Arc<dyn crate::runtime::AppRuntime>> {
    // The deployed image ref carries the immutable digest after the `@`.
    let reff = fetched
        .manifest
        .runtime
        .registry_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!("firecracker runtime requires a registry_ref (image to convert)")
        })?;
    let digest = reff.rsplit_once('@').map(|(_, d)| d).ok_or_else(|| {
        anyhow::anyhow!(
            "registry_ref {reff:?} has no @<digest>; need an immutable digest for the rootfs cache"
        )
    })?;

    // Spec §7 step 1: PULL the OCI image from the mesh registry FIRST. Both the
    // OCI-config read (`docker inspect`) and the rootfs export (`docker export`)
    // operate on the LOCAL daemon, so the image must be present locally before
    // either runs. The local versioned tag matches what `resolve_rootfs` reads
    // from (`versioned_image_tag(uuid, version)`).
    // (Skipped iff the rootfs is already cached by digest — fc-3.)
    if !rootfs_is_cached(data_dir, uuid, digest) {
        let vtag = crate::docker::protocol::versioned_image_tag(uuid, fetched.version);
        pull_and_tag(reff, &vtag, runner).await?;
    }

    let rootfs = resolve_rootfs(uuid, fetched, digest, data_dir, runner).await?;

    let vm = crate::firecracker::FirecrackerRuntime::launch_with_uuid(
        &rootfs,
        &fetched.manifest.runtime,
        fc,
        uuid,
        data_dir,
    )
    .await?;
    Ok(std::sync::Arc::new(vm))
}

#[cfg(test)]
#[path = "firecracker_tests.rs"]
mod tests;
