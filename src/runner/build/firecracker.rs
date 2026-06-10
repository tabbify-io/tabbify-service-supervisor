//! Generic Firecracker runtime-build: convert ANY OCI image into a bootable
//! `rootfs.ext4` + a minimal PID-1 init, then hand it to the existing
//! [`crate::firecracker::FirecrackerRuntime`] (guest `172.31.0.2:8080`, kernel
//! `/opt/tabbify/vmlinux`, per-uuid pidfile + warm-snapshot). Invoked from the
//! `"firecracker"` arm of [`crate::build::build_runtime`] — this is a
//! RUNTIME-build helper, NOT the CI-build pipeline in the sibling `docker.rs` /
//! `wasm.rs` (clone → build → push).
//!
//! ## OCI → ext4 contract (CURRENT — docker-less)
//! The conversion path no longer shells the LOCAL docker daemon: a bare-metal
//! FC node needs only `oras` + `tar` + `mkfs.ext4`. On a cache miss
//! `run_firecracker_build` pulls the layout (`pull_oci_layout`) and reads its
//! config (`read_oci_config_from_layout`), then hands `layout`/`config` to
//! `resolve_rootfs` for the conversion.
//! 1. PULL: `pull_oci_layout` pulls the deployed image from the mesh OCI
//!    registry into `<out_dir>/oci` as a spec-compliant OCI LAYOUT via the
//!    `oras copy --from-plain-http <ref> --to-oci-layout <dir>` argv form
//!    (`--from-plain-http` is the SOURCE flag for the plain-HTTP mesh registry,
//!    NOT `--plain-http`). This is the form the fc-dl-1 step-0a probe PROVED:
//!    `oras pull -o <dir>` (the now-removed `oras pull` form) does NOT
//!    produce a layout for a normal container image — `oras` skips layers
//!    lacking an `org.opencontainers.image.title` annotation (all docker-built
//!    layers) and leaves the dir EMPTY. The `--to-oci-layout` form yields the
//!    full layout: `oci-layout` + `index.json` + `blobs/<alg>/<hex>` for
//!    manifest+config+layers. See the "fc-dl-1 probe outcome" section below for
//!    the recorded evidence. Skipped iff the rootfs is already cached by digest.
//! 2. CONFIG: `read_oci_config_from_layout` reads `index.json` → first image
//!    manifest descriptor → manifest blob → config-blob under
//!    `blobs/<alg>/<hex>` → typed [`oci_spec::image::ImageConfiguration`]. No
//!    `docker inspect`. ENTRYPOINT/CMD/ENV/WORKDIR → exec-form `/init` (D3, see
//!    [`render_init`]).
//! 3. UNPACK: `unpack_oci_layers` iterates the manifest's layer descriptors in
//!    order, `tar -xf <blob>` per layer (shelled host `tar`, no daemon and no
//!    `docker create`/`export`), then applies OCI WHITEOUTS (`.wh.<name>`
//!    file-deletes + `.wh..wh..opq` opaque-dir clears). The diff-id ↔ layer-blob
//!    mapping is validated against `config.rootfs().diff_ids()`.
//! 4. EXT4: `mkfs.ext4 -d <staging> rootfs.ext4` — populates a fresh ext4 from
//!    the staging contents with NO loop device and NO root (e2fsprogs ≥ 1.43),
//!    with the exec-form `/init` injected BEFORE mkfs (D3).
//! 5. CACHE: keyed by the IMMUTABLE image digest (fc-3), under
//!    `<data_dir>/apps/<uuid>/fc/<digest>/rootfs.ext4`. A redeploy of an
//!    unchanged image skips the pull and conversion entirely.
//! 6. BOOT: `FirecrackerRuntime::launch_with_uuid` with the converted rootfs.
//!    The kernel `ip=` boot-arg already configures `eth0`/`172.31.0.2`; the
//!    init only verifies it, then `exec`s the image entrypoint so the same
//!    image that runs under `runtime=docker` also runs under
//!    `runtime=firecracker`.
//!
//! ## fc-dl-1 probe outcome (RECORDED — `oras` 1.3.2, real registry)
//! Step 0a probed `oras` against a real registry (docker.io busybox). RESULT:
//! `oras pull -o <dir>` (the now-removed `oras pull` form) does
//! NOT produce a layout for a normal container image — `oras` skips layers that
//! lack an `org.opencontainers.image.title` annotation (all docker-built image
//! layers) and leaves the output dir EMPTY, printing:
//! "Skipped pulling layers without file name ... Use 'oras copy ...
//! --to-oci-layout' to pull all layers." The WORKING form is therefore
//! `oras copy <ref> --to-oci-layout <dir>`, which on a single-platform
//! digest-pinned ref yields a clean layout whose `index.json.manifests[0]` is an
//! `application/vnd.oci.image.manifest.v1+json` (plus `oci-layout` +
//! `blobs/sha256/<hex>` for manifest+config+layers).
//! NOTE: for a multi-arch TAG, `oras copy --to-oci-layout` leaves a top-level
//! image-INDEX as `manifests[0]`, so `read_oci_config_from_layout` selects the
//! image-manifest descriptor (it does not blindly take the first). For the
//! plain-HTTP mesh registry `oras copy` uses `--from-plain-http` (the SOURCE),
//! NOT `--plain-http`.
//!
//! ## Risks (spec §7)
//! - **OCI-config → init translation.** ENTRYPOINT/CMD/ENV/WORKDIR are mapped
//!   to an exec-form `/init`. Shell-form entrypoints (images that rely on a
//!   base-image shell) are DEFERRED (D3): [`render_init`] returns a clear error
//!   rather than guessing `/bin/sh -c`. USER, HEALTHCHECK, and signal semantics
//!   are NOT yet honoured.
//! - **Conversion latency.** pull + untar + mkfs is seconds-to-minutes for a
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

/// External-command seam for the DOCKER-LESS OCI→ext4 conversion (`oras copy`,
/// `tar`, `mkfs.ext4`). Receives the full argv (first element = program) and
/// returns `(exit_ok, stdout_bytes)`: `exit_ok` is `true` iff the process exits
/// 0, and `stdout_bytes` is the captured STDOUT. The OCI config is now read from
/// the pulled layout on disk (`read_oci_config_from_layout`), not from a
/// command's STDOUT, so all current callers ignore the second tuple element; the
/// stdout slot is retained for the seam's shape. Unit tests inject a fake runner
/// that side-effects the rootfs file for `mkfs.ext4` and no-ops `oras copy`.
pub type FcBuildRunner = Arc<dyn Fn(Vec<String>) -> BoxFut<'static, (bool, Vec<u8>)> + Send + Sync>;

/// Name of the produced rootfs image inside the output dir.
const ROOTFS_NAME: &str = "rootfs.ext4";

/// Map the HOST CPU architecture (`std::env::consts::ARCH`) to the OCI image
/// architecture name that [`oci_spec::image::Arch`] uses
/// (`x86_64 -> amd64`, `aarch64 -> arm64`). Any other host falls back to the
/// raw `ARCH` string so the guard can still report it verbatim. This is the
/// value the architecture guard compares `config.architecture()` against.
#[must_use]
fn host_oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// Fail FAST when an OCI image's declared architecture does not match the host.
///
/// A Firecracker guest can only execute binaries of the host CPU architecture,
/// so converting (the slow `oras` pull + layer unpack + `mkfs.ext4`) and then
/// booting a cross-arch image is doomed. This guard runs BEFORE any of that —
/// the moment the typed config is available — so a mismatch surfaces a clear
/// error naming BOTH the image arch and the host arch instead of wasting the
/// conversion and failing opaquely at boot. The image architecture is always
/// logged at `info` for diagnostics.
///
/// # Errors
/// The image's `config.architecture()` (mapped to its OCI name) differs from
/// [`host_oci_arch`].
fn guard_arch_matches_host(config: &oci_spec::image::ImageConfiguration) -> Result<()> {
    let image_arch = config.architecture().to_string();
    let host_arch = host_oci_arch();
    tracing::info!(
        image_arch = %image_arch,
        host_arch,
        "firecracker rootfs build: OCI image architecture"
    );
    if image_arch != host_arch {
        bail!(
            "OCI image architecture {image_arch:?} does not match host architecture \
             {host_arch:?}; a firecracker guest can only run host-architecture \
             binaries — deploy an image built for {host_arch:?}"
        );
    }
    Ok(())
}

/// Convert an OCI image (already pulled as a LAYOUT under `layout`, with its
/// typed `config` read from that layout) into a bootable `rootfs.ext4` under
/// `out_dir`, ROOTLESS and LOOPLESS, DOCKER-LESS.
///
/// ## OCI → ext4 contract (see fc-8 for the full risk write-up)
/// 1. `unpack_oci_layers` untars the image's layers (in manifest order) into a
///    staging dir, whiteout-aware (`.wh.<name>` + `.wh..wh..opq`) — no docker
///    `create`/`export`, no daemon, no overlay.
/// 2. `mkfs.ext4 -d <staging> <out_dir>/rootfs.ext4` — the `-d` flag populates a
///    fresh ext4 image from the staging dir's contents WITHOUT a loop device
///    and WITHOUT root (e2fsprogs ≥ 1.43). This is the crux of the rootless
///    path: no `mount`, no `losetup`, no `sudo`.
///
/// `size_mib` sizes the ext4 image; callers pad it over the unpacked size.
///
/// # Errors
/// A failing layer untar, a layer/diff_id mismatch, or a failing `mkfs.ext4`.
// The production path (`resolve_rootfs`, fc-5) calls `build_rootfs_ext4_inner`
// with `Some(&init)` directly; this no-init (`None`) wrapper is exercised only
// by the fc-1 unit tests, hence still `#[allow(dead_code)]`.
#[allow(dead_code)]
pub async fn build_rootfs_ext4(
    layout: &Path,
    config: &oci_spec::image::ImageConfiguration,
    out_dir: &Path,
    size_mib: u32,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    // fc-1 is the no-init form: unpack → mkfs with nothing injected. The fc-5
    // init path calls the same primitive with `Some(init)`. Keeping a single
    // primitive means the unpack/mkfs argv shape has ONE source of truth (no
    // drift between fc-1 and fc-5).
    build_rootfs_ext4_inner(layout, config, out_dir, size_mib, None, runner).await
}

/// Sum the on-disk byte size and entry count of a staging tree. Used to size
/// the ext4 image and its inode table from real content rather than a guess.
/// Symlinks are counted but not followed (their own size, not the target).
/// The blocking walk runs on a dedicated thread.
async fn measure_tree(root: &Path) -> Result<(u64, u64)> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut bytes = 0u64;
        let mut count = 0u64;
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in rd.flatten() {
                count += 1;
                // `DirEntry::metadata` does NOT follow symlinks — exactly what
                // we want (count the link, not its target).
                let Ok(md) = entry.metadata() else {
                    continue;
                };
                if md.is_dir() {
                    stack.push(entry.path());
                } else {
                    bytes += md.len();
                }
            }
        }
        (bytes, count)
    })
    .await
    .context("measure staging tree (join)")
}

/// Compute `(ext4_size_mib, inode_count)` from staged content. Pure + isolated
/// so it is unit-testable. Size = 1.5× content + 512 MiB slack (ext4 journal,
/// metadata, write headroom), never below the caller's hint. Inodes = 2× the
/// file count, floored at 262144 (double the e2fsprogs default density) — a
/// dind tree's small-file count, inflated further by cross-layer hardlink
/// splitting in `merge_layer`, overruns the default inode table otherwise.
fn ext4_geometry(content_bytes: u64, file_count: u64, size_hint_mib: u32) -> (u32, u64) {
    let content_mib = u32::try_from(content_bytes / (1024 * 1024)).unwrap_or(u32::MAX);
    let effective_mib = size_hint_mib.max(content_mib.saturating_mul(3) / 2 + 512);
    let inodes = file_count.saturating_mul(2).max(262_144);
    (effective_mib, inodes)
}

/// Shared OCI→ext4 primitive — the SINGLE source of truth for the
/// unpack → (optional init inject) → `mkfs.ext4 -d` sequence.
///
/// `init`:
/// - `None`  → fc-1 form (no PID-1 init written; raw image filesystem),
/// - `Some(s)` → fc-5 form: write `s` to `<staging>/init` (mode 0755) AFTER the
///   unpack and BEFORE `mkfs.ext4` so the rendered PID-1 init is baked in.
///
/// Both [`build_rootfs_ext4`] (fc-1) and the fc-5 conversion call this; neither
/// re-inlines the argv, so the shape can never drift.
///
/// # Errors
/// A failing layer untar, a layer/diff_id mismatch, init-write failure, or a
/// failing `mkfs.ext4`.
async fn build_rootfs_ext4_inner(
    layout: &Path,
    config: &oci_spec::image::ImageConfiguration,
    out_dir: &Path,
    size_mib: u32,
    init: Option<&str>,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(out_dir)
        .await
        .with_context(|| format!("create rootfs out dir {}", out_dir.display()))?;
    let staging = out_dir.join("stage");

    // 1+2. DOCKER-LESS: untar the OCI layout's layers into the staging dir,
    //      whiteout-aware (replaces `docker create` + `docker export` + flat untar).
    unpack_oci_layers(layout, config, &staging).await?;

    // 2c. fc-5 only: inject the rendered PID-1 init AFTER unpack, BEFORE mkfs.
    if let Some(script) = init {
        inject_init(&staging, script).await?;
    }

    // 3. Size the ext4 from the ACTUAL staged content, not the caller's hint
    //    (which is the guest RAM size — unrelated to disk need). A large dind
    //    rootfs with tens of thousands of small files exhausts both the byte
    //    budget AND the default inode table, which is why `mkfs.ext4 -d` failed
    //    intermittently. We measure the staged tree and provision explicitly.
    let (content_bytes, file_count) = measure_tree(&staging).await?;
    let (effective_mib, inodes) = ext4_geometry(content_bytes, file_count, size_mib);
    tracing::info!(
        content_bytes,
        file_count,
        effective_mib,
        inodes,
        size_hint_mib = size_mib,
        "fc build: sizing ext4 from staged content"
    );

    //    Pre-size the backing image to `effective_mib` MiB (sparse `set_len`,
    //    no loop device, no root), then `mkfs.ext4 -F -m 0 -N <inodes> -d
    //    <staging> <out>` formats the existing file in place. The fs-size
    //    positional is OMITTED so the OUTPUT path stays the final argv element.
    //    `-m 0` reclaims the 5% root-reserved blocks a single-purpose rootfs
    //    does not need; `-N` pins the inode count to the real file count.
    // ATOMIC publish: mkfs into a per-process temp on the SAME dir, then
    // rename onto the final name only on success. A crashed/killed conversion
    // therefore never leaves a PARTIAL rootfs.ext4 that a later run would
    // treat as a valid digest-cache hit. rename(2) within one dir is atomic.
    let rootfs = out_dir.join(ROOTFS_NAME);
    let tmp = out_dir.join(format!(".{ROOTFS_NAME}.{}.tmp", std::process::id()));
    {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .await
            .with_context(|| format!("create rootfs image {}", tmp.display()))?;
        file.set_len(u64::from(effective_mib) * 1024 * 1024)
            .await
            .with_context(|| {
                format!("size rootfs image {} to {effective_mib}MiB", tmp.display())
            })?;
    }
    let (made, _) = (runner)(vec![
        "mkfs.ext4".to_owned(),
        "-F".to_owned(), // overwrite the pre-sized image without prompting
        "-m".to_owned(),
        "0".to_owned(), // no reserved-for-root blocks
        "-N".to_owned(),
        inodes.to_string(), // explicit inode table sized to the content
        "-d".to_owned(),
        staging.to_string_lossy().into_owned(),
        tmp.to_string_lossy().into_owned(),
    ])
    .await;
    if !made {
        let _ = tokio::fs::remove_file(&tmp).await;
        bail!(
            "mkfs.ext4 -d failed for OCI layout {} (sized {effective_mib}MiB, {inodes} inodes for {file_count} files / {content_bytes} bytes; see preceding 'command failed' log for the e2fsprogs diagnostic)",
            layout.display()
        );
    }
    if !tmp.is_file() {
        bail!(
            "mkfs.ext4 reported success but {} is missing",
            tmp.display()
        );
    }
    tokio::fs::rename(&tmp, &rootfs)
        .await
        .with_context(|| format!("atomically publish rootfs {}", rootfs.display()))?;
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
/// Layout mirrors the fc snapshot cache:
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

/// The per-app TAG-ref pull work dir (`<data_dir>/apps/<uuid>/fc/.pull`), CLEARED
/// before returning so the OCI layout `oras copy --to-oci-layout` writes contains
/// ONLY the current tag's manifest.
///
/// CRITICAL: this dir is reused across deploys. `oras copy --to-oci-layout` into
/// a DIRTY layout ACCUMULATES manifests in `index.json`, and
/// [`read_manifest_digest_from_layout`] reads `manifests[0]` — the OLDEST. So
/// without clearing, a redeploy resolved the tag to the FIRST-ever digest,
/// [`rootfs_is_cached`] hit the stale rootfs, and the app served its original
/// version forever (a new `git push` "deployed" but never changed). Clearing the
/// dir each time makes the digest resolve to the tag's CURRENT image.
async fn fresh_tag_pull_dir(data_dir: &Path, uuid: &str) -> Result<PathBuf> {
    let work = data_dir.join("apps").join(uuid).join("fc").join(".pull");
    // Best-effort: a missing dir (first deploy) is success, not an error.
    tokio::fs::remove_dir_all(&work).await.ok();
    Ok(work)
}

/// The digest-keyed work dir for a digest `registry_ref` — the parent of
/// [`cached_rootfs_path`], i.e. where the OCI layout is pulled and the converted
/// `rootfs.ext4` lands. Only valid once the digest is known (digest refs); a TAG
/// ref pulls into a digest-INDEPENDENT `.pull` dir first (see
/// [`run_firecracker_build`]).
fn digest_work_dir(data_dir: &Path, uuid: &str, digest: &str) -> Result<PathBuf> {
    Ok(cached_rootfs_path(data_dir, uuid, digest)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cached rootfs path has no parent"))?
        .to_path_buf())
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

/// POSIX single-quote a string so the `/bin/sh` running `/init` reads it back as
/// a SINGLE literal token — no word-splitting, glob, `$`-expansion, or quote
/// removal. The whole value is wrapped in single quotes; an embedded `'` is
/// escaped with the standard `'\''` idiom (close-quote, escaped literal quote,
/// reopen-quote). Used for argv elements, env values, and the workdir path so a
/// space / `*` / `?` / `$` / quote in any of them survives intact (FIX 1).
fn sh_single_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Render the guest PID-1 init script from an [`Entrypoint`].
///
/// The script (run as PID 1 by the kernel `init=/init` arg) mounts the pseudo
/// filesystems, verifies `eth0` (the kernel `ip=` boot-arg already configured
/// it to `172.31.0.2` per the existing `FirecrackerRuntime` contract), exports
/// the OCI env, ensures the workdir exists and cd's into it, then `exec`s the
/// entrypoint argv so the app server becomes PID 1's successor.
///
/// All shell-interpolated values (argv elements, env VALUES, and the workdir
/// path) are POSIX single-quoted via [`sh_single_quote`] so the re-tokenizing
/// `/bin/sh` reconstructs the EXACT bytes — a space / glob / `$` / quote can no
/// longer mis-execute the entrypoint (FIX 1).
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

    // Build the exec argv: entrypoint ++ cmd. Each element is single-quoted so
    // the shell re-tokenizes the line back to the EXACT argv (no word-splitting,
    // glob, or `$`-expansion) — exec-form semantics through a `/bin/sh` PID 1.
    let mut argv = exec.entrypoint.clone();
    argv.extend(exec.cmd.iter().cloned());
    let exec_line = argv
        .iter()
        .map(|a| sh_single_quote(a))
        .collect::<Vec<_>>()
        .join(" ");

    // `export KEY=<single-quoted value>`: split only on the FIRST `=` so values
    // containing `=` stay intact, and single-quote the value so a space / glob /
    // `$` does not get re-interpreted by the shell. A malformed entry without an
    // `=` is exported as-is (best effort).
    let env_lines: String = exec
        .env
        .iter()
        .map(|kv| match kv.split_once('=') {
            Some((key, value)) => format!("export {key}={}\n", sh_single_quote(value)),
            None => format!("export {kv}\n"),
        })
        .collect();

    // POSIX sh init. Minimal OCI images (e.g. busybox) ship NO /proc /sys /dev
    // mountpoints — a container runtime normally provides them — so `mkdir -p`
    // them first, then mount BEST-EFFORT: a missing pseudo-fs (or already-mounted)
    // must NEVER kill PID 1 and panic the kernel (FIX 4: ANY image must boot, not
    // just fat ones that happen to carry the mountpoints).
    // OCI/Docker auto-create a missing WorkingDir; with `set -e` a bare `cd` into
    // an unmaterialized workdir would kill PID 1, so `mkdir -p` it first (FIX 3).
    let workdir = sh_single_quote(&exec.workdir);
    Ok(format!(
        "#!/bin/sh\n\
         set -e\n\
         mkdir -p /proc /sys /dev\n\
         mount -t proc proc /proc 2>/dev/null || true\n\
         mount -t sysfs sysfs /sys 2>/dev/null || true\n\
         mount -t devtmpfs devtmpfs /dev 2>/dev/null || mount -t tmpfs tmpfs /dev 2>/dev/null || true\n\
         # eth0 is configured by the kernel ip= boot-arg; verify it came up.\n\
         if [ ! -e /sys/class/net/eth0 ]; then\n\
         \techo 'tabbify-init: eth0 missing (kernel ip= boot-arg did not configure it)' >&2\n\
         fi\n\
         ip link show eth0 >/dev/null 2>&1 || true\n\
         {env_lines}\
         mkdir -p {workdir} 2>/dev/null; cd {workdir}\n\
         exec {exec_line}\n",
    ))
}

/// Append deploy-time `extra` entries to `oci_env` AFTER the OCI image's own
/// `config.Env` entries. `render_init` emits the env as `export KEY='value'`
/// lines in order and POSIX sh honours the LAST definition of a variable, so
/// deploy-time values win on key collision. This is the SINGLE merge primitive
/// [`resolve_rootfs`] uses — tests exercise this exact production path.
pub fn merge_extra_env(
    oci_env: &mut Vec<String>,
    extra: &std::collections::HashMap<String, String>,
) {
    oci_env.extend(extra.iter().map(|(k, v)| format!("{k}={v}")));
}

/// Resolve the bootable rootfs for an app: cache-hit by digest (fc-3) → return
/// the cached `rootfs.ext4`; cache-miss → render the PID-1 init (fc-2) from the
/// already-read OCI config and convert the image → `rootfs.ext4` (fc-1) at the
/// digest-keyed path. Extracted from [`run_firecracker_build`] so the
/// cache/convert decision is unit-testable without a VM boot.
///
/// DOCKER-LESS: the image is pulled as an OCI LAYOUT and its config is read FROM
/// that layout by the caller ([`run_firecracker_build`]); `layout`/`config` are
/// passed in here. On a cache hit neither is read.
///
/// `extra_env` is merged AFTER the OCI `config.Env` entries so deploy-time
/// values win on key collision (POSIX: last `export KEY=…` wins in the init
/// script). When `None` the guest gets exactly the OCI image's env.
///
/// CACHE CONSTRAINT: the rootfs cache is keyed by `(uuid, digest)` —
/// `cached_rootfs_path(data_dir, uuid, digest)`. Because `extra_env` is baked
/// into the rootfs, changing env on the SAME uuid+digest would NOT rebuild the
/// cache. In practice this is acceptable: a devbox always gets a fresh uuid on
/// creation, and a dev-session also creates a fresh uuid, so the uuid+digest
/// pair is always unique per deploy-time env configuration.
///
/// # Errors
/// Shell-form entrypoint (D3) or conversion failure.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_rootfs(
    uuid: &str,
    fetched: &crate::fetcher::FetchedApp,
    layout: &Path,
    config: &oci_spec::image::ImageConfiguration,
    digest: &str,
    data_dir: &Path,
    runner: &FcBuildRunner,
    extra_env: Option<&std::collections::HashMap<String, String>>,
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

    // Fail FAST on an architecture mismatch BEFORE the slow unpack + mkfs: a
    // firecracker guest can only run host-architecture binaries, so a cross-arch
    // image is rejected here rather than after a wasted conversion. Also logs the
    // image architecture at info.
    guard_arch_matches_host(config)?;

    let out_dir = cached
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cached rootfs path has no parent"))?
        .to_path_buf();

    // DOCKER-LESS: config already read from the OCI layout (no `docker inspect`).
    // Render the PID-1 init from its entrypoint (fc-2); the conversion itself
    // (unpack → inject_init → mkfs) is the SINGLE shared primitive
    // `build_rootfs_ext4_inner` (fc-1) — we just pass `Some(&init)` so the init
    // is baked in.
    let mut entry = Entrypoint::from_oci(config);
    // Merge deploy-time extra env AFTER the OCI config.Env (see
    // [`merge_extra_env`] for the collision contract).
    if let (Some(map), Entrypoint::Exec(exec)) = (extra_env, &mut entry) {
        merge_extra_env(&mut exec.env, map);
    }
    let init = render_init(&entry)?; // shell-form → clear error (D3)

    build_rootfs_ext4_inner(
        layout,
        config,
        &out_dir,
        fetched.manifest.runtime.memory_mb,
        Some(&init),
        runner,
    )
    .await
}

/// Resolve `<layout>/index.json` → first image-manifest descriptor → manifest
/// blob → config blob (`blobs/<alg>/<hex>`) → typed
/// [`oci_spec::image::ImageConfiguration`]. DOCKER-LESS replacement for the old
/// `docker inspect --format '{{json .Config}}'` path.
///
/// # Errors
/// Missing/garbled `index.json`, no image manifest, or an unreadable blob.
fn read_oci_config_from_layout(layout: &Path) -> Result<oci_spec::image::ImageConfiguration> {
    let man_desc = read_manifest_descriptor_from_layout(layout)?;
    let manifest = oci_spec::image::ImageManifest::from_file(blob_path(layout, man_desc.digest()))
        .context("read OCI image manifest blob")?;
    let cfg = oci_spec::image::ImageConfiguration::from_file(blob_path(
        layout,
        manifest.config().digest(),
    ))
    .context("read OCI image config blob")?;
    Ok(cfg)
}

/// Resolve `<layout>/index.json` → the first image-manifest descriptor. Shared
/// by [`read_oci_config_from_layout`] (which then reads the manifest+config
/// blobs) and [`read_manifest_digest_from_layout`] (which only needs the
/// descriptor's digest) so the index-parsing rule lives in ONE place (DRY).
///
/// # Errors
/// Missing/garbled `index.json` or an index with no manifest descriptor.
fn read_manifest_descriptor_from_layout(layout: &Path) -> Result<oci_spec::image::Descriptor> {
    let index = oci_spec::image::ImageIndex::from_file(layout.join("index.json"))
        .with_context(|| format!("read OCI index.json under {}", layout.display()))?;
    index
        .manifests()
        .iter()
        .find(|d| matches!(d.media_type(), oci_spec::image::MediaType::ImageManifest))
        .or_else(|| index.manifests().first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("OCI index.json has no image manifest descriptor"))
}

/// Derive the IMMUTABLE image digest (`sha256:…`) from a pulled OCI layout:
/// `index.json → manifests[0].digest`. This is the content-addressed digest of
/// the image manifest blob, i.e. the same digest a registry resolves a TAG to.
///
/// Used by [`run_firecracker_build`] for a TAG `registry_ref` (no `@<digest>`):
/// the digest is unknown until the layout is pulled, so we pull first and read
/// the digest from the layout — then key the rootfs cache by it (fc-3). The
/// descriptor resolved here is the SAME one [`read_oci_config_from_layout`]
/// walks for the config blob, so the cache key and the converted config agree.
///
/// # Errors
/// Missing/garbled `index.json` or an index with no manifest descriptor.
fn read_manifest_digest_from_layout(layout: &Path) -> Result<String> {
    Ok(read_manifest_descriptor_from_layout(layout)?
        .digest()
        .to_string())
}

/// `blobs/<alg>/<hex>` path for a content-addressed [`oci_spec::image::Digest`].
fn blob_path(layout: &Path, digest: &oci_spec::image::Digest) -> PathBuf {
    layout
        .join("blobs")
        .join(digest.algorithm().as_ref())
        .join(digest.digest())
}

/// Map a layer descriptor's media type to the explicit host-`tar` decompression
/// flag (FIX 4). Real container layers are gzip- or zstd-compressed; relying on
/// `tar` autodetect breaks on busybox / older tar (notably for zstd), so the flag
/// is selected from the media type, NOT guessed by the archiver.
///
/// Matches by media-type STRING so both the OCI spellings (`…tar+gzip` /
/// `…tar+zstd`, which `oci-spec` types as [`MediaType::ImageLayerGzip`] /
/// [`MediaType::ImageLayerZstd`]) AND the Docker v2s2 spellings real images ship
/// with (`…rootfs.diff.tar.gzip` / `.zstd`, which `oci-spec` types as
/// [`MediaType::Other`]) resolve correctly.
///
/// Extract one OCI layer blob into `dest` IN-PROCESS via the `tar` crate.
///
/// We do NOT shell the host `tar`: the runner's PATH resolves busybox tar on
/// NixOS / Alpine, and busybox tar strips the leading `/` from ABSOLUTE symlink
/// targets (so `/bin/sh -> /bin/busybox` lands as the broken `bin/busybox` ->
/// `/bin/bin/busybox`), which then breaks the guest `/init` (`#!/bin/sh`) with
/// "No working init found". The `tar` crate writes symlink targets verbatim and
/// is portable (no dependency on which `tar` binary is on PATH). Decompression
/// is chosen from the layer media type (gzip / zstd / plain), not autodetected.
///
/// Synchronous (std fs/IO) — call from a blocking task.
fn extract_layer_blob(
    blob: &Path,
    media_type: &oci_spec::image::MediaType,
    dest: &Path,
) -> Result<()> {
    let f =
        std::fs::File::open(blob).with_context(|| format!("open layer blob {}", blob.display()))?;
    let mt = media_type.to_string();
    let reader: Box<dyn std::io::Read> = if mt.ends_with("+gzip") || mt.ends_with(".tar.gzip") {
        Box::new(flate2::read::GzDecoder::new(f))
    } else if mt.ends_with("+zstd") || mt.ends_with(".tar.zstd") {
        Box::new(zstd::stream::read::Decoder::new(f).context("open zstd layer decoder")?)
    } else {
        Box::new(f)
    };
    let mut ar = tar::Archive::new(reader);
    ar.set_preserve_permissions(true);
    ar.set_preserve_mtime(false);
    ar.set_overwrite(true);
    // Unpack verbatim — in particular do NOT rewrite/sanitise symlink targets.
    ar.unpack(dest)
        .with_context(|| format!("tar-unpack layer into {}", dest.display()))?;
    Ok(())
}

/// Returns `Some("-z")` for gzip, `Some("--zstd")` for zstd, and `None` for plain
/// uncompressed tar (or an unknown type — let `tar` read the raw archive).
#[allow(dead_code)] // superseded by extract_layer_blob; kept for its unit tests
fn tar_decompress_flag(media_type: &oci_spec::image::MediaType) -> Option<&'static str> {
    let mt = media_type.to_string();
    if mt.ends_with("+gzip") || mt.ends_with(".tar.gzip") {
        Some("-z")
    } else if mt.ends_with("+zstd") || mt.ends_with(".tar.zstd") {
        Some("--zstd")
    } else {
        None
    }
}

/// Build the host-`tar` extract argv `tar -x [<flag>] -f <blob> -C <out>`.
///
/// `flag` is the media-type-derived decompression flag from
/// [`tar_decompress_flag`] (`-z` / `--zstd`), or empty for plain tar. It is
/// placed BEFORE `-f` so `tar` decompresses the blob it then reads. Kept as a
/// pure argv builder so the flag-selection ↔ argv assembly is unit-testable
/// without spawning `tar`.
#[allow(dead_code)] // superseded by extract_layer_blob; kept for its unit tests
fn unpack_tar_argv(flag: &str, blob: &Path, out: &Path) -> Vec<String> {
    let mut argv = vec!["tar".to_owned(), "-x".to_owned()];
    if !flag.is_empty() {
        argv.push(flag.to_owned());
    }
    argv.push("-f".to_owned());
    argv.push(blob.to_string_lossy().into_owned());
    argv.push("-C".to_owned());
    argv.push(out.to_string_lossy().into_owned());
    argv
}

/// Unpack the image's layers (in manifest order) into `staging`, whiteout-aware.
/// Each layer is untarred via the `tar` seam (shelled host `tar`, no daemon)
/// into its OWN per-layer dir — that dir is the authoritative set of paths the
/// layer wrote, which the merge step needs to apply OCI whiteouts correctly
/// (shelled `tar` does NOT honour `.wh.` markers). Then [`merge_layer`] overlays
/// the layer onto `staging`: `.wh..wh..opq` clears the directory's accumulated
/// lower-layer entries (the same layer's own re-adds are overlaid afterwards and
/// survive), `.wh.<name>` deletes `<name>`, and markers never leak into the
/// rootfs. The blob↔diff_id mapping is the manifest's layer order, validated
/// against `config.rootfs().diff_ids()` length.
///
/// # Errors
/// A `tar` failure, a layer/diff_id count mismatch, or a filesystem error.
async fn unpack_oci_layers(
    layout: &Path,
    config: &oci_spec::image::ImageConfiguration,
    staging: &Path,
) -> Result<()> {
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

    let layers = manifest.layers();
    let diff_ids = config.rootfs().diff_ids();
    if layers.len() != diff_ids.len() {
        bail!(
            "OCI layer count {} disagrees with rootfs diff_ids {} (corrupt layout)",
            layers.len(),
            diff_ids.len()
        );
    }

    tokio::fs::create_dir_all(staging)
        .await
        .with_context(|| format!("create staging dir {}", staging.display()))?;

    // A scratch dir holding each layer's per-layer extraction tree, kept beside
    // `staging` so it shares the same filesystem (cheap renames) and is removed
    // afterwards.
    let scratch = staging.with_file_name(format!(
        "{}.layers",
        staging
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "stage".to_owned())
    ));
    tokio::fs::create_dir_all(&scratch)
        .await
        .with_context(|| format!("create layer scratch dir {}", scratch.display()))?;

    for (i, layer) in layers.iter().enumerate() {
        // Extract each layer into its OWN dir. That dir is precisely the set of
        // paths THIS layer wrote — the authoritative "written this layer" set.
        // Relying on a before/after snapshot of the merged tree cannot tell a
        // same-layer re-add from a lower-layer carry-over (a re-added path that
        // also existed below looks identical to a pre-existing one), which is
        // exactly what an opaque whiteout must distinguish.
        let layer_dir = scratch.join(format!("layer-{i}"));
        tokio::fs::create_dir_all(&layer_dir)
            .await
            .with_context(|| format!("create layer dir {}", layer_dir.display()))?;
        let blob = blob_path(layout, layer.digest());
        // Extract IN-PROCESS (tar crate), NOT via the host `tar`: the runner's
        // PATH resolves busybox tar on NixOS/Alpine, which strips the leading `/`
        // from ABSOLUTE symlink targets (/bin/sh -> /bin/busybox lands as the
        // broken `bin/busybox`), corrupting the guest rootfs so /init can't exec.
        // The tar crate writes symlink targets verbatim; decompression is chosen
        // by the layer media type (gzip / zstd / plain).
        let mt = layer.media_type().clone();
        let blob_c = blob.clone();
        let layer_dir_c = layer_dir.clone();
        tokio::task::spawn_blocking(move || extract_layer_blob(&blob_c, &mt, &layer_dir_c))
            .await
            .context("join layer-extract task")?
            .with_context(|| format!("extract OCI layer {}", blob.display()))?;
        // Apply this layer's whiteouts against the accumulated `staging` tree
        // (clearing lower-layer entries), then overlay the layer's own files
        // on top — so a same-layer re-add always survives the opaque clear.
        merge_layer(&layer_dir, staging).await?;
    }

    tokio::fs::remove_dir_all(&scratch).await.ok();
    Ok(())
}

/// Merge a single layer's freshly extracted tree (`layer_dir`) onto the
/// accumulated `staging` tree, honouring OCI whiteouts:
/// - `.wh..wh..opq` in `layer_dir/<rel>` is OPAQUE: clear ALL of `staging/<rel>`'s
///   existing (lower-layer) entries before overlaying this layer's entries;
/// - `.wh.<name>` in `layer_dir/<rel>` deletes `staging/<rel>/<name>`;
/// - every non-marker entry of the layer is copied/overlaid onto `staging`.
///
/// Because the layer's own files come from `layer_dir` and are overlaid AFTER
/// the opaque clear, a path the same layer re-adds always survives even when it
/// also existed in a lower layer — the bug a prior-membership test could not
/// avoid.
///
/// KNOWN LIMITATION (not fixed here, minor): files are moved in via `rename`, so
/// two hardlinked entries that land across different layers become INDEPENDENT
/// copies in the merged tree rather than sharing one inode. This costs disk
/// space (bloat) but does not break boot, so it is left as-is for now.
async fn merge_layer(layer_dir: &Path, staging: &Path) -> Result<()> {
    // Walk the layer tree once, classifying entries relative to the layer root.
    let mut stack = vec![layer_dir.to_path_buf()];
    let mut opaque_rel: Vec<PathBuf> = Vec::new();
    let mut whiteouts: Vec<PathBuf> = Vec::new(); // staging path to delete
    let mut dirs_rel: Vec<PathBuf> = Vec::new();
    let mut files: Vec<(PathBuf, PathBuf)> = Vec::new(); // (src, dst)
    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel_parent = dir
                .strip_prefix(layer_dir)
                .unwrap_or_else(|_| Path::new(""))
                .to_path_buf();
            if entry.file_type().await?.is_dir() {
                let rel = rel_parent.join(&name);
                dirs_rel.push(rel);
                stack.push(path);
            } else if name == ".wh..wh..opq" {
                opaque_rel.push(rel_parent);
            } else if let Some(target) = name.strip_prefix(".wh.") {
                whiteouts.push(staging.join(rel_parent.join(target)));
            } else {
                files.push((path, staging.join(rel_parent.join(&name))));
            }
        }
    }
    // 1. Opaque dirs: clear the accumulated (lower-layer) contents first.
    for rel in opaque_rel {
        let target = staging.join(&rel);
        let mut rd = match tokio::fs::read_dir(&target).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Some(entry) = rd.next_entry().await? {
            remove_any(&entry.path()).await?;
        }
    }
    // 2. Explicit `.wh.<name>` deletions.
    for path in whiteouts {
        remove_any(&path).await.ok();
    }
    // 3. Overlay this layer's own directories then files on top of `staging`.
    for rel in dirs_rel {
        let dst = staging.join(&rel);
        // An upper layer may turn a lower-layer regular FILE (or symlink) into a
        // directory at the same path. `create_dir_all` would fail with
        // NotADirectory / AlreadyExists against a colliding non-directory, so
        // remove it first — mirroring the files-loop guard below (FIX 2).
        if let Ok(meta) = tokio::fs::symlink_metadata(&dst).await
            && !meta.is_dir()
        {
            remove_any(&dst).await.ok();
        }
        tokio::fs::create_dir_all(&dst)
            .await
            .with_context(|| format!("create merged dir {}", dst.display()))?;
    }
    for (src, dst) in files {
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create parent of {}", dst.display()))?;
        }
        // Overwrite any lower-layer file with this layer's version.
        remove_any(&dst).await.ok();
        tokio::fs::rename(&src, &dst)
            .await
            .with_context(|| format!("overlay {} -> {}", src.display(), dst.display()))?;
    }
    Ok(())
}

/// Remove a path whether it's a file or a directory tree (idempotent).
async fn remove_any(path: &Path) -> Result<()> {
    let meta = match tokio::fs::symlink_metadata(path).await {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    if meta.is_dir() {
        tokio::fs::remove_dir_all(path).await.ok();
    } else {
        tokio::fs::remove_file(path).await.ok();
    }
    Ok(())
}

/// Production [`FcBuildRunner`]: spawns `argv[0] argv[1..]`, captures STDOUT,
/// and returns `(exit_ok, stdout_bytes)`. The DOCKER-LESS conversion
/// (`oras copy`/`tar`/`mkfs.ext4`) ignores STDOUT — the OCI config is read from
/// the pulled layout on disk, not from a command's output — so callers just drop
/// the second tuple element. Used by the `"firecracker"` arm in [`crate::build`].
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
                .stderr(std::process::Stdio::piped())
                .output()
                .await
            {
                Ok(out) => {
                    if !out.status.success() {
                        // Surface the tool's own diagnostic (e.g. mkfs.ext4
                        // "Could not allocate N inodes" vs "too small") instead
                        // of discarding it — the caller only sees a bool.
                        tracing::warn!(
                            cmd = %argv.join(" "),
                            code = out.status.code().unwrap_or(-1),
                            stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                            "fc build: command failed"
                        );
                    }
                    (out.status.success(), out.stdout)
                }
                Err(e) => {
                    tracing::warn!(cmd = %argv.join(" "), error = %e, "fc build: command spawn failed");
                    (false, Vec::new())
                }
            }
        });
        fut
    })
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
/// now-removed `oras pull` form) does NOT produce a layout for a normal
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

/// Ensure the buildkit TOOLCHAIN rootfs for SANDBOXED builds (phase 2 of
/// the build/run split): pull `reff` (a tag ref — the toolchain is published
/// under a stable tag), derive the immutable digest from the pulled layout,
/// and convert ONCE into `<data_dir>/build-toolchain/<digest>/rootfs.ext4`.
/// A digest cache hit skips the conversion; a re-published tag (new digest)
/// converts fresh alongside the old one.
///
/// # Errors
/// Pull/convert failures, or a cross-architecture toolchain image.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) async fn ensure_toolchain_rootfs(
    reff: &str,
    data_dir: &Path,
    runner: &FcBuildRunner,
) -> Result<PathBuf> {
    let root = data_dir.join("build-toolchain");
    let pull = root.join(".pull");
    let layout = pull_oci_layout(reff, &pull, runner).await?;
    let digest = read_manifest_digest_from_layout(&layout)?;
    let dir = root.join(digest.replace(':', "-"));
    let cached = dir.join("rootfs.ext4");
    if cached.is_file() {
        tracing::info!(reff, digest, "toolchain rootfs cache hit");
        return Ok(cached);
    }
    let config = read_oci_config_from_layout(&layout)?;
    guard_arch_matches_host(&config)?;
    let entry = Entrypoint::from_oci(&config);
    let init = render_init(&entry)?;
    build_rootfs_ext4_inner(&layout, &config, &dir, 1024, Some(&init), runner).await
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
    is_swap: bool,
    extra_env: Option<&std::collections::HashMap<String, String>>,
) -> Result<std::sync::Arc<dyn crate::runtime::AppRuntime>> {
    let reff = fetched
        .manifest
        .runtime
        .registry_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!("firecracker runtime requires a registry_ref (image to convert)")
        })?;

    // Two ref shapes, both keyed by the IMMUTABLE digest (fc-3):
    //
    //   1. `…@sha256:…` (digest ref) — the digest is known up front, so we take
    //      the FAST path: a cache hit skips the pull entirely; a miss pulls into
    //      the digest-keyed work dir (where the rootfs already lives).
    //
    //   2. `…:tag` (tag ref) — the immutable digest is UNKNOWN until pulled. The
    //      automated build pipeline deploys tag refs, so bailing here was the #1
    //      e2e blocker. Pull into a digest-INDEPENDENT work dir, DERIVE the digest
    //      from the layout's `index.json` (`manifests[0].digest`), then convert
    //      keyed by THAT digest — preserving the digest-keyed cache guarantee.
    //
    // DOCKER-LESS throughout: pull = `oras copy --to-oci-layout`, config read FROM
    // the layout. No `docker pull`/`tag`/`inspect`/`create`/`export` anywhere.
    let rootfs = if let Some((_, digest)) = reff.rsplit_once('@') {
        if rootfs_is_cached(data_dir, uuid, digest) {
            cached_rootfs_path(data_dir, uuid, digest)
        } else {
            // The layout lands under the digest-keyed work dir (= the rootfs dir).
            let work = digest_work_dir(data_dir, uuid, digest)?;
            let layout = pull_oci_layout(reff, &work, runner).await?;
            let config = read_oci_config_from_layout(&layout)?;
            resolve_rootfs(
                uuid, fetched, &layout, &config, digest, data_dir, runner, extra_env,
            )
            .await?
        }
    } else {
        // Tag ref: pull into a digest-INDEPENDENT `.pull` work dir (the digest is
        // not yet known). `oras copy` resolves the tag, so the pull works without
        // the digest; we then read the digest from the resulting layout and the
        // digest-keyed `resolve_rootfs` writes the rootfs to its own digest dir.
        let work = fresh_tag_pull_dir(data_dir, uuid).await?;
        let layout = pull_oci_layout(reff, &work, runner).await?;
        let digest = read_manifest_digest_from_layout(&layout)?;
        if rootfs_is_cached(data_dir, uuid, &digest) {
            cached_rootfs_path(data_dir, uuid, &digest)
        } else {
            let config = read_oci_config_from_layout(&layout)?;
            resolve_rootfs(
                uuid, fetched, &layout, &config, &digest, data_dir, runner, extra_env,
            )
            .await?
        }
    };

    let vm = crate::firecracker::FirecrackerRuntime::launch_with_uuid(
        &rootfs,
        &fetched.manifest.runtime,
        fc,
        uuid,
        reff,
        data_dir,
        is_swap,
    )
    .await?;
    Ok(std::sync::Arc::new(vm))
}

#[cfg(test)]
#[path = "oci_fixtures.rs"]
mod oci_fixtures;

#[cfg(test)]
#[path = "firecracker_tests.rs"]
mod tests;
