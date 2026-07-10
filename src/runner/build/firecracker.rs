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
//! FC node needs only a plain-HTTP registry pull + `tar` + `mkfs.ext4`. On a
//! cache miss `run_firecracker_build` pulls the layout (`pull_oci_layout`) and
//! reads its config (`read_oci_config_from_layout`), then hands `layout`/`config`
//! to `resolve_rootfs` for the conversion.
//! 1. PULL: `pull_oci_layout` pulls the deployed image from the plain-HTTP mesh
//!    OCI registry into `<out_dir>/oci` as a spec-compliant OCI LAYOUT
//!    (`oci-layout` + `index.json` + `blobs/<alg>/<hex>` for
//!    manifest+config+layers) via the RESUMABLE `crate::oci_pull` puller. Each
//!    blob is downloaded with an HTTP `Range` header that resumes from the bytes
//!    already on disk after a mid-stream break. This REPLACED
//!    `oras copy --to-oci-layout`, which restarts a broken blob from byte 0: over
//!    the mesh relay a WireGuard rekey (~every 120s) breaks the TCP stream
//!    mid-blob, so a large layer that outlasts a rekey interval never completed
//!    (break → restart → break). Range-resume accumulates progress across the
//!    breaks and always converges. Skipped iff the rootfs is already cached by
//!    digest. (`oras resolve` is still used for the digest FAST-PATH.)
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

/// Stable fingerprint of the deploy-time inputs BAKED into the guest `/init`
/// BEYOND the image content itself, so the per-uuid rootfs cache key can tell
/// apart two builds of the SAME image digest whose `/init` differs (#106).
///
/// `(uuid, digest)` alone is NOT enough for a WORKSPACE: its uuid is STABLE
/// (derived from the account), yet its `/init` changes when the effective env
/// changes (a forge-url/org rewrite) OR when a cap-file is added (a
/// `workspace_add_repo` clone cap). Both must force a fresh conversion, or the
/// cached rootfs keeps the OLD exported env + cap-writes — e.g. the added repo
/// never clones.
///
/// Hashed inputs (SORTED — `HashMap` iteration is random-seeded):
///   - the effective `extra_env` map (each `k=v` baked as an `export` line), and
///   - the cap-file NAMES (each written as a `/run/tabbify/caps/<name>` file).
///
/// Cap-file VALUES are DELIBERATELY EXCLUDED: they are freshly-random tokens
/// re-minted on every create (`generate_cap`), so hashing them would force a
/// re-conversion on EVERY `workspace_ensure`. The NAME SET is the deterministic
/// STRUCTURAL fingerprint — it changes exactly when the repo/forge layout does
/// (add a repo → a new `<repo>.url` name; add a forge → `forge-admin.token`),
/// which is precisely when the rootfs must be rebaked. Empty env + no caps
/// (a normal deploy) yields a fixed fingerprint, so those paths stay stable.
#[must_use]
pub fn rootfs_env_fingerprint(
    extra_env: Option<&std::collections::HashMap<String, String>>,
    cap_files: &[(String, String)],
) -> String {
    let mut hasher = blake3::Hasher::new();
    // Effective env, sorted by key (the SAME order `merge_extra_env` bakes it).
    let mut env: Vec<(&str, &str)> = extra_env
        .map(|m| m.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect())
        .unwrap_or_default();
    env.sort_unstable();
    hasher.update(b"env\0");
    for (k, v) in env {
        hasher.update(k.as_bytes());
        hasher.update(b"\0");
        hasher.update(v.as_bytes());
        hasher.update(b"\0");
    }
    // Cap-file NAMES, sorted (values excluded — random per mint; see above).
    let mut names: Vec<&str> = cap_files.iter().map(|(n, _)| n.as_str()).collect();
    names.sort_unstable();
    hasher.update(b"caps\0");
    for n in names {
        hasher.update(n.as_bytes());
        hasher.update(b"\0");
    }
    // 16 hex chars (64 bits) — ample against collision for a per-uuid cache key,
    // and a compact single path segment.
    hasher.finalize().to_hex()[..16].to_string()
}

/// Split a raw deploy `extra_env` into its `(effective_env, cap_files)` parts
/// EXACTLY as the rootfs bake does: pop the reserved [`crate::api::CAP_FILES_ENV`]
/// JSON map, decode it into `(name, value)` cap-files (dropping traversal-unsafe
/// names via [`safe_cap_name`]), and leave the remaining vars as the env that
/// gets `export`ed into the guest `/init`.
///
/// Shared so the deploy-time env-hash comparison (the orchestrator's
/// force-cold-on-env-change gate) and the actual bake ([`run_firecracker_build`])
/// derive the SAME split — and therefore the SAME [`rootfs_env_fingerprint`] —
/// from the SAME input. A malformed cap-file JSON yields no cap-files (logged).
#[must_use]
pub fn split_env_and_caps(
    extra_env: Option<&std::collections::HashMap<String, String>>,
) -> (
    std::collections::HashMap<String, String>,
    Vec<(String, String)>,
) {
    let mut effective_env = extra_env.cloned().unwrap_or_default();
    let cap_files: Vec<(String, String)> = match effective_env.remove(crate::api::CAP_FILES_ENV) {
        Some(json) => match serde_json::from_str::<std::collections::HashMap<String, String>>(&json)
        {
            Ok(map) => map
                .into_iter()
                .filter(|(name, _)| {
                    let ok = safe_cap_name(name);
                    if !ok {
                        tracing::warn!(name = %name, "dropping unsafe cap-file name (would escape /run/tabbify/caps)");
                    }
                    ok
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "malformed CAP_FILES_ENV; no cap-files written");
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    (effective_env, cap_files)
}

/// The `/init`-baked env fingerprint (#106) for a raw deploy `extra_env`, derived
/// through the SAME [`split_env_and_caps`] the rootfs bake uses. This is the
/// value that (a) the per-uuid rootfs cache key nests under, (b) the snapshot
/// warm-restore gate stamps + checks (`snapshot::restore_matches`), and (c) the
/// orchestrator compares to force a COLD rebuild when a deploy changes the
/// env/cap set even though the image digest did not (a workspace `add_repo` /
/// secret rotation, #108). Empty env + no caps yields a fixed constant, so an
/// ordinary env-less deploy is stable across redeploys.
#[must_use]
pub fn effective_env_hash(
    extra_env: Option<&std::collections::HashMap<String, String>>,
) -> String {
    let (effective_env, cap_files) = split_env_and_caps(extra_env);
    // `rootfs_env_fingerprint` treats `None` and an EMPTY map identically, so an
    // env-less deploy (`effective_env.is_empty()`) hashes the same either way —
    // matching what `run_firecracker_build` computes after it re-binds an empty
    // effective env to `None`.
    let env_ref = if effective_env.is_empty() {
        None
    } else {
        Some(&effective_env)
    };
    rootfs_env_fingerprint(env_ref, &cap_files)
}

/// On-disk cache path for an app's converted rootfs, keyed by the IMMUTABLE
/// image digest (`sha256:…`) — NOT the tag — AND an `env_hash` fingerprint of the
/// deploy-time env/cap-files baked into its `/init` (see [`rootfs_env_fingerprint`],
/// #106). The same digest+env always maps to the same path (a redeploy of an
/// unchanged image+env skips the slow OCI→ext4 conversion), while a CHANGED env on
/// the SAME (stable) uuid+digest — a workspace's `add_repo`/forge rewrite — lands
/// at a DIFFERENT path so the rootfs is re-baked instead of served stale. A new
/// digest or env gets a fresh dir, never clobbering the old rootfs.
///
/// Layout mirrors the fc snapshot cache:
/// `<data_dir>/apps/<uuid>/fc/<digest-sanitized>/<env_hash>/rootfs.ext4`.
/// The `:` in the digest is replaced with `-` so it's a single path segment; the
/// env variants nest UNDER the digest dir (env-free image content — the pulled
/// OCI layout — is shared across env variants of a digest via the global layout
/// cache, so nesting costs no extra WAN pull).
#[must_use]
pub fn cached_rootfs_path(data_dir: &Path, uuid: &str, digest: &str, env_hash: &str) -> PathBuf {
    let sanitized = digest.replace(':', "-");
    data_dir
        .join("apps")
        .join(uuid)
        .join("fc")
        .join(sanitized)
        .join(env_hash)
        .join(ROOTFS_NAME)
}

/// Is the digest+env-keyed rootfs already converted + on disk?
#[must_use]
pub fn rootfs_is_cached(data_dir: &Path, uuid: &str, digest: &str, env_hash: &str) -> bool {
    cached_rootfs_path(data_dir, uuid, digest, env_hash).is_file()
}

// ── GLOBAL digest-shared rootfs cache ───────────────────────────────────────────
//
// The per-uuid [`cached_rootfs_path`] only speeds a redeploy of the SAME app
// (stable uuid). DEV-sessions get a FRESH uuid every start but reuse the SAME
// dev base image, so they re-pulled (~minutes) + re-converted on every start.
// This GLOBAL cache is keyed ONLY by the immutable image digest, so any uuid
// needing the same content reuses one rootfs. The rootfs is mounted READ-ONLY
// (see `firecracker::build_vm` — `is_read_only: true`), so concurrent VMs safely
// share a single inode; per-uuid materialization is a HARD LINK (zero copy).

/// Root of the global cache: `<data_dir>/rootfs-cache`.
const GLOBAL_ROOTFS_CACHE_DIR: &str = "rootfs-cache";

/// Retain at most this many digest entries in the global cache (LRU by mtime).
/// Bounded so the cache can't fill the worker disk — a past root-fs-full event
/// caused a full outage, so this cache MUST self-limit. Eviction is safe even
/// mid-use: the rootfs is opened read-only and Linux keeps an unlinked-but-open
/// inode alive until the VM exits, so a running guest is unaffected.
const GLOBAL_ROOTFS_CACHE_KEEP: usize = 6;

/// Global digest-shared rootfs path:
/// `<data_dir>/rootfs-cache/<digest-sanitized>/rootfs.ext4`. Keyed ONLY by the
/// immutable digest (NOT the uuid), so the same image content maps to one file.
#[must_use]
pub fn global_rootfs_path(data_dir: &Path, digest: &str) -> PathBuf {
    data_dir
        .join(GLOBAL_ROOTFS_CACHE_DIR)
        .join(digest.replace(':', "-"))
        .join(ROOTFS_NAME)
}

/// Is the digest's rootfs present in the GLOBAL shared cache?
#[must_use]
pub fn global_rootfs_is_cached(data_dir: &Path, digest: &str) -> bool {
    global_rootfs_path(data_dir, digest).is_file()
}

/// Resolve a tag (or digest) ref to its immutable manifest digest WITHOUT
/// pulling layer blobs (`oras resolve`, ~0.2 s). Lets the build consult the
/// digest-keyed caches BEFORE the (slow) `oras copy` and skip the pull on a hit.
///
/// `registry_config_dir`: when `Some(dir)`, passes `--registry-config <dir>` to
/// oras so the resolve is authenticated (Phase-A). Pass `None` for anonymous
/// access (today's default; all existing callers use `None`).
///
/// # Errors
/// The runner reports failure, or stdout is not a `sha256:…` digest line.
pub(crate) async fn resolve_oci_digest(
    reff: &str,
    runner: &FcBuildRunner,
    registry_config_dir: Option<&str>,
) -> Result<String> {
    let mut argv = vec!["oras".to_owned()];
    argv.extend(crate::oras::oras_resolve_args(reff, registry_config_dir));
    let (ok, out) = (runner)(argv).await;
    let digest = String::from_utf8_lossy(&out).trim().to_owned();
    if !ok || !digest.starts_with("sha256:") {
        bail!("oras resolve did not yield a digest for {reff} (ok={ok}, out={digest:?})");
    }
    Ok(digest)
}

/// On a GLOBAL-cache hit, materialize the per-uuid rootfs path as a HARD LINK to
/// the shared inode (same fs ⇒ instant, zero copy). Read-only rootfs ⇒ sharing
/// one inode across VMs is safe; a per-session purge that removes the per-uuid
/// link only decrements the link count, never touching the cache. Returns the
/// per-uuid path on success, or `None` (no hit / link failed) so the caller
/// falls back to a normal pull + build — correctness over the optimization.
async fn link_global_rootfs_to_uuid(
    data_dir: &Path,
    uuid: &str,
    digest: &str,
    env_hash: &str,
) -> Option<PathBuf> {
    let global = global_rootfs_path(data_dir, digest);
    if !global.is_file() {
        return None;
    }
    let per_uuid = cached_rootfs_path(data_dir, uuid, digest, env_hash);
    if per_uuid.is_file() {
        return Some(per_uuid); // already materialized for this uuid
    }
    let parent = per_uuid.parent()?;
    if let Err(e) = tokio::fs::create_dir_all(parent).await {
        tracing::warn!(uuid, digest, error = %e, "global rootfs link: mkdir failed; will build");
        return None;
    }
    match tokio::fs::hard_link(&global, &per_uuid).await {
        Ok(()) => Some(per_uuid),
        // Lost a race (a concurrent build linked/built it): use what's there.
        Err(_) if per_uuid.is_file() => Some(per_uuid),
        Err(e) => {
            tracing::warn!(uuid, digest, error = %e, "global rootfs link failed; will build");
            None
        }
    }
}

/// After a per-uuid build, publish the rootfs into the GLOBAL digest cache (hard
/// link, same inode) so the NEXT uuid needing this digest skips pull + build.
/// Best-effort: a failure only forfeits a future cache hit, never the current
/// build. Evicts to the [`GLOBAL_ROOTFS_CACHE_KEEP`] bound afterwards.
async fn publish_rootfs_to_global(data_dir: &Path, digest: &str, built: &Path) {
    let global = global_rootfs_path(data_dir, digest);
    if !global.is_file() {
        if let Some(parent) = global.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::warn!(digest, error = %e, "global rootfs publish: mkdir failed");
                return;
            }
        }
        match tokio::fs::hard_link(built, &global).await {
            Ok(()) => tracing::info!(digest, "rootfs published to global digest cache"),
            Err(_) if global.is_file() => {} // concurrent publish won the race
            Err(e) => tracing::warn!(digest, error = %e, "global rootfs publish failed (non-fatal)"),
        }
    }
    evict_global_rootfs_cache(data_dir).await;
}

/// Keep the global rootfs cache bounded: retain the [`GLOBAL_ROOTFS_CACHE_KEEP`]
/// most-recently-modified digest dirs, remove the rest. Safe even if a removed
/// rootfs is in use (read-only + unlink-while-open keeps the running VM alive).
async fn evict_global_rootfs_cache(data_dir: &Path) {
    let root = data_dir.join(GLOBAL_ROOTFS_CACHE_DIR);
    let Ok(mut rd) = tokio::fs::read_dir(&root).await else {
        return;
    };
    let mut dirs: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    while let Ok(Some(e)) = rd.next_entry().await {
        let path = e.path();
        if path.is_dir() {
            let mtime = e
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            dirs.push((mtime, path));
        }
    }
    if dirs.len() <= GLOBAL_ROOTFS_CACHE_KEEP {
        return;
    }
    dirs.sort_by_key(|(t, _)| *t); // oldest first
    let remove = dirs.len() - GLOBAL_ROOTFS_CACHE_KEEP;
    for (_, path) in dirs.into_iter().take(remove) {
        if let Err(e) = tokio::fs::remove_dir_all(&path).await {
            tracing::warn!(path = %path.display(), error = %e, "global rootfs evict failed");
        } else {
            tracing::info!(path = %path.display(), "global rootfs cache evicted (LRU bound)");
        }
    }
}

/// Cache lookup for a KNOWN digest: per-uuid first (an app redeploy), then —
/// ONLY when `globally_cacheable` — the GLOBAL digest-shared cache (hard-linked
/// into the per-uuid path on hit). `Some` ⇒ the rootfs is ready; skip the pull +
/// conversion entirely.
///
/// The global cache is keyed by DIGEST ALONE, so it is SOUND only for a rootfs
/// that is identical for that digest. A rootfs with deploy-specific `extra_env`
/// baked into its `/init` (a dev-FC's per-session git cap, or an app's deploy
/// secrets) is uuid-SPECIFIC — sharing it across uuids by digest would hand a
/// later uuid an EARLIER uuid's env (the #68 dev-cap mismatch + a secrets leak).
/// For those the caller passes `globally_cacheable = false`: only the per-uuid
/// cache is consulted, never the global one.
async fn lookup_cached_rootfs(
    data_dir: &Path,
    uuid: &str,
    digest: &str,
    globally_cacheable: bool,
    env_hash: &str,
) -> Option<PathBuf> {
    if rootfs_is_cached(data_dir, uuid, digest, env_hash) {
        return Some(cached_rootfs_path(data_dir, uuid, digest, env_hash));
    }
    if !globally_cacheable {
        return None;
    }
    // Global hits are env-FREE by construction (`globally_cacheable` ⇒ empty env),
    // so `env_hash` here is the fixed empty-env fingerprint — the materialized
    // per-uuid link lands at that same stable path.
    link_global_rootfs_to_uuid(data_dir, uuid, digest, env_hash).await
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

// ── Global OCI-layout cache (digest-keyed, env-FREE → ALWAYS shareable) ───────
//
// The PULLED OCI layout (manifest + config + layer blobs) is PURE image content
// addressed by digest — it carries NO deploy env (env is baked LATER, into the
// rootfs `/init` by `resolve_rootfs`). So unlike the rootfs cache — which #68
// must NOT share across uuids for an env-baked build (a dev-FC's per-session git
// cap or an app's deploy secrets) — the layout is ALWAYS safe to share by
// digest. Caching it lets a dev-FC (a FRESH uuid every start, but the SAME base
// image) skip the multi-minute WAN `oras copy` from the 2nd start on, WITHOUT
// re-introducing the #68 leak. This RESTORES the pull-skip that #68 removed when
// it gated the global ROOTFS cache off for env-baked builds (#57).

const GLOBAL_LAYOUT_CACHE_DIR: &str = "oci-layout-cache";
const GLOBAL_LAYOUT_CACHE_KEEP: usize = 6;

/// `<data_dir>/oci-layout-cache/<digest-sanitized>` — the per-digest cache entry
/// dir. The OCI layout itself lives in its `oci/` subdir (matching
/// [`pull_oci_layout`], which writes the layout to `<out_dir>/oci`).
fn global_oci_layout_entry(data_dir: &Path, digest: &str) -> PathBuf {
    data_dir
        .join(GLOBAL_LAYOUT_CACHE_DIR)
        .join(digest.replace(':', "-"))
}

/// The shareable OCI layout root for `digest`, or `None` on a MISS. A hit is an
/// `oci/` dir with a readable `index.json` — a half-published entry without it
/// is treated as a miss (the caller pulls), never a corrupt build.
async fn lookup_global_layout(data_dir: &Path, digest: &str) -> Option<PathBuf> {
    let layout = global_oci_layout_entry(data_dir, digest).join("oci");
    if tokio::fs::metadata(layout.join("index.json")).await.is_ok() {
        Some(layout)
    } else {
        None
    }
}

/// Recursively HARD-LINK every file under `src` into `dst` (same inode, no data
/// copy — `data_dir` is one filesystem), recreating the dir tree. Falls back to
/// a byte copy for a file whose hard link fails (e.g. cross-device). Best-effort
/// at the call site.
async fn hardlink_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((s, d)) = stack.pop() {
        tokio::fs::create_dir_all(&d).await?;
        let mut rd = tokio::fs::read_dir(&s).await?;
        while let Some(ent) = rd.next_entry().await? {
            let ft = ent.file_type().await?;
            let sp = ent.path();
            let dp = d.join(ent.file_name());
            if ft.is_dir() {
                stack.push((sp, dp));
            } else if tokio::fs::hard_link(&sp, &dp).await.is_err()
                && tokio::fs::metadata(&dp).await.is_err()
            {
                // cross-device (no hard link) and not already present → copy.
                tokio::fs::copy(&sp, &dp).await?;
            }
        }
    }
    Ok(())
}

/// Publish a freshly-pulled `layout` (the `<work>/oci` dir) into the GLOBAL
/// layout cache keyed by `digest`, so the NEXT uuid with this digest skips the
/// pull. Atomic: build a uuid-scoped temp entry (so concurrent builders never
/// collide) then `rename` it into place; a lost race (entry already present) is
/// success. Best-effort — a failure only forfeits a future cache hit, never the
/// current build. Evicts to [`GLOBAL_LAYOUT_CACHE_KEEP`] afterwards.
async fn publish_layout_to_global(data_dir: &Path, digest: &str, uuid: &str, layout: &Path) {
    let entry = global_oci_layout_entry(data_dir, digest);
    if lookup_global_layout(data_dir, digest).await.is_some() {
        evict_global_layout_cache(data_dir).await;
        return; // already cached
    }
    let Some(parent) = entry.parent() else {
        return;
    };
    if let Err(e) = tokio::fs::create_dir_all(parent).await {
        tracing::warn!(digest, error = %e, "global layout publish: mkdir failed (non-fatal)");
        return;
    }
    let tmp = parent.join(format!(".tmp.{}.{uuid}", digest.replace(':', "-")));
    let _ = tokio::fs::remove_dir_all(&tmp).await;
    if let Err(e) = hardlink_tree(layout, &tmp.join("oci")).await {
        tracing::warn!(digest, error = %e, "global layout publish: link tree failed (non-fatal)");
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        return;
    }
    match tokio::fs::rename(&tmp, &entry).await {
        Ok(()) => tracing::info!(digest, "oci layout published to global cache"),
        // Lost a race (a concurrent build published it first) — fine.
        Err(_) if lookup_global_layout(data_dir, digest).await.is_some() => {
            let _ = tokio::fs::remove_dir_all(&tmp).await;
        }
        Err(e) => {
            tracing::warn!(digest, error = %e, "global layout publish: rename failed (non-fatal)");
            let _ = tokio::fs::remove_dir_all(&tmp).await;
        }
    }
    evict_global_layout_cache(data_dir).await;
}

/// Bound the global layout cache to [`GLOBAL_LAYOUT_CACHE_KEEP`] entries (LRU by
/// mtime). Safe even if a removed layout is mid-read by another build: the
/// reader holds open fds (unlink-while-open) and the per-uuid rootfs is already
/// built from it. In-flight `.tmp.*` entries are skipped.
async fn evict_global_layout_cache(data_dir: &Path) {
    let root = data_dir.join(GLOBAL_LAYOUT_CACHE_DIR);
    let Ok(mut rd) = tokio::fs::read_dir(&root).await else {
        return;
    };
    let mut dirs: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    while let Ok(Some(e)) = rd.next_entry().await {
        let path = e.path();
        let is_tmp = path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with(".tmp."));
        if path.is_dir() && !is_tmp {
            let mtime = e
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            dirs.push((mtime, path));
        }
    }
    if dirs.len() <= GLOBAL_LAYOUT_CACHE_KEEP {
        return;
    }
    dirs.sort_by_key(|(t, _)| *t); // oldest first
    let remove = dirs.len() - GLOBAL_LAYOUT_CACHE_KEEP;
    for (_, path) in dirs.into_iter().take(remove) {
        if let Err(e) = tokio::fs::remove_dir_all(&path).await {
            tracing::warn!(path = %path.display(), error = %e, "global layout evict failed");
        } else {
            tracing::info!(path = %path.display(), "global layout cache evicted (LRU bound)");
        }
    }
}

/// The digest+env-keyed work dir for a digest `registry_ref` — the parent of
/// [`cached_rootfs_path`], i.e. where the OCI layout is pulled and the converted
/// `rootfs.ext4` lands. Only valid once the digest is known (digest refs); a TAG
/// ref pulls into a digest-INDEPENDENT `.pull` dir first (see
/// [`run_firecracker_build`]).
fn digest_work_dir(data_dir: &Path, uuid: &str, digest: &str, env_hash: &str) -> Result<PathBuf> {
    Ok(cached_rootfs_path(data_dir, uuid, digest, env_hash)
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

/// ALL the TCP ports the OCI image declares in `config.ExposedPorts` — sorted
/// ascending and de-duplicated — or an EMPTY vec when the image declares none
/// (or only UDP ports).
///
/// These are the app's OWN declared candidate ports (the middle tier of the port
/// precedence, `crate::firecracker::protocol::resolve_port_plan`). This REPLACES
/// the old lowest-only pick: an image `FROM nginx:alpine` inherits `EXPOSE 80`
/// from its base AND may `EXPOSE 8730` for its real listener, so its config
/// carries `ExposedPorts {"80/tcp":{}, "8730/tcp":{}}`. Probing ONLY the lowest
/// (`80`) hammers the base-inherited port where NOTHING listens → the readiness
/// probe times out → crash-loop. Returning BOTH lets the launch path probe them
/// ALL (first-answering-wins) so the port the app truly LISTENS on is the one the
/// readiness probe + reverse proxy target — with ZERO user action.
///
/// `oci_spec` deserializes the JSON `ExposedPorts` MAP (`{"80/tcp":{}}`) into a
/// `Vec<String>` of `"<port>[/<proto>]"` entries; in a non-test build that Vec
/// comes from a `HashMap` (UNORDERED), so the result is SORTED for a stable,
/// order-independent candidate list. Entries without a protocol default to TCP
/// (per the OCI spec). UDP ports are SKIPPED — the supervisor proxies HTTP/TCP
/// into the guest, so a UDP `ExposedPort` is not a serveable app port; an image
/// that exposes ONLY UDP ports yields an EMPTY vec and the caller falls back to
/// 8080. Port `0` (never a real listen port) is ignored defensively.
#[must_use]
pub fn exposed_tcp_ports(config: &oci_spec::image::ImageConfiguration) -> Vec<u16> {
    let Some(inner) = config.config().as_ref() else {
        return Vec::new();
    };
    let Some(ports) = inner.exposed_ports().as_ref() else {
        return Vec::new();
    };
    let mut out: Vec<u16> = ports
        .iter()
        .filter_map(|spec| {
            // "<port>", "<port>/tcp", or "<port>/udp" — default proto is tcp.
            let (port_str, proto) = match spec.split_once('/') {
                Some((p, proto)) => (p, proto),
                None => (spec.as_str(), "tcp"),
            };
            if !proto.eq_ignore_ascii_case("tcp") {
                return None; // UDP (or other) — not a TCP/HTTP app port.
            }
            port_str.trim().parse::<u16>().ok().filter(|p| *p != 0)
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
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

/// The broker uid the workspace cap-files are `chown`ed to (matches the
/// workspace image's broker user; the agent uid has no read access). Kept here
/// as the single source so the writer + the image agree.
pub const BROKER_UID: &str = "9000";

/// Render the POSIX-sh lines that materialize the §12 S1 cap-files inside the
/// guest BEFORE `exec`: create `/run/tabbify/caps` (0700), write each
/// `<name>` → `<value>` as a 0600 file (umask 077 so the create is private), and
/// `chown` the dir+files to the broker uid so ONLY the broker (not the agent)
/// can read them. Returns "" when there are no cap-files (regular apps), so the
/// generic init is byte-identical to today for non-workspace images.
///
/// `cap_files` is `(name, value)` pairs already validated by [`safe_cap_name`]
/// (no `/`, no `..`); the value is single-quoted so a `$`/space/glob in a URL is
/// not re-interpreted by the shell.
#[must_use]
pub fn render_cap_files_init(cap_files: &[(String, String)]) -> String {
    if cap_files.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "mkdir -p /run/tabbify/caps\n\
         chmod 0700 /run/tabbify/caps\n",
    );
    for (name, value) in cap_files {
        // safe_cap_name guarantees `name` is a bare filename; quote defensively.
        let qname = sh_single_quote(&format!("/run/tabbify/caps/{name}"));
        let qval = sh_single_quote(value);
        out.push_str(&format!(
            "(umask 077; printf %s {qval} > {qname})\n\
             chmod 0600 {qname}\n"
        ));
    }
    out.push_str(&format!(
        "chown -R {BROKER_UID}:{BROKER_UID} /run/tabbify/caps 2>/dev/null || true\n"
    ));
    out
}

/// The workspace marker env key (`TABBIFY_WORKSPACE_UUID`). Kept as a fn so the
/// `crate::api` re-export is referenced from exactly one spot in this module.
fn tabbify_workspace_contract_marker() -> &'static str {
    crate::api::WORKSPACE_MARKER_ENV
}

/// Reject a cap-file name that is not a bare, traversal-safe filename. Mirrors
/// the API-side `cap_repo_basename` invariant at the consuming end so a corrupt
/// `CAP_FILES_ENV` payload can never write outside `/run/tabbify/caps`.
#[must_use]
pub fn safe_cap_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
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
pub fn render_init(entry: &Entrypoint, cap_files: &[(String, String)]) -> Result<String> {
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

    // The generic wrapper this function renders is itself injected at /init (see
    // `inject_init`). If the image's own entrypoint ALSO resolves to /init, the
    // `exec {exec_line}` at the tail re-execs THIS wrapper forever — a silent
    // PID-1 recursion that never starts the app: readiness times out at 30s and
    // the FC respawns in a loop, with ZERO userspace console output. Fail LOUD at
    // conversion time instead of shipping a workspace that hangs invisibly.
    if argv.first().map(String::as_str) == Some("/init") {
        bail!(
            "image ENTRYPOINT/CMD resolves to '/init', which collides with the \
             reserved generic-FC init slot: the supervisor injects its PID-1 \
             wrapper at /init and execs the entrypoint, so '/init' would exec the \
             wrapper itself and loop forever. Rename the image's init to a \
             distinct path, e.g. /usr/local/bin/<name>."
        );
    }

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
    // §12 S1 cap-files: write each per-repo cap-URL (+ forge-admin token) to a
    // 0600 broker-owned file under /run/tabbify/caps BEFORE exec — NOT as env,
    // NOT `export`ed (so the agent never reads them + they never freeze into a
    // Full snapshot). Empty for non-workspace images → byte-identical init.
    let cap_lines = render_cap_files_init(cap_files);
    Ok(format!(
        "#!/bin/sh\n\
         set -e\n\
         mkdir -p /proc /sys /dev\n\
         mount -t proc proc /proc 2>/dev/null || true\n\
         mount -t sysfs sysfs /sys 2>/dev/null || true\n\
         mount -t devtmpfs devtmpfs /dev 2>/dev/null || mount -t tmpfs tmpfs /dev 2>/dev/null || true\n\
         # devtmpfs can be unavailable (empty tmpfs fallback above); ensure the\n\
         # core character devices exist so tools reading /dev/urandom (e.g. git\n\
         # on repo create) don't fail with ENOENT.\n\
         [ -e /dev/null ] || mknod -m 666 /dev/null c 1 3 2>/dev/null || true\n\
         [ -e /dev/zero ] || mknod -m 666 /dev/zero c 1 5 2>/dev/null || true\n\
         [ -e /dev/random ] || mknod -m 666 /dev/random c 1 8 2>/dev/null || true\n\
         [ -e /dev/urandom ] || mknod -m 666 /dev/urandom c 1 9 2>/dev/null || true\n\
         # eth0 is configured by the kernel ip= boot-arg; verify it came up.\n\
         if [ ! -e /sys/class/net/eth0 ]; then\n\
         \techo 'tabbify-init: eth0 missing (kernel ip= boot-arg did not configure it)' >&2\n\
         fi\n\
         ip link show eth0 >/dev/null 2>&1 || true\n\
         {env_lines}\
         {cap_lines}\
         mkdir -p {workdir} 2>/dev/null; cd {workdir}\n\
         exec {exec_line}\n",
    ))
}

/// Append deploy-time `extra` entries to `oci_env` AFTER the OCI image's own
/// `config.Env` entries. `render_init` emits the env as `export KEY='value'`
/// lines in order and POSIX sh honours the LAST definition of a variable, so
/// deploy-time values win on key collision. This is the SINGLE merge primitive
/// [`resolve_rootfs`] uses — tests exercise this exact production path.
///
/// Extra entries are emitted in SORTED key order: `HashMap` iteration is
/// random-seeded per process, and an unsorted merge would make the rendered
/// `/init` (and thus the rootfs bytes) nondeterministic across builds of the
/// same image+env.
pub fn merge_extra_env(
    oci_env: &mut Vec<String>,
    extra: &std::collections::HashMap<String, String>,
) {
    let mut pairs: Vec<_> = extra.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    oci_env.extend(pairs.into_iter().map(|(k, v)| format!("{k}={v}")));
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
/// CACHE CONSTRAINT (#106): the PER-UUID rootfs cache is keyed by
/// `(uuid, digest, env_hash)` — `cached_rootfs_path(data_dir, uuid, digest,
/// env_hash)`, where `env_hash` = [`rootfs_env_fingerprint`] of the effective
/// `extra_env` + cap-file NAMES that get baked into `/init`. This makes a CHANGED
/// env on the SAME uuid+digest land at a fresh path so the rootfs is re-baked —
/// essential for a WORKSPACE, whose uuid is STABLE (account-derived) yet whose
/// `/init` changes on `add_repo` / forge rewrite. A devbox/dev-session still gets
/// a fresh uuid per creation, so its key is unique regardless. `env_hash` is
/// computed HERE from `extra_env`+`cap_files` so it always matches what
/// `render_init` bakes (the caller computes the same value for the pre-conversion
/// cache probes).
///
/// The GLOBAL digest-shared cache (#57), keyed by DIGEST ALONE, remains UNSAFE
/// for an `extra_env`-baked rootfs (it would share one uuid's env with another
/// uuid of the same digest — the #68 dev-cap mismatch + a secrets leak).
/// `run_firecracker_build` gates global publish/link on `globally_cacheable`
/// (true IFF `extra_env` is empty); see [`lookup_cached_rootfs`].
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
    cap_files: &[(String, String)],
) -> Result<PathBuf> {
    // Fingerprint the /init-baked env + cap-file names so a changed env on the
    // SAME uuid+digest (a workspace add_repo / forge rewrite) misses the cache and
    // re-bakes, instead of serving a stale rootfs (#106).
    let env_hash = rootfs_env_fingerprint(extra_env, cap_files);
    let cached = cached_rootfs_path(data_dir, uuid, digest, &env_hash);
    if rootfs_is_cached(data_dir, uuid, digest, &env_hash) {
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
    // [`merge_extra_env`] for the collision contract). A shell-form entrypoint
    // has no `OciExec.env` to merge into — `render_init` rejects it below (D3),
    // but warn here so the dropped env is visible and not a silent surprise.
    if extra_env.is_some() && !matches!(entry, Entrypoint::Exec(_)) {
        tracing::warn!(
            uuid,
            "extra_env supplied but image entrypoint is shell-form; env cannot be merged"
        );
    }
    if let (Some(map), Entrypoint::Exec(exec)) = (extra_env, &mut entry) {
        merge_extra_env(&mut exec.env, map);
    }
    let init = render_init(&entry, cap_files)?; // shell-form → clear error (D3)

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

/// Extract the registry host portion from an OCI reference (everything before
/// the first `/`). Used to key the oras auth config to the correct registry.
///
/// # Examples
/// - `"[fd5a::1]:5000/acme/app:sha"` → `"[fd5a::1]:5000"`
/// - `"reg.example.com/acme/app:v1"` → `"reg.example.com"`
fn registry_host_from_ref(reff: &str) -> &str {
    reff.split('/').next().unwrap_or(reff)
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
            // `oras` (registry pull over the relay-only mesh registry) is retried
            // on transient failure — a large blob can break mid-transfer over the
            // DERP relay (the registry proxy then 502s); local tools (`tar`,
            // `mkfs.ext4`) run exactly once. Every spawn carries a valid `HOME`
            // so `oras` never aborts "$HOME is not defined" on a clean install.
            let attempts = crate::tool_exec::attempts_for(prog);
            let mut last: (bool, Vec<u8>) = (false, Vec::new());
            for attempt in 1..=attempts {
                match Command::new(prog)
                    .args(rest)
                    .env("HOME", crate::tool_exec::tool_home())
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .output()
                    .await
                {
                    Ok(out) if out.status.success() => return (true, out.stdout),
                    Ok(out) => {
                        // Surface the tool's own diagnostic (e.g. mkfs.ext4
                        // "Could not allocate N inodes" vs "too small") instead
                        // of discarding it — the caller only sees a bool.
                        tracing::warn!(
                            cmd = %argv.join(" "),
                            code = out.status.code().unwrap_or(-1),
                            stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                            attempt,
                            attempts,
                            "fc build: command failed"
                        );
                        last = (false, out.stdout);
                    }
                    Err(e) => {
                        tracing::warn!(cmd = %argv.join(" "), error = %e, attempt, attempts, "fc build: command spawn failed");
                        last = (false, Vec::new());
                    }
                }
                if attempt < attempts {
                    tokio::time::sleep(crate::tool_exec::retry_backoff(attempt)).await;
                }
            }
            last
        });
        fut
    })
}

/// Remove an OCI layout dir, tolerating a missing dir (idempotent) but SURFACING
/// any real error. A silently-swallowed wipe failure (EBUSY / EIO / permissions /
/// a half-removed tree) leaves a DIRTY layout that makes every subsequent
/// `oras copy --to-oci-layout` fail identically with "Error from destination
/// oci-layout" — the #64 doom-loop. Returning the error lets the caller log it
/// and fall back instead of silently re-failing.
async fn wipe_oci_layout(layout: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(layout).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::Error::new(e))
            .with_context(|| format!("wipe oci layout dir {}", layout.display())),
    }
}

/// Pull the deployed OCI image from the plain-HTTP mesh registry into
/// `<out_dir>/oci` as a spec-compliant OCI LAYOUT (`oci-layout` + `index.json` +
/// `blobs/<alg>/<hex>` for manifest+config+layers), DOCKER-LESS.
///
/// The pull is RESUMABLE (`crate::oci_pull`): each blob (manifest, config, every
/// layer) is downloaded with an HTTP `Range` header that resumes from the bytes
/// already on disk after a mid-stream break. This is the crux of the fix — over
/// the mesh relay a WireGuard rekey (~every 120s) breaks the TCP stream mid-blob,
/// and `oras copy` does NOT resume (it restarts the broken blob from byte 0), so
/// a large layer that outlasts a rekey interval NEVER completes. A Range-resume
/// download accumulates progress across the breaks and always converges.
///
/// Returns the layout directory (`<out_dir>/oci`) for the config-read + unpack.
///
/// # Errors
/// A blob that never converges within its budget, or one that fails sha256
/// verification after the bounded re-pulls (see `crate::oci_pull`).
async fn pull_oci_layout(
    reff: &str,
    out_dir: &Path,
    registry_config_file: Option<&str>,
) -> Result<PathBuf> {
    let layout = crate::oci_pull::layout_subdir(out_dir);
    // SPEED + CORRECTNESS: for a MUTABLE tag ref (no `@sha256`), wipe the layout
    // dir first so the derived digest (`manifests[0].digest`) tracks the tag's
    // CURRENT image. A stale layout could otherwise leave an OLD manifest whose
    // digest the readers take, so a re-published tag would serve its original
    // version forever. A digest-pinned ref (`…@sha256:…`) is immutable and
    // content-addressed, so leave its (already-verified) blobs intact — that is
    // exactly what enables cross-invocation blob-level resume for a big layer.
    // The wipe error is SURFACED (not swallowed): a half-removed dir would leave
    // a corrupt layout that poisons the readers.
    if !reff.contains("@sha256") {
        wipe_oci_layout(&layout).await?;
    }
    tokio::fs::create_dir_all(&layout)
        .await
        .with_context(|| format!("create oci layout dir {}", layout.display()))?;
    crate::oci_pull::pull_image_http(reff, &layout, registry_config_file)
        .await
        .with_context(|| {
            format!(
                "resumable OCI pull of {reff:?} into layout {}",
                layout.display()
            )
        })?;
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
    // Toolchain pulls are not part of Phase-A auth scope; pass None (anonymous).
    let layout = pull_oci_layout(reff, &pull, None).await?;
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
    // Toolchain rootfs is NEVER a workspace → no cap-files to materialize.
    let init = render_init(&entry, &[])?;
    build_rootfs_ext4_inner(&layout, &config, &dir, 1024, Some(&init), runner).await
}

/// Entry point for the `"firecracker"` arm of [`crate::build::build_runtime`]:
/// resolve (cache or convert) the rootfs, then boot it via the existing
/// `FirecrackerRuntime` contract (guest `172.31.0.2:8080`, kernel
/// `/opt/tabbify/vmlinux`, per-uuid pidfile + warm-snapshot).
///
/// # Errors
/// Conversion failure (see [`resolve_rootfs`]) or a VM launch failure.
#[allow(clippy::too_many_arguments)]
pub async fn run_firecracker_build(
    uuid: &str,
    fetched: &crate::fetcher::FetchedApp,
    fc: &crate::config::FcConfig,
    data_dir: &Path,
    runner: &FcBuildRunner,
    is_swap: bool,
    extra_env: Option<&std::collections::HashMap<String, String>>,
    egress_allow: Option<&[String]>,
) -> Result<std::sync::Arc<dyn crate::runtime::AppRuntime>> {
    // SNAPSHOT SUPPRESS (§12 snapshot-timing). A dev-FC's `/init` async-clones
    // `/workspace`; a WORKSPACE's `cold_boot` readiness probe answers BEFORE
    // rust-analyzer finishes indexing. In BOTH cases a cold-boot snapshot would
    // freeze a wrong/cold rootfs+RAM, and a later warm-restore would resurrect
    // it. Mark the cache dir `.no-snapshot` so cold_boot NEVER snapshots → every
    // (re)launch cold-boots; the workspace's WARM snapshot is taken only later by
    // `Cmd::Snapshot` (post-index), which bypasses this marker.
    let is_dev = extra_env.is_some_and(|m| m.contains_key("TABBIFY_GIT_REMOTE"));
    let is_workspace =
        extra_env.is_some_and(|m| m.contains_key(tabbify_workspace_contract_marker()));
    if is_dev || is_workspace {
        crate::firecracker::snapshot::suppress(&crate::firecracker::snapshot::cache_dir(
            data_dir, uuid,
        ));
    }

    // §12 S1 cap-file channel + §4 env-safety guard (RUNNER process — this is the
    // point where RUNNER_EXTRA_ENV is re-baked into the rootfs /init, i.e. where a
    // leak would actually freeze). Build the EFFECTIVE env: pop the reserved
    // CAP_FILES_ENV key so its cap content is NEVER `export`ed nor snapshot-frozen,
    // decode it into cap-files, then assert NO forbidden key remains.
    let (effective_env, cap_files) = split_env_and_caps(extra_env);
    // §4: with the cap-file key removed, the remaining env must carry NO
    // snapshot-forbidden key. A workspace bakes a Full snapshot, so a leaked
    // cap/token here would survive into every warm restore. FAIL the build loudly
    // (the spawn aborts; the API's async deploy handler then revokes caps).
    if is_workspace {
        if let Err(key) = crate::firecracker::snapshot_decision::extra_env_is_snapshot_safe(
            effective_env.keys(),
        ) {
            anyhow::bail!(
                "workspace boot env carries a snapshot-forbidden key {key:?}; \
                 refusing to bake it into a Full snapshot (§4)"
            );
        }
    }
    let extra_env = if effective_env.is_empty() {
        None
    } else {
        Some(&effective_env)
    };

    // GLOBAL-cache soundness gate. The global digest-shared rootfs cache (#57) is
    // keyed by DIGEST ALONE, so it may only carry a rootfs that is identical for
    // that digest. Deploy-time `extra_env` is baked into the rootfs `/init`
    // (`render_init` emits `export KEY='value'`) — a dev-FC's per-session git cap
    // or an app's deploy secrets — making the rootfs uuid-SPECIFIC. Such a rootfs
    // must NEVER be published to or linked from the global cache, or a later uuid
    // with the same image digest would inherit THIS uuid's env: a dev-FC would
    // get the wrong git cap (`git clone` → 403 → no `/workspace`, the #68 bug) and
    // an app could inherit another app's secrets. The per-uuid cache stays sound
    // (uuid+digest+env are aligned). So: globally cacheable IFF no deploy env.
    let globally_cacheable = extra_env.is_none_or(|m| m.is_empty());

    // Fingerprint the /init-baked env + cap-file NAMES (#106) so the PER-UUID
    // rootfs cache key includes it: a CHANGED env on a STABLE uuid (a workspace's
    // add_repo / forge rewrite) then misses the cache and RE-BAKES instead of
    // serving a stale `/init`. `resolve_rootfs` recomputes the SAME value from the
    // same `(extra_env, cap_files)`; these pre-conversion probes must agree with
    // it, so compute it ONCE here and thread it to every per-uuid cache call.
    let env_hash = rootfs_env_fingerprint(extra_env, &cap_files);

    let reff = fetched
        .manifest
        .runtime
        .registry_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!("firecracker runtime requires a registry_ref (image to convert)")
        })?;

    // OCI distribution requires lowercase repository names. Lowercase the repo
    // path (preserving the tag/digest) so the PULL ref matches the build PUSH
    // ref (which lowercases the tenant) — an uppercase GitHub owner like "Lsneg"
    // would otherwise make `oras copy` fail with "invalid repository".
    let reff = crate::oras::lowercase_oci_repo(reff);
    let reff = reff.as_str();

    // Phase-A registry auth: read `TABBIFY_RUNNER_JOIN_TOKEN` once (already set
    // by the supervisor when launching this runner). When present, write a
    // docker-format auth config keyed to this ref's registry host and pass its
    // dir to every `oras resolve` / `oras copy` call so pulls are authenticated.
    // When absent (anonymous registry or Phase-A not yet enabled), pass `None`
    // — identical to today's behaviour.
    let oras_cfg_owned: Option<String> = match std::env::var("TABBIFY_RUNNER_JOIN_TOKEN") {
        Ok(token) if !token.is_empty() => {
            let cfg_dir = data_dir.join("oras-cfg");
            let host = registry_host_from_ref(reff);
            match crate::skopeo::write_registry_config(&token, host, &cfg_dir) {
                Ok(()) => {
                    tracing::debug!(host, "oras auth config written for registry pull");
                    // oras `--from-registry-config` wants the auth FILE, not its
                    // dir: write_registry_config writes `<cfg_dir>/config.json`.
                    Some(cfg_dir.join("config.json").to_string_lossy().into_owned())
                }
                Err(e) => {
                    tracing::warn!(
                        host,
                        error = %e,
                        "failed to write oras auth config; proceeding anonymous"
                    );
                    None
                }
            }
        }
        _ => None,
    };
    let oras_cfg = oras_cfg_owned.as_deref();

    // The app's OWN declared candidate ports (ALL `ExposedPorts` TCP, tier 2 of
    // `resolve_port_plan`), set whenever this launch READS the OCI config. On a
    // config-read-LESS launch (a pre-pull rootfs cache hit) it stays EMPTY; the
    // per-uuid `.app_port` companion persisted by an earlier launch's WINNING port
    // recovers it so e.g. `FROM nginx` is probed on its real port on every respawn,
    // not just the first deploy. Empty + no companion ⇒ the 8080 fallback
    // (unchanged). Multiple candidates ⇒ the launch probes them all,
    // first-answering-wins (see `resolve_port_plan` / `probe_first_answering`).
    let mut image_exposed_ports: Vec<u16> = Vec::new();

    // FAST PATH (digest-shared cache): resolve the IMMUTABLE digest WITHOUT
    // pulling layer blobs, so the digest-keyed caches (per-uuid + the GLOBAL
    // digest-shared cache) can be consulted BEFORE paying a multi-minute pull. A
    // digest ref already carries it; a tag ref is resolved via `oras resolve`
    // (~0.2 s, best-effort — a transient failure falls through to the pull path
    // that derives the digest from the pulled layout). KEY WIN for dev-sessions:
    // every start gets a FRESH uuid but reuses the SAME dev base image, so from
    // the second start the cached rootfs is hard-linked and the pull is skipped.
    let pre_digest: Option<String> = match reff.rsplit_once('@') {
        Some((_, d)) => Some(d.to_owned()),
        None => match resolve_oci_digest(reff, runner, oras_cfg).await {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::warn!(reff, error = %e, "oras resolve failed; pulling to derive digest");
                None
            }
        },
    };
    if let Some(digest) = pre_digest.as_deref() {
        if let Some(rootfs) =
            lookup_cached_rootfs(data_dir, uuid, digest, globally_cacheable, &env_hash).await
        {
            tracing::info!(
                uuid,
                digest,
                "firecracker rootfs cache hit (pre-pull); skipping pull + conversion"
            );
            // Config NOT read on this fast path → `image_exposed_ports` is still
            // EMPTY; `launch_with_uuid` recovers the winning port from the
            // `.app_port` companion an earlier launch persisted (else falls back to
            // 8080). `digest` is the resolved (immutable) image identity → the
            // `.snapshot_ref` stamp/match key, so a moved tag (`…:current`)
            // invalidates a stale warm snapshot.
            return launch_firecracker(
                &rootfs, fetched, fc, uuid, reff, data_dir, is_swap, egress_allow, is_workspace,
                &env_hash, &image_exposed_ports, digest,
            )
            .await;
        }
    }

    // GLOBAL OCI-LAYOUT fast-path: the rootfs cache missed (a fresh dev-FC uuid,
    // or an env-baked build the global ROOTFS cache excludes — #68), but we may
    // already hold this digest's PULLED LAYOUT from an earlier uuid. The layout
    // is env-FREE image content, so sharing it is ALWAYS sound (unlike the
    // rootfs). Reuse it to skip the multi-minute WAN `oras copy` and build the
    // (per-uuid, env-baked) rootfs locally. KEY WIN for dev-sessions: the pull is
    // paid ONCE per base image, then every later dev-FC builds from the cache
    // with NO pull (restores what #68 removed; #57).
    if let Some(digest) = pre_digest.as_deref() {
        if let Some(global_layout) = lookup_global_layout(data_dir, digest).await {
            // Snapshot the SHARED layout into a per-uuid dir (hard links — no data
            // copy) BEFORE reading it. The global entry is LRU-evictable and the
            // unpack opens layer blobs LAZILY (one per layer, in turn), so a
            // concurrent eviction could unlink a not-yet-opened blob mid-build →
            // ENOENT. Reading from a copy the build OWNS removes that race. A link
            // failure (e.g. the entry was evicted mid-snapshot) leaves the hit and
            // falls through to a normal pull — correctness over the optimization.
            let layout_work = data_dir.join("apps").join(uuid).join("fc").join(".layout");
            tokio::fs::remove_dir_all(&layout_work).await.ok();
            let local = layout_work.join("oci");
            if hardlink_tree(&global_layout, &local).await.is_ok()
                && tokio::fs::metadata(local.join("index.json")).await.is_ok()
            {
                tracing::info!(uuid, digest, "oci layout cache hit; skipping pull");
                let config = read_oci_config_from_layout(&local)?;
                image_exposed_ports = exposed_tcp_ports(&config);
                let built = resolve_rootfs(
                    uuid, fetched, &local, &config, digest, data_dir, runner, extra_env, &cap_files,
                )
                .await?;
                if globally_cacheable {
                    publish_rootfs_to_global(data_dir, digest, &built).await;
                }
                return launch_firecracker(
                    &built, fetched, fc, uuid, reff, data_dir, is_swap, egress_allow, is_workspace,
                    &env_hash, &image_exposed_ports, digest,
                )
                .await;
            }
        }
    }

    // MISS: pull, derive the authoritative digest from the pulled layout, convert,
    // then publish the rootfs to the GLOBAL digest cache for the next uuid.
    //
    // Two ref shapes, both keyed by the IMMUTABLE digest (fc-3):
    //   1. `…@sha256:…` — digest known up front; layout lands in the digest dir.
    //   2. `…:tag` — immutable digest UNKNOWN until pulled; the automated build
    //      pipeline deploys tag refs. Pull into a digest-INDEPENDENT work dir,
    //      DERIVE the digest from the layout's `index.json` (`manifests[0].digest`),
    //      then convert keyed by THAT digest — preserving the cache guarantee.
    //
    // DOCKER-LESS throughout: pull = `oras copy --to-oci-layout`, config read FROM
    // the layout. No `docker pull`/`tag`/`inspect`/`create`/`export` anywhere.
    // Bind BOTH the built rootfs AND the RESOLVED digest: the digest is the
    // immutable, content-addressed identity that stamps/matches the warm-start
    // `.snapshot_ref` companion (NOT `reff`, which may be a MOVING tag like
    // `…:current` — a base-image OTA advances the digest under the same tag string,
    // and a tag-vs-tag match would resurrect the STALE guest). The digest-ref arm
    // already carries it; the tag arm derives the AUTHORITATIVE digest from the
    // pulled layout.
    let (rootfs, resolved_digest) = if let Some((_, digest)) = reff.rsplit_once('@') {
        let work = digest_work_dir(data_dir, uuid, digest, &env_hash)?;
        let layout = pull_oci_layout(reff, &work, oras_cfg).await?;
        // Seed the global layout cache so the NEXT uuid with this digest skips
        // the pull (env-free → always safe; keyed by the immutable ref digest).
        publish_layout_to_global(data_dir, digest, uuid, &layout).await;
        let config = read_oci_config_from_layout(&layout)?;
        image_exposed_ports = exposed_tcp_ports(&config);
        let built = resolve_rootfs(
            uuid, fetched, &layout, &config, digest, data_dir, runner, extra_env, &cap_files,
        )
        .await?;
        if globally_cacheable {
            publish_rootfs_to_global(data_dir, digest, &built).await;
        }
        (built, digest.to_owned())
    } else {
        let work = fresh_tag_pull_dir(data_dir, uuid).await?;
        let layout = pull_oci_layout(reff, &work, oras_cfg).await?;
        let digest = read_manifest_digest_from_layout(&layout)?;
        // Seed the global layout cache keyed by the AUTHORITATIVE manifest digest
        // derived FROM the pulled layout — NOT the pre-pull `oras resolve` guess,
        // which can diverge from a mutable tag's actual content. Key == content
        // makes a future hit always serve the right image; a stale `pre_digest`
        // then yields a harmless lookup MISS + re-pull, never wrong bytes.
        publish_layout_to_global(data_dir, &digest, uuid, &layout).await;
        let built = if rootfs_is_cached(data_dir, uuid, &digest, &env_hash) {
            cached_rootfs_path(data_dir, uuid, &digest, &env_hash)
        } else {
            // Global hit found only AFTER the pull (the pre-pull resolve failed):
            // still skip the conversion — but ONLY for a globally-cacheable rootfs.
            let global_hit = if globally_cacheable {
                link_global_rootfs_to_uuid(data_dir, uuid, &digest, &env_hash).await
            } else {
                None
            };
            if let Some(linked) = global_hit {
                tracing::info!(uuid, digest, "rootfs global-cache hit (post-pull); skipping conversion");
                linked
            } else {
                let config = read_oci_config_from_layout(&layout)?;
                image_exposed_ports = exposed_tcp_ports(&config);
                let built = resolve_rootfs(
                    uuid, fetched, &layout, &config, &digest, data_dir, runner, extra_env, &cap_files,
                )
                .await?;
                if globally_cacheable {
                    publish_rootfs_to_global(data_dir, &digest, &built).await;
                }
                built
            }
        };
        (built, digest)
    };

    launch_firecracker(
        &rootfs, fetched, fc, uuid, reff, data_dir, is_swap, egress_allow, is_workspace, &env_hash,
        &image_exposed_ports, &resolved_digest,
    )
    .await
}

/// Launch the Firecracker microVM from a prepared rootfs and wrap it as an
/// [`crate::runtime::AppRuntime`]. Shared by the cache-hit fast path and the
/// pull+build path of [`run_firecracker_build`].
///
/// `snapshot_ref` is the RESOLVED image digest (`sha256:…`) this launch runs — the
/// immutable, content-addressed identity used to stamp/match the warm-start
/// `.snapshot_ref` companion. Every caller passes the DIGEST (not the possibly-
/// moving `reff` tag) so a base-image OTA under a stable tag (`…:current`) is
/// detected as a content change and invalidates the stale snapshot (see
/// `FirecrackerRuntime::launch_with_uuid`).
#[allow(clippy::too_many_arguments)]
async fn launch_firecracker(
    rootfs: &Path,
    fetched: &crate::fetcher::FetchedApp,
    fc: &crate::config::FcConfig,
    uuid: &str,
    reff: &str,
    data_dir: &Path,
    is_swap: bool,
    egress_allow: Option<&[String]>,
    is_workspace: bool,
    env_hash: &str,
    image_exposed_ports: &[u16],
    snapshot_ref: &str,
) -> Result<std::sync::Arc<dyn crate::runtime::AppRuntime>> {
    let vm = crate::firecracker::FirecrackerRuntime::launch_with_uuid(
        rootfs,
        &fetched.manifest.runtime,
        fc,
        uuid,
        reff,
        data_dir,
        is_swap,
        egress_allow,
        is_workspace,
        env_hash,
        image_exposed_ports,
        snapshot_ref,
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
