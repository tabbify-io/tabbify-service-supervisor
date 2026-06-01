//! `node-firecracker` builder: boot a prebuilt NixOS node image (vmlinux +
//! rootfs.ext4) as a recursive tabbify-node microVM. The image is host-local
//! (`FcConfig::node_image_dir`); no OCI->ext4 conversion (that is generic-FC).

use std::path::Path;
use std::sync::Arc;

use crate::config::FcConfig;
use crate::fetcher::FetchedApp;
use crate::firecracker::FirecrackerRuntime;
use crate::runtime::AppRuntime;

/// Resolve `vmlinux` + `rootfs.ext4` under `node_image_dir` and boot the VM.
///
/// # Errors
/// Returns an error if the node image files are missing, or if the microVM
/// fails to launch.
pub async fn run_node_firecracker_build(
    uuid: &str,
    _fetched: &FetchedApp,
    fc: &FcConfig,
    data_dir: &Path,
) -> anyhow::Result<Arc<dyn AppRuntime>> {
    let kernel = fc.node_image_dir.join("vmlinux");
    let rootfs = fc.node_image_dir.join("rootfs.ext4");
    if !kernel.is_file() || !rootfs.is_file() {
        anyhow::bail!(
            "node image not found: expected vmlinux + rootfs.ext4 under {} \
             (populate via `nix build` on the host; S3 fetch comes later)",
            fc.node_image_dir.display()
        );
    }
    let vm = FirecrackerRuntime::launch_node_with_uuid(&kernel, &rootfs, fc, uuid, data_dir).await?;
    Ok(Arc::new(vm))
}
