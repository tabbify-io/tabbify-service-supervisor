//! Pure, host-agnostic decisions for a STATEFUL app's persistent data disk +
//! guest mount. NO cfg gate so the decision logic is unit-testable on macOS —
//! the actual disk attach (`configure_and_boot` → `ensure_data_disk`) and the
//! `/init` mount bake (`render_init`) that CONSUME these decisions are behind
//! the Linux / build paths, but the "should there be a disk, and where does it
//! mount" logic has no KVM dependency and is exercised here directly.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

use crate::manifest::Runtime;

/// The persistent data-disk image path for an app IFF it is stateful.
///
/// Returns `Some(<data_dir>/apps/<uuid>/data.ext4)` when `runtime.stateful` —
/// the create-once ext4 ([`crate::runner::build::fc_sandbox::ensure_data_disk`],
/// never reformatted) attached as the non-root `/dev/vdb`. Returns `None` for a
/// non-stateful app, so its boot config is byte-identical to the historical
/// path (no data disk, snapshots NOT suppressed).
///
/// The image is a SIBLING of the per-uuid `apps/<uuid>/cache` dir (same
/// `apps/<uuid>/` base the runner already derives for the cache / fc rootfs) and
/// is unique per uuid, so two apps never share a disk.
#[must_use]
pub fn stateful_data_disk(data_dir: &Path, uuid: &str, runtime: &Runtime) -> Option<PathBuf> {
    if runtime.stateful {
        Some(data_dir.join("apps").join(uuid).join("data.ext4"))
    } else {
        None
    }
}

/// The guest mount path baked into `/init` for a stateful app's `/dev/vdb`.
///
/// - non-stateful → `Ok(None)`: no mount line, the rendered `/init` (and thus
///   the rootfs bytes) stay byte-identical to today.
/// - stateful WITH a non-empty `data_mount` → `Ok(Some(path))`.
/// - stateful WITHOUT a (non-empty) `data_mount` → `Err`. This is a HARD ERROR
///   by design: a stateful app whose disk attaches at `/dev/vdb` but is never
///   mounted would silently write to the EPHEMERAL rootfs and lose everything on
///   the next VM restart. Failing the bake LOUDLY (root cause, not a silent
///   footgun) is the only safe choice — a stateful app MUST declare its mount.
///
/// # Errors
/// `stateful = true` with an absent / blank `data_mount`.
pub fn stateful_data_mount(runtime: &Runtime) -> Result<Option<&str>> {
    if !runtime.stateful {
        return Ok(None);
    }
    match runtime.data_mount.as_deref() {
        Some(m) if !m.trim().is_empty() => Ok(Some(m)),
        _ => bail!(
            "app declares [runtime].stateful = true but no (non-empty) \
             [runtime].data_mount: a stateful app MUST declare its guest mount \
             path, otherwise the persistent /dev/vdb disk attaches but is never \
             mounted and the app's writes land on the ephemeral rootfs (lost on \
             the next restart)"
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A firecracker [`Runtime`] fixture with the given persistent-disk intent.
    fn rt(stateful: bool, data_mount: Option<&str>) -> Runtime {
        Runtime {
            r#type: "firecracker".to_owned(),
            entry: "Dockerfile".to_owned(),
            fuel_per_request: 0,
            memory_mb: 512,
            vcpus: Some(1),
            port: None,
            kernel: None,
            registry_ref: None,
            stateful,
            data_mount: data_mount.map(str::to_owned),
        }
    }

    #[test]
    fn non_stateful_app_gets_no_data_disk() {
        let dd = Path::new("/data");
        assert_eq!(stateful_data_disk(dd, "u1", &rt(false, None)), None);
        // A stray data_mount on a NON-stateful app is inert — still no disk.
        assert_eq!(stateful_data_disk(dd, "u1", &rt(false, Some("/x"))), None);
    }

    #[test]
    fn stateful_app_yields_per_uuid_data_ext4() {
        let dd = Path::new("/data");
        assert_eq!(
            stateful_data_disk(dd, "abc", &rt(true, Some("/var/lib/tabbify-forge"))),
            Some(PathBuf::from("/data/apps/abc/data.ext4"))
        );
        // The disk is UNIQUE per uuid (never shared between two apps).
        assert_ne!(
            stateful_data_disk(dd, "abc", &rt(true, Some("/x"))),
            stateful_data_disk(dd, "def", &rt(true, Some("/x")))
        );
    }

    #[test]
    fn non_stateful_data_mount_is_none_not_error() {
        assert_eq!(stateful_data_mount(&rt(false, None)).unwrap(), None);
        assert_eq!(stateful_data_mount(&rt(false, Some("/x"))).unwrap(), None);
    }

    #[test]
    fn stateful_with_mount_returns_it() {
        assert_eq!(
            stateful_data_mount(&rt(true, Some("/var/lib/tabbify-forge"))).unwrap(),
            Some("/var/lib/tabbify-forge")
        );
    }

    #[test]
    fn stateful_without_mount_is_a_hard_error() {
        // Absent mount → error (never a silent unmounted disk).
        assert!(stateful_data_mount(&rt(true, None)).is_err());
        // Blank / whitespace-only mount → treated as missing → error.
        assert!(stateful_data_mount(&rt(true, Some(""))).is_err());
        assert!(stateful_data_mount(&rt(true, Some("   "))).is_err());
    }
}
