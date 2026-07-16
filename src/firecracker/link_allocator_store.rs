//! Filesystem persistence and live-interface discovery for the link allocator.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{AssignmentFile, LiveLinks, SERVING_LINK_SLOTS, normalize_subnet};

pub(super) fn open_lock(root: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(root.join("allocator.lock"))
        .context("open FC link allocator lock")
}

pub(super) fn lock_exclusive(file: &File) -> Result<()> {
    // SAFETY: `file` owns a valid descriptor for the duration of the call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("flock FC link allocator")
    }
}

fn state_path(root: &Path) -> PathBuf {
    root.join("assignments.json")
}

pub(super) fn read_state(root: &Path) -> Result<AssignmentFile> {
    let path = state_path(root);
    let mut raw = String::new();
    match File::open(&path) {
        Ok(mut file) => {
            file.read_to_string(&mut raw)
                .with_context(|| format!("read {}", path.display()))?;
            serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(AssignmentFile::default()),
        Err(error) => Err(error).with_context(|| format!("open {}", path.display())),
    }
}

pub(super) fn write_state(root: &Path, state: &AssignmentFile) -> Result<()> {
    let path = state_path(root);
    let tmp = root.join(format!("assignments.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(&serde_json::to_vec(state)?)?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))
}

pub(super) fn live_links(tap_subnet: &str) -> Result<LiveLinks> {
    #[cfg(target_os = "linux")]
    {
        let output = std::process::Command::new("ip")
            .args(["-o", "-4", "addr", "show"])
            .output()
            .context("run `ip -o -4 addr show` for FC link migration")?;
        if !output.status.success() {
            anyhow::bail!("`ip -o -4 addr show` failed with {}", output.status);
        }
        Ok(parse_live_links(
            tap_subnet,
            &String::from_utf8_lossy(&output.stdout),
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = tap_subnet;
        Ok(LiveLinks::default())
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) fn parse_live_links(tap_subnet: &str, output: &str) -> LiveLinks {
    let base = normalize_subnet(tap_subnet)
        .ok()
        .and_then(|s| s.split('/').next()?.parse::<Ipv4Addr>().ok())
        .map(u32::from);
    let Some(base) = base else {
        return LiveLinks::default();
    };
    let mut links = LiveLinks::default();
    for line in output.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(tap) = fields.get(1).map(|v| v.trim_end_matches(':')) else {
            continue;
        };
        let Some(ip) = fields
            .iter()
            .position(|v| *v == "inet")
            .and_then(|i| fields.get(i + 1))
            .and_then(|v| v.split('/').next())
            .and_then(|v| v.parse::<Ipv4Addr>().ok())
        else {
            continue;
        };
        let Some(offset) = u32::from(ip).checked_sub(base) else {
            continue;
        };
        if offset % 4 != 1 {
            continue;
        }
        let slot = offset / 4;
        if slot < SERVING_LINK_SLOTS {
            links.by_tap.insert((*tap).to_owned(), slot);
            links.slots.insert(slot);
        }
    }
    links
}
