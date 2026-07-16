//! Host-wide allocation of Firecracker `/30` link slots.
//!
//! Detached app runners coordinate through a filesystem `flock` under the
//! shared data directory. Assignments are durable per `vm_key`, but stale
//! reservations are reclaimed when no live TAP, snapshot, or runner record owns
//! them.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

#[path = "link_allocator_store.rs"]
mod store;
#[cfg(test)]
use store::parse_live_links;
use store::{live_links, lock_exclusive, open_lock, read_state, write_state};

/// Number of `/30` slots available to serving VMs. The final usable `/30` is
/// reserved for the build VM.
pub const SERVING_LINK_SLOTS: u32 = 16_382;
const RESERVATION_TTL_SECS: u64 = 15 * 60;
const LAUNCH_RESERVATION_SECS: u64 = 2 * 60;

/// Normalize an IPv4 CIDR by masking host bits and rendering its prefix.
pub fn normalize_subnet(subnet: &str) -> Result<String> {
    let (ip, prefix) = subnet
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid tap subnet {subnet:?}"))?;
    let ip = ip
        .parse::<Ipv4Addr>()
        .with_context(|| format!("invalid tap subnet address {ip:?}"))?;
    let prefix = prefix
        .parse::<u8>()
        .with_context(|| format!("invalid tap subnet prefix {prefix:?}"))?;
    if prefix > 30 {
        bail!("tap subnet prefix /{prefix} cannot contain /30 links");
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    };
    Ok(format!(
        "{}/{}",
        Ipv4Addr::from(u32::from(ip) & mask),
        prefix
    ))
}

/// Derive the host and guest addresses for one allocated `/30` slot.
pub fn link_ips(subnet: &str, slot: u32) -> Result<(Ipv4Addr, Ipv4Addr)> {
    let normalized = normalize_subnet(subnet)?;
    let capacity = subnet_slot_capacity(&normalized)?;
    if u64::from(slot) >= capacity {
        bail!("tap subnet {normalized} has {capacity} /30 slots; slot {slot} is out of range");
    }
    let base = normalized
        .split('/')
        .next()
        .and_then(|value| value.parse::<Ipv4Addr>().ok())
        .ok_or_else(|| anyhow!("invalid normalized tap subnet {normalized:?}"))?;
    let host = u32::from(base)
        .checked_add(slot * 4 + 1)
        .ok_or_else(|| anyhow!("tap subnet exhausted at slot {slot}"))?;
    Ok((Ipv4Addr::from(host), Ipv4Addr::from(host + 1)))
}

fn subnet_slot_capacity(subnet: &str) -> Result<u64> {
    let prefix = normalize_subnet(subnet)?
        .split_once('/')
        .and_then(|(_, prefix)| prefix.parse::<u8>().ok())
        .ok_or_else(|| anyhow!("invalid normalized tap subnet {subnet:?}"))?;
    Ok(1_u64 << (30 - u32::from(prefix)))
}

fn validate_subnet_capacity(subnet: &str) -> Result<()> {
    let normalized = normalize_subnet(subnet)?;
    let capacity = subnet_slot_capacity(&normalized)?;
    let required = u64::from(SERVING_LINK_SLOTS) + 1;
    if capacity < required {
        bail!(
            "tap subnet {normalized} has {capacity} /30 slots; {required} required for serving + build VMs"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAssignment {
    slot: u32,
    updated_at_unix: u64,
    #[serde(default)]
    launch_reserved_until_unix: u64,
    #[serde(default)]
    launch_owner: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AssignmentFile {
    #[serde(default)]
    assignments: HashMap<String, StoredAssignment>,
}

/// A durable host link assignment for one Firecracker `vm_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkAllocation {
    /// `/30` index within the configured tap subnet.
    pub slot: u32,
    /// Deterministic TAP name used by this VM generation.
    pub tap_name: String,
    /// Normalized tap subnet used to derive the link addresses.
    pub tap_subnet: String,
}

/// RAII launch claim. Dropping an unconfirmed claim immediately releases only
/// the matching owner token, including when an async launch future is cancelled.
#[derive(Debug)]
pub struct LinkReservation {
    allocator: LinkSlotAllocator,
    vm_key: String,
    owner: String,
    allocation: LinkAllocation,
    active: bool,
}

impl LinkReservation {
    /// Allocation held by this launch claim.
    pub fn allocation(&self) -> &LinkAllocation {
        &self.allocation
    }

    /// Confirm a successfully-created runtime and clear the transient launch
    /// claim. The durable allocation remains owned by TAP/snapshot/record state.
    pub fn confirm(mut self) -> Result<LinkAllocation> {
        self.allocator
            .clear_launch_owner(&self.vm_key, &self.owner)?;
        self.active = false;
        Ok(self.allocation.clone())
    }
}

impl Drop for LinkReservation {
    fn drop(&mut self) {
        if self.active {
            if let Err(error) = self.allocator.clear_launch_owner(&self.vm_key, &self.owner) {
                tracing::warn!(vm_key = %self.vm_key, %error, "failed to release dropped FC launch reservation");
            }
        }
    }
}

#[derive(Debug, Default)]
struct LiveLinks {
    by_tap: HashMap<String, u32>,
    slots: HashSet<u32>,
}

/// Host-wide allocator backed by `<data_dir>/fc-links/assignments.json`.
#[derive(Debug, Clone)]
pub struct LinkSlotAllocator {
    data_dir: PathBuf,
    root: PathBuf,
    tap_subnet: String,
}

impl LinkSlotAllocator {
    /// Construct an allocator over the data directory shared by supervisord and
    /// all detached runner processes.
    pub fn new(data_dir: &Path, tap_subnet: &str) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            root: data_dir.join("fc-links"),
            tap_subnet: tap_subnet.to_owned(),
        }
    }

    /// Reserve or look up a link for API-side dev/workspace routing.
    ///
    /// A live deterministic legacy TAP is adopted at its ACTUAL address so an
    /// API lookup never silently reroutes an already-running VM.
    pub fn reserve(&self, vm_key: &str) -> Result<LinkAllocation> {
        validate_subnet_capacity(&self.tap_subnet)?;
        self.allocate(
            vm_key,
            AllocationMode::Reserve,
            live_links(&self.tap_subnet)?,
            None,
        )
    }

    /// Acquire a link for a new Firecracker launch.
    ///
    /// Unlike [`Self::reserve`], this refuses while the deterministic TAP is
    /// still live. This closes the A -> B -> A delayed-drain race: generation A
    /// cannot delete/recreate its TAP while the old A is still draining.
    pub fn acquire(&self, vm_key: &str) -> Result<LinkReservation> {
        validate_subnet_capacity(&self.tap_subnet)?;
        let owner = format!("{}:{}", std::process::id(), uuid::Uuid::new_v4());
        let allocation = self.allocate(
            vm_key,
            AllocationMode::Acquire,
            live_links(&self.tap_subnet)?,
            Some(&owner),
        )?;
        Ok(LinkReservation {
            allocator: self.clone(),
            vm_key: vm_key.to_owned(),
            owner,
            allocation,
            active: true,
        })
    }

    /// Read an existing assignment without creating or refreshing it.
    #[cfg(test)]
    pub fn lookup(&self, vm_key: &str) -> Result<Option<LinkAllocation>> {
        validate_subnet_capacity(&self.tap_subnet)?;
        self.with_locked_state(|state, _| {
            Ok(state
                .assignments
                .get(vm_key)
                .map(|stored| self.allocation(vm_key, stored.slot)))
        })
    }

    /// Release every assignment belonging to `uuid` after a successful purge.
    pub fn release_uuid(&self, uuid: &str) -> Result<usize> {
        validate_subnet_capacity(&self.tap_subnet)?;
        if !self.root.exists() {
            return Ok(0);
        }
        self.with_locked_state(|state, live| {
            let prefix = format!("{uuid}:");
            let before = state.assignments.len();
            state.assignments.retain(|key, assignment| {
                if key == uuid || key.starts_with(&prefix) {
                    let tap = super::fc_tap_name_for_key(key);
                    if live.by_tap.get(&tap) == Some(&assignment.slot) {
                        tracing::warn!(
                            uuid,
                            vm_key = key,
                            tap,
                            "refusing to release live FC link assignment"
                        );
                        return true;
                    }
                    return false;
                }
                true
            });
            Ok(before - state.assignments.len())
        })
    }

    fn allocate(
        &self,
        vm_key: &str,
        mode: AllocationMode,
        live: LiveLinks,
        launch_owner: Option<&str>,
    ) -> Result<LinkAllocation> {
        self.with_locked_state_using(live, |state, live| {
            self.gc(state, live, Some(vm_key));
            let tap_name = super::fc_tap_name_for_key(vm_key);
            let live_expected = live.by_tap.get(&tap_name).copied();
            if mode == AllocationMode::Acquire && live_expected.is_some() {
                bail!("Firecracker TAP {tap_name} for {vm_key} is still live; refusing generation reuse");
            }
            if mode == AllocationMode::Acquire
                && state
                    .assignments
                    .get(vm_key)
                    .is_some_and(|stored| stored.launch_reserved_until_unix > now_unix())
            {
                bail!("Firecracker launch for {vm_key} is already reserved by another runner");
            }

            if let Some(actual_slot) = live_expected {
                if state.assignments.iter().any(|(key, assignment)| {
                    key != vm_key && assignment.slot == actual_slot
                }) {
                    bail!("live legacy TAP {tap_name} uses slot {actual_slot}, already assigned to another vm_key");
                }
                let (launch_reserved_until_unix, launch_owner) = state
                    .assignments
                    .get(vm_key)
                    .map(|stored| {
                        (
                            stored.launch_reserved_until_unix,
                            stored.launch_owner.clone(),
                        )
                    })
                    .unwrap_or_default();
                state.assignments.insert(
                    vm_key.to_owned(),
                    StoredAssignment {
                        slot: actual_slot,
                        updated_at_unix: now_unix(),
                        launch_reserved_until_unix,
                        launch_owner,
                    },
                );
                return Ok(self.allocation(vm_key, actual_slot));
            }

            if let Some(existing) = state.assignments.get_mut(vm_key) {
                if !live.slots.contains(&existing.slot) {
                    existing.updated_at_unix = now_unix();
                    if mode == AllocationMode::Acquire {
                        existing.launch_reserved_until_unix =
                            now_unix().saturating_add(LAUNCH_RESERVATION_SECS);
                        existing.launch_owner = launch_owner.map(str::to_owned);
                    }
                    return Ok(self.allocation(vm_key, existing.slot));
                }
            }

            let preferred = preferred_slot(vm_key);
            let assigned: HashSet<u32> = state.assignments.values().map(|a| a.slot).collect();
            let slot = first_free_slot(preferred, &assigned, &live.slots)?;
            state.assignments.insert(
                vm_key.to_owned(),
                StoredAssignment {
                    slot,
                    updated_at_unix: now_unix(),
                    launch_reserved_until_unix: if mode == AllocationMode::Acquire {
                        now_unix().saturating_add(LAUNCH_RESERVATION_SECS)
                    } else {
                        0
                    },
                    launch_owner: launch_owner.map(str::to_owned),
                },
            );
            tracing::info!(vm_key, slot, preferred, "allocated durable FC link slot");
            Ok(self.allocation(vm_key, slot))
        })
    }

    fn clear_launch_owner(&self, vm_key: &str, owner: &str) -> Result<()> {
        self.mutate_state(|state| {
            let stored = state
                .assignments
                .get_mut(vm_key)
                .ok_or_else(|| anyhow!("FC link assignment disappeared for {vm_key}"))?;
            if stored.launch_owner.as_deref() != Some(owner) {
                bail!("FC launch reservation owner changed for {vm_key}");
            }
            stored.launch_owner = None;
            stored.launch_reserved_until_unix = 0;
            Ok(())
        })
    }

    fn allocation(&self, vm_key: &str, slot: u32) -> LinkAllocation {
        LinkAllocation {
            slot,
            tap_name: super::fc_tap_name_for_key(vm_key),
            tap_subnet: normalize_subnet(&self.tap_subnet)
                .unwrap_or_else(|_| self.tap_subnet.clone()),
        }
    }

    fn gc(&self, state: &mut AssignmentFile, live: &LiveLinks, preserve: Option<&str>) {
        let now = now_unix();
        state.assignments.retain(|vm_key, assignment| {
            vm_key == preserve.unwrap_or_default()
                || live.by_tap.get(&super::fc_tap_name_for_key(vm_key)) == Some(&assignment.slot)
                || self.snapshot_owns(vm_key, assignment.slot)
                || self.runner_record_owns(vm_key)
                || now.saturating_sub(assignment.updated_at_unix) <= RESERVATION_TTL_SECS
        });
    }

    fn snapshot_owns(&self, vm_key: &str, slot: u32) -> bool {
        let uuid = vm_uuid(vm_key);
        let cache = super::snapshot::cache_dir(&self.data_dir, uuid);
        super::snapshot::files_present(&cache)
            && super::snapshot::read_link(&cache).is_some_and(|meta| {
                meta.slot == slot
                    && meta.tap_name == super::fc_tap_name_for_key(vm_key)
                    && meta.tap_subnet == normalize_subnet(&self.tap_subnet).unwrap_or_default()
            })
    }

    fn runner_record_owns(&self, vm_key: &str) -> bool {
        let path = self
            .data_dir
            .join("runners")
            .join(format!("{}.json", vm_uuid(vm_key)));
        let Ok(raw) = std::fs::read(&path) else {
            return false;
        };
        let Ok(record) = serde_json::from_slice::<serde_json::Value>(&raw) else {
            return false;
        };
        match record.get("image_ref").and_then(serde_json::Value::as_str) {
            Some(reff) => vm_key == format!("{}:{reff}", vm_uuid(vm_key)),
            None => vm_key == vm_uuid(vm_key),
        }
    }

    fn with_locked_state<T>(
        &self,
        f: impl FnOnce(&mut AssignmentFile, &LiveLinks) -> Result<T>,
    ) -> Result<T> {
        self.with_locked_state_using(live_links(&self.tap_subnet)?, f)
    }

    fn with_locked_state_using<T>(
        &self,
        live: LiveLinks,
        f: impl FnOnce(&mut AssignmentFile, &LiveLinks) -> Result<T>,
    ) -> Result<T> {
        std::fs::create_dir_all(&self.root)
            .with_context(|| format!("create FC link allocator dir {}", self.root.display()))?;
        let lock = open_lock(&self.root)?;
        lock_exclusive(&lock)?;
        let mut state = read_state(&self.root)?;
        let result = f(&mut state, &live)?;
        write_state(&self.root, &state)?;
        Ok(result)
    }

    fn mutate_state<T>(&self, f: impl FnOnce(&mut AssignmentFile) -> Result<T>) -> Result<T> {
        std::fs::create_dir_all(&self.root)
            .with_context(|| format!("create FC link allocator dir {}", self.root.display()))?;
        let lock = open_lock(&self.root)?;
        lock_exclusive(&lock)?;
        let mut state = read_state(&self.root)?;
        let result = f(&mut state)?;
        write_state(&self.root, &state)?;
        Ok(result)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AllocationMode {
    Reserve,
    Acquire,
}

fn vm_uuid(vm_key: &str) -> &str {
    vm_key.split(':').next().unwrap_or(vm_key)
}

/// Historical hash preference, exposed for regression tests.
pub(crate) fn preferred_slot(vm_key: &str) -> u32 {
    let digest = blake3::hash(vm_key.as_bytes());
    let b = digest.as_bytes();
    let hash48 = (u64::from(b[0]) << 40)
        | (u64::from(b[1]) << 32)
        | (u64::from(b[2]) << 24)
        | (u64::from(b[3]) << 16)
        | (u64::from(b[4]) << 8)
        | u64::from(b[5]);
    u32::try_from(hash48 % u64::from(SERVING_LINK_SLOTS)).unwrap_or(0)
}

fn first_free_slot(preferred: u32, assigned: &HashSet<u32>, live: &HashSet<u32>) -> Result<u32> {
    (0..SERVING_LINK_SLOTS)
        .map(|offset| (preferred + offset) % SERVING_LINK_SLOTS)
        .find(|slot| !assigned.contains(slot) && !live.contains(slot))
        .ok_or_else(|| anyhow!("Firecracker serving link slots exhausted"))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
#[path = "link_allocator_tests.rs"]
mod tests;
