use super::*;

const PROD_A: &str = "019f674a-8050-7121-91b4-3c7612090b18:[fd5a:1f00:0:3::1]:5000/n_sp5gctk4thaa/019f674a-8050-7121-91b4-3c7612090b18:7776b634da806dd2af91114ea11b451139384bc7";
const PROD_B: &str = "f4fc5fe0-dfdd-5f6c-9778-3895396b8842:[fd5a:1f00:0:3::1]:5000/platform/11b6d6f2-7ed1-5a03-adb7-69b6bd11c15c:current";

fn no_live() -> LiveLinks {
    LiveLinks::default()
}

#[test]
fn production_collision_hashes_collide_then_allocator_falls_back() {
    assert_eq!(super::super::fc_tap_name_for_key(PROD_A), "fc-7c5d2265519c");
    assert_eq!(super::super::fc_tap_name_for_key(PROD_B), "fc-5179fd44f3be");
    assert_eq!(preferred_slot(PROD_A), 15_622);
    assert_eq!(preferred_slot(PROD_A), preferred_slot(PROD_B));

    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let first = allocator.acquire(PROD_A).unwrap();
    let second = allocator.acquire(PROD_B).unwrap();
    assert_eq!(first.allocation().slot, 15_622);
    assert_eq!(second.allocation().slot, 15_623);
}

#[test]
fn forced_preferred_collision_falls_back_and_is_durable() {
    let assigned = HashSet::from([123]);
    assert_eq!(
        first_free_slot(123, &assigned, &HashSet::new()).unwrap(),
        124
    );
}

#[test]
fn reserve_adopts_expected_legacy_tap_actual_slot() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let tap = super::super::fc_tap_name_for_key(PROD_A);
    let live = LiveLinks {
        by_tap: HashMap::from([(tap, 77)]),
        slots: HashSet::from([77]),
    };
    let allocation = allocator
        .allocate(PROD_A, AllocationMode::Reserve, live, None)
        .unwrap();
    assert_eq!(allocation.slot, 77);
    assert_eq!(allocator.lookup(PROD_A).unwrap().unwrap().slot, 77);
}

#[test]
fn acquire_refuses_while_expected_tap_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let tap = super::super::fc_tap_name_for_key(PROD_A);
    let live = LiveLinks {
        by_tap: HashMap::from([(tap, 77)]),
        slots: HashSet::from([77]),
    };
    let error = allocator
        .allocate(PROD_A, AllocationMode::Acquire, live, Some("test-owner"))
        .unwrap_err();
    assert!(error.to_string().contains("still live"));
}

#[test]
fn one_live_reservation_blocks_another() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let _first = allocator.acquire(PROD_A).unwrap();
    let error = allocator.acquire(PROD_A).unwrap_err();
    assert!(error.to_string().contains("already reserved"));
}

#[test]
fn dropped_failed_reservation_permits_immediate_reacquire() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let failed = allocator.acquire(PROD_A).unwrap();
    drop(failed);
    assert!(allocator.acquire(PROD_A).is_ok());
}

#[test]
fn confirmed_reservation_permits_reacquire_after_tap_teardown() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let allocation = allocator.acquire(PROD_A).unwrap().confirm().unwrap();
    let next = allocator.acquire(PROD_A).unwrap();
    assert_eq!(next.allocation(), &allocation);
}

#[test]
fn stale_reservation_drop_cannot_clear_new_owner() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let stale = allocator.acquire(PROD_A).unwrap();
    allocator
        .mutate_state(|state| {
            state
                .assignments
                .get_mut(PROD_A)
                .unwrap()
                .launch_reserved_until_unix = 0;
            Ok(())
        })
        .unwrap();
    let current = allocator.acquire(PROD_A).unwrap();
    drop(stale);
    assert!(allocator.acquire(PROD_A).is_err());
    drop(current);
    assert!(allocator.acquire(PROD_A).is_ok());
}

#[test]
fn parses_live_tap_name_and_slot() {
    let links = parse_live_links(
        "172.31.19.9/16",
        "7: fc-7c5d2265519c inet 172.31.244.25/30 scope global fc-7c5d2265519c\n8: eth0 inet 10.0.0.2/24",
    );
    assert_eq!(links.by_tap.get("fc-7c5d2265519c"), Some(&15_622));
    assert!(links.slots.contains(&15_622));
}

#[test]
fn normalize_subnet_masks_host_bits() {
    assert_eq!(normalize_subnet("172.31.19.9/16").unwrap(), "172.31.0.0/16");
}

#[test]
fn undersized_subnet_is_rejected_before_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/17");
    let error = allocator.reserve("vm:ref").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("required for serving + build VMs")
    );
}

fn expire(allocator: &LinkSlotAllocator, vm_key: &str) {
    allocator
        .with_locked_state(|state, _| {
            state.assignments.get_mut(vm_key).unwrap().updated_at_unix = 0;
            Ok(())
        })
        .unwrap();
}

#[test]
fn expired_unowned_assignment_is_garbage_collected() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    allocator
        .allocate("old:ref", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    expire(&allocator, "old:ref");
    allocator
        .allocate("new:ref", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    assert!(allocator.lookup("old:ref").unwrap().is_none());
}

#[test]
fn matching_snapshot_retains_expired_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    let allocation = allocator
        .allocate("old:ref", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    let cache = super::super::snapshot::cache_dir(dir.path(), "old");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(super::super::snapshot::vmstate_path(&cache), b"state").unwrap();
    std::fs::write(super::super::snapshot::mem_path(&cache), b"mem").unwrap();
    super::super::snapshot::write_link(
        &cache,
        &super::super::snapshot::SnapshotLink {
            slot: allocation.slot,
            tap_subnet: allocation.tap_subnet,
            tap_name: allocation.tap_name,
        },
    );
    expire(&allocator, "old:ref");
    allocator
        .allocate("new:ref", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    assert!(allocator.lookup("old:ref").unwrap().is_some());
}

#[test]
fn only_current_runner_record_generation_retains_expired_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    allocator
        .allocate("app:old", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    allocator
        .allocate("app:current", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    expire(&allocator, "app:old");
    expire(&allocator, "app:current");
    let runners = dir.path().join("runners");
    std::fs::create_dir_all(&runners).unwrap();
    std::fs::write(
        runners.join("app.json"),
        serde_json::to_vec(&serde_json::json!({ "image_ref": "current" })).unwrap(),
    )
    .unwrap();
    allocator
        .allocate("new:ref", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    assert!(allocator.lookup("app:old").unwrap().is_none());
    assert!(allocator.lookup("app:current").unwrap().is_some());
}

#[test]
fn release_uuid_removes_all_non_live_generations() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = LinkSlotAllocator::new(dir.path(), "172.31.0.0/16");
    allocator
        .allocate("app:a", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    allocator
        .allocate("app:b", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    allocator
        .allocate("other:c", AllocationMode::Reserve, no_live(), None)
        .unwrap();
    assert_eq!(allocator.release_uuid("app").unwrap(), 2);
    assert!(allocator.lookup("app:a").unwrap().is_none());
    assert!(allocator.lookup("app:b").unwrap().is_none());
    assert!(allocator.lookup("other:c").unwrap().is_some());
}
