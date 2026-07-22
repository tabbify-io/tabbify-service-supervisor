//! Per-runner WireGuard listen-port allocation.
//!
//! ## Why runners cannot share a port
//!
//! Every joiner defaults to [`DEFAULT_RUNNER_WG_PORT_BASE`]-adjacent `51820`,
//! and the joiner sets `SO_REUSEPORT`, so N co-resident runners all bound
//! `0.0.0.0:51820` SUCCESSFULLY. Linux then load-balances inbound UDP across
//! those sockets by 4-tuple hash, so a handshake response reaches the joiner
//! that actually owns the session with roughly 1-in-N luck; the rest are
//! decrypted against the wrong `Tunn` and silently dropped. Measured on the
//! dedik: the supervisor plus all 16 runners on `0.0.0.0:51820`.
//!
//! The joiner already documents this ("CO-RESIDENCE CAVEAT"): co-resident
//! joiners MUST be given distinct `--listen-port` values. Nothing was giving
//! them one. This module is that allocator.
//!
//! ## Why supervisor-assigned and persisted, not ephemeral
//!
//! Binding `:0` would also produce distinct ports, and the joiner advertises the
//! port it actually bound (`local_addr()`), so it would be self-consistent. It
//! was rejected because the port would change on EVERY respawn: the coordinator
//! caches each peer's `listen_endpoint`, peers cache dial targets, and a
//! port-preserving NAT keys its mapping on the source port. A respawn would
//! silently invalidate every peer's dial target until the next heartbeat
//! propagated — endpoint churn of exactly the kind that aborted the direct
//! canary.
//!
//! A persisted per-runner port is stable across respawn, mirroring how the app
//! ULA is already deterministic per uuid: same app, same address, same port.

/// First port handed to a runner. The supervisor's own joiner keeps the
/// WireGuard-conventional `51820`, so runners start one above it.
pub const RUNNER_WG_PORT_BASE: u16 = 51821;

/// How many consecutive ports the runner pool spans (`51821..=52076`).
///
/// Comfortably above any realistic per-host runner count (16 on the dedik
/// today) while staying a bounded, greppable window for `ss -ulpn`.
pub const RUNNER_WG_PORT_COUNT: u16 = 256;

/// One past the last runner port.
const RUNNER_WG_PORT_END: u16 = RUNNER_WG_PORT_BASE + RUNNER_WG_PORT_COUNT;

/// Pick the lowest free port in the runner pool, given the ports already
/// `taken` by other runner records on this host.
///
/// Lowest-free (rather than next-after-highest) keeps the pool dense, so the
/// ports a human sees in `ss -ulpn` stay in a tight, recognizable band as
/// runners come and go.
///
/// Returns `None` when the pool is exhausted. The caller treats that as "spawn
/// without an explicit port" rather than failing the spawn: the runner then
/// falls back to the joiner's own bind path, which is degraded (it may collide)
/// but never worse than today's behavior.
#[must_use]
pub fn allocate_wg_port(taken: &[u16]) -> Option<u16> {
    (RUNNER_WG_PORT_BASE..RUNNER_WG_PORT_END).find(|port| !taken.contains(port))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn first_allocation_is_the_pool_base() {
        assert_eq!(allocate_wg_port(&[]), Some(RUNNER_WG_PORT_BASE));
    }

    /// The supervisor's own joiner owns the WireGuard-conventional 51820. No
    /// allocation may ever return it, or the fleet reproduces the exact
    /// collision this module exists to remove.
    #[test]
    fn never_hands_out_the_supervisors_own_port() {
        const SUPERVISOR_PORT: u16 = 51_820;
        for taken_count in 0..8u16 {
            let taken: Vec<u16> = (0..taken_count).map(|i| RUNNER_WG_PORT_BASE + i).collect();
            assert_ne!(allocate_wg_port(&taken), Some(SUPERVISOR_PORT));
        }
        // …and the pool as a whole never contains it.
        assert!(!(RUNNER_WG_PORT_BASE..RUNNER_WG_PORT_END).contains(&SUPERVISOR_PORT));
    }

    #[test]
    fn skips_ports_already_taken_and_fills_the_lowest_gap() {
        let taken = vec![RUNNER_WG_PORT_BASE, RUNNER_WG_PORT_BASE + 1];
        assert_eq!(allocate_wg_port(&taken), Some(RUNNER_WG_PORT_BASE + 2));

        // A freed port in the middle is reused before extending the high end.
        let sparse = vec![RUNNER_WG_PORT_BASE, RUNNER_WG_PORT_BASE + 2];
        assert_eq!(allocate_wg_port(&sparse), Some(RUNNER_WG_PORT_BASE + 1));
    }

    #[test]
    fn allocations_are_unique_across_a_full_fleet() {
        // The property that matters: allocate repeatedly, feeding each result
        // back as taken, and no port may ever repeat.
        let mut taken: Vec<u16> = Vec::new();
        for _ in 0..64 {
            let port = allocate_wg_port(&taken).expect("pool must cover a 64-runner fleet");
            assert!(!taken.contains(&port), "allocator handed out {port} twice");
            taken.push(port);
        }
    }

    #[test]
    fn exhausted_pool_yields_none_rather_than_a_colliding_port() {
        let taken: Vec<u16> = (RUNNER_WG_PORT_BASE..RUNNER_WG_PORT_END).collect();
        assert_eq!(allocate_wg_port(&taken), None);
    }

    #[test]
    fn pool_stays_inside_the_u16_range() {
        // A base+count that overflowed would wrap and hand out low, privileged
        // ports; pin the arithmetic.
        assert!(
            RUNNER_WG_PORT_BASE
                .checked_add(RUNNER_WG_PORT_COUNT)
                .is_some()
        );
        assert_eq!(RUNNER_WG_PORT_END, 52_077);
    }
}
