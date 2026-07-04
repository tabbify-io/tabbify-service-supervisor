//! Pure iptables arg builder for the IPv4 tap-gateway proxy firewalls (git-proxy
//! `:8788` and forge-proxy `:8789`).
//!
//! Both proxies bind `0.0.0.0:<port>` so an IPv4-only FC guest can reach them via
//! its tap default gateway — which also makes the port reachable on the host's
//! WiFi uplink. The firewall closes that exposure as depth-in-defence: ACCEPT
//! inbound from the FC tap subnet, DROP inbound on the uplink interface. The
//! primary guard is elsewhere (the git-proxy's 256-bit capability; the mesh ACL
//! for the forge) — the firewall is best-effort.
//!
//! This module holds ONLY the pure rule-argument construction (NO cfg gate, so
//! the rule logic is unit-testable on macOS), mirroring `egress_filter`. The
//! actual `iptables` shell-out that consumes these lives in
//! `linux.rs::setup_proxy_ipv4_firewall` (behind the Linux gate).

/// The four idempotent `iptables` INPUT-rule arg vectors that guard a proxy IPv4
/// port: the tap-subnet ACCEPT (`-C` check + `-I` add) and the uplink DROP (`-C`
/// check + `-I` add). Each `check`/`add` pair feeds `ensure_iptables` (run `-C`;
/// if absent, run `-I`). Borrows its string inputs — the caller owns the backing
/// `port` string, tap subnet, and uplink name.
#[derive(Debug)]
pub struct ProxyFirewallRules<'a> {
    /// `iptables -C INPUT -s <subnet> -p tcp --dport <port> -j ACCEPT`.
    pub accept_check: Vec<&'a str>,
    /// `iptables -I INPUT 1 -s <subnet> -p tcp --dport <port> -j ACCEPT` — head
    /// of the chain so it precedes the DROP (position 2).
    pub accept_add: Vec<&'a str>,
    /// `iptables -C INPUT -i <uplink> -p tcp --dport <port> -j DROP`.
    pub drop_check: Vec<&'a str>,
    /// `iptables -I INPUT 2 -i <uplink> -p tcp --dport <port> -j DROP` — after
    /// the ACCEPT so tap traffic is still allowed.
    pub drop_add: Vec<&'a str>,
}

/// Build the [`ProxyFirewallRules`] for `port_str` (the decimal port): ACCEPT
/// from `tap_subnet`, DROP on `uplink`. Pure + isolated so the exact `iptables`
/// argv is unit-testable without touching the host firewall.
///
/// SAFETY ORDERING (enforced by the caller, `setup_proxy_ipv4_firewall`): the
/// ACCEPT is installed at INPUT position 1 and the DROP at position 2, and the
/// DROP is NEVER installed unless the ACCEPT succeeded first — a position-1 DROP
/// without the preceding ACCEPT would also drop tap traffic and silently break
/// the guest.
#[must_use]
pub fn proxy_ipv4_firewall_rules<'a>(
    tap_subnet: &'a str,
    uplink: &'a str,
    port_str: &'a str,
) -> ProxyFirewallRules<'a> {
    ProxyFirewallRules {
        accept_check: vec![
            "-C", "INPUT", "-s", tap_subnet, "-p", "tcp", "--dport", port_str, "-j", "ACCEPT",
        ],
        accept_add: vec![
            "-I", "INPUT", "1", "-s", tap_subnet, "-p", "tcp", "--dport", port_str, "-j", "ACCEPT",
        ],
        drop_check: vec![
            "-C", "INPUT", "-i", uplink, "-p", "tcp", "--dport", port_str, "-j", "DROP",
        ],
        drop_add: vec![
            "-I", "INPUT", "2", "-i", uplink, "-p", "tcp", "--dport", port_str, "-j", "DROP",
        ],
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The tap-subnet ACCEPT lands at INPUT position 1 and matches the subnet +
    /// port; the uplink DROP lands at position 2 and matches the uplink iface +
    /// port. This is the exact argv the git/forge proxy firewalls install.
    #[test]
    fn rules_carry_subnet_uplink_port_and_positions() {
        let r = proxy_ipv4_firewall_rules("172.31.0.0/16", "wlan0", "8789");

        assert_eq!(
            r.accept_add,
            vec![
                "-I", "INPUT", "1", "-s", "172.31.0.0/16", "-p", "tcp", "--dport", "8789", "-j",
                "ACCEPT",
            ],
            "ACCEPT must be at position 1 (before the DROP), keyed on the tap subnet + port",
        );
        assert_eq!(
            r.drop_add,
            vec![
                "-I", "INPUT", "2", "-i", "wlan0", "-p", "tcp", "--dport", "8789", "-j", "DROP",
            ],
            "DROP must be at position 2 (after the ACCEPT), keyed on the uplink iface + port",
        );
        // The check (`-C`) forms mirror the add (`-I`) forms sans the position,
        // so `ensure_iptables` de-dupes correctly.
        assert_eq!(r.accept_check[0], "-C");
        assert!(r.accept_check.contains(&"172.31.0.0/16"));
        assert!(r.accept_check.contains(&"8789"));
        assert_eq!(r.drop_check[0], "-C");
        assert!(r.drop_check.contains(&"wlan0"));
        assert!(r.drop_check.contains(&"8789"));
    }

    /// The port threads through verbatim — the git-proxy (8788) and forge-proxy
    /// (8789) get distinct rules from the same builder.
    #[test]
    fn port_threads_through_for_both_proxies() {
        let git = proxy_ipv4_firewall_rules("172.31.0.0/16", "eth0", "8788");
        assert!(git.accept_add.contains(&"8788"));
        assert!(git.drop_add.contains(&"8788"));
        let forge = proxy_ipv4_firewall_rules("172.31.0.0/16", "eth0", "8789");
        assert!(forge.accept_add.contains(&"8789"));
        assert!(forge.drop_add.contains(&"8789"));
    }
}
