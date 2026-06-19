//! Pure builder for the host-side egress allow-list iptables rules (Track 7).
//!
//! Today `setup_guest_nat` installs MASQUERADE + a BLANKET `FORWARD -i <tap> -o
//! <uplink> -j ACCEPT` → the guest has unrestricted internet egress (the
//! git-proxy is a SEPARATE mediated path, not an egress block). When an
//! allow-list is supplied we instead:
//! 1. keep MASQUERADE (so allowed flows are still SNAT'd),
//! 2. ACCEPT forward to each allowed destination IP,
//! 3. DROP everything else forwarded from the tap to the uplink.
//!
//! The mesh uplink to the coordinator/relay and the git-proxy host are ALWAYS
//! allowed implicitly (the VM must stay on the mesh + be able to push/pull) —
//! the caller passes those in `always_allow`.
//!
//! Rules are returned as `(check_args, add_args)` pairs for the existing
//! idempotent `ensure_iptables` helper, so this module stays pure (no iptables
//! exec, no async) and is unit-testable on macOS.

/// One `(iptables -C …, iptables -A/-I …)` pair.
pub type IptablesRule = (Vec<String>, Vec<String>);

/// Build the egress rules for a tap whose guest may reach only `allow_ips`
/// (already resolved to IP literals) plus `always_allow` (mesh uplink + git-proxy
/// host IPs). An EMPTY `allow_ips` AND empty `always_allow` ⇒ return empty (the
/// caller falls back to the legacy blanket-ACCEPT — unrestricted).
#[must_use]
pub fn egress_rules(
    tap: &str,
    uplink: &str,
    allow_ips: &[String],
    always_allow: &[String],
) -> Vec<IptablesRule> {
    let mut out = Vec::new();
    if allow_ips.is_empty() && always_allow.is_empty() {
        return out; // no allow-list → caller keeps blanket behavior
    }
    // ACCEPT forward to each allowed destination (inserted at head so they
    // precede the catch-all DROP and any docker-managed policy).
    for ip in allow_ips.iter().chain(always_allow.iter()) {
        out.push((
            svec(&[
                "-C", "FORWARD", "-i", tap, "-o", uplink, "-d", ip, "-j", "ACCEPT",
            ]),
            svec(&[
                "-I", "FORWARD", "1", "-i", tap, "-o", uplink, "-d", ip, "-j", "ACCEPT",
            ]),
        ));
    }
    // Allow return traffic for already-established flows.
    out.push((
        svec(&[
            "-C", "FORWARD", "-i", uplink, "-o", tap, "-m", "state", "--state",
            "RELATED,ESTABLISHED", "-j", "ACCEPT",
        ]),
        svec(&[
            "-I", "FORWARD", "1", "-i", uplink, "-o", tap, "-m", "state", "--state",
            "RELATED,ESTABLISHED", "-j", "ACCEPT",
        ]),
    ));
    // Catch-all DROP for everything else forwarded out of the tap (APPENDED so it
    // is the LAST FORWARD rule for this tap — the ACCEPTs above win).
    out.push((
        svec(&["-C", "FORWARD", "-i", tap, "-o", uplink, "-j", "DROP"]),
        svec(&["-A", "FORWARD", "-i", tap, "-o", uplink, "-j", "DROP"]),
    ));
    out
}

fn svec(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| (*s).to_owned()).collect()
}

/// Resolve each allow-list `host` (DNS name, IP literal, or CIDR) to the IP
/// literal(s) the iptables `-d` rules need. DNS-pinning: we resolve once at rule
/// install. A name that does not resolve is skipped with a warning (it simply
/// stays unreachable — fail-closed). IP/CIDR literals pass through unchanged.
///
/// `tokio::net::lookup_host` is available on all targets, so no cfg attr is
/// strictly required; the caller (`setup_guest_nat`) is Linux-only.
pub async fn resolve_hosts(hosts: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for h in hosts {
        // IP or CIDR literal → pass through.
        if h.parse::<std::net::IpAddr>().is_ok() || h.contains('/') {
            out.push(h.clone());
            continue;
        }
        // DNS name → A/AAAA. Append :0 so lookup_host parses it as host:port.
        match tokio::net::lookup_host(format!("{h}:0")).await {
            Ok(addrs) => {
                for a in addrs {
                    out.push(a.ip().to_string());
                }
            }
            Err(e) => {
                tracing::warn!(host = %h, error = %e, "egress: host did not resolve; staying blocked");
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allow_list_yields_no_rules() {
        assert!(egress_rules("tap0", "eth0", &[], &[]).is_empty());
    }

    #[test]
    fn builds_accept_per_ip_plus_catch_all_drop() {
        let rules = egress_rules(
            "tap0",
            "eth0",
            &["149.154.167.220".into()],
            &["10.0.0.1".into()], // mesh uplink
        );
        // 2 ACCEPTs (allow + always) + 1 ESTABLISHED + 1 DROP.
        assert_eq!(rules.len(), 4);
        // The last add-rule is the catch-all DROP.
        let last_add = &rules.last().unwrap().1;
        assert!(last_add.contains(&"DROP".to_string()));
        assert!(last_add.contains(&"-A".to_string()));
        // An allowed dst appears in an ACCEPT add-rule.
        assert!(rules.iter().any(|(_, add)| add.contains(&"149.154.167.220".to_string())
            && add.contains(&"ACCEPT".to_string())));
    }

    #[test]
    fn always_allow_is_accepted_even_without_user_hosts() {
        let rules = egress_rules("tap0", "eth0", &[], &["10.0.0.1".into()]);
        assert!(rules.iter().any(|(_, add)| add.contains(&"10.0.0.1".to_string())
            && add.contains(&"ACCEPT".to_string())));
        // Still a catch-all DROP, so non-mesh egress is closed.
        assert!(rules.last().unwrap().1.contains(&"DROP".to_string()));
    }

    #[test]
    fn resolve_passes_through_ip_literals() {
        // Synchronous wrapper around the async helper (no DNS — pure pass-through).
        let out = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(resolve_hosts(&["1.2.3.4".into(), "10.0.0.0/24".into()]));
        assert_eq!(out, vec!["1.2.3.4".to_string(), "10.0.0.0/24".to_string()]);
    }
}
