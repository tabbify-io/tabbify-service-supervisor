//! Shared spawn policy for the external CLI tools the build path drives
//! (`oras` / `skopeo` / `tar` / `mkfs.ext4`).
//!
//! Two clean-install hazards live here so every spawn site treats them
//! identically:
//!
//! 1. **`HOME`** — a clean systemd unit may leave `HOME` unset, and `oras`
//!    aborts with `failed to get user home directory: $HOME is not defined`
//!    before it does any work. We force a valid `HOME` on every tool spawn so
//!    the docker-less build path works regardless of how the supervisor was
//!    installed (the NixOS module sets it; the imperative installer did not).
//! 2. **Transient registry I/O over the relay** — the mesh registry is
//!    relay-only, so `oras` pull/push traverses the DERP relay; a large blob can
//!    transiently fail (the registry proxy returns 502 when the forwarded body
//!    breaks). `oras` copy/push are idempotent, so we retry them a bounded
//!    number of times. Local tools (`tar`, `mkfs.ext4`, the `skopeo`
//!    daemon→layout step) are deterministic and never retried.

use std::ffi::OsString;
use std::time::Duration;

/// `HOME` for a spawned tool: the inherited value when set + non-empty, else
/// `/root` (the supervisor runs as root for `/dev/kvm` access). Without this
/// `oras` aborts: `failed to get user home directory: $HOME is not defined`.
#[must_use]
pub fn tool_home() -> OsString {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| OsString::from("/root"))
}

/// Attempts for an idempotent network-tool invocation (the first try plus
/// retries). Bounds the worst-case latency of a genuinely-broken op.
pub const NET_TOOL_ATTEMPTS: u32 = 3;

/// Whether a tool's failure is worth retrying. ONLY `oras` — it is the only tool
/// that talks to the (relay-only, flaky-for-large-blobs) mesh registry.
/// `tar` / `mkfs.ext4` / `skopeo`(local daemon→layout) are deterministic, so
/// retrying them would only delay surfacing a real failure.
#[must_use]
pub fn is_net_tool(prog: &str) -> bool {
    prog == "oras"
}

/// Number of attempts to make for `prog`: [`NET_TOOL_ATTEMPTS`] for a network
/// tool, otherwise exactly one.
#[must_use]
pub fn attempts_for(prog: &str) -> u32 {
    if is_net_tool(prog) {
        NET_TOOL_ATTEMPTS
    } else {
        1
    }
}

/// Linear backoff before retry: `500ms * attempt` (attempt is 1-based — the wait
/// AFTER the 1st failed try, BEFORE the 2nd).
#[must_use]
pub fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(500 * u64::from(attempt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_oras_is_retried() {
        assert!(is_net_tool("oras"));
        assert!(!is_net_tool("skopeo"));
        assert!(!is_net_tool("tar"));
        assert!(!is_net_tool("mkfs.ext4"));
    }

    #[test]
    fn net_tool_gets_multiple_attempts_others_one() {
        assert_eq!(attempts_for("oras"), NET_TOOL_ATTEMPTS);
        assert_eq!(attempts_for("tar"), 1);
        assert_eq!(attempts_for("mkfs.ext4"), 1);
        // The network tool must get strictly more attempts than a local one.
        assert!(attempts_for("oras") > attempts_for("tar"));
    }

    #[test]
    fn tool_home_falls_back_when_unset() {
        // The fallback must be a non-empty absolute path so `oras` is satisfied.
        let h = tool_home();
        assert!(!h.is_empty());
    }

    #[test]
    fn backoff_grows_per_attempt() {
        assert!(retry_backoff(2) > retry_backoff(1));
    }
}
