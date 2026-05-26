//! Runner control-socket protocol — shared between the runner (server side)
//! and the supervisor orchestrator (client side, Phase 2).
//!
//! Framing: one JSON value per line (newline-delimited JSON). The runner reads
//! one [`Cmd`] per connection and writes one [`Reply`], then closes.

use serde::{Deserialize, Serialize};

/// Commands the supervisor (or tests) send to a runner's control socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Cmd {
    /// Liveness probe — returns [`Reply::Pong`].
    Ping,
    /// Request current lifecycle state — returns [`Reply::Health`].
    Health,
    /// Tear down the per-app listener and stop the app. Returns [`Reply::Ok`].
    Stop,
    /// Stop + clear the on-disk artifact cache (and docker image if applicable).
    /// Returns [`Reply::Ok`].
    Purge,
    /// Stop + signal the process to exit. Returns [`Reply::Ok`] before exiting.
    Shutdown,
}

/// Replies the runner sends back over the control socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum Reply {
    /// Response to [`Cmd::Ping`].
    Pong,
    /// Response to [`Cmd::Health`]: current lifecycle snapshot.
    Health {
        /// `"running"` or `"stopped"`.
        state: String,
        /// The app's deterministic ULA (`fd5a:…`).
        app_ula: String,
        /// The app UUID (string form).
        app_uuid: String,
        /// PID of the runner process.
        pid: u32,
    },
    /// Generic success.
    Ok,
    /// Generic error (the runner could not fulfil the command).
    ///
    /// Uses a struct variant (not a newtype) so `serde(tag = "reply")` can
    /// include the discriminant alongside the `message` field without hitting
    /// the tagged-newtype-string limitation.
    Err {
        /// Human-readable error description.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Every Cmd variant round-trips through JSON without loss.
    #[test]
    fn cmd_serde_round_trip() {
        for cmd in [Cmd::Ping, Cmd::Health, Cmd::Stop, Cmd::Purge, Cmd::Shutdown] {
            let json = serde_json::to_string(&cmd).unwrap();
            let back: Cmd = serde_json::from_str(&json).unwrap();
            assert_eq!(cmd, back, "round-trip failed for {json}");
        }
    }

    /// Every Reply variant round-trips through JSON without loss.
    #[test]
    fn reply_serde_round_trip() {
        let replies = [
            Reply::Pong,
            Reply::Health {
                state: "running".to_owned(),
                app_ula: "fd5a:1f02::1".to_owned(),
                app_uuid: "abc-123".to_owned(),
                pid: 42,
            },
            Reply::Ok,
            Reply::Err {
                message: "something went wrong".to_owned(),
            },
        ];
        for reply in replies {
            let json = serde_json::to_string(&reply).unwrap();
            let back: Reply = serde_json::from_str(&json).unwrap();
            assert_eq!(reply, back, "round-trip failed for {json}");
        }
    }

    /// Ping / Pong are the simplest variants — spot-check their wire form.
    #[test]
    fn ping_pong_wire_form() {
        let ping = serde_json::to_string(&Cmd::Ping).unwrap();
        assert!(ping.contains("ping"), "got: {ping}");
        let pong = serde_json::to_string(&Reply::Pong).unwrap();
        assert!(pong.contains("pong"), "got: {pong}");
    }
}
