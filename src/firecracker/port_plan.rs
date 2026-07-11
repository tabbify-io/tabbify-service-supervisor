//! Readiness-port PLANNING + multi-port PROBING for a firecracker app launch.
//!
//! Split from [`super::protocol`] (which owns the resolved-single-port helpers
//! [`super::protocol::resolve_port`] / [`super::protocol::workspace_or_resolved_port`])
//! so each file stays focused. Pure + host-agnostic (the probe dials `host:port`
//! over plain TCP), so it is unit-tested on any host with real localhost
//! listeners — NOT gated to Linux.
//!
//! The old model resolved ONE port (the image's LOWEST `ExposedPorts` TCP) up
//! front and used it for BOTH the readiness probe and the reverse proxy. That
//! mis-picks for an app `FROM nginx:alpine` (base carries `EXPOSE 80`) that also
//! declares its real `EXPOSE 8730` + `listen 8730`: the lowest is `80`, nothing
//! listens there, the probe times out → crash-loop. [`PortPlan::Probe`] instead
//! probes ALL exposed candidates concurrently, first-answering-wins.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::config::FcConfig;
use crate::manifest::Runtime;

/// How a firecracker launch resolves the guest port it probes for readiness and
/// reverse-proxies app traffic to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortPlan {
    /// Probe exactly this ONE port; `guest_base` targets it. Used for a WORKSPACE
    /// (8080 forced), an explicit manifest `[runtime].port` override, a cache-hit
    /// respawn's persisted `.app_port` (an earlier launch's winner), a
    /// single-exposed-port image, or the 8080 default.
    Fixed(u16),
    /// A NON-workspace APP whose real listen port is UNKNOWN up front — its image
    /// declares MULTIPLE `ExposedPorts` (a base-inherited `EXPOSE 80` plus the
    /// app's own `EXPOSE 8730`). Probe ALL concurrently, FIRST-ANSWERING-WINS; the
    /// winner becomes `guest_base` and is persisted to `.app_port` so later
    /// config-read-less respawns target it directly. Always carries ≥ 2 ports (a
    /// single candidate collapses to [`PortPlan::Fixed`]).
    Probe(Vec<u16>),
}

/// Decide the [`PortPlan`] for a launch. Precedence (highest → lowest):
///
/// 1. **WORKSPACE** → `Fixed(cfg.app_port)`: the fixed workspace-init port is
///    FORCED (image `ExposedPorts` + any manifest override are IGNORED),
///    preserving the [`super::protocol::workspace_or_resolved_port`] guard — a
///    workspace base declares `EXPOSE 2222` (SSH), which is NOT its readiness
///    port, so probing the image port would wedge it in `provisioning` forever.
/// 2. **Manifest `[runtime].port`** (`rt.port`) → `Fixed(port)`: an explicit user
///    override WINS outright and SHORT-CIRCUITS the probe (mirrors how
///    `[runtime].vcpus` overrides the default).
/// 3. **Freshly-read image `ExposedPorts`** (`exposed`, this launch read the OCI
///    config): exactly one → `Fixed(that)`; two or more → `Probe(all)`.
/// 4. **Persisted `.app_port`** (`persisted`, a config-read-less cache-hit
///    respawn recovering an earlier launch's WINNING port) → `Fixed(it)`.
/// 5. `Fixed(cfg.app_port)` — the 8080 default (unchanged backward-compat: an app
///    that serves on 8080 and declares neither a manifest port, an ExposedPort,
///    nor a companion keeps working).
#[must_use]
pub fn resolve_port_plan(
    is_workspace: bool,
    rt: &Runtime,
    exposed: &[u16],
    persisted: Option<u16>,
    cfg: &FcConfig,
) -> PortPlan {
    if is_workspace {
        return PortPlan::Fixed(cfg.app_port);
    }
    if let Some(p) = rt.port {
        return PortPlan::Fixed(p);
    }
    match exposed {
        [] => {}
        [only] => return PortPlan::Fixed(*only),
        many => return PortPlan::Probe(many.to_vec()),
    }
    if let Some(p) = persisted {
        return PortPlan::Fixed(p);
    }
    PortPlan::Fixed(cfg.app_port)
}

/// Poll every port in `ports` on `host` concurrently and return the FIRST whose
/// HTTP server answers (ANY status) within `overall`. Bounded work: the candidate
/// set is an image's `ExposedPorts` (a handful of `EXPOSE` lines), one polling
/// task each. Each task retries with its OWN exponential backoff
/// (`backoff_start` → `backoff_cap`) and a per-request `poll_timeout`; the FIRST
/// task to get a response WINS and the rest are cancelled
/// ([`tokio::task::JoinSet::abort_all`]).
///
/// HARD-FAIL: returns a self-diagnosing `Err` when NONE of the candidates answers
/// within `overall` — the caller aborts the launch (no hang, no false-heal on a
/// dead port). An empty `ports` is a programming error and also `Err`s.
///
/// # Errors
/// No candidate answered within `overall`, or `ports` was empty.
#[allow(clippy::too_many_arguments)]
pub async fn probe_first_answering(
    client: &reqwest::Client,
    host: Ipv4Addr,
    ports: &[u16],
    overall: Duration,
    poll_timeout: Duration,
    backoff_start: Duration,
    backoff_cap: Duration,
) -> Result<u16> {
    if ports.is_empty() {
        return Err(anyhow!(
            "probe_first_answering called with no candidate ports (internal error)"
        ));
    }
    let deadline = tokio::time::Instant::now() + overall;
    let mut set = tokio::task::JoinSet::new();
    for &port in ports {
        let client = client.clone();
        let base = format!("http://{host}:{port}");
        set.spawn(async move {
            let mut backoff = backoff_start;
            loop {
                // Any HTTP answer (even a 4xx/5xx) means the app is LISTENING on
                // this port — that is the winning port. Only a transport error
                // (connection refused / no route / timeout) is "not ready yet".
                if client
                    .get(&base)
                    .timeout(poll_timeout)
                    .send()
                    .await
                    .is_ok()
                {
                    return Ok::<u16, u16>(port);
                }
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(port);
                }
                let remaining = deadline.saturating_duration_since(now);
                tokio::time::sleep(backoff.min(remaining)).await;
                backoff = (backoff * 2).min(backoff_cap);
            }
        });
    }

    let mut failed: Vec<u16> = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            // FIRST answering port wins — cancel the still-polling siblings.
            Ok(Ok(port)) => {
                set.abort_all();
                return Ok(port);
            }
            Ok(Err(port)) => failed.push(port),
            // A poller task was cancelled or panicked — treat as non-answering.
            Err(_) => {}
        }
    }

    failed.sort_unstable();
    Err(anyhow!(
        "no exposed port answered within {}s: probed {failed:?} on {host}, none responded. \
         Make the app LISTEN on one of its Dockerfile EXPOSE ports and keep PID 1 in the \
         FOREGROUND (it must not daemonize/exit). If it serves on a DIFFERENT port, set \
         `[runtime].port` in tabbify.toml to that port.",
        overall.as_secs(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;

    use super::{PortPlan, probe_first_answering, resolve_port_plan};
    use crate::config::FcConfig;
    use crate::manifest::Runtime;

    fn test_runtime() -> Runtime {
        Runtime {
            r#type: "firecracker".to_owned(),
            entry: "rootfs.ext4".to_owned(),
            fuel_per_request: 0,
            memory_mb: 512,
            vcpus: Some(1),
            port: None,
            kernel: None,
            registry_ref: None,
            stateful: false,
            data_mount: None,
        }
    }

    // ---- resolve_port_plan ---------------------------------------------------

    /// WORKSPACE forces `Fixed(cfg.app_port)` (8080) regardless of exposed ports,
    /// a persisted companion, OR a manifest `[runtime].port` — the workspace-init
    /// port guard must not regress (`EXPOSE 2222` is SSH, not readiness).
    #[test]
    fn resolve_port_plan_workspace_forces_fixed_app_port() {
        let cfg = FcConfig::default(); // app_port == 8080
        let mut rt = test_runtime();
        rt.port = Some(9999);
        assert_eq!(
            resolve_port_plan(true, &rt, &[80, 2222], Some(3000), &cfg),
            PortPlan::Fixed(8080)
        );
    }

    /// A manifest `[runtime].port` override WINS outright and SHORT-CIRCUITS the
    /// probe, even when the image exposes several ports.
    #[test]
    fn resolve_port_plan_manifest_port_wins_no_probe() {
        let cfg = FcConfig::default();
        let mut rt = test_runtime();
        rt.port = Some(8788);
        assert_eq!(
            resolve_port_plan(false, &rt, &[80, 8730], None, &cfg),
            PortPlan::Fixed(8788)
        );
    }

    /// A NON-workspace app with MULTIPLE freshly-read exposed ports ⇒ probe them
    /// ALL (the nginx-base regression: `EXPOSE 80` inherited + real `EXPOSE 8730`).
    #[test]
    fn resolve_port_plan_multiple_exposed_probes_all() {
        let cfg = FcConfig::default();
        let rt = test_runtime(); // port None
        assert_eq!(
            resolve_port_plan(false, &rt, &[80, 8730], None, &cfg),
            PortPlan::Probe(vec![80, 8730])
        );
    }

    /// A single exposed port needs no probe → `Fixed(that)`.
    #[test]
    fn resolve_port_plan_single_exposed_is_fixed() {
        let cfg = FcConfig::default();
        let rt = test_runtime();
        assert_eq!(
            resolve_port_plan(false, &rt, &[80], None, &cfg),
            PortPlan::Fixed(80)
        );
    }

    /// A config-read-less cache-hit respawn (no fresh exposed ports) recovers the
    /// WINNING port a prior launch persisted in `.app_port` → `Fixed(it)`.
    #[test]
    fn resolve_port_plan_persisted_used_on_cache_hit() {
        let cfg = FcConfig::default();
        let rt = test_runtime();
        assert_eq!(
            resolve_port_plan(false, &rt, &[], Some(8730), &cfg),
            PortPlan::Fixed(8730)
        );
    }

    /// A FRESH multi-port config read takes precedence over a stale persisted
    /// companion — a redeploy re-probes the (possibly changed) image's ports.
    #[test]
    fn resolve_port_plan_fresh_exposed_beats_persisted() {
        let cfg = FcConfig::default();
        let rt = test_runtime();
        assert_eq!(
            resolve_port_plan(false, &rt, &[80, 8730], Some(9000), &cfg),
            PortPlan::Probe(vec![80, 8730])
        );
    }

    /// No manifest port, no exposed ports, no companion ⇒ the 8080 default
    /// (unchanged backward-compat).
    #[test]
    fn resolve_port_plan_falls_back_to_default() {
        let cfg = FcConfig::default();
        let rt = test_runtime();
        assert_eq!(
            resolve_port_plan(false, &rt, &[], None, &cfg),
            PortPlan::Fixed(8080)
        );
    }

    // ---- probe_first_answering ----------------------------------------------

    /// Bind a localhost TCP listener that answers a minimal HTTP/1.1 response to
    /// every connection; returns its ephemeral port. Cross-platform (127.0.0.1).
    async fn spawn_http_ok() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
                let _ = sock.shutdown().await;
            }
        });
        port
    }

    /// An ephemeral port with NOTHING listening (bind, grab the port, drop) — a
    /// connection to it is refused, i.e. the app is "not listening here".
    async fn closed_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    /// The FIRST-ANSWERING port wins: given a dead port and a live one, the probe
    /// returns the live port (the reverse-proxy then targets the real listener).
    #[tokio::test]
    async fn probe_first_answering_picks_the_listening_port() {
        let dead = closed_port().await;
        let live = spawn_http_ok().await;
        let client = reqwest::Client::new();
        let winner = probe_first_answering(
            &client,
            Ipv4Addr::LOCALHOST,
            &[dead, live],
            Duration::from_secs(5),
            Duration::from_millis(200),
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        assert_eq!(winner, live, "the port with a live HTTP server must win");
    }

    /// HARD-FAIL (no hang, no false-heal) when NONE of the candidates answers
    /// within the overall budget — with a self-diagnosing message.
    #[tokio::test]
    async fn probe_first_answering_hard_fails_when_none_answer() {
        let a = closed_port().await;
        let b = closed_port().await;
        let client = reqwest::Client::new();
        let err = probe_first_answering(
            &client,
            Ipv4Addr::LOCALHOST,
            &[a, b],
            Duration::from_millis(400),
            Duration::from_millis(100),
            Duration::from_millis(20),
            Duration::from_millis(80),
        )
        .await
        .expect_err("no listener on any candidate ⇒ hard fail");
        let msg = err.to_string();
        assert!(
            msg.contains("no exposed port answered"),
            "self-diagnosing message expected, got: {msg}"
        );
    }

    /// An empty candidate list is a programming error, not a hang → `Err`.
    #[tokio::test]
    async fn probe_first_answering_empty_is_error() {
        let client = reqwest::Client::new();
        let err = probe_first_answering(
            &client,
            Ipv4Addr::LOCALHOST,
            &[],
            Duration::from_millis(50),
            Duration::from_millis(50),
            Duration::from_millis(10),
            Duration::from_millis(10),
        )
        .await
        .expect_err("empty ports ⇒ error");
        assert!(err.to_string().contains("no candidate ports"));
    }
}
