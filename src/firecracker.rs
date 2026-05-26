//! Firecracker microVM runtime (contract: firecracker-runtime-design).
//!
//! A second [`crate::runtime::AppRuntime`] implementation that boots an app's
//! rootfs as a Firecracker microVM and proxies HTTP to the app's server inside
//! the guest. Unlike the in-process WASM runtime, this needs real hardware
//! virtualization (`/dev/kvm`) and Linux host networking (`iproute2` taps), so:
//!
//! - the REAL implementation is `#[cfg(target_os = "linux")]`;
//! - a `#[cfg(not(target_os = "linux"))]` STUB lets the supervisor still BUILD
//!   and run (serving WASM apps) on a macOS dev host — its `launch` returns a
//!   clear `Err`.
//!
//! KVM-gated: [`kvm_available`] decides at startup whether this host can host
//! firecracker apps at all (it also drives the `firecracker` mesh tag). A
//! no-KVM host serves WASM and refuses firecracker apps loudly.
//!
//! ## Firecracker API client
//! Firecracker is configured over a per-VM UNIX-socket HTTP/1.1 REST API with
//! tiny JSON bodies (`PUT /machine-config`, `/boot-source`, `/drives/...`,
//! `/network-interfaces/...`, `/actions`). Rather than pull a heavy crate, the
//! request BODIES are built by pure functions in the `protocol` submodule
//! (unit-tested without a socket) and a ~60-line hand-rolled HTTP/1.1-over-
//! `tokio::net::UnixStream` client (Linux-only, in the `linux` submodule) sends
//! them. This keeps the dependency set unchanged and compiles cleanly on musl.

// The real machinery and the stub share these cross-platform pieces (config
// type, request-body builders, hop-by-hop header set, the KVM probe).
use crate::config::FcConfig;

/// Path to the KVM device node; presence + R/W openability gates firecracker.
const DEV_KVM: &str = "/dev/kvm";

/// Is this host able to run Firecracker microVMs? True iff `/dev/kvm` exists
/// AND can be opened read-write (KVM requires R/W). EC2 `t3.micro` (no nested
/// virt) has no `/dev/kvm`, so this returns `false` there and the supervisor
/// degrades to WASM-only.
#[must_use]
pub fn kvm_available() -> bool {
    protocol::kvm_available_with(default_kvm_check)
}

/// Default KVM probe used by [`kvm_available`]: try to open `/dev/kvm` R/W.
fn default_kvm_check() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(DEV_KVM)
        .is_ok()
}

/// Cross-platform firecracker protocol helpers: the REST request-body builders
/// and the hop-by-hop header filter. These are consumed by the Linux runtime
/// ([`linux`]) and by the unit tests; on a non-Linux build only the tests use
/// them, hence the module-level `allow(dead_code)` for that case (a plain
/// `cargo build` on macOS compiles them but doesn't call them).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod protocol {
    use anyhow::{Result, anyhow};
    use serde_json::{Value, json};
    use tokio::io::{AsyncRead, AsyncReadExt};

    /// Hop-by-hop headers (RFC 7230 §6.1) that MUST NOT be forwarded when
    /// proxying between the inbound request and the guest, nor copied back from
    /// the guest response. Lower-cased for case-insensitive match.
    const HOP_BY_HOP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
    ];

    /// [`super::kvm_available`] with an injectable probe — lets tests assert the
    /// gate logic without a real `/dev/kvm` (absent on the macOS CI host).
    pub fn kvm_available_with(check: impl Fn() -> bool) -> bool {
        check()
    }

    /// Build the `PUT /machine-config` body: vCPU count + guest RAM (MiB).
    pub fn machine_config_body(vcpus: u32, mem_size_mib: u32) -> Value {
        json!({
            "vcpu_count": vcpus,
            "mem_size_mib": mem_size_mib,
            "smt": false,
        })
    }

    /// Build the `PUT /boot-source` body: kernel image + serial console + a
    /// static guest network config baked into the kernel command line. The
    /// `ip=` argument is the kernel's built-in IP autoconfiguration
    /// (`ip=<client>::<gw>:<mask>::<dev>:off`).
    pub fn boot_source_body(kernel_image_path: &str, guest_ip: &str, host_ip: &str) -> Value {
        let boot_args = format!(
            "console=ttyS0 reboot=k panic=1 pci=off \
             ip={guest_ip}::{host_ip}:255.255.255.252::eth0:off"
        );
        json!({
            "kernel_image_path": kernel_image_path,
            "boot_args": boot_args,
        })
    }

    /// Build the `PUT /drives/rootfs` body: the app's rootfs image, mounted as
    /// the (writable) root device.
    pub fn rootfs_drive_body(path_on_host: &str) -> Value {
        json!({
            "drive_id": "rootfs",
            "path_on_host": path_on_host,
            "is_root_device": true,
            "is_read_only": false,
        })
    }

    /// Build the `PUT /network-interfaces/eth0` body: bind the guest `eth0` to
    /// the host tap device with a deterministic guest MAC.
    pub fn network_iface_body(host_dev_name: &str, guest_mac: &str) -> Value {
        json!({
            "iface_id": "eth0",
            "host_dev_name": host_dev_name,
            "guest_mac": guest_mac,
        })
    }

    /// Build the `PUT /actions` body that boots the configured VM.
    pub fn instance_start_body() -> Value {
        json!({ "action_type": "InstanceStart" })
    }

    /// Is `name` a hop-by-hop header (case-insensitive)?
    pub fn is_hop_by_hop(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        HOP_BY_HOP.iter().any(|h| *h == lower)
    }

    /// Copy `src` headers into `dst`, dropping hop-by-hop headers. Used both
    /// when forwarding the inbound request to the guest and when relaying the
    /// guest's response back out, so neither carries connection-scoped headers
    /// across the proxy boundary.
    pub fn copy_filtered_headers(src: &http::HeaderMap, dst: &mut http::HeaderMap) {
        for (name, value) in src {
            if !is_hop_by_hop(name.as_str()) {
                dst.append(name.clone(), value.clone());
            }
        }
    }

    /// Proxy one inbound request to `guest_base` (e.g.
    /// `http://172.31.0.2:8080`) and buffer the guest's response.
    ///
    /// The path+query is forwarded verbatim, method + non-hop-by-hop headers +
    /// body are sent on, and the guest's status + filtered headers + body are
    /// relayed back. This is the VM-independent core of the firecracker
    /// runtime's `handle`, so it can be exercised against a wiremock "fake VM"
    /// on any platform.
    ///
    /// # Errors
    /// A transport failure talking to the guest, or a malformed response.
    pub async fn proxy_request(
        client: &reqwest::Client,
        guest_base: &str,
        request: http::Request<bytes::Bytes>,
    ) -> anyhow::Result<http::Response<bytes::Bytes>> {
        use anyhow::Context as _;

        let (parts, body) = request.into_parts();
        let path_and_query = parts
            .uri
            .path_and_query()
            .map_or_else(|| "/".to_owned(), |pq| pq.as_str().to_owned());
        let url = format!("{guest_base}{path_and_query}");

        let mut out_headers = http::HeaderMap::new();
        copy_filtered_headers(&parts.headers, &mut out_headers);
        let upstream = client
            .request(parts.method, &url)
            .headers(out_headers)
            .body(body.to_vec())
            .send()
            .await
            .with_context(|| format!("proxy to guest {url}"))?;

        let status = upstream.status();
        let mut resp = http::Response::builder().status(status);
        if let Some(h) = resp.headers_mut() {
            copy_filtered_headers(upstream.headers(), h);
        }
        let bytes = upstream
            .bytes()
            .await
            .context("collect guest response body")?;
        resp.body(bytes).context("build proxied response")
    }

    /// Parse the HTTP status code out of a raw HTTP/1.1 response head.
    pub fn parse_status_line(raw: &[u8]) -> Result<u16> {
        let text = String::from_utf8_lossy(raw);
        let line = text
            .lines()
            .next()
            .ok_or_else(|| anyhow!("empty firecracker API response"))?;
        // "HTTP/1.1 204 No Content"
        let code = line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| anyhow!("malformed status line {line:?}"))?;
        code.parse::<u16>()
            .map_err(|e| anyhow!("bad status code in {line:?}: {e}"))
    }

    /// Read an HTTP/1.1 response head from `reader` and return its status code.
    ///
    /// Firecracker's API server uses HTTP keep-alive and IGNORES a request
    /// `Connection: close`, so it does NOT close the socket after replying —
    /// reading to EOF would block until the connection is torn down elsewhere.
    /// Our PUTs only need the status code (success is `204 No Content`, no body),
    /// so this stops at the end-of-headers marker (`\r\n\r\n`) and never waits on
    /// a kept-alive connection.
    pub async fn read_http_status<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u16> {
        let mut buf = Vec::with_capacity(256);
        let mut chunk = [0u8; 256];
        loop {
            let n = reader.read(&mut chunk).await?;
            if n == 0 {
                break; // EOF before the header terminator; parse what we have.
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break; // full response head received; status is known.
            }
        }
        parse_status_line(&buf)
    }
}

// The concrete `FirecrackerRuntime` differs by platform. The real Linux impl
// owns a child process + tap and proxies over the WG/tap network; the non-Linux
// stub exists only so the crate builds on macOS dev hosts.
#[cfg(target_os = "linux")]
pub use linux::FirecrackerRuntime;
#[cfg(not(target_os = "linux"))]
pub use stub::FirecrackerRuntime;

// ---------------------------------------------------------------------------
// Non-Linux stub: builds, never boots a VM.
// ---------------------------------------------------------------------------
#[cfg(not(target_os = "linux"))]
mod stub {
    use std::path::Path;

    use anyhow::{Result, bail};
    use bytes::Bytes;
    use http::{Request, Response};

    use super::FcConfig;
    use crate::manifest::Runtime;
    use crate::runtime::{AppRuntime, BoxRespFut};

    /// Non-Linux stub. Firecracker needs Linux + `/dev/kvm`, so on macOS the
    /// supervisor still builds + serves WASM, but any attempt to host a
    /// firecracker app fails loudly here.
    pub struct FirecrackerRuntime;

    impl FirecrackerRuntime {
        /// Always `Err` on non-Linux hosts (no KVM, no tap networking).
        ///
        /// # Errors
        /// Always — firecracker is Linux + `/dev/kvm` only.
        #[allow(clippy::unused_async)]
        pub async fn launch(_rootfs: &Path, _rt: &Runtime, _cfg: &FcConfig) -> Result<Self> {
            bail!("firecracker runtime requires Linux + /dev/kvm (host is not Linux)")
        }
    }

    impl AppRuntime for FirecrackerRuntime {
        fn handle<'a>(&'a self, _request: Request<Bytes>) -> BoxRespFut<'a> {
            // Unreachable in practice (`launch` never returns `Ok` off Linux),
            // but the trait must be satisfied for the type to exist.
            Box::pin(async {
                Ok(Response::builder()
                    .status(http::StatusCode::NOT_IMPLEMENTED)
                    .body(Bytes::from_static(
                        b"firecracker not supported on this host",
                    ))?)
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Linux: the real microVM runtime.
// ---------------------------------------------------------------------------
#[cfg(target_os = "linux")]
mod linux {
    use std::net::Ipv4Addr;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use anyhow::{Context, Result, anyhow, bail};
    use bytes::Bytes;
    use http::Request;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;
    use tokio::process::{Child, Command};

    use super::protocol::{
        boot_source_body, instance_start_body, machine_config_body, network_iface_body,
        proxy_request, read_http_status, rootfs_drive_body,
    };
    use super::{FcConfig, kvm_available};
    use crate::manifest::Runtime;
    use crate::runtime::{AppRuntime, BoxRespFut};

    /// How long to wait for the guest app's HTTP server to come up after boot.
    const READY_TIMEOUT: Duration = Duration::from_secs(30);
    /// Poll interval while waiting for the guest app.
    const READY_POLL: Duration = Duration::from_millis(250);
    /// How long to wait for one firecracker API call over the unix socket.
    const API_TIMEOUT: Duration = Duration::from_secs(5);

    /// Monotonic per-process counter → a unique tap name + /30 offset per VM, so
    /// concurrently-hosted firecracker apps don't collide on tap devices/links.
    static VM_SEQ: AtomicU32 = AtomicU32::new(0);

    /// A booted Firecracker microVM hosting one app. Owns the firecracker child
    /// process + the host tap device; [`Drop`] tears both down.
    pub struct FirecrackerRuntime {
        /// The firecracker child. `Option` so [`Drop`] can take it and spawn a
        /// kill+reap (so no `<defunct>` zombie lingers). `Some` while alive.
        child: Option<Child>,
        tap_name: String,
        api_sock: PathBuf,
        /// `http://<guest_ip>:<app_port>` — the base the proxy targets.
        guest_base: String,
        client: reqwest::Client,
    }

    impl FirecrackerRuntime {
        /// Boot `rootfs` as a microVM and wait for its app HTTP server.
        ///
        /// Steps (design §4): KVM guard → allocate tap + /30 → spawn
        /// `firecracker --api-sock` → configure via the unix-socket REST API
        /// (machine-config, boot-source, rootfs drive, eth0 tap, InstanceStart)
        /// → poll the guest app until ready.
        ///
        /// # Errors
        /// `!kvm_available()`, tap setup failure, firecracker spawn failure, any
        /// REST configuration call failing, or the guest app not answering
        /// within [`READY_TIMEOUT`].
        pub async fn launch(rootfs: &Path, rt: &Runtime, cfg: &FcConfig) -> Result<Self> {
            if !kvm_available() {
                bail!("firecracker runtime requires Linux + /dev/kvm (/dev/kvm not R/W-openable)");
            }
            if !rootfs.is_file() {
                bail!("firecracker rootfs not found at {}", rootfs.display());
            }

            let seq = VM_SEQ.fetch_add(1, Ordering::SeqCst);
            let (host_ip, guest_ip) = derive_link_ips(&cfg.tap_subnet, seq)
                .with_context(|| format!("derive /30 from subnet {}", cfg.tap_subnet))?;
            let tap_name = format!("fc-tap{seq}");
            let guest_mac = derive_guest_mac(seq);

            // Host tap + /30 link (design §4.2). Best-effort cleanup of a stale
            // tap of the same name first (ignore failure — it may not exist).
            let _ = run_ip(&["link", "del", &tap_name]).await;
            setup_tap(&tap_name, host_ip).await?;

            // Spawn firecracker with its API socket. Clean any stale socket.
            let api_sock = PathBuf::from(format!("/tmp/firecracker-{tap_name}.sock"));
            let _ = std::fs::remove_file(&api_sock);
            let child = Command::new(&cfg.bin)
                .arg("--api-sock")
                .arg(&api_sock)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .with_context(|| format!("spawn firecracker binary {:?}", cfg.bin))?;

            let me = Self {
                child: Some(child),
                tap_name,
                api_sock: api_sock.clone(),
                guest_base: format!("http://{guest_ip}:{}", cfg.app_port),
                client: reqwest::Client::new(),
            };

            // Configure + boot the VM, then wait for the guest app. On any
            // failure `me` drops → child killed + tap deleted.
            me.configure_and_boot(rootfs, rt, cfg, &guest_ip, &host_ip, &guest_mac)
                .await?;
            me.wait_until_ready().await?;
            Ok(me)
        }

        /// Push the full firecracker REST configuration, then start the VM.
        async fn configure_and_boot(
            &self,
            rootfs: &Path,
            rt: &Runtime,
            cfg: &FcConfig,
            guest_ip: &Ipv4Addr,
            host_ip: &Ipv4Addr,
            guest_mac: &str,
        ) -> Result<()> {
            // The API socket appears asynchronously after spawn; wait for it.
            wait_for_socket(&self.api_sock).await?;

            let kernel = rt.kernel.clone().unwrap_or_else(|| cfg.kernel.clone());
            let rootfs_str = rootfs
                .to_str()
                .ok_or_else(|| anyhow!("rootfs path is not valid UTF-8"))?;

            self.api_put(
                "/machine-config",
                &machine_config_body(cfg.vcpus, rt.memory_mb),
            )
            .await
            .context("PUT /machine-config")?;
            self.api_put(
                "/boot-source",
                &boot_source_body(&kernel, &guest_ip.to_string(), &host_ip.to_string()),
            )
            .await
            .context("PUT /boot-source")?;
            self.api_put("/drives/rootfs", &rootfs_drive_body(rootfs_str))
                .await
                .context("PUT /drives/rootfs")?;
            self.api_put(
                "/network-interfaces/eth0",
                &network_iface_body(&self.tap_name, guest_mac),
            )
            .await
            .context("PUT /network-interfaces/eth0")?;
            self.api_put("/actions", &instance_start_body())
                .await
                .context("PUT /actions InstanceStart")?;
            Ok(())
        }

        /// One firecracker REST `PUT` over the API unix socket. Hand-rolled
        /// HTTP/1.1: firecracker speaks plain HTTP/1.1 with `Content-Length`
        /// bodies and replies `204 No Content` (or 200) on success, an error
        /// JSON otherwise.
        async fn api_put(&self, path: &str, body: &serde_json::Value) -> Result<()> {
            let payload = serde_json::to_vec(body)?;
            let status =
                tokio::time::timeout(API_TIMEOUT, unix_http_put(&self.api_sock, path, &payload))
                    .await
                    .map_err(|_| anyhow!("firecracker API timed out on PUT {path}"))??;
            if !(200..300).contains(&status) {
                bail!("firecracker API PUT {path} returned HTTP {status}");
            }
            Ok(())
        }

        /// Poll the guest app's HTTP server until it answers (any status) or
        /// [`READY_TIMEOUT`] elapses.
        async fn wait_until_ready(&self) -> Result<()> {
            let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
            loop {
                match self
                    .client
                    .get(&self.guest_base)
                    .timeout(READY_POLL)
                    .send()
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(_) if tokio::time::Instant::now() < deadline => {
                        tokio::time::sleep(READY_POLL).await;
                    }
                    Err(e) => {
                        bail!("guest app at {} never became ready: {e}", self.guest_base)
                    }
                }
            }
        }
    }

    impl AppRuntime for FirecrackerRuntime {
        fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a> {
            // Delegate to the VM-independent proxy core (tested via wiremock).
            Box::pin(proxy_request(&self.client, &self.guest_base, request))
        }
    }

    impl Drop for FirecrackerRuntime {
        fn drop(&mut self) {
            // Kill AND REAP the firecracker child. `start_kill` alone SIGKILLs but
            // leaves a zombie until tokio's orphan reaper runs (seconds later);
            // spawning `kill()` (SIGKILL + `wait`) reaps it immediately so no
            // `<defunct>` lingers in the process table. We're normally inside the
            // tokio runtime here; if not (e.g. a sync drop in a test), fall back
            // to a best-effort non-reaping kill. The tap delete stays synchronous
            // (quick) so it happens even if there's no runtime to spawn on.
            if let Some(mut child) = self.child.take() {
                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => {
                        handle.spawn(async move {
                            let _ = child.kill().await;
                        });
                    }
                    Err(_) => {
                        let _ = child.start_kill();
                    }
                }
            }
            let _ = std::fs::remove_file(&self.api_sock);
            let tap = self.tap_name.clone();
            match std::process::Command::new("ip")
                .args(["link", "del", &tap])
                .status()
            {
                Ok(s) if s.success() => {}
                Ok(s) => tracing::warn!(%tap, code = ?s.code(), "ip link del tap nonzero exit"),
                Err(e) => tracing::warn!(%tap, error = %e, "ip link del tap failed"),
            }
        }
    }

    /// Create the host tap device and assign it the /30 host IP.
    async fn setup_tap(tap_name: &str, host_ip: Ipv4Addr) -> Result<()> {
        run_ip(&["tuntap", "add", tap_name, "mode", "tap"])
            .await
            .with_context(|| format!("ip tuntap add {tap_name}"))?;
        run_ip(&["addr", "add", &format!("{host_ip}/30"), "dev", tap_name])
            .await
            .with_context(|| format!("ip addr add {host_ip}/30 dev {tap_name}"))?;
        run_ip(&["link", "set", tap_name, "up"])
            .await
            .with_context(|| format!("ip link set {tap_name} up"))?;
        Ok(())
    }

    /// Run an `ip ...` command, erroring on a non-zero exit.
    async fn run_ip(args: &[&str]) -> Result<()> {
        let out = Command::new("ip")
            .args(args)
            .output()
            .await
            .with_context(|| format!("spawn ip {}", args.join(" ")))?;
        if !out.status.success() {
            bail!(
                "ip {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    /// Derive a (host_ip, guest_ip) /30 pair for VM index `seq` out of
    /// `subnet`. We carve sequential /30s: VM `n` gets hosts
    /// `base + 4n + 1` (host) and `base + 4n + 2` (guest).
    fn derive_link_ips(subnet: &str, seq: u32) -> Result<(Ipv4Addr, Ipv4Addr)> {
        let base = subnet
            .split('/')
            .next()
            .and_then(|s| s.parse::<Ipv4Addr>().ok())
            .ok_or_else(|| anyhow!("invalid tap subnet {subnet:?}"))?;
        let base_u32 = u32::from(base);
        let host = base_u32
            .checked_add(seq * 4 + 1)
            .ok_or_else(|| anyhow!("tap subnet exhausted at seq {seq}"))?;
        let guest = host + 1;
        Ok((Ipv4Addr::from(host), Ipv4Addr::from(guest)))
    }

    /// Deterministic locally-administered guest MAC from the VM index.
    fn derive_guest_mac(seq: u32) -> String {
        let b = seq.to_le_bytes();
        // 02:xx → locally administered, unicast.
        format!("02:FC:{:02X}:{:02X}:{:02X}:{:02X}", b[0], b[1], b[2], b[3])
    }

    /// Wait (bounded) for firecracker to create its API socket after spawn.
    async fn wait_for_socket(sock: &Path) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !sock.exists() {
            if tokio::time::Instant::now() >= deadline {
                bail!("firecracker API socket {} never appeared", sock.display());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Hand-rolled HTTP/1.1 `PUT` over a unix socket: write the request, read
    /// the status line, return the status code. Bodies are small JSON; we send
    /// `Content-Length` and read just enough of the reply to learn the status
    /// (firecracker responds 204/200 on success).
    async fn unix_http_put(sock: &Path, path: &str, body: &[u8]) -> Result<u16> {
        let mut stream = UnixStream::connect(sock)
            .await
            .with_context(|| format!("connect firecracker socket {}", sock.display()))?;

        let head = format!(
            "PUT {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await?;

        // Firecracker's API server uses keep-alive and ignores `Connection:
        // close`, so it doesn't close the socket; read just the response head
        // (NOT to EOF, which would hang) and parse its status.
        read_http_status(&mut stream).await
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    mod tests {
        use super::*;

        #[test]
        fn link_ips_carve_sequential_slash30s() {
            let (h0, g0) = derive_link_ips("172.31.0.0/16", 0).unwrap();
            assert_eq!(h0, Ipv4Addr::new(172, 31, 0, 1));
            assert_eq!(g0, Ipv4Addr::new(172, 31, 0, 2));
            let (h1, g1) = derive_link_ips("172.31.0.0/16", 1).unwrap();
            assert_eq!(h1, Ipv4Addr::new(172, 31, 0, 5));
            assert_eq!(g1, Ipv4Addr::new(172, 31, 0, 6));
        }

        #[test]
        fn guest_mac_is_locally_administered_and_deterministic() {
            assert_eq!(derive_guest_mac(0), "02:FC:00:00:00:00");
            assert_eq!(derive_guest_mac(1), "02:FC:01:00:00:00");
        }

        /// REAL microVM boot — Linux + `/dev/kvm` + a provisioned kernel + a
        /// rootfs only. `#[ignore]`d so CI / the macOS dev host never runs it;
        /// run it by hand on a KVM box (e.g. Leo's Lima Ubuntu):
        ///
        /// ```text
        /// # On the Lima guest, as root (needs /dev/kvm + iproute2 + firecracker):
        /// #   - put a vmlinux at /opt/tabbify/vmlinux
        /// #   - put a rootfs whose app serves HTTP on :8080 at /tmp/rootfs.ext4
        /// sudo -E cargo test -p tabbify-service-supervisor \
        ///     firecracker::linux::tests::real_vm_boots_and_serves -- --ignored --nocapture
        /// ```
        ///
        /// Asserts the VM boots, the guest app answers, and `Drop` tears the VM
        /// + tap down.
        #[tokio::test]
        #[ignore = "requires Linux + /dev/kvm + a provisioned kernel/rootfs (run on Lima)"]
        async fn real_vm_boots_and_serves() {
            use crate::runtime::AppRuntime;

            let rootfs = std::path::PathBuf::from("/tmp/rootfs.ext4");
            let rt = crate::manifest::Runtime {
                r#type: "firecracker".to_owned(),
                entry: "rootfs.ext4".to_owned(),
                fuel_per_request: 0,
                memory_mb: 256,
                kernel: None,
            };
            let cfg = FcConfig::default();

            let vm = FirecrackerRuntime::launch(&rootfs, &rt, &cfg)
                .await
                .expect("boot microVM");
            let req = Request::builder()
                .method("GET")
                .uri("http://app/")
                .body(Bytes::new())
                .unwrap();
            let resp = vm.handle(req).await.expect("proxy to guest");
            assert!(resp.status().is_success());
            drop(vm); // child killed + tap deleted
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::protocol::{
        boot_source_body, copy_filtered_headers, instance_start_body, is_hop_by_hop,
        kvm_available_with, machine_config_body, network_iface_body, parse_status_line,
        read_http_status, rootfs_drive_body,
    };

    #[test]
    fn kvm_gate_reflects_the_injected_probe() {
        assert!(kvm_available_with(|| true));
        assert!(!kvm_available_with(|| false));
    }

    #[test]
    fn machine_config_body_carries_vcpus_and_mem() {
        let b = machine_config_body(2, 512);
        assert_eq!(b["vcpu_count"], 2);
        assert_eq!(b["mem_size_mib"], 512);
        assert_eq!(b["smt"], false);
    }

    #[test]
    fn boot_source_body_has_kernel_and_ip_boot_arg() {
        let b = boot_source_body("/opt/tabbify/vmlinux", "172.31.0.2", "172.31.0.1");
        assert_eq!(b["kernel_image_path"], "/opt/tabbify/vmlinux");
        let args = b["boot_args"].as_str().unwrap();
        assert!(args.contains("ip=172.31.0.2::172.31.0.1:255.255.255.252::eth0:off"));
        assert!(args.contains("console=ttyS0"));
    }

    #[test]
    fn rootfs_drive_body_is_root_and_writable() {
        let b = rootfs_drive_body("/var/lib/tabbify/rootfs.ext4");
        assert_eq!(b["drive_id"], "rootfs");
        assert_eq!(b["path_on_host"], "/var/lib/tabbify/rootfs.ext4");
        assert_eq!(b["is_root_device"], true);
        assert_eq!(b["is_read_only"], false);
    }

    #[test]
    fn network_iface_body_binds_tap_to_eth0() {
        let b = network_iface_body("fc-tap0", "02:FC:00:00:00:00");
        assert_eq!(b["iface_id"], "eth0");
        assert_eq!(b["host_dev_name"], "fc-tap0");
        assert_eq!(b["guest_mac"], "02:FC:00:00:00:00");
    }

    #[test]
    fn instance_start_body_is_instance_start() {
        assert_eq!(instance_start_body()["action_type"], "InstanceStart");
    }

    #[test]
    fn hop_by_hop_detection_is_case_insensitive() {
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("transfer-encoding"));
        assert!(is_hop_by_hop("HOST"));
        assert!(!is_hop_by_hop("content-type"));
        assert!(!is_hop_by_hop("x-app-header"));
    }

    #[test]
    fn copy_filtered_headers_drops_hop_by_hop_keeps_the_rest() {
        let mut src = http::HeaderMap::new();
        src.insert("content-type", "application/json".parse().unwrap());
        src.insert("connection", "keep-alive".parse().unwrap());
        src.insert("host", "guest.local".parse().unwrap());
        src.insert("x-custom", "abc".parse().unwrap());

        let mut dst = http::HeaderMap::new();
        copy_filtered_headers(&src, &mut dst);

        assert_eq!(dst.get("content-type").unwrap(), "application/json");
        assert_eq!(dst.get("x-custom").unwrap(), "abc");
        assert!(dst.get("connection").is_none());
        assert!(dst.get("host").is_none());
    }

    // The proxy core is tested against a wiremock "fake VM" HTTP server — the
    // same path the Linux firecracker `handle` uses, exercised on any platform.
    use bytes::Bytes;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::protocol::proxy_request;

    #[tokio::test]
    async fn proxy_forwards_path_and_returns_guest_body() {
        let vm = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/data"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("x-guest", "yes")
                    .set_body_string("guest-says-hi"),
            )
            .mount(&vm)
            .await;

        let req = http::Request::builder()
            .method("GET")
            .uri("http://app-ula/api/data?q=1")
            .body(Bytes::new())
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &vm.uri(), req)
            .await
            .expect("proxy");

        assert_eq!(resp.status(), 201);
        assert_eq!(resp.headers().get("x-guest").unwrap(), "yes");
        assert_eq!(String::from_utf8_lossy(resp.body()), "guest-says-hi");
    }

    #[tokio::test]
    async fn proxy_strips_hop_by_hop_request_headers_before_forwarding() {
        let vm = MockServer::start().await;
        // The guest must NOT see `connection`/`host` forwarded, but MUST see the
        // custom header. wiremock asserts header presence/absence on match.
        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(wiremock::matchers::header("x-keep", "1"))
            .and(wiremock::matchers::header_exists("content-type"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&vm)
            .await;

        let req = http::Request::builder()
            .method("POST")
            .uri("http://app-ula/submit")
            .header("connection", "keep-alive")
            .header("x-keep", "1")
            .header("content-type", "text/plain")
            .body(Bytes::from_static(b"payload"))
            .unwrap();
        let resp = proxy_request(&reqwest::Client::new(), &vm.uri(), req)
            .await
            .expect("proxy");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "ok");
    }

    #[test]
    fn parses_firecracker_status_line() {
        assert_eq!(
            parse_status_line(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap(),
            204
        );
        assert_eq!(
            parse_status_line(b"HTTP/1.1 400 Bad Request\r\n\r\n{\"x\":1}").unwrap(),
            400
        );
    }

    /// Firecracker replies then KEEPS THE SOCKET OPEN (keep-alive, ignoring our
    /// `Connection: close`). `read_http_status` must return at the header
    /// terminator and not block waiting for an EOF that never comes.
    #[tokio::test]
    async fn read_http_status_returns_on_keepalive_without_eof() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, mut server) = tokio::io::duplex(1024);
        server
            .write_all(
                b"HTTP/1.1 204 \r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
        // Deliberately keep `server` alive (no EOF) across the read.
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_status(&mut client),
        )
        .await
        .expect("must not block waiting for EOF on a kept-alive socket")
        .expect("status parsed");
        assert_eq!(status, 204);
        drop(server);
    }

    /// A non-2xx reply carries a JSON body; only the status from the head is
    /// needed, and the trailing body must not cause a hang either.
    #[tokio::test]
    async fn read_http_status_reads_head_then_ignores_body() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, mut server) = tokio::io::duplex(1024);
        server
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 9\r\n\r\n{\"err\":1}")
            .await
            .unwrap();
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_status(&mut client),
        )
        .await
        .expect("no hang")
        .expect("status");
        assert_eq!(status, 400);
        drop(server);
    }
}
