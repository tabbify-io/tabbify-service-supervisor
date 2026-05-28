//! Cross-platform firecracker protocol helpers: the REST request-body builders
//! and the hop-by-hop header filter. These are consumed by the Linux runtime
//! ([`super::linux`]) and by the unit tests; on a non-Linux build only the
//! tests use them, hence the module-level `allow(dead_code)` for that case (a
//! plain `cargo build` on macOS compiles them but doesn't call them).

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

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

/// Build the `PATCH /vm` body that pauses the running VM.
///
/// Used before taking a snapshot: the VM state must be quiescent so
/// the memory dump is consistent.
pub fn pause_body() -> Value {
    json!({ "state": "Paused" })
}

/// Build the `PATCH /vm` body that resumes a paused VM.
///
/// Called after snapshot creation completes to return the VM to serving.
pub fn resume_body() -> Value {
    json!({ "state": "Resumed" })
}

/// Build the `PUT /snapshot/create` body.
///
/// `snapshot_path` — destination path for the vmstate file (small, metadata).
/// `mem_file_path`  — destination path for the guest RAM dump (large).
///
/// Firecracker API: `PUT /snapshot/create` with `"snapshot_type": "Full"`.
pub fn snapshot_create_body(snapshot_path: &str, mem_file_path: &str) -> Value {
    json!({
        "snapshot_type": "Full",
        "snapshot_path": snapshot_path,
        "mem_file_path": mem_file_path,
    })
}

/// Build the `PUT /snapshot/load` body.
///
/// `snapshot_path` — path to the previously-saved vmstate file.
/// `mem_file_path`  — path to the previously-saved RAM dump.
/// `resume`         — when `true`, the guest resumes execution immediately
///                    after the snapshot is loaded (skip an extra PATCH /vm).
///
/// Firecracker API: `PUT /snapshot/load`.
pub fn snapshot_load_body(snapshot_path: &str, mem_file_path: &str, resume: bool) -> Value {
    json!({
        "snapshot_path": snapshot_path,
        "mem_backend": {
            "backend_path": mem_file_path,
            "backend_type": "File",
        },
        "resume_vm": resume,
    })
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
