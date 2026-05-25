# Firecracker Runtime — Design (supervisor)

> Leo: "у супервизора будет проверка — если есть KVM, позволяем запустить Firecracker";
> "по умолчанию Firecracker запускается на Linux."
>
> Goal: the supervisor hosts apps via TWO interchangeable runtimes behind one
> `AppRuntime` trait, chosen by `manifest.runtime.type`, served **identically on the
> per-app-ULA endpoint** (`[derive_app_ula(uuid)]:8730`, lazy/always-on lifecycle —
> all unchanged). **WASM = any host. Firecracker = Linux + `/dev/kvm` only (KVM-gated).**
> Firecracker cannot run without hardware KVM (confirmed: EC2 t3.micro has no /dev/kvm),
> so the supervisor degrades gracefully: a no-KVM host serves WASM and refuses
> firecracker apps; a KVM host serves both and advertises the capability.

## 1. `AppRuntime` trait — `src/runtime.rs`
The per-app listener (`host.rs:serve_app`) only needs `handle`. Introduce (boxed-future,
matching the existing `MeshHost` pattern at `host.rs:46` — no `async_trait` dep):
```rust
pub trait AppRuntime: Send + Sync {
    fn handle<'a>(&'a self, req: http::Request<Bytes>)
        -> Pin<Box<dyn Future<Output = anyhow::Result<http::Response<Bytes>>> + Send + 'a>>;
}
```
- `WasmRuntime` impls it (keep `load_with_fuel`; wrap the existing async `handle`).
- `FirecrackerRuntime` impls it.
- `AppServe.runtime` (host.rs) becomes `Arc<dyn AppRuntime>` (replaces the `WasmRuntime: Clone` sharing). `host_app` (registry.rs:376) takes `Arc<dyn AppRuntime>`.

## 2. Runtime selection — `src/registry.rs`
The two `WasmRuntime::load_with_fuel(...)` sites (register ~228, ensure_running ~295) branch:
```rust
let rt: Arc<dyn AppRuntime> = match fetched.manifest.runtime.r#type.as_str() {
    "wasm-http" => Arc::new(WasmRuntime::load_with_fuel(&fetched.wasm, fuel)?),
    "firecracker" => Arc::new(FirecrackerRuntime::launch(&fetched.cached_path, &fetched.manifest.runtime, &fc_cfg).await?),
    other => anyhow::bail!("unknown runtime type: {other}"),
};
```
`FirecrackerRuntime::launch` returns `Err` (clear message) if `!kvm_available()` or on non-Linux → the host-attempt fails loudly, the app stays `Available`/errors (WASM apps unaffected).

## 3. KVM gate + capability — `src/firecracker.rs`
```rust
pub fn kvm_available() -> bool   // /dev/kvm exists AND is R/W openable
```
At startup the supervisor calls it once; if true, ADD the mesh tag `"firecracker"` to the
joiner tags (so coordinator/node know which supervisors can host firecracker apps — node
sends a firecracker app's `start` to a `firecracker`-tagged supervisor). Logged either way.

## 4. `FirecrackerRuntime` — `src/firecracker.rs`
Real impl `#[cfg(target_os = "linux")]`; a `#[cfg(not(target_os = "linux"))]` stub whose
`launch` returns `Err("firecracker runtime requires Linux + /dev/kvm")` so the supervisor
still BUILDS + runs (WASM) on macOS dev.

Linux impl, `launch(rootfs: &Path, rt: &manifest::Runtime, cfg: &FcConfig) -> Result<Self>`:
1. `kvm_available()` guard.
2. Allocate a per-VM tap + /30 link: `ip tuntap add <tap> mode tap`, `ip addr add <host_ip>/30 dev <tap>`, `ip link set <tap> up`. Guest IP = host_ip+1.
3. Spawn `firecracker --api-sock <sock>` (`cfg.bin`, default `firecracker`).
4. Configure via the unix-socket HTTP API (firecracker REST):
   - `PUT /machine-config` `{vcpu_count: cfg.vcpus, mem_size_mib: rt.memory_mb}`
   - `PUT /boot-source` `{kernel_image_path: rt.kernel||cfg.kernel, boot_args: "console=... ip=<guest_ip>::<host_ip>:255.255.255.252::eth0:off ..."}`
   - `PUT /drives/rootfs` `{drive_id:"rootfs", path_on_host:<rootfs>, is_root_device:true, is_read_only:false}`
   - `PUT /network-interfaces/eth0` `{iface_id:"eth0", host_dev_name:<tap>, guest_mac:<mac>}`
   - `PUT /actions` `{action_type:"InstanceStart"}`
5. Poll `http://<guest_ip>:<cfg.app_port>` until ready (timeout) — the app's HTTP server inside the VM.
6. `handle(req)`: proxy via `reqwest` to `http://<guest_ip>:<app_port>` (whole path → the VM app), stream the response back. Strip hop-by-hop headers.
7. `Drop`: kill the firecracker child + `ip link del <tap>`.

Use the firecracker unix-socket API over hyper/reqwest with a unix connector (or `firepilot`/`rust-firecracker-api` crate if clean; else hand-rolled minimal client — prefer hand-rolled, ~150 LOC, no heavy dep).

## 5. Manifest — `src/manifest.rs`
`runtime.type` accepts `"firecracker"`. Add `#[serde(default)] pub kernel: Option<String>`
(kernel image path/name; `None` → supervisor's `cfg.kernel`). `entry` = the rootfs filename
for firecracker (e.g. `rootfs.ext4`), the wasm filename for wasm-http. (cli's vendored
manifest tolerates these via no-`deny_unknown_fields`; sync it as a minor follow-up.)

## 6. Fetcher — `src/fetcher.rs`
Fix the hardcoded `app.wasm` cache path (fetcher.rs:128,131) → use `manifest.runtime.entry`
for BOTH the cache write and the cache-hit check. `FetchedApp` gains `cached_path: PathBuf`
(the on-disk entry file). WASM reads bytes (keep `wasm: Bytes` or read from path); Firecracker
gets `cached_path` (the rootfs — never load a multi-hundred-MB rootfs into memory).

## 7. Config — `src/config.rs` (`FcConfig`, all `#[arg(long, env)]`)
`--firecracker-bin` (def `firecracker`), `--firecracker-kernel` (def `/opt/tabbify/vmlinux`),
`--firecracker-vcpus` (def 1), `--firecracker-tap-subnet` (def `172.31.0.0/16` → per-VM /30),
`--firecracker-app-port` (def 8080). Only consulted when hosting a firecracker app.

## 8. Tests (TDD; runtime-verified later on a KVM host — Leo's Lima)
Cross-platform (run on macOS dev): `AppRuntime` trait + WasmRuntime serves the fixture
through the trait (existing test, adapted); runtime selection branch (wasm-http → WasmRuntime,
firecracker+no-KVM → clear Err, unknown → Err); `kvm_available` with an injected checker;
manifest firecracker-type + kernel parse; the firecracker REST request bodies (build + assert
JSON without a real socket); proxy logic via wiremock (fake VM HTTP). The real VM boot
(`#[cfg(target_os="linux")]` + `#[ignore]` integration) is documented for Lima.
**MUST**: `cargo build` + `cargo test` + clippy + fmt green on macOS; AND `cargo check
--target x86_64-unknown-linux-musl` green (so the `cfg(linux)` real impl compiles).

## 9. Deploy/run note (`deploy/README.md`)
A firecracker-capable supervisor needs: Linux + `/dev/kvm` + the `firecracker` binary + a
`vmlinux` kernel + `iproute2`. The app's S3 artifact = a rootfs image (ext4) named per
`manifest.runtime.entry`, with `runtime.type="firecracker"`. (tcli rootfs packaging = a
follow-up; the supervisor already fetches the entry file generically.) Host on a KVM box
(bare-metal / nested-virt VPS / Lima); WASM supervisors run anywhere and route to it via the
mesh by app-ULA — no change to the node/coordinator.
