# tabbify-service-supervisor

App-layer **supervisor** for the Tabbify mesh (build target B of the
[App-Layer Phase-1 contract](../APP_LAYER_CONTRACT.md)).

It joins the Tabbify WireGuard mesh as a `supervisor`-tagged peer, fetches WASM
apps from S3 by UUID, runs them per a TOML lifecycle, and serves them over the
mesh on `[my_ula]:8730`. The `tabbify-service-node` proxies public traffic to
this supervisor's control/serve API.

## What it does (pipeline)

```
node ──mesh──▶ supervisor [my_ula]:8730
                 │  ANY /apps/<uuid>/<rest>
                 ▼
            AppRegistry (lifecycle)
                 │  lazy-spawn (on_request) / eager (always_on)
                 ▼
            S3Fetcher  ──HTTPS GET──▶ tabbify-apps.s3…/apps/<uuid>/{latest,v<N>/…}
                 │  cache <data_dir>/apps/<uuid>/v<N>/
                 ▼
            WasmRuntime (wasmtime 26, wasi:http/proxy)
                 │  fresh Store + fuel per request
                 ▼
            http::Response  ──▶ back to the node
```

## Modules

| File | Responsibility |
|---|---|
| `src/config.rs` | clap+env config: bind addr, coordinator URL, data dir, S3 base URL, repeatable `--app <uuid>`, `--no-mesh`. Defaults bake the prod EIP + bucket. |
| `src/manifest.rs` | Vendored `manifest.toml` schema (contract §3) — **byte-identical** to the cli's copy. No `deny_unknown_fields` (forward-compat). |
| `src/app_ula.rs` | Vendored `derive_app_ula` (contract §4) + golden test. Reported in the API but **not used for binding** in Phase-1. |
| `src/runtime.rs` | The `AppRuntime` trait (the runtime seam the per-app listener dispatches to, boxed-future / object-safe) + the minimal wasmtime-26 `wasi:http/proxy` runtime (contract §8), stripped of any custom host import. `WasmRuntime::load` compiles once; `handle` runs one request on a fresh `Store` with its own fuel budget. Normalizes path-only server URIs to satisfy `wasi:http`'s authority requirement. |
| `src/firecracker.rs` | The second `AppRuntime`: a **KVM-gated Firecracker microVM** runtime. Real impl on Linux (`#[cfg(target_os="linux")]`: per-VM tap + `/30`, spawns `firecracker`, configures via a hand-rolled unix-socket HTTP/1.1 REST client, boots the VM, proxies HTTP to the guest app, tears down on `Drop`); a stub elsewhere so the supervisor still builds + serves WASM on macOS. `kvm_available()` gates it + drives the `firecracker` mesh tag. |
| `src/fetcher.rs` | `S3Fetcher`: anonymous HTTPS GET `latest → v<N>/manifest.toml + v<N>/<runtime.entry>`, cached on disk under the entry's real name (`app.wasm` for wasm-http, `rootfs.ext4` for firecracker). `FetchedApp.cached_path` is the on-disk entry path (firecracker uses it directly — a rootfs is never read into RAM). Base URL is injectable (tests point it at a wiremock server). |
| `src/registry.rs` | `AppRegistry` + the lifecycle state machine. Policy is expressed as pure functions (`spawn_on_register`, `should_reap`) for unit-testing; the registry wires them to a `DashMap` + the fetcher + runtime. Per-uuid spawn lock prevents double-compile on concurrent first-requests. |
| `src/mesh.rs` | `MeshMembership`: wraps `Joiner::join` with `tags=["supervisor"]`, `insecure_no_mtls=true`, the env/baked coordinator URL; surfaces `my_ula` + `peer_id`. |
| `src/api.rs` | axum 0.7 router: `GET /health`, `GET /v1/apps`, `GET /v1/apps/:uuid`, `POST /v1/apps/:uuid/{start,stop}`, `ANY /apps/:uuid/*rest` (+ bare root). |
| `src/main.rs` | Thin shim: parse config → join mesh (or `--no-mesh`) → build registry → pre-register `--app` uuids → spawn idle reaper → bind `[my_ula]:8730` → serve. |

## Lifecycle semantics (contract §5)

- **`always_on`**: the instance is compiled + marked `running` as soon as the
  app is registered (pre-register at boot, or on first discovery). Never reaped.
- **`on_request`**: the instance lazy-spawns on the first `/apps/<uuid>/…`
  request; a background reaper stops it after `idle_timeout_sec` of no traffic,
  **unless pinned**.
- **API `start` pins** (sticky) → the reaper skips it ("API overrides
  on_request"). **`stop` unpins** and drops the instance.
- **States**: `available` (known/fetchable, not running) · `running` · `stopped`.

## Mesh dependency — wired as a **path dep** (compiles ✅)

The contract (§0) specifies the mesh git dependency:

```toml
mesh-joiner = { git = "ssh://git@github.com/tabbify-io/tabbify-service-mesh.git", package = "tabbify-mesh-joiner", rev = "5dccb67" }
```

This repo instead uses the **sibling path dep**, which the contract explicitly
permits as the local-build fallback:

```toml
mesh-joiner = { path = "../tabbify-service-mesh/crates/mesh-joiner", package = "tabbify-mesh-joiner" }
```

Reason: `tabbify-mesh-joiner` lives inside the mesh **workspace** — it declares
`[lints] workspace = true` and depends on its sibling `tabbify-mesh-fabric` via
a workspace-relative `{ path = "../mesh-fabric" }`. A git-dep on a single
workspace member is fragile (cargo must resolve the workspace context + the
sibling path). The local checkout is at `../tabbify-service-mesh` @ `5dccb67`
(verified), so the path dep is the reliable choice for a clean Phase-1 local
build. **It compiles cleanly** (`tabbify-mesh-fabric` + `tabbify-mesh-joiner`
build as part of `cargo build`).

**For CI / a standalone checkout**, swap the path dep for the git line above and
provide an SSH deploy key (the mesh remote is `git@github.com:…`, SSH-only).

## Build / run / test

```bash
cp .env.example .env          # optional, for local runs
just build                    # cargo build
just test                     # 27 tests, no network (S3 is mocked via wiremock)
just lint                     # cargo clippy --all-targets -- -D warnings
just fmt                      # cargo fmt --check

# Run joined to the mesh (needs root / NET_ADMIN + /dev/net/tun for the TUN):
sudo -E just run
#   ... or pre-host apps:
sudo -E cargo run --bin supervisord -- --app <uuid> --app <uuid>

# Run locally WITHOUT the mesh (loopback, no TUN — for poking the API by hand):
just run-local                # binds 127.0.0.1:8730, --no-mesh
```

### Cross-build for Kamatera (Phase-1 deploy)

```bash
rustup target add x86_64-unknown-linux-musl
just build-musl               # -> target/x86_64-unknown-linux-musl/release/supervisord
scp target/x86_64-unknown-linux-musl/release/supervisord <kamatera>:/opt/tabbify/
# on the host, run with the prod coordinator (baked default) + your app uuids:
./supervisord --app <uuid>
```

Cross-compiling from macOS needs a musl cross-linker (e.g. `cargo install cross`
and `cross build --release --target x86_64-unknown-linux-musl`, or build on a
Linux box). The `just build-musl` recipe is the cargo invocation; supply the
linker for your environment.

### CI release — musl binaries published to S3

`.github/workflows/release.yml` builds a static `supervisord` (musl) for BOTH
CPU architectures on every push to `main`, on tags `v*`, and on manual
`workflow_dispatch`, then uploads each to a per-arch S3 key. Both arches are
cross-compiled on a standard `ubuntu-latest` (x86_64) runner via
[`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) (zig is the
cross-linker), so no ARM runners are needed. No ECR, no SSM — just the binaries.

**Published paths:**

```
s3://<RELEASE_S3_BUCKET>/supervisor/x86_64/supervisord    # Intel/AMD hosts
s3://<RELEASE_S3_BUCKET>/supervisor/aarch64/supervisord   # ARM hosts (e.g. aarch64 Lima)
s3://<RELEASE_S3_BUCKET>/supervisor/supervisord           # legacy alias == x86_64 (kept for back-compat)
```

The un-prefixed `supervisor/supervisord` key is still published (a copy of the
x86_64 binary) so pre-existing consumers (the Phase-1 runbook, older curl
snippets) keep working; new consumers should use the per-arch key.

**Fetch and run on a Kamatera (or any Linux) host:**

```bash
ARCH=$(uname -m)   # x86_64 | aarch64
curl -fsSL \
  "https://<RELEASE_S3_BUCKET>.s3.eu-central-1.amazonaws.com/supervisor/${ARCH}/supervisord" \
  -o supervisord
chmod +x supervisord
sudo ./supervisord --name sup-kamatera --app <UUID>
```

**Required repo configuration** (Settings → Secrets and variables → Actions):

| Kind | Name | Value |
|---|---|---|
| Secret | `AWS_ROLE_ARN` | ARN of the IAM role to assume via GitHub OIDC. Trust policy must allow this repo; permission policy must grant `s3:PutObject` on `<bucket>/supervisor/*` (covers the per-arch keys and the legacy key). |
| Variable | `AWS_REGION` | AWS region of the bucket, e.g. `eu-central-1`. |
| Variable | `RELEASE_S3_BUCKET` | Bucket name (no `s3://` prefix), e.g. `tabbify-releases`. |

The `mesh-joiner` dependency is fetched anonymously over public HTTPS, so no
deploy key / ssh-agent is needed in CI.

### Docker

`Dockerfile` wraps the **prebuilt** static musl binary (see above) in
`debian:bookworm-slim` with `ca-certificates`. At runtime the supervisor opens a
TUN device, so the container needs `--cap-add NET_ADMIN --device /dev/net/tun`
(and `network_mode: host` to reach the coordinator + serve on the ULA), or
`--no-mesh` to skip the TUN entirely.

## HTTP API (contract §5)

| Method + path | Behavior |
|---|---|
| `GET /health` | `{"status":"ok","supervisor_id":"…","ula":"…"}` |
| `GET /v1/apps` | `{"apps":[{uuid,version,name,lifecycle,state}]}` |
| `GET /v1/apps/:uuid` | `{uuid,present,version,state,app_ula}`; `present:false` + 404 if not fetchable. Discovery tries S3 for unknown uuids. |
| `POST /v1/apps/:uuid/start` | fetch (if needed) + spawn + **pin** → `{"state":"running","app_ula":"…"}` |
| `POST /v1/apps/:uuid/stop` | stop + unpin → `{"state":"stopped"}` |
| `ANY /apps/:uuid/*rest` (+ bare root) | strip `/apps/<uuid>`, lazy-spawn per lifecycle, run the wasm, return its response |

## Tests

`cargo test` runs **27 tests** with no network access (the S3 object store is
mocked with `wiremock`; the WASM runtime executes the committed pure-`wasi:http`
fixture at `tests/fixtures/hello.wasm`). Highlights:

- `runtime::*` — the runtime compiles + executes the fixture (full URI and
  path-only+Host both return `200 "Hello, Tabbify!"`).
- `manifest::*` — canonical parse, defaults, unknown-field tolerance, stamped id.
- `app_ula::app_ula_is_stable` — golden ULA (`fd5a:1f02:44a5:240b:121a::1`).
- `registry::reap_policy_matrix` / `spawn_policy` — pure lifecycle policy.
- `integration::serve_app_runs_fixture_end_to_end` — `/apps/<uuid>/` through the
  full router → lazy-spawn → wasm → `Hello, Tabbify!`.
- `integration::serve_app_with_subpath_runs_fixture` — prefix strip + subpath + query.
- `integration::start_pins_app_so_reaper_skips_it` — pin overrides the reaper.
- `integration::always_on_app_spawns_on_register` — eager spawn.
- `integration::get_app_present_when_fetchable_from_s3` / `…_absent_…` — discovery.
- `integration::fetcher_*` — `latest`→manifest→wasm fetch + disk cache, 404, bad `latest`.

## Firecracker microVM runtime (KVM-gated, Linux)

A second runtime behind the `AppRuntime` trait, selected by
`manifest.runtime.type = "firecracker"` (vs `"wasm-http"`). Served identically
on the per-app-ULA endpoint — the node/coordinator route by app-ULA and need no
change. **WASM runs on any host; Firecracker runs only on Linux with `/dev/kvm`.**

A firecracker-capable supervisor host needs:

- **Linux** + **`/dev/kvm`** (bare-metal, a nested-virt VPS, or Lima/UTM Ubuntu);
- the **`firecracker`** binary on `$PATH` (or `--firecracker-bin`);
- a guest **`vmlinux`** kernel (default `/opt/tabbify/vmlinux`, or
  `--firecracker-kernel` / per-app `manifest.runtime.kernel`);
- **`iproute2`** (`ip tuntap/addr/link`) and the privilege to create taps.

The app's S3 artifact is a **rootfs image** (e.g. `rootfs.ext4`) named per
`manifest.runtime.entry`, with an HTTP server inside the guest on
`--firecracker-app-port` (default 8080). At startup `kvm_available()` adds the
`firecracker` mesh tag on a KVM host (and logs the capability either way); a
no-KVM host serves WASM and refuses firecracker apps with a clear error. WASM
supervisors run anywhere and route firecracker apps to a KVM box over the mesh.

Config (all `--flag` / `ENV`): `--firecracker-bin` (`SUPERVISOR_FC_BIN`),
`--firecracker-kernel` (`SUPERVISOR_FC_KERNEL`), `--firecracker-vcpus`
(`SUPERVISOR_FC_VCPUS`), `--firecracker-tap-subnet` (`SUPERVISOR_FC_TAP_SUBNET`,
per-VM `/30`), `--firecracker-app-port` (`SUPERVISOR_FC_APP_PORT`).

A REAL VM boot is exercised by `firecracker::linux::tests::real_vm_boots_and_serves`
(`#[cfg(target_os="linux")] #[ignore]`) — run it on a KVM box:

```sh
# Lima Ubuntu, as root: /opt/tabbify/vmlinux + /tmp/rootfs.ext4 (app on :8080)
sudo -E cargo test real_vm_boots_and_serves -- --ignored --nocapture
```

The cross-platform tests (runtime selection, the firecracker REST request
bodies, the unix-socket status parsing, the proxy via a wiremock "fake VM",
manifest + config parsing) run on the macOS dev host with no KVM. (tcli rootfs
packaging is a follow-up; the supervisor already fetches the entry file
generically.)

## Notes / Phase-1 deviations

- **Mesh dep is a path dep, not the git rev** (see above) — permitted fallback;
  compiles. Swap to git + deploy key for CI.
- **App-ULA is reported but not used for binding** (contract §4 / §5): the
  supervisor serves every app on its own peer-ULA. Per-app-ULA binding is a
  deferred optimization.
- **`--no-mesh`** is an extra local-testing escape hatch (binds a plain
  loopback/`--bind` addr, skips the TUN) — not in the contract, added so the
  service is runnable + testable without root. Production runs join the mesh.
