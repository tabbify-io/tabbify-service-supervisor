# Tabbify App-Layer — Phase 1 Integration Contract

> ⚠️ **SINGLE-RUNTIME MODEL (2026-06-03, Phase 4 / fly.io model):** Tabbify now
> runs ONE runtime — an OCI image booted as a Firecracker microVM. The wire
> `Runtime` enum (§3 / D4) collapsed to a single `Firecracker` variant with
> LENIENT deserialize (any legacy `"docker"`/`"wasm-http"`/`"node-firecracker"`
> string coerces to `Firecracker`). The **runtime-override clause (D10) is
> REMOVED** — the `{"runtime":"…"}` body is still accepted for back-compat but is
> inert and ignored. Applied IDENTICALLY in `tabbify-cli` + `tabbify-service-node`
> + `tabbify-service-supervisor`.

> ⚠️ **SUPERVISOR MODEL SUPERSEDED (2026-05-26):** `tabbify-service-supervisor`
> is now an **orchestrator-of-runners**. It no longer hosts apps in-process. Each
> app runs in a separate detached `tabbify-runner` process (a mesh peer in its own
> right). See the [Runner architecture](#runner-architecture-orchestrator--tabbify-runner)
> section below for the current model. All wire-format sections (§2 S3, §3 manifest,
> §4 app-ULA, §5 mesh/ports, §6 node) remain authoritative.

> ⚠️ **ROUTING SUPERSEDED (2026-05-25):** the Phase-1 routing shortcut below in
> §5/§6 (supervisor serves apps on its peer-ULA; node queries supervisors + proxies)
> has been REPLACED by the proper **per-app-ULA direct routing** design:
> `tabbify-service-mesh/docs/superpowers/specs/2026-05-25-per-app-ula-routing-design.md`.
> Now: supervisor binds each app on `[derive_app_ula(uuid)]:8730` + advertises it
> (coordinator `hosted_app_ulas`); the joiner installs the mesh route; node dials
> `[app_ula]:8730` DIRECTLY (no query/proxy-via-supervisor). Everything else here
> (S3 layout §2, manifest §3, app-ULA §4, UUID/lock §1, conventions §0) is current.

> Status: authoritative for Phase 1. This is the **shared seam** the three services
> (`tabbify-cli`, `tabbify-service-supervisor`, `tabbify-service-node`) + the test
> WASM app are built against. If a wire format / port / path is written here, it is
> binding — do not diverge. Vendored types (manifest, app-ULA) MUST match byte-for-byte
> across repos; the golden tests below pin them.

Workspace layout (sibling git repos, no root git):
```
tabbify-io/
  tabbify-service-mesh/     # existing — mesh-joiner lib + coordinator (git@github.com:tabbify-io/tabbify-service-mesh.git @ main)
  tabbify-service-auth/     # existing — standalone-service TEMPLATE (Dockerfile/justfile/CI/config)
  tabbify-infra/            # existing — terraform (live/ + live/modules/)
  tabbify-cli/              # NEW — build target A
  tabbify-service-supervisor/  # NEW — build target B
  tabbify-service-node/     # NEW — build target C
```

Personal AWS account `104218736623`, region `eu-central-1`. Coordinator EIP `3.124.69.92`.

---

## 0. Global conventions (all three repos)

Copy from `tabbify-service-auth`:
- Rust **edition 2024**, `rust-toolchain.toml` channel `stable`, `clippy.toml` (`msrv = "1.85.0"`), `rustfmt.toml` (max_width 100, imports_granularity Crate, group_imports StdExternalCrate).
- Thin `[[bin]]` + fat `[lib]` split (logic in lib, binary is a shim) — enables unit tests.
- Config via `clap::Parser` struct, every field `#[arg(long, env = "VAR")]`. No dotenv crate in prod; `set dotenv-load` in justfile for local.
- `tracing` + `tracing_subscriber::EnvFilter::try_from_default_env()` with a hardcoded fallback.
- `anyhow` (bin) / `thiserror` (lib error enums).
- `justfile` with `default`/`dev`/`run`/`test`/`lint`/`fmt`.
- `.gitignore`, `.dockerignore` mirrored from auth.
- **English only** in code/docs/comments/logs/commits. **Single squashed `initial commit`** (public repo rule). No AI attribution anywhere.
- TDD: every behavior gets a failing test first. `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` all green before commit.
- Git identity is already set (Leo / leontij@pm.me). Commit with precise paths inside each repo only.

Mesh dependency (supervisor + node):
```toml
mesh-joiner = { git = "ssh://git@github.com/tabbify-io/tabbify-service-mesh.git", package = "tabbify-mesh-joiner", rev = "5dccb67" }
```
Local cargo fetches over Leo's SSH key (the mesh remote is SSH and already authenticates). For CI builds, the repo must be reachable (SSH deploy key secret or public); note it in the README — do not block Phase-1 local builds on it.

---

## 1. Identity, versioning, lockfile

- **App UUID = UUID v7**, minted by `tcli` on first push.
- **`tabbify.lock`** (TOML) lives in the app source dir, authoritative for identity:
  ```toml
  app_id = "0191e7c2-....-....-....-............"   # uuid v7, minted once
  latest_version = 3                                  # integer, monotonic, bumped each push
  ```
- Re-running `tcli push <dir>` reads the lock → same `app_id`, `latest_version + 1`.
- **Versions are integers**: `1, 2, 3, …`. S3 stores them as `v<N>`.
- `tcli` stamps `app.id = <app_id>` into the uploaded `manifest.toml` (overriding/inserting), so the manifest in S3 always carries the correct UUID. The author need not hand-write `app.id`.

---

## 2. S3 layout — bucket `tabbify-apps` (region eu-central-1)

```
tabbify-apps/
  apps/<uuid>/
    latest                       # text body = current version number, e.g. "3"
    v<N>/
      manifest.toml              # stamped manifest (app.id set, see §3)
      app.wasm                   # the wasm32-wasip2 component (named per manifest runtime.entry; canonical name app.wasm)
      <any other files in the app dir, preserved by relative path>
```

- **`tcli push`**: uploads every file in `<app-dir>` (except `tabbify.lock`) under `apps/<uuid>/v<N>/`, guaranteeing `manifest.toml` + the entry wasm are present; then writes/overwrites `apps/<uuid>/latest` with `<N>`. PutObject via `aws-sdk-s3` (default cred chain — Leo's creds).
- **supervisor fetch** (no AWS creds on Kamatera): plain anonymous HTTPS GET. Base URL:
  `https://tabbify-apps.s3.eu-central-1.amazonaws.com/apps/<uuid>/`
  Read `latest` → N → GET `v<N>/manifest.toml` and `v<N>/app.wasm`. Cache locally under `<data_dir>/apps/<uuid>/v<N>/`.
- Bucket policy: **public read** on `apps/*` (GetObject for `*`), writes IAM-gated. Rationale: the supervisor host (Kamatera) has no AWS identity; mirrors the existing public-read releases bucket. (Deferred hardening: presigned URLs / IAM read — out of Phase-1 RnD scope.)

---

## 3. `manifest.toml` schema (canonical — vendor IDENTICALLY in cli + supervisor)

Derived from substrate `tabbify-app-manifest`, **simplified to Phase-1 lifecycle vocabulary**. Define this Rust module identically in `tabbify-cli` and `tabbify-service-supervisor` (suggest `src/manifest.rs`). Do **NOT** use `deny_unknown_fields` (forward-compat). Use `serde` + `toml`.

> **Single-runtime / fly.io model (Phase 4).** `[runtime].type` is now INERT.
> Tabbify runs ONE runtime — an OCI image booted as a Firecracker microVM — so a
> legacy `type = "wasm-http"` / `"docker"` is tolerated by serde and IGNORED (the
> wire `Runtime` enum, separate from this `[runtime]` table, deserializes any
> string to `Firecracker`; see D4). `entry` / `fuel_per_request` are likewise
> WASM-era leftovers kept for back-compat.

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppManifest {
    pub app: AppMeta,
    pub lifecycle: Lifecycle,
    pub runtime: Runtime,
    #[serde(default)]
    pub routes: Routes,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppMeta {
    #[serde(default)]
    pub id: Option<Uuid>,          // optional in source; tcli stamps it before upload
    pub name: String,
    #[serde(default)]
    pub version: String,           // display only; S3 `latest` is authoritative
    #[serde(default = "default_kind")]
    pub kind: String,              // "headless" | "widget" | ... (free string, Phase-1)
    #[serde(default)]
    pub description: String,
}
fn default_kind() -> String { "headless".into() }

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Lifecycle {
    pub mode: LifecycleMode,
    #[serde(default = "default_idle")]
    pub idle_timeout_sec: u64,     // used by on_request to stop idle instances
}
fn default_idle() -> u64 { 300 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleMode {
    AlwaysOn,    // "always_on"  — spawn on deploy/registration, keep running
    OnRequest,   // "on_request" — lazy spawn on first request, stop after idle_timeout_sec
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Runtime {
    #[serde(rename = "type", default = "default_rt")]
    pub r#type: String,            // "wasm-http" (Phase-1 only)
    #[serde(default = "default_entry")]
    pub entry: String,             // "app.wasm"
    #[serde(default = "default_fuel")]
    pub fuel_per_request: u64,     // 1_000_000_000
    #[serde(default = "default_mem")]
    pub memory_mb: u32,            // 64
}
fn default_rt() -> String { "wasm-http".into() }
fn default_entry() -> String { "app.wasm".into() }
fn default_fuel() -> u64 { 1_000_000_000 }
fn default_mem() -> u32 { 64 }

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Routes {
    #[serde(default)]
    pub dynamic_prefixes: Vec<String>,   // Phase-1: ["/"] = all paths go to wasm
}
```

Canonical example `manifest.toml`:
```toml
[app]
name        = "hello-tabbify"
kind        = "headless"
description = "Phase-1 hello-world WASI-HTTP component"

[lifecycle]
mode             = "on_request"
idle_timeout_sec = 300

[runtime]
type             = "wasm-http"
entry            = "app.wasm"
fuel_per_request = 1000000000
memory_mb        = 64

[routes]
dynamic_prefixes = ["/"]
```

### D4 — runtime wire enum (FROZEN, vendor IDENTICALLY in cli + node + supervisor)

The deploy-time runtime selector is a vendored `Runtime` enum (`src/runtime.rs`),
distinct from the `[runtime]` table above. **Single-runtime / fly.io model
(Phase 4):** it has ONE variant, `Firecracker`, and serializes to the FROZEN
wire string `"firecracker"`. Deserialize is **LENIENT** — any legacy string
(`"docker"`, `"wasm-http"`, `"node-firecracker"`, …) COERCES to `Firecracker`
rather than erroring, so older wire payloads, `tabbify.toml`s, and on-disk
records keep deserializing. `Runtime::default()` is `Firecracker`. Each repo
carries the golden round-trip test pinning serialize→`"firecracker"` + the
lenient coerce. Do **NOT** add `deny_unknown_fields` to bodies carrying it.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Runtime { #[default] Firecracker }
impl Runtime { pub fn as_wire(self) -> &'static str { "firecracker" } }
// Serialize → "firecracker"; Deserialize → accept ANY string, coerce to Firecracker.
```

---

## 4. App ULA derivation (vendor IDENTICALLY in supervisor + node; golden-tested)

```rust
use std::net::Ipv6Addr;
use uuid::Uuid;

const APP_ULA_PREFIX_HI: u16 = 0xfd5a;
const APP_ULA_PREFIX_LO: u16 = 0x1f02;

/// Deterministic per-app ULA: fd5a:1f02:<blake3(uuid)[0..6] as 3×u16 BE>::1
pub fn derive_app_ula(app_uuid: Uuid) -> Ipv6Addr {
    let digest = blake3::hash(app_uuid.as_bytes());
    let b = digest.as_bytes();
    let h0 = u16::from_be_bytes([b[0], b[1]]);
    let h1 = u16::from_be_bytes([b[2], b[3]]);
    let h2 = u16::from_be_bytes([b[4], b[5]]);
    Ipv6Addr::new(APP_ULA_PREFIX_HI, APP_ULA_PREFIX_LO, h0, h1, h2, 0, 0, 1)
}
```
Golden test (MUST pass identically in both repos):
```rust
#[test]
fn app_ula_is_stable() {
    let u = Uuid::parse_str("0191e7c2-1111-7222-8333-444455556666").unwrap();
    // blake3 of those 16 bytes — compute once, then pin the literal here.
    assert!(derive_app_ula(u).to_string().starts_with("fd5a:1f02:"));
}
```
NOTE for Phase-1: app-ULA is vendored + tested for forward-compat but **NOT used for binding**. The supervisor serves apps on its own peer-ULA (see §5). Per-app-ULA binding is a deferred optimization.

---

## 5. Mesh membership + supervisor control/serve API

Both supervisor and node embed `mesh-joiner` and call at startup:
```rust
let joiner = Joiner::join(JoinConfig {
    display_name: <name>,
    tags: vec!["supervisor".into()],   // node: vec!["node".into()]
    insecure_no_mtls: true,
    ..Default::default()               // coordinator_url default is the baked prod EIP via the CLI; set it explicitly here:
}).await?;
```
IMPORTANT: `JoinConfig::default().coordinator_url` is the **dev** `127.0.0.1:8888`. Each service MUST set `coordinator_url` to the prod coordinator, taken from env `TABBIFY_MESH_COORDINATOR` with default **`http://3.124.69.92:8888`** (bake the prod EIP as the default, mirroring the joiner binary — zero-config).
Then `let my_ula = joiner.my_ula();` and bind listeners on `[my_ula]:PORT`.

**Ports:**
| Service | Listener | Purpose |
|---|---|---|
| coordinator | `3.124.69.92:8888` | mesh control (existing) |
| supervisor | `[my_ula]:8730` | control API + app traffic (over mesh, plaintext HTTP) |
| node | `0.0.0.0:8090` | PUBLIC REST + MCP + health (8090 is taken by auth on the box) |
| node | (mesh peer; dials supervisor `[ula]:8730`) | discovery + proxy |

**Supervisor HTTP API (axum, on `[my_ula]:8730`):**

- `GET /health` → `200 {"status":"ok","supervisor_id":"<peer_id>","ula":"<my_ula>"}`
- `GET /v1/apps` → list everything this supervisor knows:
  ```json
  {"apps":[{"uuid":"...","version":3,"name":"hello-tabbify","lifecycle":"on_request","state":"running|stopped|available"}]}
  ```
  `state`: `available` = artifact known/fetchable but not running; `running` = instance live; `stopped` = explicitly stopped.
- `GET /v1/apps/<uuid>` → "do you have it?":
  ```json
  {"uuid":"...","present":true,"version":3,"state":"running","app_ula":"fd5a:1f02:..::1"}
  ```
  `present=false` (404 body or `{"present":false}`) when this supervisor cannot serve it.
- `POST /v1/apps/<uuid>/start` → fetch (if needed) + instantiate + pin → `{"state":"running","app_ula":"..."}`. **Pinning**: an API start sets a sticky flag so the idle reaper will NOT stop it (this is the "API overrides on_request" rule).
- **Runtime override (D10) — REMOVED (single-runtime / fly.io model, Phase 4).**
  There is no longer a runtime to override: Tabbify runs ONE runtime (an OCI
  image booted as a Firecracker microVM). `POST /v1/apps/<uuid>/start` and
  `.../deploy` STILL ACCEPT an optional `{"runtime": "…"}` body for wire
  back-compat, but the value is now INERT — any string deserializes (lenient
  coerce, see D4) and is ignored; the supervisor always builds the single
  runtime. Old clients sending `{"runtime":"docker"}` keep working unchanged.
- `POST /v1/apps/<uuid>/stop` → stop + unpin → `{"state":"stopped"}`.
- **App traffic**: `ANY /apps/<uuid>/{*rest}` → if not running, lazy-spawn per lifecycle (on_request) → strip `/apps/<uuid>` prefix → hand the rewritten `http::Request<Bytes>` to the WASM instance → return its `http::Response<Bytes>`. This is the path the node proxies to.

**Lifecycle semantics:**
- `always_on`: supervisor spawns the instance as soon as the app is registered/known (Phase-1: on first `GET /v1/apps/<uuid>` resolution or an explicit registration call — simplest is to spawn on first fetch and never idle-reap).
- `on_request`: instance spawns on the first `/apps/<uuid>/...` request; an idle reaper stops it after `idle_timeout_sec` of no requests, UNLESS pinned via API start.
- How does a supervisor learn which apps exist? Phase-1: a supervisor is told to host an app by **`POST /v1/apps/<uuid>/start`** (from node, or directly), OR it is configured with a static list (env/CLI `--app <uuid>` repeatable) to pre-host. Implement BOTH: a `--app <uuid>` flag to pre-register (fetch metadata, honor always_on), and on-demand registration when a start/serve arrives for an unknown uuid (fetch from S3, then serve). Discovery (node→supervisor `GET /v1/apps/<uuid>`) returns `present:true` if the supervisor has the uuid in its known set OR can fetch it from S3 (Phase-1: try fetch; if S3 has it, present:true).

---

## 6. node — public REST(OpenAPI) + MCP (one binary)

- Public listener `0.0.0.0:8090` (env `TABBIFY_NODE_BIND`). NOTE: **8090, not 8090** — the auth service already occupies `:8090` on the same EC2 box. Joins mesh tagged `["node"]` to dial supervisor ULAs.
- **Auth**: hardcoded external key, constant-time `Authorization: Bearer <KEY>` check (copy auth's `constant_time_eq`). Const default `TABBIFY_NODE_KEY` overridable by env `TABBIFY_NODE_KEY`. `GET /health` and the OpenAPI/swagger docs are unauthenticated; everything else requires the key. (RnD: hardcoded default is fine; auth-server integration is later.)
- **OpenAPI** via `utoipa = "5"` + `utoipa-swagger-ui = "8"` (axum 0.7). Serve spec at `GET /openapi.json`, Swagger UI at `/swagger-ui`.

**REST endpoints:**
- `GET /health` (no auth) → `{"status":"ok"}`.
- `GET /v1/supervisors` → query coordinator `GET http://3.124.69.92:8888/v1/mesh/peers`, filter `tags` contains `"supervisor"`, return `[{"peer_id","ula","display_name","tags"}]`.
- `GET /v1/apps` → fan-out `GET http://[sup_ula]:8730/v1/apps` to all supervisors, merge → `[{uuid,version,name,lifecycle,state,supervisor_ula}]`.
- `GET /app/<uuid>` and `ANY /app/<uuid>/{*rest}` → **the core path**:
  1. cache lookup `uuid → supervisor_ula` (in-memory `DashMap`, TTL ok to skip Phase-1).
  2. on miss: `GET /v1/mesh/peers` → for each `supervisor`-tagged peer, `GET http://[ula]:8730/v1/apps/<uuid>` until `present:true`; cache the winner.
  3. **proxy** the incoming request (method, headers, body, the `<rest>` subpath + query) to `http://[supervisor_ula]:8730/apps/<uuid>/<rest>`; stream back status+headers+body.
  4. 404 if no supervisor has it.
- `POST /v1/apps/<uuid>/start` → resolve a supervisor (cached or any) → proxy `POST /v1/apps/<uuid>/start`; cache it.
- `POST /v1/apps/<uuid>/stop` → resolve → proxy stop.

**MCP** (`POST /mcp`, same Bearer auth): hand-rolled JSON-RPC 2.0 (copy substrate `tabbify-mcp-server` dispatcher), `protocolVersion "2024-11-05"`. Methods: `initialize`, `tools/list`, `tools/call`, `ping`. Tools (mirror REST): `list_supervisors`, `list_apps`, `get_app` (args `{uuid}` → resolve+proxy GET /, return body), `start_app` `{uuid}`, `stop_app` `{uuid}`. Single request→response JSON (no SSE needed Phase-1).

---

## 7. Test WASM app — `tabbify-cli/examples/hello-tabbify/`

- `cdylib`, `wit-bindgen = "0.34"`, world = pure `wasi:http/proxy@0.2` only (NO custom imports — must instantiate with stock `wasmtime-wasi` + `wasmtime-wasi-http` linkers). Mirror substrate `crates/wasm-http-runtime/tests/fixtures/hello-fixture-src` (NOT `example-http-app`, which imports a substrate `event-log` interface).
- Behavior: any request → `200 text/plain` body `Hello, Tabbify!` (include a build-stamped marker so a re-push shows a different body, e.g. `Hello, Tabbify! [build 2]`).
- Ships with its `manifest.toml` (the §3 canonical example) and builds via `cargo build --target wasm32-wasip2 --release` → copy artifact to `app.wasm` next to the manifest so `tcli push examples/hello-tabbify` works.

---

## 8. WASM runtime (inside supervisor) — exact wasmtime 26 glue

Minimal, standalone (no substrate dep, no custom host imports). Crates:
```toml
wasmtime           = { version = "26", features = ["component-model", "async"] }
wasmtime-wasi      = "26"
wasmtime-wasi-http = "26"
http = "1"
bytes = "1"
```
Pattern (from substrate `wasm-http-runtime`, stripped of EventLogHost):
- `Engine` with `Config::new().wasm_component_model(true).async_support(true).consume_fuel(true)`.
- Linker: `wasmtime_wasi::add_to_linker_async(&mut linker)` + `wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)` (or the proxy linker helper for v26).
- Store ctx implements `WasiView` + `WasiHttpView`; `store.set_fuel(fuel_per_request)` per request.
- Per request: fresh `Store`, `ProxyPre::new(linker.instantiate_pre(&component)?)`, `proxy.wasi_http_incoming_handler().call_handle(store, incoming, outparam).await`, collect response via the `oneshot` `ResponseOutparam` channel → `http::Response<Bytes>`.
- Public API: `WasmRuntime::load(&wasm_bytes) -> WasmRuntime` (compiles Component once), `runtime.handle(http::Request<Bytes>) -> Result<http::Response<Bytes>>`.
- Unit-test it with a committed fixture `tests/fixtures/hello.wasm` (build a tiny pure-proxy component once and commit the artifact, so `cargo test` needs no wasm build step).

---

## 9. Deploy (agent D)

- **terraform** in `tabbify-infra` (write only — apply is via push→CI, surfaced for confirmation):
  - new module `live/modules/apps/` mirroring `releases/` but **public-read on `apps/*`** (so Kamatera supervisor can GET without creds); versioning + BucketOwnerEnforced same as releases. Bucket name `tabbify-apps`. Wire `module "apps"` in `live/main.tf`, output the bucket name.
  - extend `module "registry".repository_names` with `"tabbify-node"`.
  - `coordinator` security group: add ingress `tcp/8090` from `0.0.0.0/0` (node public API).
  - GHA release role: add `s3:PutObject` on `tabbify-apps/apps/*` is NOT needed (tcli uses Leo's creds, not CI) — skip unless node-CI needs it. Node image push reuses existing ECR push perms (the registry module + gha_release_ecr already covers `values(repository_arns)`, which now includes node).
- **node Dockerfile**: build musl in CI, `COPY` the static binary into `debian:bookworm-slim` (or distroless) runtime; needs `ca-certificates`. (Node creates a TUN → at runtime the container needs `cap_add: NET_ADMIN` + `/dev/net/tun`; document in compose.)
- **node release CI** (`.github/workflows/release.yml`): mirror mesh's — checkout node + mesh repo (sibling, for the git-dep/path), `cargo build --release --target x86_64-unknown-linux-musl`, ECR login, build+push `tabbify-node` image, then **SSM send-command** (POSIX `set -eu`, target `Key=tag:Name,Values=tabbify-coordinator`) → `cd /opt/tabbify && docker compose pull && docker compose up -d`. Poll `list-command-invocations` for Success.
- **compose.yaml.tftpl** (coordinator module): add `node` service — image `${node_image}`, `network_mode: host` (needs to dial mesh + bind 8090), `cap_add: [NET_ADMIN]`, `devices: ["/dev/net/tun:/dev/net/tun"]`, env `TABBIFY_NODE_KEY`, `TABBIFY_MESH_COORDINATOR=http://3.124.69.92:8888`. Add `ecr_node_repo` + `node_image_tag` vars + `node_image` local.
- supervisor: build musl locally (Leo) → scp to Kamatera → run with `--app <uuid>` (or systemd later). Provide a `justfile` recipe `build-musl`. No CI required Phase-1.

---

## 10. Phase-1 E2E acceptance (the goal)

1. Leo compiles `tcli` locally (musl or native). 
2. `examples/hello-tabbify` builds to `app.wasm`.
3. `tcli push examples/hello-tabbify` → S3 `apps/<uuid>/v1/...` + `latest=1` → prints UUID.
4. Edit app, `tcli push` again → `v2`, `latest=2` (same UUID).
5. supervisor on Kamatera (auto-mesh, tag supervisor) serves the WASM (fetch from S3 by uuid).
6. `curl -H 'Authorization: Bearer <KEY>' http://3.124.69.92:8090/app/<uuid>` → `Hello, Tabbify!` (node discovers the supervisor via coordinator roster, proxies, caches; second curl is cache-fast).
7. Flip manifest `lifecycle` to `always_on`, re-push, observe always-running vs on_request lazy-spawn+idle-stop.
8. node endpoints: `GET /v1/supervisors`, `GET /v1/apps`, `POST /v1/apps/<uuid>/start|stop` all work; MCP `tools/list` + `tools/call get_app` works.

---

## Deferred (post-Phase-1, do NOT block on these)
- Per-app-ULA binding (use `derive_app_ula` to bind `[app_ula]:80` and let node route directly without asking supervisors).
- Stable peer-ULA from persistent keypair (currently idx-based, changes on rejoin).
- node reserved/stable ULA for Mac-over-mesh access (Phase-1 node is reached via public IP).
- S3 read hardening (presigned/IAM instead of public-read).
- auth-server integration (replace hardcoded node key).
- MCP Streamable-HTTP full compliance (SSE, sessions).

---

## Runner architecture — orchestrator + tabbify-runner

> Implemented and proven live in `feat/per-app-runner` (2026-05-26).
> This section supersedes the "supervisor hosts apps in-process" model from the
> original §5 control API description.

### Orchestrator / runner split

`supervisord` is now a **control-plane orchestrator only**. It does not run app
runtimes in-process. Instead it spawns one detached `tabbify-runner` process per app
and manages their lifecycle.

```
supervisord (orchestrator — control-plane only)
    │  HTTP control API  POST /v1/apps/:uuid/{start,stop,purge}
    │                    GET  /v1/apps[/:uuid]
    │
    ├── spawns detached ──▶  tabbify-runner --uuid <A> --parent <sup_ula> …
    │                             ├─ mesh peer  ULA = derive_app_ula(A)  kind=runner
    │                             ├─ AppRuntime (WASM / Firecracker / Docker)
    │                             └─ unix control socket  <runner_dir>/<A>.sock
    │
    ├── spawns detached ──▶  tabbify-runner --uuid <B> …
    └── monitor loop (5 s) — probe pid + socket, kill hung pids, respawn dead runners
```

**Control API → orchestrator behavior:**

| API call | Orchestrator action |
|---|---|
| `POST /v1/apps/:uuid/start` | Spawn runner (idempotent if alive), wait socket healthy (30 s), return `app_ula`. |
| `POST /v1/apps/:uuid/stop` | `Shutdown` runner via socket, forget on-disk record. Artifact cache kept on disk. |
| `POST /v1/apps/:uuid/purge` | `Purge` runner (clears cache + docker image), then `Shutdown`, forget record, reclaim cache. |
| `GET /v1/apps` | Enumerate on-disk records + socket health probe per runner. |
| `GET /v1/apps/:uuid` | Load record + probe socket → `{uuid, app_ula, state}`. |

### tabbify-runner

A second binary in `tabbify-service-supervisor` (built from `src/bin/runner.rs`,
shares the library). It hosts exactly one app:

- Claims `app_ula = derive_app_ula(uuid)` as its mesh ULA (`requested_ula` in the
  coordinator join, uniqueness-checked). Uses `identity_path` to persist keypair + ULA
  across restarts (sticky identity).
- Runs the app's `AppRuntime` (wasmtime WASM / Firecracker microVM / Docker container).
- Serves on `[app_ula]:8730` (mesh) or loopback (--no-mesh).
- Binds a unix domain control socket at `--control-sock` (`<runner_dir>/<uuid>.sock`
  when spawned by the orchestrator).
- Declares `kind = "runner"` and `parent = <supervisor_ula>` in the coordinator roster.

Key flags: `--uuid`, `--control-sock`, `--parent`, `--no-mesh`, `--s3-base-url`,
`--data-dir`, `--port`.

### Control protocol (`src/control_proto.rs`)

Unix socket, JSON-lines (one `Cmd` per connection, one `Reply`, then close).

```
Cmd:    Ping | Health | Stop | Purge | Shutdown | Deploy{reff, runtime?}
Reply:  Pong | Health{state,app_ula,app_uuid,pid} | Ok | Err{message}
```

Wire examples: `{"cmd":"ping"}` → `{"reply":"pong"}`;
`{"cmd":"health"}` → `{"reply":"health","state":"running","app_ula":"fd5a:…","app_uuid":"…","pid":N}`.

### Resilience (proven live)

When `supervisord` is killed (SIGKILL), detached runners keep running — their
workloads continue serving traffic. A restarted `supervisord` reads on-disk records,
probes each socket, and re-adopts every living runner (no respawn, no blip). Only dead
runners are respawned. See the Obsidian vault → "Knowledge Base/Deployment/09 - Per-app runner E2E runbook".

### Topology endpoint (`GET /v1/topology` on the node)

```json
{
  "supervisors": [
    {
      "ula":          "<supervisor_peer_ula>",
      "display_name": "sup-kamatera",
      "runners": [
        { "app_uuid": "<uuid>", "app_ula": "fd5a:1f02:…::1" }
      ]
    }
  ],
  "orphaned": []
}
```

Topology is metadata, not addressing: `app_ula = derive_app_ula(uuid)` is always
UUID-deterministic and host-independent. An app keeps its address when migrated or
respawned on a different supervisor. The supervisor → runner ownership in the roster
is for visibility only.

### Per-arch S3 binary paths

Both binaries are published per arch on every CI release:

```
s3://<RELEASE_S3_BUCKET>/supervisor/x86_64/supervisord
s3://<RELEASE_S3_BUCKET>/supervisor/x86_64/tabbify-runner
s3://<RELEASE_S3_BUCKET>/supervisor/aarch64/supervisord
s3://<RELEASE_S3_BUCKET>/supervisor/aarch64/tabbify-runner
s3://<RELEASE_S3_BUCKET>/supervisor/supervisord    # legacy alias = x86_64
```

Fetch pattern:
```bash
ARCH=$(uname -m)
BASE="https://<bucket>.s3.eu-central-1.amazonaws.com/supervisor/${ARCH}"
curl -fsSL "${BASE}/supervisord"    -o supervisord    && chmod +x supervisord
curl -fsSL "${BASE}/tabbify-runner" -o tabbify-runner && chmod +x tabbify-runner
sudo ./supervisord --runner-bin ./tabbify-runner
```

## Builder role (build/run split, 2026-06-04)

The supervisor is a UNIVERSAL node; its fleet role is expressed via mesh
capability tags (frozen strings, additive-only):

| Tag | Meaning | Source |
|---|---|---|
| `firecracker` | can RUN apps (`/dev/kvm` R/W) | auto-detected |
| `docker` | docker daemon reachable (can build images) | auto-detected |
| `builder` | operator-DESIGNATED build host | explicit: `SUPERVISOR_BUILDER=1` / `--builder` |

Node-side selection semantics:

- **Builder for `/v1/build`** — explicit hint wins (REST/MCP `builder` field,
  or `tabbify.toml [build].builder` = display name or ULA, no fallback);
  otherwise: `builder`-tagged peers (roster order) → `docker`-tagged peers →
  legacy first-in-roster (untagged dev fleets). The pipeline retries the next
  candidate on build failure (bounded: 3 attempts).
- **Default deploy fan-out** (no explicit `targets`) — only
  `firecracker`-tagged supervisors. A roster where capability tags exist but
  nobody can RUN is a loud error, not a silent half-failed fan-out. A fully
  untagged fleet keeps the historical fan-out-to-all.
- `/v1/build` itself is NOT gated by the tag (transition compat + explicit
  hints must work on any docker-capable host).

`tabbify.toml` addition:

```toml
[build]
kind = "docker"
builder = "buildbox"   # optional: pin the build host (name or ULA)
```
