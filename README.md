# tabbify-service-supervisor

Two-binary **orchestrator + per-app runner** for the Tabbify mesh (build target B of the
[App-Layer Phase-1 contract](../APP_LAYER_CONTRACT.md)).

`supervisord` is the **orchestrator** (control-plane only — it does not host apps
in-process). `tabbify-runner` is the **per-app binary** — one process per live app,
each a mesh peer in its own right. See the
[runner architecture](#runner-architecture-orchestrator--per-app-process) section
for the full picture.

---

## Runner architecture — orchestrator + per-app process

### Overview

```
supervisord (orchestrator)
    │  control API  POST /v1/apps/:uuid/{start,stop,purge}
    │               GET  /v1/apps[/:uuid]
    │
    ├── spawns (detached) ──▶  tabbify-runner --uuid <A> …
    │                              ├─ mesh peer  app_ula = derive_app_ula(A)
    │                              ├─ AppRuntime (WASM / Firecracker / Docker)
    │                              └─ unix control socket  runners/<A>.sock
    │
    ├── spawns (detached) ──▶  tabbify-runner --uuid <B> …
    │                              └─ …
    └── monitor loop — liveness = pid alive + control-socket health
```

**`supervisord`** is a pure orchestrator:

- Receives control commands via its HTTP API.
- `start` = spawn a detached `tabbify-runner` child, wait for its control socket to
  become healthy (30 s timeout), return the app ULA.
- `stop` = send `Shutdown` over the control socket, forget the on-disk record (the
  runner exits keeping its artifact cache on disk for a fast restart).
- `purge` = send `Purge` (clears cache + docker image) then `Shutdown`, forget record,
  reclaim cache on the orchestrator side too.
- `list` = enumerate on-disk runner records + probe each socket for live state.
- **Monitor loop** (every 5 s): probe pid alive AND control socket. Dead runner → kill
  any hung pid first (avoid orphan), then respawn. Living runner → leave untouched.
- **Re-adopt on restart**: on startup the orchestrator scans on-disk records and re-adopts
  every living runner (same pid, no respawn, no traffic blip). Only truly dead ones are
  respawned. This is what makes the crash-survival property work.

**`tabbify-runner`** hosts exactly one app:

- Claims `app_ula = derive_app_ula(uuid)` as its mesh peer ULA (or loopback in
  `--no-mesh`). The `--parent` flag carries the supervisor's ULA so the node can build
  the topology tree.
- Runs the app's `AppRuntime` in-process (WASM via wasmtime / Firecracker microVM /
  Docker container).
- Serves traffic on its ULA (port 8730 by default).
- Binds a unix domain control socket at `--control-sock` (default
  `/run/tabbify/runners/runner.sock`; the orchestrator uses `<runner_dir>/<uuid>.sock`).
- Persists a `<uuid>.json` runner record so `supervisord` can find it after a restart.

Key flags for `tabbify-runner`:

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--uuid` | `RUNNER_UUID` | (required) | App UUID to host |
| `--control-sock` | `RUNNER_CONTROL_SOCK` | `/run/tabbify/runners/runner.sock` | Unix socket path |
| `--parent` | `RUNNER_PARENT` | — | Supervisor's mesh ULA (for topology) |
| `--no-mesh` | `RUNNER_NO_MESH` | `false` | Skip TUN, bind loopback |
| `--s3-base-url` | `RUNNER_S3_BASE_URL` | prod S3 URL | Artifact fetch URL |
| `--data-dir` | `RUNNER_DATA_DIR` | `./data` | Local artifact cache |
| `--port` | `RUNNER_PORT` | `8730` | Listener port on the mesh ULA |

### Resilience (crash-survival)

When `supervisord` is killed (or crashes), its detached runners **keep running**.
The app workloads (Firecracker microVMs, Docker containers, WASM processes) continue
serving traffic. A restarted `supervisord` reads the on-disk records, contacts each
runner's control socket, and re-adopts every living runner — same pid, no respawn, no
traffic blip. Only dead runners are respawned.

This property is exercised by the E2E runbook
(`RUNNER_E2E_RUNBOOK.md` in the workspace root).

### Control protocol

Unix-domain socket, JSON-lines framing (one command per connection, one reply, then
close). Defined in `src/control_proto.rs`:

**Commands (`Cmd`):**

| Variant | Wire | Effect |
|---|---|---|
| `Ping` | `{"cmd":"ping"}` | Liveness probe |
| `Health` | `{"cmd":"health"}` | Returns lifecycle snapshot |
| `Stop` | `{"cmd":"stop"}` | Tear down listener + stop app |
| `Purge` | `{"cmd":"purge"}` | Stop + clear artifact cache + docker image |
| `Shutdown` | `{"cmd":"shutdown"}` | Stop + exit process |

**Replies (`Reply`):**

| Variant | Wire | When |
|---|---|---|
| `Pong` | `{"reply":"pong"}` | Response to `Ping` |
| `Health{state,app_ula,app_uuid,pid}` | `{"reply":"health","state":"running","app_ula":"fd5a:…","app_uuid":"…","pid":N}` | Response to `Health` |
| `Ok` | `{"reply":"ok"}` | Generic success |
| `Err{message}` | `{"reply":"err","message":"…"}` | Failure |

---

## Modules

| File | Responsibility |
|---|---|
| `src/config.rs` | `supervisord` clap+env config: bind addr, coordinator URL, data dir, S3 base URL, `--runner-bin`, `--no-mesh`. |
| `src/runner/config.rs` | `tabbify-runner` config (see flags table above). |
| `src/control_proto.rs` | `Cmd` / `Reply` shared between runner (server) and orchestrator client. |
| `src/orchestrator/mod.rs` | `Orchestrator` + `SharedRunnerConfig` — long-lived fleet owner; `readopt` + `run_monitor`. |
| `src/orchestrator/api.rs` | Orchestrator lifecycle ops: `start_app`, `stop_app`, `purge_app`, `app_summary`, `app_summaries`. |
| `src/orchestrator/spawn.rs` | `spawn_runner`: fork a detached runner process + persist its record. |
| `src/orchestrator/handle.rs` | `RunnerHandle` — on-disk JSON record (`uuid`, `pid`, `control_sock`, `app_ula`, `parent`). |
| `src/orchestrator/client.rs` | `ControlClient` — unix-socket control protocol client. |
| `src/orchestrator/monitor.rs` | Monitor/respawn loop (`reconcile_record`): probe liveness, kill hung pid, respawn dead runners. |
| `src/manifest.rs` | Vendored `manifest.toml` schema (contract §3) — byte-identical to the cli's copy. |
| `src/app_ula.rs` | Vendored `derive_app_ula` (contract §4) + golden test. |
| `src/runtime.rs` | `AppRuntime` trait + wasmtime-26 `wasi:http/proxy` runtime. |
| `src/firecracker.rs` | Firecracker microVM `AppRuntime` (KVM-gated, Linux only). |
| `src/docker.rs` | Docker container `AppRuntime`. |
| `src/fetcher.rs` | `S3Fetcher`: anonymous HTTPS GET `latest → v<N>/manifest.toml + entry`; disk cache. |
| `src/mesh.rs` | `MeshMembership`: wraps `Joiner::join`, surfaces `my_ula` + `peer_id`. |
| `src/api.rs` | axum router wired to the orchestrator (replaces the old in-process `AppRegistry`). |
| `src/runner/` | Runner binary modules: `control.rs` (socket server), `serve.rs` (app listener), `wire.rs` (config bridge). |
| `src/bin/runner.rs` | `tabbify-runner` entrypoint — parse config, start `RunnerServe`, bind control socket. |
| `src/main.rs` | `supervisord` entrypoint — parse config, join mesh, build orchestrator, re-adopt fleet, start monitor, serve. |

---

## HTTP API

The control API drives the orchestrator. All routes on `[my_ula]:8730` (or
`127.0.0.1:8730` in `--no-mesh`).

| Method + path | Behavior |
|---|---|
| `GET /health` | `{"status":"ok","supervisor_id":"…","ula":"…"}` |
| `GET /v1/apps` | `{"apps":[{uuid,app_ula,state}]}` — runner records + live health probe. |
| `GET /v1/apps/:uuid` | `{uuid,app_ula,state}` for one app; 404 if no record. |
| `POST /v1/apps/:uuid/start` | Spawn runner (idempotent if alive) → `{"state":"running","app_ula":"…"}` |
| `POST /v1/apps/:uuid/stop` | Shutdown runner, forget record (keeps artifacts) → `{"state":"stopped"}` |
| `POST /v1/apps/:uuid/purge` | Purge cache + image, shutdown runner, forget record → `{"state":"stopped"}` |

`state` in API responses: `"running"` = live runner answers control socket;
`"stopped"` = no live runner.

---

## Mesh (coordinator roster extension)

Coordinator roster peers now carry `kind` / `parent` / `app_uuid` metadata:

- `supervisord` joins with `kind = "supervisor"`.
- `tabbify-runner` joins with `kind = "runner"`, `parent = <supervisor_ula>`,
  `app_uuid = <uuid>`.
- The coordinator honors a peer's `requested_ula` (uniqueness-checked; same-peer
  re-claim allowed → sticky identity across runner restarts). Runners use
  `identity_path` to persist their keypair + ULA across process restarts.
- `JoinConfig` gains `requested_ula` / `metadata` / `identity_path`.

---

## Node topology endpoint

`GET /v1/topology` on the node returns the supervisor → runners tree derived from the
coordinator roster:

```json
{
  "supervisors": [
    {
      "ula":          "fd5a:1f00:1::1",
      "display_name": "sup-kamatera",
      "runners": [
        { "app_uuid": "0191e7c2-…", "app_ula": "fd5a:1f02:44a5:240b:121a::1" }
      ]
    }
  ],
  "orphaned": []
}
```

Because runners carry their `parent` in the roster, the topology survives a supervisor
restart: runners are visible as orphaned until the supervisor re-joins, then re-parented
automatically.

`app_ula` is always `derive_app_ula(uuid)` (deterministic, host-independent), so an app
keeps the same address when migrated or respawned on a different supervisor.

---

## App-ULA addressing

`app_ula = derive_app_ula(uuid)` is the runner's own mesh peer ULA — the address the
app serves on. It is:

- **deterministic** — derived from the app UUID via BLAKE3, supervisor-independent.
- **stable across restarts** — a runner always reclaims the same ULA (sticky identity
  via `identity_path`).
- **the topological identity** — the node routes directly to `[app_ula]:8730`; the
  supervisor → runner ownership is roster metadata only.

---

## Lifecycle semantics (Phase-1 pre-runner, now superseded for in-process hosting)

The old `AppRegistry` / in-process lifecycle (always_on / on_request / idle reaper)
has been **replaced** by the orchestrator model above. The lifecycle is now:

- **start** (API) = spawn a runner, wait healthy, runner self-manages its app.
- **stop** (API) = shutdown runner, forget record (artifacts kept).
- **monitor** = detect dead runner, respawn (no manual intervention needed).
- **adopt** = re-adopt living runners on supervisor restart.

---

## Mesh dependency — wired as a path dep (compiles ✅)

```toml
mesh-joiner = { path = "../tabbify-service-mesh/crates/mesh-joiner", package = "tabbify-mesh-joiner" }
```

The contract permits this as the local-build fallback (the mesh workspace uses
workspace-relative path deps that make a single-crate git dep fragile). The local
checkout is at `../tabbify-service-mesh` (verified). For CI, swap to:

```toml
mesh-joiner = { git = "ssh://git@github.com/tabbify-io/tabbify-service-mesh.git", package = "tabbify-mesh-joiner", rev = "5dccb67" }
```

---

## Build / run / test

```bash
just build                    # cargo build --workspace
just test                     # all tests, no network
just lint                     # cargo clippy --all-targets -- -D warnings
just fmt                      # cargo fmt --check

# Run supervisord joined to the mesh (needs root / NET_ADMIN + /dev/net/tun):
sudo -E just run

# Run locally WITHOUT the mesh (loopback, no TUN):
just run-local                # --no-mesh, binds 127.0.0.1:8730
```

Both binaries are built by `cargo build --workspace`:

```bash
cargo build --release --workspace
# -> target/release/supervisord
# -> target/release/tabbify-runner
```

### Cross-build (musl, for deployment)

```bash
just build-musl
# -> target/x86_64-unknown-linux-musl/release/supervisord
# -> target/x86_64-unknown-linux-musl/release/tabbify-runner
```

Uses `cargo-zigbuild` (zig as cross-linker) so no musl toolchain is needed on macOS.

### CI release — both binaries published to S3

`.github/workflows/release.yml` builds static musl binaries for **both CPU architectures**
and both binaries on every push to `main`, on tags `v*`, and on manual
`workflow_dispatch`.

**Published paths:**

```
s3://<RELEASE_S3_BUCKET>/supervisor/x86_64/supervisord         # Intel/AMD
s3://<RELEASE_S3_BUCKET>/supervisor/x86_64/tabbify-runner      # Intel/AMD
s3://<RELEASE_S3_BUCKET>/supervisor/aarch64/supervisord        # ARM
s3://<RELEASE_S3_BUCKET>/supervisor/aarch64/tabbify-runner     # ARM
s3://<RELEASE_S3_BUCKET>/supervisor/supervisord                # legacy alias = x86_64
```

**Fetch and run on a Linux host:**

```bash
ARCH=$(uname -m)   # x86_64 | aarch64
BASE="https://<RELEASE_S3_BUCKET>.s3.eu-central-1.amazonaws.com/supervisor/${ARCH}"
curl -fsSL "${BASE}/supervisord"      -o supervisord      && chmod +x supervisord
curl -fsSL "${BASE}/tabbify-runner"   -o tabbify-runner   && chmod +x tabbify-runner

# supervisord expects tabbify-runner on $PATH or via --runner-bin:
sudo ./supervisord --runner-bin ./tabbify-runner
```

**Required repo configuration** (Settings → Secrets and variables → Actions):

| Kind | Name | Value |
|---|---|---|
| Secret | `AWS_ROLE_ARN` | IAM role ARN; trust policy allows this repo; permission grants `s3:PutObject` on `<bucket>/supervisor/*`. |
| Variable | `AWS_REGION` | e.g. `eu-central-1` |
| Variable | `RELEASE_S3_BUCKET` | Bucket name (no `s3://` prefix) |

---

## Firecracker microVM runtime (KVM-gated, Linux)

Selected by `manifest.runtime.type = "firecracker"`. The runner spawns and proxies a
Firecracker microVM per app. Requires:

- Linux + `/dev/kvm` (bare-metal, nested-virt VPS, Lima/UTM Ubuntu);
- `firecracker` binary on `$PATH` (or `--firecracker-bin`);
- guest `vmlinux` kernel (default `/opt/tabbify/vmlinux`);
- `iproute2` + privilege to create tap devices.

Config flags: `--firecracker-bin`, `--firecracker-kernel`, `--firecracker-vcpus`,
`--firecracker-tap-subnet`, `--firecracker-app-port`.

The runner stub builds on macOS (no-op runtime, returns an error on actual invocation)
so the workspace compiles everywhere.

---

## Docker runtime

Selected by `manifest.runtime.type = "docker"`. The runner starts a Docker container
and proxies traffic to it. Requires Docker on the host.

---

## Tests

`cargo test --workspace` runs all tests with no network access (S3 mocked via wiremock;
WASM fixture committed at `tests/fixtures/hello.wasm`). Key coverage:

- **Orchestrator unit tests** — `spawn_spec_for`, `control_sock_for`, `app_ula_for`,
  state wire strings, re-adopt logic.
- **Control protocol** — `Cmd` / `Reply` serde round-trips + wire-form spot-checks.
- **Runtime** — wasmtime compiles + executes the fixture (full URI and path-only+Host).
- **Manifest** — canonical parse, defaults, unknown-field tolerance, stamped id.
- **App-ULA** — golden ULA (`fd5a:1f02:44a5:240b:121a::1`).
- **Integration** — end-to-end through the full router (mocked S3 + real wasmtime).
- **Node topology** — supervisor→runner tree building, orphan detection.

---

## Notes

- **Mesh dep is a path dep** (see above) — permitted fallback; swap to git + deploy key for CI.
- **`--no-mesh`** skips TUN + mesh join; binds loopback. Not in the contract; added so the
  service is runnable without root. Production joins the mesh.
- **`supervisord` no longer hosts apps in-process.** All app traffic flows through
  `tabbify-runner` processes. The old `AppRegistry` / `WasmRuntime` in-process path
  was replaced by the orchestrator model in `feat/per-app-runner`.
