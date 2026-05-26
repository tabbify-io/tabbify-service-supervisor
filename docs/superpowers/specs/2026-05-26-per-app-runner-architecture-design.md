# Per-App Runner Architecture — Design

> Decomposes the monolithic supervisor (one process holding the mesh transport +
> all app-ULAs + all runtimes + the proxy) into a **thin control-plane
> supervisor** + **one detached `runner` process per app**, so an app survives a
> supervisor crash and is reachable directly over the mesh. Approved via
> brainstorming 2026-05-26.

## 1. Motivation

Today `supervisord` is a single process that:
- holds the mesh WG transport (boringtun + a TUN device bound to the process fd),
- claims every hosted app's `app_ula` `/128` on that TUN,
- runs every runtime (WASM in-process, Firecracker child VM, Docker container),
- proxies inbound `[app_ula]:8730` traffic to each runtime.

**Failure mode** (the thing we're fixing): if `supervisord` crashes, its TUN
disappears with the process → the **whole host's mesh connectivity dies**, every
`app_ula` stops being advertised, and the proxy is gone. Firecracker/Docker
workloads keep running (separate process / daemon) but are **orphaned and
unreachable**; WASM dies outright. One process is a single point of failure for
**all** apps on the host, and every app's availability is coupled to it.

### Goal (this spec — "A")
Each app is an **independent unit** that:
- survives a `supervisord` **process** crash, and
- stays reachable directly over the mesh (supervisor out of the data path).

### Non-goal (deferred — "B", but designed-for)
Cross-host **migration/failover** (a *whole node* dies → another supervisor pulls
the app's artifact by uuid and re-hosts it under the same `app_ula`). Out of
scope here, but every decision below keeps it cheap to add later.

## 2. Decisions (from brainstorming)

- **Per-app `runner`**: a *detached* Rust process per app. It is a mesh peer
  whose ULA = `derive_app_ula(uuid)`, it runs the app's runtime, and it serves on
  its own `app_ula`. It outlives the supervisor.
- **`supervisord` → control-plane** only: spawns runners detached, monitors them
  over a local socket, restarts dead ones, and **re-adopts** living runners on
  its own restart. Never in the data path.
- **WASM is unified**: because a runner is itself a Rust process, it runs WASM
  *in-process* (as the supervisor does today), Firecracker as its child VM, and
  Docker via the daemon — all behind one `AppRuntime`. (In the rejected
  "joiner-in-container" / "host-CNI" shapes, WASM had no netns/process to own a
  mesh identity; here it fits cleanly.)
- **Transport = per-runner mesh peer.** Accepted cost: N WG peers + N mesh joins
  per host (the mesh was designed for "peers with ULAs"; our scale is units–dozens
  of apps/host). Sharing a host transport is a future optimization (YAGNI now).
- **Lifecycle = self-managed** (detached + re-adopt), NOT systemd-delegated —
  keeps everything in our Rust stack and portable (Lima / Kamatera / containers).
- **Topology = roster metadata, NOT address structure.** `app_ula` MUST stay
  globally `derive_app_ula(uuid)` — supervisor-independent — or migration (B)
  would change an app's address and break node. So the "supervisor owns these
  runners" relationship is expressed as **per-peer metadata** the runner
  declares, never as a per-supervisor ULA subnet.
- **node data path UNCHANGED**: node still dials `derive_app_ula(uuid)`; now a
  runner answers instead of the supervisor. (This is the payoff of the
  deterministic app-ULA design.)
- **Persistent peer identity (peers remember their IP).** A peer keeps the same
  mesh ULA across restarts. **Runners get this for free** — `app_ula =
  derive_app_ula(uuid)` is deterministic, recomputed from the uuid (no persist).
  **Supervisors persist** their (keypair + assigned ULA) to disk and **re-claim
  the same ULA on rejoin** via `requested_ula` (Phase 0), instead of churning
  idx-assigned ULAs. Same mechanism, both stable.

## 3. Architecture

```
NOW:   node →[mesh]→ app_ula → ┌ supervisord (ONE process) ───────────┐
                               │ TUN(all app_ula)+proxy+wasm/fc/docker │→ workloads
                               └───────────────────────────────────────┘
                               crash → EVERYTHING on the host unreachable

A:     ┌ supervisord = thin control-plane ┐ spawn/monitor/restart (detached)
       └──────────────┬───────────────────┘ ↕ local unix socket
       node →[mesh]→ app_ula(uuid) → ┌ runner(uuid)  detached PID ───┐
                                     │ mesh-peer(ULA=app_ula)         │→ own fc-VM / docker
                                     │ + runtime (wasm = in-process)  │
                                     └────────────────────────────────┘
                                     supervisor crash → runners LIVE, app reachable
```

## 4. Components

### 4.1 `runner` (new binary, same repo — reuses the lib)
A second binary in `tabbify-service-supervisor` (shares the `tabbify_supervisor`
lib for max reuse). Per-app, scoped to ONE uuid.
- **Input**: `--uuid`, S3 base, coordinator URL, runtime configs (fc/docker),
  `--parent <supervisor peer id/ULA>`, `--control-sock <path>`.
- **Mesh**: joins claiming `ULA = derive_app_ula(uuid)` (NOT an idx-assigned
  peer-ULA); declares metadata `{kind: runner, parent, app_uuid}`.
- **Serve**: fetch from S3 (`S3Fetcher`), build the runtime (`runtime.rs` /
  `firecracker.rs` / `docker.rs`), bind `[app_ula]:8730` (loopback in `--no-mesh`
  for tests), serve the whole path (`host.rs::serve_app`). All reused.
- **Control socket**: a local unix socket the supervisor drives — `health`,
  `stop`, `purge`, `shutdown`; runner→supervisor `handshake {pid, app_ula,
  state}`.
- **Owns its workload lifecycle**: reconcile/adopt its container/VM on (re)start;
  the firecracker kill+reap `Drop` and the docker purge live here.

### 4.2 `supervisord` (refactored → control-plane orchestrator)
- Drops in-process app hosting. May still join the mesh as a `supervisor`-tagged
  peer (control/observability + so node's directory sees it), but it no longer
  claims app-ULAs or proxies.
- `registry` → **runner table**: `{uuid, pid, control_sock, app_ula, state,
  parent, last_health}`.
- **Spawn**: launch the runner *detached* (`setsid`/own session — NOT a child
  that dies with the parent); persist a record at
  `/var/lib/tabbify/runners/<uuid>.json` (pid + socket + app_ula).
- **Monitor/restart**: poll health over the socket / liveness; restart a dead
  runner (which re-adopts or recreates its workload).
- **Re-adopt on restart**: scan the runner dir, reconnect to living runners,
  health-check, restart any that died while the supervisor was down.
- **Control API** (existing `/v1/apps/...`): `start` → spawn runner; `stop` →
  command the runner to stop; `purge` → runner purges its artifacts + forget;
  `list`/`get` → from the runner table.

### 4.3 `tabbify-service-mesh`
- **joiner**: support joining with a **specific ULA** (`app_ula`) instead of the
  idx-derived peer-ULA; attach per-peer **metadata** (`kind`, `parent`,
  `app_uuid`). Backward-compatible (existing peers omit them).
- **coordinator**: `PeerInfo` += `kind`, `parent`, `app_uuid`; roster
  (`GET /v1/mesh/peers`) exposes them.

### 4.4 `tabbify-service-node`
- `directory.rs`: parse the new `PeerInfo` fields; build the **supervisor →
  runners** tree.
- New `GET /v1/topology` (and/or enrich `/v1/supervisors`):
  ```
  supervisor S (1f00::3)
    ├─ runner app=uuid-a (1f02:aaaa::1) running
    └─ runner app=uuid-b (1f02:bbbb::1) running
  ```
- **Data path unchanged** (dials `app_ula`). Because runners declare their own
  parent, when a supervisor's peer drops from the roster its runners remain and
  still show `parent = S` → node sees "orphaned runners of dead S" (the future-B
  signal), and topology survives the supervisor crash.

## 5. Supervisor ↔ Runner control protocol
- One local unix socket per runner (`/run/tabbify/runners/<uuid>.sock`).
- Small framed/JSON-lines messages:
  - runner→supervisor: `Handshake { pid, app_ula, app_uuid, state }`, `Health
    { state, last_activity }`.
  - supervisor→runner: `Stop`, `Purge`, `Shutdown`, `Ping`.
- The runner is the source of truth for its own state; the supervisor
  queries/commands. Loss of the socket ≠ runner dead (re-adopt re-handshakes).

## 6. Lifecycle & crash semantics
- **Normal**: supervisor spawns runner (detached) → runner joins mesh + serves →
  supervisor monitors via socket.
- **Supervisor crash**: runners keep running (detached, own mesh peer + workload)
  → apps stay reachable. On supervisor restart → scan dir, reconnect, health,
  restart any dead. **This is the headline property.**
- **Runner crash**: supervisor (if up) detects → restarts it → it re-adopts or
  recreates its workload. If the supervisor is also down, the app is down until
  the supervisor returns (acceptable for A; whole-host loss is B's problem).
- **Reconcile** (solves today's orphan problem, now per-runner): on (re)start a
  runner adopts its existing workload by deterministic name (Docker
  `tbf-<uuid>`, Firecracker via a pidfile) or recreates it — no duplicates.

## 7. Runtime unification (all behind `AppRuntime`, one per runner)
- **wasm**: in-process in the runner (`WasmRuntime`).
- **firecracker**: runner spawns the child microVM (`firecracker.rs`); the
  kill+reap `Drop` moves into the runner.
- **docker**: runner builds/runs via the daemon (`docker.rs`); `purge` removes
  the image. (The existing real-docker + Lima-firecracker E2E must pass through
  the new path.)

## 8. Migration path from current code (incremental, ALWAYS green)
- **Phase 0 — mesh**: joiner can claim a specific ULA + carry metadata;
  coordinator `PeerInfo` fields. Backward-compat; current supervisor untouched.
- **Phase 1 — `runner` binary**: build it in the supervisor repo, reusing
  `fetcher`/`runtime`/`firecracker`/`docker`/`host`/`mesh`, scoped to one app +
  its own mesh join claiming `app_ula` + parent metadata. Test standalone
  (≈ today's supervisor for one app, `--no-mesh` loopback like the current
  integration tests). Existing `supervisord` still works.
- **Phase 2 — `supervisord` → orchestrator**: replace in-process hosting with
  spawn-detached-runner + control socket + monitor/restart + re-adopt. Migrate
  the `registry`/`api`. The runtime code stays in the lib, now *used by the
  runner*.
- **Phase 3 — node topology**: additive (`directory.rs` + `/v1/topology`).
- **Phase 4 — E2E**: Lima firecracker + real docker + wasm through the new
  architecture; **add the supervisor-crash-survival test** (kill supervisord,
  assert the runner still serves); update runbooks.
- Each phase: TDD, green, independently mergeable.

## 9. Testing
- Reuse the harness (wiremock S3, loopback hosting).
- **Runner**: standalone start/serve/stop/purge/reconcile (loopback, no mesh).
- **Supervisor**: spawn/monitor/re-adopt/restart (against a real runner);
  the **crash-survival** test (kill the supervisor, runner keeps serving) — run
  live in Lima.
- Keep ALL existing tests green (runtimes, fetcher, manifest, app_ula, purge,
  reap).

## 10. Security
- Each runner needs a WG keypair + coordinator authorization for its `app_ula`.
  The supervisor (already a trusted mesh member) delegates a scoped join token,
  or the coordinator issues per-runner tokens bound to `app_ula` + `parent`.
  Ties into the mesh auth plan; RnD phase keeps it simple, hardens later.

## 11. Open questions to resolve during planning
- Exact control-protocol framing (JSON-lines vs length-prefixed).
- Runner-dir record format + locking (concurrent supervisor + runner writes).
- Does the supervisor itself still join the mesh (for the `supervisor` roster
  node), or does node infer supervisors purely from runners' `parent`? (Leaning:
  supervisor joins as a lightweight peer so it's visible even with zero runners.)
- aarch64/musl: the runner is a 3rd musl artifact in the dual-arch CI.
