# Per-App Runner Architecture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> Each task is TDD'd **against the real code at execution time**: read the named
> files first, then write the failing test, watch it fail, implement minimally,
> watch it pass, commit. Test sketches below give the required assertions; match
> the surrounding code's style/types. Keep every phase green + mergeable.

**Goal:** Split the monolithic `supervisord` into a thin control-plane supervisor + one detached `runner` process per app (each a mesh peer on its `app_ula`), so an app survives a supervisor crash and is reachable directly over the mesh.

**Architecture:** Per-app `runner` = mesh-peer(ULA=`derive_app_ula(uuid)`) + the app's `AppRuntime` (wasm in-proc / fc child / docker) + serves its `app_ula`. `supervisord` becomes an orchestrator: spawns runners detached, monitors over a local unix socket, restarts dead ones, re-adopts living ones on its own restart. Topology (supervisor→runners) is roster metadata, not address structure. node data path unchanged.

**Tech Stack:** Rust, tokio, axum, clap, dashmap, `tabbify-mesh-joiner`, wasmtime, firecracker, docker CLI; mesh coordinator (axum); cargo-zigbuild dual-arch CI. Spec: `docs/superpowers/specs/2026-05-26-per-app-runner-architecture-design.md`.

**Repos:** `tabbify-service-mesh` (Phase 0), `tabbify-service-supervisor` (Phase 1–2, runner+orchestrator share the lib), `tabbify-service-node` (Phase 3), all + infra (Phase 4).

---

## Phase 0 — Mesh: specific-ULA join + peer metadata

> Lets a runner join claiming `app_ula` (not an idx peer-ULA) and advertise
> `{kind, parent, app_uuid}` so node can build the topology. Backward-compatible:
> existing peers omit the new fields. Read `tabbify-service-mesh` coordinator
> `PeerInfo`/`RosterResponse` + the join handler + the joiner `JoinConfig`/`join`
> before starting.

### Task 0.1: Coordinator `PeerInfo` carries optional `kind`/`parent`/`app_uuid`
**Files:** Modify the coordinator crate's roster types (`PeerInfo`, the join-request DTO) + the roster handler; Test: the coordinator's existing roster test module.

- [ ] **Write failing test:** a peer registered with `kind="runner"`, `parent="<ula>"`, `app_uuid="<uuid>"` appears in `GET /v1/mesh/peers` with those fields; a peer registered WITHOUT them round-trips with `kind` defaulting (e.g. `"peer"`) and `parent`/`app_uuid` absent/null.
- [ ] **Run → fail** (fields don't exist).
- [ ] **Implement:** add `#[serde(default)] kind: String` (default `"peer"`), `#[serde(default)] parent: Option<String>`, `#[serde(default)] app_uuid: Option<String>` to `PeerInfo` + the join DTO; thread them through registration → roster. No `deny_unknown_fields`.
- [ ] **Run → pass.** Commit `feat: peer metadata (kind/parent/app_uuid) in coordinator roster`.

### Task 0.2: Coordinator accepts a peer's requested ULA
**Files:** Modify the join handler + the ULA-assignment logic; Test: coordinator join tests.

- [ ] **Write failing test:** a join request carrying `requested_ula = "fd5a:1f02:aaaa::1"` results in that peer's `ula` being exactly that value (not an idx-derived one); two different peers requesting the SAME ula → the second is rejected (409/error) to prevent split-brain.
- [ ] **Run → fail.**
- [ ] **Implement:** join DTO gains `#[serde(default)] requested_ula: Option<String>`; if present + well-formed + unclaimed, assign it verbatim; else fall back to the existing idx assignment. Track claimed ULAs for the uniqueness check.
- [ ] **Run → pass.** Commit `feat: honor requested_ula on mesh join (uniqueness-checked)`.

### Task 0.3: Joiner `JoinConfig` supports requested ULA + metadata
**Files:** Modify `tabbify-mesh-joiner` `JoinConfig` + `Joiner::join` (+ wherever the join request is built); Test: joiner unit/integration tests.

- [ ] **Write failing test:** `Joiner::join(JoinConfig { requested_ula: Some(app_ula), kind: "runner", parent: Some(p), app_uuid: Some(u), .. })` → `joiner.my_ula() == app_ula`, and the join request sent to the coordinator carries the metadata (assert against a wiremock/fake coordinator).
- [ ] **Run → fail.**
- [ ] **Implement:** add the fields to `JoinConfig` (all `Option`/defaulted, backward-compatible); include them in the outgoing join request; when `requested_ula` is set, use it as `my_ula`.
- [ ] **Run → pass.** Commit `feat: JoinConfig requested_ula + peer metadata`. Tag a new mesh rev for the git-dep bump in Phase 1.

### Task 0.4: Persistent peer identity (peers remember their ULA across restarts)
**Files:** Modify `tabbify-mesh-joiner` `JoinConfig`/`Joiner::join` + an identity-persistence helper (check how the keypair is currently persisted — Wave-2 "persistent keypair" may already exist; this adds ULA stickiness); Test: joiner tests.

- [ ] **Write failing test:** a joiner built with `identity_path = <tmp>/id.json` and NO prior state joins a fake coordinator → gets assigned ULA `X`, and `<tmp>/id.json` now holds `{keypair, ula: X}`. A SECOND joiner with the same `identity_path` (simulating a restart) joins → it sends `requested_ula = X` + reuses the persisted keypair → `my_ula() == X` again (stable across restart). (Runners don't need this — they pass `requested_ula = derive_app_ula(uuid)` explicitly, deterministic; this is for supervisors/long-lived peers.)
- [ ] **Run → fail.**
- [ ] **Implement:** `JoinConfig` gains `identity_path: Option<PathBuf>`. On `join`: if the file exists, load `{keypair, ula}` → reuse the keypair + set `requested_ula = ula`; else join fresh, then persist `{keypair, assigned_ula}` to the path (0600). Reuse the existing keypair-persistence if present; only add the ULA field + re-request.
- [ ] **Run → pass.** Commit `feat: persistent peer identity (sticky ULA via identity_path)`.

**Phase 0 acceptance:** mesh + coordinator tests green; a peer can join with a chosen `app_ula` + metadata; a peer with an `identity_path` keeps the same ULA across restarts; roster exposes metadata. Existing supervisor (old `host_app_ula` path) still works (additive). Merge + note the mesh rev. (Supervisor wires `identity_path` in Phase 2; runners pass `requested_ula = derive_app_ula(uuid)` in Phase 1.)

---

## Phase 1 — `runner` binary (per-app, reuses the lib)

> A 2nd binary in `tabbify-service-supervisor` sharing the `tabbify_supervisor`
> lib. ≈ today's supervisor scoped to ONE app + its own mesh join. Read
> `src/config.rs`, `src/registry.rs` (`ensure_running`/`host_app`/`build_runtime`),
> `src/host.rs`, `src/mesh.rs`, `src/main.rs` first.

### Task 1.1: `RunnerConfig` (clap)
**Files:** Create `src/runner/config.rs` (+ `src/runner/mod.rs`, `pub mod runner;` in `lib.rs`); Test: inline `#[cfg(test)]`.

- [ ] **Write failing test:**
```rust
let c = RunnerConfig::try_parse_from([
  "tabbify-runner","--uuid","0191e7c2-1111-7222-8333-444455556666",
  "--parent","fd5a:1f00:0:3::1","--control-sock","/run/tabbify/runners/x.sock","--no-mesh",
]).unwrap();
assert_eq!(c.uuid.to_string(), "0191e7c2-1111-7222-8333-444455556666");
assert_eq!(c.parent.as_deref(), Some("fd5a:1f00:0:3::1"));
assert!(c.no_mesh);
```
- [ ] **Run → fail.**
- [ ] **Implement:** `#[derive(Parser)] RunnerConfig { uuid: Uuid, parent: Option<String>, control_sock: PathBuf, no_mesh: bool, bind: Option<SocketAddr>, coordinator_url, s3_base_url, data_dir, #[command(flatten)] firecracker: FcConfig, #[command(flatten)] docker: DockerConfig, port }`. Reuse the `DEFAULT_*` consts from `config.rs`.
- [ ] **Run → pass.** Commit `feat(runner): RunnerConfig`.

### Task 1.2: Runner serves one app on its app-ULA (loopback path first)
**Files:** Create `src/runner/serve.rs`; Test: `tests/runner_integration.rs` (mirror `tests/integration.rs` harness — wiremock S3, loopback `AppHost`).

- [ ] **Write failing test:** start the runner core against a wiremock S3 serving the `hello.wasm` fixture (`ON_REQUEST`/`always_on`), in `--no-mesh` loopback; dial the bound addr → `"Hello, Tabbify!"` on `/` and a deep subpath. (Reuse `mock_s3`, `HELLO_WASM`.)
- [ ] **Run → fail.**
- [ ] **Implement:** `runner::serve::run(cfg)` = join mesh claiming `derive_app_ula(uuid)` (or `AppHost::loopback()` if `--no-mesh`) → `S3Fetcher::fetch` → `build_runtime` → `AppHost::host(app_ula, AppServe::new(rt, on_request))` → keep the `HostedApp` alive. Reuse `registry::build_runtime` logic (extract a free fn `build_runtime(uuid,&FetchedApp,&FcConfig,&DockerConfig)` from the registry method so both share it). Hold the membership for process lifetime.
- [ ] **Run → pass.** Commit `feat(runner): serve one app on its app-ULA`.

### Task 1.3: Runner mesh join claims `app_ula` + declares parent/app_uuid
**Files:** Modify `src/mesh.rs` (`MeshMembership::join` → accept a `JoinConfig` with requested_ula+metadata) + `src/runner/serve.rs`; Test: `tests/runner_integration.rs` (fake coordinator asserts the requested ULA + metadata).

- [ ] **Write failing test:** runner in mesh mode joins a fake coordinator with `requested_ula = derive_app_ula(uuid)`, `kind="runner"`, `parent`, `app_uuid` — assert the coordinator received them and `membership.my_ula() == derive_app_ula(uuid)`.
- [ ] **Run → fail.**
- [ ] **Implement:** thread the Phase-0 `JoinConfig` fields through `MeshMembership::join`; runner passes `requested_ula=derive_app_ula(uuid)`, `kind="runner"`, `parent=cfg.parent`, `app_uuid=uuid`.
- [ ] **Run → pass.** Commit `feat(runner): claim app-ULA + declare parent on mesh join`.

### Task 1.4: Control socket (handshake/health/stop/purge/shutdown)
**Files:** Create `src/runner/control.rs` (server) + `src/control_proto.rs` (shared message enum, used by supervisor in Phase 2); Test: inline + `tests/runner_integration.rs`.

- [ ] **Write failing test:** spawn the runner's control server on a temp unix socket; a client sends `Stop` → the runner reports `state=stopped` + the listener is torn down; `Purge` → artifacts cleared + the runner exits; `Health` → returns `{state, app_ula, app_uuid, pid}`.
- [ ] **Run → fail.**
- [ ] **Implement:** `control_proto.rs`: `enum Cmd { Ping, Health, Stop, Purge, Shutdown }`, `enum Reply { Pong, Health{state,app_ula,app_uuid,pid}, Ok, Err(String) }` (serde, JSON-lines framed). `runner::control::serve(sock, handle)` accepts connections, dispatches to the runtime/lifecycle (stop = unhost; purge = unhost + `fetcher.purge_cache` + docker `purge_image`; the firecracker reap `Drop` already handles the VM).
- [ ] **Run → pass.** Commit `feat(runner): local control socket + protocol`.

### Task 1.5: Reconcile/adopt workload on (re)start
**Files:** Modify `src/runner/serve.rs` + the runtimes (`docker.rs`/`firecracker.rs` adopt helpers); Test: `tests/runner_integration.rs` (docker-gated like the existing real-docker test).

- [ ] **Write failing test (docker-gated):** pre-create a container named `tbf-<uuid>-0` from the fixture image; start the runner for that uuid → it ADOPTS the existing container (does not build/run a duplicate) and serves; assert only one container exists.
- [ ] **Run → fail (or skip if no docker — then verify the wasm no-op adopt path instead).**
- [ ] **Implement:** before `build_runtime`, probe for an existing workload by deterministic name (docker `ps -aq --filter name=tbf-<uuid>`; fc pidfile) → if alive, wrap it in the runtime handle (adopt); else create. wasm: no-op (always fresh, in-proc).
- [ ] **Run → pass.** Commit `feat(runner): adopt existing workload on restart (no duplicates)`.

### Task 1.6: `runner` binary entrypoint
**Files:** Create `src/bin/runner.rs`; Modify `Cargo.toml` (`[[bin]] name="tabbify-runner"`); Test: smoke (`--help`).

- [ ] **Write failing test:** `assert_cmd`/process: `tabbify-runner --help` exits 0 and mentions `--uuid`/`--control-sock`. (Or a `Config::command().debug_assert()` unit test.)
- [ ] **Run → fail.**
- [ ] **Implement:** `main` = init tracing → `RunnerConfig::from_env` → spawn control server + `runner::serve::run` → run until shutdown.
- [ ] **Run → pass.** Commit `feat(runner): binary entrypoint`.

**Phase 1 acceptance:** `tabbify-runner` serves the wasm fixture standalone (loopback), claims its app-ULA in mesh mode, obeys the control socket, adopts existing workloads. Lib `build_runtime` shared with the registry. Existing `supervisord` untouched + green. Merge.

---

## Phase 2 — `supervisord` → orchestrator

> Replace in-process hosting with spawn-detached-runner + control + monitor +
> re-adopt. Read `src/registry.rs`, `src/api.rs`, `src/host.rs`, `src/main.rs`.
> The runtime code stays in the lib (now used by the runner). The registry's
> `AppRecord.hosted: Option<HostedApp>` becomes `Option<RunnerHandle>`.

### Task 2.1: `RunnerHandle` + runner-table type
**Files:** Create `src/orchestrator/handle.rs`; Test: inline.

- [ ] **Write failing test:** a `RunnerHandle { uuid, pid, control_sock, app_ula, parent }` serializes to/from the on-disk record JSON round-trip; `record_path(dir, uuid)` = `<dir>/<uuid>.json`.
- [ ] **Run → fail.**
- [ ] **Implement:** the struct + (de)serialize + path helper.
- [ ] **Run → pass.** Commit `feat(orchestrator): RunnerHandle + on-disk record`.

### Task 2.2: Spawn a runner detached + persist its record
**Files:** Create `src/orchestrator/spawn.rs`; Test: `tests/orchestrator_integration.rs` (spawns the real `tabbify-runner` bin built in Phase 1, `--no-mesh`).

- [ ] **Write failing test:** `spawn_runner(uuid, cfg, runner_dir)` launches `tabbify-runner` detached, writes `<runner_dir>/<uuid>.json`, and the runner becomes reachable on its control socket (handshake `Health` returns the uuid). Killing the SPAWNER's process handle does NOT kill the runner (detached) — assert the runner still answers its socket.
- [ ] **Run → fail.**
- [ ] **Implement:** build the `tabbify-runner` argv from cfg; `Command::new(runner_bin)...` with `setsid`/`pre_exec` to detach (own session); do NOT hold the child handle as owner-of-life (or set so it isn't killed on drop); write the record. Locate the runner bin next to the supervisord bin.
- [ ] **Run → pass.** Commit `feat(orchestrator): spawn runner detached + record`.

### Task 2.3: Control client (supervisor → runner)
**Files:** Create `src/orchestrator/client.rs` (uses `control_proto.rs`); Test: `tests/orchestrator_integration.rs`.

- [ ] **Write failing test:** against a spawned runner, the client `health()` returns the runner's state; `stop()` stops it; `purge()` purges + the runner exits.
- [ ] **Run → fail.**
- [ ] **Implement:** connect the unix socket, send `Cmd`, read `Reply`. Timeouts + clear errors.
- [ ] **Run → pass.** Commit `feat(orchestrator): runner control client`.

### Task 2.4: Monitor loop + restart dead runners
**Files:** Create `src/orchestrator/monitor.rs`; Test: `tests/orchestrator_integration.rs`.

- [ ] **Write failing test:** spawn a runner, kill it (SIGKILL its pid), run one monitor tick → the orchestrator detects it dead (socket gone / pid gone) and respawns it (new pid, same uuid + app_ula, reachable again).
- [ ] **Run → fail.**
- [ ] **Implement:** periodic tick: for each record, liveness-check (pid alive + socket health); if dead, respawn via Task 2.2 + update the record. (Mirror the existing reaper-loop shape in `main.rs`.)
- [ ] **Run → pass.** Commit `feat(orchestrator): monitor + restart dead runners`.

### Task 2.5: Re-adopt living runners on supervisor restart
**Files:** Modify `src/orchestrator/mod.rs` (startup); Test: `tests/orchestrator_integration.rs`.

- [ ] **Write failing test:** spawn a runner via orchestrator A; construct a FRESH orchestrator B over the same `runner_dir` (simulating a supervisor restart) → B discovers the living runner (record + health), adopts it into its table WITHOUT respawning (same pid), and a dead record is cleaned/respawned.
- [ ] **Run → fail.**
- [ ] **Implement:** on startup scan `runner_dir/*.json` → for each, health-check the socket: alive → adopt (record into table); dead → respawn (or drop the stale record).
- [ ] **Run → pass.** Commit `feat(orchestrator): re-adopt living runners on restart`.

### Task 2.6: Rewire the control API to the orchestrator
**Files:** Modify `src/api.rs` + `src/registry.rs` (→ becomes/wraps the orchestrator) + `src/main.rs`; Test: `tests/integration.rs` (existing start/stop/purge/list tests now drive the orchestrator).

- [ ] **Write failing test:** adapt the existing `start_hosts_*`, `stop_*`, `purge_*`, `list_apps_*` tests so `POST /start` spawns a runner (table shows it + the runner serves), `POST /stop` stops it, `POST /purge` purges + forgets, `GET /v1/apps` lists from the table. Keep the assertions (state strings, bound/app_ula).
- [ ] **Run → fail.**
- [ ] **Implement:** registry methods now delegate to the orchestrator (spawn/stop/purge/list) instead of in-process `host_app`. `AppSummary.bound_addr` becomes the runner's reported addr. Remove the in-process `HostedApp` hosting from the supervisor path (it lives in the runner now). `main.rs` builds the orchestrator + runs the monitor loop + re-adopt on startup.
- [ ] **Run → pass.** Commit `refactor(supervisor): control API drives the runner orchestrator`.

**Phase 2 acceptance:** `supervisord` spawns/monitors/re-adopts runners; existing API tests green against the new path; supervisor no longer hosts apps in-process. Merge.

---

## Phase 3 — node topology

> Additive; data path unchanged. Read `tabbify-service-node` `src/directory.rs`,
> `src/proxy.rs`, `src/openapi.rs`, `src/http/rest.rs`.

### Task 3.1: Directory parses the new `PeerInfo` fields
**Files:** Modify `src/directory.rs`; Test: inline.

- [ ] **Write failing test:** a roster JSON with a `supervisor`-tagged peer + two `runner`-kind peers (each with `parent` = the supervisor's ula + `app_uuid`) decodes into peers exposing `kind`/`parent`/`app_uuid`.
- [ ] **Run → fail.**
- [ ] **Implement:** add the fields to the roster DTO (`#[serde(default)]`).
- [ ] **Run → pass.** Commit `feat(node): parse kind/parent/app_uuid from roster`.

### Task 3.2: Build the supervisor→runners tree
**Files:** Create `src/topology.rs`; Test: inline.

- [ ] **Write failing test:** given peers [supervisor S, runner A(parent=S), runner B(parent=S), runner C(parent=Sdead-absent)], `build_topology(peers)` → S with children [A,B]; C appears under an `orphaned` group (parent not in the roster) — proving topology survives a missing supervisor.
- [ ] **Run → fail.**
- [ ] **Implement:** group runner-peers by `parent`; attach to the matching supervisor-peer; runners whose parent isn't present → `orphaned`.
- [ ] **Run → pass.** Commit `feat(node): build supervisor→runners topology`.

### Task 3.3: `GET /v1/topology`
**Files:** Modify `src/http/rest.rs` + `src/openapi.rs` + the router; Test: `tests/` (oneshot against a fake Directory).

- [ ] **Write failing test:** `GET /v1/topology` (authed) → JSON `{ supervisors: [{ula, runners:[{app_uuid,app_ula,state}]}], orphaned:[...] }` reflecting the fake roster.
- [ ] **Run → fail.**
- [ ] **Implement:** handler calls the Directory → `build_topology` → JSON; register in OpenAPI.
- [ ] **Run → pass.** Commit `feat(node): GET /v1/topology`.

**Phase 3 acceptance:** node exposes the tree; data path (proxy to `app_ula`) unchanged + still green. Merge.

---

## Phase 4 — CI, E2E, crash-survival

### Task 4.1: Dual-arch CI builds the `tabbify-runner` bin
**Files:** Modify `tabbify-service-supervisor/.github/workflows/release.yml`; Test: CI run.

- [ ] **Implement:** add `--bin tabbify-runner` to the zigbuild step (or a 2nd build) for both targets; upload `supervisor/<arch>/tabbify-runner`. Mirror the `supervisord` upload + legacy alias scheme. (No path-filter regressions.)
- [ ] **Verify:** push → both arches build both bins → anonymous HTTP 200 on the new keys. Commit `ci: build + publish tabbify-runner for both arches`.

### Task 4.2: Lima E2E — orchestrated firecracker app
**Files:** `PHASE1_E2E_RUNBOOK.md` (or a new runbook) + the Lima `kvmcheck` VM.

- [ ] **Implement + verify (live):** copy `supervisord` + `tabbify-runner` (aarch64) into Lima; run `supervisord` (orchestrator) `--app <fc-uuid>` → it spawns a `tabbify-runner` → runner boots the firecracker microVM + serves on its app-ULA (or loopback in `--no-mesh`); `curl` returns the hello. Document the commands.

### Task 4.3: Crash-survival test (the headline property)
**Files:** A Lima script `fc-crash-survival.sh`.

- [ ] **Implement + verify (live):** start orchestrator + runner serving the app; `curl` OK; **`kill -9` the supervisord** (orchestrator); `curl` the app again → **still OK** (runner + workload survive); restart `supervisord` → it re-adopts the running runner (table shows it, no respawn, same pid). This is the proof that A works.

### Task 4.4: Real docker + wasm through the new path
**Files:** the Lima/Mac harness.

- [ ] **Verify (live):** docker app via orchestrator→runner (build/run/serve/purge; image removed on purge); wasm app via orchestrator→runner (in-proc serve). Both green.

### Task 4.5: Docs + memory
**Files:** `APP_LAYER_CONTRACT.md`, READMEs, runbooks; memory.

- [ ] **Implement:** update the contract (runner component, control protocol, topology endpoint, ports), READMEs (two bins), runbooks (orchestrated run + crash-survival). Update memory with the shipped architecture.

**Phase 4 acceptance:** CI publishes both bins for both arches; Lima shows orchestrated fc/docker/wasm + **app survives a supervisord kill** + re-adopt on restart; docs current.

---

## Parallelization notes
- Phase 0 (mesh) is the only hard prerequisite for Phase 1's mesh-mode tasks (1.3+). Phase 1 loopback tasks (1.1, 1.2, 1.4, 1.5, 1.6) can start in parallel with Phase 0.
- Phase 3 (node) depends only on Phase 0's roster fields → can run in parallel with Phases 1–2.
- Phase 2 depends on Phase 1 (needs the runner bin). Phase 4 depends on 1–3.
- Within a phase, tasks are mostly sequential (later tasks build on earlier types).

## Risks / watch-items
- **Detached spawn**: the runner must NOT die with the supervisor — verify `setsid`/session-leader; the orchestrator must not hold the child as kill-on-drop. (Task 2.2 test asserts this.)
- **Specific-ULA uniqueness**: two runners for one uuid = split-brain; coordinator rejects the 2nd (Task 0.2). Single-host duplicate is also guarded by the runner-dir record + adopt (Task 1.5/2.5).
- **N mesh peers/host**: acceptable now; if coordinator load shows up, revisit a shared host transport (out of scope).
- **Runner mesh keys/auth**: Phase 0/1 keep it as-permissive-as-today (RnD); per-runner scoped tokens are a follow-up tied to the mesh auth plan.
- **musl**: `tabbify-runner` is a 3rd artifact; the firecracker `cfg(linux)` + docker code compile for it identically to `supervisord` (same lib).
