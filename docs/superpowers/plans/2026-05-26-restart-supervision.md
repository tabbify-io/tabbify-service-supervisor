# Restart Supervision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Fresh subagent per task, TDD, two-stage review. Steps use `- [ ]`.

**Goal:** self-healing at every layer — supervisor restarts on crash (backend), and it restarts its apps with exponential backoff + crash-loop handling + a reset escape hatch.

**Architecture:** one restart brain at L2 (the supervisor monitor). L3 = runner fail-fast (exit on app death → L2 respawns). L1 = backend `--restart`/systemd (launcher only). Per the spec `docs/superpowers/specs/2026-05-26-restart-supervision-design.md`.

**Tech Stack:** Rust (supervisor + tcli + node), TDD with injected clocks (no sleeps in unit tests).

**Defaults:** `BASE=10s`, `CAP=300s`, `STABLE=60s`, `CRASHLOOP_THRESHOLD=5`.

---

## Phase 1 — RestartPolicy (pure, supervisor repo)

### Task 1.1: Backoff + crash-loop state machine
**Files:** Create `tabbify-service-supervisor/src/orchestrator/restart.rs`; declare `pub mod restart;` in `src/orchestrator/mod.rs`.

Pure, clock-injected. No I/O, no async.

```rust
/// Tunable params (defaults from the spec; overridable via Config later).
#[derive(Debug, Clone, Copy)]
pub struct BackoffParams { pub base_secs: u64, pub cap_secs: u64, pub stable_secs: u64, pub crashloop_threshold: u32 }
impl Default for BackoffParams { fn default() -> Self { Self { base_secs: 10, cap_secs: 300, stable_secs: 60, crashloop_threshold: 5 } } }

/// Persisted per-runner restart state (serde — lands in RunnerHandle in Phase 2).
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RestartState { pub consecutive_failures: u32, pub last_exit_at: u64, pub next_retry_at: u64, pub last_healthy_at: u64 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartStatus { Running, Backoff, CrashLoop }
```

Functions (all `(state, params, now_secs) -> ...`, pure):
- `backoff_delay(failures, p) -> u64` = `min(p.base * 2^(failures-1), p.cap)` for `failures>=1`, `0` for `0`. Guard the shift (saturating).
- `on_exit(state, p, now) -> RestartState`: `consecutive_failures+1`, `last_exit_at=now`, `next_retry_at = now + backoff_delay(failures+1, p)`.
- `on_healthy(state, p, now) -> RestartState`: if `now - last_exit_at >= p.stable_secs` (or `last_exit_at==0`) → reset to `RestartState{ last_healthy_at: now, ..default }`; else just set `last_healthy_at=now`.
- `should_respawn(state, now) -> bool` = `now >= state.next_retry_at`.
- `status(state, p, now) -> RestartStatus`: `consecutive_failures==0` → `Running`; `>= p.crashloop_threshold` → `CrashLoop`; else `Backoff`.
- `reset(state) -> RestartState` = `RestartState::default()`.

- [ ] **Step 1: failing tests** in `restart.rs` `#[cfg(test)]`:
  - `backoff_delay`: f=0→0, f=1→10, f=2→20, f=3→40, f=5→160, f=10→300 (capped), no overflow at f=64.
  - `on_exit` from default at now=1000 → failures=1, next_retry_at=1010.
  - two `on_exit` → failures=2, next_retry_at = last_exit + 20.
  - `on_healthy` after STABLE since last_exit → resets failures to 0; before STABLE → keeps failures, updates last_healthy_at.
  - `should_respawn`: now<next_retry_at → false; now>=next_retry_at → true.
  - `status`: 0→Running, 1..4→Backoff, 5+→CrashLoop.
- [ ] **Step 2:** run, watch fail (module/methods missing).
- [ ] **Step 3:** implement the functions minimally.
- [ ] **Step 4:** `cargo test --lib orchestrator::restart`, then `cargo clippy --all-targets -- -D warnings`, `cargo fmt`.
- [ ] **Step 5:** commit `feat: restart backoff + crash-loop policy (pure)`.

---

## Phase 2 — persist + wire into the monitor (supervisor repo)

### Task 2.1: Add RestartState to the runner record
**Files:** Modify `src/orchestrator/handle.rs` (the `RunnerHandle` struct + its serde save/load); its tests.

- Add `#[serde(default)] pub restart: RestartState` to `RunnerHandle` (so old records without the field still load).
- [ ] Test: a `RunnerHandle` with a non-default `restart` round-trips through `save`→`load`; an old JSON without `restart` loads with `RestartState::default()`. Implement (add field, `#[serde(default)]`). Gate (test/clippy/fmt). Commit `feat: persist restart state in runner record`.

### Task 2.2: Backoff-gate the monitor respawn
**Files:** Modify `src/orchestrator/monitor.rs` (`reconcile_record` / `do_respawn`); add tests.

- On `PidDecision::RespawnDead` (and the post-kill respawn arm): consult `restart::should_respawn(record.restart, now)`.
  - If respawn is due → `do_respawn`, then `record.restart = restart::on_exit(record.restart, params, now)`, persist the updated record (`RunnerHandle::save`).
  - If NOT due (`now < next_retry_at`) → skip respawn this tick; return a new outcome `RecordOutcome::Backoff` (the runner stays dead, waiting). Log at debug.
- On a healthy adopt (`AdoptInGrace` past STABLE, or `CheckSocket`+socket_ok) → `record.restart = restart::on_healthy(...)`, persist if changed.
- Inject `now`/params so the reconcile remains unit-testable (extend the existing pure-decision pattern: add a small pure helper `decide_respawn(restart_state, now) -> RespawnNow|WaitBackoff` tested directly).
- [ ] Tests: a record whose `next_retry_at` is in the future → `reconcile_record` does NOT respawn (Backoff); a record past `next_retry_at` with dead pid → respawns + bumps `consecutive_failures` + advances `next_retry_at`. Use the injected clock; do not spawn real processes (assert via the pure `decide_respawn` + a fake `do_respawn` seam where practical).
- [ ] Gate; commit `feat: gate runner respawn behind backoff policy`.

---

## Phase 3 — status surfaced + reset endpoint (supervisor repo)

### Task 3.1: Expose restart status in the control API
**Files:** Modify `src/orchestrator/api.rs` (the `list`/`get` app views) + `src/api.rs` (`/v1/apps`, `/health` JSON) — match the existing shapes.

- Add `restart_status` (`running`/`backoff`/`crashloop`), `restart_count`, `next_retry_at` to the per-app JSON returned by `GET /v1/apps[/:uuid]`. Computed via `restart::status(record.restart, params, now)`.
- [ ] Test: an app record with `consecutive_failures=5` reports `restart_status:"crashloop"` in the JSON; a clean one reports `running`. Gate; commit `feat: surface restart status in app API`.

### Task 3.2: POST /v1/apps/:uuid/reset
**Files:** Modify `src/orchestrator/api.rs` (add `reset_app`), `src/api.rs` (route `POST /v1/apps/:uuid/reset`); tests.

- `reset_app(uuid)`: load the record; set `record.restart = restart::reset(record.restart)` (also `next_retry_at=0`); persist; then trigger an immediate reconcile/respawn (reuse `reconcile_record` or `do_respawn`). Return `200` with the new status. `404` if no record.
- [ ] Test: after reset, the record's `restart` is default + `should_respawn(now)` is true; the route returns 200 for a known uuid, 404 for unknown. Gate; commit `feat: add POST /v1/apps/:uuid/reset`.

---

## Phase 4 — L3 runner fail-fast (supervisor repo)

### Task 4.1: Runner exits when its app runtime dies
**Files:** Modify the runner serve/lifecycle (`src/runner/serve.rs` + `src/runner/*`); read them first to find where the `AppRuntime` (fc child / docker container) is owned.

- After the app is up, the runner watches the runtime:
  - **firecracker:** await the `firecracker` child process; on exit → log + `std::process::exit(1)`.
  - **docker:** poll `docker wait`/`docker inspect` (or the existing handle) for the container; on exit → log + exit(1).
  - **wasm:** no long-lived child → nothing to watch (skip).
- The watch runs concurrently with the serve loop (a `tokio::select!` or a spawned task that exits the process). Must NOT exit on a normal `Shutdown` (clean stop) — only on unexpected runtime death.
- [ ] Test: a fake runtime whose "process" future resolves (dies) → the watcher signals exit (test the seam: a `watch_runtime(rt) -> ExitReason` that returns `RuntimeDied` vs `Shutdown`, without calling `process::exit` directly). Gate; commit `feat: runner exits on app runtime death (fail-fast to L2)`.

---

## Phase 5 — node passthrough (node repo: tabbify-service-node)

### Task 5.1: Reset proxy + restart status in topology
**Files:** Read the node's existing start/stop proxy + `directory.rs`/`topology.rs`; modify `src/http/rest.rs` + `openapi.rs`.

- Add `POST /app/:uuid/reset` (or under the existing admin surface) that proxies to the supervisor's `POST /v1/apps/:uuid/reset` over the mesh (mirror how start/stop proxy).
- Carry `restart_status` through the roster/topology JSON if the supervisor advertises it (best-effort; optional field).
- [ ] Tests mirroring the existing proxy tests; OpenAPI updated. Gate (`cargo test`/`clippy`/`fmt` in the node repo); commit `feat: node reset passthrough + restart status`.

---

## Phase 6 — L1 launcher restart + systemd unit (tcli + supervisor deploy)

### Task 6.1: `--restart` in node.toml + docker argv
**Files:** Modify `tabbify-cli/src/node.rs` (`Backend` gets `#[serde(default)] restart: Option<String>` defaulting to `"on-failure"`; `build_docker_argv` inserts `--restart <policy>` right after `-d`); update `examples/node.toml`; tests.

- [ ] Test: default manifest → argv contains `--restart on-failure`; explicit `restart = "unless-stopped"` → that value; argv order: `docker run -d --restart <p> --name …`. Gate; commit `feat: tcli node up sets docker --restart`.

### Task 6.2: Sample systemd unit + docs
**Files:** Create `tabbify-service-supervisor/deploy/tabbify-supervisor.service`; add an "L1: keeping the supervisor alive" section to `deploy/README.md`.

```ini
[Unit]
Description=Tabbify supervisor node
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
ExecStart=/usr/local/bin/supervisord
Restart=on-failure
RestartSec=10
# systemd >=254 backoff (ignored on older):
RestartSteps=5
RestartMaxDelaySec=300
[Install]
WantedBy=multi-user.target
```
- [ ] Validate it parses (`systemd-analyze verify` if available, else a comment); document docker `--restart` vs systemd. Commit `docs: L1 supervisor restart (systemd unit + docker --restart)`.

---

## Self-review notes
- Spec coverage: L1 (6.1/6.2), L2 (1.1/2.1/2.2/3.1), L3 (4.1), reset (3.2), node (5.1), status model (3.1) — all covered.
- Type consistency: `RestartState`/`RestartStatus`/`BackoffParams` defined in 1.1, reused by name in 2.x/3.x.
- No sleeps in unit tests — injected `now_secs`. Integration (crashloop live) is manual/Lima, noted in the spec §9.
- Always-green: each task compiles + passes independently; phases ordered so deps exist (1→2→3; 4 independent; 5 needs 3.2; 6 independent).
