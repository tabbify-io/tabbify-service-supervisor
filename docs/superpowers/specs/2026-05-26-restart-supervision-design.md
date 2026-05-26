# Restart Supervision — Design

> **Goal:** make every layer self-heal correctly — the supervisor restarts when
> it crashes, and it restarts its apps when they crash — with proper **backoff**
> and **crash-loop** handling (not hot-looping), plus a **reset** escape hatch.
> Approved via brainstorming 2026-05-26.

## 1. Three supervision levels (one restart brain)

```
init (docker --restart / systemd)  ──restarts──▶  supervisord        (L1)
supervisord  monitor loop          ──restarts──▶  tabbify-runner'ы    (L2)  ← THE restart brain
tabbify-runner                     ──exits on──▶  its app runtime     (L3)  ← fail-fast, no policy here
```

Key simplification: **the restart policy lives in ONE place — L2 (the monitor).**
- **L1** is provided by the launch backend — not our code (docker `--restart`,
  systemd `Restart=`). The launcher just sets it.
- **L3** has NO policy of its own: when a runner's app runtime dies, the runner
  **exits non-zero** (fail-fast). That turns an app crash into a dead-runner that
  L2 already detects — so app-crash and runner-crash flow through the SAME L2
  restart logic. No duplicated policy.

Why this is correct (and not Erlang-style escalation): apps are independent
tenants, so one crash-looping app must NOT take down the node or its siblings —
L2 isolates the failure per-app (k8s `CrashLoopBackOff` model), never escalates
up to kill the supervisor.

## 2. The restart policy — three knobs

Mature supervisors (k8s, systemd, runit/s6, OTP) all separate three concerns:

**WHEN to restart** — already correct at L2, for free:
- `stop`/`purge` **removes the runner record** → the monitor never sees it →
  never respawns it. An intentional stop stays stopped.
- A crash = record still present + pid dead → restart. So "restart on failure,
  not on intentional stop" already holds; we only add the other two knobs.

**HOW FAST — exponential backoff** (so a broken app can't hot-loop):
- `delay = min(BASE * 2^(consecutive_failures-1), CAP)`,
  `BASE = 10s`, `CAP = 300s` (5 min). 10s → 20s → 40s → … → 300s.
- The monitor only respawns a dead runner once `now ≥ next_retry_at`; before
  that the app sits in `Backoff` (the tick is a no-op for it).
- **Reset:** once a restarted runner has been healthy for ≥ `STABLE = 60s`, the
  failure count + backoff reset to zero (a once-a-week crash never accumulates).

**HOW MANY — crash-loop status, never a hard give-up** (the "сколько раз" answer):
- We do NOT use a fixed total ("restart 5 times then quit forever") — that
  punishes a service that crashes rarely. We use a **rate**: consecutive failures
  within the backoff window.
- After `CRASHLOOP_THRESHOLD = 5` consecutive failures the app is labelled
  `CrashLoop` (surfaced everywhere) but **keeps retrying at the 5-min cap** —
  k8s-style. So a transient infra outage self-heals once it clears, without
  anyone touching it; a genuinely broken app sits visibly in `CrashLoop` at 1
  retry / 5 min instead of burning the box.
- `reset` (below) is the escape hatch to skip the wait once you've fixed the cause.

## 3. Status model

Each app/runner carries a restart status (persisted, see §4.2), surfaced in
`GET /v1/apps[/:uuid]`, `/health`, and the node `/v1/topology`:

| status | meaning |
|---|---|
| `running` | healthy; failure count 0 (or reset after STABLE) |
| `backoff` | crashed, waiting until `next_retry_at` to respawn |
| `crashloop` | ≥ CRASHLOOP_THRESHOLD consecutive failures; retrying at the CAP |

`next_retry_at` + `restart_count` are included so an operator (or the node UI)
can see exactly what's happening.

## 4. Per-level design

### 4.1 L1 — init restarts the supervisor (backend-provided)
- **docker (incl. the test/dogfood path):** the launcher sets
  `--restart=on-failure` (or `unless-stopped`). Docker's built-in restart has
  exponential backoff; no init process needed in the image. → `tcli node up` adds
  `--restart` (from a `node.toml` `[backend] restart = "on-failure"`, default
  `on-failure`).
- **firecracker microVM / bare VPS:** a systemd unit inside the guest:
  `Restart=on-failure`, `RestartSec=` + `RestartSteps`/`RestartMaxDelaySec`
  (backoff), `StartLimitIntervalSec`/`StartLimitBurst` (rate cap). Ship a sample
  unit `deploy/tabbify-supervisor.service` + document it in `deploy/README.md`.
- No supervisor code changes for L1 — it is purely how the supervisor is launched
  (consistent with the responsibility model: the owner owns the launch).

### 4.2 L2 — supervisor restarts runners (THE policy; supervisor code)
Extend the existing monitor (`src/orchestrator/monitor.rs`), which today respawns
a dead runner **every tick** with no backoff:
- New **pure** module `src/orchestrator/restart.rs`: a `RestartPolicy` /
  `BackoffState` with the §2 maths — inputs `(consecutive_failures, last_exit_at,
  last_healthy_at, now)`, output a `RestartDecision` (`RespawnNow` /
  `WaitUntil(t)` / status). Fully unit-tested with an injected clock (no sleeps).
- Persist restart state in the `RunnerHandle` record (`handle.rs`): add
  `restart_count: u32`, `last_exit_at: u64`, `next_retry_at: u64`,
  `last_healthy_at: u64`, `status` — so the policy **survives a supervisor
  restart** (re-adopt reads it back).
- `reconcile_record`: when the pid is dead, consult the policy instead of
  respawning unconditionally — respawn only if `now ≥ next_retry_at`, then bump
  the count + compute the next backoff; on a healthy adopt past `STABLE`, reset.
- The decision matrix (grace window, kill-before-respawn) is unchanged; backoff
  wraps only the `RespawnDead` / post-kill-respawn arms.

### 4.3 L3 — runner fail-fast on app death (small runner change)
The runner is 1:1 with its app and is the natural watcher:
- **firecracker / docker:** the runner already spawns a child (the `firecracker`
  process / a detached container). It watches it; when the child/container exits,
  the runner **exits non-zero** → L2 respawns the runner (re-claiming the same
  uuid-derived ULA via the sticky identity), which re-boots the app.
- **wasm:** runs per-request in-process (no long-lived child) — nothing to watch;
  a failed invocation is a request error, not a process death. n/a.
- This keeps ALL restart intelligence at L2 (the runner has no backoff logic).

## 5. The reset endpoint
`POST /v1/apps/:uuid/reset` on the supervisor control API (the analog of
`systemctl reset-failed` / clearing `CrashLoopBackOff`):
- Clears `restart_count` + `next_retry_at` + `status`, and triggers an immediate
  respawn (skip the backoff wait). Use after fixing the cause of a crash loop.
- Exposed through the node gateway (`POST /app/:uuid/reset` or under the admin
  surface) so it's reachable without mesh access, mirroring start/stop/purge.
- Distinct from `purge` (which also clears the artifact cache + docker image);
  `reset` keeps the cache and just clears the failure state.

## 6. Defaults (tunable)
`BASE=10s`, `CAP=300s`, `STABLE=60s`, `CRASHLOOP_THRESHOLD=5`. k8s-like; no hard
give-up. Surfaced as config (`SUPERVISOR_RESTART_*` env) but the defaults are
sane out of the box.

## 7. What already exists vs. new
- **Exists:** L2 detects dead runners + respawns (no backoff); `stop` removes the
  record (correct WHEN); sticky ULA across runner restarts; the grace-window /
  kill-before-respawn matrix.
- **New:** the `RestartPolicy`/backoff module; persisted restart state + status;
  backoff-gated respawn; `crashloop` status surfaced; the runner's fail-fast app
  watch (L3); the `reset` endpoint; the launcher `--restart` + sample systemd unit.

## 8. Out of scope (follow-ups)
- Alerting/metrics on `crashloop` (emit an event; a dashboard consumes it).
- Per-app override of the restart defaults via the app manifest.
- `run-once` (batch) apps with `Restart=no` semantics — YAGNI (all apps are
  long-running servers today).

## 9. Testing
- **Unit (no sleeps):** `restart.rs` backoff schedule + reset + crashloop
  threshold with an injected clock; the persisted-state round-trip in `handle.rs`.
- **Integration:** an app that exits immediately → observe `backoff` →
  `crashloop` (retry interval grows to the cap, not a hot loop); `reset` →
  immediate retry; a flapping app that then stays up → status returns to
  `running` after `STABLE`. Reuse the Lima fc path + a local wasm/docker app.
- **L1:** assert `tcli node up` emits `--restart=on-failure`; the systemd unit is
  documented + lint-parseable.
