# Warm Start — Design

> **Goal:** kill cold-start latency per runtime. Today: wasm recompiles
> (Cranelift) on every runner (re)start; docker **builds from source on the hot
> path** (minutes — `build_timeout=300s`); fc boots a kernel each time (~seconds).
> Each lever is an optimization INSIDE the runtime's build/load — **no new
> contract method needed** (the lifecycle contract stays `handle`/`health`/
> `watch_for_exit`/`shutdown`). Approved via brainstorming 2026-05-26.

## Levers (by value ÷ effort)

| # | runtime | lever | cold today | warm after |
|---|---|---|---|---|
| W1 | wasm | AOT `.cwasm` cache + pooling allocator | Cranelift each (re)start | `deserialize` (~ms) on restart |
| W2 | docker | supervisor-local image cache | rebuild each host/restart | reuse cached image |
| W3 | docker | build-at-push → S3 image tar | build-from-source (minutes) on 1st request | `docker load` (seconds) |
| W4 | fc | snapshot/restore | kernel boot + app start (~s) | `LoadSnapshot` (~ms) |

## W1 — wasm AOT cache + pooling (supervisor-local)
- `Engine::precompile_component(bytes)` → serialized `.cwasm`; cache at
  `<data_dir>/apps/<uuid>/v<N>/app.cwasm`. On `WasmRuntime::load`: if the cache
  exists, `unsafe { Component::deserialize(&engine, bytes) }` (wasmtime embeds an
  engine/version/cfg header → `deserialize` REJECTS a mismatch → on any error
  fall back to `Component::new` + re-cache). So a runner restart skips Cranelift
  entirely; a wasmtime upgrade self-heals (mismatch → recompile).
- Engine `Config`: add a `PoolingAllocationConfig` (pre-reserved instance slots
  + bounded memory) so per-request `instantiate_async` is faster + memory churn
  is bounded under load. Tunable; sane defaults.
- Pure-testable: the cache decision (`load_cached_or_compile`) with an injected
  "cache hit/miss/corrupt" → compile vs deserialize vs recompile-on-error.

## W2 — docker supervisor-local image cache (cheap restart win)
- Tag built images deterministically by `uuid` + version (already
  `tbf-<uuid>-<seq>`? — switch the IMAGE tag to content-stable `tbf-img-<uuid>-v<N>`).
- Before `docker build`: `docker image inspect <tag>` — if present, SKIP the
  build and run the cached image. A re-host / restart on the same supervisor is
  then warm (no rebuild). `purge` still removes the image (existing behavior).
- Smallest change, big restart win; independent of W3.

## W3 — docker build-at-push → S3 image tar (kills the first-build minutes)
- `tcli push` for a `docker` app: `docker build` the image locally, then
  `docker save | gzip` → upload `…/<uuid>/v<N>/image.tar.gz` to S3 alongside the
  manifest. (The pusher has docker — they're shipping a docker app.)
- Supervisor docker runtime: if `image.tar.gz` exists in S3, fetch + `docker
  load` it (warm) INSTEAD of build-from-source; fall back to source build (W2) if
  absent. Additive — old source-only apps still work.
- **Arch-matching caveat:** the saved image is the pusher's arch. Same-arch hosts
  load directly; cross-arch needs `tcli push --platform` (buildx) or a per-arch
  tar. v1: build for the host arch + document; multi-arch tars = follow-up.
- Decision rationale: reuse the platform's anonymous-S3 fetch model (no registry
  auth on supervisors), consistent with how wasm/fc artifacts already ship.

## W4 — fc snapshot/restore (most complex; Lima-gated)
- Boot the app VM once, wait-until-ready, `PUT /snapshot/create` (pause + dump
  memory + vmstate to `<cache>/snap.{mem,vmstate}`), then serve from it. On a
  later start/restart: `PUT /snapshot/load` (restore in ~ms, skipping kernel boot
  + app init) + re-attach the tap.
- **Caveats (validate in Lima FIRST, do not rabbit-hole):** snapshots are
  host-kernel + cpu-template specific; the restored guest resumes mid-execution
  (the app must tolerate resume — our stateless hello-server does); networking
  (tap/MAC) must be re-wired on restore; clock/entropy resume quirks. Because of
  these, W4 is **feasibility-gated**: spike it in Lima; if it fights, ship W1–W3
  and leave fc-snapshot as a documented follow-up (fc boot is ~seconds, not the
  minutes W3 saves — lowest urgency).

## Non-goals (YAGNI)
- No new contract method — warm is internal to each runtime's construction.
- No cross-host warm pool / pre-booted spare runtimes (a scheduler concern, later).
- No precompile-in-CI for wasm (`.cwasm` is wasmtime-version-specific → compile on
  the supervisor + cache locally is simpler + self-healing).

## Sequencing (always-green, by value÷effort)
W1 (wasm, clean) → W2 (docker cache, cheap) → W3 (docker build-at-push, spans
tcli+S3+supervisor) → W4 (fc snapshot, Lima-gated, last). Each TDD; W3/W4 get a
live check (docker app warm-load; fc restore in Lima).

## Testing
- W1: `load_cached_or_compile` decision (hit→deserialize, miss→compile+cache,
  corrupt→recompile) with injected cache; a real round-trip
  `precompile → deserialize → handle serves` test.
- W2: the "image exists → skip build" decision (injected `docker image inspect`).
- W3: `tcli` save+upload (dry-run asserts the S3 key + that `docker save` is
  invoked); supervisor load-vs-build decision (injected "tar present"); a live
  docker app that loads from a tar.
- W4: Lima — boot → snapshot → restore → `200 OK`, restore latency << cold boot.
