# Warm Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Fresh subagent per task, TDD. Per the spec `docs/superpowers/specs/2026-05-26-warm-start-design.md`.

**Sequencing (always-green, by value÷effort):** W1 → W2 → W3 → W4. No new contract method — warm is internal to each runtime's build/load.

---

### W1 — wasm AOT `.cwasm` cache + pooling allocator (supervisor)
**Files:** `src/runtime.rs` (`WasmRuntime` load path + engine `Config`), `src/build.rs` (pass the per-app cache dir into the wasm load), tests.

- `Engine::precompile_component(bytes) -> Vec<u8>`; cache at `<app cache dir>/app.cwasm`.
- New `WasmRuntime::load_cached_or_compile(bytes, cache_path)`: if `cache_path` exists → `unsafe { Component::deserialize_file(&engine, cache_path) }`; on ANY error (missing/corrupt/version-mismatch) → `Component::new(&engine, bytes)` then write the cache (`engine.precompile_component` → file). The existing `load(bytes)` stays (no cache) for tests/back-compat; `build_runtime` calls the cached variant with the app's cache dir.
- Engine `Config`: add `PoolingAllocationConfig` (sane bounded defaults) via `cfg.allocation_strategy(...)`.
- [ ] Tests: the pure cache decision (hit→deserialize path, miss→compile+write, corrupt bytes→fall back to compile) with a temp dir; a round-trip `precompile → deserialize → handle serves 200`. Gate (`cargo test --lib`, clippy `-D warnings`, fmt). Commit `perf: wasm AOT .cwasm cache + pooling allocator`.

### W2 — docker supervisor-local image cache (supervisor)
**Files:** `src/docker.rs` (image tag + build path), tests.

- Tag the built image content-stably per app+version: `tbf-img-<uuid>-v<N>` (distinct from the per-run container name `tbf-<uuid>-<seq>`).
- Before `docker build`: `docker image inspect <tag>` (injectable command seam, mirror the existing probe/watcher seams) — present → SKIP build, run the cached image; absent → build then tag. `purge` removes the image (keep existing behavior).
- [ ] Tests: the "image present → skip build" decision (injected inspect → exists/not); the tag is deterministic for a given uuid+version. Gate. Commit `perf: cache built docker image, skip rebuild on restart`.

### W3 — docker build-at-push → S3 image tar (tcli + supervisor)
**Files (tcli):** `tabbify-cli/src/push.rs` (+ manifest) — for a `docker` app, `docker build` then `docker save | gzip` → add `image.tar.gz` to the upload set. **Files (supervisor):** `src/docker.rs` + `src/build.rs`/`fetcher.rs` — fetch+`docker load` if the tar exists, else source build (W2).
- tcli: detect `runtime.type=="docker"`, build (host arch; `--platform` passthrough later), `docker save <tag> | gzip` → S3 key `…/<uuid>/v<N>/image.tar.gz`. `--dry-run` asserts the key + that save is invoked (don't run docker in tests).
- supervisor: if the tar is present in S3 → fetch → `docker load` → run; else fall back to build-from-source. Additive.
- [ ] Tests: tcli save+upload-plan (dry-run, injected docker); supervisor load-vs-build decision (injected "tar present"); a live docker app that loads from a tar (Lima/Mac). Gate (both repos). Commits: tcli `feat: tcli builds + uploads docker image tar at push`; supervisor `feat: docker runtime loads prebuilt image tar (warm), source-build fallback`.

### W4 — fc snapshot/restore (supervisor; Lima-gated)
**Files:** `src/firecracker.rs` (snapshot create on first boot + load on restart), tests + Lima E2E.
- **FIRST: feasibility spike in Lima** — boot → `PUT /snapshot/create` (pause+dump) → `PUT /snapshot/load` in a fresh fc → re-attach tap → `200 OK`, measure restore vs cold boot. If it fights (networking-on-resume, host-specificity), STOP and ship W1–W3, leave W4 a documented follow-up.
- If feasible: cache `snap.{mem,vmstate}` per app; `launch` loads the snapshot when present (skip boot), else cold-boot then create the snapshot.
- [ ] Tests: protocol bodies for create/load (pure, like the existing fc REST body builders); Lima live restore. Gate. Commit `feat: firecracker snapshot/restore warm start` (or a follow-up note if deferred).

---

## Self-review
- Spec coverage: W1 (wasm), W2+W3 (docker), W4 (fc) — all levers. No contract change (correct). Arch-matching (W3) + fc caveats (W4) flagged.
- Always-green: each phase independent + additive (cache/load preferred, old path as fallback).
- No real daemon/VM in unit tests — inject command/cache seams; live checks for W3/W4.
