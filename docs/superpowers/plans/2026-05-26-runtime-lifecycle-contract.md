# Runtime Lifecycle Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Fresh subagent per task, TDD, two-stage review.

**Goal:** grow `AppRuntime` into a per-runtime lifecycle contract (`handle` +
`health` + `watch_for_exit` + `shutdown`), with default methods so wasm stays
trivial and fc/docker own their specifics. orchestrator/runner become fully
runtime-agnostic. External API stays common. Per the spec
`docs/superpowers/specs/2026-05-26-runtime-lifecycle-contract-design.md`.

**Always-green:** default trait methods mean adding a method never breaks the
runtimes that haven't overridden it yet. Sequential (all tasks touch the shared
`runtime.rs` trait + the per-runtime files).

**Shared types (add in Task 1, in `runtime.rs` next to `BoxRespFut`):**
```rust
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub enum RuntimeHealth { Serving, Unavailable(String) }
pub enum ExitReason { Died(String) }   // grows if needed
```

---

### Task 1: `health()` on the contract
**Files:** `src/runtime.rs` (trait + `WasmRuntime` default), `src/firecracker.rs` + `src/docker.rs` (overrides), `src/runner/control.rs`/`serve.rs` (wire control `health` to the app's health), tests in each.

- [ ] Add `fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth>` to `AppRuntime` with a **default** returning `Serving` (wasm uses it).
- [ ] fc override: probe the guest (a cheap GET / TCP-connect to `172.31.0.2:app_port`) → `Serving`/`Unavailable`. docker override: `docker inspect`/a probe of the published port.
- [ ] Wire the runner's control-socket `Health` reply to include the app's `health()` (today it only proves the runner process is up).
- [ ] TDD: wasm `health()` == `Serving` (default); fc/docker health maps a reachable/unreachable probe to `Serving`/`Unavailable` (inject the probe, no real VM). Gate (`cargo test --lib`, clippy `-D warnings`, fmt). Commit `feat: AppRuntime::health contract method`.

### Task 2: `watch_for_exit()` + runner fail-fast (= restart Phase 4)
**Files:** `src/runtime.rs` (trait + wasm default), `src/firecracker.rs` + `src/docker.rs` (overrides), `src/runner/serve.rs` (the `select!`), tests.

- [ ] Add `fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason>` with a **default** that never resolves (`std::future::pending()`) — wasm has no long-lived process.
- [ ] fc override: await the firecracker child process (reuse the existing child/pidfile handle) → `Died`. docker override: `docker wait <container>` → `Died`.
- [ ] runner serve loop: `tokio::select!` between `watch_for_exit()` and the existing shutdown signal. Factor a **pure** `fn decide_exit(which: Branch) -> RunnerExit { Crashed | CleanShutdown }` and unit-test THAT; on `Crashed` the runner logs + `process::exit(1)` (→ L2 respawns), on `CleanShutdown` it calls `shutdown()` (Task 3; until then just returns) and exits 0. Do NOT call `process::exit` in tests.
- [ ] TDD: a fake runtime whose `watch_for_exit` resolves → `decide_exit` == `Crashed`; the shutdown branch → `CleanShutdown`; wasm `watch_for_exit` is pending (assert it doesn't resolve immediately). Gate. Commit `feat: runner fail-fast via AppRuntime::watch_for_exit`.

### Task 3: `shutdown()` + route stop/purge through it
**Files:** `src/runtime.rs` (trait + wasm default no-op), `src/firecracker.rs` (kill VM + tap teardown), `src/docker.rs` (stop + rm), the runner/orchestrator stop path, tests.

- [ ] Add `fn shutdown<'a>(&'a self) -> BoxFut<'a, ()>` with a **default** no-op (wasm drops). Must be idempotent (`&self`).
- [ ] fc override: kill the VM + tear down the tap (consolidate the logic currently in Drop / scattered). docker override: `docker stop` + `docker rm`. Keep the existing Drop reaping as a safety net (idempotent with `shutdown`).
- [ ] Route the runner's `Stop`/`Shutdown` control command + the orchestrator purge through `shutdown()` so the teardown is the contract, not bespoke per-call code.
- [ ] TDD: wasm `shutdown` is a no-op (returns Ok, drop still works); fc/docker `shutdown` issues the expected teardown (inject the command-runner seam, assert the kill/stop+rm calls — no real VM/container). Gate. Commit `feat: AppRuntime::shutdown contract method`.

### Task 4: wasm warm fix — hoist pre-instantiation
**Files:** `src/runtime.rs` (`WasmRuntime`), tests.

- [ ] Move `linker.instantiate_pre(&component)` + `ProxyPre::new(pre)` out of `handle` into `WasmRuntime::load`; store a `proxy_pre: ProxyPre<Ctx>` field. `handle` now does only `Store::new` + `self.proxy_pre.instantiate_async(&mut store)` per request.
- [ ] TDD: `handle` still serves a request end-to-end (reuse/adapt the existing wasm handle test); the struct holds the `ProxyPre` (compile + link happen once, in `load`). Gate. Commit `perf: hoist wasm pre-instantiation to load (warm path)`.

---

## Self-review
- Spec coverage: contract methods (1/2/3), wasm warm (4), runtime-agnostic wiring (1/2/3 wire the runner). External API untouched (correct). Warm-start + node Phase 5 explicitly deferred.
- Type consistency: `BoxFut`/`RuntimeHealth`/`ExitReason` defined in Task 1, reused 2/3. `decide_exit`/`RunnerExit` in Task 2.
- No real VMs/containers/process::exit in unit tests — inject seams.
