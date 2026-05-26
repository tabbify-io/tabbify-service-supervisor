# Runtime Lifecycle Contract — Design

> **Goal:** formalize `AppRuntime` from a one-method data-plane seam into a clean
> per-runtime **lifecycle contract**, so the orchestrator + runner stay fully
> runtime-agnostic and each runtime (wasm / firecracker / docker) owns its whole
> behaviour behind ONE interface. The external control API stays common.
> Approved via brainstorming 2026-05-26. Absorbs the restart plan's Phase 4
> (fail-fast = a contract method).

## 1. The split: common vs per-type

| Layer | Common (shared, runtime-agnostic) | Per-type (each runtime owns) |
|---|---|---|
| **External HTTP API** | `start`/`stop`/`purge`/`reset`/`health`/`list`/`topology` — uuid-based, identical for all. **Stays common** (the node + operator never branch on runtime). | — (runtime-unique ops, e.g. a future fc snapshot, would be an *optional extension*, never a core change) |
| **Runtime contract** (`trait AppRuntime`) | the lifecycle methods below | the implementations |
| **Build / fetch** | fetch artifact from S3; `build_runtime` factory dispatch | how each builds: wasm compiles a Component; fc boots a microVM (kernel+rootfs+tap); docker builds+runs a container |
| **Networking / proxy** | "an app serves HTTP on its app-ULA" | wasm in-process call; fc proxy → guest `172.31.0.2:8080`; docker proxy → published host port |
| **Restart policy** | backoff + crash-loop + reset (L2 monitor) — runtime-agnostic | — |
| **Config** | uuid, data_dir, mesh | `FcConfig`, `DockerConfig`, wasm fuel |
| **Warm levers** | — | wasm: ProxyPre/pool/AOT; fc: snapshot; docker: prebuilt image (separate warm spec) |

wasm isn't even a "container" (in-process) — which is exactly why the common seam
must be a **contract**, not a shared implementation.

## 2. Current state (audit)
- `trait AppRuntime` has ONE method: `handle(req) -> resp` (`runtime.rs:50`). Good
  data-plane seam.
- Everything else is **scattered**: build dispatch in `build.rs`; respawn in
  `orchestrator`; stop/cleanup split across the runner + `firecracker.rs` Drop +
  `docker.rs`; health = only the runner's control-socket liveness, not the app's.
- Per-runtime impls already live in their own files (`runtime.rs`, `docker.rs`,
  `firecracker.rs`) with their own config — the bones are right; the contract is
  just too thin.

## 3. The contract (grow `AppRuntime`)
Add a small `BoxFut<'a, T>` alias next to the existing `BoxRespFut`. Grow the
trait to the lifecycle the system ACTUALLY uses (no speculative methods — YAGNI):

```rust
pub trait AppRuntime: Send + Sync {
    /// Data plane: drive one HTTP request. (exists)
    fn handle<'a>(&'a self, req: Request<Bytes>) -> BoxRespFut<'a>;

    /// Liveness of the APP (not the runner process). Default: healthy — a wasm
    /// runtime is serviceable as soon as it is loaded. fc/docker probe the
    /// guest / container.
    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> { /* default Ok */ }

    /// Resolve when the runtime dies UNEXPECTEDLY (fail-fast → L2 restarts).
    /// Default: never resolves (wasm has no long-lived process). fc awaits the
    /// firecracker child; docker `docker wait`s the container.
    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> { /* default pending */ }

    /// Graceful stop + resource cleanup. Default: no-op (wasm drops). fc kills
    /// the VM + tears down the tap; docker stops + removes the container.
    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> { /* default no-op */ }
}
```
- `RuntimeHealth` = `{ Serving, Unavailable(reason) }`. `ExitReason` = `{ Died(detail), … }`.
- **Default methods** keep wasm trivial (it overrides only `handle`); fc/docker
  override `health`/`watch_for_exit`/`shutdown`. This IS the "common contract,
  per-type impl" — wasm doesn't carry fc/docker concerns.
- The `build_runtime(uuid, app, fc_cfg, docker_cfg, data_dir) -> Box<dyn AppRuntime>`
  factory stays as the construct+start entry (per-runtime dispatch on
  `manifest.runtime.type`); it already isolates per-type construction.
- Object-safety preserved (all methods take `&self`, return boxed futures — same
  pattern as `handle`).

## 4. Wiring (orchestrator + runner stay runtime-agnostic)
- **runner** holds `Arc<dyn AppRuntime>` and uses ONLY the contract:
  - serve loop → `handle`;
  - **fail-fast** → `select!` on `watch_for_exit()` vs the shutdown signal; on
    `ExitReason::Died` log + `process::exit(1)` (→ L2 respawns); on shutdown call
    `shutdown()` and exit cleanly (this replaces the scattered runner exit logic
    and is the restart plan's Phase 4);
  - control `health` → `health()` (true app health, not just "runner up").
- **stop/purge** (orchestrator/runner) call `shutdown()` instead of bespoke
  per-runtime teardown; the `firecracker.rs`/`docker.rs` Drop reaping stays as the
  belt-and-suspenders safety net.
- Nothing in the orchestrator/runner branches on runtime type any more — only
  `build_runtime` does (one place).

## 5. Migration — incremental, always-green
1. **`health` + `RuntimeHealth`** on the trait; wasm default, fc probes the guest,
   docker probes the container; wire the runner control `health` to it.
2. **`watch_for_exit` + `ExitReason`**; wasm default (pending), fc awaits its
   child (reuse the existing pidfile/child handle), docker `docker wait`; wire the
   runner fail-fast `select!`. (= restart Phase 4; supersedes its §4.3.)
3. **`shutdown`**; wasm no-op, fc kill-VM+tap-teardown, docker stop+rm; route
   stop/purge through it; keep Drop as the safety net.
4. **wasm warm fix**: hoist `instantiate_pre`/`ProxyPre::new` out of `handle` into
   `WasmRuntime::load` (store a `ProxyPre<Ctx>`); per request only
   `instantiate_async`. (Correctness/warm win flagged separately; lands in the
   wasm module while we're in it.)

Each step compiles + passes independently; default trait methods mean a step that
adds a method doesn't break the other two runtimes before they override it.

## 6. Out of scope (tracked separately)
- **Warm-start spec** (the bigger one): wasm pooling allocator + AOT `.cwasm`
  cache; fc snapshot/restore; docker build-at-push (not at-request). The contract
  gets a `prepare`/`warm` hook THEN, not now.
- **Restart Phase 5** (node `reset` passthrough) — external API, proceeds on the
  restart plan independently of this refactor.
- Runtime-unique external endpoints (e.g. fc snapshot) — only if/when needed, as
  optional extensions.

## 7. Testing
- Per-runtime contract conformance: each impl's `health`/`watch_for_exit`/
  `shutdown` behave (wasm uses the defaults — assert wasm's `watch_for_exit` is
  pending and `health` is Serving; fc/docker test their overrides with a faked
  child/container handle, no real VM in unit tests).
- The runner fail-fast `select!` seam: a fake runtime whose `watch_for_exit`
  resolves → the runner's decision is `Exit(Died)`; a shutdown signal → `Exit(Clean)`
  — test the pure decision, not `process::exit`.
- Keep `cargo test --lib` green at every migration step.
