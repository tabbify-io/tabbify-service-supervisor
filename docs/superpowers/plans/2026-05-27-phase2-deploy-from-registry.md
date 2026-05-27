# Phase 2 — Deploy from Registry (supervisor pull + zero-downtime swap)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.
> Fresh subagent per task, TDD, always-green. Steps use `- [ ]`.
>
> **Spec:** `docs/superpowers/specs/2026-05-27-build-deploy-pipeline-design.md` §4.3 (+ §2 flow step 5).
> **Phase 1 (the registry) is built:** `../tabbify-service-registry` serves OCI `/v2` on its mesh ULA.

**Goal:** the supervisor can PULL an app's image from the mesh registry by ref and a
`POST /v1/apps/:uuid/deploy { ref }` performs a **zero-downtime** swap to that ref — the
app keeps serving on its unchanged app-ULA throughout. Rollback = deploy an older ref.

**Architecture (concretizes §4.3, already approved):** The swap is **in-process inside the
per-app runner** — the runner's mesh peer identity / app-ULA never changes (two runners
can't co-claim one app-ULA, confirmed in `runner/serve.rs`). The runner holds its runtime
behind an `ArcSwap<dyn AppRuntime>`; `handle()` loads the active runtime per request. A new
`Deploy { ref }` control message makes the runner: build a NEW runtime from the ref →
poll `health()` until Serving (timeout ⇒ abort, old stays active = failed deploy, **no
downtime**) → **atomically store** the new runtime (flip) → spawn a drain task that
`shutdown()`s the old after a grace period. The deployed ref is persisted in the
`RunnerHandle` so a supervisor restart respawns the same version. Image source: the docker
runtime gains a `docker pull <ref>` path slotted **before** build-from-source in the
existing W3→W2→build decision; the `ref` already embeds the registry ULA
(`<registry_ula>:5000/<tenant>/<app>:<sha>`), so no roster discovery is needed — the host's
docker daemon must list the registry ULA under `insecure-registries` (mesh is encrypted).

**Scope:** **docker-first** (the pipeline's primary artifact). The swap mechanism is generic
over the runtime contract, so wasm swaps in-process trivially; **wasm-from-registry (`oras
pull`) and firecracker swap are follow-ups** — fc additionally needs per-uuid taps (a known
tracked limitation) before two fc instances can co-exist on one host during a swap.

**Tech Stack:** Rust 2024, the existing supervisor (`supervisord` + `tabbify-runner`),
`arc-swap` (new dep, lock-free runtime cell), the existing control-socket protocol, axum,
the node passthrough pattern.

---

## File structure (what each task touches)

```
tabbify-service-supervisor/
├── src/manifest.rs            # + Runtime.registry_ref: Option<String>          (P2.1)
├── src/docker.rs              # + docker pull <ref> source in the launch decision (P2.1)
├── src/runtime.rs             # build_runtime gains an optional image_ref override (P2.1)
├── src/runner/
│   ├── serve.rs               # hold ArcSwap<dyn AppRuntime>; handle() loads active (P2.2)
│   └── control / protocol     # + Deploy{ref} control message + handler            (P2.3)
├── src/orchestrator/
│   ├── handle.rs              # RunnerHandle + image_ref: Option<String> (persist) (P2.3)
│   └── api.rs                 # + deploy_app(uuid, ref)                             (P2.4)
├── src/api.rs                 # + POST /v1/apps/:uuid/deploy { ref }                (P2.4)
tabbify-service-node/
└── src/{service,proxy,http/rest,http/mcp,openapi}.rs  # deploy passthrough + MCP tool (P2.5)
```

---

### Task P2.1 — docker `pull <ref>` image source + manifest `registry_ref`

**Files:** `src/manifest.rs`, `src/docker.rs`, `src/runtime.rs`. **Tests:** inline + docker decision tests (injected command seam — no real docker).

The docker runtime's `launch_with_id` decides the image source: W3 (prebuilt tar) → W2
(cached `tbf-img-<uuid>-v<N>`) → build-from-source. Insert a **registry-pull** step: when an
image ref is provided (manifest `registry_ref` or a deploy override), `docker pull <ref>`
and `docker tag <ref> <vtag>` it into the local cache **before** the W2/build branch; on
pull success skip build; on failure fall back to the existing path. Use the existing
injectable command-runner seam (like `TarLoadRunner`/`InspectRunner`) so tests don't run
docker.

- [ ] **Step 1** Add to `Runtime` in `manifest.rs`:
```rust
    /// Optional OCI image ref to pull instead of building from source (docker only),
    /// e.g. "[fd5a:1f02:..::1]:5000/acme/app:<sha>". When set, the runtime pulls it.
    #[serde(default)]
    pub registry_ref: Option<String>,
```
- [ ] **Step 2 (TDD)** In `docker.rs`, write a failing test for the pure pull decision:
```rust
#[test]
fn pull_decision_uses_ref_when_present_else_skips() {
    assert_eq!(pull_decision(Some("[fd5a::1]:5000/acme/app:abc")), PullDecision::Pull("[fd5a::1]:5000/acme/app:abc".into()));
    assert_eq!(pull_decision(None), PullDecision::Skip);
}
```
and a test that a successful injected pull short-circuits the build (mirror `image_cache_decision`'s test style with an injected pull-runner returning success → `SourceDecision::Skip`).
- [ ] **Step 3** Implement `pull_decision(Option<&str>) -> PullDecision { Pull(String), Skip }` and a `pull_and_tag(docker_bin, reff, vtag, runner) -> bool` using `pull_args(reff)` + `tag_args(reff, vtag)` (mirror `load_args`/`inspect_args`). Wire it into `launch_with_id` before the W2 `image_cache_decision`: if a ref is present and pull+tag succeed, set the cache decision to Skip; else continue to W2/build.
- [ ] **Step 4** In `runtime.rs`, thread an `image_ref: Option<&str>` into `build_runtime` (default `None` from current callers — additive) so a deploy can override the manifest. The docker arm passes it to `launch_with_id`; wasm/fc ignore it for now (documented).
- [ ] **Step 5** Gate (`cargo test --lib`, clippy `-D warnings`, fmt) + commit `feat: docker runtime pulls image by ref (registry source) before build`.

---

### Task P2.2 — runner holds a swappable runtime (`ArcSwap`)

**Files:** `Cargo.toml` (+ `arc-swap`), `src/runner/serve.rs` (+ wherever the runtime is held/served). **Tests:** the load-per-request behavior with a fake runtime.

Today the runner builds one `Arc<dyn AppRuntime>` and serves it. Make the served runtime a
swappable cell so a later `Deploy` can replace it atomically without touching the listener
or the mesh peer.

- [ ] **Step 1** Add `arc-swap = "1"` to `[dependencies]`.
- [ ] **Step 2 (TDD)** Write a failing test: an `ActiveRuntime` (newtype over `Arc<ArcSwap<dyn AppRuntime>>`) returns runtime A for `handle`/`health`, then after `.swap(B)` returns B — using two trivial fake `AppRuntime`s whose `handle` returns distinguishable bodies.
- [ ] **Step 3** Introduce `ActiveRuntime` wrapping `arc_swap::ArcSwap<dyn AppRuntime>` (store `Arc<dyn AppRuntime>`); methods `load() -> Arc<dyn AppRuntime>`, `swap(new) -> Arc<dyn AppRuntime>` (returns the previous, for draining). Implement `AppRuntime` for `ActiveRuntime` by delegating to `self.load()` (so the existing serve path is unchanged — it just calls the contract on the active one).
- [ ] **Step 4** Thread `ActiveRuntime` through `RunnerServe`/`run_until_exit`/the app router so the served handler loads the active runtime per request. `watch_for_exit`/`shutdown` apply to the active runtime. Keep behavior identical when no swap ever happens (the existing tests must stay green).
- [ ] **Step 5** Gate + commit `refactor: runner serves a swappable ActiveRuntime (no behavior change)`.

---

### Task P2.3 — `Deploy { ref }` control message → zero-downtime swap

**Files:** the control-socket protocol enum + the runner's control handler (`src/runner/…`), `src/orchestrator/handle.rs` (persist `image_ref`). **Tests:** the swap state machine with fake runtimes (health-ok → flip + old.shutdown called; health-timeout → abort, old kept, new.shutdown called).

- [ ] **Step 1 (TDD)** Failing test for the swap routine `perform_swap(active, new, drain, deadline)`:
  - new becomes healthy within deadline ⇒ `active` now serves new; the returned/old runtime's `shutdown()` was invoked (assert via a fake that records shutdown); result `Ok`.
  - new never healthy ⇒ result `Err`; `active` still serves the OLD; the NEW's `shutdown()` was invoked; no flip.
  Use fake `AppRuntime`s with a controllable `health()` and a shutdown-recording flag.
- [ ] **Step 2** Implement `perform_swap`: poll `new.health()` until `Serving` or `deadline`; on success `let old = active.swap(new); tokio::spawn(async move { sleep(drain).await; old.shutdown().await; }); Ok(())`; on timeout `new.shutdown().await; Err(..)`.
- [ ] **Step 3** Add `Deploy { reff: String }` to the control protocol enum + a `Reply` variant (`Deployed`/`Ok`). In the runner's control handler: build a new runtime via `build_runtime(..., image_ref = Some(&reff))` (fetch/pull as needed), then `perform_swap`. Reply `Ok` on success, an error reply on failure.
- [ ] **Step 4** Persist the ref: add `#[serde(default)] image_ref: Option<String>` to `RunnerHandle` (`orchestrator/handle.rs`); the orchestrator sets it on a successful deploy and passes it into the `SpawnSpec`/runner args on (re)spawn so a supervisor restart respawns the deployed version. (Runner gains an optional `--image-ref` arg consumed into the initial `build_runtime`.)
- [ ] **Step 5** Gate + commit `feat: runner Deploy{ref} control msg — in-process zero-downtime swap`.

---

### Task P2.4 — orchestrator `deploy_app` + `POST /v1/apps/:uuid/deploy { ref }`

**Files:** `src/orchestrator/api.rs`, `src/api.rs`. **Tests:** the handler/orchestrator path (mirror reset/purge tests).

- [ ] **Step 1 (TDD)** Failing test: `deploy_app(uuid, ref)` on a live runner sends `Deploy{ref}` over the control client and returns an `AppSummary` (reuse the reset/start test harness with a fake control server that records the `Deploy`).
- [ ] **Step 2** Implement `deploy_app(&self, uuid, reff) -> Result<AppSummary>`: if the runner is healthy → `client.deploy(reff)`; if not running → start a runner with `image_ref = Some(reff)` (reuse `spawn_spec_for_uuid` + set the ref) then `wait_healthy`. On success update the persisted `RunnerHandle.image_ref`.
- [ ] **Step 3** Add the route `.route("/v1/apps/:uuid/deploy", post(deploy_app))` + handler taking `Path(uuid)` + `Json(DeployBody { reff: String })` (accept JSON key `ref`); mirror `reset_app`'s response/error mapping. (Use `#[serde(rename = "ref")]` since `ref` is a Rust keyword.)
- [ ] **Step 4** Gate + commit `feat: POST /v1/apps/:uuid/deploy {ref} — orchestrator deploy + swap`.

---

### Task P2.5 — node passthrough + MCP tool for deploy

**Files:** `tabbify-service-node/src/{service.rs,proxy.rs,http/rest.rs,http/mcp.rs,openapi.rs}`. **Tests:** mirror the reset passthrough tests.

- [ ] **Step 1 (TDD)** Failing test: node `deploy_app(uuid, ref)` resolves a supervisor from the roster and POSTs `/v1/apps/:uuid/deploy {ref}` (mirror the `reset_app` proxy test).
- [ ] **Step 2** Add `deploy_app` to the `SupervisorControl` trait + `ReqwestProxy` impl (POST with the `{ "ref": ... }` body), and `NodeService::deploy_app`.
- [ ] **Step 3** Add the REST route + the OpenAPI entry + an MCP tool `deploy_app(uuid, ref)` (mirror the reset MCP tool).
- [ ] **Step 4** Gate (node repo: `cargo test`, clippy, fmt) + commit `feat: node deploy passthrough + MCP tool`.

---

### Task P2.6 — Lima E2E + docs

**Files:** a runbook section (`deploy/README.md` or `docs/`), a smoke script. **Live (Lima/AWS — Leo's env).**

- [ ] **Step 1** Document + script the E2E: push image `v1` to the registry → `POST /deploy {ref=v1}` → supervisor pulls → app serves; build `v2`, push, `POST /deploy {ref=v2}` → **seamless swap** (a tight `curl` loop sees no failed request across the flip); `POST /deploy {ref=v1}` → rollback. Requires the host docker daemon's `insecure-registries` to include the registry ULA.
- [ ] **Step 2** Commit `docs: phase-2 deploy-from-registry E2E runbook + smoke script`.

---

## Self-Review
- **Spec §4.3 coverage:** registry-pull source (P2.1) ✓ · `POST /deploy {ref}` (P2.4) ✓ ·
  runner zero-downtime swap, app-ULA unchanged (P2.2+P2.3) ✓ · rollback = deploy old ref
  (P2.6) ✓ · node passthrough (P2.5) ✓.
- **Always-green:** P2.1 additive (ref source falls back to build). P2.2 is a behavior-
  preserving refactor (ActiveRuntime delegates; existing tests stay green). P2.3 adds a new
  control msg (unused until P2.4 calls it). P2.4 the endpoint. P2.5 node. Each TDD with
  fakes; no real docker/VM in unit tests; live swap is Lima/AWS.
- **Failed deploy = no downtime:** `perform_swap` only flips AFTER the new runtime is
  healthy; on timeout the old stays active and the new is shut down (P2.3 test asserts both).
- **Type consistency:** `ActiveRuntime` (P2.2) used by `perform_swap` (P2.3); `image_ref`
  flows manifest→`build_runtime` (P2.1)→`SpawnSpec`/`RunnerHandle` (P2.3)→`deploy_app`
  (P2.4). `DeployBody{ref}` ↔ node proxy body (P2.5).
- **Scope honesty:** docker-first; wasm-from-registry (`oras pull`) + fc swap (needs
  per-uuid taps) are explicit follow-ups, flagged in the header, not silently dropped.
- **Open confirm-point:** the exact control-socket protocol enum + `RunnerHandle` shape — the
  P2.3 implementer reads the real `src/runner/` control code + `orchestrator/handle.rs` and
  adapts (the existing `Shutdown`/`Purge`/`Health` messages are the pattern to mirror).
