# Build → Registry → Deploy Pipeline — Design + Implementation Plan

> **Goal:** source-to-running over the mesh — GitHub clone → sandboxed
> **build-runner** → push OCI to **tabbify-registry** (a wrapper over Zot) →
> **target supervisor** pulls + zero-downtime updates. Docker stays a **runtime**
> (NO Fly-style OCI→fc repack). All components are mesh peers; nothing public.
> Approved via brainstorm 2026-05-27.

## 1. Topology (every box is a mesh peer with a ULA)

```
 deploy request (repo, ref, token, tenant, app, targets)
        │
        ▼
 ┌──────────────┐  clone(token)+build(sandbox)+push   ┌──────────────────┐
 │ build-runner │ ──────────────────────────────────▶ │ tabbify-registry │
 │ (builder     │           OCI image | wasm           │  (Zot + our auth)│
 │  supervisor) │ ◀── ArtifactRef ───┐                 └────────┬─────────┘
 └──────────────┘                    │                          │ pull by ref
        ▲ BuildJob                   │ ArtifactRef              ▼
 ┌──────┴───────────┐                │                 ┌──────────────────┐
 │ deploy           │ ───────────────┴───────────────▶ │ target supervisor│
 │ control-plane    │      "deploy app X = <ref>"       │  runner: pull →  │
 │ (node / new)     │                                   │  health → swap   │
 └──────────────────┘                                   └──────────────────┘
```

- **build-runner** — a specialized supervisor whose "app" is a *build job*: clone
  → sandboxed build → push. Ephemeral or a small dedicated pool.
- **tabbify-registry** — Zot (OCI registry) wrapped with tabbify auth +
  per-tenant namespaces. Stores docker images AND wasm (as OCI artifacts).
- **target supervisor** — the existing supervisor, extended to *pull* from the
  registry and do a zero-downtime swap (no more build-from-source on the host).
- **deploy control-plane** — triggers the build, then tells the target(s) to pull
  the resulting `ArtifactRef`. Lives in the node gateway (or a small controller).

All traffic (push, pull, dispatch) rides the **mesh** (ULAs); the registry is
never publicly exposed.

## 2. End-to-end flow

1. A deploy request arrives (control-plane): `{repo, ref, github_token(short),
   tenant, app, targets}`.
2. Control-plane creates a **BuildJob** and dispatches it to a build-runner over
   the mesh.
3. build-runner: `git clone <repo>@<ref>` with the short-lived token → detect
   build type → **build in a sandbox** → tag `<tenant>/<app>:<sha>` (+ `:vN`) →
   `push` to tabbify-registry over the mesh → return the **ArtifactRef**.
4. Control-plane notifies the target supervisor(s): "deploy `<app>` = `<ref>`".
5. target supervisor → its per-app **runner**: `pull <ref>` from the registry
   (mesh) → bring up the NEW instance alongside the old → `health()` gate →
   atomic traffic flip → drain + `shutdown()` the old. **Zero downtime.**
   Rollback = deploy an older `<ref>`.

## 3. Contracts (the seams — pin these first)

- **`BuildJob`**: `{ job_id, repo_url, git_ref, github_token, tenant, app_uuid,
  build_kind: Docker|Wasm|Auto }`.
- **`ArtifactRef`**: `<registry_ula>/<tenant>/<app_uuid>:<git_sha>` (immutable) +
  a moving `:vN`. Same scheme for images and wasm artifacts.
- **registry auth**: short-lived bearer tokens — `push` scope for build-runners,
  `pull` scope for supervisors, per-tenant — minted by the auth-service / mesh
  identity.
- **deploy notification**: `POST [target_ula]:8730/v1/apps/:uuid/deploy { ref }`.

## 4. Components in detail

### 4.1 tabbify-registry (wrapper over Zot) — NEW repo `tabbify-service-registry`
- Run **Zot** (CNCF, OCI-native, lightweight, supports OCI artifacts incl. wasm).
- **Storage:** Zot's S3 backend (durable, reuses our S3) or local disk + volume;
  layer dedup + a GC policy.
- **Mesh:** the service joins the mesh (the joiner, like the supervisor) and binds
  Zot's `/v2/` API on its **peer ULA** — reachable only over the mesh.
- **Auth:** put tabbify token-validation in front of Zot (Zot's bearer/auth hooks,
  or a thin reverse-proxy in the same container) — per-tenant namespaces, short
  TTL, validated against the auth-service.
- **Packaging:** dockerized + capability-self-detect like the supervisor image.
- **Both artifact types:** docker = standard OCI image; wasm = OCI artifact
  (`oras push` with a wasm mediaType) — one `/v2` protocol for both.

### 4.2 build-runner (the "builder supervisor")
- Reuses the supervisor's mesh + lifecycle infra (decision §6); its "app" is a
  `BuildJob`, not a long-running server.
- **Clone:** short-lived GitHub installation token → `git clone --depth=1
  <repo>@<ref>` into a scratch dir. Token used once, never persisted.
- **Detect:** explicit `tabbify.toml`/manifest `build_kind`, else heuristics
  (`Dockerfile` → Docker; a wasm crate → Wasm).
- **Build — SANDBOXED (untrusted source!):**
  - the build-runner itself runs **inside an fc** (hardware boundary) — arbitrary
    `RUN`/build scripts are contained;
  - docker images: **Kaniko** or **rootless BuildKit** (build OCI without a
    privileged daemon / no host socket);
  - wasm: a sandboxed toolchain build → the `.wasm` component.
- **Tag + push:** `<tenant>/<app>:<sha>` → push to tabbify-registry (BuildKit/oras
  push) over the mesh, with a short-lived `push` token.
- **Report:** the `ArtifactRef` (+ logs) back to the control-plane.

### 4.3 supervisor — pull + deploy (extend the existing supervisor)
- **New runtime input — pull from registry:** the docker runtime gains a
  `docker pull <ref>` path; wasm gains an `oras pull <ref>` path. This sits beside
  the existing "load tar (W3) / cached image (W2) / build-from-source" decision —
  **registry pull becomes the preferred warm source**; build-from-source stays as
  the last-resort fallback (or is removed once the registry is mandatory).
- **New control endpoint:** `POST /v1/apps/:uuid/deploy { ref }` → pull `<ref>` →
  hand to the per-app runner for a **zero-downtime swap** (the runner-level
  blue-green from the contract: new instance → `health()` → atomic flip →
  drain + `shutdown()` old). The app-ULA never changes.
- The supervisor's docker daemon is configured to pull from the mesh-ULA registry
  (mesh is already encrypted → an http registry over the mesh is acceptable, or
  mesh-internal certs — §6).

### 4.4 deploy control-plane (node gateway, or a small controller)
- `POST /v1/deploy { repo, ref, token, tenant, app, targets }` → create BuildJob →
  pick a build-runner (mesh roster, `builder`-tagged) → await `ArtifactRef` →
  `POST /deploy { ref }` to each target supervisor.
- Tracks build/deploy status (queryable). Webhook receiver (git push → deploy) is
  the **git-connect wrapper**, a later phase.

## 5. Implementation phases (TDD, subagent-driven, always-green)

- **Phase 1 — tabbify-registry (new repo).** Zot + mesh-join + auth front + S3
  storage + dockerized. **Verify:** `oras push`/`docker push` an artifact to the
  registry's ULA over the mesh, then `pull` it from another peer.
- **Phase 2 — supervisor pull + `POST /deploy`.** Add the registry-pull path +
  the deploy endpoint; wire the runner zero-downtime swap. **Verify (Lima):**
  manually push an image to the registry → `POST /deploy` → supervisor pulls →
  serves; deploy a 2nd version → seamless swap; deploy old ref → rollback.
- **Phase 3 — build-runner.** Clone + sandboxed build (Kaniko/BuildKit + wasm) +
  push. **Verify (Lima):** a real repo → build-runner → registry → supervisor
  pull → run, end-to-end.
- **Phase 4 — deploy control-plane.** `/v1/deploy` → dispatch build-runner →
  notify targets; status tracking.
- **Phase 5 (later) — git-connect.** GitHub App (OAuth) + webhooks → auto-deploy
  on push (the authoring UX wrapper).

## 6. Decisions to confirm (before Phase 1)
1. **Build tool:** Kaniko vs rootless BuildKit vs DinD-in-fc. *Lean:* rootless
   BuildKit inside the fc build-runner (fast, OCI-native); Kaniko if BuildKit
   rootless fights.
2. **build-runner = supervisor "builder mode" vs a dedicated binary.** *Lean:*
   reuse the supervisor/runner infra (a `build` runtime kind) — avoids a 4th
   codebase; the mesh/lifecycle/sandbox plumbing already exists.
3. **Registry storage:** Zot + S3 backend (durable, reuses infra) vs local disk.
   *Lean:* S3 backend.
4. **Registry transport over mesh:** plain http (mesh already encrypts) vs
   mesh-internal TLS certs. *Lean:* http-over-mesh to start (registry is mesh-only,
   never public).
5. **Registry auth:** Zot bearer hooks vs a thin auth-proxy in front. *Lean:* Zot
   bearer validated against the auth-service token.

## 7. Security
- **build-runner runs untrusted source** → contained by fc + a rootless/daemonless
  builder (no host docker socket); egress restricted to clone + push.
- **registry** → per-tenant namespaces; short-lived push (builder) / pull
  (supervisor) tokens; mesh-only (no public exposure).
- **GitHub token** → short-lived installation token, used once for the clone,
  never stored.
- **isolation of the running app** is unchanged and orthogonal: the topology
  (dedicated VPS, or fc-per-client) provides tenant isolation, not docker.

## 8. What already exists (leverage)
- Mesh (join, ULAs, roster) · the supervisor + per-app runner + the lifecycle
  contract (`handle`/`health`/`watch_for_exit`/`shutdown`) — the runner-level
  zero-downtime swap (§4.3) is a direct extension · the node gateway · the
  dockerized-supervisor packaging (the registry + build-runner reuse it) · W2/W3
  docker build/cache/load (build-from-source moves to the build-runner; load
  becomes registry-pull).
