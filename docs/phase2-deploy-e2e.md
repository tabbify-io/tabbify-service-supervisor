# Phase 2 — Deploy-from-Registry E2E (Lima / AWS)

Verifies the supervisor pulls an app image from the mesh registry by ref and
swaps it with **zero downtime** via `POST /v1/apps/:uuid/deploy { ref }`.

## What was built (code-complete)
- **Registry-pull source** (`docker.rs`): image priority is prebuilt-tar → **registry
  pull (`docker pull <ref>`)** → cached image → build-from-source. The ref carries the
  registry's mesh address (`[<registry_ula>]:5000/<tenant>/<app>:<sha>`).
- **Swappable runtime** (`runner/active.rs`): the runner serves an `ActiveRuntime`
  (`ArcSwap`) and a re-arming crash-watch so a post-swap old-runtime death is not a crash.
- **Zero-downtime swap** (`perform_swap`): build the new runtime → health-gate → atomic
  flip → drain + `shutdown()` the old. A failed deploy keeps the old serving (no downtime).
- **`Deploy{ref}` control msg** + **`POST /v1/apps/:uuid/deploy {ref}`** (supervisor) +
  **node passthrough + MCP tool**. The deployed ref is persisted (`RunnerHandle.image_ref`,
  `--image-ref`) so a supervisor restart respawns the same version.

## Prerequisites
1. A coordinator + a supervisor joined to the mesh, running a **docker** app `APP_UUID`.
2. The **registry** (Phase 1) on the mesh, holding two refs of the app (`V1`, `V2`) —
   push them with `docker push [<registry_ula>]:5000/<tenant>/<app>:<sha>`.
3. The supervisor host's docker daemon trusts the registry ULA — in
   `/etc/docker/daemon.json`: `{"insecure-registries": ["[<registry_ula>]:5000"]}`,
   then restart docker. (The mesh link is already encrypted; this just allows plain-http
   pull over it.)

## Run
```sh
APP_UUID=<uuid> \
APP_ULA=<app's mesh ULA> \
DEPLOY_BASE=http://[<node_ula>]:8090 \   # or the supervisor: http://[<sup_ula>]:8730
V1=[<registry_ula>]:5000/<tenant>/<app>:<sha1> \
V2=[<registry_ula>]:5000/<tenant>/<app>:<sha2> \
./scripts/deploy-rollback-smoke.sh
```

The script: deploys V1 (app serves) → runs a tight request loop while deploying V2 and
asserts **every** request returned 200 across the flip (zero downtime) → deploys V1 again
(rollback). Any non-200 during the swap fails the test.

## Notes
- **Docker-first.** wasm-from-registry (`oras pull`) and firecracker swap are follow-ups;
  fc additionally needs per-uuid taps before two fc instances can coexist on one host
  during a swap.
- During the swap window both the old and new containers run — they don't collide
  (per-launch container name `tbf-<uuid>-<seq>` + a fresh ephemeral host port).
