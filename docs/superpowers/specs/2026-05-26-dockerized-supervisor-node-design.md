# Dockerized Supervisor Node — Design

> **Goal:** one command launches a `tabbify-supervisor`, it auto-finds the
> coordinator, joins the mesh, self-detects which runtimes it can offer, and is
> ready to run apps — **out of the box**. Dockerized now; a backend-agnostic
> `node.toml` lets the same node be launched in Docker (today) or Firecracker
> (later). Approved via brainstorming 2026-05-26.
>
> **This is production**, not RnD. Most of the behavior already exists in the
> supervisor (capability detection, zero-config coordinator, sticky identity) —
> this spec is mostly **packaging + a launcher**, not new core logic.

## 1. Responsibility model (the security boundary)
- **The machine owner** decides HOW the supervisor is launched (Docker vs
  Firecracker, whether `/dev/kvm` / the docker socket / `--privileged` are
  given). The supervisor does NOT police its own launch — it adapts to what it
  was given. "How I was started" is not the supervisor's concern.
- **The supervisor** is responsible for NOT leaking its launch privileges into
  the apps it runs: its app `docker run` is built from a fixed template and
  never passes the host socket / host mounts / `--privileged` / devices into an
  app container. (Already true — `docker.rs::run_args` is fixed.) So nothing
  leaks even though we are not adding extra restrictions yet.
- **The platform** isolates UNTRUSTED code by RUNTIME choice (untrusted →
  firecracker microVM / wasm; docker is for trusted/self-hosted). This is a
  scheduling policy (future), independent of how a supervisor is launched.

## 2. Turnkey UX (the centerpiece)
```bash
# Minimal — wasm-only node, auto-joins the baked-in coordinator:
docker run -d --name tbf-sup \
  --device /dev/net/tun --cap-add NET_ADMIN \
  ghcr.io/tabbify-io/tabbify-supervisor

# + Firecracker (host has KVM):
  --device /dev/kvm

# + Docker apps (owner opts in by exposing docker):
  -v /var/run/docker.sock:/var/run/docker.sock

# Persist the node's mesh identity (sticky ULA) + runner state across restarts:
  -v tbf-state:/var/lib/tabbify
```
That's it. The supervisor starts → joins the mesh at the **baked-in coordinator**
→ advertises the capabilities it detected → the `tabbify-node` gateway discovers
it via the coordinator roster and routes apps to it. No config required.

## 3. What's ALREADY in the supervisor (leverage, don't rebuild)
- **Zero-config coordinator**: `config.rs::DEFAULT_COORDINATOR_URL` is baked
  (prod EIP `http://3.124.69.92:8888`), overridable via `TABBIFY_MESH_COORDINATOR`.
  → "finds the coordinator" needs nothing.
- **Capability auto-detection**: `kvm_available()` → `firecracker`;
  `docker_available()` (runs `docker info`) → `docker`; wasm always. Each lights
  a mesh tag + shows in `GET /health` (`{firecracker,docker}`). → "figures out
  what it can do" needs nothing.
- **Sticky identity**: the joiner persists keypair+ULA via `identity_path`
  (Phase 0). → the node keeps its ULA across restarts if `/var/lib/tabbify` is a
  volume.
- **Orchestrator**: spawns detached `tabbify-runner`s, survives its own crash,
  re-adopts on restart. → resilient by design.

## 4. The Docker image (thin consumer)
`Dockerfile` (multi-stage; base `debian:bookworm-slim`):
- COPY the prebuilt musl binaries `supervisord` + `tabbify-runner` (per-arch,
  from the release CI / S3).
- INSTALL: `iproute2` (mesh TUN + fc taps), `docker` **CLI only** (to talk to a
  mounted socket — NOT the daemon), `ca-certificates`.
- BUNDLE for firecracker: the `firecracker` binary + a default `vmlinux` at
  `/opt/tabbify/vmlinux` (so fc apps work the moment `/dev/kvm` is present).
- `ENTRYPOINT ["/usr/local/bin/supervisord"]` (see §5). EXPOSE nothing (mesh is
  the data plane; control is the per-app-ULA listeners).
- `VOLUME /var/lib/tabbify` (identity + runner records + artifact cache).
- Built per-arch (x86_64 + aarch64) by the existing dual-arch release CI;
  published to a registry (e.g. `ghcr.io/tabbify-io/tabbify-supervisor`).

## 5. Entrypoint behavior
The container runs `supervisord` directly (it already does the right thing):
1. init tracing; read config from env (coordinator baked, overridable).
2. detect capabilities (kvm/docker) → log them + set the mesh tags.
3. join the mesh with `identity_path=/var/lib/tabbify/node-id.json` (sticky ULA),
   `display_name` from `--name`/env or the hostname.
4. spawn `run_monitor` (re-adopt living runners from `/var/lib/tabbify/runners`,
   then the monitor loop).
5. serve the control API on the peer-ULA.
No bespoke shell entrypoint needed beyond `exec supervisord` — the binary is the
entrypoint. (A tiny wrapper only if we need to e.g. `modprobe` or fix TUN perms.)

## 6. `node.toml` — backend-agnostic node descriptor
For owners who want a declarative "describe a node" instead of a raw `docker
run`. Symmetric with the app manifest (app → runner picks runtime; node →
launcher picks backend):
```toml
[node]
name = "edge-fra-1"

[backend]
type = "docker"                 # docker | firecracker

[artifact]
image  = "ghcr.io/tabbify-io/tabbify-supervisor:latest"   # backend=docker
# rootfs = "supervisor.ext4"; kernel = "vmlinux"          # backend=firecracker

[capabilities]                  # which runtimes this node should offer
firecracker = true              # docker → --device /dev/kvm ; fc → nested KVM
docker      = true              # docker → mount socket / DinD
# wasm — always

[mesh]
coordinator   = "http://3.124.69.92:8888"   # default baked; override here
identity_path = "/var/lib/tabbify/node-id.json"

[resources]
cpus = 4
memory_mb = 8192
```

## 7. `tabbify node up` — the launcher
A small command (new subcommand; see Open Questions for where it lives) that
materializes a `node.toml` (or flags) into a running node:
- **`backend=docker`** → builds + runs:
  `docker run -d --name <node.name> --device /dev/net/tun --cap-add NET_ADMIN
   [--device /dev/kvm if cap.firecracker] [-v /var/run/docker.sock:… if cap.docker]
   -v <state-vol>:/var/lib/tabbify -e TABBIFY_MESH_COORDINATOR=<mesh.coordinator>
   <artifact.image>`.
- **`backend=firecracker`** → boots a microVM from `artifact.rootfs` +
  `artifact.kernel` with the supervisor inside, the same env, `vcpus`/`mem` from
  `[resources]`, a tap for the mesh. (See §8 for the nested-fc caveat.)
The launcher is pure plumbing — it does NOT change supervisor behavior; it just
encodes the owner's launch decision (§1).

## 8. Backend = firecracker (the future node-in-microVM)
- A node launched as a Firecracker microVM isolates the supervisor itself
  (defense in depth for prod multi-tenant).
- **Nested-virt caveat**: a fc-NODE that wants to run fc-APPS needs **nested
  KVM** inside the node VM (microVM-in-microVM). On metal / nested-virt-capable
  cloud this works; otherwise a fc-node offers only wasm/docker and fc-apps land
  on bare-metal nodes. The `capabilities.firecracker` flag + the host's nested
  support decide; the supervisor's `kvm_available()` already gates it correctly
  inside the VM.

## 9. Networking notes (validate during build)
- Mesh needs `/dev/net/tun` + `NET_ADMIN`. Each spawned runner is its OWN mesh
  peer with its OWN WG TUN → **multiple TUNs in one container netns** (supervisor
  + N runners). Expected to work (distinct `/128` routes) but MUST be validated
  on the real image.
- **fc-tap allocation bug (carry-over follow-up)**: firecracker tap + /30 derive
  from a per-PROCESS `VM_SEQ` (each runner process resets to 0 → all use
  `fc-tap0`/`172.31.0.2`) → multiple fc runners on one node COLLIDE. Fix: derive
  the tap name + /30 from the app uuid. **Required before >1 fc app per node.**

## 10. Security (model is set; enforcement staged)
Per §1 the model is sound for prod without new restrictions NOW, because the
supervisor's app-run logic already doesn't leak privileges into apps. The
elaborate enforcement (manifest host-knob allowlist, container hardening flags
`--cap-drop ALL`/`--read-only`/`--pids-limit`, the negative-test invariant that
`run_args` never emits the socket/privileged/devices, the untrusted→firecracker
scheduling policy) is **designed and tracked**, to be enforced **before opening
docker apps to untrusted tenants**. Until then: untrusted code → firecracker/wasm
(real isolation); docker → trusted/self-hosted.

## 11. Open questions
1. **Launcher home**: a new `tabbify` CLI subcommand (`tabbify node up`) vs part
   of `tcli`. Leaning: a small standalone (or a `node` subcommand) — `tcli` is
   the app-push tool, conceptually distinct from node provisioning.
2. **Registry**: `ghcr.io/tabbify-io/...` (GitHub Container Registry, fits the
   existing GH setup) vs ECR. Leaning ghcr (public pulls, no AWS creds to pull).
3. **Default vmlinux in the image**: bundle one (fc works instantly) vs fetch on
   first fc-app. Leaning bundle (turnkey).
4. **TUN/devices in restricted hosts**: some hosts need `--privileged` or extra
   setup for `/dev/net/tun`; document the minimal caps.

## 12. Out of scope (designed-for, later)
- Untrusted-tenant docker hardening + scheduling policy (§10) — enforce before
  public untrusted docker.
- Firecracker backend launcher (§8) — sketch now, build when a fc-node is needed.
- Auto-scaling / a control plane that launches nodes from `node.toml` fleets.

## 13. Testing
- **Local (no CI needed)**: `docker build` the image on the Mac; `docker run` it
  `--no-mesh`? no — run it pointed at a local/real coordinator; assert `/health`
  reports the detected caps; with `--device /dev/kvm` (Linux/Lima) assert a fc
  app runs through the orchestrated runner; with the socket mounted assert a
  docker app runs. Reuse the `kvmcheck` Lima VM for the KVM path.
- **CI**: build + push the per-arch image (extends the release workflow).
- The crash-survival + orchestration are already proven (per-app-runner E2E).
