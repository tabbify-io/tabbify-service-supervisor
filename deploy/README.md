# Running the supervisor as a Docker container

Turnkey operator guide for the **`tabbify-supervisor`** image: one `docker run`
and the supervisor joins the mesh, self-detects which runtimes it can offer, and
is ready to host apps — out of the box.

- **Design + rationale + verification:** [`../docs/superpowers/specs/2026-05-26-dockerized-supervisor-node-design.md`](../docs/superpowers/specs/2026-05-26-dockerized-supervisor-node-design.md)
- **Orchestrator / runner internals:** [`../README.md`](../README.md)
- **App push + node gateway flow:** [`../../APP_LAYER_CONTRACT.md`](../../APP_LAYER_CONTRACT.md)

---

## TL;DR

```bash
docker run -d --name tbf-sup \
  --device /dev/net/tun --cap-add NET_ADMIN \    # mesh transport (always)
  -v tbf-state:/var/lib/tabbify \                # persist identity + artifact cache
  tabbify-supervisor
```

The supervisor joins the **baked-in coordinator**, advertises `wasm` (+`firecracker`
if `/dev/kvm` is passed, +`docker` if a socket is mounted), and the `tabbify-node`
gateway routes apps to it. No config file, no flags beyond what you choose to expose.

---

## 1. Build the image

> **Published image:** the release CI builds + pushes the lean image to
> `ghcr.io/tabbify-io/tabbify-supervisor` (`:x86_64`, `:aarch64`, and a
> multi-arch `:latest`) on every push to `main`. Once it has run you can
> `docker run ghcr.io/tabbify-io/tabbify-supervisor` directly. Build locally
> (below) for dev iteration or the firecracker-capable image.

The image consumes prebuilt **static-musl** binaries staged into `deploy/bin/`
(gitignored). Stage them, then build:

```bash
# from the repo root — build the two binaries (musl, per target arch)
cargo zigbuild --release --target aarch64-unknown-linux-musl \
  --bin supervisord --bin tabbify-runner

mkdir -p deploy/bin
cp target/aarch64-unknown-linux-musl/release/{supervisord,tabbify-runner} deploy/bin/

docker build -t tabbify-supervisor deploy/        # lean: wasm + (docker via socket)
```

**Full (firecracker-capable) image** — also stage `firecracker` + a guest kernel
and uncomment the two `COPY` lines in the `Dockerfile`:

```bash
cp $(which firecracker) deploy/bin/firecracker     # firecracker release binary
cp /path/to/vmlinux     deploy/bin/vmlinux         # e.g. firecracker-ci kernel
# uncomment `COPY bin/firecracker …` + `COPY bin/vmlinux …` in deploy/Dockerfile
docker build -t tabbify-supervisor:fc deploy/
```

> The release CI stages all four binaries and publishes the full per-arch image.

---

## 2. Launch — capabilities are à la carte

The machine **owner** decides what the supervisor can do by what they expose at
launch; the supervisor self-detects it (shown in `/health` + advertised as mesh
tags). It never escalates beyond what it was given.

| You pass | Supervisor offers | Detected by |
|---|---|---|
| *(nothing extra)* | `wasm` | always (in-process wasmtime) |
| `--device /dev/kvm` | `firecracker` | opens `/dev/kvm` |
| `-v /var/run/docker.sock:/var/run/docker.sock` | `docker` | `docker info` succeeds |

```bash
# wasm-only node
docker run -d --name tbf-sup --device /dev/net/tun --cap-add NET_ADMIN \
  -v tbf-state:/var/lib/tabbify tabbify-supervisor

# + firecracker (host has KVM; needs the :fc image)
docker run -d --name tbf-sup --device /dev/net/tun --cap-add NET_ADMIN \
  --device /dev/kvm \
  -v tbf-state:/var/lib/tabbify tabbify-supervisor:fc

# + docker apps (trusted use; see the DooD caveat in §6)
docker run -d --name tbf-sup --device /dev/net/tun --cap-add NET_ADMIN \
  -v /var/run/docker.sock:/var/run/docker.sock --network host \
  -v tbf-state:/var/lib/tabbify tabbify-supervisor
```

Point at a non-default coordinator with `-e TABBIFY_MESH_COORDINATOR=http://host:8888`.
Other knobs: `-e SUPERVISOR_NAME=edge-fra-1`, `-e SUPERVISOR_DATA_DIR=/var/lib/tabbify`.

---

## 3. Verify it came up

```bash
docker logs tbf-sup            # look for: "joined mesh my_ula=fd5a:… addr=[…]:8730"
                               # + capability lines (firecracker/docker/wasm-only)
```

The control API binds the peer **ULA** (`[my_ula]:8730`), reachable over the mesh
(from the node or another peer), not on host loopback. Two ways to inspect:

```bash
# (a) ask the coordinator — the supervisor should appear in the roster:
curl -s http://<coordinator>:8888/v1/mesh/peers | jq '.peers[] | {ula,display_name,tags}'

# (b) dev mode: run WITHOUT the mesh and publish the control port locally
docker run --rm -p 8730:8730 tabbify-supervisor --no-mesh --bind 0.0.0.0:8730
curl -s localhost:8730/health     # {"firecracker":…,"docker":…,"status":"ok","ula":…}
```

---

## 4. Work with it — run an app

**Production flow (via the gateway):**

1. `tcli push ./my-app` → returns an app `uuid` (artifact lands in S3). See
   [`APP_LAYER_CONTRACT.md`](../../APP_LAYER_CONTRACT.md).
2. The `tabbify-node` gateway discovers supervisors via the coordinator roster,
   routes `/app/<uuid>` to a **capable** one (matching the app's runtime to the
   supervisor's advertised tags), which spawns a `tabbify-runner` for it.
3. The runner serves the app on its **own** mesh ULA (`derive_app_ula(uuid)`);
   the node proxies to it. App survives nothing-else — it's its own peer.

**Direct / dev flow (no gateway):** pre-start an app on the supervisor itself —

```bash
docker run -d --name tbf-sup --device /dev/net/tun --cap-add NET_ADMIN \
  --device /dev/kvm -v tbf-state:/var/lib/tabbify \
  tabbify-supervisor:fc --app <uuid>
# or at runtime over the control API (on the peer ULA, from a mesh peer):
#   POST [my_ula]:8730/v1/apps/<uuid>/start | /stop | /purge
#   GET  [my_ula]:8730/v1/apps
```

---

## 5. Operate

- **Persistence:** mount a volume at `/var/lib/tabbify`. It holds the mesh
  **identity** (`mesh-identity.json` → STABLE ULA across restarts), the runner
  records (`runners/`), and the artifact cache (`apps/`). Drop the volume → the
  node re-registers as a fresh peer next start.
- **Restart:** `docker restart tbf-sup` → reloads the identity → re-claims the
  **same ULA** → re-adopts any still-running in-container runners.
- **Stop / purge an app:** `POST …/v1/apps/<uuid>/stop` (keeps the cache) or
  `…/purge` (clears cache + any docker image), over the control API.
- **Logs:** `docker logs -f tbf-sup` (orchestrator + monitor). Each runner is a
  detached process; its lifecycle shows in the monitor lines.

---

## 6. Caveats — read before production

- **Coordinator credential.** Today's E1/dev coordinators run `--insecure-no-mtls`
  and the supervisor joins plaintext. Production join (mTLS client cert / join
  token via the auth-service) is pending — the turnkey `docker run` will then add
  `-e TABBIFY_JOIN_TOKEN=…`.
- **Docker apps over the host socket (DooD).** App containers become siblings on
  the host daemon; their published ports land on the **host**, not the
  supervisor's netns, so the proxy can't reach `127.0.0.1:<port>`. Run the
  supervisor with `--network host` for socket-mount docker apps (or use DinD).
  Untrusted code should go to firecracker/wasm anyway, not raw docker.
- **Failure domain = the container.** `supervisord` is PID 1, so its death stops
  the container and every in-container runner. A `--restart` policy gives cold
  auto-recovery (re-fetch + respawn), not the host-process crash-survival. True
  crash-survival + per-tenant isolation wants runners as **sibling** microVMs/
  containers (the "runner-in-firecracker" direction).

---

## Appendix — reproduce the in-container firecracker E2E

Verified 2026-05-26 in a Lima VM with real `/dev/kvm` (a microVM boots + serves
inside a **non-privileged** container):

```bash
# in a Linux host with /dev/kvm + docker, with the :fc image built:
UUID=<firecracker-app-uuid>
python3 -m http.server 9000 --directory ./fakes3 &     # or real S3 base url

docker run -d --name tbf-fc \
  --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
  --add-host host.docker.internal:host-gateway \
  tabbify-supervisor:fc --no-mesh \
  --s3-base-url http://host.docker.internal:9000 --app "$UUID"

# the microVM serves inside the container's netns at 172.31.0.2:8080.
# NOTE: a slim image has no curl — probe with bash /dev/tcp:
docker exec tbf-fc bash -c \
  'exec 3<>/dev/tcp/172.31.0.2/8080; printf "GET / HTTP/1.0\r\n\r\n" >&3; cat <&3'
# -> 200 OK
```
