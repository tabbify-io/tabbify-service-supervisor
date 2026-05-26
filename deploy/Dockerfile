# Turnkey tabbify-supervisor node image.
#
# Build context = ./deploy with the prebuilt static-musl binaries staged into
# ./deploy/bin/ (built by the release CI or `cargo zigbuild ... --bin supervisord
# --bin tabbify-runner`; bin/ is gitignored). Multi-arch: stage the matching arch
# + set DOCKER_ARCH.
#
# supervisord IS the entrypoint — it reads env (coordinator baked in,
# overridable), self-detects capabilities (/dev/kvm -> firecracker, reachable
# docker -> docker, wasm always), joins the mesh, and runs the orchestrator.
#
#   docker run -d --device /dev/net/tun --cap-add NET_ADMIN \
#     [--device /dev/kvm] [-v /var/run/docker.sock:/var/run/docker.sock] \
#     -v tbf-state:/var/lib/tabbify tabbify-supervisor
FROM debian:bookworm-slim

# docker CLI ONLY (client; talks to a mounted host socket — no daemon in-image).
ARG DOCKER_CLI_VERSION=27.3.1
ARG DOCKER_ARCH=aarch64
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates curl \
 && curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" \
      | tar xz -C /usr/local/bin --strip-components=1 docker/docker \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

COPY bin/supervisord    /usr/local/bin/supervisord
COPY bin/tabbify-runner /usr/local/bin/tabbify-runner
# Full image also bundles firecracker + a default kernel so fc apps work the
# moment /dev/kvm is present (staged into ./deploy/bin when building the full image):
# COPY bin/firecracker  /usr/local/bin/firecracker
# COPY bin/vmlinux      /opt/tabbify/vmlinux

ENV SUPERVISOR_DATA_DIR=/var/lib/tabbify
VOLUME /var/lib/tabbify

ENTRYPOINT ["/usr/local/bin/supervisord"]
