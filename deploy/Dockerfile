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
# trixie REQUIRED (not bookworm): the mesh registry lives at a bracketed
# IPv6 address ([fd5a:…]:5000/…) and bookworm's skopeo 1.9 cannot parse
# bracketed-IPv6 image references (support landed in skopeo >= 1.14 via
# distribution/reference v0.5) — the build push fails with "invalid
# reference format" on the older parser.
FROM debian:trixie-slim

# docker CLI ONLY (client; talks to a mounted host socket — no daemon in-image).
# oras CLI for pulling WASM OCI artifacts from the mesh registry (plain-http over WG).
# skopeo + oras for the docker build push (TWO steps, supervisor-side so the
# daemon never needs a mesh route): skopeo copies the built image out of the
# docker daemon into an OCI layout (local transports — skopeo cannot parse the
# registry's bracketed-IPv6 ref on ANY version), then oras pushes the layout
# to the mesh registry (oras parses such refs; same binary the run side pulls
# with).
ARG DOCKER_CLI_VERSION=27.3.1
ARG ORAS_VERSION=1.2.0
# TARGETARCH is auto-populated by buildx per platform leg (amd64 / arm64).
# BOTH tool arches derive from it IN here — passing arch build-args from CI
# proved fragile: only DOCKER_ARCH was wired, so every image shipped the
# Dockerfile-default ARM oras and the amd64 builder died with
# "Exec format error" the first time the push path actually ran oras.
ARG TARGETARCH
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) DOCKER_ARCH=x86_64 ;; \
      arm64) DOCKER_ARCH=aarch64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates curl git skopeo \
 && curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" \
      | tar xz -C /usr/local/bin --strip-components=1 docker/docker \
 && curl -fsSL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_${TARGETARCH}.tar.gz" \
      | tar xz -C /usr/local/bin oras \
 && chmod +x /usr/local/bin/oras \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*
# `git` stays in the image: the builder runner shells out to `git clone` in
# the build pipeline (see runner::build::clone). Curl is removed after the
# one-time CLI downloads above; git is permanent.

COPY bin/supervisord    /usr/local/bin/supervisord
COPY bin/tabbify-runner /usr/local/bin/tabbify-runner
# FULL (fc-capable) image: stage `firecracker` + a default kernel into
# ./deploy/bin and uncomment the COPYs below — fc apps then work the moment
# /dev/kvm is present. VERIFIED in Lima 2026-05-26: a microVM boots + serves
# 200 OK inside a NON-privileged container (`--device /dev/kvm --device
# /dev/net/tun --cap-add NET_ADMIN`, no --privileged). CI stages all four
# binaries for the published image.
# COPY bin/firecracker  /usr/local/bin/firecracker
# COPY bin/vmlinux      /opt/tabbify/vmlinux

ENV SUPERVISOR_DATA_DIR=/var/lib/tabbify
VOLUME /var/lib/tabbify

ENTRYPOINT ["/usr/local/bin/supervisord"]
