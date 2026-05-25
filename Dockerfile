# tabbify-service-supervisor runtime image.
#
# Phase-1 build model (contract §9/§10): the supervisor is cross-built on the
# host with `just build-musl` (the mesh dependency is a sibling path dep, so an
# in-container cargo build would need both repos in the build context). This
# Dockerfile then wraps the prebuilt STATIC musl binary in a slim runtime.
#
# Build the binary first:
#     rustup target add x86_64-unknown-linux-musl
#     just build-musl     # -> target/x86_64-unknown-linux-musl/release/supervisord
#
# Then build the image:
#     docker build -t tabbify-supervisor .
#
# RUNTIME REQUIREMENTS — the supervisor opens a TUN device to join the mesh, so
# the container needs:
#     --cap-add NET_ADMIN --device /dev/net/tun
# (in compose: `cap_add: [NET_ADMIN]`, `devices: ["/dev/net/tun:/dev/net/tun"]`,
#  and `network_mode: host` to reach the coordinator + serve on the ULA).
# Run with `--no-mesh` to skip the TUN device (loopback-only; no extra caps).

FROM debian:bookworm-slim AS runtime

# CA certificates for the outbound HTTPS S3 fetch.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Static musl binary built on the host (`just build-musl`).
COPY target/x86_64-unknown-linux-musl/release/supervisord /usr/local/bin/supervisord

# Control/serve port (over the mesh ULA in production).
EXPOSE 8730

# Configuration is via env (TABBIFY_MESH_COORDINATOR, SUPERVISOR_*, RUST_LOG).
# Pre-register apps with repeated `--app <uuid>` args.
ENTRYPOINT ["/usr/local/bin/supervisord"]
