# tabbify-service-supervisor task runner (https://github.com/casey/just).
# `set dotenv-load` reads .env automatically for local runs.
# Copy .env.example to .env first: `cp .env.example .env`.

set dotenv-load

# Show the available recipes (default when you run `just`).
default:
    @just --list

# Build the supervisor.
build:
    cargo build

# Run the supervisor (joins the mesh; needs root / NET_ADMIN + /dev/net/tun).
run:
    cargo run --bin supervisord

# Run locally WITHOUT joining the mesh (loopback, no TUN). Useful for poking the
# API by hand. Override SUPERVISOR_BIND / SUPERVISOR_S3_BASE_URL via .env or env.
run-local:
    cargo run --bin supervisord -- --no-mesh --bind 127.0.0.1:8730

# Run the full test suite (unit + integration; no network — S3 is mocked).
test:
    cargo test

# Clippy with warnings denied.
lint:
    cargo clippy --all-targets -- -D warnings

# Format check (CI gate).
fmt:
    cargo fmt --check

# Format in place.
fmt-fix:
    cargo fmt

# All pre-commit gates in one shot.
check: build test lint fmt

# Cross-build a static musl binary (for scp to a Linux host, e.g. Kamatera).
# Requires the musl target: `rustup target add x86_64-unknown-linux-musl`.
build-musl:
    cargo build --release --target x86_64-unknown-linux-musl

# Cross-build a static aarch64 musl binary (for an ARM Linux host, e.g. an
# aarch64 Lima VM for Firecracker testing). On an Apple Silicon Mac the cleanest
# path is a native arm64 Linux container (no cross-linker, no emulation):
#     docker run --rm --platform linux/arm64 \
#       -v "$PWD":/work -w /work \
#       -v "$HOME/.cargo/registry":/usr/local/cargo/registry \
#       -v "$HOME/.cargo/git":/usr/local/cargo/git \
#       rust:bookworm bash -c \
#       'rustup target add aarch64-unknown-linux-musl && \
#        apt-get update -qq && apt-get install -y -qq musl-tools && \
#        cargo build --release --target aarch64-unknown-linux-musl --bin supervisord'
# Output: target/aarch64-unknown-linux-musl/release/supervisord
# On an aarch64 Linux host the bare cargo invocation below is enough (after
# `rustup target add aarch64-unknown-linux-musl` + installing musl-tools).
build-musl-aarch64:
    cargo build --release --target aarch64-unknown-linux-musl
