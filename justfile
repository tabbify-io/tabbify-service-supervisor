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
