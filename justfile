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

# Cross-build static musl binaries via cargo-zigbuild (matches CI). zig supplies
# the cross-linker + C toolchain, so BOTH arches build from ANY host (macOS or
# Linux, x86_64 or arm64) — no native ARM runner, no container, no QEMU.
# One-time setup: install zig (`brew install zig`) + `cargo install
# cargo-zigbuild`, and `rustup target add <triple>`.
build-musl:
    cargo zigbuild --release --target x86_64-unknown-linux-musl --bin supervisord

# aarch64 static musl (for an ARM Linux host, e.g. the aarch64 Lima VM used for
# Firecracker testing). Output: target/aarch64-unknown-linux-musl/release/supervisord
build-musl-aarch64:
    cargo zigbuild --release --target aarch64-unknown-linux-musl --bin supervisord
