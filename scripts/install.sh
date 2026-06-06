#!/bin/sh
# install.sh — turnkey Tabbify supervisor node for any systemd Linux
# (Debian, Ubuntu, Arch, Fedora, ...). One command on a clean machine:
#
#   curl -fsSL https://tabbify-releases-leo.s3.eu-central-1.amazonaws.com/supervisor/install | sudo sh
#
# What it does (mirrors the NixOS module nixos/tabbify-node.nix 1:1):
#   - resolves the latest release from the public bucket, downloads the
#     static musl binaries for THIS arch (x86_64 / aarch64), verifies
#     sha256 against the release manifest (x86_64)
#   - lays down the versioned layout the Rust self-update engine owns:
#     /opt/tabbify/releases/v<VER>/{supervisord,tabbify-runner},
#     top-level symlinks, atomic VERSION file
#   - fetches the Firecracker guest kernel + the firecracker/oras helper
#     binaries (microVM runtime; best-effort — the node joins the mesh
#     and serves HTTP apps even without KVM)
#   - installs systemd units: the supervisor itself plus the OTA
#     self-update timer (poll every 2 min, health-gated swap + rollback)
#   - starts everything; the supervisor joins the mesh on its own (the
#     coordinator + TLS relay endpoints are baked into the binary —
#     nothing to configure)
#
# The mesh is INSIDE the supervisor (the joiner is a library crate in
# supervisord) — installing the supervisor is all a workload node needs.
#
# Re-running upgrades: it stages the newest release and hands activation
# to the audited self-update engine (probe -> swap -> watchdog).
#
# Uninstall:
#   systemctl disable --now tabbify-supervisor tabbify-update.timer
#   rm -rf /opt/tabbify /etc/systemd/system/tabbify-*
set -eu

BASE="${TABBIFY_RELEASE_BASE_URL:-https://tabbify-releases-leo.s3.eu-central-1.amazonaws.com}"
DATA=/opt/tabbify
FC_VERSION=v1.12.0
ORAS_VERSION=1.2.3
KERNEL_SERIES=v1.12
KERNEL_IMAGE=vmlinux-6.1.128
# Phase-2 join token (optional). When set, it is persisted to a 0600 env file
# the supervisor service reads via EnvironmentFile, so a token-validating
# coordinator (AUTH_URL set) accepts this node's register and stamps its network
# + tags from the token CLAIMS. Without it, a validating coordinator rejects the
# register (401). Pass it inline:  TABBIFY_JOIN_TOKEN=<jwt> curl … | sudo sh
JOIN_TOKEN="${TABBIFY_JOIN_TOKEN:-}"
ENV_FILE="$DATA/supervisor.env"

if [ -t 1 ]; then G='\033[1;32m'; Y='\033[1;33m'; R='\033[1;31m'; N='\033[0m'; else G=''; Y=''; R=''; N=''; fi
log()  { printf "${G}==>${N} %s\n" "$*"; }
warn() { printf "${Y}warn:${N} %s\n" "$*" >&2; }
die()  { printf "${R}error:${N} %s\n" "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root:  curl -fsSL .../supervisor/install | sudo sh"
command -v curl >/dev/null 2>&1 || die "curl is required"
command -v systemctl >/dev/null 2>&1 || die "systemd is required (this installer manages systemd units)"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required (coreutils/busybox)"

ARCH=$(uname -m)
case "$ARCH" in
  x86_64) ORAS_ARCH=amd64 ;;
  aarch64|arm64) ARCH=aarch64; ORAS_ARCH=arm64 ;;
  *) die "unsupported architecture: $ARCH (x86_64 / aarch64 only)" ;;
esac

# ── resolve the latest release ──────────────────────────────────────────
MANIFEST=$(curl -fsSL "$BASE/supervisor/latest") || die "cannot fetch $BASE/supervisor/latest"
VER=$(printf '%s' "$MANIFEST" | grep -o '"latest":"[^"]*"' | cut -d'"' -f4)
[ -n "$VER" ] || die "could not resolve the latest version from the manifest"
log "Tabbify supervisor $VER ($ARCH)"

REL="$DATA/releases/$VER"
mkdir -p "$REL" "$DATA/data" "$DATA/bin"

# ── binaries (sha256-verified on x86_64; the manifest carries no
#    per-arch hashes for aarch64 yet — HTTPS + bucket trust there) ──────
fetch_bin() {
  name="$1"
  if [ -x "$REL/$name" ]; then
    log "$name $VER already present"
    return 0
  fi
  log "downloading $name $VER"
  curl -fSL -o "$REL/$name.tmp" "$BASE/supervisor/$VER/$ARCH/$name" \
    || die "download failed: $BASE/supervisor/$VER/$ARCH/$name"
  # Per-arch manifest key (`<bin>_aarch64`); the plain key holds the x86_64
  # hash. Older manifests carry no aarch64 hash — skip with a warning rather
  # than failing the install (HTTPS + bucket trust still applies).
  KEY="$name"
  [ "$ARCH" = aarch64 ] && KEY="${name}_aarch64"
  want=$(printf '%s' "$MANIFEST" | grep -o "\"$KEY\":[[:space:]]*\"[a-f0-9]*\"" | grep -o '[a-f0-9]\{64\}') || true
  if [ -n "$want" ]; then
    got=$(sha256sum "$REL/$name.tmp" | cut -d' ' -f1)
    [ "$want" = "$got" ] || die "$name sha256 mismatch (manifest $want, downloaded $got)"
  else
    warn "manifest carries no $ARCH sha256 for $name (older release) — skipping verification"
  fi
  chmod +x "$REL/$name.tmp"
  mv "$REL/$name.tmp" "$REL/$name"
}
fetch_bin supervisord
fetch_bin tabbify-runner

# ── first-install activation (the self-update engine owns the symlinks
#    + VERSION afterwards — never clobber an updated node, see the
#    2026-06-04 downgrade incident in the NixOS module) ─────────────────
if [ ! -f "$DATA/VERSION" ]; then
  ln -sfn "releases/$VER" "$DATA/current"
  ln -sfn "current/supervisord"    "$DATA/supervisord"
  ln -sfn "current/tabbify-runner" "$DATA/tabbify-runner"
  printf '{"current":"%s","previous":[]}\n' "$VER" > "$DATA/VERSION.tmp"
  mv "$DATA/VERSION.tmp" "$DATA/VERSION"
  FRESH=1
else
  log "existing install detected — staged $VER; the OTA engine will activate it"
  FRESH=0
fi

# ── Firecracker microVM runtime (best-effort: HTTP/wasm-class apps and
#    the mesh work without it; FC apps need /dev/kvm + these) ───────────
if [ ! -f "$DATA/vmlinux" ]; then
  log "fetching Firecracker guest kernel ($KERNEL_IMAGE $ARCH)"
  if curl -fSL -o "$DATA/vmlinux.tmp" \
      "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/$KERNEL_SERIES/$ARCH/$KERNEL_IMAGE"; then
    mv "$DATA/vmlinux.tmp" "$DATA/vmlinux"
  else
    warn "guest kernel download failed — Firecracker apps will not boot until $DATA/vmlinux exists"
  fi
fi
if ! command -v firecracker >/dev/null 2>&1 && [ ! -x "$DATA/bin/firecracker" ]; then
  log "fetching firecracker $FC_VERSION"
  tmp=$(mktemp -d)
  # The whole fetch+extract+install chain is ONE guarded condition:
  # under `set -e` an unguarded tar/install in the if-BODY would abort
  # the installer before the systemd units are written — a torn,
  # dead node from one flaky GitHub download. Best-effort means
  # best-effort.
  if curl -fSL -o "$tmp/fc.tgz" \
      "https://github.com/firecracker-microvm/firecracker/releases/download/$FC_VERSION/firecracker-$FC_VERSION-$ARCH.tgz" \
      && tar -xzf "$tmp/fc.tgz" -C "$tmp" \
      && install -m 0755 "$tmp/release-$FC_VERSION-$ARCH/firecracker-$FC_VERSION-$ARCH" "$DATA/bin/firecracker"; then
    :
  else
    warn "firecracker fetch failed — microVMs unavailable until firecracker is on PATH"
  fi
  rm -rf "$tmp"
fi
if ! command -v oras >/dev/null 2>&1 && [ ! -x "$DATA/bin/oras" ]; then
  log "fetching oras $ORAS_VERSION (OCI image pull, docker-less)"
  tmp=$(mktemp -d)
  if curl -fSL -o "$tmp/oras.tgz" \
      "https://github.com/oras-project/oras/releases/download/v$ORAS_VERSION/oras_${ORAS_VERSION}_linux_${ORAS_ARCH}.tar.gz" \
      && tar -xzf "$tmp/oras.tgz" -C "$tmp" oras \
      && install -m 0755 "$tmp/oras" "$DATA/bin/oras"; then
    :
  else
    warn "oras fetch failed — image-based app deploys unavailable until oras is on PATH"
  fi
  rm -rf "$tmp"
fi

# ── host capability warnings (informational, never fatal) ───────────────
[ -e /dev/kvm ] || warn "no /dev/kvm — Firecracker microVM apps cannot run on this host"
command -v ip >/dev/null 2>&1 || warn "iproute2 ('ip') missing — the mesh TUN cannot be configured; install iproute2"
command -v ip6tables >/dev/null 2>&1 || warn "ip6tables missing — the supervisor cannot self-manage firewall trust; inbound overlay connections may be filtered"
command -v mkfs.ext4 >/dev/null 2>&1 || warn "mkfs.ext4 (e2fsprogs) missing — OCI->ext4 rootfs conversion unavailable"

# ── join token → 0600 EnvironmentFile (Phase-2) ─────────────────────────
# Persist the token OUTSIDE the unit (the unit file is world-readable) so it is
# not exposed to local users, and so a re-run WITHOUT the token leaves an
# existing token intact. The unit references it via `EnvironmentFile=-` (the `-`
# makes a missing file non-fatal — a no-mesh / dev node still starts).
if [ -n "$JOIN_TOKEN" ]; then
  log "persisting join token to $ENV_FILE (0600)"
  ( umask 077; printf 'TABBIFY_JOIN_TOKEN=%s\n' "$JOIN_TOKEN" > "$ENV_FILE.tmp" )
  chmod 600 "$ENV_FILE.tmp"
  mv "$ENV_FILE.tmp" "$ENV_FILE"
elif [ -f "$ENV_FILE" ]; then
  log "keeping existing join token at $ENV_FILE"
else
  warn "no TABBIFY_JOIN_TOKEN provided — this node will register WITHOUT a join token; a token-validating coordinator will reject it (401). Re-run with TABBIFY_JOIN_TOKEN=<jwt> to fix."
fi

# ── systemd units (mirror the NixOS module semantics) ───────────────────
# NODE_NAME defaults to the host name; override with NODE_NAME=<name> in env.
NODE_NAME="${NODE_NAME:-$(uname -n)}"
log "installing systemd units (node name: $NODE_NAME)"

cat > /etc/systemd/system/tabbify-supervisor.service <<EOF
[Unit]
Description=Tabbify supervisor node
Wants=network-online.target
After=network-online.target

[Service]
# supervisord emits sd_notify(READY=1) once the control listener is bound
# and the mesh is joined — systemd treats "started" as "actually serving".
Type=notify
NotifyAccess=main
TimeoutStartSec=60
ExecStart=$DATA/supervisord
WorkingDirectory=$DATA
# Phase-2 join token (0600, written above when TABBIFY_JOIN_TOKEN was provided).
# The leading '-' makes a missing file non-fatal so a dev/no-mesh node still starts.
EnvironmentFile=-$ENV_FILE
Environment=SUPERVISOR_NAME=$NODE_NAME
Environment=SUPERVISOR_DATA_DIR=$DATA/data
Environment=SUPERVISOR_FC_KERNEL=$DATA/vmlinux
Environment=RUST_LOG=info
# $DATA/bin carries the fetched firecracker/oras helpers.
Environment=PATH=$DATA/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Version RESOLUTION lives in a tiny helper script (no systemd-vs-shell
# escaping, no jq dependency); everything past it is the audited Rust
# engine — idempotent, a no-op when already on the latest version.
cat > "$DATA/bin/tabbify-update.sh" <<EOF
#!/bin/sh
set -eu
V=\$(curl -fsSL "$BASE/supervisor/latest" | grep -o '"latest":"[^"]*"' | cut -d'"' -f4)
[ -n "\$V" ] || { echo "no desired version (manifest unreadable)"; exit 1; }
echo "self-update -> \$V (delegating to the Rust engine)"
exec $DATA/supervisord self-update --to "\$V"
EOF
chmod +x "$DATA/bin/tabbify-update.sh"

cat > /etc/systemd/system/tabbify-update.service <<EOF
[Unit]
Description=Tabbify health-gated self-update (fetch -> probe -> swap -> watchdog)
Wants=network-online.target
After=network-online.target tabbify-supervisor.service

[Service]
Type=oneshot
Environment=TABBIFY_RELEASE_BASE_URL=$BASE
Environment=TABBIFY_INSTALL_DIR=$DATA
Environment=TABBIFY_RELEASES_DIR=$DATA/releases
Environment=RUST_LOG=info
Environment=PATH=$DATA/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=$DATA/bin/tabbify-update.sh
EOF

cat > /etc/systemd/system/tabbify-update.timer <<EOF
[Unit]
Description=Poll for a new Tabbify supervisor release (OTA auto-update)

[Timer]
OnBootSec=2min
OnUnitActiveSec=2min
Unit=tabbify-update.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now tabbify-update.timer >/dev/null 2>&1
systemctl enable tabbify-supervisor.service >/dev/null 2>&1

if [ "$FRESH" = 1 ]; then
  log "starting tabbify-supervisor"
  systemctl restart tabbify-supervisor.service
else
  # Upgrade path: let the gated engine activate the staged release.
  systemctl restart tabbify-supervisor.service
  systemctl start --no-block tabbify-update.service || true
fi

# ── wait for the mesh join and report the node's overlay address ───────
log "waiting for the mesh join (up to 45s)..."
i=0
ULA=""
while [ $i -lt 45 ]; do
  ULA=$(journalctl -u tabbify-supervisor --since "-3 min" --no-pager 2>/dev/null \
    | grep -o 'my_ula=[0-9a-f:]*' | tail -1 | cut -d= -f2) || true
  if [ -n "$ULA" ] && systemctl is-active --quiet tabbify-supervisor; then
    break
  fi
  i=$((i + 3)); sleep 3
done

if [ -n "$ULA" ]; then
  log "node '$NODE_NAME' is LIVE on the mesh: $ULA"
  log "control API:  http://[$ULA]:8730/v1/apps"
  log "OTA updates:  automatic (tabbify-update.timer, every 2 min)"
else
  warn "supervisor started but the mesh join was not confirmed within 45s"
  warn "inspect:  journalctl -u tabbify-supervisor -f"
fi
