#!/bin/sh
# Build-VM guest entrypoint (v1 contract — see runner/build/fc_sandbox.rs).
#
# Drives: /dev/vda = this rootfs, /dev/vdb = scratch (src/ in, out/ + result
# out), /dev/vdc = persistent buildkit cache. The host cloned the source and
# will push the result — this guest only BUILDS. No git, no tokens, no mesh;
# network egress (NAT'd tap) is for base-image pulls only.
#
# The guest powers off when done (sysrq), which exits the firecracker
# process — that exit IS the completion signal the host waits on.

ok=false
finish() {
  echo "{\"ok\":$ok}" > /scratch/result.json 2>/dev/null
  sync
  # Mark the ext4s CLEAN on the way out: stop buildkitd, then unmount cache
  # (persistent — a dirty cache forces a host-side e2fsck/quarantine) and
  # scratch. Best-effort; the host also e2fscks the cache before reuse.
  kill "$BK_PID" 2>/dev/null
  umount /cache 2>/dev/null
  umount /scratch 2>/dev/null
  sync
  # Power off: sysrq 'o'. Fallback: exit PID1 (kernel panic also ends the VM,
  # just noisier in the console log).
  echo 1 > /proc/sys/kernel/sysrq 2>/dev/null
  echo o > /proc/sysrq-trigger 2>/dev/null
  exit 0
}
trap finish EXIT

set -x

# HOME on a WRITABLE path: the root user's default HOME is `/` (the RO
# rootfs), so buildctl's `mkdir ~/.docker` fails. Point HOME + DOCKER_CONFIG
# at the /run tmpfs (mounted below).
export HOME=/run
export DOCKER_CONFIG=/run/.docker

# The rootfs is READ-ONLY: /scratch and /cache are baked into the image
# (Dockerfile), /tmp /run /etc /sys exist in the moby/buildkit base + are
# tmpfs/sysfs-mounted here. No mkdir on the rootfs.
#
# devtmpfs populates /dev with the virtio-blk nodes: the kernel only mounts
# the ROOT device (/dev/vda) before init; the scratch (/dev/vdb) + cache
# (/dev/vdc) nodes only appear once devtmpfs is mounted over /dev.
mount -t devtmpfs none /dev 2>/dev/null || true
mount -t ext4 /dev/vdb /scratch || exit 1
mount -t ext4 /dev/vdc /cache || exit 1
mount -t tmpfs tmpfs /tmp 2>/dev/null
mount -t tmpfs tmpfs /run 2>/dev/null
# runc (buildkit's executor) wants cgroup2 (the dir exists under sysfs).
mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null

# DNS: the RO rootfs's /etc/resolv.conf is empty (Docker does NOT persist
# writes to it in an image layer), so buildkitd falls back to [::1]:53 and
# base-image pulls fail. Overlay /etc on a WRITABLE tmpfs — but copy the
# existing /etc across FIRST so the CA bundle (/etc/ssl, needed for registry
# TLS) survives — then write a working resolv.conf.
cp -a /etc /run/etc-copy
mount -t tmpfs tmpfs /etc
cp -a /run/etc-copy/. /etc/
echo "nameserver 1.1.1.1" > /etc/resolv.conf

mkdir -p /scratch/out /cache/buildkit /cache/bk

# buildkitd with its state on the persistent cache disk. The NATIVE
# snapshotter works on any kernel (no overlayfs requirement) — builds are
# already disk-backed and serialized, so the overlay speed-up is not worth a
# kernel-feature dependency.
buildkitd --root /cache/buildkit --oci-worker-snapshotter=native \
  > /scratch/out/buildkitd.log 2>&1 &
BK_PID=$!

i=0
while [ ! -S /run/buildkit/buildkitd.sock ]; do
  i=$((i + 1))
  [ "$i" -gt 100 ] && exit 1
  sleep 0.2
done

if buildctl build \
    --frontend dockerfile.v0 \
    --local context=/scratch/src \
    --local dockerfile=/scratch/src \
    --output type=oci,name=build,dest=/scratch/out/oci.tar \
    --export-cache type=local,dest=/cache/bk,mode=max \
    --import-cache type=local,src=/cache/bk \
    > /scratch/out/build.log 2>&1; then
  ok=true
fi
