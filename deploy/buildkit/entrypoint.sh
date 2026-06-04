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
  mkdir -p /scratch 2>/dev/null
  echo "{\"ok\":$ok}" > /scratch/result.json 2>/dev/null
  sync
  # Power off: sysrq 'o'. Fallback: exit PID1 (kernel panic also ends the VM,
  # just noisier in the console log).
  echo 1 > /proc/sys/kernel/sysrq 2>/dev/null
  echo o > /proc/sysrq-trigger 2>/dev/null
  exit 0
}
trap finish EXIT

set -x

# Mounts the minimal microVM init doesn't provide.
mkdir -p /scratch /cache /tmp /run
mount /dev/vdb /scratch || exit 1
mount /dev/vdc /cache || exit 1
mount -t tmpfs tmpfs /tmp 2>/dev/null
mount -t tmpfs tmpfs /run 2>/dev/null
# runc (buildkit's executor) wants cgroup2.
mkdir -p /sys/fs/cgroup
mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null

# DNS for base-image pulls (the kernel ip= config set the iface + route).
echo "nameserver 1.1.1.1" > /etc/resolv.conf

mkdir -p /scratch/out /cache/buildkit /cache/bk

# buildkitd with its state on the persistent cache disk. The NATIVE
# snapshotter works on any kernel (no overlayfs requirement) — builds are
# already disk-backed and serialized, so the overlay speed-up is not worth a
# kernel-feature dependency.
buildkitd --root /cache/buildkit --oci-worker-snapshotter=native \
  > /scratch/out/buildkitd.log 2>&1 &

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
