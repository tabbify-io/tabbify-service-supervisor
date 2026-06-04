# tabbify/buildkit-toolchain — the BUILD-VM guest image (phase 2 of the
# build/run split). Boots as an ephemeral Firecracker microVM on a builder
# node: buildkitd + runc build /scratch/src into an OCI tarball, then the VM
# powers itself off. See deploy/buildkit/entrypoint.sh for the v1 contract.
#
# Published to the mesh registry as tabbify/buildkit-toolchain:v1
# (runner/build/fc_sandbox.rs TOOLCHAIN_REPO).
FROM moby/buildkit:v0.16.0
COPY entrypoint.sh /tbf-build-entrypoint.sh
# Bake the custom mount points: the build VM attaches this rootfs READ-ONLY
# (shared, digest-cached), so the guest entrypoint CANNOT `mkdir` them at
# runtime — they must already exist in the image.
# ca-certificates: buildkit pulls base images over TLS from Docker Hub.
# resolv.conf baked in (the rootfs is mounted READ-ONLY at runtime, so the
# guest can't write it — and a tmpfs over /etc would SHADOW the CA bundle,
# breaking TLS verification of the base-image registry).
RUN chmod +x /tbf-build-entrypoint.sh \
 && mkdir -p /scratch /cache \
 && apk add --no-cache ca-certificates \
 && update-ca-certificates 2>/dev/null || true
ENTRYPOINT ["/bin/sh", "/tbf-build-entrypoint.sh"]
