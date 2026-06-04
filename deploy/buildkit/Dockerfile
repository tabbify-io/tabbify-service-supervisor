# tabbify/buildkit-toolchain — the BUILD-VM guest image (phase 2 of the
# build/run split). Boots as an ephemeral Firecracker microVM on a builder
# node: buildkitd + runc build /scratch/src into an OCI tarball, then the VM
# powers itself off. See deploy/buildkit/entrypoint.sh for the v1 contract.
#
# Published to the mesh registry as tabbify/buildkit-toolchain:v1
# (runner/build/fc_sandbox.rs TOOLCHAIN_REPO).
FROM moby/buildkit:v0.16.0
COPY entrypoint.sh /tbf-build-entrypoint.sh
RUN chmod +x /tbf-build-entrypoint.sh
ENTRYPOINT ["/bin/sh", "/tbf-build-entrypoint.sh"]
