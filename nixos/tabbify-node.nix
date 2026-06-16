# tabbify-node.nix — turnkey Tabbify supervisor node for a clean NixOS machine.
#
# WHAT IT DOES (no developer needed, no AWS account needed):
#   - loads the kernel modules the platform needs (TUN for the mesh, KVM for Firecracker)
#   - installs the host tools the runtimes shell out to (firecracker, iproute2, busybox…)
#   - downloads the supervisor binaries + a Firecracker guest kernel on first boot
#     (anonymous HTTPS — no credentials)
#   - runs the supervisor as a managed systemd service: starts on boot, auto-restarts
#     on crash, joins the mesh automatically
#   - runs a tiny local file server so apps can be staged and started on this node
#
# INSTALL: see the Obsidian vault → "Knowledge Base/Deployment/12 - NixOS node
#          install (turnkey)" (copy this file to /etc/nixos/, add one import line,
#          run `sudo nixos-rebuild switch`).
{ config, pkgs, lib, ... }:

let
  ##########################################################################
  ##  EDIT THIS ONE LINE: a unique, human name for this machine.          ##
  ##  It is how the node shows up in the fleet (e.g. "thinkpad").         ##
  nodeName = "thinkpad";
  ##########################################################################

  # Fixed download locations (anonymous, public read — no AWS account).
  releaseBase = "https://tabbify-releases-leo.s3.eu-central-1.amazonaws.com";
  kernelUrl   = "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.12/x86_64/vmlinux-6.1.128";
  dataDir     = "/opt/tabbify";

  # On-host versioned layout (persistent, OUTSIDE the Nix store — this module
  # manages the systemd UNIT, never the binaries as derivations). The binaries
  # live under releases/v<VER>/{supervisord,tabbify-runner}; the top-level
  # /opt/tabbify/{supervisord,tabbify-runner} are symlinks into a release, and an
  # atomic VERSION file records {current, previous[], pending_confirm?}.
  #
  #   tabbify-fetch (svc #4)  bootstraps the FIRST release: lays down the binaries,
  #                           points `current` + the top-level symlinks at it,
  #                           writes VERSION.
  #   tabbify-update (svc #5b) resolves the desired version, then delegates the
  #                           WHOLE fetch -> probe -> atomic swap to the audited
  #                           Rust engine (`supervisord self-update --to <ver>`),
  #                           which re-points the top-level binary symlinks +
  #                           rotates VERSION directly. The post-restart watchdog
  #                           (inside supervisord) then confirms or rolls back.
  #
  # NOTE: after a self-update swap the top-level binary symlinks point straight
  # at releases/<ver>/<bin> (the Rust engine does not maintain `current`), so
  # `current` reflects only the first-boot bootstrap. The supervisor ExecStart
  # targets the top-level symlinks, which the engine keeps correct.
  #
  # This keeps NixOS pure (no mutable binary in the store) while letting the
  # node self-update under health gating without a `nixos-rebuild`.
  arch        = "x86_64";                 # matches the kvm-intel/x86_64 host
  bootVersion = "v1.4.0";                 # first release pinned for first boot
  releasesDir = "${dataDir}/releases";    # releases/v<VER>/{supervisord,tabbify-runner}
  currentLink = "${dataDir}/current";     # -> releases/<current>
  versionFile = "${dataDir}/VERSION";     # atomic {current, previous[]}
  meshDir     = "${dataDir}/mesh";        # mesh/v<VER>/tabbify-mesh (on-host joiner)

  # Fail-safe on-host joiner self-update, run as the supervisor's ExecStartPre
  # (svc #5). It fetches the desired standalone `tabbify-mesh` joiner from
  # mesh/v<VER>/<arch>/ but promotes it ONLY if it passes its own `--version`
  # self-check, so a broken joiner can NEVER become `current`:
  #   - broken candidate + a working prior joiner  -> keep prior (exit 0)
  #   - broken candidate + NO prior joiner at all   -> exit 1 -> ExecStartPre
  #     fails -> the unit (Type=notify, Restart=on-failure) fails to start ->
  #     the host is left exactly as it was (no swap onto a broken joiner).
  # The supervisord binary carries the joiner in-process (mesh-joiner crate);
  # this standalone artifact is the canonical host joiner for CLI/diagnostics.
  supervisorFetchJoiner = pkgs.writeShellScript "tabbify-supervisor-fetch-joiner" ''
    set -eu
    arch="${arch}"
    manifest="$(${pkgs.curl}/bin/curl -fsSL "${releaseBase}/mesh/latest" || echo '{}')"
    desired="''${TABBIFY_MESH_VERSION:-$(printf '%s' "$manifest" | ${pkgs.jq}/bin/jq -r '.latest // empty')}"
    # No desired (offline / no manifest): keep whatever is current. Fail-safe.
    [ -n "$desired" ] || { echo "no desired mesh version; keeping current joiner"; exit 0; }

    rel="${meshDir}/$desired"
    if [ ! -x "$rel/tabbify-mesh" ]; then
      ${pkgs.coreutils}/bin/mkdir -p "$rel"
      ${pkgs.curl}/bin/curl -fSL -o "$rel/tabbify-mesh" \
        "${releaseBase}/mesh/$desired/$arch/tabbify-mesh" \
        || ${pkgs.curl}/bin/curl -fSL -o "$rel/tabbify-mesh" "${releaseBase}/mesh/tabbify-mesh"
      ${pkgs.coreutils}/bin/chmod +x "$rel/tabbify-mesh"
    fi

    # Self-check the candidate BEFORE promoting it. A binary that cannot even
    # report its version is broken -> refuse to promote.
    if "$rel/tabbify-mesh" --version >/dev/null 2>&1; then
      ${pkgs.coreutils}/bin/ln -sfn "mesh/$desired/tabbify-mesh" "${dataDir}/tabbify-mesh"
      echo "promoted joiner $desired"
    else
      echo "candidate joiner $desired failed --version self-check"
      # If there is NO working prior joiner at all, fail hard so the unit does
      # not start onto a host with a broken joiner.
      [ -x "${dataDir}/tabbify-mesh" ] || { echo "and no prior joiner present -> refusing to start"; exit 1; }
      echo "keeping prior joiner (fail-safe)"
    fi
  '';
in {
  # 1. Kernel modules.  kvm-intel = Intel CPU (Core i7).  On an AMD machine,
  #    change "kvm-intel" to "kvm-amd".
  boot.kernelModules = [ "tun" "kvm-intel" ];

  # 2. Host tools the supervisor and the runtimes invoke.
  #    FC rootfs conversion is DOCKER-LESS: oras pulls the image as an OCI
  #    layout, the supervisor untars layers (tar, via busybox) and runs
  #    mkfs.ext4 (e2fsprogs). No docker daemon is required on an FC node.
  environment.systemPackages = with pkgs; [
    firecracker   # the Firecracker VMM
    e2fsprogs     # mkfs.ext4 (building app rootfs images, docker-less FC path)
    busybox       # static busybox: minimal rootfs + `tar` for OCI layer unpack
    iproute2      # `ip` — the mesh + Firecracker tap networking shells out to it
    oras          # pulls WASM artifacts AND OCI images (docker-less FC) from the mesh registry
    curl jq python3 cacert
  ];

  # 3. Open the WireGuard listen port (helps NAT traversal to the public coordinator).
  networking.firewall.allowedUDPPorts = [ 51820 ];

  # 3b. Trust the mesh overlay TUN interfaces.
  #
  #     Decrypted overlay traffic arrives INBOUND on a tun* device (a per-app
  #     runner's TUN, the supervisor's TUN, ...) destined for a local overlay
  #     /128 — e.g. an app listener on [app_ula]:8730. The default nixos-fw
  #     chain only accepts lo / ESTABLISHED / a handful of ports, so a NEW
  #     inbound connection arriving over the mesh (a peer dialing an app) was
  #     REFUSED before it ever reached the listener: the runner decapsulated
  #     the SYN and wrote it to its TUN, the firewall dropped it, no SYN-ACK
  #     was ever generated, and the public `/app/<uuid>` request hung at 000.
  #
  #     The overlay is the trust boundary: traffic only reaches a tun* device
  #     after WireGuard authenticated the sending peer, and the joiner enforces
  #     the per-peer source allowed-set on RX (spec §5.5). So accept everything
  #     arriving on the mesh TUNs. Names are dynamic (tun0/tun1/tunN, one per
  #     runner), hence the `tun+` wildcard. The overlay is IPv6-only (ULAs);
  #     the v4 rule is added for symmetry and is harmless.
  #
  #     UNCONDITIONAL insert, deliberately no `-C` pre-check: iptables-nft
  #     1.8.11 false-positives `-C` with iface matches (observed live: the
  #     check exits 0 for rules that are NOT in the chain), which silently
  #     skipped this insert. No duplicate risk anyway — extraCommands runs
  #     right after the firewall script rebuilt `nixos-fw` from scratch, so
  #     the rule can never pre-exist at this point.
  networking.firewall.extraCommands = ''
    ip6tables -I nixos-fw 1 -i tun+ -j nixos-fw-accept
    iptables  -I nixos-fw 1 -i tun+ -j nixos-fw-accept
  '';
  networking.firewall.extraStopCommands = ''
    ip6tables -D nixos-fw -i tun+ -j nixos-fw-accept 2>/dev/null || true
    iptables  -D nixos-fw -i tun+ -j nixos-fw-accept 2>/dev/null || true
  '';

  # 4. First-boot fetch of the FIRST versioned release + Firecracker kernel.
  #    Lays down releases/v<VER>/{supervisord,tabbify-runner}, points the
  #    `current` symlink + the top-level binary symlinks at it, and writes an
  #    atomic VERSION file. Idempotent: skips a release dir / kernel already
  #    present. Later updates are owned by tabbify-update (svc #5b), NOT this
  #    oneshot. To force a re-bootstrap of the first release:
  #      sudo rm -rf /opt/tabbify/releases /opt/tabbify/VERSION \
  #                  /opt/tabbify/{current,supervisord,tabbify-runner}
  #      sudo systemctl restart tabbify-fetch tabbify-supervisor
  systemd.services.tabbify-fetch = {
    description = "Bootstrap first Tabbify release + Firecracker kernel";
    wantedBy = [ "multi-user.target" ];
    before   = [ "tabbify-supervisor.service" ];
    after    = [ "network-online.target" ];
    wants    = [ "network-online.target" ];
    path     = [ pkgs.curl pkgs.coreutils ];
    serviceConfig = { Type = "oneshot"; RemainAfterExit = true; };
    script = ''
      set -eu
      mkdir -p ${releasesDir}/${bootVersion} ${dataDir}/srv ${dataDir}/data
      cd ${dataDir}

      rel="${releasesDir}/${bootVersion}"
      # Fetch the FIRST release's binaries (versioned key; legacy key as fallback).
      if [ ! -x "$rel/supervisord" ]; then
        curl -fSL -o "$rel/supervisord" \
          "${releaseBase}/supervisor/${bootVersion}/${arch}/supervisord" \
          || curl -fSL -o "$rel/supervisord" "${releaseBase}/supervisor/${arch}/supervisord"
        chmod +x "$rel/supervisord"
      fi
      if [ ! -x "$rel/tabbify-runner" ]; then
        curl -fSL -o "$rel/tabbify-runner" \
          "${releaseBase}/supervisor/${bootVersion}/${arch}/tabbify-runner" \
          || curl -fSL -o "$rel/tabbify-runner" "${releaseBase}/supervisor/${arch}/tabbify-runner"
        chmod +x "$rel/tabbify-runner"
      fi

      # FIRST-BOOT ONLY: point `current` + the top-level binary symlinks at
      # the bootstrap release and write the initial VERSION. The whole block
      # is guarded on the VERSION file — once a node has self-updated, the
      # top-level symlinks point at releases/v<NEWER>/<bin> and re-running
      # this oneshot (any `nixos-rebuild switch` restarts it) MUST NOT
      # clobber them back to ${bootVersion}. That exact clobber happened
      # live on 2026-06-04: a rebuild restarted tabbify-fetch, the symlinks
      # snapped back to v1.4.0, and the node silently downgraded — v1.4.0
      # even predates the `self-update` subcommand, so OTA could not
      # recover it without manual symlink surgery.
      if [ ! -f "${versionFile}" ]; then
        # `ln -sfn` replaces an existing symlink in place (-n: do not
        # dereference an existing dir-symlink) — the portable atomic swap.
        ln -sfn "releases/${bootVersion}" "${currentLink}"
        ln -sfn "current/supervisord"    "${dataDir}/supervisord"
        ln -sfn "current/tabbify-runner" "${dataDir}/tabbify-runner"
        printf '{"current":"%s","previous":[]}\n' "${bootVersion}" > "${versionFile}.tmp"
        mv -f "${versionFile}.tmp" "${versionFile}"
      fi

      [ -f vmlinux ] || curl -fSL -o vmlinux ${kernelUrl}
    '';
  };

  # 5. The supervisor itself — managed, auto-restart, starts on boot.
  #    Runs as root: it opens a host TUN device (CAP_NET_ADMIN), creates
  #    Firecracker taps, and opens /dev/kvm. The coordinator address is already
  #    baked into the binary (production EIP) — nothing to configure.
  systemd.services.tabbify-supervisor = {
    description = "Tabbify supervisor node";
    wantedBy = [ "multi-user.target" ];
    after    = [ "tabbify-fetch.service" "network-online.target" ];
    wants    = [ "network-online.target" ];
    requires = [ "tabbify-fetch.service" ];
    # tools the supervisor execs at runtime must be on its PATH (oras + tar(busybox) + mkfs.ext4 = docker-less FC);
    # curl + jq back the ExecStartPre joiner self-update (svc #5 / NX-4);
    # iptables for per-tap guest-egress NAT (MASQUERADE/FORWARD) set up after the
    # FC tap so the in-VM node can reach the public mesh coordinator (B3);
    # git clones the source on the HOST when this node is a `builder` (the
    # FC-build sandbox clones host-side so the token never enters the guest):
    path = [ pkgs.firecracker pkgs.iproute2 pkgs.iptables pkgs.busybox pkgs.e2fsprogs pkgs.oras pkgs.coreutils pkgs.curl pkgs.jq pkgs.git ];
    environment = {
      SUPERVISOR_NAME        = nodeName;
      SUPERVISOR_DATA_DIR    = "${dataDir}/data";
      SUPERVISOR_S3_BASE_URL = "http://127.0.0.1:9000";   # local app-staging server (svc #6)
      SUPERVISOR_FC_KERNEL   = "${dataDir}/vmlinux";
      # FC build-mode + builder role baked into the unit so a rebooted node
      # comes back as an FC-builder (ephemeral OCI->ext4 FC build sandbox)
      # without any post-boot manual `systemctl set-environment`. The tools the
      # build path execs (firecracker, oras, git, mkfs.ext4/debugfs from
      # e2fsprogs) are on the service `path` above.
      SUPERVISOR_FC_BUILD    = "true";
      SUPERVISOR_BUILDER     = "true";
      # oras (docker-less registry I/O) aborts "$HOME is not defined" without a
      # HOME; a systemd unit has none by default. The binary self-defaults this
      # too, but set it here so every exec'd tool has it.
      HOME                   = "/root";
      # This node is RELAY-ONLY: it sits behind a home NAT and every EC2 peer
      # drops inbound UDP 51820, so a direct WireGuard endpoint can never land.
      # Declaring relay_only makes the coordinator suppress this peer's reflexive
      # direct endpoint AND any hole-punch directives for pairs involving it, so
      # the WG handshake completes single-sided over the DERP relay instead of
      # thrashing on unreachable direct dials. The supervisor reads this env and
      # forwards `--mesh-relay-only` to every runner it spawns (which share this
      # host's NAT/firewall), so the whole node converges over the relay.
      TABBIFY_MESH_RELAY_ONLY = "true";
      # Always capture the Firecracker serial console: runners inherit this env
      # and append guest console output to <data_dir>/fc/<uuid>.console.log
      # (src/firecracker/linux.rs::console_stdio) — without it a spawn failure
      # inside the guest is invisible (the 500 only carries the top-level error).
      SUPERVISOR_FC_DEBUG    = "1";
      RUST_LOG               = "info";
    };
    serviceConfig = {
      # SU-1 (tabbify-service-supervisor): supervisord emits sd_notify(READY=1)
      # EXACTLY ONCE, after the control listener is bound and (unless --no-mesh)
      # the mesh is joined — i.e. once it can actually serve on the sticky ULA.
      # So systemd treats "started" as "actually serving", and TimeoutStartSec
      # bounds that bind+join window. SHIPPED: supervisord calls sd_notify; this
      # unit must stay `Type = "notify"` (do NOT downgrade to "exec"). The
      # readiness emission lives in the binary (`readiness::notify_ready`), not
      # here, and is best-effort (no-op off systemd).
      Type             = "notify";
      NotifyAccess     = "main";
      # SU-2: app runners are `setsid`-detached so they OUTLIVE a supervisord
      # restart (OTA self-update) — the crash-survival contract. But `setsid`
      # escapes the process GROUP, NOT the systemd CGROUP, so the default
      # `KillMode=control-group` SIGKILLs the WHOLE cgroup (every runner + its
      # Firecracker child) on each `systemctl restart`. The new supervisord then
      # sees the runners dead → respawns them → a dev-FC warm-restores its
      # snapshot (taken mid-/workspace-clone) → /workspace is LOST. `process`
      # signals ONLY the main supervisord on stop, leaving the detached runners +
      # FCs alive so startup `readopt()` ADOPTS them untouched (no respawn, no
      # restore). `Delegate` gives the unit its own cgroup subtree so systemd
      # does not reap the surviving children. Regular-app on_request fast-wake is
      # unaffected (warm-restore still works on a genuine cold respawn).
      KillMode         = "process";
      Delegate         = true;
      # The probe/start gate ceiling from the spec (§4): a candidate that does
      # not reach READY within 60s is killed by systemd -> rollback territory.
      TimeoutStartSec  = 60;
      # Fail-safe on-host joiner self-update at boot (NX-4): a broken joiner
      # makes this exit non-zero -> the unit fails to start -> host left as-is.
      ExecStartPre     = "${supervisorFetchJoiner}";
      ExecStart        = "${dataDir}/supervisord";   # symlink -> current/supervisord (svc #4)
      WorkingDirectory = dataDir;
      # Phase-2 join token. A token-validating coordinator (AUTH_URL set) requires
      # this node to present a join token on register; the coordinator then stamps
      # the node's network + tags from the token CLAIMS. Kept OUT of the Nix store
      # (no secret in git / world-readable store): drop it out-of-band into
      #   /etc/tabbify/supervisor.env   ->   TABBIFY_JOIN_TOKEN=<jwt>   (chmod 600)
      # before `nixos-rebuild switch`. The leading '-' makes a missing file
      # non-fatal, so a dev / no-mesh node still starts.
      EnvironmentFile  = "-/etc/tabbify/supervisor.env";
      # on-failure (NOT always): a clean exit during a watchdog rollback must
      # NOT auto-respawn the just-swapped-out binary. Exponential-ish backoff
      # (RestartSec grows over RestartSteps up to RestartMaxDelaySec) avoids a
      # tight crash loop on a broken release.
      Restart            = "on-failure";
      RestartSec         = 3;
      RestartSteps       = 5;
      RestartMaxDelaySec = 60;
      # root for TUN + Firecracker tap + /dev/kvm:
      User = "root";
    };
  };

  # 5b. Health-gated self-update (on-demand, NOT wantedBy any target). Invoked
  #     manually (`systemctl start tabbify-update`) or by a node-side trigger.
  #
  #     This unit ONLY resolves the desired version and then hands the WHOLE
  #     fetch -> probe -> swap flow to the audited Rust engine:
  #         /opt/tabbify/supervisord self-update --to <ver>
  #     so there is a SINGLE real self-update path (no bash reimplementation):
  #       - VersionFetcher.fetch_version (sha256-verified) against the release
  #         bucket (TABBIFY_RELEASE_BASE_URL),
  #       - out-of-band candidate probe via `supervisord --check
  #         --candidate-identity-path <transient>` (transient identity + loopback
  #         ephemeral bind/port — never the sticky ULA, never the runner dir),
  #       - on gate PASS: atomic symlink swap + VERSION rotation + a
  #         pending-confirm marker, then `systemctl restart tabbify-supervisor`,
  #       - on gate FAIL: NO swap, exit non-zero (this unit fails, live install
  #         untouched).
  #     The post-restart self-watchdog + rollback then runs INSIDE the restarted
  #     supervisord (it reads the pending-confirm marker and confirms or reverts
  #     to previous-good). Fail-safe: a non-zero engine exit leaves the node
  #     exactly as it was.
  #
  #     TABBIFY_INSTALL_DIR + TABBIFY_RELEASES_DIR point the engine at this host's
  #     layout (the engine maintains the top-level supervisord/tabbify-runner
  #     symlinks + VERSION directly; the `current` symlink is only used by the
  #     first-boot bootstrap above).
  systemd.services.tabbify-update = {
    description = "Tabbify health-gated self-update (Rust engine: fetch -> probe -> swap -> watchdog)";
    after = [ "network-online.target" "tabbify-supervisor.service" ];
    wants = [ "network-online.target" ];
    # curl + jq resolve the desired version; systemd gives the engine `systemctl`
    # for the swap restart; iproute2 (`ip`) lets the out-of-band candidate probe
    # (`supervisord --check`) bring up its TUN device + join the mesh, and the
    # FC/Docker tools let it detect host capabilities exactly as a real boot does.
    path  = [ pkgs.curl pkgs.jq pkgs.coreutils pkgs.systemd pkgs.iproute2 pkgs.firecracker pkgs.oras ];
    serviceConfig = {
      Type = "oneshot";
      Environment = [
        # Point the Rust self-update engine at this host's release bucket + layout.
        "TABBIFY_RELEASE_BASE_URL=${releaseBase}"
        "TABBIFY_INSTALL_DIR=${dataDir}"
        "TABBIFY_RELEASES_DIR=${releasesDir}"
        "RUST_LOG=info"
      ];
    };
    script = ''
      set -eu
      cd ${dataDir}

      # RESOLVE desired version: explicit env wins, else the `latest` manifest.
      # (Version RESOLUTION stays here; everything past it is the Rust engine.)
      manifest="$(curl -fsSL "${releaseBase}/supervisor/latest" || echo '{}')"
      desired="''${TABBIFY_DESIRED_VERSION:-$(printf '%s' "$manifest" | jq -r '.latest // empty')}"
      [ -n "$desired" ] || { echo "no desired version (env + latest both empty)"; exit 1; }
      echo "self-update -> $desired (delegating to the Rust engine)"

      # Hand the whole fetch/probe/swap to the audited engine. It is idempotent
      # (a no-op when already on $desired) and returns non-zero on any
      # fetch/gate/swap failure WITHOUT swapping -> this oneshot fails fail-safe.
      exec ${dataDir}/supervisord self-update --to "$desired"
    '';
  };

  # 5c. OTA auto-update timer (fleet): poll supervisor/latest every 2 min and run
  #     tabbify-update. The Rust engine is idempotent (a no-op when already on the
  #     latest version), so this only ACTS when a new version is published — this
  #     is what makes updates "arrive automatically" without a manual `systemctl
  #     start`. Per-node control (poll the node's per-supervisor desired-version
  #     instead of the fleet `latest`) is a follow-up; this is the fleet-OTA form.
  systemd.timers.tabbify-update = {
    description = "Poll for a new Tabbify supervisor release (OTA auto-update)";
    wantedBy = [ "timers.target" ];
    timerConfig = {
      OnBootSec       = "2min";
      OnUnitActiveSec = "2min";
      Unit            = "tabbify-update.service";
    };
  };

  # 6. Local artifact server on :9000 — lets you stage an app under
  #    /opt/tabbify/srv/apps/<uuid>/{latest,v1/manifest.toml,v1/<entry>} and
  #    start it with `POST http://127.0.0.1:8730/v1/apps/<uuid>/start`.
  systemd.services.tabbify-appsrv = {
    description = "Tabbify local app artifact server (:9000)";
    wantedBy = [ "multi-user.target" ];
    after    = [ "tabbify-fetch.service" ];
    serviceConfig = {
      ExecStart = "${pkgs.python3}/bin/python3 -m http.server 9000 --directory ${dataDir}/srv";
      Restart   = "always";
    };
  };
}
