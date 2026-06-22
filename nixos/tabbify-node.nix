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
  # ⚠ PINNED kernel (F4, audit #93). The stock Firecracker CI vmlinux (ACPI on →
  #   working LAPIC, no idle core-spin) is the CORRECT kernel; the legacy
  #   docker-derived kernel baked `acpi=off` which busy-spun a core. This URL is
  #   version-pinned (`v1.12/.../vmlinux-6.1.128`) so the boot path can never
  #   silently regress to a busy-spin kernel. The supervisor's `boot_source_body`
  #   never emits `acpi=off` (guard-tested in `protocol.rs`); pin the SOURCE here.
  kernelUrl   = "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.12/x86_64/vmlinux-6.1.128";
  dataDir     = "/opt/tabbify";

  # F1 (audit #93) — AGGREGATE CPU ceiling for the `tabbify-fc.slice` (the sum of
  # ALL Firecracker guests). `300%` == at most 3 full cores for ALL guests
  # combined, reserving the rest of the box for the supervisor + mesh data-plane
  # so a swarm/orphan of guests can never saturate the host and black-hole the
  # mesh. ⚠ OWNER DECISION (capacity planning on the sole MSI worker): raise on a
  # bigger box, lower to reserve more headroom. The PER-GUEST caps are the
  # supervisor's own knobs (`fcCpuQuotaServing`/`fcCpuQuotaBuild`), independent.
  fcSliceCpuQuota   = "300%";
  # Per-guest caps baked onto the supervisor unit (inherited by the one-shot
  # build runner via env). Serving ~1 core, build ~2 cores; weight below the
  # supervisor's default 100 so the box stays steerable under guest contention.
  fcCpuQuotaServing = "100";
  fcCpuQuotaBuild   = "200";
  fcCpuWeight       = "80";

  # Prod mesh coordinator control-plane URL — the SAME baked EIP the supervisor
  # binary defaults to (src/config.rs `DEFAULT_COORDINATOR_URL`) and the
  # standalone `tabbify-mesh` tool defaults to (tools/tabbify-mesh cli.rs). The
  # supervisor's in-process joiner uses its baked default (no `--coordinator`
  # passed in svc #5); the lifeline unit (svc #5a) passes this explicitly so the
  # two joiners always reach the SAME coordinator.
  coordinatorUrl = "http://3.124.69.92:8888";

  # Track C signed remote-restart: the super-admin Ed25519 PUBLIC key (64-char
  # hex) this node verifies every remote command against, end-to-end. It is a
  # PUBLIC key (safe in git / the Nix store), so it is BAKED here as the default
  # — a fresh box has Track-C armed without a drop-in. For rotation without a
  # rebuild it can also be supplied via the `/etc/tabbify/supervisor.env`
  # EnvironmentFile drop-in (TABBIFY_MESH_SUPER_ADMIN_PUBKEY=<hex>), which
  # overrides this default.
  #
  # ⚠ SPLIT PER UNIT (Track-C single-authority — mesh-resilience review fix #5):
  # this baked pubkey is routed ONLY to the `tabbify-mesh-lifeline` unit's
  # `environment` (the lifeline is the SOLE Track-C restart authority on the
  # host). The IN-PROCESS supervisor joiner is given an EMPTY pubkey
  # (`TABBIFY_MESH_SUPER_ADMIN_PUBKEY = ""` on the `tabbify-supervisor` unit), so
  # there is exactly ONE armed peer per host → no addressing ambiguity and a
  # SINGLE host-wide RebootGuard (≤3 reboots/hr), never two.
  meshSuperAdminPubkey = "da24f9580c671a7b26c85175631b5797682041a4e8e695a32f70ee21f16324ba";

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

  # Lifeline joiner self-update — the lifeline unit's OWN ExecStartPre
  # (mesh-resilience review fix #7). It is a copy of `supervisorFetchJoiner` that
  # promotes to a DISTINCT top-level symlink `${dataDir}/tabbify-mesh-lifeline`
  # (NOT the shared `${dataDir}/tabbify-mesh` the supervisor's ExecStartPre
  # writes). Why a separate fetch+symlink instead of reusing
  # `supervisorFetchJoiner`: the lifeline runs `Restart=always` and the two units
  # are only `before`-ordered (NOT serialized across a single-unit restart), so a
  # supervisor OTA restart re-running ITS ExecStartPre while the lifeline restarts
  # on its own cycle would have both `ln -sfn` the SAME path concurrently → a
  # symlink race where one unit yanks the target out from under the other. With a
  # lifeline-distinct symlink there is no shared mutable path → no race. It also
  # saves `${dataDir}/tabbify-mesh-lifeline.prev` (the prior promoted target)
  # BEFORE `ln -sfn`, so the lifeline can roll its own joiner back on repeated
  # failure (spec §3.7) without ever touching the supervisor's diagnostics joiner.
  supervisorFetchJoinerLifeline = pkgs.writeShellScript "tabbify-supervisor-fetch-joiner-lifeline" ''
    set -eu
    arch="${arch}"
    manifest="$(${pkgs.curl}/bin/curl -fsSL "${releaseBase}/mesh/latest" || echo '{}')"
    desired="''${TABBIFY_MESH_VERSION:-$(printf '%s' "$manifest" | ${pkgs.jq}/bin/jq -r '.latest // empty')}"
    # No desired (offline / no manifest): keep whatever is current. Fail-safe.
    [ -n "$desired" ] || { echo "no desired mesh version; keeping current lifeline joiner"; exit 0; }

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
      # Save the prior promoted target (if any) so the lifeline can roll back a
      # bad joiner on repeated failure (spec §3.7). Best-effort: a missing prior
      # symlink (first promote) just leaves no .prev.
      if [ -L "${dataDir}/tabbify-mesh-lifeline" ]; then
        prev_target="$(${pkgs.coreutils}/bin/readlink "${dataDir}/tabbify-mesh-lifeline" || true)"
        [ -n "$prev_target" ] && ${pkgs.coreutils}/bin/ln -sfn "$prev_target" "${dataDir}/tabbify-mesh-lifeline.prev" || true
      fi
      ${pkgs.coreutils}/bin/ln -sfn "mesh/$desired/tabbify-mesh" "${dataDir}/tabbify-mesh-lifeline"
      echo "promoted lifeline joiner $desired"
    else
      echo "candidate lifeline joiner $desired failed --version self-check"
      # If there is NO working prior lifeline joiner at all, fail hard so the unit
      # does not start onto a host with a broken joiner.
      [ -x "${dataDir}/tabbify-mesh-lifeline" ] || { echo "and no prior lifeline joiner present -> refusing to start"; exit 1; }
      echo "keeping prior lifeline joiner (fail-safe)"
    fi
  '';

  # LIFELINE self-revert script (FIX 4, spec §3.7) — the `ExecStart` of the
  # `tabbify-mesh-lifeline-revert` OnFailure unit (svc #5e). The other half of the
  # half-implemented §3.7: `supervisorFetchJoinerLifeline` already SAVES
  # `${dataDir}/tabbify-mesh-lifeline.prev` (the prior promoted joiner) before each
  # promote, but until now NOTHING READ it — a freshly-promoted lifeline joiner
  # that crash-loops had no rollback. This rolls the lifeline symlink back to
  # `.prev` after the lifeline unit keeps failing.
  #
  # ⚠ IDEMPOTENT / RE-ENTRANT (mirrors `tabbifyBootRevertScript`): on systemd v256
  # `OnFailure=` fires on EVERY failed start, so this runs repeatedly per burst. A
  # DURABLE per-attempt counter
  # (`${dataDir}/data/self-heal/lifeline-revert-attempts`) gates the action: every
  # fire bumps + reads `count`; sub-threshold is a STRICT no-op (exit 0) so the
  # lifeline's own `Restart=always`/`RestartSec` handles a transient retry; only
  # the fire that crosses THRESHOLD actually repoints the symlink, and it CLEARS
  # the counter so the rolled-back joiner gets a fresh budget (and a re-promote of
  # a newer joiner later starts from zero). The counter is also reset on a
  # successful lifeline join (svc #5f timer) so slow accumulation across unrelated
  # flaps never trips a spurious revert.
  #
  # SAFE FAIL DIRECTIONS: no `.prev` (first promote / nothing to roll back to) →
  # exit 0 and leave the counter, so the lifeline keeps retrying its current joiner
  # (the fail-safe direction — never strand the lifeline on a dangling symlink). A
  # `.prev` that points at the SAME target as the live symlink → still repoint
  # (idempotent) and clear, breaking the loop. Best-effort throughout; a revert
  # write failure is logged and the lifeline keeps retrying.
  tabbifyLifelineRevertScript = pkgs.writeShellScript "tabbify-mesh-lifeline-revert" ''
    # NO `set -e`: this branches on readlink/ln outcomes; an `-e` abort would
    # defeat the gate. `set -u` for unset-var safety.
    set -u
    PATH="${pkgs.systemd}/bin:${pkgs.coreutils}/bin:$PATH"

    THRESHOLD=3
    SELF_HEAL="${dataDir}/data/self-heal"
    ATTEMPTS_FILE="$SELF_HEAL/lifeline-revert-attempts"
    LINK="${dataDir}/tabbify-mesh-lifeline"
    PREV="${dataDir}/tabbify-mesh-lifeline.prev"

    mkdir -p "$SELF_HEAL"

    # Durable per-attempt counter (plain integer file — no jq dependency on this
    # unit). Missing/torn reads as 0 (fail-safe: a revert is at most delayed).
    count=0
    [ -f "$ATTEMPTS_FILE" ] && count="$(cat "$ATTEMPTS_FILE" 2>/dev/null || echo 0)"
    case "$count" in (*[!0-9]*) count=0;; ("") count=0;; esac
    count=$((count + 1))
    printf '%s\n' "$count" > "$ATTEMPTS_FILE"
    echo "tabbify-mesh-lifeline-revert: count=$count threshold=$THRESHOLD"

    # ── Sub-threshold fire: STRICT no-op; let the lifeline's Restart re-try. ───
    if [ "$count" -lt "$THRESHOLD" ]; then
      echo "tabbify-mesh-lifeline-revert: sub-threshold ($count < $THRESHOLD) — letting the lifeline RestartSec re-try (transient)."
      exit 0
    fi

    # ── count >= THRESHOLD: roll the lifeline joiner back to .prev. ────────────
    if [ ! -L "$PREV" ] && [ ! -e "$PREV" ]; then
      echo "tabbify-mesh-lifeline-revert: no .prev to roll back to (first promote / nothing prior) — leaving current joiner; lifeline keeps retrying."
      exit 0
    fi
    prev_target="$(readlink "$PREV" 2>/dev/null || true)"
    if [ -z "$prev_target" ]; then
      echo "tabbify-mesh-lifeline-revert: .prev is not a readable symlink — cannot roll back; leaving current joiner."
      exit 0
    fi

    echo "tabbify-mesh-lifeline-revert: threshold reached — rolling lifeline joiner back to .prev ($prev_target)"
    if ln -sfn "$prev_target" "$LINK"; then
      # Cleared so the rolled-back joiner gets its OWN fresh budget, and a later
      # re-promote of a newer joiner starts from zero.
      rm -f "$ATTEMPTS_FILE"
      echo "tabbify-mesh-lifeline-revert: rolled back + counter cleared. systemd will restart the lifeline onto the prior joiner."
      exit 0
    else
      echo "tabbify-mesh-lifeline-revert: ln -sfn rollback failed — leaving current joiner, lifeline keeps retrying."
      exit 0
    fi
  '';

  # LIFELINE counter-reset script (FIX 4) — run by a short OnUnitActiveSec timer
  # (svc #5f) so a lifeline that has been actively joined for a while clears its
  # revert-attempt counter. Without this, attempts could SLOWLY accumulate across
  # unrelated, far-apart flaps and eventually trip a spurious rollback of a
  # perfectly-good joiner.
  # ⚠ LIVENESS GATE (review fix): the timer's `OnUnitActiveSec`/`OnBootSec` clock
  # off the RESET service's OWN activation/boot, NOT the lifeline's — so an
  # unconditional `rm -f` would also clear the counter mid-crash-loop (e.g. wipe a
  # legit count=2 during a slow flap, so the §3.7 rollback never trips). Gate the
  # clear on the lifeline being CURRENTLY `active` (a stably-joined unit), so the
  # reset only fires for a genuinely-healthy lifeline and never rescues a flapping
  # one from its own revert.
  tabbifyLifelineResetAttemptsScript = pkgs.writeShellScript "tabbify-mesh-lifeline-reset-attempts" ''
    set -u
    PATH="${pkgs.coreutils}/bin:${pkgs.systemd}/bin:$PATH"
    state="$(systemctl show tabbify-mesh-lifeline.service -p ActiveState --value 2>/dev/null || echo unknown)"
    if [ "$state" != "active" ]; then
      echo "tabbify-mesh-lifeline-reset-attempts: lifeline ActiveState=$state (not stably active) — keeping the revert-attempt counter."
      exit 0
    fi
    rm -f "${dataDir}/data/self-heal/lifeline-revert-attempts"
    echo "tabbify-mesh-lifeline-reset-attempts: lifeline active — revert-attempt counter cleared."
  '';

  # Crash-at-startup CATCH-NET script — the `ExecStart` of the
  # `tabbify-boot-revert` OnFailure unit (svc #5d, mesh-resilience Phase 2). It is
  # the no-live-process layer that rolls supervisord back to the previous-good
  # release when the start path keeps crashing (the 2026-06-22 brick class).
  #
  # ⚠ IDEMPOTENT / RE-ENTRANT (review fixes #1 + #2). On systemd v256 `OnFailure=`
  # fires on EVERY failed ExecStart, so this runs ~3-5× per crash burst. The
  # durable `BootAttempts` counter (written by the supervisor binary at
  # `${dataDir}/data/self-heal/boot-attempts.json`) is what makes per-attempt
  # firing correct: every fire reads `count`; only the fire on which `count`
  # crossed the threshold actually reverts, and `reset-failed && start` is
  # STRICTLY gated behind a revert ACTUALLY performed THIS fire.
  #
  # ⚠ NEVER `systemctl reset-failed` on a sub-threshold fire — that resets the
  # start-rate-limit counter (systemd #10529) → the §4 circuit-breaker NEVER
  # trips → unbounded restart loop = the 06-22 class re-armed. A sub-threshold
  # fire is a strict no-op (exit 0); `RestartSec` handles the transient retry.
  #
  # PATHS: the script and the `revert-to-previous` subcommand it invokes resolve
  # the SAME on-host layout the supervisor binary uses — `TABBIFY_INSTALL_DIR`
  # (=${dataDir}, the symlinks+VERSION ledger), `TABBIFY_RELEASES_DIR`
  # (=${releasesDir}), and `SUPERVISOR_DATA_DIR` (=${dataDir}/data, the
  # BootAttempts sidecar + the SHARED reboot-guard.json the Rust subcommand's
  # RebootGuard consults — ONE host-wide ≤3/hr budget). These are set on the
  # OnFailure unit's `environment` (svc #5d) AND re-asserted here so the script is
  # correct even if invoked by hand.
  #
  # EXIT CODES of `supervisord revert-to-previous` (the load-bearing contract,
  # main.rs `revert_exit`): 0 PERFORMED, 2 NO_PREVIOUS (O4 first-boot bail —
  # nothing to roll back to), 3 FAILED (a real error), 4 REBOOT_PARKED (reboot
  # last-resort exhausted via the guard → park for a human).
  tabbifyBootRevertScript = pkgs.writeShellScript "tabbify-boot-revert" ''
    # NOTE: deliberately NO `set -e` — this script BRANCHES on the non-zero exit
    # codes of `revert-to-previous`, so an `-e` abort would defeat the whole gate.
    set -u
    PATH="${pkgs.systemd}/bin:${pkgs.coreutils}/bin:${pkgs.jq}/bin:$PATH"

    # ⚠ FIX 10 — LOCKSTEP with the Rust `REVERT_THRESHOLD` const
    # (src/boot_health/mod.rs). THIS bash `THRESHOLD` is what ACTUALLY gates the
    # production revert: the script reads the durable BootAttempts `count` from
    # the sidecar and decides, on this fire, whether to call `revert-to-previous`.
    # The Rust const of the same value gates ONLY unit tests
    # (`BootAttempts::should_revert`); the boot path never calls it. Keep the two
    # equal — change one, change the other. (Deliberately NOT shared via codegen:
    # this script must work even with a supervisord binary that predates any
    # threshold mechanism.)
    THRESHOLD=3
    DATA_DIR="${dataDir}/data"
    SUPERVISORD="${dataDir}/supervisord"
    ATTEMPTS_FILE="$DATA_DIR/self-heal/boot-attempts.json"

    # The revert subcommand resolves its layout from these — keep them in lockstep
    # with the supervisor unit so the symlinks/VERSION/sidecar/reboot-guard all
    # land on the SAME files.
    export TABBIFY_INSTALL_DIR="${dataDir}"
    export TABBIFY_RELEASES_DIR="${releasesDir}"
    export SUPERVISOR_DATA_DIR="$DATA_DIR"

    # Read the durable counter. A missing/torn sidecar reads as count=0 (the
    # fail-safe direction: a revert is at most delayed by one boot, never
    # spuriously triggered) — exactly mirroring `BootAttempts::load`.
    count=0
    reverted_to=""
    if [ -f "$ATTEMPTS_FILE" ]; then
      count="$(jq -r '.count // 0' "$ATTEMPTS_FILE" 2>/dev/null || echo 0)"
      reverted_to="$(jq -r '.reverted_to // ""' "$ATTEMPTS_FILE" 2>/dev/null || echo "")"
    fi
    case "$count" in (*[!0-9]*) count=0;; esac   # defensive: non-numeric -> 0
    echo "tabbify-boot-revert: count=$count threshold=$THRESHOLD reverted_to='$reverted_to'"

    # ── Sub-threshold fire: STRICT no-op. NEVER touch the breaker. ────────────
    if [ "$count" -lt "$THRESHOLD" ]; then
      echo "tabbify-boot-revert: sub-threshold ($count < $THRESHOLD) — letting RestartSec re-try (transient). No reset-failed."
      exit 0
    fi

    # ── FIX 1: OLD-BINARY PROBE (must precede every `revert-to-previous`). ─────
    # The whole rc ladder below invokes `"$SUPERVISORD" revert-to-previous` and
    # branches on its EXIT CODE (0 PERFORMED / 2 NO_PREVIOUS / 3 FAILED / 4
    # REBOOT_PARKED). But if the live (possibly OLD) supervisord binary PREDATES
    # the `revert-to-previous` subcommand, clap aborts with "unrecognized
    # subcommand" and exits 2 — which the ladder MIS-MAPS to NO_PREVIOUS, so the
    # script does a NO-OP escalation that itself can't run on the old binary →
    # no revert, no reboot, SILENT brick (the exact class this catch-net exists to
    # break). `--help` short-circuits clap BEFORE any fallible startup (binds,
    # mesh join, ...), exits 0, and lists the subcommands; an old binary prints
    # help WITHOUT `revert-to-previous`. So probe FIRST: if the subcommand is
    # absent, the rc ladder is meaningless — escalate straight to a guarded
    # last-resort reboot.
    #
    # ⚠ REBOOT LOOP-GUARD (shell-side): the Rust RebootGuard (≤3/hr,
    # reboot-guard.json) is NOT reachable here precisely because the binary is too
    # old to run any subcommand. So gate the reboot with a tiny durable
    # timestamp-ring file ($DATA_DIR/self-heal/old-binary-reboots) — at most 3
    # reboots per hour — so a host whose old binary keeps crash-looping reboots a
    # few times (which can recover a transient/disk/network fault) and then PARKS
    # (exit 0, no reboot) for a human instead of looping forever.
    if ! "$SUPERVISORD" --help 2>&1 | grep -q 'revert-to-previous'; then
      echo "tabbify-boot-revert: live supervisord has NO 'revert-to-previous' subcommand (OLD binary) — rc ladder is unusable; escalating to guarded last-resort reboot"
      RING="$DATA_DIR/self-heal/old-binary-reboots"
      mkdir -p "$DATA_DIR/self-heal"
      now="$(date +%s)"
      # Keep only timestamps within the last hour; count them.
      kept=""
      n=0
      if [ -f "$RING" ]; then
        while IFS= read -r ts; do
          case "$ts" in (*[!0-9]*) continue;; ("") continue;; esac
          if [ $((now - ts)) -lt 3600 ]; then
            kept="$kept$ts
"
            n=$((n + 1))
          fi
        done < "$RING"
      fi
      if [ "$n" -ge 3 ]; then
        echo "tabbify-boot-revert: old-binary reboot budget exhausted (>=3/hr) — PARKING for a human (no reboot). Re-image or re-bootstrap the supervisord binary."
        printf '%s' "$kept" > "$RING"
        exit 0
      fi
      printf '%s%s\n' "$kept" "$now" > "$RING"
      echo "tabbify-boot-revert: rebooting (old-binary reboot $((n + 1))/3 this hour)"
      systemctl reboot
      exit 0
    fi

    # ── count >= THRESHOLD ────────────────────────────────────────────────────
    if [ -z "$reverted_to" ]; then
      # First escalation: try a revert to previous-good.
      echo "tabbify-boot-revert: threshold reached, no prior revert — attempting revert-to-previous"
      "$SUPERVISORD" revert-to-previous
      rc=$?
      if [ "$rc" -eq 0 ]; then
        # A revert was ACTUALLY performed this fire — ONLY now is it safe to
        # reset the breaker and kick the (reverted) binary.
        echo "tabbify-boot-revert: revert PERFORMED — reset-failed + start the reverted binary"
        systemctl reset-failed tabbify-supervisor
        systemctl start tabbify-supervisor
        exit 0
      elif [ "$rc" -eq 2 ]; then
        # NO_PREVIOUS (O4 first-boot bail): nothing to roll back to → escalate to
        # reboot-as-last-resort (guarded). Do NOT reset-failed (no revert happened).
        echo "tabbify-boot-revert: no previous-good release (NO_PREVIOUS) — escalating with --reboot-on-exhausted"
        "$SUPERVISORD" revert-to-previous --reboot-on-exhausted
        exit $?
      else
        # FAILED (3) or any other error: leave the breaker intact, let systemd
        # park. Do NOT reset-failed.
        echo "tabbify-boot-revert: revert-to-previous returned $rc (not performed) — leaving unit parked for the breaker/human"
        exit "$rc"
      fi
    else
      # Already reverted once this streak and STILL failing (the reverted binary
      # also crash-loops): reboot-as-last-resort, then park.
      #
      # ⚠ FIX 2 (Rust, src/main.rs `revert_to_previous_flow`): with the sidecar's
      # `reverted_to` ALREADY set + `--reboot-on-exhausted`, the subcommand now
      # SHORT-CIRCUITS straight to the guarded RebootGuard reboot seam (it does
      # NOT do a SECOND, deeper symlink revert that would walk history down to an
      # even-older — possibly equally-broken — release). It returns PERFORMED (0,
      # rebooting) or REBOOT_PARKED (4, ≤3/hr guard exhausted → leave failed for a
      # human). So this single line is now CORRECT — it reboots as last resort
      # instead of soft-bricking by reverting forever. We pass through its exit.
      echo "tabbify-boot-revert: reverted binary still failing (reverted_to='$reverted_to') — reboot-as-last-resort (Rust skips a deeper revert)"
      "$SUPERVISORD" revert-to-previous --reboot-on-exhausted
      exit $?
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

  # 3. Open the WireGuard listen ports (helps NAT traversal to the public
  #    coordinator). 51820 = the supervisor's in-process joiner; 51821 = the
  #    lifeline joiner's OWN port (FIX 8 — a distinct port so two same-host
  #    joiners never SO_REUSEPORT-split inbound UDP once direct-UDP p2p is
  #    re-enabled; see the lifeline ExecStart `--listen-port 51821`).
  networking.firewall.allowedUDPPorts = [ 51820 51821 ];

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

  # 4c. tabbify-fc.slice — the AGGREGATE CPU ceiling for every Firecracker guest
  #     (F1, audit #93). Each FC child runs in its OWN transient
  #     `tabbify-fc-<id>.scope` UNDER this slice (the supervisor spawns them via
  #     `systemd-run --scope --slice=tabbify-fc.slice -p CPUQuota=… -p CPUWeight=…`),
  #     so the slice's own `CPUQuota` caps the SUM of all guests and always leaves
  #     the host headroom the supervisor + mesh data-plane need. This is the
  #     defence that turns "one runaway/orphaned FC saturates the box → mesh
  #     starves → liveness reads black-hole → watchdog SIGKILL → crash-loop →
  #     more orphans → hotter" into a bounded, steerable load — AND it survives a
  #     dead supervisor (systemd owns the slice + scopes; `systemctl stop
  #     tabbify-fc.slice` tears every guest down even with supervisord gone).
  #
  #     ⚠ OWNER DECISION (capacity planning, #93): `fcSliceCpuQuota` reserves host
  #     headroom on the sole MSI worker. `${fcSliceCpuQuota}` here leaves ~1 core
  #     for the supervisor/mesh on a typical multi-core box; tune per host. The
  #     PER-GUEST caps are the supervisor's own knobs (SUPERVISOR_FC_CPU_QUOTA_*,
  #     baked on the unit below) — slice = aggregate, scope = per-guest, decoupled.
  systemd.slices.tabbify-fc = {
    description = "Tabbify Firecracker guests (aggregate CPU ceiling — F1, audit #93)";
    sliceConfig = {
      CPUAccounting = true;
      CPUQuota      = fcSliceCpuQuota;
    };
  };

  # 5. The supervisor itself — managed, auto-restart, starts on boot.
  #    Runs as root: it opens a host TUN device (CAP_NET_ADMIN), creates
  #    Firecracker taps, and opens /dev/kvm. The coordinator address is already
  #    baked into the binary (production EIP) — nothing to configure.
  systemd.services.tabbify-supervisor = {
    description = "Tabbify supervisor node";
    wantedBy = [ "multi-user.target" ];
    # CIRCUIT-BREAKER that TRIPS the auto-rollback catch-net — NOT a permanent
    # brick (mesh-resilience review fix #4). When the start path keeps crashing,
    # the breaker trips and `OnFailure=tabbify-boot-revert` (svc #5d) fires to roll
    # back to the previous-good release. ~3 SPACED restarts (RestartSec 10/20/30,
    # serviceConfig below) span ~60s of real retry, comfortably UNDER the 120s
    # window: a one-off transient (the 2026-06-22 ~31ms exit class) recovers on
    # the FIRST spaced retry; a persistent bad binary exhausts the burst on the
    # 4th trip → park `failed` → OnFailure → revert. The park is now the
    # DELIBERATE trigger for rollback, not a dead end.
    #
    # ⚠ This tighten (5/600 → 4/120) lands TOGETHER WITH the OnFailure unit in
    # this same nix drop (review fix #4) — tightening the breaker BEFORE the
    # catch-net exists would make the box strictly more brittle (a crash in that
    # window bricks faster with NO rollback). The Rust reboot loop-guard (the
    # SHARED RebootGuard, ≤3 reboots/hr) + systemd park remain the LAST resort if
    # even the revert is exhausted; a human clears a park with
    # `systemctl reset-failed tabbify-supervisor`.
    startLimitIntervalSec = 120;
    startLimitBurst       = 4;
    after    = [ "tabbify-fetch.service" "network-online.target" ];
    wants    = [ "network-online.target" ];
    requires = [ "tabbify-fetch.service" ];
    # tools the supervisor execs at runtime must be on its PATH (oras + tar(busybox) + mkfs.ext4 = docker-less FC);
    # curl + jq back the ExecStartPre joiner self-update (svc #5 / NX-4);
    # iptables for per-tap guest-egress NAT (MASQUERADE/FORWARD) set up after the
    # FC tap so the in-VM node can reach the public mesh coordinator (B3);
    # git clones the source on the HOST when this node is a `builder` (the
    # FC-build sandbox clones host-side so the token never enters the guest);
    # systemd provides `systemd-run` (F1 — wraps each FC spawn in a CPU-capped
    # scope under tabbify-fc.slice) + `systemctl` (scope teardown reaper):
    path = [ pkgs.firecracker pkgs.iproute2 pkgs.iptables pkgs.busybox pkgs.e2fsprogs pkgs.oras pkgs.coreutils pkgs.curl pkgs.jq pkgs.git pkgs.systemd ];
    environment = {
      # FIX 5: node name is NO LONGER baked here. systemd `Environment=`
      # (this block) OVERRIDES `EnvironmentFile=`, so a hard-set `SUPERVISOR_NAME`
      # would shadow the `/etc/tabbify/supervisor.env` drop-in forever — the exact
      # trap that let a clean rebuild silently mis-name a hand-patched box
      # ('serbia:bg:msi' on the live MSI vs 'thinkpad' in this repo). With it
      # removed, the binary sources its display name from the drop-in's
      # `TABBIFY_NODE_NAME` (the provisioner/controller writes it there), which
      # `Config::node_name_env_bridge` maps onto the `SUPERVISOR_NAME` clap arg;
      # with neither set the binary falls back to its own `default_value`
      # ("tabbify-supervisor"). One host-local source of truth = /etc.
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
      # Track C signed remote-restart: the supervisor's IN-PROCESS joiner is
      # DELIBERATELY NOT a Track-C target (mesh-resilience review fix #5). The
      # SOLE Track-C authority on this host is the out-of-process
      # `tabbify-mesh-lifeline` unit (which reads the REAL `meshSuperAdminPubkey`);
      # arming BOTH joiners would put two signed-command peers on one host →
      # addressing ambiguity + two separate RebootGuards (up to 6 reboots/hr).
      # So this in-process joiner gets an EMPTY pubkey → `parse_super_admin_pubkey`
      # returns None → fail-closed (every signed command rejected here). The
      # supervisor is restarted BY the lifeline's sink (`systemctl restart
      # tabbify-supervisor`), so it never needs to receive remote commands itself.
      # (An EnvironmentFile drop-in could still override this for a one-off, but
      # the canonical Track-C target is the lifeline.)
      TABBIFY_MESH_SUPER_ADMIN_PUBKEY = "";
      # Always capture the Firecracker serial console: runners inherit this env
      # and append guest console output to <data_dir>/fc/<uuid>.console.log
      # (src/firecracker/linux.rs::console_stdio) — without it a spawn failure
      # inside the guest is invisible (the 500 only carries the top-level error).
      SUPERVISOR_FC_DEBUG    = "1";
      # F1 (audit #93): per-FC CPU-scope caps. The supervisor wraps each
      # firecracker child in a `systemd-run --scope --slice=tabbify-fc.slice`
      # with these quotas; the one-shot build runner reads the SAME envs. Serving
      # ~1 core, build ~2 cores, weight below the supervisor's 100 so the box
      # stays steerable. Slice aggregate ceiling is `tabbify-fc.slice` (svc #4c).
      SUPERVISOR_FC_CPU_QUOTA_SERVING = fcCpuQuotaServing;
      SUPERVISOR_FC_CPU_QUOTA_BUILD   = fcCpuQuotaBuild;
      SUPERVISOR_FC_CPU_WEIGHT        = fcCpuWeight;
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
      # Track B tier-1 (self-heal watchdog). systemd arms a hardware-style
      # watchdog on this unit: supervisord must send sd_notify(WATCHDOG=1) at
      # least every WatchdogSec or systemd SIGKILLs + restarts it. The
      # independent watchdog-pet task in supervisord pets every WatchdogSec/2
      # ONLY while the mesh data plane is healthy (Track-K dataplane_healthy);
      # on a sustained black hole (control-plane alive, WG decap-RX zero — the
      # MSI incident) the pet stops, systemd restarts the unit, and the fresh
      # register + fresh boringtun Tunns + fresh relay-WS re-handshake the
      # tunnel. 120s > ~20s heartbeat + a realistic WAN stall AND > the ~90s
      # Track-K RX-silence threshold, so dataplane_healthy() has already flipped
      # false (and stayed false across a W/2 skip) before systemd fires — a
      # healthy but momentarily-laggy node is never killed (spec §8 tuning).
      #
      # KillMode=process (above, SU-2) means this watchdog restart signals ONLY
      # supervisord — the setsid-detached app/dev runners + their Firecrackers
      # survive and are re-adopted by readopt() on the fresh boot, so a self-heal
      # never loses a /workspace. relay_only rides TABBIFY_MESH_RELAY_ONLY in the
      # env (above), so the post-restart re-join preserves it automatically.
      WatchdogSec      = 120;
      WatchdogSignal   = "SIGKILL";  # default, made explicit: hard-kill a wedged proc
      # Fail-safe on-host joiner self-update at boot (NX-4): a broken joiner
      # makes this exit non-zero -> the unit fails to start -> host left as-is.
      ExecStartPre     = "${supervisorFetchJoiner}";
      ExecStart        = "${dataDir}/supervisord";   # symlink -> current/supervisord (svc #4)
      WorkingDirectory = dataDir;
      # Phase-2 join token. A token-validating coordinator (AUTH_URL set) requires
      # this node to present a join token on register; the coordinator then stamps
      # the node's network + tags from the token CLAIMS. Kept OUT of the Nix store
      # (no secret in git / world-readable store): the provisioner drops it into
      #   /etc/tabbify/supervisor.env   ->   TABBIFY_JOIN_TOKEN=<jwt>   (chmod 600)
      # before `nixos-rebuild switch`. The leading '-' makes a missing file
      # non-fatal, so a dev / no-mesh node still starts.
      #
      # ⚠ CANONICAL PATH = /etc/tabbify/supervisor.env (BUG 2, 2026-06-22 root
      # cause + revert of the mis-aimed GAP#6 /opt canonicalization). This is where
      # the provisioner writes the VALID 1-year join token; it is the SAME path the
      # supervisor joins with today. The 06-22 brick was caused by the keystone
      # pointing this at `${dataDir}/supervisor.env` (= /opt/tabbify/supervisor.env),
      # which held a STRAY EXPIRED 1-hour token → coordinator returned 401 ("join
      # token invalid or revoked") → crash-loop. /etc is the single source of truth;
      # the lifeline unit (svc #5a) reads the SAME /etc file so both joiners present
      # the SAME valid token. Rotate the token via this one /etc path only.
      EnvironmentFile  = "-/etc/tabbify/supervisor.env";
      # on-failure (NOT always): a clean exit during a watchdog rollback must
      # NOT auto-respawn the just-swapped-out binary. Exponential-ish backoff
      # (RestartSec grows over RestartSteps up to RestartMaxDelaySec) avoids a
      # tight crash loop on a broken release. 3 SPACED attempts (RestartSec
      # 10/20/30) span ~60s < the 120s StartLimit window: a transient recovers on
      # the first retry; a persistent bad binary exhausts the burst → park →
      # OnFailure → revert (mesh-resilience review fix #4, folded in with the
      # OnFailure catch-net below so the breaker never precedes it).
      Restart            = "on-failure";
      RestartSec         = 10;
      RestartSteps       = 3;
      RestartMaxDelaySec = 30;
      # root for TUN + Firecracker tap + /dev/kvm:
      User = "root";
    };
    # CRASH-AT-STARTUP CATCH-NET (mesh-resilience Phase 2 keystone). When the
    # start path keeps crashing — the ~31ms pre-READY exit class the in-process
    # watchdog (Track B/D) can NEVER see because it arms only after join+READY —
    # systemd fires this OnFailure unit (svc #5d). On systemd v256 it fires on
    # EVERY failed ExecStart, so tabbify-boot-revert is idempotent and only the
    # fire on which the durable BootAttempts counter crosses the threshold
    # actually reverts. This is the ONLY layer that works with no live process.
    #
    # ⚠ BUG 1 (2026-06-22 root cause): `OnFailure=` is a `[Unit]` directive. The
    # keystone put it under `serviceConfig` → systemd rendered it into `[Service]`
    # and logged `Unknown key 'OnFailure' in section [Service], ignoring` → the
    # catch-net NEVER ARMED. The NixOS `.onFailure` list shortcut emits it into
    # `[Unit]` (NOT `unitConfig.OnFailure`, which would not append the systemd
    # `.service` suffix). Verified by rendering: the unit file carries
    # `OnFailure=tabbify-boot-revert.service` under `[Unit]`.
    onFailure = [ "tabbify-boot-revert.service" ];
  };

  # 5a. Mesh LIFELINE joiner — the out-of-process survivability unit
  #     (mesh-resilience Phase 1, Option B). The supervisor's in-process joiner
  #     runs inside supervisord's tokio runtime, so a supervisord crash kills the
  #     TUN, drops every WG session, and SSH-over-mesh dies WITH it. This unit is
  #     an INDEPENDENT, REDUNDANT joiner on a SECOND identity/ULA
  #     (`lifeline-identity.json`, its own ephemeral :51820) with its OWN
  #     `Restart=always` lifetime — it is unaffected by a supervisord crash-loop
  #     or a StartLimit park, so the lifeline ULA stays SSH-reachable and Leo can
  #     always get in and fix the box. Option B keeps app-ULA hosting on the
  #     supervisor's in-process joiner (zero contention); the lifeline is a pure
  #     reachability path, NOT a hard dependency (supervisor must still start even
  #     if the lifeline is flapping), so the supervisor does NOT `Requires` it.
  #
  #     Track-C SOLE AUTHORITY (review fix #5): this unit reads the REAL
  #     super-admin pubkey (baked `meshSuperAdminPubkey` default + EnvironmentFile
  #     override) and is the ONLY signed-command target on the host — the
  #     supervisor's in-process joiner runs with an EMPTY pubkey (svc #5). One
  #     armed peer per host → one RebootGuard, no addressing ambiguity. The
  #     operator finds the lifeline node-id via its roster `display_name`
  #     (`${nodeName}-lifeline`) or the `lifeline-status.json` it writes on join.
  systemd.services.tabbify-mesh-lifeline = {
    description = "Tabbify mesh lifeline joiner (survives a supervisord crash)";
    wantedBy = [ "multi-user.target" ];
    # Ordered BEFORE the supervisor so the lifeline ULA is up first, but NOT a
    # hard dep of it (Option B: redundant, not required).
    before   = [ "tabbify-supervisor.service" ];
    after    = [ "network-online.target" ];
    wants    = [ "network-online.target" ];
    # NEVER park the lifeline: disable the start-rate-limit burst cap entirely so
    # it keeps trying forever. This is a `[Unit]` directive — set via the NixOS
    # top-level `startLimitIntervalSec` option (NOT `serviceConfig`, which would
    # emit it into `[Service]` where systemd ignores it), mirroring how
    # tabbify-supervisor sets its breaker.
    startLimitIntervalSec = 0;
    # `ip`/`iptables` for the lifeline's own TUN + overlay routing; coreutils for
    # the fetch script's readlink/ln.
    path     = [ pkgs.iproute2 pkgs.iptables pkgs.coreutils pkgs.curl pkgs.jq ];
    environment = {
      # NO TABBIFY_MESH_RELAY_ONLY here (review fix #12): the `--relay-only` CLI
      # arg below is the SINGLE source of truth for the standalone tool. The env
      # var only drives the IN-PROCESS supervisor joiner (svc #5).
      RUST_LOG = "info";
      # Track-C: the REAL super-admin pubkey for the SOLE authority (the lifeline).
      # Baked default; the EnvironmentFile drop-in can rotate it without a rebuild.
      TABBIFY_MESH_SUPER_ADMIN_PUBKEY = meshSuperAdminPubkey;
    };
    serviceConfig = {
      # `exec`: the joiner joins+blocks; the standalone tool has no sd_notify yet,
      # so "ready" == "exec'd".
      Type    = "exec";
      # The lifeline's OWN fetch writing its OWN symlink (review fix #7) — never
      # the shared `${dataDir}/tabbify-mesh` the supervisor's ExecStartPre writes.
      ExecStartPre = "${supervisorFetchJoinerLifeline}";
      # Join on a SECOND identity/ULA, relay-only, as the sole Track-C target.
      #
      # ⚠ BUG 3 (2026-06-22 root cause): the lifeline joined WITHOUT a join token,
      # so a token-validating coordinator (AUTH_URL set) rejected its register with
      # 401 → the lifeline crash-looped and never appeared in the roster (its whole
      # reason to exist — an SSH-reachable lifeline — was therefore absent during
      # the incident). FIX: the lifeline now sources `/etc/tabbify/supervisor.env`
      # (the SAME VALID 1-year token the supervisor joins with — both this box's
      # tag:net peer) and passes it as `--join-token`. The standalone tool reads
      # the token from `--join-token` (env `MESH_JOIN_TOKEN`), but the canonical
      # file exports it as `TABBIFY_JOIN_TOKEN` (what the supervisor binary reads),
      # so we reconcile cleanly with a tiny `sh -c` wrapper that forwards
      # `$TABBIFY_JOIN_TOKEN` into `--join-token`. A missing/empty token degrades
      # to today's no-token behavior (the `-` on EnvironmentFile keeps it
      # non-fatal; an unset var expands empty → still a valid CLI against a
      # no-AUTH_URL coordinator). `''${TABBIFY_MESH_SUPER_ADMIN_PUBKEY}` and
      # `''${TABBIFY_JOIN_TOKEN}` are expanded by the shell from this unit's
      # environment (baked default / `/etc` EnvironmentFile).
      #
      # ⚠ BUG 5 (2026-06-22, caught on the live deploy): the standalone joiner's
      # config validation REQUIRES an explicit mTLS mode — either all three of
      # `--tls-cert/--tls-key/--tls-ca` OR `--insecure-no-mtls` — and rejects the
      # join with `invalid join config: mTLS requires all three paths` BEFORE ever
      # contacting the coordinator. The supervisor's IN-PROCESS joiner builds its
      # JoinConfig in code (no CLI validation) and defaults to no-mTLS against this
      # plaintext `http://` coordinator, so it never tripped this. Every standalone
      # joiner on THIS mesh (the coordinator runs without mTLS enforcement — real
      # peer auth is the WireGuard keypair + the validated join-token claims, not
      # mTLS) connects with `--insecure-no-mtls`; the lifeline must match. This is
      # NOT a security downgrade — it is the established mesh-wide mode, consistent
      # with the in-process joiner the lifeline mirrors.
      #
      # ⚠ FIX 8: `--listen-port 51821` gives the lifeline joiner its OWN WireGuard
      # UDP port. The supervisor's in-process joiner owns the default 51820; if the
      # lifeline also bound 51820, then once direct-UDP p2p is re-enabled (track #53)
      # SO_REUSEPORT would let TWO same-port joiners on this host SPLIT inbound UDP
      # at random → half the lifeline's handshake/keepalive datagrams would be
      # delivered to the supervisor's socket (and vice-versa), silently breaking
      # both. A distinct port keeps the two data planes cleanly separated. (51821 is
      # opened in networking.firewall.allowedUDPPorts below.) Relay-only today means
      # neither binds an inbound endpoint, but bake the separation in NOW so the
      # direct-UDP flip is safe without touching this unit.
      #
      # ⚠ FIX 5: `--name` is sourced from the `/etc` drop-in's `TABBIFY_NODE_NAME`
      # (the same host-identity var the supervisor binary reads), NOT the static
      # repo `nodeName` — so a clean rebuild on a hand-patched box (e.g. MSI =
      # 'serbia:bg:msi') names the lifeline `serbia:bg:msi-lifeline`, not
      # 'thinkpad-lifeline'. `''${TABBIFY_NODE_NAME:-thinkpad}` falls back to
      # `thinkpad` when the drop-in is absent (a fresh box), matching the binary's
      # spirit. The shell expands it from this unit's EnvironmentFile environment.
      ExecStart = ''${pkgs.bash}/bin/sh -c 'exec ${dataDir}/tabbify-mesh-lifeline join --coordinator ${coordinatorUrl} --identity-path ${dataDir}/data/lifeline-identity.json --relay-only --listen-port 51821 --insecure-no-mtls --join-token "''${TABBIFY_JOIN_TOKEN:-}" --super-admin-pubkey "''${TABBIFY_MESH_SUPER_ADMIN_PUBKEY}" --status-file ${dataDir}/data/lifeline-status.json --name "''${TABBIFY_NODE_NAME:-thinkpad}-lifeline"' '';
      # CANONICAL env path = /etc/tabbify/supervisor.env (BUG 2 + BUG 3): carries
      # the VALID 1-year TABBIFY_JOIN_TOKEN the wrapper forwards into --join-token,
      # the SAME file the supervisor unit reads. Rotation only — never a 2nd path.
      EnvironmentFile = "-/etc/tabbify/supervisor.env";
      # The lifeline NEVER gives up: always restart, spaced, and NEVER park
      # (the burst cap is disabled via top-level `startLimitIntervalSec = 0` above).
      Restart    = "always";
      RestartSec = 10;
      # root for the TUN + overlay routing.
      User = "root";
    };
    # FIX 4 (spec §3.7): when the lifeline keeps FAILING to start (a freshly
    # promoted joiner that crash-loops), fire the lifeline self-revert catch-net,
    # which rolls `${dataDir}/tabbify-mesh-lifeline` back to its saved `.prev`.
    # NixOS `.onFailure` emits this into `[Unit]` (the correct section — NOT
    # `serviceConfig`/`[Service]`, where systemd would ignore it), and appends the
    # `.service` suffix. The lifeline runs `Restart=always` + NEVER parks
    # (`startLimitIntervalSec = 0`), so on systemd v256 this OnFailure re-fires per
    # failed start — `tabbifyLifelineRevertScript` is idempotent and gated by its
    # own durable per-attempt counter so only the fire that crosses the threshold
    # actually rolls back.
    onFailure = [ "tabbify-mesh-lifeline-revert.service" ];
  };

  # 5e. LIFELINE self-revert CATCH-NET (FIX 4, spec §3.7). Triggered ONLY via
  #     `OnFailure=` on tabbify-mesh-lifeline (svc #5a) — NOT `wantedBy` any
  #     target. Rolls the lifeline joiner symlink back to its saved `.prev` when a
  #     freshly-promoted joiner crash-loops, so a bad lifeline OTA can self-heal
  #     without ever touching the supervisor's diagnostics joiner.
  #
  #     ⚠ Disable the burst cap on the catch-net ITSELF (mirror svc #5d): the
  #     lifeline's per-attempt OnFailure re-triggering on systemd v256 must never
  #     park this oneshot mid-incident. `startLimitIntervalSec = 0` is a `[Unit]`
  #     directive set via the NixOS top-level option (NOT `serviceConfig`).
  systemd.services.tabbify-mesh-lifeline-revert = {
    description = "Roll the mesh lifeline joiner back to previous on repeated start failure (spec §3.7)";
    # Deliberately NOT wantedBy any target — only fired via OnFailure=.
    startLimitIntervalSec = 0;
    # coreutils for readlink/ln/cat/rm; systemd for PATH parity (no systemctl call
    # here — systemd restarts the lifeline on its own `Restart=always` cycle).
    path = [ pkgs.coreutils pkgs.systemd ];
    serviceConfig = {
      Type = "oneshot";
      ExecStart = "${tabbifyLifelineRevertScript}";
      User = "root";
    };
  };

  # 5f. LIFELINE revert-counter reset (FIX 4). A short timer that, while the
  #     lifeline unit is active (joined), clears the lifeline-revert attempt
  #     counter so attempts can never SLOWLY accumulate across far-apart,
  #     unrelated flaps and trip a spurious rollback of a healthy joiner. The
  #     oneshot is cheap (an `rm -f`); the timer's `OnUnitActiveSec` clocks off
  #     the lifeline so it only runs once the lifeline has been up.
  systemd.services.tabbify-mesh-lifeline-reset-attempts = {
    description = "Reset the mesh lifeline revert-attempt counter while the lifeline is healthy (FIX 4)";
    # Only meaningful alongside a running lifeline.
    after = [ "tabbify-mesh-lifeline.service" ];
    path = [ pkgs.coreutils ];
    serviceConfig = {
      Type = "oneshot";
      ExecStart = "${tabbifyLifelineResetAttemptsScript}";
      User = "root";
    };
  };
  systemd.timers.tabbify-mesh-lifeline-reset-attempts = {
    description = "Periodically clear the lifeline revert-attempt counter when joined (FIX 4)";
    wantedBy = [ "timers.target" ];
    timerConfig = {
      # Clock off the lifeline being active: first reset ~10min after a join, then
      # every 30min while it stays joined. Comfortably longer than the lifeline's
      # RestartSec so a genuine crash-burst still crosses THRESHOLD before a reset.
      OnUnitActiveSec = "30min";
      OnBootSec       = "10min";
      Unit            = "tabbify-mesh-lifeline-reset-attempts.service";
    };
  };

  # 5d. Crash-at-startup auto-rollback CATCH-NET (mesh-resilience Phase 2
  #     keystone). Triggered ONLY via `OnFailure=` on tabbify-supervisor (svc #5)
  #     — NOT `wantedBy` any target. It is the no-live-process layer that reverts
  #     supervisord to the previous-good release when the start path keeps
  #     crashing (the ~31ms pre-READY exit the in-process watchdog can never see).
  #
  #     ⚠ v256 fires OnFailure PER failed attempt (~3-5×/burst): the
  #     `tabbifyBootRevertScript` is IDEMPOTENT and the durable BootAttempts
  #     counter gates the revert (only the fire that crosses the threshold acts;
  #     `reset-failed && start` is strictly gated behind a revert performed THIS
  #     fire — never on a sub-threshold fire, or the start-rate-limit counter
  #     would reset and the breaker would never trip = the 06-22 class re-armed).
  #
  #     ⚠ This oneshot itself sets StartLimitIntervalSec=0 (review fix #2): WITHOUT
  #     it the catch-net would hit its OWN default StartLimitBurst (5/10s) under
  #     the per-attempt re-triggering and get PARKED mid-incident → the catch-net
  #     goes silent exactly when it is needed.
  systemd.services.tabbify-boot-revert = {
    description = "Revert tabbify-supervisor to previous-good release on crash-at-startup";
    # Deliberately NOT wantedBy any target — only fired via OnFailure=.
    # ⚠ Disable the burst cap on the catch-net ITSELF (review fix #2) so the
    # per-attempt OnFailure re-triggering on systemd v256 can never park it
    # mid-incident. This is a `[Unit]` directive — set via the NixOS top-level
    # `startLimitIntervalSec` option (NOT `serviceConfig`, where it would land in
    # `[Service]` and be ignored).
    startLimitIntervalSec = 0;
    # systemctl (reset-failed/start), coreutils, jq (parse boot-attempts.json).
    path = [ pkgs.systemd pkgs.coreutils pkgs.jq ];
    environment = {
      # The revert subcommand + the script resolve the SAME on-host layout the
      # supervisor binary uses (symlinks+VERSION under ${dataDir}; BootAttempts
      # sidecar + the SHARED reboot-guard.json under ${dataDir}/data — one
      # host-wide ≤3/hr budget shared with Track B/C).
      SUPERVISOR_DATA_DIR  = "${dataDir}/data";
      TABBIFY_INSTALL_DIR  = dataDir;
      TABBIFY_RELEASES_DIR = releasesDir;
      RUST_LOG             = "info";
    };
    serviceConfig = {
      Type = "oneshot";
      ExecStart = "${tabbifyBootRevertScript}";
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
