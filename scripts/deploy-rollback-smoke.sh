#!/usr/bin/env bash
# Phase-2 deploy + zero-downtime swap + rollback smoke test.
#
# Verifies: pull-from-registry by ref, the deploy endpoint, the in-process
# zero-downtime swap (a tight request loop must see NO failed request across the
# flip), and rollback (deploy an older ref).
#
# Topology (all over the mesh): a supervisor is running a docker app `APP_UUID`;
# the registry holds two refs of it (V1, V2). The host's docker daemon must list
# the registry ULA under `insecure-registries` (mesh is already encrypted).
#
# Drive deploys through the NODE gateway (public API) or the supervisor control
# API directly — set DEPLOY_BASE accordingly:
#   node:        DEPLOY_BASE=http://[<node_ula>]:8090
#   supervisor:  DEPLOY_BASE=http://[<supervisor_ula>]:8730
#
# Usage:
#   APP_UUID=... APP_ULA=... DEPLOY_BASE=... V1=... V2=... ./deploy-rollback-smoke.sh
# where V1/V2 are full OCI refs, e.g. [fd5a:1f02:aa::1]:5000/acme/app:<sha>.
set -euo pipefail

APP_UUID="${APP_UUID:?set APP_UUID}"
APP_ULA="${APP_ULA:?set APP_ULA (the app mesh ULA, where it serves)}"
DEPLOY_BASE="${DEPLOY_BASE:?set DEPLOY_BASE (node or supervisor control base)}"
V1="${V1:?set V1 (OCI ref of version 1)}"
V2="${V2:?set V2 (OCI ref of version 2)}"
APP_PORT="${APP_PORT:-8730}"
APP_URL="http://[${APP_ULA}]:${APP_PORT}/"

deploy() {
  local reff="$1"
  echo "==> deploy ${reff}"
  curl -fsS -X POST "${DEPLOY_BASE}/v1/apps/${APP_UUID}/deploy" \
    -H 'content-type: application/json' \
    -d "{\"ref\":\"${reff}\"}"
  echo
}

# A tight probe loop in the background that records any non-200 across a window.
probe_loop() {
  local out="$1" secs="$2" end
  end=$(( $(date +%s) + secs ))
  : > "$out"
  while [ "$(date +%s)" -lt "$end" ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "${APP_URL}" || echo "000")
    [ "$code" = "200" ] || echo "$code" >> "$out"
  done
}

echo "### 1. deploy V1, expect the app to serve"
deploy "${V1}"
sleep 2
curl -fsS "${APP_URL}" >/dev/null && echo "OK: serving after V1 deploy"

echo "### 2. zero-downtime swap: probe loop across a V2 deploy"
FAILS="$(mktemp)"
probe_loop "${FAILS}" 12 &
PROBE_PID=$!
sleep 2
deploy "${V2}"
wait "${PROBE_PID}"
n=$(wc -l < "${FAILS}" | tr -d ' ')
if [ "${n}" = "0" ]; then
  echo "OK: zero downtime — every request during the V1->V2 swap returned 200"
else
  echo "FAIL: ${n} non-200 responses during the swap:"; sort "${FAILS}" | uniq -c
  exit 1
fi

echo "### 3. rollback: deploy V1 again"
deploy "${V1}"
sleep 2
curl -fsS "${APP_URL}" >/dev/null && echo "OK: rolled back to V1, still serving"

echo "ALL GOOD: deploy + zero-downtime swap + rollback verified"
