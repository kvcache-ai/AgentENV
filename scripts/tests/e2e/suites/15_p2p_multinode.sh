#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SUITE_DIR}/../../../.." && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "15_p2p_multinode"

log "Suite: P2P survives multi-node sandbox veth churn"

if [[ "${E2E_P2P_ENABLED:-0}" != "1" ]]; then
  warn "Skipping P2P checks because the dedicated P2P E2E mode is not enabled."
  _pass "skipped outside the dedicated P2P E2E target"
  suite_summary "15_p2p_multinode"
  exit 0
fi

if ! e2e_mode_is "compose"; then
  _fail "P2P multi-node test runs in Compose mode" "compose" "${E2E_MODE}"
  suite_summary "15_p2p_multinode"
  exit 1
fi

require_cmd docker
require_cmd jq

if [[ -z "${AENV_NODE_A_URL:-}" || -z "${AENV_NODE_B_URL:-}" ]]; then
  _fail "two AgentENV node endpoints are available" "node A and node B" "missing endpoint"
  suite_summary "15_p2p_multinode"
  exit 1
fi

NODE_A_SERVICE="${AENV_NODE_A_LABEL:-agentenv-a}"
NODE_B_SERVICE="${AENV_NODE_B_LABEL:-agentenv-b}"
P2P_GLOBAL_CONFIG="/workspace/env/overlaybd/overlaybd-global.json"
P2P_COMMIT_ROOT="/workspace/env/image-cache/commits"
P2P_SETTLE_SECONDS="${E2E_P2P_SETTLE_SECONDS:-5}"
P2P_FETCH_ATTEMPTS="${E2E_P2P_FETCH_ATTEMPTS:-45}"

sandbox_a=""
sandbox_b=""

cleanup_direct_sandboxes() {
  local status=$?
  set +e
  if [[ -n "$sandbox_a" ]]; then
    delete_sandbox_at "$AENV_NODE_A_URL" "$sandbox_a" >/dev/null 2>&1
  fi
  if [[ -n "$sandbox_b" ]]; then
    delete_sandbox_at "$AENV_NODE_B_URL" "$sandbox_b" >/dev/null 2>&1
  fi

  # helpers.sh owns the HTTP scratch files used by the node-local cleanup calls.
  _cleanup_e2e
  return "$status"
}

# The sandboxes are created against node-local APIs. Clean them up through the
# same node even when the scheduler has not reconciled their assignments yet.
trap cleanup_direct_sandboxes EXIT

compose_exec() {
  docker compose \
    -f "${REPO_ROOT}/deploy/docker-compose.yml" \
    -f "${REPO_ROOT}/scripts/tests/e2e/docker-compose.e2e.yml" \
    exec -T "$@"
}

p2p_config_value() {
  local service="$1"
  local expression="$2"
  compose_exec "$service" jq -r "$expression" "$P2P_GLOBAL_CONFIG" 2>/dev/null || true
}

veth_names() {
  local service="$1"
  compose_exec "$service" ip -o link show 2>/dev/null \
    | awk -F': ' '$2 ~ /^veth-/ { name = $2; sub(/@.*/, "", name); print name }' \
    | sort
}

assert_veth_changed() {
  local service="$1"
  local baseline="$2"
  local current
  current="$(veth_names "$service")"
  if [[ -n "$current" && "$current" != "$baseline" ]]; then
    _pass "${service} veth set changes after starting its sandbox"
  else
    _fail "${service} veth set changes after starting its sandbox" "a new veth-* interface" "${current:-<empty>}"
  fi
}

wait_for_veth_set() {
  local service="$1"
  local expected="$2"
  local timeout="${3:-20}"
  local current attempt
  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    if ! current="$(veth_names "$service")"; then
      sleep 0.5
      continue
    fi
    [[ "$current" == "$expected" ]] && return 0
    sleep 0.5
  done
  return 1
}

publish_and_fetch() {
  local provider_service="$1"
  local provider_read_address="$2"
  local consumer_service="$3"
  local consumer_read_address="$4"
  local direction="$5"
  local nonce fixture_dir fixture_path download_path publish_response_path
  local size digest_hex digest origin_url publish_url publish_request
  local origin_status publish_status publish_body fetch_status downloaded_size downloaded_digest
  local attempt

  nonce="$(date +%s%N)-${RANDOM}"
  fixture_dir="${P2P_COMMIT_ROOT}/e2e-p2p-${nonce}"
  fixture_path="${fixture_dir}/overlaybd.commit"
  download_path="/tmp/e2e-p2p-${nonce}.download"
  publish_response_path="/tmp/e2e-p2p-${nonce}.publish-response"

  compose_exec "$provider_service" sh -c \
    'mkdir -p "$1" && dd if=/dev/urandom of="$2" bs=4096 count=16 status=none' \
    sh "$fixture_dir" "$fixture_path"

  size="$(compose_exec "$provider_service" stat -c %s "$fixture_path" | tr -d '\r\n')"
  digest_hex="$(compose_exec "$provider_service" sha256sum "$fixture_path" | awk '{print $1}')"
  digest="sha256:${digest_hex}"

  if [[ "$size" =~ ^[0-9]+$ ]] && ((size > 0)); then
    _pass "${direction} provider created a non-empty layer fixture"
  else
    _fail "${direction} provider created a non-empty layer fixture" "positive size" "${size:-empty}"
    return 0
  fi
  if [[ "$digest_hex" =~ ^[0-9a-f]{64}$ ]]; then
    _pass "${direction} provider fixture has a SHA256 digest"
  else
    _fail "${direction} provider fixture has a SHA256 digest" "64 lowercase hex characters" "$digest_hex"
    return 0
  fi

  origin_url="http://127.0.0.1:1/v2/e2e/blobs/${digest}"
  origin_status="$(
    compose_exec "$consumer_service" curl -sS \
      --connect-timeout 1 \
      --max-time 2 \
      -o /dev/null \
      -w '%{http_code}' \
      "$origin_url" 2>/dev/null || true
  )"
  assert_eq "$origin_status" "000" "${direction} origin is unreachable, preventing HTTP fallback"

  publish_url="${provider_read_address%/p2p-http}/p2p-control/publish-layer"
  publish_request="$(jq -nc \
    --arg path "$fixture_path" \
    --arg digest "$digest" \
    --argjson size "$size" \
    --arg source_url "$origin_url" \
    '{path: $path, digest: $digest, size: $size, source_url: $source_url}')"
  publish_status="$(
    compose_exec "$provider_service" curl -sS \
      --max-time 30 \
      -o "$publish_response_path" \
      -w '%{http_code}' \
      -H 'Content-Type: application/json' \
      --data "$publish_request" \
      "$publish_url" 2>/dev/null || true
  )"
  assert_status "$publish_status" "200" "${direction} provider publishes the layer through P2P"
  if [[ "$publish_status" != "200" ]]; then
    publish_body="$(compose_exec "$provider_service" cat "$publish_response_path" 2>/dev/null || true)"
    warn "${direction} P2P publication response: ${publish_body:-<empty>}"
    return 0
  fi

  fetch_status=""
  for ((attempt = 1; attempt <= P2P_FETCH_ATTEMPTS; attempt++)); do
    fetch_status="$(
      compose_exec "$consumer_service" curl -sS \
        --path-as-is \
        --max-time 5 \
        -o "$download_path" \
        -w '%{http_code}' \
        -H "Range: bytes=0-$((size - 1))" \
        "${consumer_read_address}/${origin_url}" 2>/dev/null || true
    )"
    [[ "$fetch_status" == "206" ]] && break
    sleep 1
  done
  assert_status "$fetch_status" "206" "${direction} consumer fetches the layer from its peer"
  if [[ "$fetch_status" != "206" ]]; then
    warn "${direction} P2P fetch did not succeed after ${P2P_FETCH_ATTEMPTS} attempts."
    return 0
  fi

  downloaded_size="$(compose_exec "$consumer_service" stat -c %s "$download_path" | tr -d '\r\n')"
  downloaded_digest="$(compose_exec "$consumer_service" sha256sum "$download_path" | awk '{print $1}')"
  assert_eq "$downloaded_size" "$size" "${direction} P2P transfer preserves layer size"
  assert_eq "$downloaded_digest" "$digest_hex" "${direction} P2P transfer preserves layer SHA256"

  compose_exec "$provider_service" rm -f "$fixture_path" "$publish_response_path"
  compose_exec "$provider_service" rmdir "$fixture_dir"
  compose_exec "$consumer_service" rm -f "$download_path"
}

log "Waiting for veth cleanup from the base-template build ..."
if ! wait_for_veth_set "$NODE_A_SERVICE" "" 30; then
  _fail "${NODE_A_SERVICE} clears base-template veths" "an empty veth set" "$(veth_names "$NODE_A_SERVICE" 2>/dev/null || true)"
fi
if ! wait_for_veth_set "$NODE_B_SERVICE" "" 30; then
  _fail "${NODE_B_SERVICE} clears base-template veths" "an empty veth set" "$(veth_names "$NODE_B_SERVICE" 2>/dev/null || true)"
fi

veth_baseline_a="$(veth_names "$NODE_A_SERVICE")"
veth_baseline_b="$(veth_names "$NODE_B_SERVICE")"
assert_eq "$veth_baseline_a" "" "${NODE_A_SERVICE} starts without prewarmed sandbox veths"
assert_eq "$veth_baseline_b" "" "${NODE_B_SERVICE} starts without prewarmed sandbox veths"

log "Starting one Firecracker sandbox on each AgentENV node ..."
sandbox_a="$(create_sandbox_at "$AENV_NODE_A_URL" "$AENV_TEMPLATE_ID" 600)"; _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox on ${NODE_A_SERVICE}"
assert_not_empty "$sandbox_a" "sandboxID present on ${NODE_A_SERVICE}"
if [[ "$HTTP_STATUS" != "201" || -z "$sandbox_a" ]]; then
  suite_summary "15_p2p_multinode"
  exit 1
fi

sandbox_b="$(create_sandbox_at "$AENV_NODE_B_URL" "$AENV_TEMPLATE_ID" 600)"; _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox on ${NODE_B_SERVICE}"
assert_not_empty "$sandbox_b" "sandboxID present on ${NODE_B_SERVICE}"
if [[ "$HTTP_STATUS" != "201" || -z "$sandbox_b" ]]; then
  suite_summary "15_p2p_multinode"
  exit 1
fi
assert_not_eq "$sandbox_a" "$sandbox_b" "the two Firecracker sandboxes have distinct IDs"

if wait_for_sandbox_state_at "$AENV_NODE_A_URL" "$sandbox_a" "running" 60; then
  _pass "sandbox on ${NODE_A_SERVICE} reaches running state"
else
  _fail "sandbox on ${NODE_A_SERVICE} reaches running state" "running" "timed out"
fi
if wait_for_sandbox_state_at "$AENV_NODE_B_URL" "$sandbox_b" "running" 60; then
  _pass "sandbox on ${NODE_B_SERVICE} reaches running state"
else
  _fail "sandbox on ${NODE_B_SERVICE} reaches running state" "running" "timed out"
fi

log "Waiting ${P2P_SETTLE_SECONDS}s for sandbox veth link changes to reach netwatch ..."
sleep "$P2P_SETTLE_SECONDS"
assert_veth_changed "$NODE_A_SERVICE" "$veth_baseline_a"
assert_veth_changed "$NODE_B_SERVICE" "$veth_baseline_b"

p2p_enabled_a="$(p2p_config_value "$NODE_A_SERVICE" '.p2pConfig.enable // false')"
p2p_enabled_b="$(p2p_config_value "$NODE_B_SERVICE" '.p2pConfig.enable // false')"
p2p_address_a="$(p2p_config_value "$NODE_A_SERVICE" '.p2pConfig.address // empty')"
p2p_address_b="$(p2p_config_value "$NODE_B_SERVICE" '.p2pConfig.address // empty')"
assert_eq "$p2p_enabled_a" "true" "P2P facade is enabled on ${NODE_A_SERVICE}"
assert_eq "$p2p_enabled_b" "true" "P2P facade is enabled on ${NODE_B_SERVICE}"
assert_not_empty "$p2p_address_a" "P2P facade address is configured on ${NODE_A_SERVICE}"
assert_not_empty "$p2p_address_b" "P2P facade address is configured on ${NODE_B_SERVICE}"
if [[ "$p2p_enabled_a" != "true" || "$p2p_enabled_b" != "true" || -z "$p2p_address_a" || -z "$p2p_address_b" ]]; then
  suite_summary "15_p2p_multinode"
  exit 1
fi

publish_and_fetch \
  "$NODE_A_SERVICE" "$p2p_address_a" \
  "$NODE_B_SERVICE" "$p2p_address_b" \
  "node A to node B after sandbox creation"

log "Deleting both sandboxes to trigger another round of veth link changes ..."
delete_sandbox_at "$AENV_NODE_A_URL" "$sandbox_a"
assert_status "$HTTP_STATUS" "204" "delete sandbox on ${NODE_A_SERVICE}"
[[ "$HTTP_STATUS" == "204" ]] && sandbox_a=""
delete_sandbox_at "$AENV_NODE_B_URL" "$sandbox_b"
assert_status "$HTTP_STATUS" "204" "delete sandbox on ${NODE_B_SERVICE}"
[[ "$HTTP_STATUS" == "204" ]] && sandbox_b=""

log "Waiting ${P2P_SETTLE_SECONDS}s for sandbox veth removal to reach netwatch ..."
sleep "$P2P_SETTLE_SECONDS"
if wait_for_veth_set "$NODE_A_SERVICE" "$veth_baseline_a"; then
  _pass "${NODE_A_SERVICE} veth set returns to baseline after sandbox deletion"
else
  _fail "${NODE_A_SERVICE} veth set returns to baseline after sandbox deletion" "$veth_baseline_a" "$(veth_names "$NODE_A_SERVICE")"
fi
if wait_for_veth_set "$NODE_B_SERVICE" "$veth_baseline_b"; then
  _pass "${NODE_B_SERVICE} veth set returns to baseline after sandbox deletion"
else
  _fail "${NODE_B_SERVICE} veth set returns to baseline after sandbox deletion" "$veth_baseline_b" "$(veth_names "$NODE_B_SERVICE")"
fi

publish_and_fetch \
  "$NODE_B_SERVICE" "$p2p_address_b" \
  "$NODE_A_SERVICE" "$p2p_address_a" \
  "node B to node A after sandbox deletion"

suite_summary "15_p2p_multinode"
