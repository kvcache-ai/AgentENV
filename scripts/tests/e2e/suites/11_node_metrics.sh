#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "11_node_metrics"

log "Suite: Node Metrics"

readonly NODE_METRICS_SANDBOX_TIMEOUT_SECONDS=60
readonly SCHEDULER_BINDING_CLEANUP_TIMEOUT_SECONDS=75
readonly PROXY_HEALTH_PATH="/health"
readonly PROXY_HEALTH_PORT="${AENV_ENVD_PORT}"

wait_for_global_sandboxes_quiesced() {
  local timeout="${1:-20}"
  local attempt
  local count

  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    api_get "/v2/sandboxes?limit=1"
    if [[ "${HTTP_STATUS}" == "200" ]]; then
      count="$(echo "${HTTP_BODY}" | jq 'length' 2>/dev/null || echo -1)"
      if [[ "${count}" == "0" ]]; then
        return 0
      fi
    fi
    sleep 0.5
  done

  warn "Timed out waiting for /v2/sandboxes to become empty before baseline capture"
  return 1
}

cleanup_lingering_sandboxes() {
  local sandbox_ids
  local sandbox_id

  api_get "/v2/sandboxes?limit=100"
  [[ "${HTTP_STATUS}" == "200" ]] || return 1

  sandbox_ids="$(echo "${HTTP_BODY}" | jq -r '.[].sandboxID // empty' 2>/dev/null || true)"
  [[ -n "${sandbox_ids}" ]] || return 0

  warn "Found lingering sandboxes from earlier suites; deleting before capturing node metrics baseline"
  while IFS= read -r sandbox_id; do
    [[ -n "${sandbox_id}" ]] || continue
    delete_sandbox "${sandbox_id}"
  done <<< "${sandbox_ids}"
}

wait_for_node_runtime_allocations_quiesced() {
  local timeout="${1:-20}"
  local attempt

  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    api_get "/nodes"
    if [[ "${HTTP_STATUS}" == "200" ]] &&
      echo "${HTTP_BODY}" | jq -e 'all(.[]; (.sandboxCount == 0 and .metrics.allocatedCPU == 0 and .metrics.allocatedMemoryBytes == 0))' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done

  warn "Timed out waiting for /nodes runtime allocations to settle before baseline capture"
  return 1
}

wait_for_scheduler_binding_cleanup() {
  local sandbox_id="$1"
  local timeout="${2:-${SCHEDULER_BINDING_CLEANUP_TIMEOUT_SECONDS}}"
  local attempt

  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    proxy_get_with_sandbox "${sandbox_id}" "${PROXY_HEALTH_PATH}" "${PROXY_HEALTH_PORT}"
    if [[ "${HTTP_STATUS}" == "404" ]] &&
      [[ "${HTTP_BODY}" == *"sandbox assignment not found"* ]]; then
      return 0
    fi
    sleep 0.5
  done

  warn "Timed out waiting for scheduler binding cleanup for sandbox ${sandbox_id}"
  return 1
}

quiesce_runtime_state_before_baseline() {
  if wait_for_global_sandboxes_quiesced 10 && wait_for_node_runtime_allocations_quiesced 10; then
    return 0
  fi

  cleanup_lingering_sandboxes || true
  wait_for_global_sandboxes_quiesced 30 || return 1
  wait_for_node_runtime_allocations_quiesced 30
}

fetch_admin_nodes() {
  local base_url="${1:-${AENV_URL}}"
  api_get_at "${base_url}" "/nodes"
  [[ "${HTTP_STATUS}" == "200" ]] || return 1
  printf '%s\n' "${HTTP_BODY}"
}

wait_for_admin_nodes_count() {
  local base_url="$1"
  local min_count="$2"
  local timeout="${3:-45}"
  local attempt
  local count

  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    if fetch_admin_nodes "${base_url}" >/dev/null; then
      count="$(echo "${HTTP_BODY}" | jq 'length' 2>/dev/null || echo 0)"
      if [[ "${count}" -ge "${min_count}" ]]; then
        return 0
      fi
    fi
    sleep 0.5
  done

  warn "Timed out waiting for ${base_url}/nodes to expose at least ${min_count} nodes"
  return 1
}

wait_for_node_snapshot() {
  local node_id="$1"
  local expected_sandbox_count="$2"
  local expected_allocated_cpu="$3"
  local expected_allocated_memory="$4"
  local expected_create_successes_min="$5"
  local timeout="${6:-45}"
  local attempt
  local body
  local sandbox_count
  local allocated_cpu
  local allocated_memory
  local create_successes

  for ((attempt = 0; attempt < timeout * 2; attempt++)); do
    api_get "/nodes"
    if [[ "${HTTP_STATUS}" == "200" ]]; then
      body="${HTTP_BODY}"
      sandbox_count="$(echo "${body}" | jq -r --arg id "${node_id}" '.[] | select(.id == $id) | .sandboxCount // empty' 2>/dev/null || true)"
      allocated_cpu="$(echo "${body}" | jq -r --arg id "${node_id}" '.[] | select(.id == $id) | .metrics.allocatedCPU // empty' 2>/dev/null || true)"
      allocated_memory="$(echo "${body}" | jq -r --arg id "${node_id}" '.[] | select(.id == $id) | .metrics.allocatedMemoryBytes // empty' 2>/dev/null || true)"
      create_successes="$(echo "${body}" | jq -r --arg id "${node_id}" '.[] | select(.id == $id) | .createSuccesses // empty' 2>/dev/null || true)"
      if [[ "${sandbox_count}" == "${expected_sandbox_count}" &&
            "${allocated_cpu}" == "${expected_allocated_cpu}" &&
            "${allocated_memory}" == "${expected_allocated_memory}" &&
            -n "${create_successes}" &&
            "${create_successes}" =~ ^[0-9]+$ &&
            "${create_successes}" -ge "${expected_create_successes_min}" ]]; then
        return 0
      fi
    fi
    sleep 0.5
  done

  warn "Timed out waiting for node ${node_id} snapshot to reach expected values"
  return 1
}

node_detail_sandbox_count() {
  local base_url="$1"
  local node_id="$2"
  api_get_at "${base_url}" "/nodes/${node_id}"
  [[ "${HTTP_STATUS}" == "200" ]] || return 1
  echo "${HTTP_BODY}" | jq '.sandboxCount'
}

declare -A BASELINE_SANDBOX_COUNT=()
declare -A BASELINE_ALLOCATED_CPU=()
declare -A BASELINE_ALLOCATED_MEMORY=()
declare -A BASELINE_CREATE_SUCCESSES=()
declare -A BASELINE_DETAIL_SANDBOX_COUNT=()
declare -A NODE_URL_BY_ID=()
declare -A NODE_LABEL_BY_ID=()
declare -A OWNED_SANDBOX_COUNT=()
declare -A OWNED_ALLOCATED_CPU_DELTA=()
declare -A OWNED_ALLOCATED_MEMORY_DELTA=()

if e2e_mode_is_clustered; then
  expected_nodes=$(candidate_node_urls | sed '/^$/d' | wc -l | tr -d ' ')
else
  expected_nodes=1
fi

quiesce_runtime_state_before_baseline ||
  die "Timed out waiting for runtime sandbox state to quiesce before node metrics checks"

wait_for_admin_nodes_count "${AENV_URL}" "${expected_nodes}" 60 ||
  die "Timed out waiting for gateway/admin nodes endpoint"

api_get "/nodes"
assert_status "${HTTP_STATUS}" "200" "admin /nodes returns 200"
baseline_nodes_json="${HTTP_BODY}"

node_count="$(echo "${baseline_nodes_json}" | jq 'length')"
assert_eq "${node_count}" "${expected_nodes}" "admin /nodes exposes expected node count"

while IFS=$'\t' read -r node_id sandbox_count allocated_cpu allocated_memory create_successes; do
  [[ -n "${node_id}" ]] || continue
  BASELINE_SANDBOX_COUNT["${node_id}"]="${sandbox_count}"
  BASELINE_ALLOCATED_CPU["${node_id}"]="${allocated_cpu}"
  BASELINE_ALLOCATED_MEMORY["${node_id}"]="${allocated_memory}"
  BASELINE_CREATE_SUCCESSES["${node_id}"]="${create_successes}"

  api_get "/nodes/${node_id}"
  assert_status "${HTTP_STATUS}" "200" "admin /nodes/${node_id} returns 200"
  BASELINE_DETAIL_SANDBOX_COUNT["${node_id}"]="$(echo "${HTTP_BODY}" | jq '.sandboxCount')"
done < <(echo "${baseline_nodes_json}" | jq -r '.[] | [
  .id,
  (.sandboxCount | tostring),
  (.metrics.allocatedCPU | tostring),
  (.metrics.allocatedMemoryBytes | tostring),
  (.createSuccesses | tostring)
] | @tsv')

if e2e_mode_is_clustered; then
  while IFS= read -r node_url; do
    [[ -n "${node_url}" ]] || continue
    wait_for_admin_nodes_count "${node_url}" 1 45 ||
      die "Timed out waiting for ${node_url}/nodes"
    api_get_at "${node_url}" "/nodes"
    assert_status "${HTTP_STATUS}" "200" "node-local admin /nodes returns 200 for $(node_label_for_url "${node_url}")"
    local_node_id="$(echo "${HTTP_BODY}" | jq -r '.[0].id')"
    assert_not_empty "${local_node_id}" "node-local admin /nodes exposes node id for $(node_label_for_url "${node_url}")"
    NODE_URL_BY_ID["${local_node_id}"]="${node_url}"
    NODE_LABEL_BY_ID["${local_node_id}"]="$(node_label_for_url "${node_url}")"
    OWNED_SANDBOX_COUNT["${local_node_id}"]=0
    OWNED_ALLOCATED_CPU_DELTA["${local_node_id}"]=0
    OWNED_ALLOCATED_MEMORY_DELTA["${local_node_id}"]=0
  done < <(candidate_node_urls)
else
  single_node_id="$(echo "${baseline_nodes_json}" | jq -r '.[0].id')"
  assert_not_empty "${single_node_id}" "single-node admin /nodes exposes node id"
  NODE_URL_BY_ID["${single_node_id}"]="${AENV_URL}"
  NODE_LABEL_BY_ID["${single_node_id}"]="local-node"
  OWNED_SANDBOX_COUNT["${single_node_id}"]=0
  OWNED_ALLOCATED_CPU_DELTA["${single_node_id}"]=0
  OWNED_ALLOCATED_MEMORY_DELTA["${single_node_id}"]=0
fi

create_count=1
if e2e_mode_is_clustered && [[ "${node_count}" -gt 1 ]]; then
  create_count=2
fi

created_sandboxes=()
for idx in $(seq 1 "${create_count}"); do
  sandbox_id="$(create_sandbox "${AENV_TEMPLATE_ID}" "${NODE_METRICS_SANDBOX_TIMEOUT_SECONDS}" "{\"metadata\":{\"suite\":\"node-metrics\",\"slot\":\"${idx}\",\"run\":\"$(date +%s%N)\"}}")"
  _sync_http
  assert_status "${HTTP_STATUS}" "201" "create sandbox #${idx} for metrics validation"
  assert_not_empty "${sandbox_id}" "sandbox #${idx} ID is present"
  track_sandbox "${sandbox_id}"
  created_sandboxes+=("${sandbox_id}")
done

for sandbox_id in "${created_sandboxes[@]}"; do
  wait_for_sandbox_state "${sandbox_id}" "running" 30
done

for sandbox_id in "${created_sandboxes[@]}"; do
  if e2e_mode_is_clustered; then
    owner_url="$(find_sandbox_node_url "${sandbox_id}" || true)"
    assert_not_empty "${owner_url}" "sandbox ${sandbox_id} resolves to an owning node"

    owner_label="$(node_label_for_url "${owner_url}")"
    owner_id=""
    for candidate_node_id in "${!NODE_URL_BY_ID[@]}"; do
      if [[ "${NODE_URL_BY_ID[${candidate_node_id}]}" == "${owner_url}" ]]; then
        owner_id="${candidate_node_id}"
        break
      fi
    done
  else
    owner_url="${AENV_URL}"
    owner_label="local-node"
    owner_id="${single_node_id}"
  fi

  assert_not_empty "${owner_id}" "sandbox ${sandbox_id} owner id resolves from ${owner_label}"

  api_get "/sandboxes/${sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" "sandbox ${sandbox_id} detail returns 200 for resource attribution"
  sandbox_cpu="$(echo "${HTTP_BODY}" | jq -r '.cpuCount // empty')"
  sandbox_memory_mb="$(echo "${HTTP_BODY}" | jq -r '.memoryMB // empty')"
  assert_not_empty "${sandbox_cpu}" "sandbox ${sandbox_id} exposes cpuCount"
  assert_not_empty "${sandbox_memory_mb}" "sandbox ${sandbox_id} exposes memoryMB"
  sandbox_memory_bytes=$((sandbox_memory_mb * 1024 * 1024))

  current_owned_count="${OWNED_SANDBOX_COUNT[${owner_id}]:-0}"
  OWNED_SANDBOX_COUNT["${owner_id}"]=$((current_owned_count + 1))
  current_owned_cpu_delta="${OWNED_ALLOCATED_CPU_DELTA[${owner_id}]:-0}"
  OWNED_ALLOCATED_CPU_DELTA["${owner_id}"]=$((current_owned_cpu_delta + sandbox_cpu))
  current_owned_memory_delta="${OWNED_ALLOCATED_MEMORY_DELTA[${owner_id}]:-0}"
  OWNED_ALLOCATED_MEMORY_DELTA["${owner_id}"]=$((current_owned_memory_delta + sandbox_memory_bytes))
done

for node_id in "${!BASELINE_SANDBOX_COUNT[@]}"; do
  owned_count="${OWNED_SANDBOX_COUNT[${node_id}]:-0}"
  owned_cpu_delta="${OWNED_ALLOCATED_CPU_DELTA[${node_id}]:-0}"
  owned_memory_delta="${OWNED_ALLOCATED_MEMORY_DELTA[${node_id}]:-0}"
  expected_sandbox_count=$((BASELINE_SANDBOX_COUNT["${node_id}"] + owned_count))
  expected_allocated_cpu=$((BASELINE_ALLOCATED_CPU["${node_id}"] + owned_cpu_delta))
  expected_allocated_memory=$((BASELINE_ALLOCATED_MEMORY["${node_id}"] + owned_memory_delta))
  expected_create_successes_min=$((BASELINE_CREATE_SUCCESSES["${node_id}"] + owned_count))

  if wait_for_node_snapshot \
      "${node_id}" \
      "${expected_sandbox_count}" \
      "${expected_allocated_cpu}" \
      "${expected_allocated_memory}" \
      "${expected_create_successes_min}" \
      30; then
    _pass "gateway /nodes metrics converge for ${node_id}"
  else
    api_get "/nodes"
    _fail \
      "gateway /nodes metrics converge for ${node_id}" \
      "sandboxCount=${expected_sandbox_count}, allocatedCPU=${expected_allocated_cpu}, allocatedMemoryBytes=${expected_allocated_memory}, createSuccesses>=${expected_create_successes_min}" \
      "${HTTP_BODY}"
  fi

  api_get "/nodes/${node_id}"
  assert_status "${HTTP_STATUS}" "200" "gateway /nodes/${node_id} returns 200 after workload"
  detail_sandboxes="$(echo "${HTTP_BODY}" | jq '.sandboxCount')"
  expected_detail_sandboxes=$((BASELINE_DETAIL_SANDBOX_COUNT["${node_id}"] + owned_count))
  assert_eq "${detail_sandboxes}" "${expected_detail_sandboxes}" "gateway /nodes/${node_id} detail reflects running sandboxes"
  detail_allocated_cpu="$(echo "${HTTP_BODY}" | jq -r '.metrics.allocatedCPU')"
  assert_eq "${detail_allocated_cpu}" "${expected_allocated_cpu}" "gateway /nodes/${node_id} detail reflects allocated CPU"
  detail_allocated_memory="$(echo "${HTTP_BODY}" | jq -r '.metrics.allocatedMemoryBytes')"
  assert_eq "${detail_allocated_memory}" "${expected_allocated_memory}" "gateway /nodes/${node_id} detail reflects allocated memory"

  if [[ -n "${NODE_URL_BY_ID[${node_id}]:-}" ]]; then
    local_detail_sandboxes="$(node_detail_sandbox_count "${NODE_URL_BY_ID[${node_id}]}" "${node_id}")"
    assert_eq "${local_detail_sandboxes}" "${expected_detail_sandboxes}" "node-local /nodes/${node_id} detail reflects running sandboxes"
  fi
done

for sandbox_id in "${created_sandboxes[@]}"; do
  delete_sandbox "${sandbox_id}"
  assert_status "${HTTP_STATUS}" "204" "delete sandbox ${sandbox_id} after metrics validation"
done

if e2e_mode_is_clustered; then
  for sandbox_id in "${created_sandboxes[@]}"; do
    if wait_for_scheduler_binding_cleanup "${sandbox_id}"; then
      _pass "scheduler binding cleanup removes routing for sandbox ${sandbox_id}"
    else
      _fail \
        "scheduler binding cleanup removes routing for sandbox ${sandbox_id}" \
        "HTTP 404 containing sandbox assignment not found" \
        "HTTP ${HTTP_STATUS}: ${HTTP_BODY}"
    fi
  done
else
  _pass "scheduler binding cleanup check skipped outside clustered modes"
fi

for node_id in "${!BASELINE_SANDBOX_COUNT[@]}"; do
  expected_create_successes_min_after_cleanup=$((BASELINE_CREATE_SUCCESSES["${node_id}"] + ${OWNED_SANDBOX_COUNT[${node_id}]:-0}))
  if wait_for_node_snapshot \
      "${node_id}" \
      "${BASELINE_SANDBOX_COUNT[${node_id}]}" \
      "${BASELINE_ALLOCATED_CPU[${node_id}]}" \
      "${BASELINE_ALLOCATED_MEMORY[${node_id}]}" \
      "${expected_create_successes_min_after_cleanup}" \
      30; then
    _pass "gateway /nodes runtime allocation resets for ${node_id} after cleanup"
  else
    api_get "/nodes"
    _fail \
      "gateway /nodes runtime allocation resets for ${node_id} after cleanup" \
      "sandboxCount=${BASELINE_SANDBOX_COUNT[${node_id}]}, allocatedCPU=${BASELINE_ALLOCATED_CPU[${node_id}]}, allocatedMemoryBytes=${BASELINE_ALLOCATED_MEMORY[${node_id}]}, createSuccesses>=${expected_create_successes_min_after_cleanup}" \
      "${HTTP_BODY}"
  fi
done

suite_summary "11_node_metrics"
