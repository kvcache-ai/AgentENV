#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "16_volume_randomized"

log "Suite: Randomized Multi-Node Volume Lifecycle"

if ! e2e_mode_is compose; then
  warn "Skipping randomized multi-node volume checks outside Docker Compose mode."
  _pass "skipped outside Docker Compose mode"
  suite_summary "16_volume_randomized"
  exit 0
fi

readonly VOLUME_RANDOM_SEED="${AENV_VOLUME_RANDOM_SEED:-21106}"
readonly VOLUME_RANDOM_STEPS="${AENV_VOLUME_RANDOM_STEPS:-100}"
readonly VOLUME_SIZE_MB=16
readonly VOLUME_MOUNT_PATH="/volume"

RANDOM=$((VOLUME_RANDOM_SEED & 32767))
CURRENT_STEP="setup"
NEXT_VOLUME_NAME=0
LAST_VOLUME_ID=""
LAST_SANDBOX_ID=""
LAST_NODE_ID=""
SELECTED_VOLUME_ID=""

random_file="$(mktemp "${TMPDIR:-/tmp}/aenv-volume-random.XXXXXX")"
run_name="volume-random-${VOLUME_RANDOM_SEED}-$(date +%s%N)"

declare -A VOLUME_MODE=()
declare -A VOLUME_CONTENT=()
declare -A VOLUME_NODE_KEYS=()
declare -A SEEN_NODES=()
ALL_VOLUME_IDS=()

cleanup_randomized_volume_e2e() {
  local status=$?
  local sandbox_id volume_id

  for sandbox_id in "${_TRACKED_SANDBOX_IDS[@]}"; do
    api_delete "/sandboxes/${sandbox_id}" 2>/dev/null || true
  done
  for volume_id in "${ALL_VOLUME_IDS[@]}"; do
    [[ -n "${volume_id}" ]] || continue
    api_delete "/volumes/${volume_id}" 2>/dev/null || true
  done
  rm -f "${random_file}"
  _cleanup_e2e || true
  return "${status}"
}
trap cleanup_randomized_volume_e2e EXIT

response_node_id() {
  printf '%s\n' "${HTTP_HEADERS:-}" | awk -F': *' \
    'tolower($1) == "x-agentenv-node-id" {gsub("\\r", "", $2); print $2; exit}'
}

register_volume() {
  local volume_id="$1"
  local mode="$2"
  local content="$3"
  VOLUME_MODE["${volume_id}"]="${mode}"
  VOLUME_CONTENT["${volume_id}"]="${content}"
  ALL_VOLUME_IDS+=("${volume_id}")
}

active_volume_ids() {
  printf '%s\n' "${!VOLUME_MODE[@]}" | sed '/^$/d' | sort
}

select_random_volume() {
  local required_mode="${1:-}"
  local candidates=()
  local volume_id
  while IFS= read -r volume_id; do
    [[ -n "${volume_id}" ]] || continue
    if [[ -z "${required_mode}" || "${VOLUME_MODE[${volume_id}]}" == "${required_mode}" ]]; then
      candidates+=("${volume_id}")
    fi
  done < <(active_volume_ids)
  [[ "${#candidates[@]}" -gt 0 ]] || return 1
  SELECTED_VOLUME_ID="${candidates[RANDOM % ${#candidates[@]}]}"
}

create_empty_volume() {
  local name="${run_name}-${NEXT_VOLUME_NAME}"
  NEXT_VOLUME_NAME=$((NEXT_VOLUME_NAME + 1))
  api_post "/volumes" "$(jq -nc \
    --arg name "${name}" \
    --argjson size "${VOLUME_SIZE_MB}" \
    '{name: $name, sizeMB: $size, mode: "exclusive"}')"
  assert_status "${HTTP_STATUS}" "201" "${CURRENT_STEP}: create exclusive volume"
  LAST_VOLUME_ID="$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')"
  assert_not_empty "${LAST_VOLUME_ID}" "${CURRENT_STEP}: created volume ID is present"
  assert_json_field "${HTTP_BODY}" '.status' "ready" \
    "${CURRENT_STEP}: created volume is ready"
  register_volume "${LAST_VOLUME_ID}" "exclusive" ""
  log "seed=${VOLUME_RANDOM_SEED} step=${CURRENT_STEP} created=${LAST_VOLUME_ID}"
}

clone_volume() {
  local source_id="$1"
  local mode="$2"
  local name="${run_name}-${NEXT_VOLUME_NAME}"
  NEXT_VOLUME_NAME=$((NEXT_VOLUME_NAME + 1))
  api_post "/volumes" "$(jq -nc \
    --arg name "${name}" \
    --arg source "${source_id}" \
    --arg mode "${mode}" \
    --argjson size "${VOLUME_SIZE_MB}" \
    '{name: $name, sizeMB: $size, mode: $mode, fromVolume: $source}')"
  assert_status "${HTTP_STATUS}" "201" "${CURRENT_STEP}: clone ${mode} volume"
  LAST_VOLUME_ID="$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')"
  assert_not_empty "${LAST_VOLUME_ID}" "${CURRENT_STEP}: cloned volume ID is present"
  assert_json_field "${HTTP_BODY}" '.status' "ready" \
    "${CURRENT_STEP}: cloned volume is ready"
  register_volume "${LAST_VOLUME_ID}" "${mode}" "${VOLUME_CONTENT[${source_id}]}"
  log "seed=${VOLUME_RANDOM_SEED} step=${CURRENT_STEP} cloned=${LAST_VOLUME_ID} source=${source_id} mode=${mode}"
}

upload_random_file() {
  local sandbox_id="$1"
  local path="${2:-${VOLUME_MOUNT_PATH}/state.txt}"
  local encoded_path
  encoded_path=$(jq -rn --arg path "${path}" '$path|@uri')
  _curl_do -s -X POST \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    -F "file=@${random_file}" \
    "${AENV_PROXY_URL}/files?path=${encoded_path}"
}

download_random_file() {
  local sandbox_id="$1"
  local encoded_path
  encoded_path=$(jq -rn --arg path "${VOLUME_MOUNT_PATH}/state.txt" '$path|@uri')
  _curl_do -s \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    "${AENV_PROXY_URL}/files?path=${encoded_path}"
}

capture_sandbox_node() {
  local sandbox_id="$1"
  local volume_id="$2"
  api_get_with_headers "/sandboxes/${sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" "${CURRENT_STEP}: gateway routes sandbox details"
  LAST_NODE_ID="$(response_node_id)"
  assert_not_empty "${LAST_NODE_ID}" "${CURRENT_STEP}: gateway identifies sandbox backend"
  SEEN_NODES["${LAST_NODE_ID}"]=1
  VOLUME_NODE_KEYS["${volume_id}|${LAST_NODE_ID}"]=1
}

start_volume_sandbox() {
  local volume_id="$1"
  local mount_payload
  mount_payload=$(jq -nc \
    --arg volume_id "${volume_id}" \
    --arg suite "${run_name}" \
    --arg step "${CURRENT_STEP}" \
    --arg mount_path "${VOLUME_MOUNT_PATH}" \
    '{autoPause: false, metadata: {suite: $suite, step: $step}, volumeMounts: {($mount_path): $volume_id}}')
  LAST_SANDBOX_ID=$(create_sandbox "${AENV_TEMPLATE_ID}" 300 "${mount_payload}")
  _sync_http
  assert_status "${HTTP_STATUS}" "201" "${CURRENT_STEP}: create volume sandbox through gateway"
  if [[ "${HTTP_STATUS}" != "201" ]]; then
    error "${CURRENT_STEP}: sandbox create response: ${HTTP_BODY}"
    return 1
  fi
  assert_not_empty "${LAST_SANDBOX_ID}" "${CURRENT_STEP}: sandbox ID is present"
  track_sandbox "${LAST_SANDBOX_ID}"
  capture_sandbox_node "${LAST_SANDBOX_ID}" "${volume_id}"
}

verify_volume_content() {
  local sandbox_id="$1"
  local volume_id="$2"
  local expected="${VOLUME_CONTENT[${volume_id}]}"
  [[ -n "${expected}" ]] || return 0
  download_random_file "${sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" "${CURRENT_STEP}: read modeled volume data"
  assert_contains "${HTTP_BODY}" "${expected}" \
    "${CURRENT_STEP}: guest data matches the model"
}

write_new_volume_content() {
  local sandbox_id="$1"
  local volume_id="$2"
  local phase="$3"
  local token="seed-${VOLUME_RANDOM_SEED}-${CURRENT_STEP}-${phase}-${volume_id}"
  printf '%s\n' "${token}" >"${random_file}"
  upload_random_file "${sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" "${CURRENT_STEP}: write modeled guest data"
  VOLUME_CONTENT["${volume_id}"]="${token}"
  verify_volume_content "${sandbox_id}" "${volume_id}"
}

assert_read_only_write_fails() {
  local sandbox_id="$1"
  printf 'read-only-write-must-fail\n' >"${random_file}"
  upload_random_file "${sandbox_id}" "${VOLUME_MOUNT_PATH}/write-must-fail.txt"
  if [[ "${HTTP_STATUS}" == "200" ]]; then
    _fail "${CURRENT_STEP}: read-only guest write is rejected" "non-200" "${HTTP_STATUS}"
  else
    _pass "${CURRENT_STEP}: read-only guest write is rejected"
  fi
}

pause_and_resume_sandbox() {
  local sandbox_id="$1"
  api_post "/sandboxes/${sandbox_id}/pause"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: pause volume sandbox"
  wait_for_sandbox_state "${sandbox_id}" "paused" 30 ||
    _fail "${CURRENT_STEP}: sandbox reaches paused state" "paused" "timeout"
  api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":300}'
  assert_status "${HTTP_STATUS}" "201" "${CURRENT_STEP}: resume volume sandbox"
  wait_for_sandbox_state "${sandbox_id}" "running" 30 ||
    _fail "${CURRENT_STEP}: sandbox reaches running state" "running" "timeout"
}

exercise_volume_cycle() {
  local volume_id="$1"
  local pause_cycle="$2"
  local mode="${VOLUME_MODE[${volume_id}]}"
  start_volume_sandbox "${volume_id}"
  local sandbox_id="${LAST_SANDBOX_ID}"
  verify_volume_content "${sandbox_id}" "${volume_id}"

  if [[ "${mode}" == "exclusive" ]]; then
    write_new_volume_content "${sandbox_id}" "${volume_id}" "before-cycle"
  else
    assert_read_only_write_fails "${sandbox_id}"
  fi

  if [[ "${pause_cycle}" == "1" ]]; then
    pause_and_resume_sandbox "${sandbox_id}"
    verify_volume_content "${sandbox_id}" "${volume_id}"
    if [[ "${mode}" == "exclusive" ]]; then
      write_new_volume_content "${sandbox_id}" "${volume_id}" "after-resume"
    fi
  fi

  delete_sandbox "${sandbox_id}"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: delete sandbox and publish volume data"
}

verify_volume_cycle() {
  local volume_id="$1"
  start_volume_sandbox "${volume_id}"
  local sandbox_id="${LAST_SANDBOX_ID}"
  verify_volume_content "${sandbox_id}" "${volume_id}"
  delete_sandbox "${sandbox_id}"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: delete verification sandbox"
}

fork_volume_cycle() {
  local source_volume_id="$1"
  start_volume_sandbox "${source_volume_id}"
  local source_sandbox_id="${LAST_SANDBOX_ID}"
  verify_volume_content "${source_sandbox_id}" "${source_volume_id}"
  write_new_volume_content "${source_sandbox_id}" "${source_volume_id}" "before-fork"

  api_post "/sandboxes/${source_sandbox_id}/fork" '{"count":1}'
  assert_status "${HTTP_STATUS}" "201" "${CURRENT_STEP}: fork volume sandbox through gateway"
  local child_sandbox_id
  child_sandbox_id="$(echo "${HTTP_BODY}" | jq -r '.[0].sandbox.sandboxID // empty')"
  assert_not_empty "${child_sandbox_id}" "${CURRENT_STEP}: fork child sandbox ID is present"
  track_sandbox "${child_sandbox_id}"

  api_get "/sandboxes/${child_sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" \
    "${CURRENT_STEP}: fork child is immediately routable through gateway"
  local child_volume_id
  child_volume_id="$(echo "${HTTP_BODY}" | jq -r --arg path "${VOLUME_MOUNT_PATH}" '.volumeMounts[$path] // empty')"
  assert_not_empty "${child_volume_id}" "${CURRENT_STEP}: fork child volume ID is present"
  register_volume "${child_volume_id}" "exclusive" "${VOLUME_CONTENT[${source_volume_id}]}"
  log "seed=${VOLUME_RANDOM_SEED} step=${CURRENT_STEP} fork-volume=${child_volume_id} source=${source_volume_id}"
  capture_sandbox_node "${child_sandbox_id}" "${child_volume_id}"
  verify_volume_content "${child_sandbox_id}" "${child_volume_id}"

  delete_sandbox "${child_sandbox_id}"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: delete fork child sandbox"
  delete_sandbox "${source_sandbox_id}"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: delete fork source sandbox"
}

assert_catalog_matches_model() {
  api_get "/volumes"
  assert_status "${HTTP_STATUS}" "200" "${CURRENT_STEP}: list volumes through gateway"
  local volume_id
  while IFS= read -r volume_id; do
    [[ -n "${volume_id}" ]] || continue
    local matches status
    matches="$(echo "${HTTP_BODY}" | jq --arg id "${volume_id}" '[.[] | select(.volumeID == $id)] | length')"
    assert_eq "${matches}" "1" "${CURRENT_STEP}: catalog contains modeled volume ${volume_id}"
    status="$(echo "${HTTP_BODY}" | jq -r --arg id "${volume_id}" '.[] | select(.volumeID == $id) | .status')"
    assert_eq "${status}" "ready" "${CURRENT_STEP}: modeled volume ${volume_id} is ready"
  done < <(active_volume_ids)
}

delete_modeled_volume() {
  local volume_id="$1"
  log "seed=${VOLUME_RANDOM_SEED} step=${CURRENT_STEP} deleting=${volume_id}"
  api_delete "/volumes/${volume_id}"
  assert_status "${HTTP_STATUS}" "204" "${CURRENT_STEP}: delete modeled volume"
  unset "VOLUME_MODE[${volume_id}]"
  unset "VOLUME_CONTENT[${volume_id}]"
}

count_volume_nodes() {
  local volume_id="$1"
  local count=0 key
  for key in "${!VOLUME_NODE_KEYS[@]}"; do
    if [[ "${key}" == "${volume_id}|"* ]]; then
      count=$((count + 1))
    fi
  done
  printf '%s\n' "${count}"
}

log "Random seed: ${VOLUME_RANDOM_SEED}; steps: ${VOLUME_RANDOM_STEPS}"

# Establish the cross-node invariant up front. These sandbox creates are
# consecutive gateway scheduling operations, so round-robin places them on
# different runtime nodes while both mount the same published volume.
CURRENT_STEP="cross-node-initial"
create_empty_volume
base_volume_id="${LAST_VOLUME_ID}"
exercise_volume_cycle "${base_volume_id}" 1
CURRENT_STEP="cross-node-remount"
exercise_volume_cycle "${base_volume_id}" 1
assert_eq "$(count_volume_nodes "${base_volume_id}")" "2" \
  "the same volume is written and remounted through gateway on both nodes"
assert_eq "${#SEEN_NODES[@]}" "2" "randomized test has exercised both runtime nodes"
assert_catalog_matches_model

for ((step = 0; step < VOLUME_RANDOM_STEPS; step++)); do
  CURRENT_STEP="random-${step}"
  mapfile -t current_ids < <(active_volume_ids)
  volume_count="${#current_ids[@]}"
  operations=(list exercise exercise)
  if [[ "${volume_count}" -lt 6 ]]; then
    operations+=(create clone)
    if select_random_volume exclusive; then
      operations+=(fork)
    fi
  fi
  if [[ "${volume_count}" -gt 1 ]]; then
    operations+=(delete)
  fi
  operation="${operations[RANDOM % ${#operations[@]}]}"
  log "seed=${VOLUME_RANDOM_SEED} step=${step} operation=${operation} volumes=${volume_count}"

  case "${operation}" in
    create)
      create_empty_volume
      exercise_volume_cycle "${LAST_VOLUME_ID}" "$((RANDOM % 2))"
      ;;
    clone)
      select_random_volume
      source_volume_id="${SELECTED_VOLUME_ID}"
      log "seed=${VOLUME_RANDOM_SEED} step=${step} selected=${source_volume_id} for=clone"
      clone_mode="exclusive"
      [[ "$((RANDOM % 3))" == "0" ]] && clone_mode="ro"
      clone_volume "${source_volume_id}" "${clone_mode}"
      exercise_volume_cycle "${LAST_VOLUME_ID}" "$((RANDOM % 2))"
      ;;
    exercise)
      select_random_volume
      log "seed=${VOLUME_RANDOM_SEED} step=${step} selected=${SELECTED_VOLUME_ID} for=exercise"
      exercise_volume_cycle "${SELECTED_VOLUME_ID}" "$((RANDOM % 2))"
      ;;
    fork)
      select_random_volume exclusive
      log "seed=${VOLUME_RANDOM_SEED} step=${step} selected=${SELECTED_VOLUME_ID} for=fork"
      fork_volume_cycle "${SELECTED_VOLUME_ID}"
      ;;
    delete)
      select_random_volume
      log "seed=${VOLUME_RANDOM_SEED} step=${step} selected=${SELECTED_VOLUME_ID} for=delete"
      delete_modeled_volume "${SELECTED_VOLUME_ID}"
      ;;
    list)
      ;;
    *)
      _fail "${CURRENT_STEP}: selected randomized operation is known" \
        "known operation" "${operation}"
      ;;
  esac

  assert_catalog_matches_model
done

CURRENT_STEP="final-verification"
mapfile -t final_volume_ids < <(active_volume_ids)
for volume_id in "${final_volume_ids[@]}"; do
  verify_volume_cycle "${volume_id}"
done
assert_catalog_matches_model
assert_eq "${#SEEN_NODES[@]}" "2" "final randomized coverage includes both runtime nodes"

CURRENT_STEP="final-delete"
for volume_id in "${final_volume_ids[@]}"; do
  delete_modeled_volume "${volume_id}"
done
api_get "/volumes"
assert_status "${HTTP_STATUS}" "200" "final volume list through gateway"
for volume_id in "${final_volume_ids[@]}"; do
  remaining="$(echo "${HTTP_BODY}" | jq --arg id "${volume_id}" '[.[] | select(.volumeID == $id)] | length')"
  assert_eq "${remaining}" "0" "deleted randomized volume ${volume_id} is absent"
done

suite_summary "16_volume_randomized"
