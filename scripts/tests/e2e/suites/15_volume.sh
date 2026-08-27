#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "15_volume"

log "Suite: Persistent Volume Lifecycle"

volume_file="$(mktemp "${TMPDIR:-/tmp}/aenv-volume-e2e.XXXXXX")"
printf 'latest-volume-data\n' >"${volume_file}"

cleanup_volume_e2e() {
  local status=$?
  rm -f "${volume_file}"
  _cleanup_e2e || true
  return "${status}"
}
trap cleanup_volume_e2e EXIT

response_node_id() {
  printf '%s\n' "${HTTP_HEADERS:-}" | awk -F': *' \
    'tolower($1) == "x-agentenv-node-id" {gsub("\\r", "", $2); print $2; exit}'
}

upload_volume_file() {
  local sandbox_id="$1"
  local path="${2:-/home/state.txt}"
  local encoded_path
  encoded_path=$(jq -rn --arg path "${path}" '$path|@uri')
  _curl_do -s -X POST \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    -F "file=@${volume_file}" \
    "${AENV_PROXY_URL}/files?path=${encoded_path}"
}

download_volume_file() {
  local sandbox_id="$1"
  local path="${2:-/home/state.txt}"
  local encoded_path
  encoded_path=$(jq -rn --arg path "${path}" '$path|@uri')
  _curl_do -s \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    "${AENV_PROXY_URL}/files?path=${encoded_path}"
}

volume_name="e2e-volume-$(date +%s%N)"
api_post "/volumes" "$(jq -nc --arg name "${volume_name}" '{name: $name, sizeMB: 16}')"
assert_status "${HTTP_STATUS}" "201" "create persistent volume"
volume_id=$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')
assert_not_empty "${volume_id}" "volume ID is present"
assert_json_field "${HTTP_BODY}" '.status' "ready" "new volume is ready"

list_attempts=1
e2e_mode_is compose && list_attempts=2
declare -A volume_list_nodes=()
for attempt in $(seq 1 "${list_attempts}"); do
  api_get_with_headers "/volumes"
  assert_status "${HTTP_STATUS}" "200" "list volumes through the gateway #${attempt}"
  assert_contains "${HTTP_BODY}" "${volume_id}" \
    "gateway volume list #${attempt} includes the created volume"
  if e2e_mode_is compose; then
    list_node_id="$(response_node_id)"
    assert_not_empty "${list_node_id}" "gateway identifies volume-list backend #${attempt}"
    volume_list_nodes["${list_node_id}"]=1
  fi
done
if e2e_mode_is compose; then
  assert_eq "${#volume_list_nodes[@]}" "2" \
    "gateway volume lists reach both runtime nodes"
fi

mount_payload=$(jq -nc --arg volume_id "${volume_id}" \
  '{autoPause: false, volumeMounts: {"/home": $volume_id}}')
sandbox_id=$(create_sandbox "${AENV_TEMPLATE_ID}" 300 "${mount_payload}")
_sync_http
assert_status "${HTTP_STATUS}" "201" "create sandbox with volume mounted at /home"
assert_not_empty "${sandbox_id}" "sandbox ID is present"
track_sandbox "${sandbox_id}"
first_owner_node_id=""
if e2e_mode_is compose; then
  api_get_with_headers "/sandboxes/${sandbox_id}"
  assert_status "${HTTP_STATUS}" "200" "gateway routes first volume sandbox details"
  first_owner_node_id="$(response_node_id)"
  assert_not_empty "${first_owner_node_id}" "gateway identifies first volume sandbox backend"
fi

upload_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "write data through the mounted volume"
download_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "read data from the mounted volume"
output="${HTTP_BODY}"
assert_contains "${output}" "latest-volume-data" "read data from the mounted volume"

active_clone_name="${volume_name}-active-clone"
api_post "/volumes" "$(jq -nc \
  --arg name "${active_clone_name}" \
  --arg source "${volume_id}" \
  '{name: $name, sizeMB: 16, mode: "ro", fromVolume: $source}')"
assert_status "${HTTP_STATUS}" "409" \
  "cannot clone a volume while an exclusive sandbox owns it"

api_post "/sandboxes/${sandbox_id}/pause"
assert_status "${HTTP_STATUS}" "204" "pause sandbox with mounted volume"
wait_for_sandbox_state "${sandbox_id}" "paused" 30 || _fail \
  "sandbox reaches paused state" "paused" "timeout"

api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":300}'
assert_status "${HTTP_STATUS}" "201" "resume sandbox with mounted volume"
wait_for_sandbox_state "${sandbox_id}" "running" 30 || _fail \
  "sandbox reaches running state after resume" "running" "timeout"
download_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "read data after resume"
output="${HTTP_BODY}"
assert_contains "${output}" "latest-volume-data" "volume remains mounted after resume"

printf 'latest-after-resume\n' >"${volume_file}"
upload_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "write recent data before sandbox fork"

api_post "/sandboxes/${sandbox_id}/fork" '{"count":1}'
assert_status "${HTTP_STATUS}" "201" "fork sandbox with a mounted volume"
fork_sandbox_id=$(echo "${HTTP_BODY}" | jq -r '.[0].sandbox.sandboxID // empty')
assert_not_empty "${fork_sandbox_id}" "forked sandbox ID is present"
api_get "/sandboxes/${fork_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "get forked sandbox details"
fork_volume_id=$(echo "${HTTP_BODY}" | jq -r '.volumeMounts["/home"] // empty')
assert_not_empty "${fork_volume_id}" "forked volume ID is present"
track_sandbox "${fork_sandbox_id}"
download_volume_file "${fork_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "read recent data from forked volume"
assert_contains "${HTTP_BODY}" "latest-after-resume" \
  "volume snapshot used by fork contains recent writes"
delete_sandbox "${fork_sandbox_id}"
assert_status "${HTTP_STATUS}" "204" "delete forked sandbox"
api_get "/volumes/${fork_volume_id}"
assert_status "${HTTP_STATUS}" "200" "get forked volume after sandbox deletion"
assert_json_field "${HTTP_BODY}" '.status' "ready" \
  "forked volume is ready after publication"
api_delete "/volumes/${fork_volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete forked volume"

delete_sandbox "${sandbox_id}"
assert_status "${HTTP_STATUS}" "204" "delete sandbox publishes volume upper"

api_get "/volumes/${volume_id}"
assert_status "${HTTP_STATUS}" "200" "get volume after sandbox deletion"
assert_json_field "${HTTP_BODY}" '.status' "ready" "volume is ready after upload"

read_only_name="${volume_name}-read-only"
api_post "/volumes" "$(jq -nc \
  --arg name "${read_only_name}" \
  --arg source "${volume_id}" \
  '{name: $name, sizeMB: 16, mode: "ro", fromVolume: $source}')"
assert_status "${HTTP_STATUS}" "201" "create read-only volume from published source"
read_only_volume_id=$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')
assert_not_empty "${read_only_volume_id}" "read-only volume ID is present"
assert_json_field "${HTTP_BODY}" '.mode' "ro" "child volume is read-only"

read_only_mount_payload=$(jq -nc --arg volume_id "${read_only_volume_id}" \
  '{autoPause: false, volumeMounts: {"/home": $volume_id}}')

read_only_sandbox_count=1
e2e_mode_is compose && read_only_sandbox_count=2
read_only_sandbox_ids=()
declare -A read_only_sandbox_nodes=()
reused_on_other_node=0
for index in $(seq 1 "${read_only_sandbox_count}"); do
  second_id=$(create_sandbox "${AENV_TEMPLATE_ID}" 300 "${read_only_mount_payload}")
  _sync_http
  assert_status "${HTTP_STATUS}" "201" "create read-only volume sandbox #${index} through gateway"
  assert_not_empty "${second_id}" "read-only sandbox #${index} ID is present"
  track_sandbox "${second_id}"
  read_only_sandbox_ids+=("${second_id}")

  if e2e_mode_is compose; then
    api_get_with_headers "/sandboxes/${second_id}"
    assert_status "${HTTP_STATUS}" "200" "gateway routes read-only sandbox #${index} details"
    second_owner_node_id="$(response_node_id)"
    assert_not_empty "${second_owner_node_id}" \
      "gateway identifies read-only sandbox #${index} backend"
    read_only_sandbox_nodes["${second_owner_node_id}"]=1
    if [[ "${second_owner_node_id}" != "${first_owner_node_id}" ]]; then
      reused_on_other_node=1
    fi
  fi

  download_volume_file "${second_id}"
  assert_status "${HTTP_STATUS}" "200" "read data from reused volume in sandbox #${index}"
  assert_contains "${HTTP_BODY}" "latest-after-resume" \
    "recent source data is visible in read-only sandbox #${index}"

  upload_volume_file "${second_id}" "/home/write-must-fail.txt"
  if [[ "${HTTP_STATUS}" == "200" ]]; then
    _fail "guest write to read-only sandbox #${index} is rejected" "non-200" "${HTTP_STATUS}"
  else
    _pass "guest write to read-only sandbox #${index} is rejected"
  fi
done

if e2e_mode_is compose; then
  assert_eq "${#read_only_sandbox_nodes[@]}" "2" \
    "gateway schedules read-only volume sandboxes across both runtime nodes"
  assert_eq "${reused_on_other_node}" "1" \
    "published volume data is reused on a different runtime node"
fi

for second_id in "${read_only_sandbox_ids[@]}"; do
  delete_sandbox "${second_id}"
  assert_status "${HTTP_STATUS}" "204" "delete read-only sandbox through gateway"
done
api_delete "/volumes/${read_only_volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete read-only child volume"
api_delete "/volumes/${volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete persistent volume"

suite_summary "15_volume"
