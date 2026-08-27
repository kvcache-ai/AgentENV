#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "15_volume"

log "Suite: Persistent Volume Lifecycle"

volume_file="$(mktemp "${TMPDIR:-/tmp}/aenv-volume-e2e.XXXXXX")"
printf 'latest-volume-data\n' >"${volume_file}"
CREATED_VOLUME_IDS=()

cleanup_volume_e2e() {
  local status=$?
  rm -f "${volume_file}"
  local cleanup_volume_id
  for cleanup_volume_id in "${CREATED_VOLUME_IDS[@]}"; do
    [[ -n "${cleanup_volume_id}" ]] || continue
    api_delete "/volumes/${cleanup_volume_id}" 2>/dev/null || true
  done
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

append_proto_byte() {
  local output_file="$1"
  local value="$2"
  printf '%b' "\\$(printf '%03o' "${value}")" >>"${output_file}"
}

append_proto_string() {
  local output_file="$1"
  local field_number="$2"
  local value="$3"
  local length="${#value}"
  ((length < 128)) || return 1
  append_proto_byte "${output_file}" "$((field_number * 8 + 2))"
  append_proto_byte "${output_file}" "${length}"
  printf '%s' "${value}" >>"${output_file}"
}

# Execute BusyBox directly through envd's Connect RPC. This keeps the volume
# E2E independent of either the AgentENV or E2B CLI.
run_guest_busybox() {
  local sandbox_id="$1"
  shift
  local process_config start_request request_body response_body
  process_config=$(mktemp "${TMPDIR:-/tmp}/aenv-process-config.XXXXXX")
  start_request=$(mktemp "${TMPDIR:-/tmp}/aenv-start-request.XXXXXX")
  request_body=$(mktemp "${TMPDIR:-/tmp}/aenv-connect-request.XXXXXX")
  response_body=$(mktemp "${TMPDIR:-/tmp}/aenv-connect-response.XXXXXX")

  append_proto_string "${process_config}" 1 "/agentenv/bin/busybox"
  local arg
  for arg in "$@"; do
    append_proto_string "${process_config}" 2 "${arg}"
  done

  local process_size request_size shift_bits status
  process_size=$(wc -c <"${process_config}")
  append_proto_byte "${start_request}" 10
  append_proto_byte "${start_request}" "${process_size}"
  cat "${process_config}" >>"${start_request}"

  request_size=$(wc -c <"${start_request}")
  append_proto_byte "${request_body}" 0
  for shift_bits in 24 16 8 0; do
    append_proto_byte "${request_body}" "$(((request_size >> shift_bits) & 255))"
  done
  cat "${start_request}" >>"${request_body}"

  status=$(curl -s --max-time 15 -o "${response_body}" -w '%{http_code}' \
    -X POST \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    -H 'Connect-Protocol-Version: 1' \
    -H 'Content-Type: application/connect+proto' \
    --data-binary "@${request_body}" \
    "${AENV_PROXY_URL}/process.Process/Start" 2>/dev/null || true)
  rm -f "${process_config}" "${start_request}" "${request_body}" "${response_body}"
  [[ "${status}" == "200" ]]
}

volume_name="e2e-volume-$(date +%s%N)"
api_post "/volumes" "$(jq -nc --arg name "${volume_name}" '{name: $name, sizeMB: 16}')"
assert_status "${HTTP_STATUS}" "201" "create persistent volume"
volume_id=$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')
CREATED_VOLUME_IDS+=("${volume_id}")
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
CREATED_VOLUME_IDS+=("${fork_volume_id}")
assert_not_empty "${fork_volume_id}" "forked volume ID is present"
track_sandbox "${fork_sandbox_id}"
download_volume_file "${fork_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "read recent data from forked volume"
assert_contains "${HTTP_BODY}" "latest-after-resume" \
  "volume snapshot used by fork contains recent writes"
delete_sandbox "${fork_sandbox_id}"
assert_status "${HTTP_STATUS}" "204" "delete forked sandbox"
wait_for_volume_status "${fork_volume_id}" "ready" 30 || _fail \
  "forked volume is published after sandbox deletion" "ready" "timeout"
api_get "/volumes/${fork_volume_id}"
assert_status "${HTTP_STATUS}" "200" "get forked volume after sandbox deletion"
assert_json_field "${HTTP_BODY}" '.status' "ready" \
  "forked volume is ready after publication"
api_delete "/volumes/${fork_volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete forked volume"

if run_guest_busybox "${sandbox_id}" umount /home; then
  _pass "guest unmounts the persistent volume"
else
  _fail "guest unmounts the persistent volume" "Connect HTTP 200" "command failed"
fi
download_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "404" "unmounted volume contents are hidden"

api_post "/sandboxes/${sandbox_id}/pause"
assert_status "${HTTP_STATUS}" "204" "pause sandbox after guest unmount"
wait_for_sandbox_state "${sandbox_id}" "paused" 30 || _fail \
  "sandbox with unmounted volume reaches paused state" "paused" "timeout"
api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":300}'
assert_status "${HTTP_STATUS}" "201" "resume sandbox after guest unmount"
wait_for_sandbox_state "${sandbox_id}" "running" 30 || _fail \
  "sandbox with unmounted volume reaches running state" "running" "timeout"
download_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "404" "resume preserves the guest-unmounted volume state"

volume_snapshot_name="${volume_name}-snapshot-test"
api_post "/sandboxes/${sandbox_id}/snapshots" "$(jq -nc \
  --arg name "${volume_snapshot_name}" \
  '{name: $name}')"
assert_status "${HTTP_STATUS}" "201" "capture sandbox with an unmounted volume"
sandbox_snapshot_id=$(echo "${HTTP_BODY}" | jq -r '.snapshotID // empty')
assert_not_empty "${sandbox_snapshot_id}" "volume snapshot ID is present"
track_template "${sandbox_snapshot_id}"

download_volume_file "${sandbox_id}"
assert_status "${HTTP_STATUS}" "404" \
  "volume snapshot capture preserves the source guest-unmounted state"

api_get "/volumes?limit=100"
assert_status "${HTTP_STATUS}" "200" "list volumes after volume snapshot capture"
snapshot_volume_prefix="${volume_name}-snapshot-"
snapshot_backing_volume_id=$(echo "${HTTP_BODY}" | jq -r \
  --arg prefix "${snapshot_volume_prefix}" \
  '[.[] | select(.name | startswith($prefix))][-1].volumeID // empty')
CREATED_VOLUME_IDS+=("${snapshot_backing_volume_id}")
assert_not_empty "${snapshot_backing_volume_id}" \
  "captured sandbox records a backing volume snapshot"

restored_sandbox_id=$(create_sandbox "${sandbox_snapshot_id}" 300 '{"autoPause":false}')
_sync_http
assert_status "${HTTP_STATUS}" "201" "create sandbox from volume snapshot"
assert_not_empty "${restored_sandbox_id}" "volume snapshot sandbox ID is present"
track_sandbox "${restored_sandbox_id}"

api_get "/sandboxes/${restored_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "get volume snapshot sandbox details"
restored_volume_id=$(echo "${HTTP_BODY}" | jq -r '.volumeMounts["/home"] // empty')
CREATED_VOLUME_IDS+=("${restored_volume_id}")
assert_not_empty "${restored_volume_id}" "restored volume ID is present"

download_volume_file "${restored_sandbox_id}"
assert_status "${HTTP_STATUS}" "404" \
  "volume snapshot launch preserves the guest-unmounted state"
if run_guest_busybox "${restored_sandbox_id}" mount -n /dev/vdc /home; then
  _pass "guest remount request reaches the restored volume snapshot"
else
  _fail "guest remount request reaches the restored volume snapshot" \
    "Connect HTTP 200" "request failed"
fi
download_volume_file "${restored_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "restored volume is readable after an explicit guest mount"
assert_contains "${HTTP_BODY}" "latest-after-resume" \
  "restored volume snapshot contains the latest data"

delete_sandbox "${restored_sandbox_id}"
assert_status "${HTTP_STATUS}" "204" "delete volume snapshot sandbox"
wait_for_volume_status "${restored_volume_id}" "ready" 30 || _fail \
  "restored volume is published after sandbox deletion" "ready" "timeout"
api_delete "/volumes/${restored_volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete restored volume"
api_delete "/templates/${sandbox_snapshot_id}"
assert_status "${HTTP_STATUS}" "204" "delete sandbox volume snapshot"
api_delete "/volumes/${snapshot_backing_volume_id}"
assert_status "${HTTP_STATUS}" "204" "delete backing volume snapshot"

delete_sandbox "${sandbox_id}"
assert_status "${HTTP_STATUS}" "204" "delete sandbox publishes volume upper"
wait_for_volume_status "${volume_id}" "ready" 30 || _fail \
  "volume is published after sandbox deletion" "ready" "timeout"

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
CREATED_VOLUME_IDS+=("${read_only_volume_id}")
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
  assert_status "${HTTP_STATUS}" "500" \
    "guest write to read-only sandbox #${index} returns the documented filesystem error"
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
