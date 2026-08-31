#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "15_volume"

log "Suite: Cold-Image Volume Snapshot Lifecycle"

readonly VOLUME_MOUNT_PATH="/volume"
readonly VOLUME_SIZE_MB=16
run_name="volume-cold-snapshot-$(date +%s%N)"
volume_file="$(mktemp "${TMPDIR:-/tmp}/aenv-volume-cold.XXXXXX")"
CREATED_VOLUME_IDS=()

cleanup_volume_e2e() {
  local status=$?
  local sandbox_id volume_id

  for sandbox_id in "${_TRACKED_SANDBOX_IDS[@]}"; do
    api_delete "/sandboxes/${sandbox_id}" 2>/dev/null || true
  done
  for volume_id in "${CREATED_VOLUME_IDS[@]}"; do
    [[ -n "${volume_id}" ]] || continue
    for _attempt in $(seq 1 30); do
      api_delete "/volumes/${volume_id}" 2>/dev/null || true
      [[ "${HTTP_STATUS}" == "204" || "${HTTP_STATUS}" == "404" ]] && break
      sleep 1
    done
  done
  _cleanup_e2e || true
  rm -f "${volume_file}"
  return "${status}"
}
trap cleanup_volume_e2e EXIT

upload_volume_file() {
  local sandbox_id="$1"
  local path="$2"
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
  local path="$2"
  local encoded_path
  encoded_path=$(jq -rn --arg path "${path}" '$path|@uri')
  _curl_do -s \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    "${AENV_PROXY_URL}/files?path=${encoded_path}"
}

write_volume_files() {
  local sandbox_id="$1"
  local file_index expected
  for file_index in 0 1 2 3; do
    expected="${run_name}-file-${file_index}"
    printf '%s\n' "${expected}" >"${volume_file}"
    upload_volume_file "${sandbox_id}" "${VOLUME_MOUNT_PATH}/state-${file_index}.txt"
    assert_status "${HTTP_STATUS}" "200" "write cold-image volume file ${file_index}"
  done
}

verify_volume_files() {
  local sandbox_id="$1"
  local file_index expected
  for file_index in 0 1 2 3; do
    expected="${run_name}-file-${file_index}"
    download_volume_file "${sandbox_id}" "${VOLUME_MOUNT_PATH}/state-${file_index}.txt"
    assert_status "${HTTP_STATUS}" "200" "read volume snapshot file ${file_index}"
    assert_eq "${HTTP_BODY}" "${expected}" \
      "restored volume snapshot file ${file_index} matches exactly"
  done
}

api_post "/volumes" "$(jq -nc \
  --arg name "${run_name}" \
  --argjson size "${VOLUME_SIZE_MB}" \
  '{name: $name, sizeMB: $size, mode: "exclusive"}')"
assert_status "${HTTP_STATUS}" "201" "create cold-image source volume"
source_volume_id="$(echo "${HTTP_BODY}" | jq -r '.volumeID // empty')"
assert_not_empty "${source_volume_id}" "cold-image source volume ID is present"
CREATED_VOLUME_IDS+=("${source_volume_id}")

cold_payload=$(jq -nc \
  --arg image "${E2E_DEFAULT_USER_IMAGE}" \
  --arg volume_id "${source_volume_id}" \
  --arg mount_path "${VOLUME_MOUNT_PATH}" \
  '{
    image: $image,
    timeout: 300,
    autoPause: false,
    volumeMounts: {($mount_path): $volume_id}
  }')
api_post "/sandboxes-cold" "${cold_payload}"
assert_status "${HTTP_STATUS}" "201" "create cold-image sandbox with a volume"
if [[ "${HTTP_STATUS}" != "201" ]]; then
  error "cold-image sandbox create response: ${HTTP_BODY}"
  exit 1
fi
source_sandbox_id="$(echo "${HTTP_BODY}" | jq -r '.sandboxID // empty')"
assert_not_empty "${source_sandbox_id}" "cold-image sandbox ID is present"
track_sandbox "${source_sandbox_id}"

write_volume_files "${source_sandbox_id}"
verify_volume_files "${source_sandbox_id}"

api_post "/sandboxes/${source_sandbox_id}/snapshots" "$(jq -nc \
  --arg name "${run_name}-snapshot" \
  '{name: $name}')"
assert_status "${HTTP_STATUS}" "201" "capture cold-image sandbox volume snapshot"
if [[ "${HTTP_STATUS}" != "201" ]]; then
  error "volume snapshot response: ${HTTP_BODY}"
  exit 1
fi
snapshot_id="$(echo "${HTTP_BODY}" | jq -r '.snapshotID // empty')"
assert_not_empty "${snapshot_id}" "cold-image volume snapshot ID is present"
track_template "${snapshot_id}"
verify_volume_files "${source_sandbox_id}"

restored_sandbox_id=$(create_sandbox "${snapshot_id}" 300 '{"autoPause":false}')
_sync_http
assert_status "${HTTP_STATUS}" "201" "restore cold-image sandbox volume snapshot"
if [[ "${HTTP_STATUS}" != "201" ]]; then
  error "volume snapshot restore response: ${HTTP_BODY}"
  exit 1
fi
assert_not_empty "${restored_sandbox_id}" "restored cold-image sandbox ID is present"
track_sandbox "${restored_sandbox_id}"

api_get "/sandboxes/${restored_sandbox_id}"
assert_status "${HTTP_STATUS}" "200" "get restored cold-image sandbox"
restored_volume_id="$(echo "${HTTP_BODY}" | jq -r \
  --arg path "${VOLUME_MOUNT_PATH}" '.volumeMounts[$path] // empty')"
assert_not_empty "${restored_volume_id}" "automatically restored volume ID is present"
assert_not_eq "${restored_volume_id}" "${source_volume_id}" \
  "volume snapshot restore creates an independent volume"
CREATED_VOLUME_IDS+=("${restored_volume_id}")
verify_volume_files "${restored_sandbox_id}"

suite_summary "15_volume"
