#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "20_server_crash"
log "Suite: Sandboxes Survive a Server Crash"

# Compose and k8s restart the whole container, VMs included.
if e2e_mode_is_clustered || [[ -z "${E2E_SERVER_BIN:-}" ]]; then
  log "[SKIP] needs a single-node server this suite can restart"
  exit 0
fi
# Used by server.sh.
REPO_ROOT="$(cd "${SUITE_DIR}/../../../.." && pwd)"
export REPO_ROOT
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/server.sh"

payload=$(mktemp)
cleanup_crash_e2e() {
  local status=$?
  _cleanup_e2e || true
  rm -f "${payload}"
  return "${status}"
}
trap cleanup_crash_e2e EXIT

put_file() {
  local sandbox_id="$1" path="$2" content="$3"
  printf '%s\n' "${content}" >"${payload}"
  _curl_do -s -X POST \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    -F "file=@${payload}" \
    "${AENV_PROXY_URL}/files?path=$(jq -rn --arg path "${path}" '$path|@uri')"
}

get_file() {
  local sandbox_id="$1" path="$2"
  _curl_do -s \
    -H "x-agentenv-sandbox-id: ${sandbox_id}" \
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
    "${AENV_PROXY_URL}/files?path=$(jq -rn --arg path "${path}" '$path|@uri')"
}

server_pids() {
  sudo fuser "${AENV_PORT}/tcp" 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' || true
}

sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 600); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox"
track_sandbox "$sandbox_id"

disk_marker="disk-$(date +%s%N)"
memory_marker="memory-$(date +%s%N)"
put_file "$sandbox_id" /root/crash-disk.txt "$disk_marker"
assert_status "$HTTP_STATUS" "200" "write a file to the rootfs"
# tmpfs lives only in guest memory: it survives only if the VM keeps running.
put_file "$sandbox_id" /dev/shm/crash-memory.txt "$memory_marker"
assert_status "$HTTP_STATUS" "200" "write a file to guest memory"

# -- Crash the server --
mapfile -t pids < <(server_pids)
[[ "${#pids[@]}" -gt 0 ]] || die "no process listens on port ${AENV_PORT}"
log "Killing server (PID ${pids[*]}) with SIGKILL ..."
sudo kill -9 "${pids[@]}"
for ((i = 0; i < 50; i++)); do
  [[ -z "$(server_pids)" ]] && break
  sleep 0.2
done
[[ -z "$(server_pids)" ]] || die "killed server still holds port ${AENV_PORT}"

start_server "$E2E_SERVER_BIN" "${E2E_SERVER_CONFIG:-}"
wait_for_server

# -- The sandbox is still the same running VM --
api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "200" "sandbox is known after the restart"
assert_json_field "$HTTP_BODY" '.state' "running" "sandbox is still running"
get_file "$sandbox_id" /dev/shm/crash-memory.txt
assert_status "$HTTP_STATUS" "200" "read guest memory file after the restart"
assert_eq "$HTTP_BODY" "$memory_marker" "guest memory survived the crash"
get_file "$sandbox_id" /root/crash-disk.txt
assert_status "$HTTP_STATUS" "200" "read rootfs file after the restart"
assert_eq "$HTTP_BODY" "$disk_marker" "rootfs file survived the crash"
put_file "$sandbox_id" /root/after-crash.txt "after"
assert_status "$HTTP_STATUS" "200" "rootfs is writable after the restart"

# -- Lifecycle operations work on the adopted sandbox --
api_post "/sandboxes/${sandbox_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause the adopted sandbox"
api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":60}'
assert_status "$HTTP_STATUS" "201" "resume the adopted sandbox"
get_file "$sandbox_id" /dev/shm/crash-memory.txt
assert_status "$HTTP_STATUS" "200" "read guest memory file after resume"
assert_eq "$HTTP_BODY" "$memory_marker" "guest memory survives pause and resume"
get_file "$sandbox_id" /root/after-crash.txt
assert_status "$HTTP_STATUS" "200" "read rootfs file after resume"
assert_eq "$HTTP_BODY" "after" "rootfs write after the restart was kept"

api_delete "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "204" "delete the adopted sandbox"

suite_summary
