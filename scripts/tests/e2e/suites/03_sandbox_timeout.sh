#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "03_timeout"

log "Suite: Sandbox Timeout / TTL"

# -- Create sandbox with timeout --
sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox for timeout tests"
assert_not_empty "$sandbox_id" "sandboxID present"
track_sandbox "$sandbox_id"
wait_for_sandbox_state "$sandbox_id" "running" 30

# -- Update timeout via POST /timeout --
api_post "/sandboxes/${sandbox_id}/timeout" '{"timeout":120}'
assert_status "$HTTP_STATUS" "204" "POST /timeout returns 204"

# -- Extend timeout via POST /refreshes (must be >= current timeout) --
api_post "/sandboxes/${sandbox_id}/refreshes" '{"duration":300}'
assert_status "$HTTP_STATUS" "204" "POST /refreshes returns 204"

# -- Verify auto-pause --
# Use a 5-second timeout. The sandbox takes ~1s to boot, so it should be
# auto-paused around 5s after creation. We poll for up to 20s.
short_id=$(create_sandbox "$AENV_TEMPLATE_ID" 5); _sync_http
assert_status "$HTTP_STATUS" "201" "create short-lived sandbox"
assert_not_empty "$short_id" "short sandbox ID present"
track_sandbox "$short_id"
wait_for_sandbox_state "$short_id" "running" 30

log "Waiting for auto-pause (up to 20s) ..."
# create_sandbox sends autoPause: true; the API's default for a missing
# autoPause is to delete the expired sandbox.
if wait_for_sandbox_state "$short_id" "paused" 20; then
  _pass "short-lived sandbox was auto-paused after TTL"
else
  _fail "short-lived sandbox was auto-paused after TTL" "paused" "still running"
fi

# -- Verify what a missing autoPause means, on both create paths --
# The action a sandbox will take at its TTL is readable as lifecycle.onTimeout,
# so the mapping is asserted directly instead of by waiting out a deadline.
assert_on_timeout() {
  local id="$1" expected="$2" what="$3"
  api_get "/sandboxes/${id}"
  local on_timeout
  on_timeout=$(echo "$HTTP_BODY" | jq -r '.lifecycle.onTimeout // empty')
  if [[ "$on_timeout" == "$expected" ]]; then
    _pass "$what"
  else
    _fail "$what" "$expected" "${on_timeout:-absent}"
  fi
}

default_id=$(create_sandbox_without_auto_pause "$AENV_TEMPLATE_ID" 300); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox without autoPause"
track_sandbox "$default_id"
assert_on_timeout "$default_id" "kill" "a create without autoPause is killed at its TTL"
delete_sandbox "$default_id"

paused_id=$(create_sandbox "$AENV_TEMPLATE_ID" 300); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox with autoPause: true"
track_sandbox "$paused_id"
assert_on_timeout "$paused_id" "pause" "a create with autoPause: true is paused at its TTL"
delete_sandbox "$paused_id"

cold_payload=$(jq -nc --arg image "${E2E_DEFAULT_USER_IMAGE}" '{image: $image, timeout: 300}')
api_post "/sandboxes-cold" "${cold_payload}"
assert_status "$HTTP_STATUS" "201" "create cold sandbox without autoPause"
cold_id=$(echo "$HTTP_BODY" | jq -r '.sandboxID // empty')
assert_not_empty "$cold_id" "cold sandbox ID present"
track_sandbox "$cold_id"
assert_on_timeout "$cold_id" "kill" "a cold create without autoPause is killed at its TTL"
delete_sandbox "$cold_id"

# Clean up the long-lived sandbox
delete_sandbox "$sandbox_id"

suite_summary "03_timeout"
