#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "06_proxy"

log "Suite: Proxy Routing"

# -- Create sandbox --
sandbox_id=$(create_sandbox); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox for proxy test"
assert_not_empty "$sandbox_id" "sandboxID present"
assert_json_field "$HTTP_BODY" '.trafficAccessToken' "null" "public sandbox omits traffic token"
track_sandbox "$sandbox_id"
wait_for_sandbox_state "$sandbox_id" "running" 30

# -- Proxy request using AgentENV headers --
# The envd health endpoint runs on the configured control-plane port.
_curl_do -s --max-time 5 \
  -H "x-agentenv-sandbox-id: ${sandbox_id}" \
  -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
log "Proxy (agentenv headers) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "proxy with agentenv headers"

# -- Proxy request using E2B compat headers --
_curl_do -s --max-time 5 \
  -H "e2b-sandbox-id: ${sandbox_id}" \
  -H "e2b-sandbox-port: ${AENV_ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
log "Proxy (e2b headers) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "proxy with e2b headers"

# -- Proxy request without sandbox header returns 400 --
# Use the explicit /proxy path so both single-node and compose reach the
# node-local proxy entrypoint before header validation.
_curl_do -s --max-time 5 \
  "${AENV_URL}/proxy/health"
log "Proxy (no sandbox header) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "400" "proxy without sandbox header"

# -- Paused sandbox with auto-resume disabled returns 410 --
paused_no_resume_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"autoResume":{"enabled":false}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create paused sandbox with auto-resume disabled"
assert_not_empty "$paused_no_resume_id" "paused sandbox ID present (auto-resume disabled)"
track_sandbox "$paused_no_resume_id"
wait_for_sandbox_state "$paused_no_resume_id" "running" 30

api_post "/sandboxes/${paused_no_resume_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause sandbox with auto-resume disabled"
if wait_for_sandbox_state "$paused_no_resume_id" "paused" 20; then
  _pass "sandbox paused (auto-resume disabled)"
else
  _fail "sandbox paused (auto-resume disabled)" "paused" "timeout"
fi

_curl_do -s --max-time 10 \
  -H "x-agentenv-sandbox-id: ${paused_no_resume_id}" \
  -H "x-agentenv-target-port: ${AENV_ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
log "Proxy (paused + auto-resume disabled) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "410" "paused sandbox without auto-resume returns 410"

# -- Paused sandbox with auto-resume enabled resumes and forwards --
paused_auto_resume_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 '{"autoResume":{"enabled":true}}'); _sync_http
assert_status "$HTTP_STATUS" "201" "create paused sandbox with auto-resume enabled"
assert_not_empty "$paused_auto_resume_id" "paused sandbox ID present (auto-resume enabled)"
track_sandbox "$paused_auto_resume_id"
wait_for_sandbox_state "$paused_auto_resume_id" "running" 30

api_post "/sandboxes/${paused_auto_resume_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause sandbox with auto-resume enabled"
if wait_for_sandbox_state "$paused_auto_resume_id" "paused" 20; then
  _pass "sandbox paused (auto-resume enabled)"
else
  _fail "sandbox paused (auto-resume enabled)" "paused" "timeout"
fi

# Auto-resume may wait up to 60s in non-test runtime. Keep client timeout above that.
_curl_do -s --max-time 75 \
  -H "e2b-sandbox-id: ${paused_auto_resume_id}" \
  -H "e2b-sandbox-port: ${AENV_ENVD_PORT}" \
  "${AENV_PROXY_URL}/health"
log "Proxy (paused + auto-resume enabled) returned HTTP ${HTTP_STATUS}"
assert_status "$HTTP_STATUS" "204" "paused sandbox auto-resumes on proxy request"

if wait_for_sandbox_state "$paused_auto_resume_id" "running" 30; then
  _pass "sandbox is running after proxy-triggered auto-resume"
else
  _fail "sandbox is running after proxy-triggered auto-resume" "running" "timeout"
fi

suite_summary "06_proxy"
