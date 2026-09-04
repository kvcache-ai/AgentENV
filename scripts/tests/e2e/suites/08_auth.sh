#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "08_auth"

log "Suite: Authentication"

proxy_envd_health() {
  local sandbox_id="$1"
  local header_name="${2:-}"
  local header_value="${3:-}"
  local args=(-s --max-time 5
    -H "x-agentenv-sandbox-id: ${sandbox_id}"
    -H "x-agentenv-target-port: ${AENV_ENVD_PORT}")
  [[ -z "${header_name}" ]] || args+=(-H "${header_name}: ${header_value}")
  _curl_do "${args[@]}" "${AENV_PROXY_URL}/health"
}

# -- Request without auth header returns 401 --
api_get_no_auth "/sandboxes"
assert_status "$HTTP_STATUS" "401" "no auth header returns 401"

# -- Alternative and malformed credentials are rejected --
_curl_do -s -H "X-API-Key: ${AENV_API_KEY}x" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "wrong API key returns 401"

_curl_do -s -H "Authorization: Bearer ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "Authorization does not authenticate AgentENV"

_curl_do -s -H "X-Admin-Token: ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "legacy admin token does not authenticate AgentENV"

_curl_do -s -H "X-Team-ID: ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "legacy team key does not authenticate AgentENV"

_curl_do -s \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "X-API-Key: ${AENV_API_KEY}x" \
  "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "valid and invalid API key headers return 401"

_curl_do -s \
  -H "X-API-Key: ${AENV_API_KEY}x" \
  -H "X-API-Key: ${AENV_API_KEY}y" \
  "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "conflicting invalid API key headers return 401"

# -- Request with valid API key succeeds --
api_get "/sandboxes"
assert_status "$HTTP_STATUS" "200" "valid API key authenticates successfully"

# -- Sandbox-scoped credentials cannot authenticate the control plane --
secure_sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 \
  '{"secure":true,"network":{"allowPublicTraffic":false}}'); _sync_http
if [[ -n "$secure_sandbox_id" ]]; then
  track_sandbox "$secure_sandbox_id"
fi
assert_status "$HTTP_STATUS" "201" "create private secure sandbox"
assert_not_empty "$secure_sandbox_id" "private secure sandbox ID present"

if [[ "$HTTP_STATUS" != "201" || -z "$secure_sandbox_id" ]]; then
  suite_summary "08_auth" || true
  exit 1
fi

traffic_access_token=$(echo "$HTTP_BODY" | jq -r '.trafficAccessToken // empty')
envd_access_token=$(echo "$HTTP_BODY" | jq -r '.envdAccessToken // empty')
assert_not_empty "$traffic_access_token" "private sandbox returns traffic token"
assert_not_empty "$envd_access_token" "secure sandbox returns envd token"
if [[ -z "$traffic_access_token" || -z "$envd_access_token" ]]; then
  suite_summary "08_auth" || true
  exit 1
fi
wait_for_sandbox_state "$secure_sandbox_id" "running" 30

_curl_do -s \
  -H "e2b-traffic-access-token: ${traffic_access_token}" \
  "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "traffic token does not authenticate control plane"

_curl_do -s \
  -H "X-Access-Token: ${envd_access_token}" \
  "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "envd token does not authenticate control plane"

# -- Secure envd accepts only its envd access token --
proxy_envd_health "${secure_sandbox_id}"
assert_status "$HTTP_STATUS" "401" "secure envd rejects missing token"

proxy_envd_health "${secure_sandbox_id}" "X-API-Key" "${AENV_API_KEY}"
assert_status "$HTTP_STATUS" "401" "secure envd rejects API key"

proxy_envd_health \
  "${secure_sandbox_id}" "e2b-traffic-access-token" "${traffic_access_token}"
assert_status "$HTTP_STATUS" "401" "secure envd rejects traffic token"

proxy_envd_health "${secure_sandbox_id}" "X-Access-Token" "${envd_access_token}"
assert_status "$HTTP_STATUS" "204" "secure envd accepts envd token"

# -- Envd tokens are scoped to one sandbox --
other_secure_sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60 \
  '{"secure":true,"network":{"allowPublicTraffic":false}}'); _sync_http
if [[ -n "$other_secure_sandbox_id" ]]; then
  track_sandbox "$other_secure_sandbox_id"
fi
assert_status "$HTTP_STATUS" "201" "create second private secure sandbox"
assert_not_empty "$other_secure_sandbox_id" "second private secure sandbox ID present"
if [[ "$HTTP_STATUS" != "201" || -z "$other_secure_sandbox_id" ]]; then
  suite_summary "08_auth" || true
  exit 1
fi
wait_for_sandbox_state "$other_secure_sandbox_id" "running" 30

proxy_envd_health "${other_secure_sandbox_id}" "X-Access-Token" "${envd_access_token}"
assert_status "$HTTP_STATUS" "401" "envd token cannot authenticate another sandbox"

# -- Health and Prometheus metrics work without application auth --
api_get_no_auth "/health"
assert_status "$HTTP_STATUS" "204" "/health works without auth"

api_get_no_auth "/metrics"
if e2e_mode_is_clustered; then
  assert_status "$HTTP_STATUS" "404" "gateway client listener does not expose /metrics"
else
  assert_status "$HTTP_STATUS" "200" "/metrics works without auth"
fi

while IFS= read -r node_url; do
  [[ -z "${node_url}" ]] && continue
  api_get_no_auth_at "${node_url}" "/metrics"
  assert_status "$HTTP_STATUS" "200" "node /metrics works without auth at ${node_url}"
done < <(printf '%s\n' "${AENV_NODE_URLS:-}" | tr ' ' '\n')

suite_summary "08_auth"
