#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "08_auth"

log "Suite: Authentication"

# -- Request without auth header returns 401 --
api_get_no_auth "/sandboxes"
assert_status "$HTTP_STATUS" "401" "no auth header returns 401"

# -- Alternative and malformed credentials are rejected --
_curl_do -s -H "X-API-Key: wrong-key" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "wrong API key returns 401"

_curl_do -s -H "Authorization: Bearer ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "Authorization does not authenticate AgentENV"

_curl_do -s -H "X-Admin-Token: ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "legacy admin token does not authenticate AgentENV"

_curl_do -s -H "X-Team-ID: ${AENV_API_KEY}" "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "legacy team key does not authenticate AgentENV"

_curl_do -s \
  -H "X-API-Key: ${AENV_API_KEY}" \
  -H "X-API-Key: ${AENV_API_KEY}" \
  "${AENV_URL}/sandboxes"
assert_status "$HTTP_STATUS" "401" "duplicate API key headers return 401"

# -- Request with valid API key succeeds --
api_get "/sandboxes"
assert_not_eq "$HTTP_STATUS" "401" "valid API key does not return 401"

# -- Health endpoint works without auth --
api_get_no_auth "/health"
assert_status "$HTTP_STATUS" "204" "/health works without auth"

suite_summary "08_auth"
