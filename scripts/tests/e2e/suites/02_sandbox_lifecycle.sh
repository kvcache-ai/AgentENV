#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "02_lifecycle"

log "Suite: Sandbox Lifecycle"

# -- Create sandbox --
sandbox_id=$(create_sandbox); _sync_http
assert_status "$HTTP_STATUS" "201" "POST /sandboxes returns 201"
assert_not_empty "$sandbox_id" "sandboxID is present"
track_sandbox "$sandbox_id"

# -- Get sandbox --
api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /sandboxes/{id} returns 200"
assert_json_field "$HTTP_BODY" '.state' "running" "sandbox state is running"

# -- List sandboxes --
api_get "/sandboxes"
assert_status "$HTTP_STATUS" "200" "GET /sandboxes returns 200"
assert_contains "$HTTP_BODY" "$sandbox_id" "sandbox appears in list"

# -- Pause sandbox --
api_post "/sandboxes/${sandbox_id}/pause"
assert_status "$HTTP_STATUS" "204" "POST /pause returns 204"
if wait_for_sandbox_state "$sandbox_id" "paused" 15; then
  _pass "sandbox state is paused"
else
  _fail "sandbox state is paused" "paused" "timeout"
fi

# -- Resume (connect) sandbox --
api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":60}'
assert_status "$HTTP_STATUS" "201" "POST /connect returns 201"
if wait_for_sandbox_state "$sandbox_id" "running" 30; then
  _pass "sandbox state is running after connect"
else
  _fail "sandbox state is running after connect" "running" "timeout"
fi

# -- Delete sandbox --
api_delete "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "204" "DELETE /sandboxes/{id} returns 204"

# -- Get deleted sandbox returns 404 --
api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "404" "GET deleted sandbox returns 404"

# -- V2 defaults, secure access, and optional connect bodies --
body=$(jq -n --arg template "$AENV_TEMPLATE_ID" '{templateID: $template}')
api_post "/v2/sandboxes" "$body"
assert_status "$HTTP_STATUS" "201" "v2 create with omitted timeout returns 201"
sandbox_id=$(echo "$HTTP_BODY" | jq -r '.sandboxID // empty')
track_sandbox "$sandbox_id"
envd_access_token=$(echo "$HTTP_BODY" | jq -r '.envdAccessToken // empty')
assert_not_empty "$envd_access_token" "v2 create enables secure envd access"
api_get "/sandboxes/${sandbox_id}"
assert_json_field "$HTTP_BODY" '((.endAt | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) - now) | . > 280 and . <= 300' "true" "v2 create defaults to 300 seconds"

_curl_do -s -H "e2b-sandbox-id: ${sandbox_id}" -H "e2b-sandbox-port: ${AENV_ENVD_PORT}" "${AENV_PROXY_URL}/health"
assert_status "$HTTP_STATUS" "401" "v2 envd rejects missing access token"
_curl_do -s -H "e2b-sandbox-id: ${sandbox_id}" -H "e2b-sandbox-port: ${AENV_ENVD_PORT}" -H "X-Access-Token: ${envd_access_token}" "${AENV_PROXY_URL}/health"
assert_status "$HTTP_STATUS" "204" "v2 envd accepts its access token"

api_post "/v2/sandboxes/${sandbox_id}/connect" '{"timeout":600}'
assert_status "$HTTP_STATUS" "200" "v2 connect accepts explicit timeout"
api_get "/sandboxes/${sandbox_id}"
end_at=$(echo "$HTTP_BODY" | jq -r '.endAt')
for body in '' '{}' '{"timeout":1}'; do
  api_post "/v2/sandboxes/${sandbox_id}/connect" "$body"
  assert_status "$HTTP_STATUS" "200" "v2 connect accepts body '${body}'"
  api_get "/sandboxes/${sandbox_id}"
  assert_json_field "$HTTP_BODY" '.endAt' "$end_at" "v2 connect does not shorten TTL"
done
_curl_do -s -X POST -H "X-API-Key: ${AENV_API_KEY}" "${AENV_URL}/v2/sandboxes/${sandbox_id}/connect"
assert_status "$HTTP_STATUS" "200" "v2 connect accepts no body or content-type"
api_post "/v2/sandboxes/${sandbox_id}/connect" '{"timeout":0}'
assert_status "$HTTP_STATUS" "400" "v2 connect rejects zero timeout"
api_post "/v2/sandboxes/${sandbox_id}/connect" '{'
assert_status "$HTTP_STATUS" "400" "v2 connect rejects malformed JSON"
api_post "/v2/sandboxes" "$(jq -n --arg template "$AENV_TEMPLATE_ID" '{templateID: $template, timeout: 0}')"
assert_status "$HTTP_STATUS" "400" "v2 create rejects zero timeout"

api_post "/sandboxes/${sandbox_id}/pause"
assert_status "$HTTP_STATUS" "204" "pause v2 sandbox"
api_post "/v2/sandboxes/${sandbox_id}/connect"
assert_status "$HTTP_STATUS" "201" "v2 connect resumes with no body"
api_get "/sandboxes/${sandbox_id}"
assert_json_field "$HTTP_BODY" '.state' "running" "v2 connect restores running state"
assert_json_field "$HTTP_BODY" '((.endAt | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) - now) | . > 280 and . <= 300' "true" "v2 resume defaults to 300 seconds"
api_delete "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "204" "delete v2 sandbox"
api_post "/v2/sandboxes/${sandbox_id}/connect" '{}'
assert_status "$HTTP_STATUS" "404" "v2 connect preserves not-found response"

suite_summary "02_lifecycle"
