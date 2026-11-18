#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "12_template_shared_repository"

log "Suite: Shared POSIX Template Repository"

if ! e2e_mode_is_clustered; then
  warn "Skipping shared repository checks outside clustered modes."
  _pass "skipped outside clustered modes"
  suite_summary "12_template_shared_repository"
  exit 0
fi

if [[ -z "${AENV_NODE_A_URL:-}" || -z "${AENV_NODE_B_URL:-}" ]]; then
  warn "Skipping shared repository checks because fewer than two AgentENV node endpoints are available."
  _pass "skipped because clustered runtime exposed fewer than two AgentENV nodes"
  suite_summary "12_template_shared_repository"
  exit 0
fi

assert_postgres_retired() {
  e2e_mode_is "compose" || return 0
  local id="$1" expected="$2" actual
  actual=$(
    source "${SUITE_DIR}/../lib/runtime.sh"
    _compose_cmd exec -T postgres psql -XAt -v ON_ERROR_STOP=1 -U agentenv -d agentenv -v id="$id" <<'SQL'
SELECT retired FROM snapshots WHERE snapshot_id = :'id'::uuid;
SQL
  )
  assert_eq "$actual" "$expected" "shared PostgreSQL retirement state for ${id}"
}

assert_cross_node_template_flow() {
  local builder_url="$1"
  local builder_label="$2"
  local consumer_url="$3"
  local consumer_label="$4"
  local alias="$5"

  create_template_at "$builder_url" "$alias"
  assert_status "$HTTP_STATUS" "202" "create and start template ${alias} on ${builder_label}"

  local template_id build_id
  template_id=$(echo "$HTTP_BODY" | jq -r '.templateID // empty')
  build_id=$(echo "$HTTP_BODY" | jq -r '.buildID // empty')
  assert_not_empty "$template_id" "templateID present for ${alias}"
  track_template "$template_id"

  if wait_for_template_build_at "$builder_url" "$template_id" 120 "$build_id"; then
    _pass "template ${alias} built on ${builder_label}"
  else
    _fail "template ${alias} built on ${builder_label}" "ready" "failed or timed out"
    return 1
  fi

  api_get_at "$consumer_url" "/v2/templates"
  assert_status "$HTTP_STATUS" "200" "list templates on ${consumer_label}"
  assert_contains "$HTTP_BODY" "$template_id" "${consumer_label} sees template ${alias}"

  api_get_at "$consumer_url" "/templates/${template_id}"
  assert_status "$HTTP_STATUS" "200" "get template ${alias} on ${consumer_label}"
  assert_contains "$HTTP_BODY" "$alias" "${consumer_label} template payload includes alias ${alias}"

  local sandbox_id
  sandbox_id=$(create_sandbox_at "$consumer_url" "$alias" 60); _sync_http
  assert_status "$HTTP_STATUS" "201" "create sandbox from ${alias} on ${consumer_label}"
  assert_not_empty "$sandbox_id" "sandboxID present for ${alias} on ${consumer_label}"
  track_sandbox "$sandbox_id"
  wait_for_sandbox_state_at "$consumer_url" "$sandbox_id" "running" 30

  local snapshot_id
  api_post_at "$consumer_url" "/sandboxes/${sandbox_id}/snapshots" '{}'
  assert_status "$HTTP_STATUS" "201" "capture sandbox snapshot on ${consumer_label}"
  snapshot_id=$(jq -er '.snapshotID' <<< "$HTTP_BODY")
  assert_postgres_retired "$snapshot_id" f
  api_get_at "$builder_url" "/snapshots/${snapshot_id}"
  assert_status "$HTTP_STATUS" "200" "read ${consumer_label} snapshot on ${builder_label}"
  api_delete_at "$builder_url" "/snapshots/${snapshot_id}"
  assert_status "$HTTP_STATUS" "204" "retire ${consumer_label} snapshot on ${builder_label}"
  assert_postgres_retired "$snapshot_id" t

  delete_sandbox_at "$consumer_url" "$sandbox_id"
  assert_status "$HTTP_STATUS" "204" "delete sandbox from ${alias} on ${consumer_label}"

  api_delete_at "$consumer_url" "/templates/${template_id}"
  assert_status "$HTTP_STATUS" "204" "delete template ${alias} on ${consumer_label}"

  api_get_at "$builder_url" "/v2/templates"
  assert_status "$HTTP_STATUS" "200" "list templates on ${builder_label} after deleting ${alias}"
  if echo "$HTTP_BODY" | grep -Fq "$template_id"; then
    _fail "${builder_label} no longer lists deleted template ${alias}" "template removed" "$template_id still listed"
    return 1
  fi
  _pass "${builder_label} no longer lists deleted template ${alias}"
}

assert_cross_node_template_flow \
  "${AENV_NODE_A_URL}" \
  "${AENV_NODE_A_LABEL:-agentenv-a}" \
  "${AENV_NODE_B_URL}" \
  "${AENV_NODE_B_LABEL:-agentenv-b}" \
  "shared-posix-a-$(date +%s%N)"

assert_cross_node_template_flow \
  "${AENV_NODE_B_URL}" \
  "${AENV_NODE_B_LABEL:-agentenv-b}" \
  "${AENV_NODE_A_URL}" \
  "${AENV_NODE_A_LABEL:-agentenv-a}" \
  "shared-posix-b-$(date +%s%N)"

suite_summary "12_template_shared_repository"
