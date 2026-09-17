#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "13_snapshot"

log "Suite: Snapshot Lifecycle"

snapshot_alias="e2e-snapshot-$(date +%s%N)"

# -- Create source sandbox --
source_sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create source sandbox"
assert_not_empty "$source_sandbox_id" "source sandbox ID is present"
track_sandbox "$source_sandbox_id"

if wait_for_sandbox_state "$source_sandbox_id" "running" 30; then
  _pass "source sandbox reaches running state"
else
  _fail "source sandbox reaches running state" "running" "timeout"
fi

# -- Capture snapshot from source sandbox --
api_post "/sandboxes/${source_sandbox_id}/snapshots" "$(jq -nc \
  --arg name "$snapshot_alias" \
  '{name: $name}')"
assert_status "$HTTP_STATUS" "201" "POST /sandboxes/{id}/snapshots returns 201"

snapshot_id=$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')
snapshot_name=$(echo "$HTTP_BODY" | jq -r '.names[0] // empty')
assert_not_empty "$snapshot_id" "snapshotID is present"
assert_not_empty "$snapshot_name" "snapshot name is present"
assert_contains "$snapshot_name" "$snapshot_alias" "snapshot name contains requested alias"
track_template "$snapshot_id"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "distributed" \
  "omitted snapshotType defaults to distributed availability"

# -- List snapshots filtered by source sandbox --
api_get "/snapshots?sandboxID=${source_sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /snapshots filtered by sandboxID returns 200"

listed_snapshot_count=$(echo "$HTTP_BODY" | jq -r --arg id "$snapshot_id" \
  '[.[] | select(.snapshotID == $id)] | length')
assert_eq "$listed_snapshot_count" "1" "snapshot appears in filtered snapshot list"

# -- Local snapshots require publication before launch --
api_post "/sandboxes/${source_sandbox_id}/snapshots" '{"snapshotType":"local","name":"invalid-local-name"}'
rejected_snapshot_id=$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')
if [[ -n "$rejected_snapshot_id" ]]; then track_template "$rejected_snapshot_id"; fi
assert_status "$HTTP_STATUS" "400" "Local snapshots reject names"

api_post "/sandboxes/${source_sandbox_id}/snapshots" '{"snapshotType":"local"}'
assert_status "$HTTP_STATUS" "201" "capture Local snapshot"
local_snapshot_id=$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')
assert_not_empty "$local_snapshot_id" "Local snapshot ID is present"
track_template "$local_snapshot_id"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType')" "local" "Local capture is not published"

api_get "/snapshots?name=${snapshot_alias}"
assert_status "$HTTP_STATUS" "200" "filter snapshots by name"
assert_eq "$(echo "$HTTP_BODY" | jq 'length')" "1" "name filter excludes the Local snapshot"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.[0].snapshotID')" "$snapshot_id" "name filter returns the named snapshot"

rejected_sandbox_id=$(create_sandbox "$local_snapshot_id" 60); _sync_http
if [[ -n "$rejected_sandbox_id" ]]; then track_sandbox "$rejected_sandbox_id"; fi
assert_status "$HTTP_STATUS" "400" "Local snapshot cannot launch before promotion"
assert_contains "$HTTP_BODY" "promoted to Distributed" "Local launch explains how to proceed"

api_post "/snapshots/${local_snapshot_id}/promote" '{}'
assert_status "$HTTP_STATUS" "200" "promote Local snapshot"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotID')" "$local_snapshot_id" "promotion keeps the snapshot ID"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType')" "distributed" "promotion publishes the snapshot"
promoted_at=$(echo "$HTTP_BODY" | jq -r '.updatedAt')
api_post "/snapshots/${local_snapshot_id}/promote" '{}'
assert_status "$HTTP_STATUS" "200" "repeat promotion"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.updatedAt')" "$promoted_at" "repeat promotion is idempotent"

# -- Launch the promoted snapshot; reuse the named snapshot below --
relaunched_sandbox_id=$(create_sandbox "$local_snapshot_id" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox from promoted snapshot"
assert_not_empty "$relaunched_sandbox_id" "relaunched sandbox ID is present"
track_sandbox "$relaunched_sandbox_id"

if wait_for_sandbox_state "$relaunched_sandbox_id" "running" 30; then
  _pass "sandbox created from snapshot reaches running state"
else
  _fail "sandbox created from snapshot reaches running state" "running" "timeout"
fi

# -- Delete source sandbox; snapshot should remain reusable --
delete_sandbox "$source_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete source sandbox"

api_get "/sandboxes/${source_sandbox_id}"
assert_status "$HTTP_STATUS" "404" "deleted source sandbox returns 404"

api_get "/snapshots?sandboxID=${source_sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /snapshots still works after deleting source sandbox"
listed_after_delete=$(echo "$HTTP_BODY" | jq -r --arg id "$snapshot_id" \
  '[.[] | select(.snapshotID == $id)] | length')
assert_eq "$listed_after_delete" "1" "snapshot remains listed after deleting source sandbox"

# -- Snapshot remains launchable after source sandbox deletion --
reused_sandbox_id=$(create_sandbox "$snapshot_name" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create second sandbox from snapshot after source deletion"
assert_not_empty "$reused_sandbox_id" "reused sandbox ID is present"
track_sandbox "$reused_sandbox_id"

if wait_for_sandbox_state "$reused_sandbox_id" "running" 30; then
  _pass "snapshot remains launchable after source sandbox deletion"
else
  _fail "snapshot remains launchable after source sandbox deletion" "running" "timeout"
fi

# -- Cleanup relaunched sandboxes --
delete_sandbox "$relaunched_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete first sandbox created from snapshot"

delete_sandbox "$reused_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete second sandbox created from snapshot"

api_delete "/templates/${snapshot_id}"
assert_status "$HTTP_STATUS" "204" "delete reusable snapshot template"

api_delete "/snapshots/${local_snapshot_id}"
assert_status "$HTTP_STATUS" "204" "delete promoted snapshot"

suite_summary "13_snapshot"
