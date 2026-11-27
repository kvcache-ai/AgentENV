#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "20_abandoned_create"

log "Suite: Create Whose Caller Left"

# A client that closes its create request never receives the sandbox ID, so
# the runtime must delete that sandbox once its create finishes.
#
# This is a smoke test of that path through the real API, and through the
# gateway in the clustered modes. It passes when it sees the sandbox of such a
# create go away.
#
# It does not fail when the sandbox stays. It cannot see when the server
# dropped the handler of the closed request, and a handler that is still alive
# when the create finishes takes the result. The runtime is then right to keep
# the sandbox. The suite reports a sandbox that stays as a warning, and the
# orchestrator unit tests are the regression gate.
#
# It also cannot see why a sandbox went away, so it does not tell a deleted
# sandbox from a create that failed after the client left.

readonly ABANDON_ATTEMPTS=3
readonly CREATE_SEEN_TIMEOUT_SECONDS=30
readonly GONE_TIMEOUT_SECONDS=60
readonly LIST_REQUEST_TIMEOUT_SECONDS=10
readonly CLEANUP_ROUNDS=5
# A sandbox that the cleanup misses ends at this timeout. It is longer than
# all waits of one attempt together, so a sandbox never goes away because of
# it.
readonly SANDBOX_TIMEOUT_SECONDS=300

run_id="abandoned-$(date +%s)-$$"
create_status_file=$(mktemp)
create_pid=""
# Each create gets its own marker, so a list never shows the sandbox of
# another create of this suite.
markers=()
observed_ids=()

# Prints the IDs of the sandboxes with this marker. Without a state the v2
# list also shows a sandbox whose create still runs, which gives the ID of a
# create that nobody answered yet. With the state `running` it shows only a
# sandbox whose create finished.
marked_sandbox_ids() {
  local marker="$1" state="${2:-}"
  local query="metadata=e2e_abandoned%3D${marker}"
  if [[ -n "${state}" ]]; then
    query+="&state=${state}"
  fi
  _curl_do -s --max-time "${LIST_REQUEST_TIMEOUT_SECONDS}" \
    -H "X-API-Key: ${AENV_API_KEY}" \
    "${AENV_URL}/v2/sandboxes?${query}"
  [[ "${HTTP_STATUS}" == "200" ]] || return 1
  echo "${HTTP_BODY}" | jq -r '.[].sandboxID // empty'
}

# A cold start takes about a second, so the request is closed long before its
# answer.
create_payload() {
  local marker="$1"
  jq -nc --arg image "${E2E_DEFAULT_USER_IMAGE}" --arg m "${marker}" \
    --argjson timeout "${SANDBOX_TIMEOUT_SECONDS}" \
    '{image: $image, timeout: $timeout, autoPause: false, metadata: {e2e_abandoned: $m}}'
}

# Deletes a sandbox through the gateway and, in the clustered modes, at each
# node: a gateway has no route to a sandbox whose create answer it never saw.
delete_everywhere() {
  local sandbox_id="$1" node_url
  delete_sandbox "${sandbox_id}"
  if e2e_mode_is_clustered; then
    while IFS= read -r node_url; do
      if [[ -n "${node_url}" ]]; then
        delete_sandbox_at "${node_url}" "${sandbox_id}"
      fi
    done < <(candidate_node_urls)
  fi
}

# Deletes every sandbox of this suite, then reads the lists again: a delete
# can fail, and a create that no list showed yet can still add a sandbox.
delete_suite_sandboxes() {
  local sandbox_id marker round listed clean
  for sandbox_id in ${observed_ids[@]+"${observed_ids[@]}"}; do
    delete_everywhere "${sandbox_id}"
  done
  for ((round = 1; round <= CLEANUP_ROUNDS; round++)); do
    clean=1
    for marker in ${markers[@]+"${markers[@]}"}; do
      if ! listed=$(marked_sandbox_ids "${marker}"); then
        clean=0
        continue
      fi
      while IFS= read -r sandbox_id; do
        if [[ -n "${sandbox_id}" ]]; then
          clean=0
          delete_everywhere "${sandbox_id}"
        fi
      done <<< "${listed}"
    done
    if [[ "${clean}" == "1" ]]; then
      return 0
    fi
    sleep 1
  done
  warn "cleanup could not confirm that every sandbox of this suite is gone; one that is left ends at its ${SANDBOX_TIMEOUT_SECONDS}s timeout"
}

now_ms() {
  date +%s%3N
}

# Watches a sandbox after its create request was closed, until a list that
# works no longer shows it. Fails when it is still listed at the deadline.
#
# Sets `last_not_running_ms`: how long after the close the last list request
# was sent that did not show the sandbox as running. It goes into the report
# of a sandbox that stays and proves nothing: the create may have finished
# before, with the sandbox in another state than running. Sets
# `lists_answered` to the number of list requests that worked.
watch_abandoned_sandbox() {
  local marker="$1" sandbox_id="$2" closed_at_ms="$3"
  local deadline=$((SECONDS + GONE_TIMEOUT_SECONDS))
  local listed asked_at_ms create_finished=0
  last_not_running_ms=0
  lists_answered=0
  while ((SECONDS < deadline)); do
    if [[ "${create_finished}" == "0" ]]; then
      asked_at_ms=$(now_ms)
      if listed=$(marked_sandbox_ids "${marker}" running); then
        if grep -qxF "${sandbox_id}" <<< "${listed}"; then
          create_finished=1
        else
          last_not_running_ms=$((asked_at_ms - closed_at_ms))
        fi
      fi
    fi
    if listed=$(marked_sandbox_ids "${marker}"); then
      lists_answered=$((lists_answered + 1))
      if ! grep -qxF "${sandbox_id}" <<< "${listed}"; then
        return 0
      fi
    fi
    if [[ "${create_finished}" == "1" ]]; then
      sleep 0.5
    else
      sleep 0.05
    fi
  done
  return 1
}

cleanup_suite() {
  if [[ -n "${create_pid}" ]]; then
    kill "${create_pid}" 2>/dev/null || true
  fi
  delete_suite_sandboxes || true
  rm -f "${create_status_file}"
  _cleanup_e2e
}
trap cleanup_suite EXIT

# The same request must work when its client stays, or the rest proves nothing.
markers+=("${run_id}-waited")
api_post "/sandboxes-cold" "$(create_payload "${run_id}-waited")"
assert_status "${HTTP_STATUS}" "201" "cold create succeeds when the client waits"
delivered_id=$(echo "${HTTP_BODY}" | jq -r '.sandboxID // empty')
if [[ -n "${delivered_id}" ]]; then
  observed_ids+=("${delivered_id}")
  delete_everywhere "${delivered_id}"
fi

# Sends the create in the background and records its HTTP status, if any.
start_create() {
  local marker="$1"
  : > "${create_status_file}"
  curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "X-API-Key: ${AENV_API_KEY}" \
    -H "Content-Type: application/json" \
    -d "$(create_payload "${marker}")" \
    "${AENV_URL}/sandboxes-cold" > "${create_status_file}" 2>/dev/null &
  create_pid=$!
}

# Each attempt closes the request once a list shows the create and then
# watches the sandbox. The suite passes with the first sandbox that goes away.
# A create that was answered proves nothing, because its handler took the
# result. A sandbox that stays proves nothing either. Both are tried again.
deleted=0
stayed=0
answered=0
failure=""
for ((attempt = 1; attempt <= ABANDON_ATTEMPTS; attempt++)); do
  marker="${run_id}-${attempt}"
  markers+=("${marker}")
  start_create "${marker}"
  seen_id=""
  list_requests=0
  list_failures=0
  seen_deadline=$((SECONDS + CREATE_SEEN_TIMEOUT_SECONDS))
  while ((SECONDS < seen_deadline)); do
    # Read this before the list: a request that ended before a list was sent
    # gets no later chance to be shown as a running create.
    request_ended=0
    kill -0 "${create_pid}" 2>/dev/null || request_ended=1
    list_requests=$((list_requests + 1))
    if listed=$(marked_sandbox_ids "${marker}"); then
      seen_id=$(head -n 1 <<< "${listed}")
    else
      list_failures=$((list_failures + 1))
    fi
    if [[ -n "${seen_id}" || "${request_ended}" == "1" ]]; then
      break
    fi
    sleep 0.05
  done
  kill "${create_pid}" 2>/dev/null || true
  wait "${create_pid}" 2>/dev/null || true
  create_pid=""
  closed_at_ms=$(now_ms)
  create_status=$(<"${create_status_file}")
  if [[ -z "${seen_id}" ]]; then
    failure="no list showed the create (${list_failures} of ${list_requests} list requests failed; the create was answered with HTTP ${create_status:-nothing})"
    break
  fi
  observed_ids+=("${seen_id}")
  if [[ -n "${create_status}" ]]; then
    answered=$((answered + 1))
    log "attempt ${attempt}: proves nothing, the create was answered with HTTP ${create_status} before the request closed"
    delete_everywhere "${seen_id}"
    continue
  fi
  if watch_abandoned_sandbox "${marker}" "${seen_id}" "${closed_at_ms}"; then
    deleted=1
    break
  fi
  if ((lists_answered == 0)); then
    failure="no list request worked for ${GONE_TIMEOUT_SECONDS}s after the request was closed"
    break
  fi
  stayed=$((stayed + 1))
  log "attempt ${attempt}: sandbox ${seen_id} stayed; the last list that did not show it as running was sent ${last_not_running_ms} ms after the request was closed"
  delete_everywhere "${seen_id}"
done

if [[ "${deleted}" == "1" ]]; then
  _pass "sandbox of a create that outlived its request is gone"
elif [[ -n "${failure}" ]]; then
  _fail "the lists show a create whose request was closed" "listed" "${failure}"
else
  warn "the delete was not observed in ${ABANDON_ATTEMPTS} attempts: the sandbox stayed in ${stayed}, and the create was answered in ${answered}"
fi

suite_summary "20_abandoned_create"
