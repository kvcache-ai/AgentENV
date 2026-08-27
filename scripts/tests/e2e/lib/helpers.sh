#!/usr/bin/env bash
# Shared HTTP and sandbox helpers for e2e test suites.

if [[ -z "${E2E_HELPERS_SH_LOADED:-}" ]]; then
  E2E_HELPERS_SH_LOADED=1

  : "${AENV_URL:?AENV_URL must be set}"
  : "${AENV_API_KEY:=e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"
  : "${AENV_TEMPLATE_ID:=ubuntu}"
  : "${AENV_PROXY_URL:=${AENV_URL}/proxy}"
  : "${AENV_ENVD_PORT:=49983}"
  : "${E2E_MODE:=single-node}"
  : "${E2E_DEFAULT_USER_IMAGE:=ghcr.io/linuxserver/baseimage-ubuntu:noble}"
  : "${E2E_TEMPLATE_USER_IMAGE:=${E2E_DEFAULT_USER_IMAGE}}"

  # ---- low-level HTTP wrappers ------------------------------------------------
  # curl writes the response body to _E2E_BODY and the HTTP status code to
  # _E2E_STATUS.  Callers read HTTP_BODY / HTTP_STATUS after each call.
  # This avoids subshell issues with $() losing variable assignments.

  _E2E_BODY=$(mktemp)
  _E2E_STATUS=$(mktemp)
  _E2E_HEADERS=""  # populated by api_get_with_headers

  HTTP_STATUS=""
  HTTP_BODY=""
  HTTP_HEADERS=""
  _E2E_ERROR_REPORTED=0
  _E2E_SUITE_SUMMARY_RAN=0

  e2e_report_unexpected_error() {
    local status="${1:-$?}"
    local command="${2:-${BASH_COMMAND:-unknown}}"
    local source_file="${3:-${BASH_SOURCE[1]:-${BASH_SOURCE[0]:-unknown}}}"
    local line="${4:-${BASH_LINENO[0]:-0}}"
    local phase="${5:-${LOG_TAG:-E2E}}"

    if [[ "${status}" -eq 0 ]]; then
      return 0
    fi
    if [[ "${_E2E_ERROR_REPORTED:-0}" == "1" ]]; then
      return 0
    fi

    _E2E_ERROR_REPORTED=1
    error "${phase} failed unexpectedly (exit ${status}) at ${source_file}:${line}"
    printf "       command: %s\n" "${command}" >&2
  }

  e2e_report_suite_error() {
    local status=$?
    local command="${BASH_COMMAND:-unknown}"
    local source_file="${BASH_SOURCE[1]:-${BASH_SOURCE[0]:-unknown}}"
    local line="${BASH_LINENO[0]:-0}"

    if [[ "${_E2E_SUITE_SUMMARY_RAN:-0}" == "1" ]]; then
      return "${status}"
    fi

    e2e_report_unexpected_error "${status}" "${command}" "${source_file}" "${line}" "${LOG_TAG:-suite}"
    return "${status}"
  }

  _curl_do() {
    curl "$@" -o "$_E2E_BODY" -w '%{http_code}' > "$_E2E_STATUS" 2>/dev/null || true
    HTTP_STATUS=$(<"$_E2E_STATUS")
    HTTP_BODY=$(<"$_E2E_BODY")
  }

  e2e_mode_is() {
    [[ "${E2E_MODE}" == "$1" ]]
  }

  e2e_mode_is_clustered() {
    e2e_mode_is "compose" || e2e_mode_is "k8s"
  }

  # Re-read HTTP_STATUS/HTTP_BODY from disk after a subshell call.
  # Use after sandbox_id=$(create_sandbox) or state=$(get_sandbox_state ...).
  _sync_http() {
    HTTP_STATUS=$(<"$_E2E_STATUS")
    HTTP_BODY=$(<"$_E2E_BODY")
  }

  api_get() {
    local path="$1"
    _curl_do -s \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${AENV_URL}${path}"
  }

  api_get_at() {
    local base_url="$1"
    local path="$2"
    _curl_do -s \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${base_url}${path}"
  }

  # GET that also captures response headers into HTTP_HEADERS.
  api_get_with_headers() {
    local path="$1"
    [[ -z "$_E2E_HEADERS" ]] && _E2E_HEADERS=$(mktemp)
    curl -s -o "$_E2E_BODY" -D "$_E2E_HEADERS" -w '%{http_code}' \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${AENV_URL}${path}" > "$_E2E_STATUS" 2>/dev/null || true
    HTTP_STATUS=$(<"$_E2E_STATUS")
    HTTP_BODY=$(<"$_E2E_BODY")
    HTTP_HEADERS=$(<"$_E2E_HEADERS")
  }

  api_get_with_headers_at() {
    local base_url="$1"
    local path="$2"
    [[ -z "$_E2E_HEADERS" ]] && _E2E_HEADERS=$(mktemp)
    curl -s -o "$_E2E_BODY" -D "$_E2E_HEADERS" -w '%{http_code}' \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${base_url}${path}" > "$_E2E_STATUS" 2>/dev/null || true
    HTTP_STATUS=$(<"$_E2E_STATUS")
    HTTP_BODY=$(<"$_E2E_BODY")
    HTTP_HEADERS=$(<"$_E2E_HEADERS")
  }

  api_post() {
    local path="$1"
    local body="${2:-}"
    local args=(-s -X POST
      -H "X-API-Key: ${AENV_API_KEY}"
      -H "Content-Type: application/json")
    [[ -n "$body" ]] && args+=(-d "$body")
    _curl_do "${args[@]}" "${AENV_URL}${path}"
  }

  api_post_at() {
    local base_url="$1"
    local path="$2"
    local body="${3:-}"
    local args=(-s -X POST
      -H "X-API-Key: ${AENV_API_KEY}"
      -H "Content-Type: application/json")
    [[ -n "$body" ]] && args+=(-d "$body")
    _curl_do "${args[@]}" "${base_url}${path}"
  }

  api_delete() {
    local path="$1"
    _curl_do -s -X DELETE \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${AENV_URL}${path}"
  }

  api_delete_at() {
    local base_url="$1"
    local path="$2"
    _curl_do -s -X DELETE \
      -H "X-API-Key: ${AENV_API_KEY}" \
      "${base_url}${path}"
  }

  api_get_no_auth() {
    local path="$1"
    _curl_do -s "${AENV_URL}${path}"
  }

  api_get_no_auth_at() {
    local base_url="$1"
    local path="$2"
    _curl_do -s "${base_url}${path}"
  }

  proxy_get_with_sandbox() {
    local sandbox_id="$1"
    local path="${2:-/health}"
    local target_port="${3:-${AENV_ENVD_PORT}}"
    _curl_do -s --max-time 5 \
      -H "x-agentenv-sandbox-id: ${sandbox_id}" \
      -H "x-agentenv-target-port: ${target_port}" \
      "${AENV_PROXY_URL}${path}"
  }

  # ---- sandbox lifecycle helpers -----------------------------------------------

  # Tracked sandbox IDs for automatic cleanup.
  _TRACKED_SANDBOX_IDS=()
  # Tracked template IDs for automatic cleanup.
  _TRACKED_TEMPLATE_IDS=()

  _cleanup_e2e() {
    for sid in "${_TRACKED_SANDBOX_IDS[@]}"; do
      api_delete "/sandboxes/${sid}" 2>/dev/null || true
    done
    for tid in "${_TRACKED_TEMPLATE_IDS[@]}"; do
      api_delete "/templates/${tid}" 2>/dev/null || true
    done
    rm -f "$_E2E_BODY" "$_E2E_STATUS" "${_E2E_HEADERS:-}"
  }
  trap _cleanup_e2e EXIT

  # Register a sandbox ID for cleanup on exit.
  track_sandbox() {
    _TRACKED_SANDBOX_IDS+=("$1")
  }

  # Register a template ID for cleanup on exit.
  track_template() {
    _TRACKED_TEMPLATE_IDS+=("$1")
  }

  # Create a template through the v3 API and start its build. HTTP-level failures
  # are left in HTTP_STATUS/HTTP_BODY for caller assertions instead of tripping
  # set -e before the assertion can report the response.
  # Sets HTTP_STATUS and HTTP_BODY to the create response when both steps succeed.
  # fromImage defaults to E2E_TEMPLATE_USER_IMAGE. Pass an explicit empty string to omit it.
  create_template() {
    local name="$1"
    local user_image="${2-${E2E_TEMPLATE_USER_IMAGE}}"
    local payload
    payload=$(jq -nc \
      --arg name "$name" \
      '{name: $name, cpuCount: 1, memoryMB: 128}')
    api_post "/v3/templates" "$payload"
    local create_status="$HTTP_STATUS"
    local create_body="$HTTP_BODY"
    [[ "$create_status" == "202" ]] || return 0

    local template_id build_id start_payload
    template_id=$(echo "$create_body" | jq -r '.templateID // empty')
    build_id=$(echo "$create_body" | jq -r '.buildID // empty')
    start_payload=$(jq -nc '{}')
    if [[ -n "$user_image" ]]; then
      start_payload=$(echo "$start_payload" | jq --arg image "$user_image" '. + {fromImage: $image}')
    fi
    api_post "/v2/templates/${template_id}/builds/${build_id}" "$start_payload"
    if [[ "$HTTP_STATUS" == "202" ]]; then
      printf '%s' "$create_status" > "$_E2E_STATUS"
      printf '%s' "$create_body" > "$_E2E_BODY"
      HTTP_STATUS="$create_status"
      HTTP_BODY="$create_body"
    fi
  }

  create_template_at() {
    local base_url="$1"
    local name="$2"
    local user_image="${3-${E2E_TEMPLATE_USER_IMAGE}}"
    local payload
    payload=$(jq -nc \
      --arg name "$name" \
      '{name: $name, cpuCount: 1, memoryMB: 128}')
    api_post_at "$base_url" "/v3/templates" "$payload"
    local create_status="$HTTP_STATUS"
    local create_body="$HTTP_BODY"
    [[ "$create_status" == "202" ]] || return 0

    local template_id build_id start_payload
    template_id=$(echo "$create_body" | jq -r '.templateID // empty')
    build_id=$(echo "$create_body" | jq -r '.buildID // empty')
    start_payload=$(jq -nc '{}')
    if [[ -n "$user_image" ]]; then
      start_payload=$(echo "$start_payload" | jq --arg image "$user_image" '. + {fromImage: $image}')
    fi
    api_post_at "$base_url" "/v2/templates/${template_id}/builds/${build_id}" "$start_payload"
    if [[ "$HTTP_STATUS" == "202" ]]; then
      printf '%s' "$create_status" > "$_E2E_STATUS"
      printf '%s' "$create_body" > "$_E2E_BODY"
      HTTP_STATUS="$create_status"
      HTTP_BODY="$create_body"
    fi
  }

  # Create a sandbox. Sets HTTP_STATUS and HTTP_BODY.
  # Prints sandboxID to stdout.
  create_sandbox() {
    local template="${1:-$AENV_TEMPLATE_ID}"
    local timeout="${2:-60}"
    local extra_json="${3:-}"
    local payload
    payload=$(jq -nc \
      --arg t "$template" \
      --argjson to "$timeout" \
      '{templateID: $t, timeout: $to, autoPause: true}')
    if [[ -n "$extra_json" ]]; then
      payload=$(echo "$payload" "$extra_json" | jq -s '.[0] * .[1]')
    fi
    api_post "/sandboxes" "$payload"
    echo "$HTTP_BODY" | jq -r '.sandboxID // empty'
  }

  create_sandbox_at() {
    local base_url="$1"
    local template="${2:-$AENV_TEMPLATE_ID}"
    local timeout="${3:-60}"
    local extra_json="${4:-}"
    local payload
    payload=$(jq -nc \
      --arg t "$template" \
      --argjson to "$timeout" \
      '{templateID: $t, timeout: $to, autoPause: true}')
    if [[ -n "$extra_json" ]]; then
      payload=$(echo "$payload" "$extra_json" | jq -s '.[0] * .[1]')
    fi
    api_post_at "$base_url" "/sandboxes" "$payload"
    echo "$HTTP_BODY" | jq -r '.sandboxID // empty'
  }

  delete_sandbox() {
    local id="$1"
    api_delete "/sandboxes/${id}"
  }

  delete_sandbox_at() {
    local base_url="$1"
    local id="$2"
    api_delete_at "$base_url" "/sandboxes/${id}"
  }

  get_sandbox_state() {
    local id="$1"
    api_get "/sandboxes/${id}"
    echo "$HTTP_BODY" | jq -r '.state // empty'
  }

  get_sandbox_state_at() {
    local base_url="$1"
    local id="$2"
    api_get_at "$base_url" "/sandboxes/${id}"
    echo "$HTTP_BODY" | jq -r '.state // empty'
  }

  wait_for_sandbox_state() {
    local id="$1" target_state="$2" timeout="${3:-30}"
    for ((i = 0; i < timeout * 2; i++)); do
      local state
      state=$(get_sandbox_state "$id")
      [[ "$state" == "$target_state" ]] && return 0
      sleep 0.5
    done
    warn "Timed out waiting for sandbox $id to reach state '$target_state'"
    return 1
  }

  wait_for_sandbox_state_at() {
    local base_url="$1" id="$2" target_state="$3" timeout="${4:-30}"
    for ((i = 0; i < timeout * 2; i++)); do
      local state
      state=$(get_sandbox_state_at "$base_url" "$id")
      [[ "$state" == "$target_state" ]] && return 0
      sleep 0.5
    done
    warn "Timed out waiting for sandbox $id at ${base_url} to reach state '$target_state'"
    return 1
  }

  wait_for_volume_status() {
    local id="$1" target_status="$2" timeout="${3:-30}"
    for ((i = 0; i < timeout * 2; i++)); do
      api_get "/volumes/${id}"
      if [[ "$HTTP_STATUS" == "200" ]] \
        && [[ "$(echo "$HTTP_BODY" | jq -r '.status // empty')" == "$target_status" ]]; then
        return 0
      fi
      sleep 0.5
    done
    warn "Timed out waiting for volume $id to reach status '$target_status'"
    return 1
  }

  # Poll until a template build reaches "ready" or "error".
  _template_build_status_field() {
    local body="$1"
    echo "$body" | jq -r '(.builds[0].status // .buildStatus // .status) // empty' 2>/dev/null || true
  }

  _template_build_reason_field() {
    local body="$1"
    echo "$body" | jq -r '
      [
        .reason.message?,
        .builds[0].reason.message?,
        .error.message?,
        .message?,
        (.reason? | if type == "string" then . else empty end),
        (.builds[0].reason? | if type == "string" then . else empty end)
      ]
      | map(select(. != null and . != ""))
      | first // empty
    ' 2>/dev/null || true
  }

  _template_build_step_field() {
    local body="$1"
    echo "$body" | jq -r '(.reason.step // .builds[0].reason.step) // empty' 2>/dev/null || true
  }

  _log_template_build_failure() {
    local base_url="$1" template_id="$2" label="$3" body="$4" build_id="${5:-}"
    local status reason step detail_status detail_body

    status="$(_template_build_status_field "$body")"
    reason="$(_template_build_reason_field "$body")"
    step="$(_template_build_step_field "$body")"

    if [[ -z "$reason" && -n "$build_id" ]]; then
      if [[ "$base_url" == "$AENV_URL" ]]; then
        api_get "/templates/${template_id}/builds/${build_id}/status"
      else
        api_get_at "$base_url" "/templates/${template_id}/builds/${build_id}/status"
      fi
      detail_status="$HTTP_STATUS"
      detail_body="$HTTP_BODY"
      [[ -z "$status" ]] && status="$(_template_build_status_field "$detail_body")"
      reason="$(_template_build_reason_field "$detail_body")"
      step="$(_template_build_step_field "$detail_body")"
    else
      detail_status="$HTTP_STATUS"
      detail_body="$body"
    fi

    warn "${label} failed${status:+ with status '${status}'}"
    [[ -n "$step" ]] && warn "Template build failure step: ${step}"
    if [[ -n "$reason" ]]; then
      warn "Template build failure reason: ${reason}"
    else
      warn "Template build failure details unavailable; status response (HTTP ${detail_status:-unknown}): ${detail_body:-<empty>}"
    fi
  }

  wait_for_template_build() {
    local template_id="$1" timeout="${2:-120}" build_id="${3:-}"
    local build_status=""
    for ((i = 0; i < timeout; i++)); do
      api_get "/templates/${template_id}"
      build_status="$(_template_build_status_field "$HTTP_BODY")"
      case "$build_status" in
        ready) return 0 ;;
        error)
          _log_template_build_failure "$AENV_URL" "$template_id" "Template build" "$HTTP_BODY" "$build_id"
          return 1
          ;;
      esac
      sleep 1
    done
    warn "Template build timed out after ${timeout}s"
    _log_template_build_failure "$AENV_URL" "$template_id" "Template build" "$HTTP_BODY" "$build_id"
    return 1
  }

  wait_for_template_build_at() {
    local base_url="$1" template_id="$2" timeout="${3:-120}" build_id="${4:-}"
    local build_status=""
    for ((i = 0; i < timeout; i++)); do
      api_get_at "$base_url" "/templates/${template_id}"
      build_status="$(_template_build_status_field "$HTTP_BODY")"
      case "$build_status" in
        ready) return 0 ;;
        error)
          _log_template_build_failure "$base_url" "$template_id" "Template build at ${base_url}" "$HTTP_BODY" "$build_id"
          return 1
          ;;
      esac
      sleep 1
    done
    warn "Template build timed out after ${timeout}s at ${base_url}"
    _log_template_build_failure "$base_url" "$template_id" "Template build at ${base_url}" "$HTTP_BODY" "$build_id"
    return 1
  }

  candidate_node_urls() {
    if [[ -n "${AENV_NODE_URLS:-}" ]]; then
      for node_url in ${AENV_NODE_URLS}; do
        [[ -n "${node_url}" ]] && printf '%s\n' "${node_url}"
      done
      return 0
    fi
    [[ -n "${AENV_NODE_A_URL:-}" ]] && printf '%s\n' "${AENV_NODE_A_URL}"
    [[ -n "${AENV_NODE_B_URL:-}" ]] && printf '%s\n' "${AENV_NODE_B_URL}"
  }

  node_label_for_url() {
    local url="$1"
    local mapping
    local key
    local value
    local old_ifs

    if [[ -n "${AENV_NODE_URL_LABEL_MAP:-}" ]]; then
      old_ifs="${IFS}"
      IFS=';'
      for mapping in ${AENV_NODE_URL_LABEL_MAP}; do
        key="${mapping%%=*}"
        value="${mapping#*=}"
        if [[ "${key}" == "${url}" ]]; then
          IFS="${old_ifs}"
          printf '%s\n' "${value}"
          return 0
        fi
      done
      IFS="${old_ifs}"
    fi

    case "${url}" in
      "${AENV_NODE_A_URL:-}")
        printf '%s\n' "${AENV_NODE_A_LABEL:-agentenv-a}"
        ;;
      "${AENV_NODE_B_URL:-}")
        printf '%s\n' "${AENV_NODE_B_LABEL:-agentenv-b}"
        ;;
      *)
        printf '%s\n' "$1"
        ;;
    esac
  }

  find_sandbox_node_url() {
    local sandbox_id="$1"
    local found=""
    local node_url

    while IFS= read -r node_url; do
      [[ -z "$node_url" ]] && continue
      api_get_at "$node_url" "/sandboxes/${sandbox_id}"
      if [[ "$HTTP_STATUS" == "200" ]]; then
        if [[ -n "$found" && "$found" != "$node_url" ]]; then
          warn "sandbox ${sandbox_id} resolved on multiple runtime nodes"
        fi
        found="$node_url"
      fi
    done < <(candidate_node_urls)

    [[ -n "$found" ]] && printf '%s\n' "$found"
  }

  # ---- suite init --------------------------------------------------------------

  init_suite() {
    set -E

    local suite_dir
    suite_dir="$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)"
    local e2e_dir="${suite_dir}/.."
    local repo_root="${e2e_dir}/../../.."

    local common_sh="${repo_root}/scripts/lib/common.sh"
    [[ -f "$common_sh" ]] || { echo "Missing ${common_sh}" >&2; exit 1; }
    # shellcheck source=/dev/null
    source "$common_sh"

    # shellcheck source=/dev/null
    source "${e2e_dir}/lib/assertions.sh"
    # shellcheck source=/dev/null
    source "${e2e_dir}/lib/helpers.sh"

    LOG_TAG="${1:-E2E}"
    _E2E_SUITE_SUMMARY_RAN=0
    trap e2e_report_suite_error ERR
  }
fi
