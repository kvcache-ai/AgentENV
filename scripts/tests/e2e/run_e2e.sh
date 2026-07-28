#!/usr/bin/env bash
# End-to-end test runner for AgentENV.
#
# Builds the server, starts it, creates a base template, then runs each suite.
# Requires: curl, jq, and a Linux host whose /dev/kvm and modules match
# AENV_VIRTUALIZATION_MODE (kvm by default).
#
# Environment variables:
#   AENV_PORT         - Port for the test server (default: 18080)
#   CONFIG_PATH           - Path to config file (default: config/default.toml)
#   AENV_TEMPLATE_ID  - Pre-existing template to use (skips template creation)
#   E2E_DEFAULT_USER_IMAGE - Default fromImage for template builds
#   E2E_TEMPLATE_USER_IMAGE - Optional fromImage override for the base template build
#   SKIP_BUILD            - Set to 1 to skip the build step
#   SUITE_FILTER          - Run only suites matching this glob (e.g. "02*")
#   E2E_MODE              - Runtime mode: single-node, compose, or k8s
set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/../../.."

COMMON_SH="${REPO_ROOT}/scripts/lib/common.sh"
[[ -f "$COMMON_SH" ]] || { echo "Missing ${COMMON_SH}" >&2; exit 1; }
# Track if user explicitly provided a template ID (before helpers.sh defaults it)
_E2E_TEMPLATE_FROM_USER=0
[[ -n "${AENV_TEMPLATE_ID:-}" ]] && _E2E_TEMPLATE_FROM_USER=1

# shellcheck source=/dev/null
source "$COMMON_SH"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/runtime.sh"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/assertions.sh"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/helpers.sh"

LOG_TAG="E2E"
export E2E_DEFAULT_USER_IMAGE="${E2E_DEFAULT_USER_IMAGE:-ghcr.io/linuxserver/baseimage-ubuntu:noble}"
export E2E_TEMPLATE_USER_IMAGE="${E2E_TEMPLATE_USER_IMAGE:-$E2E_DEFAULT_USER_IMAGE}"

_E2E_PHASE="initialization"
report_unexpected_error() {
  local status=$?
  local command="${BASH_COMMAND:-unknown}"
  local source_file="${BASH_SOURCE[1]:-${BASH_SOURCE[0]:-unknown}}"
  local line="${BASH_LINENO[0]:-0}"

  e2e_report_unexpected_error "${status}" "${command}" "${source_file}" "${line}" "${_E2E_PHASE}"
  return "${status}"
}
trap report_unexpected_error ERR

require_cmd curl
require_cmd jq

# ---- Build -------------------------------------------------------------------

SERVER_BIN=""

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  _E2E_PHASE="build"
  require_cmd make

  if e2e_mode_is "compose"; then
    log "Building compose runtime images ..."
    _run_deploy_target deploy-build
  elif e2e_mode_is "k8s"; then
    require_cmd docker
    require_cmd kubectl
    log "Building kubernetes runtime images ..."
    make --no-print-directory -C "${REPO_ROOT}" k8s-build
  else
    log "Building single-node e2e runtime ..."
    make --no-print-directory -C "${REPO_ROOT}" build-server
  fi
fi

if ! e2e_mode_is "compose" && ! e2e_mode_is "k8s"; then
  _E2E_PHASE="build"
  server_profile="debug"

  SERVER_BIN="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/${server_profile}/server"
  [[ -x "$SERVER_BIN" ]] || die "Server binary not found at ${SERVER_BIN}"
fi

# ---- Start server ------------------------------------------------------------

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    (dump_test_runtime_diagnostics) || true
  fi
  _cleanup_e2e
  stop_test_runtime
  return "${status}"
}
trap cleanup EXIT

_E2E_PHASE="start runtime"
start_test_runtime "$SERVER_BIN" "${AENV_CONFIG_PATH:-${CONFIG_PATH:-}}"
wait_for_test_runtime

# ---- Create base template ----------------------------------------------------
# If no AENV_TEMPLATE_ID was explicitly set, build one via the API.
# (helpers.sh defaults it to "ubuntu", so we check if it was set before sourcing.)

if [[ "${_E2E_TEMPLATE_FROM_USER:-0}" != "1" ]]; then
  _E2E_PHASE="create base template"
  E2E_TEMPLATE_NAME="e2e-base-$(date +%s)"
  log "Creating base template '${E2E_TEMPLATE_NAME}' ..."

  create_template "$E2E_TEMPLATE_NAME" "$E2E_TEMPLATE_USER_IMAGE"

  if [[ "$HTTP_STATUS" != "202" ]]; then
    die "Failed to create base template (HTTP ${HTTP_STATUS}): ${HTTP_BODY}"
  fi

  E2E_TEMPLATE_ID=$(echo "$HTTP_BODY" | jq -r '.templateID // empty')
  E2E_TEMPLATE_BUILD_ID=$(echo "$HTTP_BODY" | jq -r '.buildID // empty')
  [[ -n "$E2E_TEMPLATE_ID" ]] || die "No templateID in response"
  track_template "$E2E_TEMPLATE_ID"

  log "Waiting for template build to complete (id: ${E2E_TEMPLATE_ID}) ..."
  wait_for_template_build "$E2E_TEMPLATE_ID" 120 "$E2E_TEMPLATE_BUILD_ID" || die "Template build failed or timed out"
  log "Template build ready."

  export AENV_TEMPLATE_ID="$E2E_TEMPLATE_NAME"
  export E2E_TEMPLATE_UUID="$E2E_TEMPLATE_ID"  # UUID, for cleanup and cross-referencing
  log "Using template: ${AENV_TEMPLATE_ID}"
fi

# ---- Run suites --------------------------------------------------------------

SUITES_DIR="${SCRIPT_DIR}/suites"
suite_filter="${SUITE_FILTER:-*.sh}"

total_suites=0
passed_suites=0
failed_suites=0
failed_names=()

for suite in "${SUITES_DIR}"/${suite_filter}; do
  [[ -f "$suite" ]] || continue
  suite_name="$(basename "$suite" .sh)"
  ((total_suites++)) || true

  _E2E_PHASE="suite ${suite_name}"
  log "Running suite: ${suite_name}"
  if bash "$suite"; then
    ((passed_suites++)) || true
  else
    ((failed_suites++)) || true
    failed_names+=("$suite_name")
  fi
done

# ---- Summary -----------------------------------------------------------------

echo ""
echo "========================================"
log "E2E Test Summary"
echo "========================================"
printf "  Total:  %d\n" "$total_suites"
printf "  %bPassed: %d%b\n" "${LOG_COLOR_GREEN}" "$passed_suites" "${LOG_COLOR_RESET}"
if [[ "$failed_suites" -gt 0 ]]; then
  printf "  %bFailed: %d%b\n" "${LOG_COLOR_RED}" "$failed_suites" "${LOG_COLOR_RESET}"
  for name in "${failed_names[@]}"; do
    printf "    - %s\n" "$name"
  done
fi
echo "========================================"

if [[ "$failed_suites" -gt 0 ]]; then
  exit 1
fi
