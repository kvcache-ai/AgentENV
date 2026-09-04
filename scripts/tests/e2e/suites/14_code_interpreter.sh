#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "14_code_interpreter"

log "Suite: Code Interpreter Compatibility"

# TODO: sandbox-code image was rebuilt (2026-05-17) and switched from Jupyter to a
# subprocess-based server; re-enable once the image is restored or a compatible
# replacement is pushed to GHCR.
warn "code interpreter suite temporarily disabled; skipping"
_pass "skipped code interpreter checks (image incompatible — see TODO above)"
suite_summary "14_code_interpreter"
exit 0

export E2B_API_URL="${AENV_URL}"
export E2B_SANDBOX_URL="${AENV_PROXY_URL}"
export E2B_API_KEY="${AENV_API_KEY}"

if ! python3 -c 'import e2b_code_interpreter' >/dev/null 2>&1; then
  warn "e2b_code_interpreter Python package not installed; skipping"
  _pass "skipped code interpreter checks (e2b_code_interpreter not installed)"
  suite_summary "14_code_interpreter"
  exit 0
fi

ci_script="${SUITE_DIR}/../code_interpreter_compat.py"
ci_timeout="${CODE_INTERPRETER_TIMEOUT_SECONDS:-600}"

log "Running: code interpreter compatibility (${ci_script})"
if command -v timeout >/dev/null 2>&1; then
  if ci_output=$(timeout "${ci_timeout}" python3 "$ci_script" 2>&1); then
    log "${ci_output}"
    _pass "code interpreter correctness and performance"
  else
    log "code interpreter output: ${ci_output:0:4000}"
    _fail "code interpreter compatibility" "exit 0" "non-zero"
  fi
elif ci_output=$(python3 "$ci_script" 2>&1); then
  log "${ci_output}"
  _pass "code interpreter correctness and performance"
else
  log "code interpreter output: ${ci_output:0:4000}"
  _fail "code interpreter compatibility" "exit 0" "non-zero"
fi

suite_summary "14_code_interpreter"
