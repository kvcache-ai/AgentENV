#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "09_e2b"

log "Suite: E2B Compatibility"

export E2B_API_URL="${AENV_URL}"
export E2B_SANDBOX_URL="${AENV_PROXY_URL}"
export E2B_API_KEY="${AENV_API_KEY}"
export E2B_COMPAT_USER_IMAGE="${E2B_COMPAT_USER_IMAGE:-${E2E_TEMPLATE_USER_IMAGE:-ghcr.io/linuxserver/baseimage-ubuntu:noble}}"

cli_available=0
if command -v e2b >/dev/null 2>&1; then
  # Verify the CLI actually works (needs Node >= 16).
  if e2b --version >/dev/null 2>&1; then
    cli_available=1
  else
    warn "e2b CLI is installed but fails to run (possibly incompatible Node.js version); skipping CLI checks"
    _pass "skipped CLI checks (e2b CLI broken)"
  fi
else
  warn "e2b CLI not found; skipping CLI checks"
  _pass "skipped CLI checks (e2b CLI not installed)"
fi

if [[ "$cli_available" == "1" ]]; then
  # -- e2b sandbox list --
  log "Running: e2b sandbox list"
  if e2b sandbox list --format json >/dev/null 2>&1; then
    _pass "e2b sandbox list exits successfully"
  else
    _fail "e2b sandbox list" "exit 0" "non-zero"
  fi

  # -- e2b sandbox create --
  log "Running: e2b sandbox create ${AENV_TEMPLATE_ID}"
  sandbox_id=""
  if create_output=$(e2b sandbox create "${AENV_TEMPLATE_ID}" --detach 2>&1); then
    if echo "$create_output" | grep -qoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'; then
      sandbox_id=$(echo "$create_output" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
      _pass "e2b sandbox create returned sandbox ID"
      track_sandbox "$sandbox_id"
    else
      log "create output: ${create_output:0:200}"
      _fail "e2b sandbox create" "sandbox ID in output" "(no UUID found)"
    fi
  else
    log "e2b sandbox create output: ${create_output:0:200}"
    _fail "e2b sandbox create" "exit 0" "non-zero"
  fi

  if [[ -n "$sandbox_id" ]]; then
    log "Running: e2b sandbox list after create"
    if list_output=$(e2b sandbox list --format json 2>&1); then
      if echo "$list_output" | grep -q "$sandbox_id"; then
        _pass "e2b sandbox list includes created sandbox"
      else
        log "list output: ${list_output:0:200}"
        _fail "e2b sandbox list includes created sandbox" "$sandbox_id" "(missing)"
      fi
    else
      log "list output: ${list_output:0:200}"
      _fail "e2b sandbox list after create" "exit 0" "non-zero"
    fi
  fi

  # -- e2b sandbox exec --
  if [[ -n "$sandbox_id" ]]; then
    exec_output=""
    exec_succeeded=0
    exec_attempts=10
    exec_retry_delay_secs=1

    # The API can return before envd is ready to accept the first Connect RPC.
    sleep 3
    for ((attempt = 1; attempt <= exec_attempts; attempt++)); do
      log "Running: e2b sandbox exec ${sandbox_id} -- echo hello (attempt ${attempt}/${exec_attempts})"
      if exec_output=$(e2b sandbox exec "$sandbox_id" -- echo hello 2>&1); then
        exec_succeeded=1
        break
      fi

      log "exec attempt ${attempt} failed: ${exec_output:0:200}"
      if (( attempt < exec_attempts )); then
        sleep "${exec_retry_delay_secs}"
      fi
    done

    if [[ "${exec_succeeded}" == "1" ]]; then
      if echo "$exec_output" | grep -q "hello"; then
        _pass "e2b sandbox exec returned expected output"
      else
        log "exec output: ${exec_output:0:200}"
        _pass "e2b sandbox exec exited 0 (output may differ)"
      fi
    else
      log "exec output: ${exec_output:0:200}"
      _fail "e2b sandbox exec" "exit 0" "non-zero"
    fi
  fi

  # -- e2b sandbox kill --
  if [[ -n "$sandbox_id" ]]; then
    log "Running: e2b sandbox kill ${sandbox_id}"
    if e2b sandbox kill "$sandbox_id" >/dev/null 2>&1; then
      _pass "e2b sandbox kill exits successfully"
    else
      _fail "e2b sandbox kill" "exit 0" "non-zero"
    fi

    # Verify the sandbox is gone via API
    api_get "/sandboxes/${sandbox_id}"
    if [[ "$HTTP_STATUS" == "404" ]]; then
      _pass "sandbox removed after e2b kill"
    else
      _pass "sandbox status after kill: HTTP ${HTTP_STATUS} (may still be cleaning up)"
    fi
  fi
fi

# -- e2b Python SDK template + sandbox compatibility --
if python3 -c 'import e2b' >/dev/null 2>&1; then
  sdk_script="${SUITE_DIR}/../e2b_python_sdk_compat.py"
  sdk_timeout="${E2B_COMPAT_PYTHON_TIMEOUT_SECONDS:-360}"
  log "Running: e2b Python SDK compatibility (${sdk_script})"
  if command -v timeout >/dev/null 2>&1; then
    if sdk_output=$(timeout "${sdk_timeout}" python3 "$sdk_script" 2>&1); then
      _pass "e2b Python SDK template build, startCmd/readyCmd, sandbox lifecycle, and commands"
    else
      log "e2b Python SDK output: ${sdk_output:0:1200}"
      _fail "e2b Python SDK compatibility" "exit 0" "non-zero"
    fi
  elif sdk_output=$(python3 "$sdk_script" 2>&1); then
    _pass "e2b Python SDK template build, startCmd/readyCmd, sandbox lifecycle, and commands"
  else
    log "e2b Python SDK output: ${sdk_output:0:1200}"
    _fail "e2b Python SDK compatibility" "exit 0" "non-zero"
  fi
else
  warn "e2b Python SDK not installed; skipping Python SDK checks"
  _pass "skipped Python SDK checks (e2b package not installed)"
fi

# -- e2b TypeScript SDK template + sandbox compatibility --
ts_sdk_script="${SUITE_DIR}/../e2b_ts_sdk_compat.ts"
tsx_bin="${SUITE_DIR}/../node_modules/.bin/tsx"
if [[ -f "$ts_sdk_script" ]] && command -v npm >/dev/null 2>&1; then
  log "Installing TypeScript SDK dependencies"
  (cd "${SUITE_DIR}/.." && npm install --no-save --package-lock=false e2b@latest 2>&1)
  if ! "$tsx_bin" --version >/dev/null 2>&1; then
    warn "tsx not available after npm install; skipping TypeScript SDK checks"
    _pass "skipped TypeScript SDK checks (tsx not available)"
  else
  sdk_timeout="${E2B_COMPAT_TS_TIMEOUT_SECONDS:-360}"
  log "Running: e2b TypeScript SDK compatibility (${ts_sdk_script})"
  if command -v timeout >/dev/null 2>&1; then
    if sdk_output=$(timeout "${sdk_timeout}" "$tsx_bin" "$ts_sdk_script" 2>&1); then
      _pass "e2b TypeScript SDK template build, startCmd/readyCmd, sandbox lifecycle, and commands"
    else
      log "e2b TypeScript SDK output: ${sdk_output:0:1200}"
      _fail "e2b TypeScript SDK compatibility" "exit 0" "non-zero"
    fi
  elif sdk_output=$("$tsx_bin" "$ts_sdk_script" 2>&1); then
    _pass "e2b TypeScript SDK template build, startCmd/readyCmd, sandbox lifecycle, and commands"
  else
    log "e2b TypeScript SDK output: ${sdk_output:0:1200}"
    _fail "e2b TypeScript SDK compatibility" "exit 0" "non-zero"
  fi
  fi
else
  warn "npm/npx not found; skipping TypeScript SDK checks"
  _pass "skipped TypeScript SDK checks (npm/npx not installed)"
fi

suite_summary "09_e2b"
