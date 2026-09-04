#!/usr/bin/env bash
# Runtime lifecycle helpers for e2e tests.

if [[ -z "${E2E_RUNTIME_SH_LOADED:-}" ]]; then
  E2E_RUNTIME_SH_LOADED=1

  E2E_RUNTIME_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  E2E_REPO_ROOT="$(cd "${E2E_RUNTIME_DIR}/../../../.." && pwd)"

  _E2E_API_KEY_FROM_USER=0
  if [[ "${AENV_API_KEY+x}" == "x" ]]; then
    if [[ -z "${AENV_API_KEY}" ]]; then
      echo "AENV_API_KEY must not be empty" >&2
      return 1
    fi
    _E2E_API_KEY_FROM_USER=1
  fi

  # shellcheck source=/dev/null
  source "${E2E_RUNTIME_DIR}/server.sh"

  : "${E2E_MODE:=single-node}"
  export AENV_API_KEY
  : "${E2E_COMPOSE_FILE:=deploy/docker-compose.yml}"
  : "${E2E_COMPOSE_OVERRIDE_FILE:=scripts/tests/e2e/docker-compose.e2e.yml}"
  : "${E2E_COMPOSE_START_TIMEOUT:=120}"
  : "${AENV_GATEWAY_PORT:=8000}"
  : "${AENV_NODE_A_PORT:=8001}"
  : "${AENV_NODE_B_PORT:=8002}"
  : "${E2E_K8S_NAMESPACE:=agentenv-system}"
  : "${E2E_K8S_APPLY_TARGET:=k8s-apply-dev}"
  : "${E2E_K8S_DELETE_TARGET:=k8s-delete-dev}"
  : "${E2E_K8S_LOAD_TARGET:=k8s-load-dev}"
  : "${E2E_K8S_GATEWAY_SERVICE:=agentenv-gateway}"
  : "${E2E_K8S_NODE_SELECTOR:=app.kubernetes.io/name=agentenv-node}"
  : "${E2E_K8S_START_TIMEOUT:=180}"
  : "${E2E_K8S_GATEWAY_LOCAL_PORT:=18080}"
  : "${E2E_K8S_NODE_LOCAL_PORT_BASE:=18081}"

  _K8S_PORT_FORWARD_PIDS=()
  _K8S_PORT_FORWARD_LOGS=()

  e2e_mode_is() {
    [[ "${E2E_MODE}" == "$1" ]]
  }

  e2e_mode_is_clustered() {
    e2e_mode_is "compose" || e2e_mode_is "k8s"
  }

  _compose_override_enabled() {
    [[ -n "${E2E_COMPOSE_OVERRIDE_FILE:-}" ]]
  }

  _compose_file_spec() {
    local base_file override_file

    base_file="$(_resolve_runtime_path "${E2E_COMPOSE_FILE}")"
    [[ -f "${base_file}" ]] || die "Compose file not found at ${base_file}"

    if _compose_override_enabled; then
      override_file="$(_resolve_runtime_path "${E2E_COMPOSE_OVERRIDE_FILE}")"
      [[ -f "${override_file}" ]] || die "Compose override file not found at ${override_file}"
      printf '%s -f %s\n' "${base_file}" "${override_file}"
      return 0
    fi

    printf '%s\n' "${base_file}"
  }

  _deploy_make_cmd() {
    if [[ "${_E2E_API_KEY_FROM_USER}" == "1" ]]; then
      make --no-print-directory -C "${E2E_REPO_ROOT}" "$@"
    else
      env -u AENV_API_KEY make --no-print-directory -C "${E2E_REPO_ROOT}" "$@"
    fi
  }

  _run_deploy_target() {
    local target="${1:?usage: _run_deploy_target <target> [config_path]}"
    local config="${2:-}"

    if [[ -n "${config}" ]]; then
      DEPLOY_COMPOSE_FILE="$(_compose_file_spec)" \
        CONFIG_PATH="${config}" \
        _deploy_make_cmd "${target}"
    else
      DEPLOY_COMPOSE_FILE="$(_compose_file_spec)" \
        _deploy_make_cmd "${target}"
    fi
  }

  _compose_cmd() {
    local base_file override_file
    local -a compose_args

    base_file="$(_resolve_runtime_path "${E2E_COMPOSE_FILE}")"
    compose_args=(-f "${base_file}")

    if _compose_override_enabled; then
      override_file="$(_resolve_runtime_path "${E2E_COMPOSE_OVERRIDE_FILE}")"
      compose_args+=(-f "${override_file}")
    fi

    docker compose "${compose_args[@]}" "$@"
  }

  dump_compose_runtime_diagnostics() {
    echo ""
    echo "========================================"
    log "Compose runtime diagnostics"
    echo "========================================"

    echo ""
    echo "----- docker compose ps -----"
    _compose_cmd ps 2>&1 || true

    echo ""
    echo "----- docker compose logs -----"
    _compose_cmd logs --no-color --timestamps --tail="${E2E_COMPOSE_LOG_TAIL:-1000}" 2>&1 || true
  }

  _print_k8s_logs_for_selector() {
    local label="$1"
    local selector="$2"

    echo ""
    echo "----- kubectl logs (${label}) -----"
    kubectl -n "${E2E_K8S_NAMESPACE}" logs \
      -l "${selector}" \
      --all-containers=true \
      --prefix=true \
      --tail="${E2E_K8S_LOG_TAIL:-1000}" 2>&1 || true
  }

  dump_k8s_runtime_diagnostics() {
    echo ""
    echo "========================================"
    log "Kubernetes runtime diagnostics"
    echo "========================================"

    echo ""
    echo "----- kubectl get pods -----"
    kubectl -n "${E2E_K8S_NAMESPACE}" get pods -o wide 2>&1 || true

    echo ""
    echo "----- kubectl get deploy,ds,svc -----"
    kubectl -n "${E2E_K8S_NAMESPACE}" get deploy,ds,svc -o wide 2>&1 || true

    echo ""
    echo "----- kubectl events -----"
    kubectl -n "${E2E_K8S_NAMESPACE}" get events --sort-by=.lastTimestamp 2>&1 || true

    _print_k8s_logs_for_selector "gateway" "app.kubernetes.io/name=agentenv-gateway"
    _print_k8s_logs_for_selector "scheduler" "app.kubernetes.io/name=agentenv-scheduler"
    _print_k8s_logs_for_selector "agentenv-node" "${E2E_K8S_NODE_SELECTOR}"
  }

  dump_test_runtime_diagnostics() {
    if e2e_mode_is "compose"; then
      dump_compose_runtime_diagnostics
    elif e2e_mode_is "k8s"; then
      dump_k8s_runtime_diagnostics
    fi
    return 0
  }

  _run_k8s_target() {
    local target="${1:?usage: _run_k8s_target <target>}"
    [[ -n "${target}" && "${target}" != "none" ]] || return 0
    _deploy_make_cmd "${target}"
  }

  _read_k8s_api_key() {
    kubectl -n "${E2E_K8S_NAMESPACE}" get secret agentenv-auth \
      -o 'go-template={{index .data "AENV_API_KEY" | base64decode}}'
  }

  _resolve_runtime_path() {
    local path="$1"
    if [[ "$path" = /* ]]; then
      printf '%s\n' "$path"
    else
      printf '%s\n' "${E2E_REPO_ROOT}/${path#./}"
    fi
  }

  configure_runtime_endpoints() {
    if e2e_mode_is "compose"; then
      export AENV_URL="http://127.0.0.1:${AENV_GATEWAY_PORT}"
      export AENV_PROXY_URL="${AENV_URL}"
      export AENV_NODE_A_URL="http://127.0.0.1:${AENV_NODE_A_PORT}"
      export AENV_NODE_B_URL="http://127.0.0.1:${AENV_NODE_B_PORT}"
      export AENV_NODE_A_LABEL="agentenv-a"
      export AENV_NODE_B_LABEL="agentenv-b"
      export AENV_NODE_URLS="${AENV_NODE_A_URL} ${AENV_NODE_B_URL}"
      export AENV_NODE_URL_LABEL_MAP="${AENV_NODE_A_URL}=agentenv-a;${AENV_NODE_B_URL}=agentenv-b"
    elif e2e_mode_is "k8s"; then
      export AENV_URL="http://127.0.0.1:${E2E_K8S_GATEWAY_LOCAL_PORT}"
      export AENV_PROXY_URL="${AENV_URL}"
      export AENV_NODE_A_URL="${AENV_NODE_A_URL:-}"
      export AENV_NODE_B_URL="${AENV_NODE_B_URL:-}"
      export AENV_NODE_A_LABEL="${AENV_NODE_A_LABEL:-}"
      export AENV_NODE_B_LABEL="${AENV_NODE_B_LABEL:-}"
      export AENV_NODE_URLS="${AENV_NODE_URLS:-}"
      export AENV_NODE_URL_LABEL_MAP="${AENV_NODE_URL_LABEL_MAP:-}"
    else
      export AENV_URL="http://127.0.0.1:${AENV_PORT}"
      export AENV_PROXY_URL="${AENV_URL}/proxy"
      export AENV_NODE_A_URL=""
      export AENV_NODE_B_URL=""
      export AENV_NODE_A_LABEL=""
      export AENV_NODE_B_LABEL=""
      export AENV_NODE_URLS=""
      export AENV_NODE_URL_LABEL_MAP=""
    fi
  }

  _wait_for_health_url() {
    local label="$1"
    local url="$2"
    local timeout="$3"
    log "Waiting for ${label} at ${url}/health (timeout ${timeout}s) ..."
    for ((i = 1; i <= timeout; i++)); do
      if curl -sf "${url}/health" >/dev/null 2>&1; then
        log "${label} is ready."
        return 0
      fi
      sleep 1
    done
    return 1
  }

  _runtime_node_count() {
    local count=0
    local node_url

    while IFS= read -r node_url; do
      [[ -z "${node_url}" ]] && continue
      count=$((count + 1))
    done < <(printf '%s\n' "${AENV_NODE_URLS:-}" | tr ' ' '\n')

    printf '%s\n' "${count}"
  }

  _wait_for_scheduler_ready_nodes() {
    local timeout="${1:-180}"
    local expected_count="${2:-1}"
    local nodes_body=""
    local response
    local status
    local ready_count

    log "Waiting for scheduler to observe ${expected_count} ready node(s) via ${AENV_URL}/nodes (timeout ${timeout}s) ..."
    for ((i = 1; i <= timeout; i++)); do
      response=$(curl -s \
        -H "X-API-Key: ${AENV_API_KEY}" \
        -w $'\n%{http_code}' \
        "${AENV_URL}/nodes" 2>/dev/null || true)
      status="${response##*$'\n'}"
      nodes_body="${response%$'\n'*}"
      if [[ "${status}" == "200" ]]; then
        ready_count=$(printf '%s' "${nodes_body}" | jq '[.[] | select(.status == "ready")] | length' 2>/dev/null || printf '0')
        if [[ "${ready_count}" -ge "${expected_count}" ]]; then
          log "scheduler has ${ready_count} ready node(s)."
          return 0
        fi
      fi
      sleep 1
    done

    warn "Timed out waiting for scheduler ready nodes; last /nodes response:"
    printf '%s\n' "${nodes_body}" >&2
    return 1
  }

  start_compose_runtime() {
    local config="${1:-}"
    require_cmd docker
    require_cmd make
    configure_runtime_endpoints

    if [[ -n "$config" ]]; then
      config="$(_resolve_runtime_path "$config")"
      [[ -f "$config" ]] || die "Compose runtime config not found at ${config}"
    fi

    log "Resetting compose deployment via make deploy-down ..."
    _run_deploy_target deploy-down

    log "Starting compose deployment via make deploy-up-no-build from $(_compose_file_spec) ..."
    if [[ -n "$config" ]]; then
      log "Using compose runtime config: ${config}"
      _run_deploy_target deploy-up-no-build "${config}"
    else
      _run_deploy_target deploy-up-no-build
    fi
  }

  _kill_local_port() {
    local port="${1:?usage: _kill_local_port <port>}"
    if command -v fuser >/dev/null 2>&1; then
      fuser -k "${port}/tcp" 2>/dev/null || true
      sleep 0.2
    fi
  }

  _k8s_wait_for_rollout() {
    local resource="$1"
    local timeout="${2:-$E2E_K8S_START_TIMEOUT}s"
    kubectl -n "${E2E_K8S_NAMESPACE}" rollout status "${resource}" --timeout="${timeout}"
  }

  _start_k8s_port_forward() {
    local resource="$1"
    local local_port="$2"
    local remote_port="$3"
    local log_name="$4"
    local log_file
    local pid

    _kill_local_port "${local_port}"
    log_file="$(mktemp)"
    kubectl -n "${E2E_K8S_NAMESPACE}" port-forward "${resource}" "${local_port}:${remote_port}" >"${log_file}" 2>&1 &
    pid=$!
    _K8S_PORT_FORWARD_PIDS+=("${pid}")
    _K8S_PORT_FORWARD_LOGS+=("${log_file}")

    for ((i = 0; i < 20; i++)); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        warn "kubectl port-forward for ${log_name} exited early"
        [[ -f "${log_file}" ]] && cat "${log_file}" >&2
        return 1
      fi
      if bash -c "exec 3<>/dev/tcp/127.0.0.1/${local_port}" >/dev/null 2>&1; then
        return 0
      fi
      sleep 0.5
    done

    warn "kubectl port-forward for ${log_name} did not become ready on localhost:${local_port}"
    [[ -f "${log_file}" ]] && cat "${log_file}" >&2
    return 1
  }

  _export_k8s_node_endpoints() {
    local -a urls labels mappings
    local index=0
    local pod
    local local_port

    AENV_NODE_A_URL=""
    AENV_NODE_B_URL=""
    AENV_NODE_A_LABEL=""
    AENV_NODE_B_LABEL=""

    while IFS= read -r pod; do
      [[ -z "${pod}" ]] && continue
      local_port=$((E2E_K8S_NODE_LOCAL_PORT_BASE + index))
      _start_k8s_port_forward "pod/${pod}" "${local_port}" 8000 "${pod}" ||
        die "Failed to port-forward ${pod}"

      urls+=("http://127.0.0.1:${local_port}")
      labels+=("${pod}")
      mappings+=("http://127.0.0.1:${local_port}=${pod}")

      if [[ "${index}" -eq 0 ]]; then
        AENV_NODE_A_URL="http://127.0.0.1:${local_port}"
        AENV_NODE_A_LABEL="${pod}"
      elif [[ "${index}" -eq 1 ]]; then
        AENV_NODE_B_URL="http://127.0.0.1:${local_port}"
        AENV_NODE_B_LABEL="${pod}"
      fi
      index=$((index + 1))
    done < <(kubectl -n "${E2E_K8S_NAMESPACE}" get pods \
      -l "${E2E_K8S_NODE_SELECTOR}" \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)

    [[ "${#urls[@]}" -gt 0 ]] || die "No running agentenv-node pods found in namespace ${E2E_K8S_NAMESPACE}"

    export AENV_NODE_A_URL AENV_NODE_B_URL
    export AENV_NODE_A_LABEL AENV_NODE_B_LABEL
    export AENV_NODE_URLS="${urls[*]}"
    export AENV_NODE_URL_LABEL_MAP
    AENV_NODE_URL_LABEL_MAP="$(IFS=';'; printf '%s' "${mappings[*]}")"
    export AENV_NODE_URL_LABEL_MAP
  }

  start_k8s_runtime() {
    require_cmd kubectl
    require_cmd make
    configure_runtime_endpoints

    stop_k8s_runtime

    if [[ -n "${E2E_K8S_LOAD_TARGET}" && "${E2E_K8S_LOAD_TARGET}" != "none" ]]; then
      log "Loading k8s dev images via make ${E2E_K8S_LOAD_TARGET} ..."
      _run_k8s_target "${E2E_K8S_LOAD_TARGET}"
    fi

    log "Applying k8s deployment via make ${E2E_K8S_APPLY_TARGET} ..."
    _run_k8s_target "${E2E_K8S_APPLY_TARGET}"
  }

  wait_for_compose_runtime() {
    configure_runtime_endpoints
    local timeout="${1:-$E2E_COMPOSE_START_TIMEOUT}"
    local expected_nodes

    _wait_for_health_url "gateway" "${AENV_URL}" "${timeout}" ||
      die "Gateway failed to become ready within ${timeout}s"
    _wait_for_health_url "agentenv-a" "${AENV_NODE_A_URL}" "${timeout}" ||
      die "agentenv-a failed to become ready within ${timeout}s"
    _wait_for_health_url "agentenv-b" "${AENV_NODE_B_URL}" "${timeout}" ||
      die "agentenv-b failed to become ready within ${timeout}s"

    if [[ "${_E2E_API_KEY_FROM_USER}" != "1" ]]; then
      AENV_API_KEY="$(_compose_cmd exec -T agentenv-a cat /workspace/env/secrets/api-key)" ||
        die "Failed to read the Compose deployment API key"
    fi
    [[ "${AENV_API_KEY}" =~ ^[A-Za-z0-9._~-]{32,256}$ ]] ||
      die "Compose deployment returned an invalid API key"
    export AENV_API_KEY

    expected_nodes="$(_runtime_node_count)"
    [[ "${expected_nodes}" -gt 0 ]] || expected_nodes=1
    _wait_for_scheduler_ready_nodes "${timeout}" "${expected_nodes}" ||
      die "Scheduler failed to observe ${expected_nodes} ready compose node(s) within ${timeout}s"
  }

  wait_for_k8s_runtime() {
    local timeout="${1:-$E2E_K8S_START_TIMEOUT}"
    local expected_nodes

    log "Waiting for gateway deployment rollout in namespace ${E2E_K8S_NAMESPACE} ..."
    _k8s_wait_for_rollout "deploy/agentenv-gateway" "${timeout}" ||
      die "agentenv-gateway rollout failed in namespace ${E2E_K8S_NAMESPACE}"
    log "Waiting for scheduler deployment rollout in namespace ${E2E_K8S_NAMESPACE} ..."
    _k8s_wait_for_rollout "deploy/agentenv-scheduler" "${timeout}" ||
      die "agentenv-scheduler rollout failed in namespace ${E2E_K8S_NAMESPACE}"
    log "Waiting for node daemonset rollout in namespace ${E2E_K8S_NAMESPACE} ..."
    _k8s_wait_for_rollout "ds/agentenv-node" "${timeout}" ||
      die "agentenv-node rollout failed in namespace ${E2E_K8S_NAMESPACE}"

    _start_k8s_port_forward "svc/${E2E_K8S_GATEWAY_SERVICE}" "${E2E_K8S_GATEWAY_LOCAL_PORT}" 8080 "gateway" ||
      die "Failed to port-forward gateway service"
    _export_k8s_node_endpoints
    configure_runtime_endpoints

    _wait_for_health_url "gateway" "${AENV_URL}" "${timeout}" ||
      die "Gateway failed to become ready within ${timeout}s"

    local node_url label
    while IFS= read -r node_url; do
      [[ -z "${node_url}" ]] && continue
      label="$(node_label_for_url "${node_url}")"
      _wait_for_health_url "${label}" "${node_url}" "${timeout}" ||
        die "${label} failed to become ready within ${timeout}s"
    done < <(printf '%s\n' "${AENV_NODE_URLS}" | tr ' ' '\n')

    if [[ "${_E2E_API_KEY_FROM_USER}" != "1" ]]; then
      AENV_API_KEY="$(_read_k8s_api_key)" ||
        die "Failed to read the Kubernetes deployment API key"
    fi
    [[ "${AENV_API_KEY}" =~ ^[A-Za-z0-9._~-]{32,256}$ ]] ||
      die "Kubernetes deployment returned an invalid API key"
    export AENV_API_KEY

    expected_nodes="$(_runtime_node_count)"
    [[ "${expected_nodes}" -gt 0 ]] || expected_nodes=1
    _wait_for_scheduler_ready_nodes "${timeout}" "${expected_nodes}" ||
      die "Scheduler failed to observe ${expected_nodes} ready k8s node(s) within ${timeout}s"
  }

  stop_compose_runtime() {
    log "Stopping compose deployment via make deploy-down ..."
    _run_deploy_target deploy-down
  }

  stop_k8s_runtime() {
    local pid
    local log_file

    for pid in "${_K8S_PORT_FORWARD_PIDS[@]}"; do
      kill "${pid}" 2>/dev/null || true
    done
    _K8S_PORT_FORWARD_PIDS=()

    for log_file in "${_K8S_PORT_FORWARD_LOGS[@]}"; do
      rm -f "${log_file}"
    done
    _K8S_PORT_FORWARD_LOGS=()

    AENV_NODE_A_URL=""
    AENV_NODE_B_URL=""
    AENV_NODE_A_LABEL=""
    AENV_NODE_B_LABEL=""
    AENV_NODE_URLS=""
    AENV_NODE_URL_LABEL_MAP=""
    configure_runtime_endpoints

    if [[ -n "${E2E_K8S_DELETE_TARGET}" && "${E2E_K8S_DELETE_TARGET}" != "none" ]]; then
      log "Stopping k8s deployment via make ${E2E_K8S_DELETE_TARGET} ..."
      _run_k8s_target "${E2E_K8S_DELETE_TARGET}"
    fi
  }

  start_test_runtime() {
    local binary="${1:-}"
    local config="${2:-}"

    configure_runtime_endpoints
    if e2e_mode_is "compose"; then
      start_compose_runtime "${config}"
    elif e2e_mode_is "k8s"; then
      start_k8s_runtime
    else
      start_server "${binary:?usage: start_test_runtime <binary> [config_path]}" "${config}"
    fi
  }

  wait_for_test_runtime() {
    if e2e_mode_is "compose"; then
      wait_for_compose_runtime
    elif e2e_mode_is "k8s"; then
      wait_for_k8s_runtime
    else
      wait_for_server
    fi
  }

  stop_test_runtime() {
    if e2e_mode_is "compose"; then
      stop_compose_runtime
    elif e2e_mode_is "k8s"; then
      stop_k8s_runtime
    else
      stop_server
    fi
  }

  configure_runtime_endpoints
fi
