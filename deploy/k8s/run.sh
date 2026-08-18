#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <render|apply|delete> [kubectl args...]" >&2
  exit 1
fi

MODE="$1"
shift
case "${MODE}" in
  render|apply|delete) ;;
  *)
    echo "unsupported mode: ${MODE}" >&2
    exit 1
    ;;
esac

KUBECTL_BIN="${KUBECTL:-kubectl}"
OVERLAY_NAME="${K8S_OVERLAY:-default}"
NAMESPACE="${K8S_NAMESPACE:-agentenv-system}"
if [[ ${#NAMESPACE} -gt 63 || ! "${NAMESPACE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "K8S_NAMESPACE must be a valid Kubernetes namespace name" >&2
  exit 1
fi

KUBECTL_TARGET_ARGS=()
DRY_RUN=0
ARGS=("$@")
for ((i = 0; i < ${#ARGS[@]}; i++)); do
  arg="${ARGS[i]}"
  case "${arg}" in
    --context|--kubeconfig)
      if ((i + 1 >= ${#ARGS[@]})); then
        echo "${arg} requires a value" >&2
        exit 1
      fi
      KUBECTL_TARGET_ARGS+=("${arg}" "${ARGS[i + 1]}")
      i=$((i + 1))
      ;;
    --context=*|--kubeconfig=*) KUBECTL_TARGET_ARGS+=("${arg}") ;;
    --dry-run)
      if ((i + 1 < ${#ARGS[@]})) && [[ "${ARGS[i + 1]}" == "none" ]]; then
        DRY_RUN=0
      else
        DRY_RUN=1
      fi
      ;;
    --dry-run=client|--dry-run=server) DRY_RUN=1 ;;
    --dry-run=none) DRY_RUN=0 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

sed_in_place() {
  local expression="$1"
  local file="$2"
  if sed --version >/dev/null 2>&1; then
    sed -i "${expression}" "${file}"
  else
    sed -i '' "${expression}" "${file}"
  fi
}

render_api_key() {
  local file="$1"
  local rendered_file

  rendered_file="$(mktemp "${TEMP_DIR}/api-key.XXXXXX")"
  if ! {
    printf '%s\n' "${API_KEY_VALUE}"
    cat "${file}"
  } | awk '
    NR == 1 { api_key = $0; next }
    /^      - AENV_API_KEY=/ { print "      - AENV_API_KEY=" api_key; replaced = 1; next }
    { print }
    END { if (!replaced) exit 1 }
  ' >"${rendered_file}"; then
    return 1
  fi
  mv "${rendered_file}" "${file}"
}

cp -R "${SCRIPT_DIR}" "${TEMP_DIR}/k8s"
cp "${REPO_ROOT}/config/default.toml" "${TEMP_DIR}/k8s/base/config/agentenv.toml"
OVERLAY_PATH="${TEMP_DIR}/k8s/overlays/${OVERLAY_NAME}"
if [[ ! -d "${OVERLAY_PATH}" ]]; then
  echo "unknown overlay: ${OVERLAY_NAME}" >&2
  exit 1
fi
sed_in_place "s#^namespace: agentenv-system#namespace: ${NAMESPACE}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
sed_in_place "s#^namespace: agentenv-system#namespace: ${NAMESPACE}#" "${OVERLAY_PATH}/kustomization.yaml"
sed_in_place "s#  name: agentenv-system#  name: ${NAMESPACE}#" "${TEMP_DIR}/k8s/base/namespace.yaml"
sed_in_place "s#\"namespace\": \"agentenv-system\"#\"namespace\": \"${NAMESPACE}\"#" "${TEMP_DIR}/k8s/base/config/scheduler.json"

read_existing_api_key() {
  local value=""

  if ! value="$("${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" -n "${NAMESPACE}" get secret agentenv-auth \
    --ignore-not-found -o 'go-template={{index .data "AENV_API_KEY" | base64decode}}')"; then
    echo "failed to read AENV_API_KEY from Secret ${NAMESPACE}/agentenv-auth" >&2
    return 1
  fi
  printf '%s' "${value}"
}

ensure_namespace() {
  if ! "${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" create namespace "${NAMESPACE}" \
    --dry-run=client -o yaml | "${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" apply -f - >/dev/null; then
    echo "failed to create or verify namespace ${NAMESPACE}" >&2
    return 1
  fi
}

generate_api_key() {
  printf 'e2b_%s' "$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"
}

bootstrap_api_key() {
  local create_error secret_file

  if ! API_KEY_VALUE="$(read_existing_api_key)"; then
    return 1
  fi
  if [[ -n "${API_KEY_VALUE}" ]]; then
    return 0
  fi

  secret_file="${TEMP_DIR}/bootstrap-api-key"
  create_error="${TEMP_DIR}/bootstrap-api-key.err"
  generate_api_key >"${secret_file}"
  chmod 0600 "${secret_file}"

  # A concurrent apply can win this create. The persisted reread below is
  # authoritative whether this command succeeds or reports AlreadyExists.
  "${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" -n "${NAMESPACE}" create secret generic agentenv-auth \
    --from-file="AENV_API_KEY=${secret_file}" >/dev/null 2>"${create_error}" || true

  if ! API_KEY_VALUE="$(read_existing_api_key)"; then
    return 1
  fi
  if [[ -z "${API_KEY_VALUE}" ]]; then
    cat "${create_error}" >&2
    echo "failed to bootstrap Secret ${NAMESPACE}/agentenv-auth" >&2
    return 1
  fi
}

if [[ "${MODE}" != "delete" ]]; then
  restore_xtrace=0
  if [[ $- == *x* ]]; then
    restore_xtrace=1
    set +x
  fi
  API_KEY_VALUE=""
  if [[ "${MODE}" == "render" ]]; then
    API_KEY_VALUE="REDACTED"
  else
    if [[ "${AENV_API_KEY+x}" == "x" ]]; then
      if [[ -z "${AENV_API_KEY}" ]]; then
        echo "AENV_API_KEY must not be empty" >&2
        exit 1
      fi
      API_KEY_VALUE="${AENV_API_KEY}"
    elif [[ "${DRY_RUN}" == "1" ]]; then
      API_KEY_VALUE="$(generate_api_key)"
    else
      ensure_namespace || exit 1
      bootstrap_api_key || exit 1
    fi
    if [[ ! "${API_KEY_VALUE}" =~ ^[A-Za-z0-9._~-]{32,256}$ ]]; then
      echo "AENV_API_KEY must contain between 32 and 256 URL-safe characters" >&2
      exit 1
    fi
  fi

  render_api_key "${TEMP_DIR}/k8s/base/kustomization.yaml"
  [[ "${restore_xtrace}" == "0" ]] || set -x
fi

if [[ "${SANDBOX_PROXY_DOMAINS+x}" == "x" ]]; then
  ESCAPED_SANDBOX_PROXY_DOMAINS="${SANDBOX_PROXY_DOMAINS//\\/\\\\}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//&/\\&}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//#/\\#}"
  sed_in_place "s#- SANDBOX_PROXY_DOMAINS=.*#- SANDBOX_PROXY_DOMAINS=${ESCAPED_SANDBOX_PROXY_DOMAINS}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
fi

if [[ "${OVERLAY_NAME}" == "local-dev" ]]; then
  REPO_ENV_PATH="${AENV_LOCAL_REPO_ENV_PATH:-${REPO_ROOT}/env}"
  if [[ ! -d "${REPO_ENV_PATH}" ]]; then
    echo "local-dev overlay requires a readable env directory at ${REPO_ENV_PATH}" >&2
    exit 1
  fi

  ESCAPED_REPO_ENV_PATH="${REPO_ENV_PATH//\\/\\\\}"
  ESCAPED_REPO_ENV_PATH="${ESCAPED_REPO_ENV_PATH//&/\\&}"
  sed_in_place "s#path: \"\"#path: \"${ESCAPED_REPO_ENV_PATH}\"#" "${OVERLAY_PATH}/kustomization.yaml"

  if grep -q 'path: ""' "${OVERLAY_PATH}/kustomization.yaml"; then
    echo "failed to render local-dev repo env hostPath; path is still empty" >&2
    exit 1
  fi
fi

case "${MODE}" in
  render)
    "${KUBECTL_BIN}" kustomize "${OVERLAY_PATH}" "$@"
    ;;
  apply)
    "${KUBECTL_BIN}" apply -k "${OVERLAY_PATH}" "$@"
    if [[ "${DRY_RUN}" == "0" ]]; then
      "${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" -n "${NAMESPACE}" rollout restart \
        deployment/agentenv-gateway daemonset/agentenv-node
      echo "AgentENV API key stored in Secret ${NAMESPACE}/agentenv-auth." >&2
      echo "Read it with:" >&2
      printf '  %q' "${KUBECTL_BIN}" "${KUBECTL_TARGET_ARGS[@]}" >&2
      printf " -n %q get secret agentenv-auth -o go-template='%s'\n" \
        "${NAMESPACE}" \
        '{{index .data "AENV_API_KEY" | base64decode}}{{"\n"}}' >&2
    fi
    ;;
  delete)
    "${KUBECTL_BIN}" delete --ignore-not-found -k "${OVERLAY_PATH}" "$@"
    ;;
esac
