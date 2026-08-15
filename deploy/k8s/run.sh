#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <render|apply|delete> [kubectl args...]" >&2
  exit 1
fi

MODE="$1"
shift
KUBECTL_BIN="${KUBECTL:-kubectl}"
OVERLAY_NAME="${K8S_OVERLAY:-default}"
NAMESPACE="${K8S_NAMESPACE:-agentenv-system}"
if [[ ${#NAMESPACE} -gt 63 || ! "${NAMESPACE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "K8S_NAMESPACE must be a valid Kubernetes namespace name" >&2
  exit 1
fi

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

namespace_name=""
if [[ "${MODE}" == "apply" ]]; then
  if ! namespace_name="$("${KUBECTL_BIN}" get namespace "${NAMESPACE}" --ignore-not-found -o name)"; then
    echo "failed to check namespace ${NAMESPACE}" >&2
    exit 1
  fi
fi

read_existing_api_key() {
  local encoded_value=""

  if [[ -z "${namespace_name}" ]]; then
    return 0
  fi
  if ! encoded_value="$("${KUBECTL_BIN}" -n "${NAMESPACE}" get secret agentenv-auth \
    --ignore-not-found -o 'go-template={{index .data "AENV_API_KEY"}}')"; then
    echo "failed to read AENV_API_KEY from Secret ${NAMESPACE}/agentenv-auth" >&2
    return 1
  fi
  if [[ -n "${encoded_value}" ]]; then
    printf '%s' "${encoded_value}" | base64 -d
  fi
}

if [[ "${MODE}" != "delete" ]]; then
  restore_xtrace=0
  if [[ $- == *x* ]]; then
    restore_xtrace=1
    set +x
  fi
  API_KEY_VALUE=""
  if [[ "${AENV_API_KEY+x}" == "x" ]]; then
    API_KEY_VALUE="${AENV_API_KEY}"
  elif ! API_KEY_VALUE="$(read_existing_api_key)"; then
    exit 1
  fi

  if [[ -z "${API_KEY_VALUE}" ]]; then
    API_KEY_VALUE="e2b_$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"
  fi
  if [[ ! "${API_KEY_VALUE}" =~ ^[A-Za-z0-9._~-]{32,4096}$ ]]; then
    echo "AENV_API_KEY must contain between 32 and 4096 URL-safe characters" >&2
    exit 1
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
    echo "AgentENV API key stored in Secret ${NAMESPACE}/agentenv-auth." >&2
    echo "Read it with:" >&2
    echo "  ${KUBECTL_BIN} -n ${NAMESPACE} get secret agentenv-auth -o go-template='{{index .data \"AENV_API_KEY\" | base64decode}}{{\"\\n\"}}'" >&2
    ;;
  delete)
    "${KUBECTL_BIN}" delete --ignore-not-found -k "${OVERLAY_PATH}" "$@"
    ;;
  *)
    echo "unsupported mode: ${MODE}" >&2
    exit 1
    ;;
esac
