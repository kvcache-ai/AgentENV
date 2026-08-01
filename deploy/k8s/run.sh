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

cp -R "${SCRIPT_DIR}" "${TEMP_DIR}/k8s"
cp "${REPO_ROOT}/config/default.toml" "${TEMP_DIR}/k8s/base/config/agentenv.toml"
if [[ "${MODE}" != "delete" ]]; then
  API_KEY_VALUE=""
  if [[ "${AENV_API_KEY+x}" == "x" ]]; then
    API_KEY_VALUE="${AENV_API_KEY}"
  elif [[ "${MODE}" == "apply" ]]; then
    if ! encoded_key="$("${KUBECTL_BIN}" -n "${NAMESPACE}" get secret agentenv-auth \
      --ignore-not-found -o jsonpath='{.data.AENV_API_KEY}')"; then
      echo "failed to read existing Secret ${NAMESPACE}/agentenv-auth" >&2
      exit 1
    fi
    if [[ -n "${encoded_key}" ]]; then
      if ! API_KEY_VALUE="$(printf '%s' "${encoded_key}" | base64 -d)"; then
        echo "failed to decode the existing agentenv-auth Secret" >&2
        exit 1
      fi
    fi
  fi

  if [[ -z "${API_KEY_VALUE}" ]]; then
    API_KEY_VALUE="e2b_$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"
  fi
  if [[ ! "${API_KEY_VALUE}" =~ ^[A-Za-z0-9._~-]{32,}$ ]]; then
    echo "AENV_API_KEY must contain at least 32 URL-safe characters" >&2
    exit 1
  fi

  sed_in_place \
    "s#- AENV_API_KEY=.*#- AENV_API_KEY=${API_KEY_VALUE}#" \
    "${TEMP_DIR}/k8s/base/kustomization.yaml"
fi


if [[ "${SANDBOX_PROXY_DOMAINS+x}" == "x" ]]; then
  ESCAPED_SANDBOX_PROXY_DOMAINS="${SANDBOX_PROXY_DOMAINS//\\/\\\\}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//&/\\&}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//#/\\#}"
  sed_in_place "s#- SANDBOX_PROXY_DOMAINS=.*#- SANDBOX_PROXY_DOMAINS=${ESCAPED_SANDBOX_PROXY_DOMAINS}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
fi

OVERLAY_PATH="${TEMP_DIR}/k8s/overlays/${OVERLAY_NAME}"
if [[ ! -d "${OVERLAY_PATH}" ]]; then
  echo "unknown overlay: ${OVERLAY_NAME}" >&2
  exit 1
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
