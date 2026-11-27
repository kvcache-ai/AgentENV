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

render_secret_literal() {
  local file="$1"
  local key="$2"
  local value="$3"
  local rendered_file

  rendered_file="$(mktemp "${TEMP_DIR}/secret-literal.XXXXXX")"
  if ! {
    printf '%s\n' "${key}"
    printf '%s\n' "${value}"
    cat "${file}"
  } | awk '
    NR == 1 { key = $0; next }
    NR == 2 { val = $0; next }
    {
      prefix = "      - " key "="
      if (index($0, prefix) == 1) {
        escaped = val
        gsub(/\047/, "\047\047", escaped)
        print "      - \047" key "=" escaped "\047"
        replaced = 1
        next
      }
      print
    }
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
if [[ -f "${TEMP_DIR}/k8s/overlays/postgres/kustomization.yaml" ]]; then
  sed_in_place "s#^namespace: agentenv-system#namespace: ${NAMESPACE}#" "${TEMP_DIR}/k8s/overlays/postgres/kustomization.yaml"
fi
sed_in_place "s#^namespace: agentenv-system#namespace: ${NAMESPACE}#" "${OVERLAY_PATH}/kustomization.yaml"
sed_in_place "s#  name: agentenv-system#  name: ${NAMESPACE}#" "${TEMP_DIR}/k8s/base/namespace.yaml"
for scheduler_cfg in \
  "${TEMP_DIR}/k8s/base/config/scheduler.json" \
  "${OVERLAY_PATH}/config/scheduler.json"; do
  if [[ -f "${scheduler_cfg}" ]]; then
    sed_in_place "s#\"namespace\": \"agentenv-system\"#\"namespace\": \"${NAMESPACE}\"#" "${scheduler_cfg}"
    sed_in_place "s#\"lease_namespace\": \"agentenv-system\"#\"lease_namespace\": \"${NAMESPACE}\"#" "${scheduler_cfg}"
  fi
done

read_existing_secret_key() {
  local secret_name="$1"
  local key="$2"
  local value=""

  if ! value="$("${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} -n "${NAMESPACE}" get secret "${secret_name}" \
    --ignore-not-found -o "go-template={{with index .data \"${key}\"}}{{. | base64decode}}{{end}}")"; then
    echo "failed to read ${key} from Secret ${NAMESPACE}/${secret_name}" >&2
    return 1
  fi
  printf '%s' "${value}"
}

generate_postgres_password() {
  od -An -N24 -tx1 /dev/urandom | tr -d '[:space:]'
}

desired_postgres_password() {
  if [[ "${AENV_POSTGRES_PASSWORD+x}" == "x" ]]; then
    if [[ -z "${AENV_POSTGRES_PASSWORD}" ]]; then
      echo "AENV_POSTGRES_PASSWORD must not be empty" >&2
      return 1
    fi
    printf '%s' "${AENV_POSTGRES_PASSWORD}"
  else
    generate_postgres_password
  fi
}

uri_encode() {
  printf '%s' "$1" | LC_ALL=C od -An -tx1 -v | tr -s '[:space:]' '\n' | awk '
    BEGIN {
      hex_digits = "0123456789ABCDEF"
      for (i = 0; i < 16; i++) {
        hv[substr(hex_digits, i + 1, 1)] = i
      }
    }
    NF == 0 { next }
    {
      hex = toupper($1)
      b = hv[substr(hex, 1, 1)] * 16 + hv[substr(hex, 2, 1)]
      # RFC 3986 unreserved: A-Z (65-90), a-z (97-122), 0-9 (48-57), - (45), . (46), _ (95), ~ (126)
      if ((b >= 48 && b <= 57) || (b >= 65 && b <= 90) || (b >= 97 && b <= 122) ||
          b == 45 || b == 46 || b == 95 || b == 126) {
        printf "%c", b
      } else {
        printf "%%%s", hex
      }
    }
  '
}

postgres_url_for_password() {
  printf 'postgresql://agentenv:%s@postgres:5432/agentenv' "$(uri_encode "$1")"
}

bootstrap_in_cluster_postgres_credentials() {
  local existing_password="" create_error pw_file url_file candidate_url

  if [[ "${DRY_RUN}" == "1" ]]; then
    POSTGRES_PASSWORD_VALUE="$(desired_postgres_password)" || return 1
    DATABASE_POSTGRES_URL_VALUE="$(postgres_url_for_password "${POSTGRES_PASSWORD_VALUE}")"
    return 0
  fi

  ensure_namespace || return 1
  if ! existing_password="$(read_existing_secret_key "agentenv-database" "POSTGRES_PASSWORD")"; then
    return 1
  fi

  if [[ -n "${existing_password}" ]]; then
    if [[ -n "${AENV_POSTGRES_PASSWORD:-}" && "${AENV_POSTGRES_PASSWORD}" != "${existing_password}" ]]; then
      echo "AENV_POSTGRES_PASSWORD differs from existing Secret ${NAMESPACE}/agentenv-database; refusing to overwrite credentials for an initialized PostgreSQL volume." >&2
      return 1
    fi
    POSTGRES_PASSWORD_VALUE="${existing_password}"
    DATABASE_POSTGRES_URL_VALUE="$(postgres_url_for_password "${POSTGRES_PASSWORD_VALUE}")"
    return 0
  fi

  POSTGRES_PASSWORD_VALUE="$(desired_postgres_password)" || return 1
  candidate_url="$(postgres_url_for_password "${POSTGRES_PASSWORD_VALUE}")"
  pw_file="${TEMP_DIR}/bootstrap-pg-password"
  url_file="${TEMP_DIR}/bootstrap-pg-url"
  create_error="${TEMP_DIR}/bootstrap-pg.err"
  printf '%s' "${POSTGRES_PASSWORD_VALUE}" >"${pw_file}"
  printf '%s' "${candidate_url}" >"${url_file}"
  chmod 0600 "${pw_file}" "${url_file}"

  # A concurrent apply can win this create. The persisted reread below is
  # authoritative whether this command succeeds or reports AlreadyExists.
  "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} -n "${NAMESPACE}" create secret generic agentenv-database \
    --from-file="POSTGRES_PASSWORD=${pw_file}" \
    --from-file="AENV_DATABASE_POSTGRES_URL=${url_file}" >/dev/null 2>"${create_error}" || true

  if ! POSTGRES_PASSWORD_VALUE="$(read_existing_secret_key "agentenv-database" "POSTGRES_PASSWORD")"; then
    return 1
  fi
  if [[ -z "${POSTGRES_PASSWORD_VALUE}" ]]; then
    cat "${create_error}" >&2
    echo "failed to bootstrap Secret ${NAMESPACE}/agentenv-database" >&2
    return 1
  fi
  DATABASE_POSTGRES_URL_VALUE="$(postgres_url_for_password "${POSTGRES_PASSWORD_VALUE}")"
}

resolve_external_postgres_url() {
  if [[ -n "${AENV_DATABASE_POSTGRES_URL:-}" ]]; then
    DATABASE_POSTGRES_URL_VALUE="${AENV_DATABASE_POSTGRES_URL}"
    return 0
  fi
  if [[ "${DRY_RUN}" == "1" ]]; then
    DATABASE_POSTGRES_URL_VALUE="postgresql://agentenv:dry-run@postgres.example.internal:5432/agentenv"
    return 0
  fi
  if ! DATABASE_POSTGRES_URL_VALUE="$(read_existing_secret_key "agentenv-database" "AENV_DATABASE_POSTGRES_URL")"; then
    return 1
  fi
  if [[ -z "${DATABASE_POSTGRES_URL_VALUE}" ]]; then
    echo "AENV_DATABASE_POSTGRES_URL is required for multi-node Kubernetes overlay '${OVERLAY_NAME}'." >&2
    echo "Set AENV_DATABASE_POSTGRES_URL to a shared external PostgreSQL URL, or use K8S_OVERLAY=postgres / K8S_OVERLAY=metadata for in-cluster PostgreSQL." >&2
    return 1
  fi
}

ensure_namespace() {
  if ! "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} create namespace "${NAMESPACE}" \
    --dry-run=client -o yaml | "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} apply -f - >/dev/null; then
    echo "failed to create or verify namespace ${NAMESPACE}" >&2
    return 1
  fi
}

generate_api_key() {
  printf 'e2b_%s' "$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"
}

bootstrap_api_key() {
  local create_error secret_file

  if ! API_KEY_VALUE="$(read_existing_secret_key "agentenv-auth" "AENV_API_KEY")"; then
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
  "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} -n "${NAMESPACE}" create secret generic agentenv-auth \
    --from-file="AENV_API_KEY=${secret_file}" >/dev/null 2>"${create_error}" || true

  if ! API_KEY_VALUE="$(read_existing_secret_key "agentenv-auth" "AENV_API_KEY")"; then
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
    if [[ ${#API_KEY_VALUE} -lt 32 || ${#API_KEY_VALUE} -gt 256 || ! "${API_KEY_VALUE}" =~ ^[A-Za-z0-9._~-]+$ ]]; then
      echo "AENV_API_KEY must contain between 32 and 256 URL-safe characters" >&2
      exit 1
    fi
  fi

  render_secret_literal "${TEMP_DIR}/k8s/base/kustomization.yaml" "AENV_API_KEY" "${API_KEY_VALUE}"

  POSTGRES_PASSWORD_VALUE=""
  DATABASE_POSTGRES_URL_VALUE=""
  case "${OVERLAY_NAME}" in
    local-dev)
      ;;
    postgres|metadata)
      if [[ "${MODE}" == "render" ]]; then
        POSTGRES_PASSWORD_VALUE="REDACTED"
        DATABASE_POSTGRES_URL_VALUE="postgresql://agentenv:REDACTED@postgres:5432/agentenv"
      else
        bootstrap_in_cluster_postgres_credentials || exit 1
      fi
      render_secret_literal "${TEMP_DIR}/k8s/overlays/postgres/kustomization.yaml" "POSTGRES_PASSWORD" "${POSTGRES_PASSWORD_VALUE}"
      render_secret_literal "${TEMP_DIR}/k8s/overlays/postgres/kustomization.yaml" "AENV_DATABASE_POSTGRES_URL" "${DATABASE_POSTGRES_URL_VALUE}"
      ;;
    *)
      if [[ "${MODE}" == "render" ]]; then
        DATABASE_POSTGRES_URL_VALUE="postgresql://agentenv:REDACTED@postgres.example.internal:5432/agentenv"
      else
        resolve_external_postgres_url || exit 1
      fi
      render_secret_literal "${TEMP_DIR}/k8s/base/kustomization.yaml" "AENV_DATABASE_POSTGRES_URL" "${DATABASE_POSTGRES_URL_VALUE}"
      ;;
  esac
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
      RESTART_TARGETS=(deployment/agentenv-gateway daemonset/agentenv-node)
      if [[ "${OVERLAY_NAME}" == "metadata" ]]; then
        RESTART_TARGETS+=(deployment/agentenv-metadata)
      fi
      if [[ "${OVERLAY_NAME}" == "ha" ]]; then
        RESTART_TARGETS+=(deployment/agentenv-scheduler)
      fi
      "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} -n "${NAMESPACE}" rollout restart \
        "${RESTART_TARGETS[@]}"
      echo "AgentENV API key stored in Secret ${NAMESPACE}/agentenv-auth." >&2
      echo "Read it with:" >&2
      printf '  %q' "${KUBECTL_BIN}" ${KUBECTL_TARGET_ARGS[@]+"${KUBECTL_TARGET_ARGS[@]}"} >&2
      printf " -n %q get secret agentenv-auth -o go-template='%s'\n" \
        "${NAMESPACE}" \
        '{{index .data "AENV_API_KEY" | base64decode}}{{"\n"}}' >&2
    fi
    ;;
  delete)
    "${KUBECTL_BIN}" delete --ignore-not-found -k "${OVERLAY_PATH}" "$@"
    ;;
esac
