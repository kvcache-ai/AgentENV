#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
installer="${repo_root}/scripts/install.sh"

bash -n "$installer"
bash -n "${repo_root}/scripts/docker-setup.sh"
bash -n "${repo_root}/deploy/k8s/run.sh"
bash -n "${repo_root}/scripts/run-with-capabilities.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
env_file="${tmp_dir}/aenv.env"
unit_file="${tmp_dir}/aenv.service"
: > "$env_file"

awk '
  /^[[:space:]]*sudo tee "\$SERVICE_FILE".*<<EOF$/ { capture = 1; next }
  capture && /^EOF$/ { exit }
  capture { print }
' "$installer" |
  sed \
    -e "s|\${SERVICE_USER}|aenv|g" \
    -e "s|\${SERVICE_GROUP}|aenv|g" \
    -e "s|\${ENV_FILE}|${env_file}|g" \
    -e "s|\${INSTALL_DIR}/server|/bin/true|g" > "$unit_file"

for directive in \
  'User=aenv' \
  'Group=aenv' \
  'SupplementaryGroups=kvm' \
  "EnvironmentFile=${env_file}" \
  'RuntimeDirectory=aenv' \
  'AmbientCapabilities=CAP_NET_ADMIN CAP_SYS_ADMIN' \
  'CapabilityBoundingSet=CAP_NET_ADMIN CAP_SYS_ADMIN' \
  'NoNewPrivileges=true'; do
  grep -Fqx "$directive" "$unit_file"
done

grep -Fqx 'LimitMEMLOCK=infinity' "$unit_file"

systemd-analyze verify "$unit_file"

grep -Fq 'AENV_API_KEY=${API_KEY_VALUE}' "$installer"
grep -Fq 'install -o root -g "$SERVICE_GROUP" -m 0640' "$installer"
grep -Fq 'AUTH_ENV_FILE="${AENV_AUTH_ENV_FILE:-/etc/aenv/auth.env}"' "${repo_root}/scripts/docker-setup.sh"
