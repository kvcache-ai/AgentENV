#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
init_script="${repo_root}/tools-image/init"

bash -n "$init_script"

# Patterns match literal shell source.
# shellcheck disable=SC2016
pre_pivot_dirs=$(sed -n \
  '/^\$BB mkdir -p/,/^\$BB chmod 1777/p' \
  "$init_script")

# Ubuntu supplies /var/run as an absolute symlink to /run. Before pivot_root,
# traversing it escapes the mounted user root and produces a deterministic
# mkdir error. The real /run mount target is still required.
if grep -Fq '/mnt/user/var/run' <<<"$pre_pivot_dirs"; then
  echo 'pre-pivot bootstrap must not traverse /mnt/user/var/run' >&2
  exit 1
fi
grep -Fq '/mnt/user/run' <<<"$pre_pivot_dirs"
# Pattern matches literal shell source.
# shellcheck disable=SC2016
grep -Fq '$BB mkdir -p /var/run /tmp /var/log/agentenv/envd /run/sv/envd/log' \
  "${repo_root}/tools-image/pivot-init"

# A server built from this fork must download the drive that contains the
# script above, not the identically versioned upstream artifact.
grep -Fq 'version = "0.1.0-tonbo.1"' "${repo_root}/config/deps_manifest.toml"
grep -Fq 'url = "ghcr.io/tonbo-io/agentenv-tools:{version}"' \
  "${repo_root}/config/deps_manifest.toml"
