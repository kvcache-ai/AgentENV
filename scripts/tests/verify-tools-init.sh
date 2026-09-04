#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
init_script="${repo_root}/tools-image/init"
pivot_init_script="${repo_root}/tools-image/pivot-init"

bash -n "$init_script"
bash -n "$pivot_init_script"

# Inspect executable source through the root switch, excluding comments that
# document paths which must not be traversed at this stage.
# Pattern matches literal shell source.
# shellcheck disable=SC2016
pre_pivot_script=$(sed -n '1,/^\$BB pivot_root /p' "$init_script")
pre_pivot_commands=$(sed '/^[[:space:]]*#/d' <<<"$pre_pivot_script")

# Absolute symlinks in the user image resolve against the tools root before
# pivot_root. Only prepare the real /run directory at this stage; pivot-init
# handles /var/run once absolute symlinks resolve inside the user root.
if grep -Fq '/mnt/user/var/run' <<<"$pre_pivot_commands"; then
  echo 'pre-pivot bootstrap must not traverse /mnt/user/var/run' >&2
  exit 1
fi
grep -Fq '/mnt/user/run' <<<"$pre_pivot_commands"

# Pattern matches literal shell source.
# shellcheck disable=SC2016
grep -Fq '$BB mkdir -p /var/run /tmp /var/log/agentenv/envd /run/sv/envd/log' \
  "$pivot_init_script"
