#!/usr/bin/env bash
set -euo pipefail

if [[ "${RUN_BUILDKIT_TESTS:-0}" != 1 ]]; then
    echo 'BuildKit suite requires RUN_BUILDKIT_TESTS=1 and BUILDCTL_BIN'
    exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export XDG_CONFIG_HOME="$work"
mkdir -p "$work/aenv"
printf 'url = "%s"\napi_key = "%s"\n' "$AENV_URL" "$AENV_API_KEY" >"$work/aenv/credentials"
chmod 600 "$work/aenv/credentials"
make -C "$repo_root" test-buildkit
