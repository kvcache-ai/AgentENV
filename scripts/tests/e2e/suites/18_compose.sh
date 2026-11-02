#!/usr/bin/env bash
set -euo pipefail

# Use a prebuilt guest image from images/compose rather than building the
# bundled one, which needs internet access from the template builder.
if [[ -z "${AENV_COMPOSE_BASE_IMAGE:-}" ]]; then
  echo "AENV_COMPOSE_BASE_IMAGE not set; skipping Compose checks"
  exit 0
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export XDG_CONFIG_HOME="$work"
mkdir -p "$work/aenv"
printf 'url = "%s"\napi_key = "%s"\n' "$AENV_URL" "$AENV_API_KEY" >"$work/aenv/credentials"
chmod 600 "$work/aenv/credentials"
make -C "$repo_root" test-compose
