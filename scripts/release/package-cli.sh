#!/usr/bin/env bash
# Package aenv, its Compose planner and the matching BuildKit client for a GitHub release.
set -euo pipefail

if [[ $# != 5 ]]; then
    echo "usage: $0 <aenv-binary> <compose-planner-binary> <output-tar.gz> <release-version> <buildkit-platform>" >&2
    exit 2
fi
aenv_binary=$1
planner_binary=$2
output=$3
release_version=$4
platform=$5
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version="$(<"$root/config/buildkit-version")"
test -x "$aenv_binary"
test -x "$planner_binary"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

bash "$root/scripts/buildkit/install-buildctl.sh" "$work/client" "$platform"
mkdir -p "$work/package" "$(dirname "$output")"
install -m 0755 "$aenv_binary" "$work/package/aenv"
install -m 0755 "$planner_binary" "$work/package/aenv-compose-plan"
install -m 0755 "$work/client/buildctl" "$work/package/aenv-buildctl"
jq -n --arg aenv "$release_version" --arg buildkit "$version" --arg platform "$platform" \
    '{aenvVersion: $aenv, buildkitVersion: $buildkit, platform: $platform}' >"$work/package/manifest.json"
tar -czf "$output" -C "$work/package" aenv aenv-buildctl aenv-compose-plan manifest.json
