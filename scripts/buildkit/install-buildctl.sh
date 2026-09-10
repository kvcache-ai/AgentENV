#!/usr/bin/env bash
# Install the pinned BuildKit client for release packaging and local tests.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
destination=${1:?usage: install-buildctl.sh <directory> [linux-amd64|linux-arm64|darwin-amd64|darwin-arm64]}
platform=${2:-}
version="$(<"$root/config/buildkit-version")"
if [[ -z "$platform" ]]; then
    case "$(uname -m)" in
        x86_64) arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *) echo 'Unsupported BuildKit architecture' >&2; exit 1 ;;
    esac
    platform="$(uname -s | tr '[:upper:]' '[:lower:]')-$arch"
fi
case "$platform" in
    linux-amd64|linux-arm64|darwin-amd64|darwin-arm64) ;;
    *) echo "Unsupported BuildKit platform: $platform" >&2; exit 1 ;;
esac
asset="buildkit-${version}.${platform}.tar.gz"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL --retry 5 "https://api.github.com/repos/moby/buildkit/releases/tags/$version" -o "$work/release.json"
asset_json=$(jq -cer --arg asset "$asset" \
    '[.assets[] | select(.name == $asset)] |
     if length == 1 then .[0] else error("BuildKit asset not found or not unique") end' "$work/release.json")
url=$(jq -er '.browser_download_url' <<<"$asset_json")
digest=$(jq -er '.digest' <<<"$asset_json")
[[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]
curl -fsSL --retry 5 "$url" -o "$work/client.tar.gz"
if command -v sha256sum >/dev/null; then
    actual=$(sha256sum "$work/client.tar.gz" | awk '{print $1}')
else
    actual=$(shasum -a 256 "$work/client.tar.gz" | awk '{print $1}')
fi
if [[ "${digest#sha256:}" != "$actual" ]]; then
    echo "error: SHA256 mismatch for $asset" >&2
    exit 1
fi
tar -xzf "$work/client.tar.gz" -C "$work" bin/buildctl
test -s "$work/bin/buildctl"
mkdir -p "$destination"
install -m 0755 "$work/bin/buildctl" "$destination/buildctl"
