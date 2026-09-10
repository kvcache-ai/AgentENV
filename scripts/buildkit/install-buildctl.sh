#!/usr/bin/env bash
# Install the pinned Linux BuildKit client for CI and local integration tests.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
destination=${1:?usage: install-buildctl.sh <directory>}
version="$(<"$root/config/buildkit-version")"
case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) echo 'Unsupported BuildKit test architecture' >&2; exit 1 ;;
esac
asset="buildkit-${version}.linux-${arch}.tar.gz"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL --retry 5 "https://api.github.com/repos/moby/buildkit/releases/tags/$version" >"$work/release.json"
url=$(jq -er --arg asset "$asset" '.assets[] | select(.name == $asset) | .browser_download_url' "$work/release.json")
digest=$(jq -er --arg asset "$asset" '.assets[] | select(.name == $asset) | .digest' "$work/release.json")
[[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]
curl -fsSL --retry 5 "$url" -o "$work/client.tar.gz"
echo "${digest#sha256:}  $work/client.tar.gz" | sha256sum -c -
tar -xzf "$work/client.tar.gz" -C "$work" bin/buildctl
install -D -m 0755 "$work/bin/buildctl" "$destination/buildctl"
