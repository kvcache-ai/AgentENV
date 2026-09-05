#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export INSTALL_TEST_ROOT="$work"
mkdir -p "$work/bin" "$work/assets/bin"
printf '#!/bin/sh\necho aenv-test\n' >"$work/assets/aenv"
printf '#!/bin/sh\necho buildctl-v0.33.0\n' >"$work/assets/bin/buildctl"
printf 'not installed\n' >"$work/assets/bin/buildkitd"
tar -czf "$work/assets/buildkit.tar.gz" -C "$work/assets" bin
cli_digest="sha256:$(sha256sum "$work/assets/aenv" | cut -d' ' -f1)"
buildkit_digest="sha256:$(sha256sum "$work/assets/buildkit.tar.gz" | cut -d' ' -f1)"
jq -n --arg cli "$cli_digest" --arg buildkit "$buildkit_digest" '{assets: [
  ["linux", "darwin"][] as $os | ["x86_64", "aarch64"][] as $arch |
  {name: ("aenv-" + $os + "-" + $arch), digest: $cli, browser_download_url: "https://test.invalid/aenv"}
]}' >"$work/release.json"
jq -n --arg digest "$buildkit_digest" '{assets: [
  ["linux", "darwin"][] as $os | ["amd64", "arm64"][] as $arch |
  {name: ("buildkit-v0.33.0." + $os + "-" + $arch + ".tar.gz"), digest: $digest, browser_download_url: "https://test.invalid/buildkit.tar.gz"}
]}' >"$work/buildkit.json"

cat >"$work/bin/stub" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "${0##*/}" in
  uname) case "$1" in -s) echo "$INSTALL_TEST_OS";; -m) echo "$INSTALL_TEST_ARCH";; esac;;
  getent) echo 'aenv:x:1234:';;
  id) echo 1234;;
  systemctl) exit 1;;
  sudo)
    [[ "${1:-}" == -v ]] && exit 0
    args=()
    for arg in "$@"; do args+=("${arg//\/usr\/local\/bin/$INSTALL_TEST_ROOT/full-install}"); done
    exec "${args[@]}";;
  curl)
    url="" dest=""
    while (($#)); do
      case "$1" in
        -o) dest="$2"; shift 2;;
        https://*) url="$1"; shift;;
        *) shift;;
      esac
    done
    case "$url" in
      */AgentENV/releases/latest) source="$INSTALL_TEST_ROOT/release.json";;
      */buildkit/releases/tags/v0.33.0) source="$INSTALL_TEST_ROOT/buildkit.json";;
      https://test.invalid/*) source="$INSTALL_TEST_ROOT/assets/${url##*/}";;
      *) echo "Unexpected download: $url" >&2; exit 1;;
    esac
    cp "$source" "$dest";;
esac
STUB
chmod +x "$work/bin/stub"
for command in curl uname sudo getent id systemctl; do ln -s stub "$work/bin/$command"; done
export PATH="$work/bin:$PATH"

for INSTALL_TEST_OS in Linux Darwin; do
  for INSTALL_TEST_ARCH in x86_64 arm64; do
    export INSTALL_TEST_OS INSTALL_TEST_ARCH
    dest="$work/install $INSTALL_TEST_OS $INSTALL_TEST_ARCH"
    INSTALL_DIR="$dest" bash "$repo_root/scripts/install-cli.sh"
    [[ $("$dest/aenv") == aenv-test ]]
    [[ $("$dest/buildctl") == buildctl-v0.33.0 ]]
    [[ ! -e "$dest/buildkitd" ]]
  done
done

# Only exercise the CLI stage of the full installer. The missing server asset
# stops it before any server files or services can be changed.
export INSTALL_TEST_OS=Linux INSTALL_TEST_ARCH=x86_64
if AENV_HOME_PATH="$work/data" SKIP_SETUP=1 bash "$repo_root/scripts/install.sh" >"$work/full.log" 2>&1; then
  echo 'Expected the deliberately absent server asset to stop installation' >&2
  exit 1
fi
grep -q 'aenv-server-linux-x86_64.tar.gz' "$work/full.log"
[[ $("$work/full-install/aenv") == aenv-test ]]
[[ $("$work/full-install/buildctl") == buildctl-v0.33.0 ]]
[[ ! -e "$work/full-install/server" ]]

printf 'corrupt\n' >>"$work/assets/buildkit.tar.gz"
for installer in install-cli.sh install.sh; do
  dest="$work/corrupt-$installer"
  if INSTALL_DIR="$dest" AENV_HOME_PATH="$work/data" SKIP_SETUP=1 \
      bash "$repo_root/scripts/$installer" >"$work/corrupt.log" 2>&1; then
    echo 'Expected a checksum failure' >&2
    exit 1
  fi
  grep -q 'SHA256 mismatch for buildkit' "$work/corrupt.log"
  [[ ! -e "$dest/aenv" && ! -e "$dest/buildctl" ]]
done
echo 'Installer BuildKit checks passed'
