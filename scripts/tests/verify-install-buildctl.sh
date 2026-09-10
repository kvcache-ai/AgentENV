#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export INSTALL_TEST_ROOT="$work"
INSTALL_TEST_MV="$(command -v mv)"
export INSTALL_TEST_MV
version="$(<"$repo_root/config/buildkit-version")"
export INSTALL_TEST_BUILDKIT_VERSION="$version"
mkdir -p "$work/bin" "$work/assets" "$work/upstream/bin"
printf '#!/bin/sh\necho aenv-test\n' >"$work/assets/aenv"
chmod +x "$work/assets/aenv"
printf 'not bundled\n' >"$work/upstream/bin/buildkitd"
for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  printf '#!/bin/sh\necho buildctl-%s-%s\n' "$version" "$platform" >"$work/upstream/bin/buildctl"
  tar -czf "$work/assets/buildkit-${version}.${platform}.tar.gz" -C "$work/upstream" bin
done
for archive in "$work/assets"/buildkit-*.tar.gz; do
  digest="sha256:$(sha256sum "$archive" | cut -d' ' -f1)"
  jq -n --arg name "${archive##*/}" --arg digest "$digest" \
    '{name: $name, digest: $digest, browser_download_url: ("https://test.invalid/" + $name)}'
done | jq -s '{assets: .}' >"$work/buildkit.json"

cat >"$work/bin/stub" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "${0##*/}" in
  uname) case "$1" in -s) echo "$INSTALL_TEST_OS";; -m) echo "$INSTALL_TEST_ARCH";; esac;;
  getent) echo 'aenv:x:1234:';;
  id) echo 1234;;
  systemctl) exit 1;;
  mv)
    if [[ "${INSTALL_TEST_FAIL_CLI:-0}" == 1 && "${@: -1}" == */aenv ]]; then
      echo 'injected aenv rename failure' >&2
      exit 1
    fi
    exec "$INSTALL_TEST_MV" "$@";;
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
      */buildkit/releases/tags/"$INSTALL_TEST_BUILDKIT_VERSION")
        [[ "${INSTALL_TEST_PHASE:-}" == package ]] || { echo 'Installer must not query BuildKit releases' >&2; exit 1; }
        source="$INSTALL_TEST_ROOT/buildkit.json";;
      https://test.invalid/buildkit-*)
        [[ "${INSTALL_TEST_PHASE:-}" == package ]] || { echo 'Installer must not download BuildKit separately' >&2; exit 1; }
        source="$INSTALL_TEST_ROOT/assets/${url##*/}";;
      https://test.invalid/*) source="$INSTALL_TEST_ROOT/assets/${url##*/}";;
      *) echo "Unexpected download: $url" >&2; exit 1;;
    esac
    cp "$source" "$dest";;
esac
STUB
chmod +x "$work/bin/stub"
for command in curl uname sudo getent id systemctl mv; do ln -s stub "$work/bin/$command"; done
export PATH="$work/bin:$PATH"

# Run the release packager for every target, including targets unlike the host.
for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  os=${platform%-*}
  arch=x86_64
  [[ "$platform" == *-arm64 ]] && arch=aarch64
  archive="$work/assets/aenv-${os}-${arch}.tar.gz"
  INSTALL_TEST_PHASE=package bash "$repo_root/scripts/release/package-cli.sh" \
    "$work/assets/aenv" "$archive" 1.2.3-rc.1 "$platform"
  [[ $(tar -tzf "$archive" | sort) == $'aenv\naenv-buildctl\nmanifest.json' ]]
  mkdir -p "$work/unpacked"
  tar -xzf "$archive" -C "$work/unpacked"
  [[ $("$work/unpacked/aenv") == aenv-test ]]
  [[ $("$work/unpacked/aenv-buildctl") == "buildctl-${version}-${platform}" ]]
  jq -e --arg version "$version" --arg platform "$platform" \
    '. == {aenvVersion: "1.2.3-rc.1", buildkitVersion: $version, platform: $platform}' \
    "$work/unpacked/manifest.json" >/dev/null

  # CI and source builds can omit the platform and use the host's client.
  host_os=Linux
  [[ "$os" == darwin ]] && host_os=Darwin
  host_arch=x86_64
  [[ "$arch" == aarch64 ]] && host_arch=arm64
  INSTALL_TEST_PHASE=package INSTALL_TEST_OS="$host_os" INSTALL_TEST_ARCH="$host_arch" \
    bash "$repo_root/scripts/buildkit/install-buildctl.sh" "$work/host-client"
  [[ $("$work/host-client/buildctl") == "buildctl-${version}-${platform}" ]]
done

# A bad upstream checksum must fail before replacing a release archive.
printf 'corrupt\n' >>"$work/assets/buildkit-${version}.linux-amd64.tar.gz"
printf 'existing archive\n' >"$work/existing.tar.gz"
if INSTALL_TEST_PHASE=package bash "$repo_root/scripts/release/package-cli.sh" \
    "$work/assets/aenv" "$work/existing.tar.gz" 1.2.3 linux-amd64 >"$work/package-failure.log" 2>&1; then
  echo 'Expected upstream checksum failure' >&2
  exit 1
fi
grep -q 'SHA256 mismatch for buildkit' "$work/package-failure.log"
[[ $(cat "$work/existing.tar.gz") == 'existing archive' ]]

write_release_metadata() {
  for archive in "$work/assets"/aenv-*.tar.gz; do
    digest="sha256:$(sha256sum "$archive" | cut -d' ' -f1)"
    jq -n --arg name "${archive##*/}" --arg digest "$digest" \
      '{name: $name, digest: $digest, browser_download_url: ("https://test.invalid/" + $name)}'
  done | jq -s '{assets: .}' >"$work/release.json"
}
write_release_metadata

for INSTALL_TEST_OS in Linux Darwin; do
  for INSTALL_TEST_ARCH in x86_64 arm64; do
    export INSTALL_TEST_OS INSTALL_TEST_ARCH
    dest="$work/install $INSTALL_TEST_OS $INSTALL_TEST_ARCH"
    mkdir -p "$dest"
    printf 'system buildctl\n' >"$dest/buildctl"
    INSTALL_DIR="$dest" bash "$repo_root/scripts/install-cli.sh"
    [[ $("$dest/aenv") == aenv-test ]]
    arch=amd64
    [[ "$INSTALL_TEST_ARCH" == arm64 ]] && arch=arm64
    os=$(tr '[:upper:]' '[:lower:]' <<<"$INSTALL_TEST_OS")
    [[ $("$dest/aenv-buildctl") == "buildctl-${version}-${os}-${arch}" ]]
    [[ $(cat "$dest/buildctl") == 'system buildctl' ]]
    [[ ! -e "$dest/buildkitd" ]]
  done
done

# Only exercise the CLI stage of the full installer. The missing server asset
# stops it before any server files or services can be changed.
export INSTALL_TEST_OS=Linux INSTALL_TEST_ARCH=x86_64
mkdir -p "$work/full-install"
printf 'system buildctl\n' >"$work/full-install/buildctl"
if AENV_HOME_PATH="$work/data" SKIP_SETUP=1 bash "$repo_root/scripts/install.sh" >"$work/full.log" 2>&1; then
  echo 'Expected the deliberately absent server asset to stop installation' >&2
  exit 1
fi
grep -q 'aenv-server-linux-x86_64.tar.gz' "$work/full.log"
[[ $("$work/full-install/aenv") == aenv-test ]]
[[ $("$work/full-install/aenv-buildctl") == "buildctl-${version}-linux-amd64" ]]
[[ $(cat "$work/full-install/buildctl") == 'system buildctl' ]]
[[ ! -e "$work/full-install/server" ]]

for installer in install-cli.sh install.sh; do
  dest="$work/failure-$installer"
  if [[ "$installer" == install.sh ]]; then dest="$work/full-install"; fi
  mkdir -p "$dest"
  printf 'existing aenv\n' >"$dest/aenv"
  printf 'system buildctl\n' >"$dest/buildctl"
  if INSTALL_TEST_FAIL_CLI=1 INSTALL_DIR="$dest" AENV_HOME_PATH="$work/data" SKIP_SETUP=1 \
      bash "$repo_root/scripts/$installer" >"$work/failure.log" 2>&1; then
    echo 'Expected the injected second rename failure' >&2
    exit 1
  fi
  grep -q 'injected aenv rename failure' "$work/failure.log"
  [[ $(cat "$dest/aenv") == 'existing aenv' ]]
  [[ $(cat "$dest/buildctl") == 'system buildctl' ]]
done

# Valid checksums do not make an incomplete bundle installable.
cp "$work/assets/aenv-linux-x86_64.tar.gz" "$work/complete.tar.gz"
tar -czf "$work/assets/aenv-linux-x86_64.tar.gz" -C "$work/assets" aenv
write_release_metadata
for installer in install-cli.sh install.sh; do
  dest="$work/incomplete-$installer"
  if [[ "$installer" == install.sh ]]; then dest="$work/full-install"; fi
  mkdir -p "$dest"
  printf 'existing aenv\n' >"$dest/aenv"
  printf 'existing private client\n' >"$dest/aenv-buildctl"
  if INSTALL_DIR="$dest" AENV_HOME_PATH="$work/data" SKIP_SETUP=1 \
      bash "$repo_root/scripts/$installer" >"$work/incomplete.log" 2>&1; then
    echo 'Expected incomplete bundle failure' >&2
    exit 1
  fi
  grep -q 'aenv-buildctl' "$work/incomplete.log"
  [[ $(cat "$dest/aenv") == 'existing aenv' ]]
  [[ $(cat "$dest/aenv-buildctl") == 'existing private client' ]]
done
cp "$work/complete.tar.gz" "$work/assets/aenv-linux-x86_64.tar.gz"
write_release_metadata
printf 'corrupt\n' >>"$work/assets/aenv-linux-x86_64.tar.gz"
for installer in install-cli.sh install.sh; do
  dest="$work/corrupt-$installer"
  if [[ "$installer" == install.sh ]]; then dest="$work/full-install"; fi
  mkdir -p "$dest"
  printf 'existing aenv\n' >"$dest/aenv"
  printf 'existing buildctl\n' >"$dest/buildctl"
  if INSTALL_DIR="$dest" AENV_HOME_PATH="$work/data" SKIP_SETUP=1 \
      bash "$repo_root/scripts/$installer" >"$work/corrupt.log" 2>&1; then
    echo 'Expected a checksum failure' >&2
    exit 1
  fi
  grep -q 'SHA256 mismatch for aenv-linux-x86_64.tar.gz' "$work/corrupt.log"
  [[ $(cat "$dest/aenv") == 'existing aenv' ]]
  [[ $(cat "$dest/buildctl") == 'existing buildctl' ]]
done
echo 'CLI bundle packaging and installer checks passed'
