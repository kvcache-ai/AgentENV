#!/usr/bin/env bash
# Install the aenv CLI from GitHub Releases.
#
# Supported platforms: linux/darwin × x86_64/aarch64 (arm64)
#
# Usage:
#   curl -fsSL https://github.com/kvcache-ai/AgentENV/releases/latest/download/install-cli.sh | bash
#
# Override install location (no sudo needed for user-local):
#   INSTALL_DIR=~/.local/bin bash install-cli.sh

set -euo pipefail

REPO="kvcache-ai/AgentENV"
INSTALL_DIR="${INSTALL_DIR:-}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
    linux|darwin) ;;
    *)
        echo "error: unsupported OS: $OS (supported: linux, darwin)" >&2
        exit 1
        ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)          ARCH_TAG="x86_64" ;;
    aarch64|arm64)   ARCH_TAG="aarch64" ;;
    *)
        echo "error: unsupported architecture: $ARCH (supported: x86_64, aarch64/arm64)" >&2
        exit 1
        ;;
esac

if [[ -z "$INSTALL_DIR" ]]; then
    if [[ -w /usr/local/bin ]]; then
        INSTALL_DIR=/usr/local/bin
    else
        INSTALL_DIR="${HOME}/.local/bin"
    fi
fi

ASSET="aenv-${OS}-${ARCH_TAG}"
RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
DEST="${INSTALL_DIR}/aenv"
TMP="$(mktemp)"
RELEASE_METADATA="$(mktemp)"
trap 'rm -f "$TMP" "$RELEASE_METADATA"' EXIT

missing_packages=()
command -v curl >/dev/null 2>&1 || missing_packages+=(curl)
command -v jq >/dev/null 2>&1 || missing_packages+=(jq)
if ! command -v sha256sum >/dev/null 2>&1 &&
   ! command -v shasum >/dev/null 2>&1; then
    missing_packages+=(coreutils)
fi

run_privileged() {
    if [[ $EUID -eq 0 ]]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        echo "error: sudo is required to install missing commands" >&2
        return 1
    fi
}

if ((${#missing_packages[@]} > 0)); then
    echo "Installing required commands: ${missing_packages[*]} ..."
    if [[ "$OS" == "darwin" ]]; then
        if ! command -v brew >/dev/null 2>&1; then
            echo "error: Homebrew is required to install: ${missing_packages[*]}" >&2
            exit 1
        fi
        brew install "${missing_packages[@]}"
    elif command -v apt-get >/dev/null 2>&1; then
        run_privileged apt-get update
        run_privileged apt-get install -y "${missing_packages[@]}"
    elif command -v dnf >/dev/null 2>&1; then
        run_privileged dnf install -y "${missing_packages[@]}"
    elif command -v yum >/dev/null 2>&1; then
        run_privileged yum install -y "${missing_packages[@]}"
    elif command -v pacman >/dev/null 2>&1; then
        run_privileged pacman -Sy --needed --noconfirm "${missing_packages[@]}"
    elif command -v apk >/dev/null 2>&1; then
        run_privileged apk add "${missing_packages[@]}"
    else
        echo "error: no supported package manager found to install: ${missing_packages[*]}" >&2
        exit 1
    fi
fi

for command in curl jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command is still unavailable after installation: ${command}" >&2
        exit 1
    fi
done

if command -v sha256sum >/dev/null 2>&1; then
    sha256_file() {
        sha256sum "$1" | awk '{print $1}'
    }
elif command -v shasum >/dev/null 2>&1; then
    sha256_file() {
        shasum -a 256 "$1" | awk '{print $1}'
    }
else
    echo "error: SHA256 command is still unavailable after installation (sha256sum or shasum)" >&2
    exit 1
fi

api_headers=(
    -H "Accept: application/vnd.github+json"
    -H "X-GitHub-Api-Version: 2022-11-28"
)

echo "Downloading aenv (${OS}/${ARCH_TAG}) ..."
curl -fsSL --retry 5 --retry-delay 10 --retry-max-time 60 \
    "${api_headers[@]}" "$RELEASE_API" -o "$RELEASE_METADATA"
asset_json="$(
    jq -cer --arg name "$ASSET" \
        '[.assets[] | select(.name == $name)] |
         if length == 1 then .[0] else error("release asset not found or not unique") end' \
        "$RELEASE_METADATA"
)" || {
    echo "error: release asset not found or not unique: ${ASSET}" >&2
    exit 1
}
url="$(jq -r '.browser_download_url // empty' <<< "$asset_json")"
digest="$(jq -r '.digest // empty' <<< "$asset_json")"
if [[ -z "$url" ]]; then
    echo "error: GitHub did not provide a download URL for ${ASSET}" >&2
    exit 1
fi
if [[ "$digest" != sha256:* ]]; then
    echo "error: GitHub did not provide a SHA256 digest for ${ASSET}" >&2
    exit 1
fi
curl -fsSL --retry 5 --retry-delay 10 --retry-max-time 60 "$url" -o "$TMP"

expected="${digest#sha256:}"
actual="$(sha256_file "$TMP")"
if [[ "$expected" != "$actual" ]]; then
    echo "error: SHA256 mismatch for ${ASSET}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    exit 1
fi

chmod 0755 "$TMP"

if [[ -w "$INSTALL_DIR" ]] || mkdir -p "$INSTALL_DIR"; then
    true
else
    sudo mkdir -p "$INSTALL_DIR"
fi
if [[ -w "$INSTALL_DIR" ]]; then
    mv "$TMP" "$DEST"
else
    sudo mv "$TMP" "$DEST"
fi

echo "Installed: ${DEST}"

if ! command -v aenv &>/dev/null; then
    echo ""
    echo "Note: ${INSTALL_DIR} is not on your PATH."
    echo "Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

echo "Run 'aenv --help' to get started."
