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

install_completion_files() {
    local marker='# managed by aenv completion installer'
    local prefix home user_mode=0 quoted_binary
    local bash_path zsh_path fish_path
    prefix="$(dirname "$INSTALL_DIR")"
    home="${HOME:-}"
    canonical_dir() {
        local path="$1" parent name
        [[ "$path" == /* ]] || path="$PWD/$path"
        if [[ -d "$path" ]]; then
            (cd "$path" && pwd -P)
            return
        fi
        parent="${path%/*}"
        name="${path##*/}"
        [[ "$parent" == "$path" ]] && parent="."
        parent="$(cd "$parent" 2>/dev/null && pwd -P)" || return 1
        printf '%s/%s\n' "$parent" "$name"
    }
    prefix="$(canonical_dir "$prefix")" || { echo "warning: could not resolve completion prefix; skipping" >&2; return 0; }
    home_real=""
    if [[ -n "$home" ]]; then
        home_real="$(canonical_dir "$home")" || home_real=""
    fi
    if [[ -n "$home_real" && ( "$prefix" == "$home_real" || "$prefix" == "$home_real"/* ) ]]; then
        user_mode=1
    fi
    if ((user_mode)); then
        [[ -n "$home" ]] || { echo "warning: HOME is unset; skipping completion setup" >&2; return 0; }
        # Shells honor the XDG base directories; write where they actually look.
        if [[ "${XDG_DATA_HOME:-}" == /* ]]; then
            data_home="$XDG_DATA_HOME"
        else
            data_home="$home/.local/share"
        fi
        if [[ "${XDG_CONFIG_HOME:-}" == /* ]]; then
            config_home="$XDG_CONFIG_HOME"
        else
            config_home="$home/.config"
        fi
        bash_path="$data_home/bash-completion/completions/aenv"
        zsh_path="$data_home/zsh/site-functions/_aenv"
        fish_path="$config_home/fish/completions/aenv.fish"
    else
        bash_path="$prefix/share/bash-completion/completions/aenv"
        zsh_path="$prefix/share/zsh/site-functions/_aenv"
        fish_path="$prefix/share/fish/vendor_completions.d/aenv.fish"
    fi
    # Canonicalize first, then validate: $PWD can introduce unsafe characters
    # when INSTALL_DIR was relative, and the canonical path is what the
    # loaders embed.
    dest_dir="${DEST%/*}"
    [[ "$dest_dir" == "$DEST" ]] && dest_dir="."
    dest_dir="$(canonical_dir "$dest_dir")" || {
        echo "warning: could not canonicalize completion binary path; skipping" >&2
        return 0
    }
    DEST="$dest_dir/${DEST##*/}"
    case "$DEST" in
        *"'"*|*"\\"*|*$'\n'*|*$'\r'*)
            echo "warning: completion binary path contains unsupported characters; skipping" >&2
            return 0
            ;;
    esac
    if [[ ! -x "$DEST" ]]; then
        echo "warning: completion binary is not executable: ${DEST}; skipping" >&2
        return 0
    fi
    quoted_binary="'$DEST'"

    put_loader() (
        local path="$1" body="$2" dir tmp lock_dir owner staged_owner stale acquired runner=()
        dir="${path%/*}"
        # ${runner[@]} expansion of an empty array breaks under bash 3.2 + set -u.
        runv() {
            if ((${#runner[@]} > 0)); then
                "${runner[@]}" "$@"
            else
                "$@"
            fi
        }
        if ! (mkdir -p "$dir" 2>/dev/null && [[ -w "$dir" ]]); then
            runner=(run_privileged)
        fi
        runv mkdir -p "$dir" || { echo "warning: could not create ${dir}" >&2; return 0; }
        lock_dir="$dir/.aenv-completion.lock"
        lock_is_stale() {
            local check_dir="$1" pid mtime now
            pid="$(runv cat "$check_dir/pid" 2>/dev/null || true)"
            if [[ "$pid" =~ ^[0-9]+$ ]]; then
                command -v ps >/dev/null 2>&1 && ! ps -p "$pid" >/dev/null 2>&1
                return
            fi
            mtime="$(runv stat -c %Y "$check_dir" 2>/dev/null || runv stat -f %m "$check_dir" 2>/dev/null || true)"
            now="$(date +%s)"
            [[ "$mtime" =~ ^[0-9]+$ ]] && ((now - mtime > 300))
        }
        # Lock creation and cleanup must use the same privilege as the writes;
        # otherwise a system-prefix install reports every directory as busy.
        acquired=0
        if runv mkdir "$lock_dir" 2>/dev/null; then
            acquired=1
        elif runv test -d "$lock_dir" &&
             owner="$(runv cat "$lock_dir/pid" 2>/dev/null || true)" &&
             lock_is_stale "$lock_dir"; then
            # Atomically quarantine the observed lock before validating it;
            # a new owner at the original path cannot be removed by us.
            stale="$lock_dir.stale.$$.${RANDOM}"
            if runv mv "$lock_dir" "$stale" 2>/dev/null; then
                staged_owner="$(runv cat "$stale/pid" 2>/dev/null || true)"
                if [[ "$staged_owner" == "$owner" ]] && lock_is_stale "$stale"; then
                    runv rm -rf "$stale" 2>/dev/null || true
                    runv mkdir "$lock_dir" 2>/dev/null && acquired=1
                else
                    runv mv "$stale" "$lock_dir" 2>/dev/null || true
                fi
            fi
        fi
        if ((acquired == 0)); then
            echo "warning: completion directory is busy; skipping ${path}" >&2
            return 0
        fi
        trap 'runv rm -rf "$lock_dir" 2>/dev/null || true' EXIT
        printf '%s' "$$" | runv tee "$lock_dir/pid" >/dev/null 2>&1 || true
        if runv test -L "$path"; then
            echo "warning: refusing to replace symlink ${path}" >&2
            return 0
        fi
        if runv test -e "$path" && ! runv grep -Fqx "$marker" "$path" 2>/dev/null; then
            echo "warning: leaving unmanaged completion file ${path} untouched" >&2
            return 0
        fi
        tmp="$(runv mktemp "$dir/.aenv-completion.XXXXXX")" || {
            echo "warning: could not stage ${path}" >&2
            return 0
        }
        if [[ "$path" == "$zsh_path" ]]; then
            if ! printf '%s\n%s\n' "$body" "$marker" | runv tee "$tmp" >/dev/null; then
                runv rm -f "$tmp" || true
                echo "warning: could not stage ${path}" >&2
                return 0
            fi
        else
            if ! printf '%s\n%s\n' "$marker" "$body" | runv tee "$tmp" >/dev/null; then
                runv rm -f "$tmp" || true
                echo "warning: could not stage ${path}" >&2
                return 0
            fi
        fi
        # The lock protects cooperating installers. A non-cooperating external
        # writer cannot be serialized by a portable shell script; preserve the
        # documented guarantee for installer processes rather than claiming
        # protection against arbitrary writers.
        if runv test -e "$path" && ! runv grep -Fqx "$marker" "$path" 2>/dev/null; then
            runv rm -f "$tmp" || true
            echo "warning: leaving newly-created unmanaged completion file ${path} untouched" >&2
            return 0
        fi
        if ! runv chmod 0644 "$tmp" || ! runv mv -f "$tmp" "$path"; then
            runv rm -f "$tmp" || true
            echo "warning: could not install ${path}" >&2
        fi
    )

    put_loader "$bash_path" "if [[ -x $quoted_binary ]]; then source <($quoted_binary completion bash); fi"
    zsh_body="#compdef aenv
if [[ -x $quoted_binary ]]; then eval \"\$($quoted_binary completion zsh)\"; fi"
    put_loader "$zsh_path" "$zsh_body"
    put_loader "$fish_path" "if test -x $quoted_binary; $quoted_binary completion fish | source; end"
    if ((user_mode)); then
        echo "If Zsh does not find the completion, add this before compinit:"
        printf '  fpath=(%q/zsh/site-functions $fpath)\n' "$data_home"
        echo "  autoload -Uz compinit"
        echo "  compinit"
    fi
}

# Test seam: source with AENV_SOURCE_ONLY=1 to load install_completion_files
# without downloading or installing anything.
if [[ -n "${AENV_SOURCE_ONLY:-}" ]]; then
    return 0 2>/dev/null || exit 0
fi

missing_packages=()
command -v curl >/dev/null 2>&1 || missing_packages+=(curl)
command -v jq >/dev/null 2>&1 || missing_packages+=(jq)
if ! command -v sha256sum >/dev/null 2>&1 &&
   ! command -v shasum >/dev/null 2>&1; then
    missing_packages+=(coreutils)
fi

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

install_completion_files

if ! command -v aenv &>/dev/null; then
    echo ""
    echo "Note: ${INSTALL_DIR} is not on your PATH."
    echo "Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

echo "Run 'aenv --help' to get started."
