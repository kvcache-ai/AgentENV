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

# BEGIN aenv_completion_install
# (keep in sync across scripts/shell-completion.sh, scripts/install-cli.sh,
# scripts/install.sh — verified by scripts/check-completion-sync.sh)

# Write a single completion loader file. Non-fatal on I/O errors.
#   $1 destination path
#   $2 file mode (e.g. 0644)
#   $3 loader content (single line; the loaders are one-liners by design)
_aenv_cc_put() {
    local path="$1" mode="$2" content="$3"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    if ! printf '%s\n' "$content" > "$path" 2>/dev/null; then
        printf 'warn: aenv completion: could not write %s\n' "$path" >&2
        return 0
    fi
    chmod "$mode" "$path" 2>/dev/null || true
}

# Return 0 (exit status) if $1's aenv marker blocks — if any — are well-formed:
# strictly alternating start/end pairs with no nesting, reordering, or
# unterminated start at EOF. Returns 1 otherwise. Used to gate both install
# idempotency and removal so a corrupted/partial block is never silently
# truncated and never auto-repaired at the cost of unrelated rc content.
_aenv_cc_rc_well_formed() {
    awk '
        BEGIN { in_block = 0 }
        /^# >>> aenv completion >>>$/ { if (in_block) exit 1; in_block = 1; next }
        /^# <<< aenv completion <<<$/ { if (!in_block) exit 1; in_block = 0; next }
        END { if (in_block) exit 1 }
    ' "$1" 2>/dev/null
}

# Append the regenerating zsh rc-snippet, idempotently. A complete, well-formed
# block already present => no-op. A partial/corrupted block => warn and leave it
# for the user (auto-repair could delete unrelated rc lines). No block => append.
#   $1 rc file path
_aenv_cc_put_zsh_rc() {
    local rc="$1"
    local dir="${rc%/*}"
    if [[ -f "$rc" ]] && grep -q '^# >>> aenv completion >>>$' "$rc" 2>/dev/null; then
        if _aenv_cc_rc_well_formed "$rc"; then
            return 0 # idempotent: a complete managed block already exists
        fi
        printf 'warn: aenv completion: malformed marker block already in %s; leaving it untouched (remove it manually to regenerate)\n' "$rc" >&2
        return 0
    fi
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    # Start the block on its own line only when the rc file is non-empty and
    # does not already end with a newline; this avoids leaving a stray blank
    # line behind after uninstall.
    local leader="" last_byte
    if [[ -s "$rc" ]]; then
        last_byte=$(tail -c 1 "$rc" 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n') || last_byte=""
        [[ "$last_byte" == "0a" ]] || leader=$'\n'
    fi
    if ! {
        printf '%s' "$leader"
        printf '# >>> aenv completion >>>\n'
        printf 'autoload -Uz compinit && compinit\n'
        # shellcheck disable=SC2016 # $(...) is literal text for zsh to eval, not bash
        printf 'eval "$(aenv completion zsh)"\n'
        printf '# <<< aenv completion <<<\n'
    } >> "$rc" 2>/dev/null; then
        printf 'warn: aenv completion: could not append to %s\n' "$rc" >&2
    fi
}

# Generate the static zsh completion into a site-functions dir. $2 is the
# just-installed aenv binary (preferred over whatever is on PATH, which may be
# stale or absent). Generation goes to a temp file in the destination dir and
# is atomically renamed on success, so a failure never truncates an existing
# valid completion file.
#   $1 destination _aenv path
#   $2 aenv binary to invoke (default: aenv from PATH)
_aenv_cc_put_zsh_static() {
    local path="$1" aenv_bin="${2:-aenv}"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    local gen=()
    if [[ -x "$aenv_bin" ]]; then
        gen=("$aenv_bin" completion zsh)
    elif command -v aenv >/dev/null 2>&1; then
        gen=(aenv completion zsh)
    else
        printf 'warn: aenv completion: aenv not found (%s not executable and aenv not on PATH); skipping static zsh file %s\n' "$aenv_bin" "$path" >&2
        return 0
    fi
    local tmp
    tmp="$(mktemp "${path}.XXXXXX" 2>/dev/null)" || tmp="$(mktemp)"
    if "${gen[@]}" > "$tmp" 2>/dev/null; then
        chmod 0644 "$tmp" 2>/dev/null || true
        if mv -f "$tmp" "$path" 2>/dev/null; then
            return 0
        fi
    fi
    rm -f "$tmp" 2>/dev/null || true
    # shellcheck disable=SC2016 # backticks are literal text in a warning
    printf 'warn: aenv completion: `aenv completion zsh` failed; skipping %s\n' "$path" >&2
}

# Remove every well-formed aenv marker block from ~/.zshrc. Refuses to touch a
# file with a malformed (partial/nested/reordered) block. The rewrite is done
# in place (cat onto the rc) so the rc's inode, mode, ownership, and — for a
# symlinked rc — the link target are preserved.
#   $1 rc file path
_aenv_cc_rm_zsh_rc() {
    local rc="$1"
    [[ -f "$rc" ]] || return 0
    grep -q '^# >>> aenv completion >>>$' "$rc" 2>/dev/null || return 0
    if ! _aenv_cc_rc_well_formed "$rc"; then
        printf 'warn: aenv completion: malformed marker block in %s; leaving it untouched\n' "$rc" >&2
        return 0
    fi
    local tmp
    tmp="$(mktemp)"
    awk '/^# >>> aenv completion >>>$/,/^# <<< aenv completion <<<$/ { next } { print }' "$rc" > "$tmp" 2>/dev/null
    # In-place rewrite preserves inode/mode/ownership and writes through a
    # symlinked rc rather than replacing the link itself.
    if cat "$tmp" > "$rc" 2>/dev/null; then
        rm -f "$tmp" 2>/dev/null || true
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    printf 'warn: aenv completion: could not update %s\n' "$rc" >&2
}

# Install or remove the aenv shell-completion loaders.
#
#   aenv_completion_install install   [--prefix=<P>] [--user]
#   aenv_completion_install uninstall [--prefix=<P>] [--user]
#
# Always returns 0 so completion setup never aborts the surrounding binary
# installer; per-shell problems are reported as warnings on stderr.
aenv_completion_install() {
    local action="" prefix="" user_mode=0
    while (($#)); do
        case "$1" in
            install|uninstall) action="$1"; shift ;;
            --prefix=*) prefix="${1#--prefix=}"; shift ;;
            --user) user_mode=1; shift ;;
            *) printf 'warn: aenv completion: ignoring unknown argument %s\n' "$1" >&2; shift ;;
        esac
    done

    if [[ "$action" != "install" && "$action" != "uninstall" ]]; then
        printf 'warn: aenv completion: expected an install or uninstall action; skipping\n' >&2
        return 0
    fi

    # Capture $HOME once, safely (set -u safe). It drives both mode detection
    # and the user-mode destination paths, and must not be dereferenced bare.
    local home="${HOME:-}"

    # Auto-select user mode for a bare invocation or a prefix under $HOME.
    if [[ $user_mode -eq 0 ]]; then
        if [[ -z "$prefix" || ( -n "$home" && ( "$prefix" == "$home" || "$prefix" == "$home"/* ) ) ]]; then
            user_mode=1
        fi
    fi

    if [[ $user_mode -eq 1 && -z "$home" ]]; then
        printf 'warn: aenv completion: user mode requested but HOME is unset; skipping\n' >&2
        return 0
    fi

    local bash_file fish_file zsh_file zsh_kind
    if [[ $user_mode -eq 1 ]]; then
        bash_file="${home}/.local/share/bash-completion/completions/aenv"
        fish_file="${home}/.config/fish/completions/aenv.fish"
        zsh_file="${home}/.zshrc"
        zsh_kind="rc"
    else
        bash_file="${prefix}/share/bash-completion/completions/aenv"
        fish_file="${prefix}/share/fish/vendor_completions.d/aenv.fish"
        zsh_file="${prefix}/share/zsh/site-functions/_aenv"
        zsh_kind="static"
    fi

    if [[ "$action" == "install" ]]; then
        _aenv_cc_put "$bash_file" 0644 'source <(aenv completion bash)'
        _aenv_cc_put "$fish_file" 0644 'aenv completion fish | source'
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_put_zsh_rc "$zsh_file"
        else
            _aenv_cc_put_zsh_static "$zsh_file" "${prefix}/bin/aenv"
        fi
    else
        rm -f "$bash_file" "$fish_file" 2>/dev/null || true
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_rm_zsh_rc "$zsh_file"
        else
            rm -f "$zsh_file" 2>/dev/null || true
        fi
    fi
    return 0
}

# END aenv_completion_install

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

# Install regenerating shell-completion loaders (best-effort; never aborts the
# binary install). INSTALL_DIR/<bin> maps to a prefix of INSTALL_DIR/.., which
# selects user mode (~/.zshrc + per-user completion dirs) when the binary is
# installed under $HOME and system mode (<prefix>/share) otherwise.
aenv_completion_install install --prefix="${INSTALL_DIR%/*}"

if ! command -v aenv &>/dev/null; then
    echo ""
    echo "Note: ${INSTALL_DIR} is not on your PATH."
    echo "Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

echo "Run 'aenv --help' to get started."
