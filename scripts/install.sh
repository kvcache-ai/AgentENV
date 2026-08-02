#!/usr/bin/env bash
# Install AgentENV: the aenv CLI and the server binaries.
# The server is configured as a systemd service (or prints a manual start
# command if systemd is unavailable).
# Downloads: aenv (cli)   -> /usr/local/bin/aenv
#            server -> /usr/local/bin/server
#            dependencies -> /var/lib/aenv/deps
#            ublk daemon -> /var/lib/aenv/ublk/uvm-ublk-daemon
#            config  -> /var/lib/aenv/config/config.toml
#            overlaybd default config -> /etc/overlaybd/overlaybd.json
#            service -> /etc/systemd/system/aenv.service
#            env     -> /etc/default/aenv
# Runs:      sudo server --setup-only  (provisions runtime dependencies)
#            sudo server --setup-host  (provisions KVM, ublk, and networking)
#
# Usage:
#   curl -fsSL https://github.com/kvcache-ai/AgentENV/releases/latest/download/install.sh | sudo bash
#
# Override the AgentENV data directory:
#   sudo AENV_HOME_PATH=/path/to/aenv/data bash install.sh

set -euo pipefail
export LC_ALL=C

if [[ $EUID -ne 0 ]] && ! sudo -v 2>/dev/null; then
    echo "error: this script requires root or sudo access" >&2
    exit 1
fi

REPO="kvcache-ai/AgentENV"
INSTALL_DIR="/usr/local/bin"
SKIP_SETUP="${SKIP_SETUP:-0}"
DATA_DIR="${AENV_HOME_PATH:-/var/lib/aenv}"
CONFIG_PATH="${DATA_DIR}/config/config.toml"
DEPS_DIR="${DATA_DIR}/deps"
UBLK_DAEMON_PATH="${DATA_DIR}/ublk/uvm-ublk-daemon"
SERVICE_NAME="aenv"
SERVICE_USER="${AENV_SERVICE_USER:-aenv}"
SERVICE_GROUP="${AENV_SERVICE_GROUP:-aenv}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_FILE="/etc/default/${SERVICE_NAME}"
RUNTIME_DIR="/run/aenv"

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64) ARCH_TAG="x86_64" ;;
    *)
        echo "error: unsupported architecture: $ARCH (server requires x86_64)" >&2
        exit 1
        ;;
esac

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$OS" != "linux" ]]; then
    echo "error: AgentENV server only supports Linux" >&2
    exit 1
fi

RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
TARBALL="aenv-server-${OS}-${ARCH_TAG}.tar.gz"

curl_get() {
    curl -fsSL --retry 5 --retry-delay 10 --retry-max-time 60 "$@"
}

# BEGIN aenv_completion_install
# (keep in sync across scripts/shell-completion.sh, scripts/install-cli.sh,
# scripts/install.sh — verified by scripts/check-completion-sync.sh)
#
# All filesystem writes flow through ONE atomic-commit primitive
# (`_aenv_cc_commit`): the new content is staged to a temp file created IN the
# destination directory (same filesystem => an atomic rename), the destination's
# symlink is resolved (so the link is preserved, not replaced), and its mode
# (and, when running as root, ownership) is copied onto the temp file first.
# This guarantees every write site shares the same atomicity / symlink /
# metadata properties, so the pattern cannot drift between functions.
#
# NOTE: this does NOT preserve ACLs, extended attributes, or security labels
# (SELinux/AppArmor contexts) — only the POSIX mode bits, and ownership when
# we are root. Callers writing to files that carry such metadata should not
# assume it survives the rename.
#
# Every generated file/block also carries an aenv ownership marker so
# `uninstall` never deletes a file it did not create (see _AENV_CC_MARKER).

_AENV_CC_MARKER="# managed by aenv-installer; do not edit (remove the whole file to opt out)"

# Portable octal mode of an existing file (e.g. 644); defaults to 0644 when the
# mode cannot be read (e.g. the file does not yet exist). GNU stat uses -c,
# BSD/macOS stat uses -f.
_aenv_cc_mode_octal() {
    stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1" 2>/dev/null || printf '0644'
}

# Resolve $1 to the real file path it refers to when it is a symlink, so writes
# land on the target and preserve the link rather than replacing it.
#   - Not a symlink: prints $1, returns 0.
#   - Symlink, resolvable: prints the resolved absolute path, returns 0.
#   - Symlink, NOT resolvable (broken link, no readlink at all): prints
#     nothing and returns 1. Callers MUST check the return status and refuse
#     to write rather than falling back to $1 — writing to $1 in that case
#     would replace the symlink itself, silently breaking the "preserve the
#     link" guarantee this whole module advertises.
_aenv_cc_resolve() {
    if [[ -L "$1" ]]; then
        local resolved
        if resolved="$(readlink -f "$1" 2>/dev/null)" && [[ -n "$resolved" ]]; then
            printf '%s' "$resolved"
            return 0
        fi
        # Portable one-hop fallback for platforms without GNU `readlink -f`
        # (e.g. some BSD/macOS readlink builds). Only handles a single-level
        # symlink, which covers the common case; anything more exotic
        # (relative multi-hop chains) is treated as unresolvable.
        if resolved="$(readlink "$1" 2>/dev/null)" && [[ -n "$resolved" ]]; then
            [[ "$resolved" = /* ]] || resolved="${1%/*}/$resolved"
            printf '%s' "$resolved"
            return 0
        fi
        return 1
    fi
    printf '%s' "$1"
    return 0
}

# Atomically publish a staged temp file as <dest>.
#   $1 temp file path — MUST live in the same directory as <dest>'s RESOLVED
#      target (caller's job) so the final rename is atomic and not a
#      cross-filesystem copy+delete.
#   $2 destination path (possibly a symlink; its target is replaced, the link
#      itself is preserved). The destination's current mode — and, when
#      running as root, its ownership — is copied onto the temp first (0644
#      default for a new file).
# Returns nonzero on failure (including an unresolvable symlink); the caller
# is responsible for cleaning up the temp in that case.
_aenv_cc_commit() {
    local tmp="$1" dest="$2"
    local target
    if ! target="$(_aenv_cc_resolve "$dest")" || [[ -z "$target" ]]; then
        return 1
    fi
    chmod "$(_aenv_cc_mode_octal "$target")" "$tmp" 2>/dev/null || chmod 0644 "$tmp" 2>/dev/null || true
    if [[ $EUID -eq 0 && -e "$target" ]]; then
        chown --reference="$target" "$tmp" 2>/dev/null || true
    fi
    mv -f "$tmp" "$target" 2>/dev/null
}

# Write a single completion loader file atomically, prefixed with the aenv
# ownership marker so a later uninstall can verify it still owns the file
# before deleting it. Non-fatal on I/O errors.
#   $1 destination path
#   $2 file mode (e.g. 0644)
#   $3 loader content (one or more lines; the marker is prepended)
_aenv_cc_put() {
    local path="$1" mode="$2" content="$3"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    local target
    if ! target="$(_aenv_cc_resolve "$path")" || [[ -z "$target" ]]; then
        printf 'warn: aenv completion: %s is a symlink that could not be resolved; skipping\n' "$path" >&2
        return 0
    fi
    local target_dir="${target%/*}"
    if ! mkdir -p "$target_dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$target_dir" >&2
        return 0
    fi
    local tmp
    tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s\n' "$path" >&2
        return 0
    }
    if { printf '%s\n' "$_AENV_CC_MARKER"; printf '%s\n' "$content"; } > "$tmp" 2>/dev/null \
        && _aenv_cc_commit "$tmp" "$path"; then
        chmod "$mode" "$path" 2>/dev/null || true
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    printf 'warn: aenv completion: could not write %s\n' "$path" >&2
}

# Return 0 (exit status) if $1's aenv marker blocks — if any — are well-formed:
# strictly alternating start/end pairs with no nesting, reordering, orphan
# markers, or unterminated start at EOF. Returns 1 otherwise. Used to gate both
# install idempotency and removal so a corrupted/partial block is never silently
# truncated and never auto-repaired at the cost of unrelated rc content.
_aenv_cc_rc_well_formed() {
    awk '
        BEGIN { in_block = 0 }
        /^# >>> aenv completion >>>$/ { if (in_block) exit 1; in_block = 1; next }
        /^# <<< aenv completion <<<$/ { if (!in_block) exit 1; in_block = 0; next }
        END { if (in_block) exit 1 }
    ' "$1" 2>/dev/null
}

# The canonical managed block content (including its start/end markers).
# Single source of truth used both to write a fresh block and to detect a
# stale one on reinstall/upgrade.
#
# The compinit call is now guarded on `compdef` already being defined, so a
# framework (oh-my-zsh, prezto, etc.) or an earlier rc section that already
# ran compinit is not forced to pay for a second (relatively expensive) run
# on every shell start.
_aenv_cc_zsh_block_canonical() {
    printf '# >>> aenv completion >>>\n'
    printf 'if command -v aenv >/dev/null 2>&1; then\n'
    printf 'type compdef >/dev/null 2>&1 || { autoload -Uz compinit && compinit; }\n'
    # shellcheck disable=SC2016 # $(...) is literal text for zsh to eval, not bash
    printf 'eval "$(aenv completion zsh)"\n'
    printf 'fi\n'
    printf '# <<< aenv completion <<<\n'
}

# Print the currently-installed managed block (markers included) from $1, or
# nothing if there isn't one. Used to detect a stale block on reinstall.
_aenv_cc_zsh_block_current() {
    awk '
        /^# >>> aenv completion >>>$/ { f = 1 }
        f { print }
        /^# <<< aenv completion <<<$/ { f = 0 }
    ' "$1" 2>/dev/null
}

# Serialize the full read-check-write of a zsh rc file with an flock-based
# lock so two concurrent installer runs (or install racing uninstall) cannot
# both observe "no marker" and both append, or otherwise interleave into a
# malformed/duplicated block. Best-effort: if flock isn't available we fall
# back to running unlocked rather than failing the (best-effort) completion
# install outright.
#   $1 rc file path
#   $2... function name + args to run inside the lock
_aenv_cc_with_zsh_lock() {
    local rc="$1"; shift
    if command -v flock >/dev/null 2>&1; then
        (
            flock -w 10 200 || {
                printf 'warn: aenv completion: could not lock %s (timed out); skipping\n' "$rc" >&2
                exit 0
            }
            "$@"
        ) 200>"${rc}.aenv-lock" 2>&2
    else
        "$@"
    fi
}

# Append (or, on upgrade, in-place replace) the regenerating zsh rc-snippet,
# idempotently and atomically.
#   - No markers present: append the canonical block.
#   - Well-formed block present, contents match canonical: no-op.
#   - Well-formed block present, contents differ (e.g. upgrade changed the
#     snippet): replace just the block, byte-for-byte, leaving everything
#     else in the file untouched.
#   - Malformed block present: warn and leave it for the user (auto-repair
#     could delete unrelated rc lines).
# The full new rc (existing content, possibly with the block replaced, or
# + the appended block) is staged to a same-directory temp and committed by
# an atomic rename, so an interruption or I/O failure never leaves a
# partial/malformed block in the live rc.
#   $1 rc file path
_aenv_cc_put_zsh_rc_impl() {
    local rc="$1"
    local dir="${rc%/*}"
    local canonical
    canonical="$(_aenv_cc_zsh_block_canonical)"

    if [[ -f "$rc" ]] && grep -qE '^# (>>> aenv completion >>>|<<< aenv completion <<<)$' "$rc" 2>/dev/null; then
        if ! _aenv_cc_rc_well_formed "$rc"; then
            printf 'warn: aenv completion: malformed marker block already in %s; leaving it untouched (remove it manually to regenerate)\n' "$rc" >&2
            return 0
        fi
        local current
        current="$(_aenv_cc_zsh_block_current "$rc")"
        if [[ "$current" == "$canonical" ]]; then
            return 0 # idempotent: a complete, up-to-date managed block already exists
        fi
        # Stale block: rewrite just that span in place, atomically.
        local target tmp
        target="$(_aenv_cc_resolve "$rc")" || {
            printf 'warn: aenv completion: %s is a symlink that could not be resolved; skipping\n' "$rc" >&2
            return 0
        }
        tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
            printf 'warn: aenv completion: could not create temp file near %s\n' "$rc" >&2
            return 0
        }
        if awk -v block="$canonical" '
                BEGIN { in_block = 0 }
                /^# >>> aenv completion >>>$/ { print block; in_block = 1; next }
                /^# <<< aenv completion <<<$/ { in_block = 0; next }
                in_block { next }
                { print }
            ' "$target" > "$tmp" 2>/dev/null && _aenv_cc_commit "$tmp" "$rc"; then
            return 0
        fi
        rm -f "$tmp" 2>/dev/null || true
        printf 'warn: aenv completion: could not update stale block in %s\n' "$rc" >&2
        return 0
    fi

    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    local target tmp last_byte
    target="$(_aenv_cc_resolve "$rc")" || {
        printf 'warn: aenv completion: %s is a symlink that could not be resolved; skipping\n' "$rc" >&2
        return 0
    }
    tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s\n' "$rc" >&2
        return 0
    }
    # Stage existing content first (guarded), then add a separating newline if
    # the existing content did not end in one, then the managed block. Each step
    # returns nonzero on I/O failure so we never commit a partial result.
    if [[ -s "$target" ]]; then
        cat "$target" > "$tmp" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; printf 'warn: aenv completion: could not stage %s\n' "$rc" >&2; return 0; }
        last_byte=$(tail -c 1 "$tmp" 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n') || last_byte=""
        [[ "$last_byte" == "0a" ]] || printf '\n' >> "$tmp" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; printf 'warn: aenv completion: could not stage %s\n' "$rc" >&2; return 0; }
    fi
    if ! printf '%s\n' "$canonical" >> "$tmp" 2>/dev/null; then
        rm -f "$tmp" 2>/dev/null
        printf 'warn: aenv completion: could not stage %s\n' "$rc" >&2
        return 0
    fi
    _aenv_cc_commit "$tmp" "$rc" || {
        rm -f "$tmp" 2>/dev/null
        printf 'warn: aenv completion: could not update %s\n' "$rc" >&2
    }
}

_aenv_cc_put_zsh_rc() {
    _aenv_cc_with_zsh_lock "$1" _aenv_cc_put_zsh_rc_impl "$1"
}

# Generate the static zsh completion into a site-functions dir, prefixed with
# the aenv ownership marker (as a comment) so uninstall can verify ownership.
# $2 is the just-installed aenv binary (preferred over whatever is on PATH,
# which may be stale or absent). Generation goes through `_aenv_cc_commit`,
# so a failure or empty output never replaces an existing valid completion
# file.
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
    local target
    if ! target="$(_aenv_cc_resolve "$path")" || [[ -z "$target" ]]; then
        printf 'warn: aenv completion: %s is a symlink that could not be resolved; skipping\n' "$path" >&2
        return 0
    fi
    local tmp
    tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s; skipping static zsh\n' "$path" >&2
        return 0
    }
    # Generate the completion body first and require it to be non-empty: a
    # broken aenv that exits 0 with no bytes must not erase a working completion
    # via the atomic rename. The ownership marker is appended AFTER this check
    # (as a trailing comment) so the generated #compdef stays on line 1 — zsh
    # only loads the function if #compdef is the first line.
    if ! "${gen[@]}" > "$tmp" 2>/dev/null || [[ ! -s "$tmp" ]]; then
        rm -f "$tmp" 2>/dev/null || true
        # shellcheck disable=SC2016 # backticks are literal text in a warning
        printf 'warn: aenv completion: `aenv completion zsh` failed or produced no output; skipping %s\n' "$path" >&2
        return 0
    fi
    printf '%s\n' "$_AENV_CC_MARKER" >> "$tmp" 2>/dev/null || {
        rm -f "$tmp" 2>/dev/null || true
        printf 'warn: aenv completion: could not stage ownership marker for %s\n' "$path" >&2
        return 0
    }
    if _aenv_cc_commit "$tmp" "$path"; then
        chmod 0644 "$path" 2>/dev/null || true
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    printf 'warn: aenv completion: could not commit static zsh file %s\n' "$path" >&2
}

# True if $1 is a plain file that starts with the aenv ownership marker —
# i.e. a file this installer created and is safe to remove. A pre-existing,
# hand-written, or package-manager-owned completion file at the same
# conventional path will NOT match, and is left alone.
_aenv_cc_owns() {
    [[ -f "$1" ]] || return 1
    # Match the marker anywhere: bash/fish stubs carry it on line 1, while the
    # static zsh file carries it as a trailing comment (its #compdef must stay
    # on line 1 for zsh to load the function).
    grep -qF -- "$_AENV_CC_MARKER" "$1" 2>/dev/null
}

# Remove $1 only if we own it (see _aenv_cc_owns); otherwise warn and leave it
# untouched so a user's own completion file, or one now owned by a package
# manager, is never silently deleted.
_aenv_cc_rm_owned() {
    local path="$1"
    [[ -e "$path" ]] || return 0
    if _aenv_cc_owns "$path"; then
        rm -f "$path" 2>/dev/null || printf 'warn: aenv completion: could not remove %s\n' "$path" >&2
    else
        printf 'warn: aenv completion: %s was not installed by aenv (no ownership marker); leaving it untouched\n' "$path" >&2
    fi
}

# Remove every well-formed aenv marker block from the zsh rc. Refuses to touch
# a file with a malformed (partial/nested/reordered/orphan) block. The
# rewrite is staged to a same-directory temp and committed by an atomic
# rename via `_aenv_cc_commit`, so the rc's inode/mode/ownership and (for a
# symlinked rc) the link itself are preserved, and a failed/partial awk never
# reaches the live file. Locked the same way as install to avoid racing a
# concurrent install/uninstall or hand-edit.
#   $1 rc file path
_aenv_cc_rm_zsh_rc_impl() {
    local rc="$1"
    [[ -f "$rc" ]] || return 0
    grep -qE '^# (>>> aenv completion >>>|<<< aenv completion <<<)$' "$rc" 2>/dev/null || return 0
    if ! _aenv_cc_rc_well_formed "$rc"; then
        printf 'warn: aenv completion: malformed marker block in %s; leaving it untouched\n' "$rc" >&2
        return 0
    fi
    local target tmp
    target="$(_aenv_cc_resolve "$rc")" || {
        printf 'warn: aenv completion: %s is a symlink that could not be resolved; leaving it untouched\n' "$rc" >&2
        return 0
    }
    tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s; leaving it untouched\n' "$rc" >&2
        return 0
    }
    if awk '/^# >>> aenv completion >>>$/,/^# <<< aenv completion <<<$/ { next } { print }' "$target" > "$tmp" 2>/dev/null && _aenv_cc_commit "$tmp" "$rc"; then
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    printf 'warn: aenv completion: could not update %s\n' "$rc" >&2
}

_aenv_cc_rm_zsh_rc() {
    _aenv_cc_with_zsh_lock "$1" _aenv_cc_rm_zsh_rc_impl "$1"
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
        # bash/fish loaders guard on aenv presence so a missing/uninstalled aenv
        # is silent rather than erroring on every shell start (matches the zsh
        # rc-snippet's `command -v aenv` guard).
        _aenv_cc_put "$bash_file" 0644 'command -v aenv >/dev/null 2>&1 && source <(aenv completion bash)'
        _aenv_cc_put "$fish_file" 0644 'type -q aenv; and aenv completion fish | source'
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_put_zsh_rc "$zsh_file"
        else
            _aenv_cc_put_zsh_static "$zsh_file" "${prefix}/bin/aenv"
        fi
    else
        _aenv_cc_rm_owned "$bash_file"
        _aenv_cc_rm_owned "$fish_file"
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_rm_zsh_rc "$zsh_file"
        else
            _aenv_cc_rm_owned "$zsh_file"
        fi
    fi
    return 0
}

# END aenv_completion_install

missing_packages=()
command -v curl >/dev/null 2>&1 || missing_packages+=(curl)
command -v jq >/dev/null 2>&1 || missing_packages+=(jq)
command -v sha256sum >/dev/null 2>&1 || missing_packages+=(coreutils)
command -v realpath >/dev/null 2>&1 || missing_packages+=(coreutils)

if ((${#missing_packages[@]} > 0)); then
    if command -v apt-get >/dev/null 2>&1; then
        echo "Installing required commands: ${missing_packages[*]} ..."
        sudo apt-get update
        sudo apt-get install -y "${missing_packages[@]}"
    else
        echo "error: missing required commands and apt-get is unavailable: ${missing_packages[*]}" >&2
        exit 1
    fi
fi

for command in curl jq sha256sum realpath; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command is still unavailable after installation: ${command}" >&2
        exit 1
    fi
done

if [[ ! "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]*$ || ! "$SERVICE_GROUP" =~ ^[a-z_][a-z0-9_-]*$ ]]; then
    echo "error: invalid AgentENV service user or group" >&2
    exit 1
fi

resolved_data_dir="$(realpath -m "$DATA_DIR")"
if [[ "$resolved_data_dir" == "/" || -L "$DATA_DIR" ]]; then
    echo "error: refusing unsafe AgentENV data directory: ${DATA_DIR}" >&2
    exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
    sudo groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    sudo useradd --system --gid "$SERVICE_GROUP" --home-dir "$DATA_DIR" \
        --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if [[ "$(id -u "$SERVICE_USER")" == "0" || "$(getent group "$SERVICE_GROUP" | cut -d: -f3)" == "0" ]]; then
    echo "error: AgentENV service user and group must be non-root" >&2
    exit 1
fi

release_metadata="$(mktemp)"
tmp_cli="$(mktemp)"
tmp_tarball="$(mktemp)"
tmp_dir="$(mktemp -d)"
current_env=""
tmp_env=""
trap 'rm -f "$release_metadata" "$tmp_cli" "$tmp_tarball" "$current_env" "$tmp_env"; rm -rf "$tmp_dir"' EXIT

api_headers=(
    -H "Accept: application/vnd.github+json"
    -H "X-GitHub-Api-Version: 2022-11-28"
)

curl_get "${api_headers[@]}" "$RELEASE_API" -o "$release_metadata"

download_release_asset() {
    local asset_name="$1"
    local destination="$2"
    local asset_json url digest expected actual

    asset_json="$(
        jq -cer --arg name "$asset_name" \
            '[.assets[] | select(.name == $name)] |
             if length == 1 then .[0] else error("release asset not found or not unique") end' \
            "$release_metadata"
    )" || {
        echo "error: release asset not found or not unique: ${asset_name}" >&2
        exit 1
    }
    url="$(jq -r '.browser_download_url // empty' <<< "$asset_json")"
    digest="$(jq -r '.digest // empty' <<< "$asset_json")"
    if [[ -z "$url" ]]; then
        echo "error: GitHub did not provide a download URL for ${asset_name}" >&2
        exit 1
    fi
    if [[ "$digest" != sha256:* ]]; then
        echo "error: GitHub did not provide a SHA256 digest for ${asset_name}" >&2
        exit 1
    fi

    curl_get "$url" -o "$destination"
    expected="${digest#sha256:}"
    actual="$(sha256sum "$destination" | awk '{print $1}')"
    if [[ "$expected" != "$actual" ]]; then
        echo "error: SHA256 mismatch for ${asset_name}" >&2
        echo "  expected: ${expected}" >&2
        echo "  actual:   ${actual}" >&2
        exit 1
    fi
}

sudo mkdir -p "$INSTALL_DIR"

# ---------------------------------------------------------------------------
# 1. Install the aenv CLI
# ---------------------------------------------------------------------------
echo "Downloading aenv CLI ..."
download_release_asset "aenv-linux-${ARCH_TAG}" "$tmp_cli"
sudo install -m 0755 "$tmp_cli" "${INSTALL_DIR}/aenv"

# Install regenerating shell-completion loaders (best-effort; never aborts the
# install). System-wide install -> system mode writes <prefix>/share loaders.
aenv_completion_install install --prefix="${INSTALL_DIR%/*}"

# ---------------------------------------------------------------------------
# 2. Install the server
# ---------------------------------------------------------------------------
if [[ -d /run/systemd/system ]] && systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    echo "Stopping existing ${SERVICE_NAME} service ..."
    sudo systemctl stop "${SERVICE_NAME}"
fi

echo "Downloading ${TARBALL} ..."
download_release_asset "$TARBALL" "$tmp_tarball"

tar -xzf "$tmp_tarball" -C "$tmp_dir"

sudo mkdir -p "$(dirname "$UBLK_DAEMON_PATH")"
sudo install -m 0755 "$tmp_dir/server" "${INSTALL_DIR}/server"
sudo install -m 0755 "$tmp_dir/ublk/uvm-ublk-daemon" "$UBLK_DAEMON_PATH"

if [[ -d "$tmp_dir/deps" ]]; then
    sudo rm -rf \
        "$DEPS_DIR/firecracker" \
        "$DEPS_DIR/kernel" \
        "$DEPS_DIR/tools" \
        "$DEPS_DIR/overlaybd" \
        "$DEPS_DIR/regctl"
    sudo mkdir -p "$DEPS_DIR"
    sudo cp -a "$tmp_dir/deps/." "$DEPS_DIR/"
    sudo chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DEPS_DIR"
fi

if [[ -f "$tmp_dir/etc/overlaybd/overlaybd.json" && ! -f "/etc/overlaybd/overlaybd.json" ]]; then
    sudo install -D -m 0644 "$tmp_dir/etc/overlaybd/overlaybd.json" /etc/overlaybd/overlaybd.json
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
    sudo mkdir -p "$(dirname "$CONFIG_PATH")"
    sudo install -o root -g "$SERVICE_GROUP" -m 0640 "$tmp_dir/default.toml" "$CONFIG_PATH"
fi

if [[ "$SKIP_SETUP" == "1" ]]; then
    echo "Skipping setup (SKIP_SETUP=1)."
else
    echo "Running dependency setup ..."
    sudo AENV_CONFIG_PATH="${CONFIG_PATH}" AENV_HOME_PATH="${DATA_DIR}" \
        "${INSTALL_DIR}/server" --setup-only

    echo "Provisioning KVM, ublk, and host networking for ${SERVICE_USER} ..."
    sudo AENV_CONFIG_PATH="${CONFIG_PATH}" AENV_HOME_PATH="${DATA_DIR}" \
        "${INSTALL_DIR}/server" --setup-host \
        --runtime-user "$SERVICE_USER" --runtime-group "$SERVICE_GROUP"
fi

# Restrict ownership migration to the dedicated AgentENV data tree. External
# repositories configured outside this tree are intentionally untouched.
sudo install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_DIR"
sudo chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DATA_DIR"
sudo install -d -o root -g "$SERVICE_GROUP" -m 0750 "$(dirname "$CONFIG_PATH")"
sudo chown root:"$SERVICE_GROUP" "$CONFIG_PATH"
sudo chmod 0640 "$CONFIG_PATH"

# ---------------------------------------------------------------------------
# 3. Configure systemd
# ---------------------------------------------------------------------------
if [[ -d /run/systemd/system ]]; then
    ENV_FILE_STATUS="exists"
    if [[ ! -f "$ENV_FILE" ]]; then
        sudo tee "$ENV_FILE" > /dev/null <<EOF
API_ADDR="127.0.0.1:8000"
AENV_CONFIG_PATH="${CONFIG_PATH}"
AENV_HOME_PATH="${DATA_DIR}"
AENV_RUNTIME_PATH="${RUNTIME_DIR}"
EOF
        ENV_FILE_STATUS="written"
    else
        if [[ "$CONFIG_PATH" == *$'\n'* || "$CONFIG_PATH" == *$'\r'* ||
              "$DATA_DIR" == *$'\n'* || "$DATA_DIR" == *$'\r'* ]]; then
            echo "error: AgentENV paths must not contain newlines" >&2
            exit 1
        fi

        escaped_config="${CONFIG_PATH//\\/\\\\}"
        escaped_config="${escaped_config//\"/\\\"}"
        escaped_data="${DATA_DIR//\\/\\\\}"
        escaped_data="${escaped_data//\"/\\\"}"
        current_env="$(mktemp)"
        tmp_env="$(mktemp)"
        # sudo is needed for the read; the redirect intentionally targets the
        # invoking user's temporary file.
        # shellcheck disable=SC2024
        sudo cat "$ENV_FILE" > "$current_env"
        found_config=0
        found_home=0
        found_runtime=0
        while IFS= read -r line || [[ -n "$line" ]]; do
            case "$line" in
                AENV_CONFIG_PATH=*)
                    printf 'AENV_CONFIG_PATH="%s"\n' "$escaped_config" >> "$tmp_env"
                    found_config=1
                    ;;
                AENV_HOME_PATH=*)
                    printf 'AENV_HOME_PATH="%s"\n' "$escaped_data" >> "$tmp_env"
                    found_home=1
                    ;;
                AENV_RUNTIME_PATH=*)
                    printf 'AENV_RUNTIME_PATH="%s"\n' "$RUNTIME_DIR" >> "$tmp_env"
                    found_runtime=1
                    ;;
                *)
                    printf '%s\n' "$line" >> "$tmp_env"
                    ;;
            esac
        done < "$current_env"
        if [[ "$found_config" == "0" ]]; then
            printf 'AENV_CONFIG_PATH="%s"\n' "$escaped_config" >> "$tmp_env"
        fi
        if [[ "$found_home" == "0" ]]; then
            printf 'AENV_HOME_PATH="%s"\n' "$escaped_data" >> "$tmp_env"
        fi
        if [[ "$found_runtime" == "0" ]]; then
            printf 'AENV_RUNTIME_PATH="%s"\n' "$RUNTIME_DIR" >> "$tmp_env"
        fi
        sudo install -m 0644 "$tmp_env" "$ENV_FILE"
        rm -f "$current_env" "$tmp_env"
        ENV_FILE_STATUS="updated"
    fi

    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=AgentENV Server
After=network.target

[Service]
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
SupplementaryGroups=kvm
EnvironmentFile=${ENV_FILE}
ExecStart=${INSTALL_DIR}/server
RuntimeDirectory=aenv
RuntimeDirectoryMode=0750
AmbientCapabilities=CAP_NET_ADMIN CAP_SYS_ADMIN
CapabilityBoundingSet=CAP_NET_ADMIN CAP_SYS_ADMIN
NoNewPrivileges=true
UMask=0027
LimitNOFILE=1048576
LimitMEMLOCK=infinity
Restart=on-failure
RestartSec=5
# KillMode=process lets the server pause and persist running sandboxes before
# exiting. The default control-group mode would SIGKILL all Firecracker child
# processes immediately, losing in-memory sandbox state.
KillMode=process
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable "${SERVICE_NAME}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Installation complete."
echo ""
echo "  CLI    : ${INSTALL_DIR}/aenv"
echo "  Server : ${INSTALL_DIR}/server"
echo "  Data   : ${DATA_DIR}"
echo "  Config : ${CONFIG_PATH}"
if [[ -d /run/systemd/system ]]; then
    if [[ "$ENV_FILE_STATUS" == "written" ]]; then
        echo "  Env    : ${ENV_FILE}"
    else
        echo "  Env    : ${ENV_FILE} (${ENV_FILE_STATUS})"
    fi
    echo "  Service: ${SERVICE_FILE}"
fi
echo ""
if [[ -d /run/systemd/system ]]; then
    echo "Start the server:"
    echo "  sudo systemctl start ${SERVICE_NAME}"
    echo ""
    echo "To change the listen port, edit API_ADDR in ${ENV_FILE} then run:"
    echo "  sudo systemctl restart ${SERVICE_NAME}"
    echo ""
    echo "Check status / logs:"
    echo "  sudo systemctl status ${SERVICE_NAME}"
    echo "  sudo journalctl -u ${SERVICE_NAME} -f"
else
    echo "systemd not detected. Start the server manually:"
    echo "  sudo setpriv --reuid=${SERVICE_USER} --regid=${SERVICE_GROUP} --init-groups \\"
    echo "    --inh-caps=+net_admin,+sys_admin --ambient-caps=+net_admin,+sys_admin \\"
    echo "    --bounding-set=-all,+net_admin,+sys_admin --nnp \\"
    echo "    env AENV_CONFIG_PATH=${CONFIG_PATH} AENV_HOME_PATH=${DATA_DIR} AENV_RUNTIME_PATH=${RUNTIME_DIR} \\"
    echo "    API_ADDR=127.0.0.1:8000 ${INSTALL_DIR}/server"
fi
echo ""
echo "For aenv CLI:"
echo "  Run 'aenv --help' to get started."
