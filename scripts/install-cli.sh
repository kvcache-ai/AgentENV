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
    local p="$1" link hops=0
    # Walk the symlink chain portably with a bound (cycle detection), so
    # multi-hop chains resolve on BSD/macOS (no `readlink -f`) as well as GNU.
    # Returns failure on an unreadable or broken symlink, so callers never mv
    # over an intermediate link. A plain (non-symlink) path is returned as-is
    # even when it does not yet exist, so first-install (which creates the file)
    # is not blocked.
    while [[ -L "$p" ]]; do
        if ! link=$(readlink "$p" 2>/dev/null) || [[ -z "$link" ]]; then
            return 1
        fi
        [[ "$link" = /* ]] || link="${p%/*}/$link"
        p="$link"
        hops=$((hops + 1))
        [[ "$hops" -lt 40 ]] || return 1
    done
    # If we followed at least one hop and landed on a non-existent path, the
    # link chain is broken — refuse rather than mv into nothing.
    [[ "$hops" -gt 0 && ! -e "$p" ]] && return 1
    printf '%s' "$p"
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
    # Preserve ownership when running as root. `chown --reference` is GNU-only,
    # so read uid:gid portably (GNU `stat -c`, BSD/macOS `stat -f`) and chown
    # explicitly; do NOT silently commit a root-owned temp over a user file.
    if [[ $EUID -eq 0 && -e "$target" ]]; then
        local ids
        ids=$(stat -c '%u:%g' "$target" 2>/dev/null || stat -f '%u:%g' "$target" 2>/dev/null || true)
        if [[ -n "$ids" ]] && ! chown "$ids" "$tmp" 2>/dev/null; then
            return 1
        fi
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
    # Do not overwrite a pre-existing file we did not create (a hand-written or
    # package-manager completion). Symmetric with _aenv_cc_rm_owned.
    if [[ -e "$target" ]] && ! _aenv_cc_owns "$target"; then
        printf 'warn: aenv completion: %s already exists and is not aenv-managed; leaving it untouched\n' "$path" >&2
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
    # Lock the rc's OWN fd (read-only open: no truncation, and no sidecar lock
    # file in the user's directory that a privileged run could be tricked into
    # following as a symlink). Best-effort: fall back to unlocked where flock is
    # missing or the rc does not yet exist (first install).
    if command -v flock >/dev/null 2>&1 && [[ -e "$rc" ]]; then
        (
            exec 200<"$rc" 2>/dev/null || { "$@"; exit; }
            flock -w 10 200 2>/dev/null || \
                printf 'warn: aenv completion: could not lock %s (timed out); proceeding unlocked\n' "$rc" >&2
            "$@"
        )
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
    if [[ -e "$target" ]] && ! _aenv_cc_owns "$target"; then
        printf 'warn: aenv completion: %s already exists and is not aenv-managed; leaving it untouched\n' "$path" >&2
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
    # Ensure a newline separates the completion body from the marker comment so
    # a generator that omits a trailing newline does not fuse the marker onto
    # the last shell statement.
    local last_byte
    last_byte=$(tail -c 1 "$tmp" 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n') || last_byte=""
    { [[ "$last_byte" == "0a" ]] || printf '\n'; printf '%s\n' "$_AENV_CC_MARKER"; } >> "$tmp" 2>/dev/null || {
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
# manager, is never silently deleted. Resolves symlinks before both the
# ownership check and the removal so an install/uninstall cycle through a
# symlinked completion path removes the managed TARGET we wrote, not the link.
_aenv_cc_rm_owned() {
    local path="$1"
    [[ -e "$path" ]] || return 0
    local target
    if ! target="$(_aenv_cc_resolve "$path")" || [[ -z "$target" ]]; then
        printf 'warn: aenv completion: %s is an unresolvable symlink; leaving it untouched\n' "$path" >&2
        return 0
    fi
    if _aenv_cc_owns "$target"; then
        rm -f "$target" 2>/dev/null || printf 'warn: aenv completion: could not remove %s\n' "$path" >&2
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
# binary install). Derive the prefix from INSTALL_DIR with dirname so edge cases
# like INSTALL_DIR=/bin map to prefix=/ rather than an empty string (which would
# be misread as user mode). A prefix under $HOME selects user mode; otherwise
# system mode (<prefix>/share).
aenv_completion_install install --prefix="$(dirname -- "$INSTALL_DIR")"

if ! command -v aenv &>/dev/null; then
    echo ""
    echo "Note: ${INSTALL_DIR} is not on your PATH."
    echo "Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

echo "Run 'aenv --help' to get started."
