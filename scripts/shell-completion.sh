#!/usr/bin/env bash
# Manage lightweight, regenerating shell-completion loaders for the `aenv` CLI.
#
# The loaders do NOT cache a generated completion script. Instead they invoke
# `aenv completion <shell>` at shell start (fish/zsh-user) or on first `aenv
# <TAB>` (bash lazy-loading), so the completion always matches the currently
# installed `aenv` binary and never goes stale when the CLI is upgraded.
#
# Usage:
#   ./scripts/shell-completion.sh install   [--prefix=<P>] [--user]
#   ./scripts/shell-completion.sh uninstall [--prefix=<P>] [--user]
#
#   --user        Force user-local mode: writes under $HOME and appends an
#                 rc-snippet to ~/.zshrc.
#   --prefix=<P>  System mode (writes under <P>/share) unless <P> is under
#                 $HOME, in which case user mode is auto-selected.
#
# When neither flag is given, defaults to user mode so a bare run never
# requires root. All installers pass an explicit flag.
#
# Destinations:
#   user   bash: ~/.local/share/bash-completion/completions/aenv
#          fish: ~/.config/fish/completions/aenv.fish
#          zsh:  rc-snippet in ~/.zshrc (regenerates every shell start)
#   system bash: <P>/share/bash-completion/completions/aenv
#          fish: <P>/share/fish/vendor_completions.d/aenv.fish
#          zsh:  static <P>/share/zsh/site-functions/_aenv (system installs are
#                refreshed by re-running the installer, so a one-shot static
#                file avoids a root-owned edit of every user's rc)
#
# The `aenv_completion_install` function (and its `_aenv_cc_*` helpers) below
# is the single source of truth. It is inlined verbatim into
# scripts/install-cli.sh and scripts/install.sh; scripts/check-completion-sync.sh
# enforces that the three copies stay byte-identical.
set -euo pipefail

# BEGIN aenv_completion_install
# (keep in sync across scripts/shell-completion.sh, scripts/install-cli.sh,
# scripts/install.sh — verified by scripts/check-completion-sync.sh)
#
# All filesystem writes flow through ONE atomic-commit primitive
# (`_aenv_cc_commit`): the new content is staged to a temp file created IN the
# destination directory (same filesystem => an atomic rename), the destination's
# symlink is resolved (so the link is preserved, not replaced) and its mode is
# copied. This guarantees every write site shares the same atomicity / symlink /
# metadata properties, so the pattern cannot drift between functions.

# Portable octal mode of an existing file (e.g. 644); defaults to 0644 when the
# mode cannot be read (e.g. the file does not yet exist). GNU stat uses -c,
# BSD/macOS stat uses -f.
_aenv_cc_mode_octal() {
    stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1" 2>/dev/null || printf '0644'
}

# Resolve $1 to the real file path it refers to when it is a symlink, so writes
# land on the target and preserve the link rather than replacing it. Falls back
# to $1 when readlink is unavailable or $1 is not a symlink.
_aenv_cc_resolve() {
    if [[ -L "$1" ]]; then
        readlink -f "$1" 2>/dev/null || printf '%s' "$1"
    else
        printf '%s' "$1"
    fi
}

# Atomically publish a staged temp file as <dest>.
#   $1 temp file path — MUST live in the same directory as <dest> (caller's job)
#      so the final rename is atomic and not a cross-filesystem copy+delete.
#   $2 destination path (possibly a symlink; its target is replaced, the link
#      itself is preserved). The destination's current mode is copied onto the
#      temp first (0644 default for a new file).
# Returns nonzero on failure; the caller is responsible for cleaning up the temp.
_aenv_cc_commit() {
    local tmp="$1" dest="$2"
    local target
    target="$(_aenv_cc_resolve "$dest")"
    chmod "$(_aenv_cc_mode_octal "$target")" "$tmp" 2>/dev/null || chmod 0644 "$tmp" 2>/dev/null || true
    mv -f "$tmp" "$target" 2>/dev/null
}

# Write a single completion loader file atomically. Non-fatal on I/O errors.
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
    local tmp
    tmp="$(mktemp "${path}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s\n' "$path" >&2
        return 0
    }
    if printf '%s\n' "$content" > "$tmp" 2>/dev/null && _aenv_cc_commit "$tmp" "$path"; then
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

# Append the regenerating zsh rc-snippet, idempotently and atomically. A
# complete, well-formed block already present => no-op. Any marker present but
# malformed => warn and leave it for the user (auto-repair could delete
# unrelated rc lines). No markers => append. The full new rc (existing content
# + managed block) is staged to a same-directory temp and committed by an atomic
# rename, so an interruption or I/O failure never leaves a partial/malformed
# block in the live rc. The appended block is guarded by `command -v aenv` so a
# missing/broken aenv never emits errors on every shell start.
#   $1 rc file path
_aenv_cc_put_zsh_rc() {
    local rc="$1"
    local dir="${rc%/*}"
    if [[ -f "$rc" ]] && grep -qE '^# (>>> aenv completion >>>|<<< aenv completion <<<)$' "$rc" 2>/dev/null; then
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
    local target tmp last_byte
    target="$(_aenv_cc_resolve "$rc")"
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
    if ! {
        printf '# >>> aenv completion >>>\n'
        printf 'if command -v aenv >/dev/null 2>&1; then\n'
        printf 'autoload -Uz compinit && compinit\n'
        # shellcheck disable=SC2016 # $(...) is literal text for zsh to eval, not bash
        printf 'eval "$(aenv completion zsh)"\n'
        printf 'fi\n'
        printf '# <<< aenv completion <<<\n'
    } >> "$tmp" 2>/dev/null; then
        rm -f "$tmp" 2>/dev/null
        printf 'warn: aenv completion: could not stage %s\n' "$rc" >&2
        return 0
    fi
    _aenv_cc_commit "$tmp" "$rc" || {
        rm -f "$tmp" 2>/dev/null
        printf 'warn: aenv completion: could not update %s\n' "$rc" >&2
    }
}

# Generate the static zsh completion into a site-functions dir. $2 is the
# just-installed aenv binary (preferred over whatever is on PATH, which may be
# stale or absent). Generation goes through `_aenv_cc_commit`, so a failure or
# empty output never replaces an existing valid completion file.
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
    tmp="$(mktemp "${path}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s; skipping static zsh\n' "$path" >&2
        return 0
    }
    # Require non-empty output: a broken aenv that exits 0 with no bytes must not
    # erase a working completion via the atomic rename.
    if "${gen[@]}" > "$tmp" 2>/dev/null && [[ -s "$tmp" ]] && _aenv_cc_commit "$tmp" "$path"; then
        chmod 0644 "$path" 2>/dev/null || true
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    # shellcheck disable=SC2016 # backticks are literal text in a warning
    printf 'warn: aenv completion: `aenv completion zsh` failed or produced no output; skipping %s\n' "$path" >&2
}

# Remove every well-formed aenv marker block from ~/.zshrc. Refuses to touch a
# file with a malformed (partial/nested/reordered/orphan) block. The rewrite is
# staged to a same-directory temp and committed by an atomic rename via
# `_aenv_cc_commit`, so the rc's inode/mode/ownership and (for a symlinked rc)
# the link itself are preserved and a failed/partial awk never reaches the live
# file.
#   $1 rc file path
_aenv_cc_rm_zsh_rc() {
    local rc="$1"
    [[ -f "$rc" ]] || return 0
    grep -qE '^# (>>> aenv completion >>>|<<< aenv completion <<<)$' "$rc" 2>/dev/null || return 0
    if ! _aenv_cc_rc_well_formed "$rc"; then
        printf 'warn: aenv completion: malformed marker block in %s; leaving it untouched\n' "$rc" >&2
        return 0
    fi
    local target tmp
    target="$(_aenv_cc_resolve "$rc")"
    # Temp in the rc's own directory so the rename is atomic (same filesystem).
    tmp="$(mktemp "${target}.XXXXXX" 2>/dev/null)" || {
        printf 'warn: aenv completion: could not create temp file near %s; leaving it untouched\n' "$rc" >&2
        return 0
    }
    # awk's exit status is the signal: on success its output (possibly empty if
    # the rc held only the managed block) is the correct new content; on failure
    # (read/parse error) it is left partial and we never commit it.
    if awk '/^# >>> aenv completion >>>$/,/^# <<< aenv completion <<<$/ { next } { print }' "$target" > "$tmp" 2>/dev/null && _aenv_cc_commit "$tmp" "$rc"; then
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

usage() {
    cat <<'EOF'
Usage: shell-completion.sh <install|uninstall> [--prefix=<P>] [--user]

Install or remove regenerating shell-completion loaders (bash, zsh, fish) for
the aenv CLI. See the header comment for destination details.
EOF
}

if [[ $# -lt 1 ]]; then
    usage >&2
    exit 2
fi

aenv_completion_install "$@"
