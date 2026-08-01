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
