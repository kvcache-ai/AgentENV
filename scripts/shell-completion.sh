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

# Append the regenerating zsh rc-snippet to ~/.zshrc, idempotently.
#   $1 rc file path
_aenv_cc_put_zsh_rc() {
    local rc="$1"
    local dir="${rc%/*}"
    if [[ -f "$rc" ]] && grep -q '^# >>> aenv completion >>>$' "$rc"; then
        return 0 # already installed
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

# Generate the static zsh completion from the installed aenv into a
# site-functions dir. Skipped (with a warning) if `aenv` is not on PATH, e.g.
# when installing into a prefix that is not yet on PATH; re-running the
# installer after fixing PATH regenerates it.
#   $1 destination _aenv path
_aenv_cc_put_zsh_static() {
    local path="$1"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    if ! command -v aenv >/dev/null 2>&1; then
        printf 'warn: aenv completion: aenv not on PATH; skipping static zsh file %s\n' "$path" >&2
        return 0
    fi
    if ! aenv completion zsh > "$path" 2>/dev/null; then
        # shellcheck disable=SC2016 # backticks are literal text in a warning
        printf 'warn: aenv completion: `aenv completion zsh` failed; skipping %s\n' "$path" >&2
        return 0
    fi
    chmod 0644 "$path" 2>/dev/null || true
}

# Remove the zsh rc-snippet block from ~/.zshrc. Only acts on a balanced
# marker pair; an unbalanced pair is left untouched to avoid truncating the
# user's rc file.
#   $1 rc file path
_aenv_cc_rm_zsh_rc() {
    local rc="$1"
    [[ -f "$rc" ]] || return 0
    local starts ends tmp
    starts=$(grep -c '^# >>> aenv completion >>>$' "$rc" 2>/dev/null || true)
    ends=$(grep -c '^# <<< aenv completion <<<$' "$rc" 2>/dev/null || true)
    starts="${starts:-0}"
    ends="${ends:-0}"
    [[ "$starts" =~ ^[0-9]+$ ]] || starts=0
    [[ "$ends" =~ ^[0-9]+$ ]] || ends=0
    [[ "$starts" -gt 0 ]] || return 0
    if [[ "$starts" -ne "$ends" ]]; then
        printf 'warn: aenv completion: unbalanced markers in %s; leaving it untouched\n' "$rc" >&2
        return 0
    fi
    tmp="$(mktemp)"
    if awk '
        /^# >>> aenv completion >>>$/ { skip=1; next }
        /^# <<< aenv completion <<<$/ { skip=0; next }
        !skip { print }
    ' "$rc" > "$tmp" && mv -f "$tmp" "$rc" 2>/dev/null; then
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

    # Auto-select user mode for a bare invocation or a prefix under $HOME.
    # Guard $HOME: if it is unset/empty, the "$prefix" == "$HOME"/* pattern
    # would collapse to "/*" and match any absolute path, and the bare $HOME
    # reference would abort under `set -u`. Capture it once, safely.
    if [[ $user_mode -eq 0 ]]; then
        local home="${HOME:-}"
        if [[ -z "$prefix" || ( -n "$home" && ( "$prefix" == "$home" || "$prefix" == "$home"/* ) ) ]]; then
            user_mode=1
        fi
    fi

    local bash_file fish_file zsh_file zsh_kind
    if [[ $user_mode -eq 1 ]]; then
        bash_file="${HOME}/.local/share/bash-completion/completions/aenv"
        fish_file="${HOME}/.config/fish/completions/aenv.fish"
        zsh_file="${HOME}/.zshrc"
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
            _aenv_cc_put_zsh_static "$zsh_file"
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
