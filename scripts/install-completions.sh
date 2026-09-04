#!/usr/bin/env bash
# Install or remove aenv completion loaders without editing shell startup files.
set -euo pipefail

MARKER='# managed by aenv completion installer'

usage() {
    printf 'Usage: %s <install|uninstall> --prefix=<prefix> --binary=<aenv>\n' "$0" >&2
}

action="${1:-}"
shift || true
prefix=""
binary=""
while (($#)); do
    case "$1" in
        --prefix=*) prefix="${1#--prefix=}" ;;
        --binary=*) binary="${1#--binary=}" ;;
        *) usage; exit 2 ;;
    esac
    shift
done

if [[ "$action" != install && "$action" != uninstall ]] || [[ -z "$prefix" ]] ||
   [[ "$action" == install && -z "$binary" ]]; then
    usage
    exit 2
fi

home="${HOME:-}"
user_mode=0
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

prefix_real="$(canonical_dir "$prefix")" || {
    printf 'warning: could not resolve completion prefix %s; skipping\n' "$prefix" >&2
    exit 0
}
home_real=""
if [[ -n "$home" ]]; then
    home_real="$(canonical_dir "$home")" || home_real=""
fi
if [[ -n "$home_real" && ( "$prefix_real" == "$home_real" || "$prefix_real" == "$home_real"/* ) ]]; then
    user_mode=1
fi

if ((user_mode)); then
    [[ -n "$home" ]] || { printf 'warning: HOME is unset; skipping completion setup\n' >&2; exit 0; }
    bash_path="$home/.local/share/bash-completion/completions/aenv"
    zsh_path="$home/.local/share/zsh/site-functions/_aenv"
    fish_path="$home/.config/fish/completions/aenv.fish"
else
    bash_path="$prefix/share/bash-completion/completions/aenv"
    zsh_path="$prefix/share/zsh/site-functions/_aenv"
    fish_path="$prefix/share/fish/vendor_completions.d/aenv.fish"
fi

write_loader() (
    local path="$1" body="$2" dir tmp lock_dir
    dir="${path%/*}"
    mkdir -p "$dir" 2>/dev/null || {
        printf 'warning: could not create completion directory %s\n' "$dir" >&2
        return 0
    }
    lock_dir="$dir/.aenv-completion.lock"
    if ! mkdir "$lock_dir" 2>/dev/null; then
        printf 'warning: completion directory is busy; skipping %s\n' "$path" >&2
        return 0
    fi
    trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT
    if [[ -L "$path" ]]; then
        printf 'warning: refusing to replace symlink %s\n' "$path" >&2
        return 0
    fi
    if [[ -e "$path" ]] && ! grep -Fqx "$MARKER" "$path" 2>/dev/null; then
        printf 'warning: leaving unmanaged completion file %s untouched\n' "$path" >&2
        return 0
    fi
    tmp="$(mktemp "$dir/.aenv-completion.XXXXXX")" || {
        printf 'warning: could not stage completion file %s\n' "$path" >&2
        return 0
    }
    if [[ "$path" == "$zsh_path" ]]; then
        if ! printf '%s\n%s\n' "$body" "$MARKER" >"$tmp"; then
            rm -f "$tmp"
            printf 'warning: could not stage completion file %s\n' "$path" >&2
            return 0
        fi
    else
        if ! printf '%s\n%s\n' "$MARKER" "$body" >"$tmp"; then
            rm -f "$tmp"
            printf 'warning: could not stage completion file %s\n' "$path" >&2
            return 0
        fi
    fi
    # Re-check immediately before replacement. The first check protects normal
    # races between installers; this check protects a user file created while
    # the temporary content was being generated.
    if [[ -e "$path" ]] && ! grep -Fqx "$MARKER" "$path" 2>/dev/null; then
        rm -f "$tmp"
        printf 'warning: leaving newly-created unmanaged completion file %s untouched\n' "$path" >&2
        return 0
    fi
    if ! { chmod 0644 "$tmp" && mv -f "$tmp" "$path"; }; then
        rm -f "$tmp"
        printf 'warning: could not install completion file %s\n' "$path" >&2
    fi
)

remove_loader() {
    local path="$1"
    [[ -e "$path" ]] || return 0
    if [[ -L "$path" ]] || ! grep -Fqx "$MARKER" "$path" 2>/dev/null; then
        printf 'warning: leaving unmanaged completion file %s untouched\n' "$path" >&2
        return 0
    fi
    rm -f "$path" || printf 'warning: could not remove completion file %s\n' "$path" >&2
}

if [[ "$action" == install ]]; then
    case "$binary" in
        *"'"*|*"\\"*|*$'\n'*|*$'\r'*)
            printf 'warning: completion binary path contains unsupported characters; skipping\n' >&2
            exit 0
            ;;
    esac
    [[ -x "$binary" ]] || {
        printf 'warning: completion binary is not executable: %s\n' "$binary" >&2
        exit 0
    }
    binary_dir="${binary%/*}"
    [[ "$binary_dir" == "$binary" ]] && binary_dir="."
    binary="$(cd "$binary_dir" && pwd -P)/${binary##*/}"
    quoted_binary="'$binary'"
    write_loader "$bash_path" "if [[ -x $quoted_binary ]]; then source <($quoted_binary completion bash); fi"
    zsh_body="#compdef aenv
if [[ -x $quoted_binary ]]; then eval \"\$($quoted_binary completion zsh)\"; fi"
    write_loader "$zsh_path" "$zsh_body"
    write_loader "$fish_path" "if test -x $quoted_binary; $quoted_binary completion fish | source; end"
    if ((user_mode)); then
        printf '\nIf Zsh does not find the completion, add this before compinit:\n'
        printf "  fpath=(~/.local/share/zsh/site-functions \$fpath)\n"
        printf '  autoload -Uz compinit\n  compinit\n'
    fi
else
    remove_loader "$bash_path"
    remove_loader "$zsh_path"
    remove_loader "$fish_path"
fi
