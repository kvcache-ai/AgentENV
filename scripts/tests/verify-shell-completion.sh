#!/usr/bin/env bash
# Functional test for the aenv shell-completion loader installer.
#
# Does not require a real aenv binary: a stub `aenv` is placed on PATH so the
# static-zsh generation path is exercised end-to-end. Run via
# `make check-shell-completion` or directly with `bash scripts/tests/verify-shell-completion.sh`.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
helper="$repo_root/scripts/shell-completion.sh"

tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

fake_home="$tmp_root/home"
fake_bin="$tmp_root/bin"
sys_prefix="$tmp_root/sys"
mkdir -p "$fake_home" "$fake_bin" "$sys_prefix"

# Stub aenv so `aenv completion <shell>` succeeds during static-zsh generation.
cat > "$fake_bin/aenv" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
    completion) echo "# fake aenv completion for ${2:-?}" ;;
    *) echo "fake aenv" ;;
esac
EOF
chmod +x "$fake_bin/aenv"
export PATH="$fake_bin:$PATH"

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_contains() { # file needle
    [[ -f "$1" ]] || fail "expected file $1 to exist"
    grep -q -- "$2" "$1" || fail "expected $1 to contain: $2"
}
assert_absent() { # path
    [[ ! -e "$1" ]] || fail "expected $1 to be absent, but it exists"
}
assert_rc_clean() { # rc-file
    [[ ! -f "$1" ]] || ! grep -q '^# >>> aenv completion >>>$' "$1" \
        || fail "expected no aenv marker block in $1"
}
marker_count() { # rc-file -> count
    if [[ -f "$1" ]]; then
        grep -c '^# >>> aenv completion >>>$' "$1" || true
    else
        echo 0
    fi
}

# ---------------------------------------------------------------------------
# Test 1: user mode
# ---------------------------------------------------------------------------
echo "==> user-mode install"
HOME="$fake_home" bash "$helper" install --user
bash_file="$fake_home/.local/share/bash-completion/completions/aenv"
fish_file="$fake_home/.config/fish/completions/aenv.fish"
zshrc="$fake_home/.zshrc"
assert_contains "$bash_file" 'source <(aenv completion bash)'
assert_contains "$fish_file" 'aenv completion fish | source'
# shellcheck disable=SC2016 # searching for a literal $(...) string in the rc
assert_contains "$zshrc" 'eval "$(aenv completion zsh)"'
[[ "$(marker_count "$zshrc")" == "1" ]] || fail "expected exactly one marker block after install"

echo "==> user-mode install is idempotent"
HOME="$fake_home" bash "$helper" install --user
[[ "$(marker_count "$zshrc")" == "1" ]] || fail "re-install appended a duplicate marker block"

echo "==> user-mode uninstall"
HOME="$fake_home" bash "$helper" uninstall --user
assert_absent "$bash_file"
assert_absent "$fish_file"
assert_rc_clean "$zshrc"

# ---------------------------------------------------------------------------
# Test 2: system mode
# ---------------------------------------------------------------------------
echo "==> system-mode install"
HOME="$fake_home" bash "$helper" install --prefix="$sys_prefix"
sys_bash="$sys_prefix/share/bash-completion/completions/aenv"
sys_fish="$sys_prefix/share/fish/vendor_completions.d/aenv.fish"
sys_zsh="$sys_prefix/share/zsh/site-functions/_aenv"
assert_contains "$sys_bash" 'source <(aenv completion bash)'
assert_contains "$sys_fish" 'aenv completion fish | source'
assert_contains "$sys_zsh" '# fake aenv completion for zsh'
assert_rc_clean "$zshrc" # system mode must NOT edit the user rc

echo "==> system-mode uninstall"
HOME="$fake_home" bash "$helper" uninstall --prefix="$sys_prefix"
assert_absent "$sys_bash"
assert_absent "$sys_fish"
assert_absent "$sys_zsh"

# ---------------------------------------------------------------------------
# Test 3: auto-detection from a prefix under $HOME behaves like user mode
# ---------------------------------------------------------------------------
echo "==> prefix-under-HOME selects user mode"
HOME="$fake_home" bash "$helper" install --prefix="$fake_home/.local"
assert_contains "$fake_home/.local/share/bash-completion/completions/aenv" 'source <(aenv completion bash)'
# shellcheck disable=SC2016 # searching for a literal $(...) string in the rc
assert_contains "$zshrc" 'eval "$(aenv completion zsh)"' # rc-snippet, not a static file
assert_absent "$fake_home/.local/share/zsh/site-functions/_aenv" # no static file in user mode
HOME="$fake_home" bash "$helper" uninstall --prefix="$fake_home/.local"
assert_absent "$fake_home/.local/share/bash-completion/completions/aenv"
assert_rc_clean "$zshrc"

# ---------------------------------------------------------------------------
# Test 4: uninstall is a no-op when nothing is installed (and never fails)
# ---------------------------------------------------------------------------
echo "==> uninstall on a clean tree is a no-op"
HOME="$fake_home" bash "$helper" uninstall --user
HOME="$fake_home" bash "$helper" uninstall --prefix="$sys_prefix"

# ---------------------------------------------------------------------------
# Test 5: an unbalanced marker block is left untouched (never truncate a rc)
# ---------------------------------------------------------------------------
echo "==> unbalanced markers are left untouched on uninstall"
malformed="$fake_home/.zshrc"
printf 'user-line-before\n# >>> aenv completion >>>\nautoload -Uz compinit\nuser-line-after\n' > "$malformed"
HOME="$fake_home" bash "$helper" uninstall --user 2>/dev/null
# Nothing is removed: both user lines and the orphan start marker remain.
assert_contains "$malformed" 'user-line-before'
assert_contains "$malformed" 'user-line-after'
assert_contains "$malformed" '# >>> aenv completion >>>'
rm -f "$malformed"

# ---------------------------------------------------------------------------
# Test 6: system mode skips the static zsh file when aenv is not on PATH but
# still writes the bash/fish stubs (graceful degradation, non-fatal).
# ---------------------------------------------------------------------------
echo "==> system mode without aenv on PATH skips only the static zsh file"
# A PATH that contains neither the fake aenv nor any other aenv.
HOME="$fake_home" PATH="/usr/bin:/bin" bash "$helper" install --prefix="$sys_prefix" 2>/dev/null
assert_contains "$sys_prefix/share/bash-completion/completions/aenv" 'source <(aenv completion bash)'
assert_contains "$sys_prefix/share/fish/vendor_completions.d/aenv.fish" 'aenv completion fish | source'
assert_absent "$sys_prefix/share/zsh/site-functions/_aenv"
HOME="$fake_home" PATH="/usr/bin:/bin" bash "$helper" uninstall --prefix="$sys_prefix"
assert_absent "$sys_prefix/share/bash-completion/completions/aenv"

# ---------------------------------------------------------------------------
# Test 7: a missing $HOME must not abort the helper (set -u) and must not make
# an absolute system prefix match the "$prefix" == "$HOME"/* glob. Regression
# guard for the empty-HOME mode-detection bug.
# ---------------------------------------------------------------------------
echo "==> missing HOME with system prefix stays in system mode and does not abort"
env -u HOME bash "$helper" install --prefix="$sys_prefix" >/dev/null 2>&1 \
    || fail "helper aborted under set -u when HOME is unset"
assert_contains "$sys_prefix/share/bash-completion/completions/aenv" 'source <(aenv completion bash)'
env -u HOME bash "$helper" uninstall --prefix="$sys_prefix" >/dev/null 2>&1
assert_absent "$sys_prefix/share/bash-completion/completions/aenv"

echo "==> all shell-completion checks passed"
