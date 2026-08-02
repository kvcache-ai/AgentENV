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

# A PATH containing only the utilities the helper needs and NO aenv, so the
# "aenv missing" case is hermetic regardless of what is installed on the host
# (no reliance on /usr/bin/aenv or /bin/aenv existing or not). `bash` is
# included so the test can launch the helper under this restricted PATH.
hermetic_bin="$tmp_root/hermetic-bin"
mkdir -p "$hermetic_bin"
for u in bash mkdir grep awk tail od tr mktemp cat chmod rm mv; do
    ln -s "$(command -v "$u")" "$hermetic_bin/$u"
done

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_contains() { # file needle
    [[ -f "$1" ]] || fail "expected file $1 to exist"
    grep -q -- "$2" "$1" || fail "expected $1 to contain: $2"
}
assert_absent() { # path
    [[ ! -e "$1" ]] || fail "expected $1 to be absent, but it exists"
}
assert_rc_clean() { # rc-file
    [[ ! -f "$1" ]] || ! grep -Eq '^# (>>> aenv completion >>>|<<< aenv completion <<<)$' "$1" \
        || fail "expected no aenv markers in $1"
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
# Test 5: every malformed marker layout is left byte-for-byte untouched by both
# install and uninstall (orphan start/end, reversed, nested). Install must not
# append onto a malformed state; uninstall must not truncate it.
# ---------------------------------------------------------------------------
echo "==> malformed marker layouts are untouched by install and uninstall"
home_mal="$tmp_root/home-mal"; mkdir -p "$home_mal"
layouts=(
    'orphan-start|user-before\n# >>> aenv completion >>>\nuser-after\n'
    'orphan-end|user-before\n# <<< aenv completion <<<\nuser-after\n'
    'reversed|# <<< aenv completion <<<\nuser-mid\n# >>> aenv completion >>>\n'
    'nested|# >>> aenv completion >>>\n# >>> aenv completion >>>\nx\n# <<< aenv completion <<<\n# <<< aenv completion <<<\n'
)
for entry in "${layouts[@]}"; do
    name="${entry%%|*}"; body="${entry#*|}"
    rc="$home_mal/.zshrc"
    printf '%b' "$body" > "$rc"
    cp "$rc" "$rc.orig"
    HOME="$home_mal" bash "$helper" install --user 2>/dev/null
    cmp -s "$rc" "$rc.orig" || fail "install mutated malformed ($name) rc"
    HOME="$home_mal" bash "$helper" uninstall --user 2>/dev/null
    cmp -s "$rc" "$rc.orig" || fail "uninstall mutated malformed ($name) rc"
    rm -f "$rc" "$rc.orig"
done

# ---------------------------------------------------------------------------
# Test 6: system mode skips the static zsh file when aenv is not on PATH but
# still writes the bash/fish stubs (graceful degradation, non-fatal).
# ---------------------------------------------------------------------------
echo "==> system mode without aenv on PATH skips only the static zsh file"
# Hermetic PATH: only the utilities the helper needs, no aenv anywhere.
HOME="$fake_home" PATH="$hermetic_bin" bash "$helper" install --prefix="$sys_prefix" 2>/dev/null
assert_contains "$sys_prefix/share/bash-completion/completions/aenv" 'source <(aenv completion bash)'
assert_contains "$sys_prefix/share/fish/vendor_completions.d/aenv.fish" 'aenv completion fish | source'
assert_absent "$sys_prefix/share/zsh/site-functions/_aenv"
HOME="$fake_home" PATH="$hermetic_bin" bash "$helper" uninstall --prefix="$sys_prefix"
assert_absent "$sys_prefix/share/bash-completion/completions/aenv"
assert_absent "$sys_prefix/share/fish/vendor_completions.d/aenv.fish"

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

# ---------------------------------------------------------------------------
# Test 8: a failing `aenv completion zsh` must not truncate an existing valid
# static file (atomic temp+rename), and must not leave temp files behind.
# ---------------------------------------------------------------------------
echo "==> failing aenv completion zsh leaves the existing managed _aenv intact"
mkdir -p "$sys_prefix/bin" "$sys_prefix/share/zsh/site-functions"
invoked="$tmp_root/failing-aenv-invoked"
# Seed a managed _aenv with a working aenv first, so the install-side ownership
# guard permits a later re-install attempt; then swap to a failing aenv.
# shellcheck disable=SC2016 # ${1:-} is literal stub text
printf '#!/usr/bin/env bash\ncase "${1:-}" in completion) echo "#compdef aenv"; echo "echo body";; esac\n' > "$sys_prefix/bin/aenv"
chmod +x "$sys_prefix/bin/aenv"
HOME="$fake_home" bash "$helper" install --prefix="$sys_prefix" >/dev/null 2>&1
assert_contains "$sys_prefix/share/zsh/site-functions/_aenv" '#compdef aenv'
cp "$sys_prefix/share/zsh/site-functions/_aenv" "$tmp_root/_aenv.orig"
printf '#!/usr/bin/env bash\nprintf x > "%s"\nexit 1\n' "$invoked" > "$sys_prefix/bin/aenv"
chmod +x "$sys_prefix/bin/aenv"
rm -f "$invoked"
HOME="$fake_home" PATH="$hermetic_bin" bash "$helper" install --prefix="$sys_prefix" 2>/dev/null \
    || fail "helper aborted when aenv completion zsh exits nonzero"
[[ -f "$invoked" ]] || fail "prefix-local aenv stub was not invoked on re-install"
cmp -s "$sys_prefix/share/zsh/site-functions/_aenv" "$tmp_root/_aenv.orig" \
    || fail "failed generation modified the existing _aenv"
leftovers=( "$sys_prefix/share/zsh/site-functions"/* )
[[ "${#leftovers[@]}" -eq 1 ]] || fail "expected no temp leftovers, found: ${leftovers[*]}"
rm -rf "${sys_prefix:?}/bin" "${sys_prefix:?}/share"

# ---------------------------------------------------------------------------
# Test 8e: install must NOT overwrite an existing completion file the installer
# did not create (install-side ownership guard, symmetric with uninstall).
# ---------------------------------------------------------------------------
echo "==> install leaves an existing non-aenv-owned completion file untouched"
mkdir -p "$sys_prefix/share/bash-completion/completions" "$sys_prefix/share/fish/vendor_completions.d"
printf '# my hand-written bash completion\n' > "$sys_prefix/share/bash-completion/completions/aenv"
printf '# my fish completion\n' > "$sys_prefix/share/fish/vendor_completions.d/aenv.fish"
HOME="$fake_home" bash "$helper" install --prefix="$sys_prefix" 2>/dev/null
assert_contains "$sys_prefix/share/bash-completion/completions/aenv" 'my hand-written'
assert_contains "$sys_prefix/share/fish/vendor_completions.d/aenv.fish" 'my fish completion'
rm -rf "${sys_prefix:?}/share"

# ---------------------------------------------------------------------------
# Test 8b: a zero-exit-but-empty `aenv completion zsh` must NOT replace a valid
# existing _aenv (the installer rejects an empty generated temp before rename).
# ---------------------------------------------------------------------------
echo "==> empty aenv completion zsh output leaves the existing managed _aenv intact"
mkdir -p "$sys_prefix/bin" "$sys_prefix/share/zsh/site-functions"
# Seed managed _aenv (working aenv), then swap to an aenv that exits 0 with no bytes.
# shellcheck disable=SC2016 # ${1:-} is literal stub text
printf '#!/usr/bin/env bash\ncase "${1:-}" in completion) echo "#compdef aenv"; echo "echo body";; esac\n' > "$sys_prefix/bin/aenv"
chmod +x "$sys_prefix/bin/aenv"
HOME="$fake_home" bash "$helper" install --prefix="$sys_prefix" >/dev/null 2>&1
cp "$sys_prefix/share/zsh/site-functions/_aenv" "$tmp_root/_aenv8b.orig"
printf '#!/usr/bin/env bash\nexit 0\n' > "$sys_prefix/bin/aenv"
chmod +x "$sys_prefix/bin/aenv"
HOME="$fake_home" PATH="$hermetic_bin" bash "$helper" install --prefix="$sys_prefix" 2>/dev/null \
    || fail "helper aborted on empty completion output"
cmp -s "$sys_prefix/share/zsh/site-functions/_aenv" "$tmp_root/_aenv8b.orig" \
    || fail "empty output replaced a valid _aenv"
rm -rf "${sys_prefix:?}/bin" "${sys_prefix:?}/share"

# ---------------------------------------------------------------------------
# Test 8c: the static zsh file keeps #compdef on line 1 (the ownership marker
# is appended, not prepended, so zsh still loads the function) and carries the
# ownership marker so a later uninstall can recognize it.
# ---------------------------------------------------------------------------
echo "==> static zsh keeps #compdef first-line and carries the ownership marker"
mkdir -p "$sys_prefix/bin" "$sys_prefix/share/zsh/site-functions"
# shellcheck disable=SC2016 # ${1:-} is literal text for the stub script, not this shell
printf '#!/usr/bin/env bash\ncase "${1:-}" in completion) echo "#compdef aenv"; echo "echo body";; esac\n' > "$sys_prefix/bin/aenv"
chmod +x "$sys_prefix/bin/aenv"
HOME="$fake_home" bash "$helper" install --prefix="$sys_prefix" >/dev/null 2>&1
[[ "$(head -1 "$sys_prefix/share/zsh/site-functions/_aenv")" == "#compdef aenv" ]] \
    || fail "static zsh #compdef must be the first line"
grep -qF -- "# managed by aenv-installer" "$sys_prefix/share/zsh/site-functions/_aenv" \
    || fail "static zsh must carry the ownership marker"
HOME="$fake_home" bash "$helper" uninstall --prefix="$sys_prefix" >/dev/null 2>&1
assert_absent "$sys_prefix/share/zsh/site-functions/_aenv"
rm -rf "${sys_prefix:?}/bin"

# ---------------------------------------------------------------------------
# Test 8d: uninstall will NOT delete a completion file the installer did not
# create (no ownership marker) — a hand-maintained or package-manager file at
# the conventional path is left untouched with a warning.
# ---------------------------------------------------------------------------
echo "==> uninstall leaves a non-aenv-owned completion file untouched"
mkdir -p "$sys_prefix/share/bash-completion/completions" "$sys_prefix/share/fish/vendor_completions.d"
printf '# my hand-written aenv completion\n' > "$sys_prefix/share/bash-completion/completions/aenv"
printf '# my fish completion\n' > "$sys_prefix/share/fish/vendor_completions.d/aenv.fish"
HOME="$fake_home" bash "$helper" uninstall --prefix="$sys_prefix" 2>/dev/null
assert_contains "$sys_prefix/share/bash-completion/completions/aenv" 'my hand-written'
assert_contains "$sys_prefix/share/fish/vendor_completions.d/aenv.fish" 'my fish completion'
rm -rf "${sys_prefix:?}/share"


echo "==> unrelated rc content survives install and uninstall"
home_surround="$tmp_root/home-surround"
mkdir -p "$home_surround"
zsrc="$home_surround/.zshrc"
printf 'alias-before=1\n' > "$zsrc"
HOME="$home_surround" bash "$helper" install --user
printf 'alias-after=2\n' >> "$zsrc"
HOME="$home_surround" bash "$helper" uninstall --user
assert_contains "$zsrc" 'alias-before=1'
assert_contains "$zsrc" 'alias-after=2'
assert_rc_clean "$zsrc"

# ---------------------------------------------------------------------------
# Test 10: user mode requested with HOME unset warns and skips (no abort under
# set -u); closes the non-fatal contract for the user-mode destination paths.
# ---------------------------------------------------------------------------
echo "==> user mode with unset HOME warns and skips without aborting"
home_unset_out="$tmp_root/home-unset.out"
env -u HOME bash "$helper" install --user >"$home_unset_out" 2>&1 \
    || fail "helper aborted during install --user with HOME unset"
grep -q 'HOME is unset' "$home_unset_out" || fail "expected a HOME-unset warning during install"
env -u HOME bash "$helper" uninstall --user >"$home_unset_out" 2>&1 \
    || fail "helper aborted during uninstall --user with HOME unset"
grep -q 'HOME is unset' "$home_unset_out" || fail "expected a HOME-unset warning during uninstall"

echo "==> all shell-completion checks passed"
