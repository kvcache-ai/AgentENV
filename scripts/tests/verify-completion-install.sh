#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
helper="$repo_root/scripts/install-completions.sh"
tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

home="$tmp_root/home"
bin="$tmp_root/bin"
prefix="$tmp_root/prefix"
mkdir -p "$home" "$bin" "$prefix"

printf '#!/usr/bin/env bash\necho completion\n' > "$bin/aenv-v1"
printf '#!/usr/bin/env bash\necho newer-completion\n' > "$bin/aenv-v2"
chmod 0755 "$bin/aenv-v1" "$bin/aenv-v2"

bash_file="$home/.local/share/bash-completion/completions/aenv"
zsh_file="$home/.local/share/zsh/site-functions/_aenv"
fish_file="$home/.config/fish/completions/aenv.fish"
marker='# managed by aenv completion installer'

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
assert_file() { [[ -f "$1" ]] || fail "expected file $1"; }
assert_absent() { [[ ! -e "$1" ]] || fail "expected $1 to be absent"; }
assert_contains() { grep -Fq -- "$2" "$1" || fail "expected $1 to contain: $2"; }
file_mode() {
    stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"
}

echo '==> user install creates standard loaders'
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
assert_file "$bash_file"
assert_file "$zsh_file"
assert_file "$fish_file"
assert_contains "$bash_file" "$marker"
assert_contains "$zsh_file" '#compdef aenv'
[[ "$(sed -n '1p' "$zsh_file")" == '#compdef aenv' ]] || fail 'zsh #compdef must remain first'
[[ "$(file_mode "$bash_file")" == 644 ]] || fail 'Bash loader mode must be 0644'
[[ "$(file_mode "$zsh_file")" == 644 ]] || fail 'Zsh loader mode must be 0644'
[[ "$(file_mode "$fish_file")" == 644 ]] || fail 'Fish loader mode must be 0644'
[[ ! -e "$home/.zshrc" ]] || fail 'installer must not create .zshrc'

if command -v zsh >/dev/null 2>&1; then
    zsh -n "$zsh_file"
fi
if command -v fish >/dev/null 2>&1; then
    fish -n "$fish_file"
fi
bash -n "$bash_file"

echo '==> unmanaged files are preserved'
printf '# user completion\n' > "$bash_file"
printf '# user zsh completion\n' > "$zsh_file"
printf '# user fish completion\n' > "$fish_file"
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_contains "$bash_file" '# user completion'
assert_contains "$zsh_file" '# user zsh completion'
assert_contains "$fish_file" '# user fish completion'

echo '==> uninstall preserves unmanaged files'
HOME="$home" bash "$helper" uninstall --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_contains "$bash_file" '# user completion'
assert_contains "$zsh_file" '# user zsh completion'
assert_contains "$fish_file" '# user fish completion'

echo '==> managed files are upgraded'
rm -f "$bash_file" "$zsh_file" "$fish_file"
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_contains "$bash_file" "$bin/aenv-v2"
[[ "$(file_mode "$bash_file")" == 644 ]] || fail 'upgraded loader mode must be 0644'

echo '==> uninstall removes only managed files'
HOME="$home" bash "$helper" uninstall --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_absent "$bash_file"
assert_absent "$zsh_file"
assert_absent "$fish_file"

echo '==> symlink destinations are preserved'
symlink_target="$tmp_root/user-completion"
printf '# user target\n' > "$symlink_target"
ln -s "$symlink_target" "$bash_file"
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
[[ -L "$bash_file" ]] || fail 'completion symlink was replaced'
assert_contains "$symlink_target" '# user target'
rm -f "$bash_file"

echo '==> staging failures leave no temporary files'
fake_tools="$tmp_root/fake-tools"
mkdir -p "$fake_tools"
printf '#!/usr/bin/env bash\nexit 1\n' > "$fake_tools/mktemp"
chmod 0755 "$fake_tools/mktemp"
HOME="$home" PATH="$fake_tools:$PATH" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
shopt -s nullglob
leftovers=(
    "$home/.local/share/bash-completion/completions"/.aenv-completion.*
    "$home/.local/share/zsh/site-functions"/.aenv-completion.*
    "$home/.config/fish/completions"/.aenv-completion.*
)
[[ "${#leftovers[@]}" -eq 0 ]] || fail 'staging failure left a temporary completion file'

echo '==> system paths use the prefix share directories'
HOME="$home" bash "$helper" install --prefix="$prefix" --binary="$bin/aenv-v1" >/dev/null
assert_file "$prefix/share/bash-completion/completions/aenv"
assert_file "$prefix/share/zsh/site-functions/_aenv"
assert_file "$prefix/share/fish/vendor_completions.d/aenv.fish"
HOME="$home" bash "$helper" uninstall --prefix="$prefix" --binary="$bin/aenv-v1" >/dev/null
assert_absent "$prefix/share/bash-completion/completions/aenv"

echo '==> path traversal cannot select user mode'
outside="$tmp_root/outside"
mkdir -p "$outside"
HOME="$home" bash "$helper" install --prefix="$home/../outside" --binary="$bin/aenv-v1" >/dev/null
assert_file "$outside/share/bash-completion/completions/aenv"
assert_absent "$home/.local/share/bash-completion/completions/aenv"
HOME="$home" bash "$helper" uninstall --prefix="$home/../outside" --binary="$bin/aenv-v1" >/dev/null
assert_absent "$outside/share/bash-completion/completions/aenv"

echo '==> unsupported loader paths are rejected'
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1\\tail" >/dev/null
assert_absent "$bash_file"
HOME="$home" bash "$helper" uninstall --prefix="$home/.local" >/dev/null

echo '==> release installers retain the shared safety hardening'
for installer in "$repo_root/scripts/install-cli.sh" "$repo_root/scripts/install.sh"; do
    assert_contains "$installer" '.aenv-completion.lock'
    assert_contains "$installer" 'leaving newly-created unmanaged completion file'
done

echo '==> completion installation checks passed'
