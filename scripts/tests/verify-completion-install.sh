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

echo '==> user install creates standard loaders'
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
assert_file "$bash_file"
assert_file "$zsh_file"
assert_file "$fish_file"
assert_contains "$bash_file" "$marker"
assert_contains "$zsh_file" '#compdef aenv'
[[ "$(sed -n '1p' "$zsh_file")" == '#compdef aenv' ]] || fail 'zsh #compdef must remain first'
[[ ! -e "$home/.zshrc" ]] || fail 'installer must not create .zshrc'

echo '==> unmanaged files are preserved'
printf '# user completion\n' > "$bash_file"
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_contains "$bash_file" '# user completion'

echo '==> managed files are upgraded'
rm -f "$bash_file"
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v1" >/dev/null
HOME="$home" bash "$helper" install --prefix="$home/.local" --binary="$bin/aenv-v2" >/dev/null
assert_contains "$bash_file" "$bin/aenv-v2"

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

echo '==> completion installation checks passed'
