#!/usr/bin/env bash
# Verify that the inlined `aenv_completion_install` blocks in the standalone
# installers stay in sync with the canonical copy in scripts/shell-completion.sh.
# Run from CI / `make check-shell-completion`.
#
# The canonical helper is a single source of truth; install-cli.sh and
# install.sh cannot source it (they are curl|bash'd as standalone scripts), so
# they inline a verbatim copy bracketed by the marker comments:
#
#     # BEGIN aenv_completion_install
#     ...
#     # END aenv_completion_install
#
# This script extracts that block from each file into a byte-preserving temp
# file (so trailing-newline differences are not silently normalized), validates
# each file has exactly one well-ordered BEGIN..END pair, and fails on any
# divergence. "In sync" means byte-identical block content across the files.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
canonical="$repo_root/scripts/shell-completion.sh"
cli="$repo_root/scripts/install-cli.sh"
full="$repo_root/scripts/install.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

# extract <input> <output>
# Copies the BEGIN..END block (inclusive) to <output>. Fails (exits 1) if the
# markers are absent, duplicated, or reversed.
extract() {
    local input="$1" out="$2"
    local begins ends first_begin last_end
    begins=$(grep -c '^# BEGIN aenv_completion_install$' "$input" || true)
    ends=$(grep -c '^# END aenv_completion_install$' "$input" || true)
    begins=${begins:-0}; ends=${ends:-0}
    [[ "$begins" =~ ^[0-9]+$ ]] || begins=0
    [[ "$ends" =~ ^[0-9]+$ ]] || ends=0
    if [[ "$begins" -ne 1 || "$ends" -ne 1 ]]; then
        echo "error: expected exactly one BEGIN and one END marker in $input (found $begins BEGIN, $ends END)" >&2
        return 1
    fi
    first_begin=$(grep -n '^# BEGIN aenv_completion_install$' "$input" | cut -d: -f1)
    last_end=$(grep -n '^# END aenv_completion_install$' "$input" | cut -d: -f1)
    if [[ "$first_begin" -gt "$last_end" ]]; then
        echo "error: END marker precedes BEGIN marker in $input" >&2
        return 1
    fi
    sed -n "${first_begin},${last_end}p" "$input" > "$out"
}

if ! extract "$canonical" "$tmp_dir/canonical"; then
    exit 1
fi

rc=0
for f in "$cli" "$full"; do
    if ! extract "$f" "$tmp_dir/cand"; then
        rc=1
        continue
    fi
    if ! cmp -s "$tmp_dir/canonical" "$tmp_dir/cand"; then
        echo "error: aenv_completion_install block in $f differs from $canonical" >&2
        diff -u "$tmp_dir/canonical" "$tmp_dir/cand" >&2 || true
        rc=1
    fi
done

if [[ $rc -eq 0 ]]; then
    echo "aenv_completion_install blocks in sync."
fi
exit "$rc"
