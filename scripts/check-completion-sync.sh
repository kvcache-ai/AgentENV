#!/usr/bin/env bash
# Verify that the inlined `aenv_completion_install` blocks in the standalone
# installers stay byte-identical to the canonical copy in
# scripts/shell-completion.sh. Run from CI / `make check-shell-completion`.
#
# The canonical helper is a single source of truth; install-cli.sh and
# install.sh cannot source it (they are curl|bash'd as standalone scripts), so
# they inline a verbatim copy bracketed by the marker comments:
#
#     # BEGIN aenv_completion_install ...
#     ...
#     # END aenv_completion_install
#
# This script extracts that block from each file and fails on any divergence.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
canonical="$repo_root/scripts/shell-completion.sh"
cli="$repo_root/scripts/install-cli.sh"
full="$repo_root/scripts/install.sh"

extract() {
    awk '/^# BEGIN aenv_completion_install$/,/^# END aenv_completion_install$/' "$1"
}

# shellcheck disable=SC2312
block_canon="$(extract "$canonical")"
if [[ -z "$block_canon" ]]; then
    echo "error: could not find aenv_completion_install block in $canonical" >&2
    exit 1
fi

rc=0
for f in "$cli" "$full"; do
    # shellcheck disable=SC2312
    block_f="$(extract "$f")"
    if [[ -z "$block_f" ]]; then
        echo "error: could not find aenv_completion_install block in $f" >&2
        rc=1
        continue
    fi
    if [[ "$block_canon" != "$block_f" ]]; then
        echo "error: aenv_completion_install block in $f differs from $canonical" >&2
        diff -u <(printf '%s\n' "$block_canon") <(printf '%s\n' "$block_f") >&2 || true
        rc=1
    fi
done

if [[ $rc -eq 0 ]]; then
    echo "aenv_completion_install blocks in sync."
fi
exit "$rc"
