#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runner="${repo_root}/scripts/run-with-capabilities.sh"
tmp_dir="$(mktemp -d)"

if [[ ${EUID} -eq 0 ]]; then
    as_root=()
else
    as_root=(sudo)
fi

cleanup() {
    "${as_root[@]}" rm -rf "$tmp_dir"
}
trap cleanup EXIT

output="$(
    "${as_root[@]}" env -u SUDO_USER -u SUDO_UID -u SUDO_GID \
        AENV_RUN_USER=root \
        AENV_HOME_PATH="${tmp_dir}/home" \
        AENV_RUNTIME_PATH="${tmp_dir}/run" \
        "$runner" sh -c '
            id -u
            sed -n \
                -e "s/^CapInh:[[:space:]]*//p" \
                -e "s/^CapPrm:[[:space:]]*//p" \
                -e "s/^CapEff:[[:space:]]*//p" \
                -e "s/^CapBnd:[[:space:]]*//p" \
                -e "s/^CapAmb:[[:space:]]*//p" \
                -e "s/^NoNewPrivs:[[:space:]]*//p" \
                /proc/self/status
            setpriv --ambient-caps=-all sh -c '\''
                sed -n \
                    -e "s/^CapEff:[[:space:]]*//p" \
                    -e "s/^CapAmb:[[:space:]]*//p" \
                    /proc/self/status
            '\''
        '
)"

mapfile -t lines <<< "$output"
[[ "${lines[0]}" == "0" ]]
for index in 1 2 3 4 5; do
    [[ "${lines[$index]}" == "0000000000203000" ]]
done
[[ "${lines[6]}" == "1" ]]
[[ "${lines[7]}" == "0000000000000000" ]]
[[ "${lines[8]}" == "0000000000000000" ]]

memlock_output="$(
    "${as_root[@]}" env -u SUDO_USER -u SUDO_UID -u SUDO_GID \
        AENV_RUN_USER=root \
        AENV_HOME_PATH="${tmp_dir}/memlock-home" \
        AENV_RUNTIME_PATH="${tmp_dir}/memlock-run" \
        AENV_MEMLOCK_LIMIT=64 \
        "$runner" sh -c 'ulimit -l'
)"
[[ "$memlock_output" == "64" ]]

if "${as_root[@]}" env AENV_RUN_USER=aenv-user-that-does-not-exist \
    "$runner" true 2>/dev/null; then
    echo "error: capability runner accepted an unknown explicit user" >&2
    exit 1
fi

if "${as_root[@]}" env AENV_RUN_USER=root AENV_HOME_PATH=/etc \
    AENV_RUNTIME_PATH="${tmp_dir}/unsafe-run" "$runner" true 2>/dev/null; then
    echo "error: capability runner accepted an unsafe state path" >&2
    exit 1
fi
