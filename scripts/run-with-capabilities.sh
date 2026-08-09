#!/usr/bin/env bash
# Run a command with AENV's runtime capability set, preferring a non-root UID.
set -euo pipefail

if (($# == 0)); then
    echo "usage: $0 <command> [args...]" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

validate_state_path() {
    local name="$1"
    local path="$2"
    local resolved
    resolved="$(realpath -m -- "$path")"
    case "$resolved" in
        "$repo_root"/*|/home/*|/tmp/*|/var/*|/opt/*|/mnt/*|/srv/*|/run/*) ;;
        *)
            echo "error: refusing unsafe ${name}: ${path}" >&2
            exit 1
            ;;
    esac
}

if [[ ${EUID} -ne 0 ]]; then
    export AENV_RUN_USER="${AENV_RUN_USER:-$(id -un)}"
    export AENV_HOME_PATH="${AENV_HOME_PATH:-${TMPDIR:-/tmp}/aenv-test-$(id -u)/home}"
    export AENV_RUNTIME_PATH="${AENV_RUNTIME_PATH:-${TMPDIR:-/tmp}/aenv-test-$(id -u)/run}"
    # This dev/CI wrapper preserves Cargo and AENV variables across sudo; it is
    # not intended as a general-purpose privilege boundary.
    exec sudo -E "$0" "$@"
fi

root_warning=""
if [[ -n "${AENV_RUN_USER:-}" ]]; then
    if ! id -u "$AENV_RUN_USER" >/dev/null 2>&1; then
        echo "error: AENV_RUN_USER does not identify an existing account: ${AENV_RUN_USER}" >&2
        exit 1
    fi
    run_user="$AENV_RUN_USER"
    if [[ "$run_user" == "root" ]]; then
        root_warning="AENV_RUN_USER=root was explicitly selected"
    fi
elif [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]] && id -u "$SUDO_USER" >/dev/null 2>&1; then
    run_user="$SUDO_USER"
else
    repo_owner="$(stat -c '%U' "$repo_root")"
    if [[ "$repo_owner" != "root" ]] && id -u "$repo_owner" >/dev/null 2>&1; then
        run_user="$repo_owner"
    elif id -u aenv >/dev/null 2>&1; then
        run_user="aenv"
    else
        run_user="root"
        root_warning="no non-root AENV account was found"
    fi
fi

run_group="$(id -gn "$run_user")"
run_uid="$(id -u "$run_user")"
if [[ "$run_uid" == "0" && -z "$root_warning" ]]; then
    root_warning="the selected account ${run_user} resolves to UID 0"
fi
export AENV_HOME_PATH="${AENV_HOME_PATH:-${TMPDIR:-/tmp}/aenv-test-${run_uid}/home}"
export AENV_RUNTIME_PATH="${AENV_RUNTIME_PATH:-${TMPDIR:-/tmp}/aenv-test-${run_uid}/run}"
validate_state_path AENV_HOME_PATH "$AENV_HOME_PATH"
validate_state_path AENV_RUNTIME_PATH "$AENV_RUNTIME_PATH"

install -d -o "$run_user" -g "$run_group" -m 0750 \
    "${AENV_HOME_PATH:?}" "${AENV_RUNTIME_PATH:?}"

# AENV_MEMLOCK_LIMIT is passed directly to `ulimit -l` (in KiB, or
# `unlimited`). The default prevents ublk/io_uring buffer registration from
# failing on hosts with the common small 8 MiB locked-memory limit; set a
# smaller value only when you intentionally want to cap locked memory in dev/CI.
memlock_limit="${AENV_MEMLOCK_LIMIT:-unlimited}"
if ! ulimit -l "$memlock_limit"; then
    echo "warning: failed to raise locked-memory ulimit to ${memlock_limit}; ublk buffer registration may fail on hosts with a small memlock limit" >&2
fi

capability_args=(
    "--inh-caps=-all,+net_admin,+net_raw,+sys_admin"
    "--ambient-caps=-all,+net_admin,+net_raw,+sys_admin"
    "--bounding-set=-all,+net_admin,+net_raw,+sys_admin"
    --nnp
)

if [[ "$run_uid" == "0" ]]; then
    echo "warning: ${root_warning}; running as UID 0 with only CAP_NET_ADMIN, CAP_NET_RAW, and CAP_SYS_ADMIN" >&2
    exec setpriv \
        --securebits=+noroot,+noroot_locked \
        "${capability_args[@]}" \
        "$@"
fi

exec setpriv \
    --reuid="$run_user" \
    --regid="$run_group" \
    --init-groups \
    "${capability_args[@]}" \
    "$@"
