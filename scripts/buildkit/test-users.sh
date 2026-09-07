#!/usr/bin/env bash
set -euo pipefail

AENV_BIN=${AENV_BIN:-aenv}
BUILDCTL_BIN=${BUILDCTL_BIN:-buildctl}
work=$(mktemp -d)
prefix="buildkit-users-$(date +%s)-$$"
sandbox=""
cleanup() {
    if [[ -n "$sandbox" ]]; then "$AENV_BIN" delete "$sandbox" || true; fi
    for log in "$work"/*.log; do
        [[ -f "$log" ]] || continue
        while read -r id; do "$AENV_BIN" template delete "$id" || true; done < <(
            awk '/^Created template / {print $3}' "$log"
        )
    done
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for identity in 0 65534 12345 12345:23456; do
    name="${identity//:/-}"
    "$AENV_BIN" build "$(dirname "$0")/users" --name "$prefix-$name" \
        --buildctl "$BUILDCTL_BIN" --progress plain --timeout 180 \
        --build-arg "RUN_AS=$identity" 2>&1 | tee "$work/$name.log"
    sandbox=$("$AENV_BIN" start "$prefix-$name" --detach)
    uid=${identity%%:*}
    case "$identity" in
        *:*) gid=${identity##*:} ;;
        65534) gid=65534 ;;
        *) gid=0 ;;
    esac
    [[ $("$AENV_BIN" exec "$sandbox" cat /tmp/uid) == "$uid" ]]
    [[ $("$AENV_BIN" exec "$sandbox" cat /tmp/gid) == "$gid" ]]
    [[ $("$AENV_BIN" exec "$sandbox" id -u) == "$uid" ]]
    [[ $("$AENV_BIN" exec "$sandbox" id -g) == "$gid" ]]
    "$AENV_BIN" delete "$sandbox"
    sandbox=""
done
echo 'BuildKit numeric-user checks passed'
