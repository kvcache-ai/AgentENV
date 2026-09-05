#!/usr/bin/env bash
set -euo pipefail

AENV_BIN=${AENV_BIN:-aenv}
BUILDCTL_BIN=${BUILDCTL_BIN:-buildctl}
work=$(mktemp -d)
prefix="buildkit-check-$(date +%s)-$$"
sandbox=""
build_pid=""
concurrent_pid=""
cleanup() {
    if [[ -n "$concurrent_pid" ]]; then
        kill -TERM "$concurrent_pid" 2>/dev/null || true
        wait "$concurrent_pid" 2>/dev/null || true
    fi
    if [[ -n "$build_pid" ]]; then
        kill -TERM "$build_pid" 2>/dev/null || true
        wait "$build_pid" 2>/dev/null || true
    fi
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
cp -R "$(dirname "$0")/fixture" "$work/context"
common=(--buildctl "$BUILDCTL_BIN" --progress plain --timeout 180 --build-arg "CACHE_KEY=$prefix")

build() {
    local name=$1
    shift
    "$AENV_BIN" build "$work/context" --name "$prefix-$name" "${common[@]}" "$@" 2>&1 | tee "$work/$name.log"
}
verify() {
    sandbox=$("$AENV_BIN" start "$prefix-$1" --detach)
    # Expand the image environment inside the guest.
    # shellcheck disable=SC2016
    "$AENV_BIN" exec "$sandbox" /bin/sh -c \
        'test "$BUILD_BACKEND" = buildkit && test ! -d /src && test ! -d /compiler-cache && test -s /started'
    [[ $("$AENV_BIN" exec "$sandbox" cat /result.txt) == "$2" ]]
    "$AENV_BIN" delete "$sandbox"
    sandbox=""
}

build first
if LC_ALL=C grep -q $'\033' "$work/first.log"; then
    echo 'Non-terminal build output contains terminal escapes' >&2
    exit 1
fi
verify first "$(cat "$work/context/input.txt")"
printf '%s\n' second-build >"$work/context/input.txt"
build changed
grep -q 'CACHED' "$work/changed.log"
verify changed second-build
build unchanged
grep -q 'CACHED' "$work/unchanged.log"
first_digest=$(sed -n 's/.*exporting manifest \(sha256:[a-f0-9]*\).*/\1/p' "$work/changed.log" | tail -1)
second_digest=$(sed -n 's/.*exporting manifest \(sha256:[a-f0-9]*\).*/\1/p' "$work/unchanged.log" | tail -1)
[[ -n "$first_digest" && "$first_digest" == "$second_digest" ]]

# The image health check requires /started, so these builds require both overrides.
build overrides --start-cmd 'echo override >/override; exec sleep infinity' \
    --ready-cmd 'test -s /override && test ! -e /started'
sandbox=$("$AENV_BIN" start "$prefix-overrides" --detach)
"$AENV_BIN" exec "$sandbox" /bin/sh -c 'test -s /override && test ! -e /started'
"$AENV_BIN" delete "$sandbox"
sandbox=""
build no-start --start-cmd "" --ready-cmd 'test ! -e /started'
sandbox=$("$AENV_BIN" start "$prefix-no-start" --detach)
"$AENV_BIN" exec "$sandbox" /bin/sh -c 'test ! -e /started && test ! -e /override'
"$AENV_BIN" delete "$sandbox"
sandbox=""

# Both branches inherit the cache-mount marker and publish independently.
cp -R "$work/context" "$work/parallel-a"
cp -R "$work/context" "$work/parallel-b"
printf '%s\n' parallel-a >"$work/parallel-a/input.txt"
printf '%s\n' parallel-b >"$work/parallel-b/input.txt"
"$AENV_BIN" build "$work/parallel-a" --name "$prefix-parallel-a" "${common[@]}" \
    --build-arg BUILD_DELAY=15 >"$work/parallel-a.log" 2>&1 &
build_pid=$!
"$AENV_BIN" build "$work/parallel-b" --name "$prefix-parallel-b" "${common[@]}" \
    --build-arg BUILD_DELAY=5 >"$work/parallel-b.log" 2>&1 &
concurrent_pid=$!
wait "$build_pid"
build_pid=""
wait "$concurrent_pid"
concurrent_pid=""
verify parallel-a parallel-a
verify parallel-b parallel-b

printf 'FROM alpine:3.22\nRUN exit 17\n' >"$work/context/Dockerfile"
if build failure; then
    echo 'Expected a failing build' >&2
    exit 1
fi
grep -q 'exit code: 17' "$work/failure.log"

printf 'FROM alpine:3.22\nRUN sleep 120\n' >"$work/context/Dockerfile"
"$AENV_BIN" build "$work/context" --name "$prefix-interrupt" "${common[@]}" >"$work/interrupt.log" 2>&1 &
build_pid=$!
for _ in $(seq 1 120); do
    if grep -q 'RUN sleep 120' "$work/interrupt.log"; then break; fi
    kill -0 "$build_pid"
    sleep 1
done
grep -q 'RUN sleep 120' "$work/interrupt.log"
build_id=$(awk '/^Created template / {print $3}' "$work/interrupt.log")
[[ -n "$build_id" ]]
sandbox_list=$("$AENV_BIN" list --output json)
if grep -qF "$build_id" <<<"$sandbox_list"; then
    echo 'Internal builder appeared in the sandbox list' >&2
    exit 1
fi
if "$AENV_BIN" exec "$build_id" true; then
    echo 'Internal builder was accessible through the sandbox API' >&2
    exit 1
fi
# Both builders must reach their RUN step before either releases its cache.
"$AENV_BIN" build "$work/context" --name "$prefix-concurrent" "${common[@]}" >"$work/concurrent.log" 2>&1 &
concurrent_pid=$!
for _ in $(seq 1 90); do
    if grep -q 'RUN sleep 120' "$work/concurrent.log"; then break; fi
    kill -0 "$concurrent_pid"
    sleep 1
done
grep -q 'RUN sleep 120' "$work/concurrent.log"
kill -0 "$build_pid"
kill -TERM "$concurrent_pid"
if wait "$concurrent_pid"; then
    echo 'Expected a cancelled concurrent build to fail' >&2
    exit 1
fi
concurrent_pid=""
kill -TERM "$build_pid"
if wait "$build_pid"; then
    echo 'Expected an interrupted build to fail' >&2
    exit 1
fi
build_pid=""

# Failure and cancellation must leave the previous shared seed usable.
cp "$(dirname "$0")/fixture/Dockerfile" "$work/context/Dockerfile"
build recovered
verify recovered second-build
if [[ $(uname -s) == Linux ]] && command -v script >/dev/null; then
    printf -v tty_command '%q ' "$AENV_BIN" build "$work/context" --name "$prefix-progress" \
        --buildctl "$BUILDCTL_BIN" --progress auto --timeout 180 --build-arg "CACHE_KEY=$prefix"
    TERM=xterm-256color script -q -e -c "$tty_command" "$work/progress.log"
    for stage in 0/3 1/3 2/3; do grep -q "$stage" "$work/progress.log"; done
    verify progress second-build
fi
echo 'BuildKit integration checks passed'
