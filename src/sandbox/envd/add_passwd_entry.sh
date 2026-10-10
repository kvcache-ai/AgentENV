# Append a passwd(5) record under shadow-utils' link lock.
#
# Usage: sh -c "$SCRIPT" sh BUSYBOX PASSWD_FILE ENTRY
#
# Image init systems may run usermod, which rewrites the passwd file through a
# temporary copy and rename; an unlocked append racing it is silently lost.
# Like shadow's commonio, take PASSWD_FILE.lock by hard-linking a file holding
# our PID, and treat a lock whose owner has exited as stale.
set -eu
bb=$1
passwd=$2
entry=$3
lock="$passwd.lock"
tmp="$passwd.$$"

printf '%s' "$$" >"$tmp"
tries=0
until "$bb" ln "$tmp" "$lock" 2>/dev/null; do
    owner=$("$bb" cat "$lock" 2>/dev/null || true)
    if [ -n "$owner" ] && ! "$bb" kill -0 "$owner" 2>/dev/null; then
        "$bb" rm -f "$lock"
        continue
    fi
    tries=$((tries + 1))
    if [ "$tries" -ge 50 ]; then
        "$bb" rm -f "$tmp"
        echo "timed out waiting for $lock" >&2
        exit 1
    fi
    "$bb" sleep 0.1
done
"$bb" rm -f "$tmp"
trap '"$bb" rm -f "$lock"' EXIT

if ! "$bb" grep -qxF "$entry" "$passwd"; then
    printf '\n%s\n' "$entry" >>"$passwd"
fi
"$bb" grep -qxF "$entry" "$passwd"
