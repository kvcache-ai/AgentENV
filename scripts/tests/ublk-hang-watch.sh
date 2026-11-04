#!/usr/bin/env bash
# Diagnostics for the ublk crash test: while <log> exists, dump ublk daemon
# thread stacks, D-state tasks, and each ublk disk's blk-mq debugfs state
# whenever <log> has not grown for STALL_SECS, plus envd connections and each
# sandbox network namespace in <netns-dir>. Run as root.
# usage: ublk-hang-watch.sh <log> <out> <netns-dir>
set -u
log=$1 out=$2 netns_dir=$3
stall_secs=${STALL_SECS:-40}
last=-1 stall=0 dumped=0

# Thread state; the comm field may contain spaces ("fc_vcpu 0").
tstate() { sed 's/.*) //' "$1"/stat 2>/dev/null | cut -d' ' -f1; }

dump() {
  echo "===== hang dump $(date -u +%FT%T) stall=${stall}s"
  for pid in $(pgrep -x uvm-ublk-daemon); do
    echo "--- ublk daemon pid $pid"
    for t in /proc/"$pid"/task/*; do
      echo "tid ${t##*/} $(cat "$t"/comm 2>/dev/null) state=$(tstate "$t")"
      sed 's/^/    /' "$t"/stack 2>/dev/null | head -16
    done
  done
  for pid in $(pgrep -x firecracker); do
    echo "--- firecracker pid $pid $(tr '\0' ' ' < /proc/"$pid"/cmdline 2>/dev/null | cut -c1-200)"
    for t in /proc/"$pid"/task/*; do
      echo "tid ${t##*/} $(cat "$t"/comm 2>/dev/null) state=$(tstate "$t") wchan=$(cat "$t"/wchan 2>/dev/null)"
      case "$(cat "$t"/comm 2>/dev/null)" in
        fc_vcpu*|fc_vmm) sed 's/^/    /' "$t"/stack 2>/dev/null | head -12 ;;
      esac
    done
  done
  echo "--- D-state tasks"
  for t in /proc/[0-9]*/task/[0-9]*; do
    [ "$(tstate "$t")" = D ] || continue
    echo "tid ${t##*/} $(cat "$t"/comm 2>/dev/null)"
    sed 's/^/    /' "$t"/stack 2>/dev/null | head -16
  done
  echo "--- host envd connections"
  ss -tnoi exclude time-wait '( dport = :49983 )'
  for ns in "$netns_dir"/*; do
    [ -e "$ns" ] || continue
    echo "--- netns ${ns##*/}"
    nsenter --net="$ns" ip -o -4 addr show
    nsenter --net="$ns" iptables -t nat -S PREROUTING
    nsenter --net="$ns" ip -s link show tap0
    if command -v conntrack >/dev/null; then
      nsenter --net="$ns" conntrack -L -p tcp --dport 49983 2>/dev/null | grep -v TIME_WAIT
    fi
  done
  echo "--- ublk disks"
  for b in /sys/block/ublkb*; do
    [ -e "$b" ] || continue
    d=${b##*/}
    echo "$d inflight=$(cat "$b"/inflight 2>/dev/null)"
    dbg=/sys/kernel/debug/block/$d
    for f in state requeue_list hctx0/state hctx0/busy hctx0/dispatch hctx0/tags; do
      [ -e "$dbg/$f" ] || continue
      content=$(head -c 2000 "$dbg/$f" 2>/dev/null)
      [ -n "$content" ] && printf '  %s:\n%s\n' "$f" "$(printf '%s\n' "$content" | sed 's/^/    /')"
    done
  done
}

mountpoint -q /sys/kernel/debug || mount -t debugfs none /sys/kernel/debug 2>/dev/null
while [ -e "$log" ]; do
  sleep 5
  size=$(stat -c %s "$log" 2>/dev/null || echo 0)
  if [ "$size" != "$last" ]; then
    last=$size stall=0 dumped=0
    continue
  fi
  stall=$((stall + 5))
  # Dump at the start of a stall and once more later, to tell a hang from a
  # slow step.
  if { [ $dumped -eq 0 ] && [ $stall -ge "$stall_secs" ]; } ||
     { [ $dumped -eq 1 ] && [ $stall -ge $((stall_secs * 3)) ]; }; then
    dumped=$((dumped + 1))
    dump >> "$out" 2>&1
  fi
done
