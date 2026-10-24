#!/bin/sh
# Copyright 2026 AgentENV contributors.
# SPDX-License-Identifier: Apache-2.0
set -eu
mode=${1:-fixture-init}
if [ "$#" -gt 0 ]; then shift; fi
case "$mode" in
    fixture-init)
        while true; do /bin/busybox sleep 3600; done
        ;;
    inner-init)
        /bin/busybox mount -t proc proc /proc
        /bin/busybox mount -t sysfs sysfs /sys
        n=0
        while true; do
            echo "INNER_TICK=$n"
            n=$((n+1))
            /bin/busybox sleep 1
        done
        ;;
    start-inner)
        /bin/busybox ip link set lo up
        exec /usr/bin/qemu-system-x86_64 -machine microvm,accel=kvm -cpu host \
            -m 128 -smp 1 -nodefaults -no-reboot -nographic -serial stdio \
            -qmp tcp:127.0.0.1:4444,server=on,wait=off \
            -kernel /inner-bzImage -initrd /inner.cpio \
            -append 'console=ttyS0 panic=1 rdinit=/init' "$@"
        ;;
    stop-inner)
        pid=$(/bin/busybox cat "${1:?PID_FILE}")
        test "$(/bin/busybox readlink /proc/"$pid"/exe)" = /usr/bin/qemu-system-x86_64
        kill -TERM "$pid"
        attempt=0
        while kill -0 "$pid" 2>/dev/null; do
            processes=$(/bin/busybox ps -o pid,stat)
            state=$(printf '%s\n' "$processes" | /bin/busybox awk -v target="$pid" '$1 == target {print $2}')
            case "$state" in Z*|"") break ;; esac
            attempt=$((attempt + 1))
            test "$attempt" -lt 100
            /bin/busybox sleep 0.1
        done
        echo "QEMU_STOPPED=$pid"
        ;;
    *) echo "Unknown guest operation: $mode" >&2; exit 2 ;;
esac
