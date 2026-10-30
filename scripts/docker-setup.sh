#!/usr/bin/env bash
# Host setup for running AgentENV in Docker.
# Loads the block device kernel module, persists it across reboots, and tunes
# the host kernel parameters that AgentENV skips writing when it detects a
# container environment.
#
# The block device backend comes from AENV_UBLK_DEVICE_BACKEND (auto, ublk, or
# nbd; default auto). auto loads ublk_drv (installing linux-modules-extra if
# missing) and falls back to nbd when the kernel has no ublk support.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
#   # or, from a cloned repository:
#   sudo bash scripts/docker-setup.sh
#   # force a backend:
#   sudo AENV_UBLK_DEVICE_BACKEND=nbd bash scripts/docker-setup.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
    exit 1
fi

DEVICE_BACKEND="${AENV_UBLK_DEVICE_BACKEND:-auto}"
case "$DEVICE_BACKEND" in
    auto|ublk|nbd) ;;
    *)
        echo "error: unsupported AENV_UBLK_DEVICE_BACKEND=${DEVICE_BACKEND@Q}; expected auto, ublk, or nbd" >&2
        exit 1
        ;;
esac

# ---------------------------------------------------------------------------
# 1. Block device kernel module (ublk_drv or nbd)
# ---------------------------------------------------------------------------
load_ublk() {
    if [[ -d /sys/module/ublk_drv ]]; then
        echo "  ublk_drv is already loaded; leaving active devices untouched"
        return 0
    fi
    if modprobe ublk_drv ublks_max=4096 2>/dev/null; then
        return 0
    fi
    echo "  ublk_drv not found; installing linux-modules-extra-$(uname -r) ..."
    if ! command -v apt-get &>/dev/null; then
        echo "  apt-get not found — install linux-modules-extra-$(uname -r) with your package manager" >&2
        return 1
    fi
    apt-get install -y "linux-modules-extra-$(uname -r)" && modprobe ublk_drv ublks_max=4096
}

if [[ "$DEVICE_BACKEND" != "nbd" ]]; then
    echo "Loading ublk_drv kernel module ..."
    if load_ublk; then
        DEVICE_BACKEND=ublk
        UBLK_MODPROBE_CONF=/etc/modprobe.d/aenv-ublk.conf
        install -d -m 0755 "$(dirname "$UBLK_MODPROBE_CONF")"
        printf '%s\n' 'options ublk_drv ublks_max=4096' > "$UBLK_MODPROBE_CONF"
        rm -f /etc/modules-load.d/aenv-nbd.conf
        echo ublk_drv | tee /etc/modules-load.d/aenv-ublk.conf > /dev/null
        echo "  ublk_drv loaded and persisted in /etc/modules-load.d/aenv-ublk.conf"
    elif [[ "$DEVICE_BACKEND" == "ublk" ]]; then
        echo "error: failed to load ublk_drv — try upgrading the kernel to 6.8+, or use AENV_UBLK_DEVICE_BACKEND=nbd" >&2
        exit 1
    else
        echo "  ublk_drv is unavailable; falling back to nbd"
        DEVICE_BACKEND=nbd
    fi
fi

if [[ "$DEVICE_BACKEND" == "nbd" ]]; then
    echo "Loading nbd kernel module ..."
    if ! modprobe nbd; then
        echo "error: failed to load nbd — the kernel must provide CONFIG_BLK_DEV_NBD" >&2
        exit 1
    fi
    rm -f /etc/modules-load.d/aenv-ublk.conf
    echo nbd | tee /etc/modules-load.d/aenv-nbd.conf > /dev/null
    echo "  nbd loaded and persisted in /etc/modules-load.d/aenv-nbd.conf"
fi

# ---------------------------------------------------------------------------
# 2. Host kernel parameters
# ---------------------------------------------------------------------------
echo "Tuning host kernel parameters ..."

SYSCTL_CONF=/etc/sysctl.d/99-aenv.conf

tee "$SYSCTL_CONF" > /dev/null <<'EOF'
# AgentENV host kernel parameters — managed by scripts/docker-setup.sh
net.ipv4.neigh.default.gc_thresh1 = 4096
net.ipv4.neigh.default.gc_thresh2 = 8192
net.ipv4.neigh.default.gc_thresh3 = 16384
net.netfilter.nf_conntrack_max = 1048576
kernel.pid_max = 4194304
fs.inotify.max_user_instances = 8192
EOF

apply_sysctl() {
    local key="$1" value="$2"
    if ! sysctl -w "${key}=${value}" 2>/dev/null; then
        echo "  warning: could not set ${key} (skipped)"
    fi
}

# nf_conntrack_max requires the module to be loaded first
modprobe nf_conntrack 2>/dev/null || true

apply_sysctl net.ipv4.neigh.default.gc_thresh1 4096
apply_sysctl net.ipv4.neigh.default.gc_thresh2 8192
apply_sysctl net.ipv4.neigh.default.gc_thresh3 16384
apply_sysctl net.netfilter.nf_conntrack_max 1048576
apply_sysctl kernel.pid_max 4194304
apply_sysctl fs.inotify.max_user_instances 8192

echo "  kernel parameters written to $SYSCTL_CONF and applied"
echo ""
echo "Host setup complete. Block device backend: ${DEVICE_BACKEND}"
if [[ "$DEVICE_BACKEND" == "nbd" ]]; then
    echo ""
    echo "Pass the backend to AgentENV containers:"
    echo "  docker run -e AENV_UBLK_DEVICE_BACKEND=nbd ..."
    echo "  AENV_UBLK_DEVICE_BACKEND=nbd make deploy-up"
fi
