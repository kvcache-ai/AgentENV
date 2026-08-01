#!/usr/bin/env bash
# Host setup for running AgentENV in Docker.
# Loads the ublk_drv kernel module (installing linux-modules-extra if missing),
# persists it across reboots, and tunes the host kernel parameters that
# AgentENV skips writing when it detects a container environment.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
#   # or, from a cloned repository:
#   sudo bash scripts/docker-setup.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
    exit 1
fi

AUTH_ENV_FILE="${AENV_AUTH_ENV_FILE:-/etc/aenv/auth.env}"
API_KEY_OVERRIDE_SET="${AENV_API_KEY+x}"

# ---------------------------------------------------------------------------
# 1. ublk_drv kernel module
# ---------------------------------------------------------------------------
echo "Loading ublk_drv kernel module ..."
if ! modprobe ublk_drv 2>/dev/null; then
    echo "  ublk_drv not found; installing linux-modules-extra-$(uname -r) ..."
    if ! command -v apt-get &>/dev/null; then
        echo "error: apt-get not found — install linux-modules-extra-$(uname -r) with your package manager" >&2
        exit 1
    fi
    apt-get install -y "linux-modules-extra-$(uname -r)"
    if ! modprobe ublk_drv; then
        echo "error: failed to load ublk_drv — try upgrading the kernel to 6.8+" >&2
        exit 1
    fi
fi
echo ublk_drv | tee /etc/modules-load.d/aenv-ublk.conf > /dev/null
echo "  ublk_drv loaded and persisted in /etc/modules-load.d/aenv-ublk.conf"

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

# ---------------------------------------------------------------------------
# 3. Authentication
# ---------------------------------------------------------------------------
if [[ -L "$AUTH_ENV_FILE" ]]; then
    echo "error: refusing symlinked auth file: $AUTH_ENV_FILE" >&2
    exit 1
fi

if [[ "$API_KEY_OVERRIDE_SET" == "x" ]]; then
    API_KEY_VALUE="${AENV_API_KEY}"
elif [[ -f "$AUTH_ENV_FILE" ]]; then
    API_KEY_VALUE="$(sed -n 's/^AENV_API_KEY=//p' "$AUTH_ENV_FILE" | tail -n 1)"
else
    API_KEY_VALUE="e2b_$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"
fi
if [[ ! "$API_KEY_VALUE" =~ ^[A-Za-z0-9._~-]{32,}$ ]]; then
    echo "error: AENV_API_KEY must contain at least 32 URL-safe characters" >&2
    exit 1
fi

auth_tmp="$(mktemp)"
trap 'rm -f "$auth_tmp"' EXIT
printf 'AENV_API_KEY=%s\n' "$API_KEY_VALUE" > "$auth_tmp"
install -d -o root -g root -m 0750 "$(dirname "$AUTH_ENV_FILE")"
if getent group docker >/dev/null 2>&1; then
    install -o root -g docker -m 0640 "$auth_tmp" "$AUTH_ENV_FILE"
else
    install -o root -g root -m 0600 "$auth_tmp" "$AUTH_ENV_FILE"
fi
rm -f "$auth_tmp"

echo ""
echo "Host setup complete."
echo "AgentENV API key stored in $AUTH_ENV_FILE."
