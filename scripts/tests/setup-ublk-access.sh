#!/usr/bin/env bash
# Provision ublk device access and OverlayBD defaults for CI tests.
set -euo pipefail

if (($# != 2)); then
    echo "usage: $0 <user> <group>" >&2
    exit 2
fi
if [[ ${EUID} -ne 0 ]]; then
    echo "error: ublk host setup must run as root" >&2
    exit 1
fi

user="$1"
group="$2"
if [[ ! "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || ! id -u "$user" >/dev/null 2>&1; then
    echo "error: invalid or unknown runtime user: ${user}" >&2
    exit 1
fi
if [[ "$(id -u "$user")" == "0" ]]; then
    echo "error: runtime user must be non-root" >&2
    exit 1
fi
if [[ ! "$group" =~ ^[a-z_][a-z0-9_-]*$ ]] || ! getent group "$group" >/dev/null; then
    echo "error: invalid or unknown ublk device group: ${group}" >&2
    exit 1
fi
if [[ "$(getent group "$group" | cut -d: -f3)" == "0" ]]; then
    echo "error: ublk device group must be non-root" >&2
    exit 1
fi

if ! modprobe ublk_drv; then
    modules_package="linux-modules-extra-$(uname -r)"
    if ! command -v apt-get >/dev/null 2>&1; then
        echo "error: ublk_drv is unavailable and ${modules_package} cannot be installed without apt-get" >&2
        exit 1
    fi

    echo "ublk_drv is unavailable; installing ${modules_package}"
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$modules_package"
    modprobe ublk_drv
fi

overlaybd_config="/etc/overlaybd/overlaybd.json"
if [[ -e "$overlaybd_config" && ! -f "$overlaybd_config" ]]; then
    echo "error: OverlayBD config is not a regular file: ${overlaybd_config}" >&2
    exit 1
fi
install -d -m 0755 "$(dirname "$overlaybd_config")"
if [[ ! -s "$overlaybd_config" ]]; then
    printf '{}\n' > "$overlaybd_config"
fi
chmod 0644 "$overlaybd_config"
jq -e 'type == "object"' "$overlaybd_config" >/dev/null

install -d -m 0755 /etc/udev/rules.d
printf '%s\n' \
    '# Managed by AENV CI' \
    "KERNEL==\"ublk-control\", MODE=\"0660\", GROUP=\"${group}\"" \
    "KERNEL==\"ublkc*\", MODE=\"0660\", GROUP=\"${group}\"" \
    "KERNEL==\"ublkb*\", MODE=\"0660\", GROUP=\"${group}\"" \
    'ACTION=="add", SUBSYSTEM=="block", KERNEL=="ublkb*", ATTR{queue/scheduler}="none"' \
    > /etc/udev/rules.d/99-agentenv-ublk.rules
udevadm control --reload-rules
udevadm trigger --subsystem-match=misc --sysname-match=ublk-control
udevadm trigger --sysname-match='ublkc*'
udevadm trigger --sysname-match='ublkb*'
udevadm settle --timeout=10

setpriv --reuid="$user" --regid="$group" --init-groups sh -c '
    test -r /etc/overlaybd/overlaybd.json
    jq -e "type == \"object\"" /etc/overlaybd/overlaybd.json >/dev/null
    exec 3<>/dev/ublk-control
'
