#!/usr/bin/env bash
# Install AgentENV: the aenv CLI and the server binaries.
# The server is configured as a systemd service (or prints a manual start
# command if systemd is unavailable).
# Downloads: aenv (cli)   -> /usr/local/bin/aenv
#            server -> /usr/local/bin/server
#            dependencies -> /var/lib/aenv/deps
#            ublk daemon -> /var/lib/aenv/ublk/uvm-ublk-daemon
#            config  -> /var/lib/aenv/config/config.toml
#            overlaybd default config -> /etc/overlaybd/overlaybd.json
#            service -> /etc/systemd/system/aenv.service
#            env     -> /etc/default/aenv
# Runs:      sudo server --setup-only  (provisions runtime dependencies)
#            sudo server --setup-host  (provisions virtualization access, ublk, and networking)
#
# Usage:
#   curl -fsSL https://github.com/kvcache-ai/AgentENV/releases/latest/download/install.sh | sudo bash
#
# Override the AgentENV data directory:
#   sudo AENV_HOME_PATH=/path/to/aenv/data bash install.sh

set -euo pipefail
export LC_ALL=C

if [[ $EUID -ne 0 ]] && ! sudo -v 2>/dev/null; then
    echo "error: this script requires root or sudo access" >&2
    exit 1
fi

REPO="kvcache-ai/AgentENV"
INSTALL_DIR="/usr/local/bin"
SKIP_SETUP="${SKIP_SETUP:-0}"
DATA_DIR="${AENV_HOME_PATH:-/var/lib/aenv}"
CONFIG_PATH="${DATA_DIR}/config/config.toml"
DEPS_DIR="${DATA_DIR}/deps"
UBLK_DAEMON_PATH="${DATA_DIR}/ublk/uvm-ublk-daemon"
SERVICE_NAME="aenv"
SERVICE_USER="${AENV_SERVICE_USER:-aenv}"
SERVICE_GROUP="${AENV_SERVICE_GROUP:-aenv}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_FILE="/etc/default/${SERVICE_NAME}"
RUNTIME_DIR="/run/aenv"
VIRTUALIZATION_MODE="${AENV_VIRTUALIZATION_MODE:-kvm}"

case "$VIRTUALIZATION_MODE" in
    kvm|pvm) ;;
    *)
        echo "error: unsupported AENV_VIRTUALIZATION_MODE=${VIRTUALIZATION_MODE@Q}; expected kvm or pvm" >&2
        exit 1
        ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH_TAG="x86_64" ;;
    aarch64|arm64) ARCH_TAG="aarch64" ;;
    *)
        echo "error: unsupported architecture: $ARCH (supported: x86_64/amd64, aarch64/arm64)" >&2
        exit 1
        ;;
esac

if [[ "$VIRTUALIZATION_MODE" == "pvm" && "$ARCH_TAG" != "x86_64" ]]; then
    echo "error: PVM virtualization mode is only supported on x86_64 hosts" >&2
    exit 1
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$OS" != "linux" ]]; then
    echo "error: AgentENV server only supports Linux" >&2
    exit 1
fi

RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
if [[ "$VIRTUALIZATION_MODE" == "pvm" ]]; then
    TARBALL="aenv-server-${OS}-${ARCH_TAG}-pvm.tar.gz"
else
    TARBALL="aenv-server-${OS}-${ARCH_TAG}.tar.gz"
fi

curl_get() {
    curl -fsSL --retry 5 --retry-delay 10 --retry-max-time 60 "$@"
}

missing_packages=()
command -v curl >/dev/null 2>&1 || missing_packages+=(curl)
command -v jq >/dev/null 2>&1 || missing_packages+=(jq)
command -v sha256sum >/dev/null 2>&1 || missing_packages+=(coreutils)
command -v realpath >/dev/null 2>&1 || missing_packages+=(coreutils)

if ((${#missing_packages[@]} > 0)); then
    if command -v apt-get >/dev/null 2>&1; then
        echo "Installing required commands: ${missing_packages[*]} ..."
        sudo apt-get update
        sudo apt-get install -y "${missing_packages[@]}"
    else
        echo "error: missing required commands and apt-get is unavailable: ${missing_packages[*]}" >&2
        exit 1
    fi
fi

for command in curl jq sha256sum realpath; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command is still unavailable after installation: ${command}" >&2
        exit 1
    fi
done

if [[ ! "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]*$ || ! "$SERVICE_GROUP" =~ ^[a-z_][a-z0-9_-]*$ ]]; then
    echo "error: invalid AgentENV service user or group" >&2
    exit 1
fi

resolved_data_dir="$(realpath -m "$DATA_DIR")"
if [[ "$resolved_data_dir" == "/" || -L "$DATA_DIR" ]]; then
    echo "error: refusing unsafe AgentENV data directory: ${DATA_DIR}" >&2
    exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
    sudo groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    sudo useradd --system --gid "$SERVICE_GROUP" --home-dir "$DATA_DIR" \
        --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if [[ "$(id -u "$SERVICE_USER")" == "0" || "$(getent group "$SERVICE_GROUP" | cut -d: -f3)" == "0" ]]; then
    echo "error: AgentENV service user and group must be non-root" >&2
    exit 1
fi

release_metadata="$(mktemp)"
tmp_cli="$(mktemp)"
tmp_tarball="$(mktemp)"
tmp_dir="$(mktemp -d)"
current_env=""
tmp_env=""
trap 'rm -f "$release_metadata" "$tmp_cli" "$tmp_tarball" "$current_env" "$tmp_env"; rm -rf "$tmp_dir"' EXIT

api_headers=(
    -H "Accept: application/vnd.github+json"
    -H "X-GitHub-Api-Version: 2022-11-28"
)

curl_get "${api_headers[@]}" "$RELEASE_API" -o "$release_metadata"

download_release_asset() {
    local asset_name="$1"
    local destination="$2"
    local asset_json url digest expected actual

    asset_json="$(
        jq -cer --arg name "$asset_name" \
            '[.assets[] | select(.name == $name)] |
             if length == 1 then .[0] else error("release asset not found or not unique") end' \
            "$release_metadata"
    )" || {
        echo "error: release asset not found or not unique: ${asset_name}" >&2
        exit 1
    }
    url="$(jq -r '.browser_download_url // empty' <<< "$asset_json")"
    digest="$(jq -r '.digest // empty' <<< "$asset_json")"
    if [[ -z "$url" ]]; then
        echo "error: GitHub did not provide a download URL for ${asset_name}" >&2
        exit 1
    fi
    if [[ "$digest" != sha256:* ]]; then
        echo "error: GitHub did not provide a SHA256 digest for ${asset_name}" >&2
        exit 1
    fi

    curl_get "$url" -o "$destination"
    expected="${digest#sha256:}"
    actual="$(sha256sum "$destination" | awk '{print $1}')"
    if [[ "$expected" != "$actual" ]]; then
        echo "error: SHA256 mismatch for ${asset_name}" >&2
        echo "  expected: ${expected}" >&2
        echo "  actual:   ${actual}" >&2
        exit 1
    fi
}

sudo mkdir -p "$INSTALL_DIR"

# ---------------------------------------------------------------------------
# 1. Install the aenv CLI
# ---------------------------------------------------------------------------
echo "Downloading aenv CLI ..."
download_release_asset "aenv-linux-${ARCH_TAG}" "$tmp_cli"
sudo install -m 0755 "$tmp_cli" "${INSTALL_DIR}/aenv"

# ---------------------------------------------------------------------------
# 2. Install the server
# ---------------------------------------------------------------------------
if [[ -d /run/systemd/system ]] && systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    echo "Stopping existing ${SERVICE_NAME} service ..."
    sudo systemctl stop "${SERVICE_NAME}"
fi

echo "Downloading ${TARBALL} ..."
download_release_asset "$TARBALL" "$tmp_tarball"

tar -xzf "$tmp_tarball" -C "$tmp_dir"

sudo mkdir -p "$(dirname "$UBLK_DAEMON_PATH")"
sudo install -m 0755 "$tmp_dir/server" "${INSTALL_DIR}/server"
sudo install -m 0755 "$tmp_dir/ublk/uvm-ublk-daemon" "$UBLK_DAEMON_PATH"

if [[ -d "$tmp_dir/deps" ]]; then
    sudo rm -rf \
        "$DEPS_DIR/firecracker" \
        "$DEPS_DIR/kernel" \
        "$DEPS_DIR/tools" \
        "$DEPS_DIR/overlaybd" \
        "$DEPS_DIR/regctl"
    sudo mkdir -p "$DEPS_DIR"
    sudo cp -a "$tmp_dir/deps/." "$DEPS_DIR/"
    sudo chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DEPS_DIR"
fi

if [[ -f "$tmp_dir/etc/overlaybd/overlaybd.json" && ! -f "/etc/overlaybd/overlaybd.json" ]]; then
    sudo install -D -m 0644 "$tmp_dir/etc/overlaybd/overlaybd.json" /etc/overlaybd/overlaybd.json
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
    sudo mkdir -p "$(dirname "$CONFIG_PATH")"
    sudo install -o root -g "$SERVICE_GROUP" -m 0640 "$tmp_dir/default.toml" "$CONFIG_PATH"
fi

if [[ "$SKIP_SETUP" == "1" ]]; then
    echo "Skipping setup (SKIP_SETUP=1)."
else
    echo "Running dependency setup ..."
    sudo AENV_CONFIG_PATH="${CONFIG_PATH}" AENV_HOME_PATH="${DATA_DIR}" \
        AENV_VIRTUALIZATION_MODE="${VIRTUALIZATION_MODE}" \
        "${INSTALL_DIR}/server" --setup-only

    echo "Provisioning virtualization device access, ublk, and host networking for ${SERVICE_USER} ..."
    sudo AENV_CONFIG_PATH="${CONFIG_PATH}" AENV_HOME_PATH="${DATA_DIR}" \
        AENV_VIRTUALIZATION_MODE="${VIRTUALIZATION_MODE}" \
        "${INSTALL_DIR}/server" --setup-host \
        --runtime-user "$SERVICE_USER" --runtime-group "$SERVICE_GROUP"
fi

# Restrict ownership migration to the dedicated AgentENV data tree. External
# repositories configured outside this tree are intentionally untouched.
sudo install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_DIR"
sudo chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DATA_DIR"
sudo install -d -o root -g "$SERVICE_GROUP" -m 0750 "$(dirname "$CONFIG_PATH")"
sudo chown root:"$SERVICE_GROUP" "$CONFIG_PATH"
sudo chmod 0640 "$CONFIG_PATH"

# ---------------------------------------------------------------------------
# 3. Configure systemd
# ---------------------------------------------------------------------------
if [[ -d /run/systemd/system ]]; then
    ENV_FILE_STATUS="exists"
    if [[ ! -f "$ENV_FILE" ]]; then
        sudo install -o root -g "$SERVICE_GROUP" -m 0640 /dev/null "$ENV_FILE"
        sudo tee "$ENV_FILE" > /dev/null <<EOF
API_ADDR="127.0.0.1:8000"
AENV_CONFIG_PATH="${CONFIG_PATH}"
AENV_HOME_PATH="${DATA_DIR}"
AENV_RUNTIME_PATH="${RUNTIME_DIR}"
AENV_VIRTUALIZATION_MODE="${VIRTUALIZATION_MODE}"
EOF
        ENV_FILE_STATUS="written"
    else
        if [[ "$CONFIG_PATH" == *$'\n'* || "$CONFIG_PATH" == *$'\r'* ||
              "$DATA_DIR" == *$'\n'* || "$DATA_DIR" == *$'\r'* ]]; then
            echo "error: AgentENV paths must not contain newlines" >&2
            exit 1
        fi

        escaped_config="${CONFIG_PATH//\\/\\\\}"
        escaped_config="${escaped_config//\"/\\\"}"
        escaped_data="${DATA_DIR//\\/\\\\}"
        escaped_data="${escaped_data//\"/\\\"}"
        current_env="$(mktemp)"
        tmp_env="$(mktemp)"
        # sudo is needed for the read; the redirect intentionally targets the
        # invoking user's temporary file.
        # shellcheck disable=SC2024
        sudo cat "$ENV_FILE" > "$current_env"
        found_config=0
        found_home=0
        found_runtime=0
        found_virtualization=0
        while IFS= read -r line || [[ -n "$line" ]]; do
            case "$line" in
                AENV_CONFIG_PATH=*)
                    printf 'AENV_CONFIG_PATH="%s"\n' "$escaped_config" >> "$tmp_env"
                    found_config=1
                    ;;
                AENV_HOME_PATH=*)
                    printf 'AENV_HOME_PATH="%s"\n' "$escaped_data" >> "$tmp_env"
                    found_home=1
                    ;;
                AENV_RUNTIME_PATH=*)
                    printf 'AENV_RUNTIME_PATH="%s"\n' "$RUNTIME_DIR" >> "$tmp_env"
                    found_runtime=1
                    ;;
                AENV_VIRTUALIZATION_MODE=*)
                    printf 'AENV_VIRTUALIZATION_MODE="%s"\n' "$VIRTUALIZATION_MODE" >> "$tmp_env"
                    found_virtualization=1
                    ;;
                *)
                    printf '%s\n' "$line" >> "$tmp_env"
                    ;;
            esac
        done < "$current_env"
        if [[ "$found_config" == "0" ]]; then
            printf 'AENV_CONFIG_PATH="%s"\n' "$escaped_config" >> "$tmp_env"
        fi
        if [[ "$found_home" == "0" ]]; then
            printf 'AENV_HOME_PATH="%s"\n' "$escaped_data" >> "$tmp_env"
        fi
        if [[ "$found_runtime" == "0" ]]; then
            printf 'AENV_RUNTIME_PATH="%s"\n' "$RUNTIME_DIR" >> "$tmp_env"
        fi
        if [[ "$found_virtualization" == "0" ]]; then
            printf 'AENV_VIRTUALIZATION_MODE="%s"\n' "$VIRTUALIZATION_MODE" >> "$tmp_env"
        fi
        sudo install -o root -g "$SERVICE_GROUP" -m 0640 "$tmp_env" "$ENV_FILE"
        rm -f "$current_env" "$tmp_env"
        ENV_FILE_STATUS="updated"
    fi

    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=AgentENV Server
After=network.target

[Service]
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
SupplementaryGroups=kvm
EnvironmentFile=${ENV_FILE}
ExecStart=${INSTALL_DIR}/server
RuntimeDirectory=aenv
RuntimeDirectoryMode=0750
AmbientCapabilities=CAP_NET_ADMIN CAP_SYS_ADMIN
CapabilityBoundingSet=CAP_NET_ADMIN CAP_SYS_ADMIN
NoNewPrivileges=true
UMask=0027
LimitNOFILE=1048576
LimitMEMLOCK=infinity
Restart=on-failure
RestartSec=5
# KillMode=process lets the server pause and persist running sandboxes before
# exiting. The default control-group mode would SIGKILL all Firecracker child
# processes immediately, losing in-memory sandbox state.
KillMode=process
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable "${SERVICE_NAME}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Installation complete."
echo ""
echo "  CLI    : ${INSTALL_DIR}/aenv"
echo "  Server : ${INSTALL_DIR}/server"
echo "  Data   : ${DATA_DIR}"
echo "  Config : ${CONFIG_PATH}"
echo "  API key: ${DATA_DIR}/secrets/api-key (auto-generated when no API key is configured)"
echo "  Mode   : ${VIRTUALIZATION_MODE}"
if [[ -d /run/systemd/system ]]; then
    if [[ "$ENV_FILE_STATUS" == "written" ]]; then
        echo "  Env    : ${ENV_FILE}"
    else
        echo "  Env    : ${ENV_FILE} (${ENV_FILE_STATUS})"
    fi
    echo "  Service: ${SERVICE_FILE}"
fi
echo ""
if [[ -d /run/systemd/system ]]; then
    echo "Start the server:"
    echo "  sudo systemctl start ${SERVICE_NAME}"
    echo ""
    echo "To change the listen port, edit API_ADDR in ${ENV_FILE} then run:"
    echo "  sudo systemctl restart ${SERVICE_NAME}"
    echo ""
    echo "Check status / logs:"
    echo "  sudo systemctl status ${SERVICE_NAME}"
    echo "  sudo journalctl -u ${SERVICE_NAME} -f"
else
    echo "systemd not detected. Start the server manually:"
    echo "  sudo setpriv --reuid=${SERVICE_USER} --regid=${SERVICE_GROUP} --init-groups \\"
    echo "    --inh-caps=+net_admin,+sys_admin --ambient-caps=+net_admin,+sys_admin \\"
    echo "    --bounding-set=-all,+net_admin,+sys_admin --nnp \\"
    echo "    env AENV_CONFIG_PATH=${CONFIG_PATH} AENV_HOME_PATH=${DATA_DIR} AENV_RUNTIME_PATH=${RUNTIME_DIR} \\"
    echo "    AENV_VIRTUALIZATION_MODE=${VIRTUALIZATION_MODE} \\"
    echo "    API_ADDR=127.0.0.1:8000 ${INSTALL_DIR}/server"
fi
echo ""
echo "For aenv CLI:"
echo "  Run 'aenv --help' to get started."
