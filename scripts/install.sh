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
#            sudo server --setup-host  (provisions KVM, ublk, and networking)
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

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64) ARCH_TAG="x86_64" ;;
    *)
        echo "error: unsupported architecture: $ARCH (server requires x86_64)" >&2
        exit 1
        ;;
esac

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$OS" != "linux" ]]; then
    echo "error: AgentENV server only supports Linux" >&2
    exit 1
fi

RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
TARBALL="aenv-server-${OS}-${ARCH_TAG}.tar.gz"

curl_get() {
    curl -fsSL --retry 5 --retry-delay 10 --retry-max-time 60 "$@"
}

# BEGIN aenv_completion_install
# (keep in sync across scripts/shell-completion.sh, scripts/install-cli.sh,
# scripts/install.sh — verified by scripts/check-completion-sync.sh)

# Write a single completion loader file. Non-fatal on I/O errors.
#   $1 destination path
#   $2 file mode (e.g. 0644)
#   $3 loader content (single line; the loaders are one-liners by design)
_aenv_cc_put() {
    local path="$1" mode="$2" content="$3"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    if ! printf '%s\n' "$content" > "$path" 2>/dev/null; then
        printf 'warn: aenv completion: could not write %s\n' "$path" >&2
        return 0
    fi
    chmod "$mode" "$path" 2>/dev/null || true
}

# Append the regenerating zsh rc-snippet to ~/.zshrc, idempotently.
#   $1 rc file path
_aenv_cc_put_zsh_rc() {
    local rc="$1"
    local dir="${rc%/*}"
    if [[ -f "$rc" ]] && grep -q '^# >>> aenv completion >>>$' "$rc"; then
        return 0 # already installed
    fi
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    # Start the block on its own line only when the rc file is non-empty and
    # does not already end with a newline; this avoids leaving a stray blank
    # line behind after uninstall.
    local leader="" last_byte
    if [[ -s "$rc" ]]; then
        last_byte=$(tail -c 1 "$rc" 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n') || last_byte=""
        [[ "$last_byte" == "0a" ]] || leader=$'\n'
    fi
    if ! {
        printf '%s' "$leader"
        printf '# >>> aenv completion >>>\n'
        printf 'autoload -Uz compinit && compinit\n'
        # shellcheck disable=SC2016 # $(...) is literal text for zsh to eval, not bash
        printf 'eval "$(aenv completion zsh)"\n'
        printf '# <<< aenv completion <<<\n'
    } >> "$rc" 2>/dev/null; then
        printf 'warn: aenv completion: could not append to %s\n' "$rc" >&2
    fi
}

# Generate the static zsh completion from the installed aenv into a
# site-functions dir. Skipped (with a warning) if `aenv` is not on PATH, e.g.
# when installing into a prefix that is not yet on PATH; re-running the
# installer after fixing PATH regenerates it.
#   $1 destination _aenv path
_aenv_cc_put_zsh_static() {
    local path="$1"
    local dir="${path%/*}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        printf 'warn: aenv completion: could not create directory %s\n' "$dir" >&2
        return 0
    fi
    if ! command -v aenv >/dev/null 2>&1; then
        printf 'warn: aenv completion: aenv not on PATH; skipping static zsh file %s\n' "$path" >&2
        return 0
    fi
    if ! aenv completion zsh > "$path" 2>/dev/null; then
        # shellcheck disable=SC2016 # backticks are literal text in a warning
        printf 'warn: aenv completion: `aenv completion zsh` failed; skipping %s\n' "$path" >&2
        return 0
    fi
    chmod 0644 "$path" 2>/dev/null || true
}

# Remove the zsh rc-snippet block from ~/.zshrc. Only acts on a balanced
# marker pair; an unbalanced pair is left untouched to avoid truncating the
# user's rc file.
#   $1 rc file path
_aenv_cc_rm_zsh_rc() {
    local rc="$1"
    [[ -f "$rc" ]] || return 0
    local starts ends tmp
    starts=$(grep -c '^# >>> aenv completion >>>$' "$rc" 2>/dev/null || true)
    ends=$(grep -c '^# <<< aenv completion <<<$' "$rc" 2>/dev/null || true)
    starts="${starts:-0}"
    ends="${ends:-0}"
    [[ "$starts" =~ ^[0-9]+$ ]] || starts=0
    [[ "$ends" =~ ^[0-9]+$ ]] || ends=0
    [[ "$starts" -gt 0 ]] || return 0
    if [[ "$starts" -ne "$ends" ]]; then
        printf 'warn: aenv completion: unbalanced markers in %s; leaving it untouched\n' "$rc" >&2
        return 0
    fi
    tmp="$(mktemp)"
    if awk '
        /^# >>> aenv completion >>>$/ { skip=1; next }
        /^# <<< aenv completion <<<$/ { skip=0; next }
        !skip { print }
    ' "$rc" > "$tmp" && mv -f "$tmp" "$rc" 2>/dev/null; then
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    printf 'warn: aenv completion: could not update %s\n' "$rc" >&2
}

# Install or remove the aenv shell-completion loaders.
#
#   aenv_completion_install install   [--prefix=<P>] [--user]
#   aenv_completion_install uninstall [--prefix=<P>] [--user]
#
# Always returns 0 so completion setup never aborts the surrounding binary
# installer; per-shell problems are reported as warnings on stderr.
aenv_completion_install() {
    local action="" prefix="" user_mode=0
    while (($#)); do
        case "$1" in
            install|uninstall) action="$1"; shift ;;
            --prefix=*) prefix="${1#--prefix=}"; shift ;;
            --user) user_mode=1; shift ;;
            *) printf 'warn: aenv completion: ignoring unknown argument %s\n' "$1" >&2; shift ;;
        esac
    done

    if [[ "$action" != "install" && "$action" != "uninstall" ]]; then
        printf 'warn: aenv completion: expected an install or uninstall action; skipping\n' >&2
        return 0
    fi

    # Auto-select user mode for a bare invocation or a prefix under $HOME.
    # Guard $HOME: if it is unset/empty, the "$prefix" == "$HOME"/* pattern
    # would collapse to "/*" and match any absolute path, and the bare $HOME
    # reference would abort under `set -u`. Capture it once, safely.
    if [[ $user_mode -eq 0 ]]; then
        local home="${HOME:-}"
        if [[ -z "$prefix" || ( -n "$home" && ( "$prefix" == "$home" || "$prefix" == "$home"/* ) ) ]]; then
            user_mode=1
        fi
    fi

    local bash_file fish_file zsh_file zsh_kind
    if [[ $user_mode -eq 1 ]]; then
        bash_file="${HOME}/.local/share/bash-completion/completions/aenv"
        fish_file="${HOME}/.config/fish/completions/aenv.fish"
        zsh_file="${HOME}/.zshrc"
        zsh_kind="rc"
    else
        bash_file="${prefix}/share/bash-completion/completions/aenv"
        fish_file="${prefix}/share/fish/vendor_completions.d/aenv.fish"
        zsh_file="${prefix}/share/zsh/site-functions/_aenv"
        zsh_kind="static"
    fi

    if [[ "$action" == "install" ]]; then
        _aenv_cc_put "$bash_file" 0644 'source <(aenv completion bash)'
        _aenv_cc_put "$fish_file" 0644 'aenv completion fish | source'
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_put_zsh_rc "$zsh_file"
        else
            _aenv_cc_put_zsh_static "$zsh_file"
        fi
    else
        rm -f "$bash_file" "$fish_file" 2>/dev/null || true
        if [[ "$zsh_kind" == "rc" ]]; then
            _aenv_cc_rm_zsh_rc "$zsh_file"
        else
            rm -f "$zsh_file" 2>/dev/null || true
        fi
    fi
    return 0
}

# END aenv_completion_install

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

# Install regenerating shell-completion loaders (best-effort; never aborts the
# install). System-wide install -> system mode writes <prefix>/share loaders.
aenv_completion_install install --prefix="${INSTALL_DIR%/*}"

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
        "${INSTALL_DIR}/server" --setup-only

    echo "Provisioning KVM, ublk, and host networking for ${SERVICE_USER} ..."
    sudo AENV_CONFIG_PATH="${CONFIG_PATH}" AENV_HOME_PATH="${DATA_DIR}" \
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
        sudo tee "$ENV_FILE" > /dev/null <<EOF
API_ADDR="127.0.0.1:8000"
AENV_CONFIG_PATH="${CONFIG_PATH}"
AENV_HOME_PATH="${DATA_DIR}"
AENV_RUNTIME_PATH="${RUNTIME_DIR}"
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
        sudo install -m 0644 "$tmp_env" "$ENV_FILE"
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
    echo "    API_ADDR=127.0.0.1:8000 ${INSTALL_DIR}/server"
fi
echo ""
echo "For aenv CLI:"
echo "  Run 'aenv --help' to get started."
