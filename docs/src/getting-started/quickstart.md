# Quick Start

## Prerequisites

- **Linux kernel 6.8+**
- `/dev/kvm` access for Firecracker microVM execution

> If your server does not support standard KVM, use the dedicated
> [PVM Deployment](../deployment/pvm.md) guide instead.

The install script attempts to install missing download and checksum commands,
provisions `/dev/kvm` permissions, loads the `ublk_drv` kernel module, and
downloads the required AgentENV runtime assets.

Installation requires root, but the installed service does not run as root. It
uses a dedicated `aenv` system account with `CAP_NET_ADMIN` and
`CAP_SYS_ADMIN`, plus group access to `/dev/kvm` and the ublk devices.

## Setup

### 1. Install and start the server

**Option A — Install Script**

The script installs both the server and the `aenv` CLI. Set `AENV_HOME_PATH` to
choose the data directory; if it is not set, AENV stores runtime dependencies
and data in `/var/lib/aenv`. After installation, start the server as a systemd
service:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh | sudo AENV_HOME_PATH=/path/to/aenv/data bash
sudo systemctl start aenv
```

To customize the server configuration, edit `config.toml` and restart the service:

```bash
sudo vim /var/lib/aenv/config/config.toml  # Or <AENV_HOME_PATH>/config/config.toml if AENV_HOME_PATH is set.
sudo systemctl restart aenv
```

To change the port, edit `API_ADDR` in `/etc/default/aenv` and restart the service.

The service's persistent state is owned by `aenv` under `/var/lib/aenv` by
default. Transient namespace and daemon-socket state lives under `/run/aenv`.

**Option B — Docker**

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
docker pull ghcr.io/kvcache-ai/aenv-server:latest
docker run --rm -it \
  --name aenv-server \
  --device /dev/kvm --privileged -v /dev:/dev \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest
```

To customize the server configuration, download and edit the configuration file, then mount it at the path used by the container:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/config/default.toml -o config.toml
vim config.toml
docker run --rm -it \
  --name aenv-server \
  --device /dev/kvm --privileged -v /dev:/dev \
  -v "$PWD/config.toml:/workspace/config/default.toml:ro" \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest
```

To change the port, add the `-e API_ADDR` and `-p` flags.

The server is accessible at `http://127.0.0.1:8000` by default. Verify it is running:

```bash
curl http://127.0.0.1:8000/health
```

### 2. Install the aenv CLI

Skip this step if you used **Option A** — the install script already includes `aenv`.

Supports Linux and macOS on x86_64 and arm64:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install-cli.sh | bash
```

### 3. Authenticate

The server generates the key on its first normal startup. Native installations
reuse the managed key; a normal Docker container keeps it in its writable
container layer:

```bash
# Native
sudo cat /var/lib/aenv/secrets/api-key
# Docker
docker exec aenv-server cat /workspace/env/secrets/api-key
```

```bash
aenv auth
# AENV server URL [http://localhost:8000]: http://127.0.0.1:8000
# API key: <paste the generated key>
```

### 4. Pull a template and run a sandbox

```bash
aenv pull ubuntu:22.04 --name ubuntu
aenv start ubuntu            # starts a sandbox and attaches an interactive shell
```

## Next Steps

- [Deployment](../deployment/manual-compile.md) — build from source, multi-node options
- [PVM Deployment](../deployment/pvm.md) — deploy when standard KVM is unavailable
- [Core Concepts](../concepts/overview.md) — how sandboxes, templates, and snapshots work
- [E2B](../integration/e2b.md) — SDK and CLI compatibility
- [API Reference](../api/index.md) — full HTTP API
