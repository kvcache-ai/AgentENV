# Docker (Single Node)

Run a single AgentENV node in a Docker container. This avoids installing the Rust toolchain on the host but still requires `/dev/kvm`.

## Prerequisites

- Linux kernel 6.8+
- `/dev/kvm` access for Firecracker microVM execution
- Docker

If the server does not support standard KVM, follow
[PVM Deployment](./pvm.md) for the required host setup and PVM image.

## Build

**Option A — Pre-built Image**

```bash
docker pull ghcr.io/kvcache-ai/aenv-server:latest

curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
```

**Option B — Build from Source**

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
sudo bash scripts/docker-setup.sh
docker build -f deploy/docker/Dockerfile.agentenv -t aenv:latest .
```

To use a regional apt mirror for both build and runtime stages, pass a base URL
that contains `debian`, `debian-security`, and `ubuntu` mirror paths:

```bash
docker build \
  --build-arg APT_MIRROR_BASE=https://mirrors.example.com \
  -f deploy/docker/Dockerfile.agentenv \
  -t aenv:latest .
```

## Run

```bash
docker run --rm -it \
  --name aenv-server \
  --device /dev/kvm --privileged -v /dev:/dev \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest   # or aenv:latest if built from source
```

The `--privileged` flag is required for Firecracker's network namespace operations (veth pairs, iptables). The server auto-downloads runtime assets on first start and is accessible at `http://127.0.0.1:8000` once ready.

On normal startup, the server generates the API key inside the container at
`/workspace/env/secrets/api-key`. Read it while the container is running with:

```bash
docker exec aenv-server cat /workspace/env/secrets/api-key
```

Removing the container also removes this generated key. Supply an explicit
`AENV_API_KEY` or a secret at `/run/secrets/api-key` when the key must remain
stable across container replacements.

## Verify

```bash
curl http://127.0.0.1:8000/health
```
