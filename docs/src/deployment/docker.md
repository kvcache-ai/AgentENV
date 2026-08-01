# Docker (Single Node)

Run a single AgentENV node in a Docker container. This avoids installing the Rust toolchain on the host but still requires `/dev/kvm`.

## Prerequisites

- Linux kernel 6.8+
- `/dev/kvm` access for Firecracker microVM execution
- Docker

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
  --env-file /etc/aenv/auth.env \
  --device /dev/kvm --privileged -v /dev:/dev \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest   # or aenv:latest if built from source
```

The `--privileged` flag is required for Firecracker's network namespace operations (veth pairs, iptables). The server auto-downloads runtime assets on first start and is accessible at `http://127.0.0.1:8000` once ready.

`docker-setup.sh` generates the shared API key once in
`/etc/aenv/auth.env`. The `--env-file` option passes it to the container
without placing the key directly in the command line.

## Verify

```bash
curl http://127.0.0.1:8000/health
```
