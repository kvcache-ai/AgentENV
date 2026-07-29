<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/heading-logo-dark.svg" />
    <img src="assets/heading-logo.svg" alt="AgentENV" />
  </picture>
  <p><strong>Running agent environments at scale</strong></p>
</div>

[![Coverage workflow status](https://github.com/kvcache-ai/AgentENV/actions/workflows/coverage.yml/badge.svg?branch=main&event=push)](https://github.com/kvcache-ai/AgentENV/actions/workflows/coverage.yml)
[![Latest coverage report](https://github.com/kvcache-ai/AgentENV/blob/coverage-data/coverage/badge.svg?raw=1)](https://github.com/kvcache-ai/AgentENV/blob/coverage-data/coverage/coverage.json)
📖 [Full documentation](https://kvcache-ai.github.io/AgentENV/)

AgentENV (AENV) is a platform for running agent environments at scale, powering agentic RL training for **Kimi K3**.

---

## 🚀 Why AgentENV

- **Scale across diverse environments**: AENV runs massive numbers of Firecracker environments across machines and diverse OCI-compatible images, loaded on demand via [overlaybd](https://containerd.github.io/overlaybd/#/). Local disk acts as a bounded cache, retaining hot data and evicting cold, so images can exceed disk capacity while startup stays fast cluster-wide, without pre-warming every host.
- **Make idle environments inexpensive**: Snapshot-backed environments boot or resume in under 50 ms and pause in under 100 ms. Idle environments can quickly release CPU and memory, then return when new work arrives.
- **Native snapshot and fork support**: AENV snapshots memory and filesystem changes incrementally, completing in under 100 ms even under heavy disk modification. A running environment can fork into multiple independent sandboxes for parallel agent workflows. Snapshots persist to S3-compatible object storage or a shared distributed filesystem to prevent data loss.
- **Preserve performance and density over time**: AENV delivers high-performance I/O via ublk while sharing the host page cache across storage and memory-snapshot data. Memory ballooning returns reclaimable guest memory to the host, sustaining high overcommit as environments run longer and diverge.

---

## 📋 Prerequisites

- **Linux kernel 6.8+**; the install script additionally requires **Ubuntu 24.04** (see *Quick Start* below for installation options)
- `/dev/kvm` access for Firecracker microVM execution

---

## ⚡ Quick Start (Single Node)

> [!WARNING]
> **AgentENV currently does not support authorization.** Do not expose the AgentENV
> API to the public network. Run it only on a trusted network or behind an
> authorization proxy with appropriate network controls.

**1. Install and start the server**

*Option A — install script (Ubuntu 24.04)*

Install both the server and the `aenv` CLI, then start the server as a systemd service:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh | sudo bash
sudo systemctl start aenv
```

*Option B — Docker*

Set up the server:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
docker pull ghcr.io/kvcache-ai/aenv-server:latest
docker run -d --privileged -v /dev:/dev -p 8000:8000 ghcr.io/kvcache-ai/aenv-server:latest
```

The server is accessible at `http://127.0.0.1:8000` by default.

**2. Install the aenv CLI** *(skip if you used Option A in step 1)*

Install separately if you used the Docker method above, or if you are running
the CLI on a different machine from the server. Supports Linux and macOS on
x86_64 and arm64:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install-cli.sh | bash
```

**3. Authenticate**

```bash
aenv auth
# AENV server URL [http://localhost:8000]: http://127.0.0.1:8000
# API key: dummy
```

**4. Pull a template and run a sandbox**

```bash
aenv pull ubuntu:22.04 --name ubuntu
aenv start ubuntu            # starts a sandbox and attaches an interactive shell
```

---

## 🗂 Deployment

For Docker Compose / Kubernetes cluster deployment and build-from-source instructions,
see 📖 [Deployment](https://kvcache-ai.github.io/AgentENV/deployment/manual-compile.html).

---

## 🔌 E2B compatibility

AgentENV exposes an E2B-compatible HTTP API. Point `E2B_API_URL` at your
server and use the standard E2B Python / TypeScript SDK without any code
changes. See 📖 [E2B integration](https://kvcache-ai.github.io/AgentENV/integration/e2b.html)
for setup details.

---

## 🛠 aenv CLI reference

```bash
# Templates
aenv pull docker.io/library/ubuntu:latest --name ubuntu    # FROM <image> → template
aenv template list                      # alias: aenv template ls

# Sandboxes
aenv start ubuntu                       # start + attach interactive shell
aenv start ubuntu --detach              # start, print sandbox ID, don't attach
aenv cn <sandbox-id>                    # reattach a shell
aenv exec <sandbox-id> ls -la /         # one-shot command
aenv ls

aenv pause   <sandbox-id>
aenv resume  <sandbox-id>
aenv timeout <sandbox-id> 600           # extend TTL to 600 s from now
aenv delete  <sandbox-id>               # alias: aenv rm
```

`aenv start` accepts a template UUID or human-readable name/alias. `aenv list`
outputs a table on TTY and JSON when piped; override with `--output table|json`.

---

## 🤝 Contributing

Bug reports, feature proposals, documentation fixes, and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or submitting a change.

Follow [SECURITY.md](SECURITY.md) to report security vulnerabilities privately; do not disclose them in a public issue.
