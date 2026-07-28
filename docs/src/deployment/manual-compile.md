# Manual Compile (Single Node)

Run AgentENV directly from source on a single Linux host, useful for development and testing.

If you want to skip building from source, see [Quick Start](../getting-started/quickstart.md).

## Prerequisites

- Ubuntu 24.04 with Linux kernel 6.8+
- `/dev/kvm` access for Firecracker microVM execution
- Rust toolchain (stable) — install via [rustup](https://rustup.rs)
- `sudo` access

If the server does not support standard KVM, follow
[PVM Deployment](./pvm.md) instead of this guide.

## Clone the Repository

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Build

```bash
# Debug build
make

# Release build (recommended for production)
make release
```

## Start the Server

```bash
# Debug build
API_ADDR=0.0.0.0:8000 make start-server

# Release build
API_ADDR=0.0.0.0:8000 make start-server-release
```

The server auto-downloads runtime assets (Firecracker binary, kernel, rootfs) on first start. Once ready, it listens at `http://127.0.0.1:8000`.

## Verify

```bash
curl http://127.0.0.1:8000/health
```

## Configuration

The server reads `config/default.toml` by default. Override with:

```bash
AENV_CONFIG_PATH=/path/to/config.toml make start-server
```

See [Configuration Reference](../configuration/reference.md) for all settings.
