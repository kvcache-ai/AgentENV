# Docker Compose (Multi-Node Simulation)

Run a full multi-node stack on a single host using Docker Compose. This simulates a production-like topology with a gateway, scheduler, and multiple AgentENV backend nodes.

For a real multi-machine deployment without Kubernetes, see
[Static Multi-Node](./static-multi-node.md).

## What Gets Started

| Service | Port | Description |
|---------|------|-------------|
| Gateway | `:8080` | HTTP/WebSocket reverse proxy |
| Scheduler | `:9090` | gRPC node selection and sandbox binding |
| agentenv-a | `:8001` | AgentENV runtime node A |
| agentenv-b | `:8002` | AgentENV runtime node B |

## Prerequisites

- Linux kernel 6.8+
- `/dev/kvm` access (passed into the runtime containers)
- Docker and Docker Compose
- `build-essential` (`sudo apt install -y build-essential`)

The checked-in Compose setup uses standard KVM. If the host does not support
it, read [PVM Deployment](./pvm.md) before adapting the runtime image and host
configuration.

## Clone the Repository

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Configure the Shared Access-Token Seed

Multi-node deployments should explicitly configure one envd access-token seed
and use the same value on every **runtime node**. Do not rely on the node-local
auto-generated seed in a scheduler-managed deployment.

Generate the value once and store it in your secret manager:

```bash
openssl rand -hex 32
```

For the checked-in Compose stack, create a private configuration copy outside
the repository and set the generated value in it:

```bash
install -d -m 0700 "$HOME/.config/agentenv"
install -m 0600 config/default.toml "$HOME/.config/agentenv/cluster.toml"
```

```toml
[sandbox]
access_token_hash_seed = "<shared-secret>"
```

Changing this value rotates the access tokens for existing secure sandboxes.

## Start the Cluster

```bash
sudo bash scripts/docker-setup.sh
CONFIG_PATH="$HOME/.config/agentenv/cluster.toml" make deploy-up
```

To enable host-based sandbox data-plane URLs, set the shared sandbox proxy
domain variable when starting the stack:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
CONFIG_PATH="$HOME/.config/agentenv/cluster.toml" make deploy-up
```

Compose passes this value to both the gateway routing allowlist and runtime
nodes' sandbox response metadata. The domain must resolve to the gateway,
usually through wildcard DNS for `*.sandbox.example.com`.

## Verify

```bash
# Health check via gateway
curl http://127.0.0.1:8080/health

# Cluster node snapshots via gateway
curl http://127.0.0.1:8080/nodes

# Direct health check on a backend node
curl http://127.0.0.1:8001/health
```

## Management Commands

```bash
make deploy-ps      # Show container status
make deploy-logs    # Stream logs from all services
make deploy-down    # Tear down the cluster
```

## Configuration

Container deployments use `deploy/docker/config/default.json`. Scheduler and backend node endpoints are configured for the Docker network.

The runtime image includes `uvm-ublk` at `/usr/local/bin/uvm-ublk`. Compose uses that path instead of a host-built `env/ublk/uvm-ublk` binary.

The compose manifest also wires node heartbeat reporting from runtime nodes to scheduler:

- `AENV_NODE_ID` is set explicitly per node container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables scheduler heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both
  `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.
