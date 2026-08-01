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

## Clone the Repository

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Start the Cluster

```bash
sudo bash scripts/docker-setup.sh
make deploy-up
```

`docker-setup.sh` generates `/etc/aenv/auth.env` once. The Makefile reads
that protected file and Compose passes the same key to the gateway and both
runtime nodes.

To enable host-based sandbox data-plane URLs, set the shared sandbox proxy
domain variable when starting the stack:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Compose passes this value to both the gateway routing allowlist and runtime
nodes' sandbox response metadata. The domain must resolve to the gateway,
usually through wildcard DNS for `*.sandbox.example.com`.

## Verify

```bash
# Health check via gateway
curl http://127.0.0.1:8080/health

# Authenticated cluster node snapshots via gateway
export AENV_API_KEY="$(sudo sed -n 's/^AENV_API_KEY=//p' /etc/aenv/auth.env)"
curl -H "X-API-Key: ${AENV_API_KEY}" http://127.0.0.1:8080/nodes

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
