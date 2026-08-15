# Docker Compose (Multi-Node Simulation)

Run a full multi-node stack on a single host using Docker Compose. This simulates a production-like topology with a gateway, scheduler, and multiple AgentENV backend nodes.

For a real multi-machine deployment without Kubernetes, see
[Static Multi-Node](./static-multi-node.md).

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

## Start the Cluster

```bash
sudo bash scripts/docker-setup.sh
make deploy-up
```

The Gateway is available at `http://127.0.0.1:8000` and forwards requests to
the backend nodes.

On first startup, the runtime nodes atomically generate one API key and sandbox
access-token seed in the
shared `agentenv-auth` volume. The gateway mounts that volume read-only at
`/run/secrets`, so all three services use the same secrets. Normal
`make deploy-down` calls preserve the volume and both values.

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
curl http://127.0.0.1:8000/health

# Cluster node snapshots via gateway
curl http://127.0.0.1:8000/nodes

# Authenticated cluster node snapshots via gateway
export AENV_API_KEY="$(docker compose -f deploy/docker-compose.yml exec -T agentenv-a \
  cat /workspace/env/secrets/api-key)"
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

Removing Compose volumes with `docker compose down -v` also removes both
secrets. The next startup generates new values, so existing clients and sandbox
access tokens are invalidated.

To provide an existing key through Docker Compose secrets, add a file-backed
secret in an override file and mount it with `target: api-key` on the gateway
and both runtime nodes. AgentENV automatically reads `/run/secrets/api-key`;
no file-path environment variable is needed.

```yaml
services:
  gateway:
    secrets: [api-key]
  agentenv-a:
    secrets: [api-key]
  agentenv-b:
    secrets: [api-key]

secrets:
  api-key:
    file: ./api-key
```

The secret name is also its default target filename, so this mounts the key at
`/run/secrets/api-key` in each service.

## Configuration

Container deployments use `deploy/docker/config/default.json`. Scheduler and backend node endpoints are configured for the Docker network.

The runtime image includes `uvm-ublk` at `/usr/local/bin/uvm-ublk`. Compose uses that path instead of a host-built `env/ublk/uvm-ublk` binary.

The compose manifest also wires node heartbeat reporting from runtime nodes to scheduler:

- `AENV_NODE_ID` is set explicitly per node container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables scheduler heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both
  `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.
