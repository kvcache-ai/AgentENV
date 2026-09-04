# Docker Compose (Multi-Node Simulation)

Run a full multi-node stack on a single host using Docker Compose. This simulates a production-like topology with a gateway, scheduler, and multiple AgentENV backend nodes.

For a real multi-machine deployment without Kubernetes, see
[Static Multi-Node](./static-multi-node.md).

## Prerequisites

- Linux kernel 6.8+
- `/dev/kvm` access (passed into the runtime containers)
- Docker Engine with Docker Compose v2 (`docker compose version`)
- `curl` for the verification commands

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

`make deploy-up` builds the runtime, gateway, and scheduler images with Docker Compose
before starting the stack. The Rust and Go toolchains are installed in the image
build stages, so they are not required on the host.

To build without starting, run `make deploy-build`. To start images that are
already built, run `make deploy-up-no-build`:

```bash
# Build only
make deploy-build

# Start previously built images
make deploy-up-no-build
```

On first startup, the runtime nodes atomically generate one API key and sandbox
access-token seed in the shared `agentenv-auth` volume. The gateway mounts that
volume read-only and reads the API key; sandbox tokens are validated by the
runtime nodes. Normal `make deploy-down` calls preserve both values.

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

# Authenticated cluster node snapshots via gateway
export AENV_API_KEY="$(docker compose -f deploy/docker-compose.yml exec -T agentenv-a \
  cat /workspace/env/secrets/api-key)"
curl -H "X-API-Key: ${AENV_API_KEY}" http://127.0.0.1:8000/nodes

# Health check from inside a backend container
docker compose -f deploy/docker-compose.yml exec -T agentenv-a \
  curl -fsS http://127.0.0.1:8000/health
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

To use an existing API key instead of the generated value, export
`AENV_API_KEY` before starting the stack. Compose passes it to the gateway and
both runtime nodes:

```bash
export AENV_API_KEY="e2b_..."
make deploy-up
```

## Configuration

Container deployments use `deploy/docker/config/default.json`. Scheduler and backend node endpoints are configured for the Docker network.

The compose manifest also wires node heartbeat reporting from runtime nodes to scheduler:

- `AENV_NODE_ID` is set explicitly per node container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables scheduler heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both
  `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.
