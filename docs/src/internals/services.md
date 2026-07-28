# Distributed Control Plane

The multi-node control plane lives in `services/` as a separate Go module. It routes client traffic across multiple AgentENV backend nodes.

## Components

- **Gateway** (`services/gateway/`): HTTP reverse proxy that routes by sandbox ID
- **Scheduler** (`services/scheduler/`): gRPC service for node selection, sandbox-to-node binding, observed node snapshots, and P2P peer endpoint discovery

## Build and Test

Prerequisites: Go 1.21+

```bash
# From services/
make build      # builds both gateway and scheduler
make test       # tests both services
make tidy       # go mod tidy + formatting
make proto      # regenerate protobuf
```

## Run Locally

```bash
# Start scheduler (default: 127.0.0.1:9090)
make -C services run-scheduler

# Start gateway
make -C services run-gateway
```

## Discovery Modes

The scheduler supports two node discovery modes:

- **static** (default): explicit node list from config
- **kubernetes**: watches EndpointSlices for a headless Service, using ready Pod IPs as backends

## Deployment

### Docker Compose

```bash
make deploy-up      # gateway + scheduler + 2 backend nodes
make deploy-ps      # status
make deploy-logs    # logs
make deploy-down    # teardown
```

### Kubernetes

```bash
make k8s-render     # render manifests
make k8s-apply      # apply to cluster
```

Deployment model:
- `gateway`: Deployment + ClusterIP Service
- `scheduler`: single-replica Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm`, one host-compatible
  KVM/PVM mode, and hostPath
- `agentenv-nodes`: headless Service for scheduler EndpointSlice discovery

## gRPC API

Proto contract: `services/api/proto/scheduler.proto`

RPCs: `Schedule`, `ListNodes`, `LookupNode`, `RecordAssignment`, `Heartbeat`, `ListObservedNodes`, `ListP2pPeers`, `GetNode`, `UnregisterNode`

Runtime node heartbeats may include an opaque `P2pEndpoint` containing a backend name and backend-specific address. The scheduler stores that endpoint with the observed-node record and returns ready peers through `ListP2pPeers(cluster_id, backend, exclude_node_id)`. The scheduler does not query artifact catalogs and never forwards artifact data.

For full configuration details (header compatibility, timeouts, logging), see the [services README](https://github.com/kvcache-ai/AgentENV/blob/main/services/README.md).
