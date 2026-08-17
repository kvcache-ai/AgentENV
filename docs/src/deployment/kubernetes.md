# Kubernetes (Multi-Node)

Deploy AgentENV across a Kubernetes cluster with a gateway, scheduler, and runtime nodes on every worker.

## Architecture

| Workload | Kind | Description |
|----------|------|-------------|
| `agentenv-gateway` | Deployment + ClusterIP Service | HTTP reverse proxy for client traffic |
| `agentenv-scheduler` | Deployment (single replica) + ClusterIP Service | gRPC node selection and sandbox binding |
| `agentenv-node` | DaemonSet (privileged) | One runtime Pod per Kubernetes node |
| `agentenv-nodes` | Headless Service | Used by the scheduler for EndpointSlice discovery |

### Why a DaemonSet for Runtime Nodes

- Each Pod needs host-local access to `/dev/kvm`
- Sandbox networking uses host iptables and network namespaces
- Runtime assets and committed snapshot state are cached per-host at `/var/lib/aenv`

## Prerequisites

- Kubernetes worker nodes with **Linux kernel 6.8+**
- `/dev/kvm` access on every runtime worker
- Runtime Pods run privileged
- Docker
- `build-essential` (`sudo apt install -y build-essential`)
- `kubectl` with Kustomize support
- shared storage across all runtime nodes, using either POSIXFS or OSS

The provided manifests use standard KVM. To prepare a separate PVM node pool
when standard KVM is unavailable, see [PVM Deployment](./pvm.md).

## Clone the Repository
```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
```

## Build Container Images

```bash
make k8s-build
```

This builds three images: `agentenv-runtime:latest`, `agentenv-gateway:latest`, and `agentenv-scheduler:latest`.

## Configure the Access-Token Seed (Optional)

See [Secure Sandboxes](../security/secure-sandboxes.md) for the optional shared seed configuration and Kubernetes Secret example.

## Deploy

```bash
# Run on each worker node before deploying
sudo bash scripts/docker-setup.sh

# Render manifests (preview)
make k8s-render

# Apply to cluster
make k8s-apply
```

To enable host-based sandbox data-plane URLs, set the shared sandbox proxy
domain variable when rendering or applying manifests:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The helper writes this value into the generated ConfigMap and applies it to
both the gateway routing allowlist and runtime nodes' sandbox response metadata.
The domain must resolve to the gateway Ingress or LoadBalancer, usually through
wildcard DNS for `*.sandbox.example.com`.

The default overlay is `deploy/k8s/overlays/default`, targeting the `agentenv-system` namespace. The gateway is exposed as ClusterIP by default. Add your own Ingress or LoadBalancer for external access.

The make targets build a temporary Kustomize context so runtime Pods mount the repository's `config/default.toml` rather than a separate checked-in copy.

The runtime DaemonSet injects scheduler-report wiring for each node Pod:

- `AENV_UBLK_DAEMON_BINARY_PATH=/usr/local/bin/uvm-ublk-daemon` so the Pod uses the `uvm-ublk-daemon` binary included in the runtime image
- `AENV_NODE_ID` from Pod metadata name (`metadata.name`)
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true`
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT=http://agentenv-scheduler:9090`
- `AENV_SANDBOX_PROXY_DOMAINS` from the shared sandbox proxy ConfigMap

The P2P listen address must be reachable Pod-to-Pod; use a concrete container port or a Pod-reachable address if your cluster policy does not allow dialing ephemeral ports.

## Operations

```bash
# Rollout restart all workloads
make k8s-redeploy

# Delete all resources
make k8s-delete
```

## Local Development (k3s)

A dedicated `local-dev` overlay mounts the repository's `env/` directory directly into the DaemonSet at `/workspace/env`, avoiding runtime asset copies:

This overlay also generates `agentenv-runtime-secrets` with a fixed test-only
seed so local and E2E deployments do not require production secret management.
Do not reuse that value outside local development.

```bash
make k8s-build
make k8s-load-dev       # Import images into k3s/containerd
make k8s-render-dev     # Preview manifests
make k8s-apply-dev      # Apply to cluster
make k8s-refresh-dev    # Build + load + rollout restart (all-in-one)
```

## Service Discovery

The scheduler watches EndpointSlices for the headless `agentenv-nodes` Service and watches Pods for optional label-based discovery policy. It schedules only serving, non-terminating DaemonSet Pods. Pods matching `scheduler.discovery.kubernetes.no_schedule_pod_selector` stay discoverable as lingering/no-schedule nodes, while Pods matching `scheduler.discovery.kubernetes.ignore_pod_selector` are excluded. Both IPv4 and IPv6 endpoint addresses are supported.

> Sandbox bindings remain in-memory, so the scheduler should run as a single replica. Bindings are lost on restart.
