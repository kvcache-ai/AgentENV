# services

Go implementation of a distributed Gateway and pluggable Scheduler for AgentENV.

## Features

- Gateway routes control-plane requests by real-time scheduling.
- Gateway aggregates `GET /sandboxes` and `GET /v2/sandboxes` across all scheduler nodes.
- Gateway aggregates `GET /nodes` across all observed nodes in the scheduler.
- Gateway resolves `GET /nodes/{id}` via scheduler and proxies to the target node.
- Gateway routes sandbox requests by existing sandbox-to-node binding.
- Scheduler exposes gRPC API and supports pluggable strategy providers.
- Built-in strategies in v1: round_robin and random.
- Scheduler supports both static node configuration and Kubernetes EndpointSlice discovery.
- Scheduler sandbox binding store can be in-memory or Redis-backed.
- Scheduler can run as a primary read/write service or as query-only replicas that serve only `LookupNode` from Redis.
- Scheduler observes node health and sandbox roster from heartbeats, and drops expired sandbox-to-node bindings on heartbeat, node unregistration, or lookup.
- HTTP and WebSocket forwarding.

## Header compatibility

Gateway treats these headers as sandbox-routing markers:

- x-agentenv-sandbox-id
- e2b-sandbox-id

If one of them exists, gateway resolves node from scheduler binding and forwards request there.

When `gateway.sandbox_proxy_domains` is configured, gateway also accepts host-based sandbox
data-plane URLs in the form `{port}-{sandboxID}.{proxy_domain}`. The host-derived
sandbox ID and port take precedence over conflicting routing headers; the gateway logs
that conflict at debug level and forwards the request to the backend node's `/proxy`
endpoint. Host-based routing requires the sandbox ID to be RFC 952/1123 DNS-label compatible
(`[a-z0-9]([a-z0-9-]*[a-z0-9])?`), and the full `{port}-{sandboxID}` label must fit
within the 63-character DNS label limit.

Sandbox data-plane routing is host- or header-based. Path-derived sandbox IDs are
only used for sandbox control-plane APIs such as `/sandboxes/{id}/pause`; clients
that proxy sandbox traffic through the gateway must use a sandbox proxy host or a
sandbox routing header.

## Build

Prerequisites:

- Go 1.21+

Commands (from `services/`):

```bash
make tidy
make proto
make build      # builds both gateway and scheduler
make test       # tests both services
```

Per-service (from `services/gateway/` or `services/scheduler/`):

```bash
make build
make test
```

## Run locally

Start scheduler:

```bash
make run-scheduler
```

Start gateway:

```bash
make run-gateway
```

The default local config uses `127.0.0.1:9090` for the scheduler.

## Scheduler configuration

Scheduler discovery modes:

- `static` (default): use `scheduler.nodes` from config.
- `kubernetes`: watch EndpointSlices for a headless Service and build the node list from serving Pod endpoints. Terminating endpoints, or Pods matching `no_schedule_pod_selector`, are kept as lingering/no-schedule nodes; Pods matching `ignore_pod_selector` are excluded.

General config notes:

- `scheduler.report_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.binding_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.placement_reservation_ttl` must be a duration string such as `"10m"` in JSON config files.
- `scheduler.report_ttl` controls how long an observed node heartbeat stays healthy.
- `scheduler.binding_ttl` controls how long sandbox-to-node bindings survive without a fresh `RecordAssignment` or heartbeat roster refresh.
- `scheduler.placement_reservation_ttl` controls the conservative fallback lifetime for creates without an observed success; it defaults to 10 minutes and should cover the deployment's maximum queueing, creation, and heartbeat acknowledgement delay.
- `scheduler.redis_addr` selects Redis-backed sandbox binding storage when set; when empty, the scheduler uses the in-memory binding store. It accepts either `host:port` or a Redis URL such as `redis://[:password@]host:6379/db`.
- `--query-only` starts a read-only scheduler that supports only `LookupNode`; it requires `scheduler.redis_addr` and does not need node discovery config.
- `scheduler.artifact_store_capacity` controls how many distinct P2P artifact keys the in-memory artifact index keeps before LRU eviction; defaults to `1000000`.
- `scheduler.artifact_lookup_node_limit` controls how many node IDs a P2P artifact lookup returns; values `<= 0` return all matching nodes.
- `SCHEDULER_BINDING_TTL=<duration>` overrides `scheduler.binding_ttl` from the environment.
- `SCHEDULER_PLACEMENT_RESERVATION_TTL=<duration>` overrides `scheduler.placement_reservation_ttl` from the environment.
- `SCHEDULER_REDIS_ADDR=<addr>` overrides `scheduler.redis_addr` from the environment.
- `SCHEDULER_ARTIFACT_STORE_CAPACITY=<count>` overrides `scheduler.artifact_store_capacity` from the environment.
- `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT=<count>` overrides `scheduler.artifact_lookup_node_limit` from the environment.

### Scheduling strategy

`scheduler.strategy` selects the algorithm used to pick a node from the eligible candidate list. Built-in strategies:

| Strategy | Behaviour |
|---|---|
| `round_robin` (default) | Cycles through eligible nodes in stable order |
| `random` | Picks a uniformly random eligible node |
| `least_loaded` | Minimizes projected active+paused CPU/memory allocation pressure using heartbeat data; uses starting/running/paused counts as tie-breakers and round-robins equal candidates |

The strategy interface receives `RichNode` values that carry the node identity (ID + endpoint) together with the latest heartbeat `NodeSnapshot` (sandbox counts, CPU, memory, disk metrics). `least_loaded` uses the snapshot and, for cold sandbox creation, the requested CPU and memory scheduling hint. The other built-in strategies ignore the snapshot.

`least_loaded` prefers nodes with observed capacity over nodes without a usable
heartbeat snapshot. If all candidates are unobserved, or if multiple candidates
have the same projected load, it distributes those candidates round-robin.
Sandbox-creation selections also create short-lived projected reservations
under the strategy lock, so concurrent creates cannot all choose the same
uniquely least-loaded node before its next heartbeat. Read/list and other
requests without a creation hint do not reserve capacity. A newer heartbeat
reconciles the projected count from the monotonic `create_successes` counter
and resources from compatible running CPU/memory allocation deltas. Heartbeats
are ordered by a scheduler-local receipt generation rather than the node wall
clock. CPU and memory acknowledgements are independent, so fields that become
consistent in different heartbeats release only their corresponding dimension.
Starting sandboxes keep both count and resources reserved until success;
an increase in `create_fails` immediately releases the oldest unconfirmed
reservation. Placements with no observed outcome use the configurable stale
fallback below.

Cold-sandbox projections use CPU and memory only when the request explicitly
provides them. If either field is omitted, the node applies its local configured
default, which is not currently included in `NodeSnapshot`; the scheduler
therefore reserves the sandbox count but cannot project that default resource
dimension until the reporting protocol exposes it.

### Node resource limit

`scheduler.node_resource_limit` defines per-node resource thresholds that are evaluated **before** the strategy runs. Any node whose heartbeat snapshot exceeds a configured limit is removed from the candidate list, regardless of which strategy is in use. This is a generic guard-rail that sits above the strategy layer — strategies only see nodes that already passed the resource filter.

Nodes that have not yet sent a heartbeat (no snapshot available) are always kept in the candidate list, since there are no metrics to evaluate.

All fields are optional. Omitting a field (or setting the whole block to `null`) disables that particular check.

| Field | Type | Description |
|---|---|---|
| `max_sandbox_count` | uint32 | Maximum total sandbox count |
| `max_sandbox_starting_count` | uint32 | Maximum concurrently starting sandboxes |
| `max_cpu_used_percent` | uint32 | Maximum observed CPU usage (0–100) |
| `max_cpu_allocated_percent` | uint32 | Maximum allocated-CPU-to-physical-CPU ratio; can exceed 100 when overcommit is allowed |
| `max_memory_used_percent` | uint32 | Maximum observed memory usage (0–100) |
| `max_memory_allocated_percent` | uint32 | Maximum allocated-memory-to-physical-memory ratio; can exceed 100 when overcommit is allowed |

Example:

```json
"node_resource_limit": {
  "max_sandbox_count": 50,
  "max_sandbox_starting_count": 10,
  "max_cpu_used_percent": 90,
  "max_cpu_allocated_percent": 150,
  "max_memory_used_percent": 85,
  "max_memory_allocated_percent": 150
}
```

When all nodes are filtered out, the scheduler returns `Unavailable` to the caller.

## Gateway configuration

- `gateway.scheduler_addr` points to the primary scheduler. The gateway uses it for scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs.
- `gateway.query_only_scheduler_addr` optionally points to a query-only scheduler. When set, sandbox `LookupNode` routing uses this client; when unset, gateway falls back to `gateway.scheduler_addr`.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- `gateway.request_timeout` applies to regular proxied HTTP requests. Streaming requests and WebSocket connections reuse the client context and are not cut off by this timeout.
- `gateway.forward_response_size` only limits how much of a successful `POST /sandboxes` response the gateway buffers while extracting a sandbox ID for `RecordAssignment`; it is not a global response-size cap for all proxied traffic.
- Cluster list requests (`GET /sandboxes`, `GET /v2/sandboxes`) fan out to every scheduler node and merge results in the gateway. Direct requests to a backend node remain node-scoped.
- Cluster list requests are strict all-or-nothing: if any node times out, returns a non-2xx response, or cannot be reached, the gateway fails the whole list request rather than returning partial data.
- `GET /nodes` returns scheduler-observed node snapshots (including runtime/resource counters), with optional `clusterID` filtering.
- `GET /nodes/{id}` resolves node endpoint via scheduler and then proxies to the runtime node's admin endpoint.
- `GATEWAY_REQUEST_TIMEOUT=<duration>` overrides `gateway.request_timeout` from the environment (for example, `1m30s`).
- `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR=<addr>` overrides `gateway.query_only_scheduler_addr` from the environment.
- `gateway.sandbox_proxy_domains` enables host-based sandbox data-plane routing for `{port}-{sandboxID}.{domain}` URLs. Domains are normalized to lowercase, deduplicated, and must be valid DNS names. Sandbox IDs used in host routes must be lowercase RFC 952/1123 DNS labels, and the full `{port}-{sandboxID}` label must be at most 63 characters.
- `GATEWAY_SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]` overrides `gateway.sandbox_proxy_domains` from the environment.

Logging format defaults to `auto`:

- `auto`: console when stdout looks like an interactive terminal, otherwise JSON
- `console`: force human-readable terminal logs
- `json`: force structured logs for containers and log pipelines

Examples:

```bash
LOG_FORMAT=console make run-scheduler
LOG_FORMAT=json make run-gateway
```

## Deploy with Docker Compose

From **repository root**, start gateway + scheduler + two backend nodes:

```bash
make deploy-up
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Repository deployment helpers also accept `SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]`
and pass it to both gateway and runtime node processes.

Check status / logs / teardown:

```bash
make deploy-ps
make deploy-logs
make deploy-down
```

Container deployments use `deploy/docker/config/default.json`, where scheduler service-discovery and backend node endpoints are set for the Docker network.

The compose stack also wires each runtime node for scheduler heartbeat reporting:

- `AENV_NODE_ID` is set per runtime container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` points runtime nodes at `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.

## Deploy on Kubernetes

From the repository root:

```bash
make k8s-render
make k8s-apply
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The default overlay is `deploy/k8s/overlays/default`.
The make targets materialize a temporary Kustomize build context so Kubernetes runtime nodes always consume the repository's single AgentENV runtime config source: `config/default.toml`.

The DaemonSet injects scheduler-report identity and endpoint wiring for runtime nodes:

- `AENV_NODE_ID` comes from Pod metadata name.
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-scheduler:9090`.
- `AENV_SANDBOX_PROXY_DOMAINS` comes from the shared sandbox proxy ConfigMap.

Shared Kubernetes helpers:

```bash
make k8s-build
make k8s-redeploy
```

For single-machine development, `make k8s-apply-dev` uses the `local-dev`
overlay and mounts the repository `env/` directory directly into the AgentENV
DaemonSet at `/workspace/env`. This avoids copying runtime assets into `/var/lib/agentenv/env`.

For local k3s-style development, use:

```bash
make k8s-load-dev
make k8s-refresh-dev
```

`k8s-load-dev` imports the locally built images into k3s/containerd, while
`k8s-refresh-dev` runs build, load, and rollout restart together.

Deployment model:

- `gateway`: Deployment + ClusterIP Service
- `scheduler`: single-replica Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm` and hostPath `/var/lib/agentenv`
- `agentenv-nodes`: headless Service used by scheduler EndpointSlice discovery

Kubernetes config keys:

- `scheduler.discovery.mode`
- `scheduler.discovery.kubernetes.namespace`
- `scheduler.discovery.kubernetes.service_name`
- `scheduler.discovery.kubernetes.port`
- `scheduler.discovery.kubernetes.scheme` (defaults to `http`)
- `scheduler.discovery.kubernetes.ignore_pod_selector` (optional Kubernetes label selector; matching Pods are excluded from discovery)
- `scheduler.discovery.kubernetes.no_schedule_pod_selector` (optional Kubernetes label selector; matching Pods are kept as lingering/no-schedule nodes)

Kubernetes endpoint address handling:

- Scheduler only accepts EndpointSlice addresses that parse as valid IPs.
- Both IPv4 and IPv6 endpoint addresses are supported.
- IPv6 endpoints are emitted using bracketed host:port form (for example, `http://[2001:db8::10]:8000`).

Operational notes:

- The scheduler uses in-cluster Kubernetes config and watches EndpointSlices plus Pods for service discovery.
- Only serving, non-terminating DaemonSet Pods are schedulable. Use `no_schedule_pod_selector` for drain/no-new-work labels and `ignore_pod_selector` for Pods that should be completely hidden from discovery.
- For the default `memory` binding store, `scheduler` should stay single-replica because sandbox bindings are process-local.
- For high availability, run one primary scheduler with `scheduler.redis_addr` set and multiple query-only scheduler replicas started with `--query-only` against the same Redis. Point gateways at the primary with `gateway.scheduler_addr` and at the query-only service with `gateway.query_only_scheduler_addr`. The primary writes sandbox bindings to Redis while query-only replicas continue to serve data-plane `LookupNode` during primary restarts or upgrades. This HA mode is intentionally data-plane only: requests that proxy to existing sandboxes can keep routing, but control-plane operations that need the primary scheduler, such as creating new sandboxes, scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs, still fail while the primary scheduler is unavailable. Artifact store state is still in-memory and is not covered by this HA mode.
- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: api/proto/scheduler.proto

Methods:

- Schedule
- ListNodes
- LookupNode
- RecordAssignment
- Heartbeat
- ListObservedNodes
- ReportSandboxEvent
- GetNode
- UnregisterNode
