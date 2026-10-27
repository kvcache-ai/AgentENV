# Gateway and Scheduler Configuration

The Gateway and Scheduler load JSON configuration through their `-config`
command-line argument. The same configuration format is used by these
multi-node deployments:

| Deployment | Configuration files |
|---|---|
| [Docker Compose](../deployment/docker-compose.md#configuration) | `deploy/docker/config/default.json`, shared by both services. |
| [Static Multi-Node](../deployment/static-multi-node.md#3-configure-static-discovery) | `/etc/agentenv/control-plane.json`, shared by both services in the guide. |
| [Kubernetes](../deployment/kubernetes.md#configuration) | `deploy/k8s/base/config/scheduler.json` and `deploy/k8s/base/config/gateway.json`, supplied through ConfigMaps. |

Configuration is loaded in this order, with later values overriding earlier
ones:

1. Built-in defaults.
2. The JSON file selected by `-config`.
3. Supported environment variables in the service process.

The Default column lists built-in values, which may differ from values explicitly
set in deployment examples. Configuration is read at startup; restart the
affected services after applying changes using the relevant deployment guide.
Runtime-node TOML settings are documented separately in the
[Runtime Node Configuration](./reference.md).

## Configuration fields

| Field | Default | Meaning |
|---|---|---|
| `log_level` | `"info"` | Zap log level; common values are `debug`, `info`, `warn`, and `error`. Overridden by `LOG_LEVEL`. |
| `log_format` | `"auto"` | `auto`, `console`, or `json`. Automatic mode selects console output for an interactive terminal and JSON otherwise. Overridden by `LOG_FORMAT`. |
| `scheduler` | See below | Scheduler configuration object. |
| `gateway` | See below | Gateway configuration object. |
| `service` | Set by the binary | Accepted by the parser but overwritten with `scheduler` or `gateway`; omit it. |

### Scheduler fields

All fields in this table are inside `scheduler`.

| Field | Default | Meaning |
|---|---|---|
| `grpc_listen_addr` | `":9090"` | gRPC listener for scheduling, binding lookup, node reports, and related RPCs. Overridden by `SCHEDULER_GRPC_LISTEN_ADDR`. |
| `metrics_listen_addr` | `":9101"` | Separate HTTP listener for Prometheus metrics. Overridden by `SCHEDULER_METRICS_LISTEN_ADDR`. |
| `strategy` | `"round_robin"` | Node selection strategy: `round_robin` or `random`. Overridden by `SCHEDULER_STRATEGY`. |
| `report_ttl` | `"30s"` | Age after which a heartbeat report is considered stale in the observed-node view. Use a duration string such as `"30s"`, not numeric seconds. |
| `binding_ttl` | `"30s"` | Lifetime of a sandbox-to-node routing binding; assignment recording and heartbeat reconciliation refresh bindings.  Use a duration string such as `"30s"`, not numeric seconds. Zero or negative durations fall back to `"30s"`. Overridden by `SCHEDULER_BINDING_TTL`. |
| `redis_addr` | `""` | Empty uses in-process binding storage; otherwise use Redis, specified as `host:port` or a Redis URL. Overridden by `SCHEDULER_REDIS_ADDR`. |
| `artifact_store_capacity` | `1000000` | Maximum distinct `(cluster_id, backend, key)` entries in the in-memory artifact-location index, with LRU eviction. Must be positive for a normal Scheduler. Overridden by `SCHEDULER_ARTIFACT_STORE_CAPACITY`. |
| `artifact_lookup_node_limit` | `0` | Maximum nodes returned by an artifact lookup; zero or a negative value means unlimited. Overridden by `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT`. |
| `nodes` | One local node | Static runtime-node list; see below. |
| `discovery` | Static discovery | Discovery configuration object; see below. |
| `node_resource_limit` | Unset | Optional per-node resource thresholds applied before node selection; see below. |

#### `scheduler.discovery`

`scheduler.discovery.mode` selects how the Scheduler discovers runtime nodes:

- `static` (the default) uses the node list in `scheduler.nodes`.

Each `scheduler.nodes` entry accepts:

| Field | Meaning |
|---|---|
| `id` | Runtime node ID, matching that runtime's `AENV_NODE_ID`. |
| `endpoint` | Runtime API URL reachable from the Gateway, such as `http://agentenv-a:8000`. |

Static mode requires a nonempty node list and nonempty IDs and endpoints. If
`nodes` is omitted entirely, the built-in list contains `local-node` at
`http://127.0.0.1:8000`.

For example, merge the following fields into the existing `scheduler` object
to use the two runtime services in the [Docker Compose deployment](../deployment/docker-compose.md):

```json
{
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "http://agentenv-a:8000"
    },
    {
      "id": "node-b",
      "endpoint": "http://agentenv-b:8000"
    }
  ],
  "discovery": {
    "mode": "static"
  }
}
```

- `kubernetes` uses `scheduler.discovery.kubernetes` to watch a Service's
  EndpointSlices.

The following fields belong under `scheduler.discovery.kubernetes` and are
used only in Kubernetes mode:

| Field | Default | Meaning |
|---|---|---|
| `namespace` | `""` | Service namespace; required in Kubernetes mode. |
| `service_name` | `""` | Service whose EndpointSlices supply runtime nodes; required. |
| `port` | `0` | EndpointSlice port to match and use in runtime URLs; must be positive. |
| `scheme` | `"http"` | Scheme used to construct runtime URLs. |
| `ignore_pod_selector` | `""` | Optional Kubernetes label selector; matching Pods are excluded from discovery entirely. |
| `no_schedule_pod_selector` | `""` | Optional label selector; matching Pods are retained as lingering nodes for existing routing but excluded from new placement. |

Nonempty selectors must be valid Kubernetes label selectors.

For example, merge the following field into the existing `scheduler` object
for the Service `agentenv-nodes` in namespace `agentenv-system`:

```json
{
  "discovery": {
    "mode": "kubernetes",
    "kubernetes": {
      "namespace": "agentenv-system",
      "service_name": "agentenv-nodes",
      "port": 8000,
      "scheme": "http"
    }
  }
}
```

#### `scheduler.node_resource_limit`

For a new scheduling request, the Scheduler takes discovered, non-lingering
nodes, filters them against `scheduler.node_resource_limit`, then selects one
using round robin or random selection.

Every resource-limit field is optional. An omitted field imposes no limit.

| Field under `scheduler.node_resource_limit` | Node metric compared with the threshold |
|---|---|
| `max_sandbox_count` | Active sandbox count, excluding paused sandboxes. |
| `max_sandbox_starting_count` | Number of sandboxes currently starting. |
| `max_cpu_used_percent` | CPU usage percentage reported in the heartbeat. |
| `max_cpu_allocated_percent` | Allocated CPU / total node CPU × 100. |
| `max_memory_used_percent` | Used memory / total node memory × 100. |
| `max_memory_allocated_percent` | Allocated memory / total node memory × 100. |
| `max_sandbox_count_including_paused` | Active plus paused sandbox count. |
| `max_allocated_cpu_including_paused` | Active plus paused CPU allocations, in CPU units, not percent. |
| `max_allocated_memory_bytes_including_paused` | Active plus paused memory allocations, in bytes. |

### Gateway fields

All fields in this table are inside `gateway`.

| Field | Default | Meaning |
|---|---|---|
| `http_listen_addr` | `":8080"` | Main listener when TLS is off. Overridden by `GATEWAY_HTTP_LISTEN_ADDR`. |
| `https_listen_addr` | `":8080"` | Main listener when TLS is enabled. Overridden by `GATEWAY_HTTPS_LISTEN_ADDR`. |
| `metrics_listen_addr` | `":9102"` | Separate HTTP listener for Prometheus metrics. Overridden by `GATEWAY_METRICS_LISTEN_ADDR`. |
| `scheduler_addr` | `"127.0.0.1:9090"` | Main Scheduler gRPC target; normally `host:port`, without an HTTP URL prefix. Overridden by `GATEWAY_SCHEDULER_ADDR`. |
| `query_only_scheduler_addr` | `""` | Optional separate Scheduler for `LookupNode`; empty reuses the main Scheduler. Overridden by `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR`. A separate query-only Scheduler is started with the command-line flag `--query-only`, not a JSON field. It requires `scheduler.redis_addr` pointing to the shared binding store and supports only the binding lookup RPC. |
| `request_timeout` | `"30s"` | Timeout for routing and bounded control-plane/scheduled requests. Use a duration string such as `"30s"`, not numeric seconds. Overridden by `GATEWAY_REQUEST_TIMEOUT`. |
| `forward_response_size` | `4194304` | Byte limit when buffering an upstream response body to extract IDs and record bindings. Exceeding it returns 502; nonpositive values disable this limit. It is not a universal limit on all proxied responses. |
| `sandbox_proxy_domains` | `[]` | Allowed domain suffixes for host-based sandbox routing, such as `["sandbox.example.com"]`. Overridden by `GATEWAY_SANDBOX_PROXY_DOMAINS`. The environment override uses a comma-separated list. |
| `debug_mode` | `false` | Enables debug behavior such as exposing the backend node ID on proxied responses. Overridden by `GATEWAY_DEBUG_MODE`. |
| `tls` | TLS off | HTTPS configuration object; see below. |

#### `gateway.tls`

| Field | Default | Meaning |
|---|---|---|
| `mode` | `"off"` | `off`, `manual` (existing certificate), or `acme` (automatic certificate management). |
| `cert_file` | `""` | Certificate file path; required in manual mode. |
| `key_file` | `""` | Private-key file path; required in manual mode. |
| `domains` | Unset | Nonempty certificate DNS-name or eligible public-IP list; required in ACME mode. |
| `storage_dir` | `""` | Persistent certificate and ACME state directory; required in ACME mode. |
| `email` | `""` | ACME account contact email. |
| `ca` | `""` | Optional HTTPS ACME directory URL; empty uses the CertMagic default CA. |
| `challenge` | `"tls-alpn-01"` | ACME challenge: `tls-alpn-01` or `dns-01`. |
| `dns_provider` | `""` | DNS-01 provider: `cloudflare`, `alidns`, `tencentcloud`, or `route53`. |
| `dns_config` | Unset | Provider-specific JSON object; required for DNS-01. |

Enabling TLS switches the main listener to HTTPS. It does not also start an
HTTP listener or an HTTP-to-HTTPS redirect.

For an existing certificate, add this object inside `gateway`:

```json
"tls": {
  "mode": "manual",
  "cert_file": "/etc/agentenv/tls/fullchain.pem",
  "key_file": "/etc/agentenv/tls/privkey.pem"
}
```

For ACME with a wildcard sandbox domain, a Cloudflare example is:

```json
"tls": {
  "mode": "acme",
  "domains": ["api.example.com", "*.sandbox.example.com"],
  "storage_dir": "/var/lib/agentenv/tls",
  "email": "operator@example.com",
  "challenge": "dns-01",
  "dns_provider": "cloudflare",
  "dns_config": {
    "api_token": "<cloudflare-api-token>"
  }
}
```

IP certificates require TLS-ALPN-01,
allow at most one IP per listener, and reject private or special-purpose IPs.
TLS-ALPN-01 must not include `dns_provider` or `dns_config`, and public TCP port 443
must reach the Gateway TLS listener for validation.

Wildcard certificates require DNS-01. When `challenge` is set to `dns-01`,
configure `dns_provider` and its corresponding `dns_config` object. Supported
providers and their configuration fields are listed below:

| Provider | Fields |
|---|---|
| `cloudflare` | Required `api_token`; optional `zone_token` for separate Zone-read access. |
| `alidns` | Required `access_key_id`, `access_key_secret`; optional `region_id`, `security_token`. |
| `tencentcloud` | Required `SecretId`, `SecretKey`; optional `SessionToken`, `Region`. These are the field names exposed by the current dependency. |
| `route53` | Optional `access_key_id` and `secret_access_key`, supplied together or both omitted to use the AWS credential chain; also `session_token`, `region`, `profile`, and `hosted_zone_id`. Additional options: `max_retries` (retry count), `route53_max_wait` (maximum internal AWS synchronization wait), `wait_for_route53_sync` (wait for that synchronization), and `skip_route53_sync_on_delete` (skip it on deletion). `route53_max_wait` takes an integer number of **nanoseconds**, not a duration string such as `"30s"`. |

Unknown provider fields are rejected.

## Example configuration

This example uses the two runtime
nodes in the [Docker Compose deployment](../deployment/docker-compose.md), static discovery, round-robin placement, and
an HTTP Gateway.

```json
{
  "log_level": "info",
  "log_format": "json",
  "scheduler": {
    "grpc_listen_addr": ":9090",
    "strategy": "round_robin",
    "discovery": {
      "mode": "static"
    },
    "nodes": [
      {
        "id": "node-a",
        "endpoint": "http://agentenv-a:8000"
      },
      {
        "id": "node-b",
        "endpoint": "http://agentenv-b:8000"
      }
    ],
    "node_resource_limit": {
      "max_cpu_used_percent": 85,
      "max_memory_used_percent": 90
    }
  },
  "gateway": {
    "http_listen_addr": ":8080",
    "scheduler_addr": "scheduler:9090",
    "request_timeout": "90s"
  }
}
```
