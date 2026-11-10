# Working with Sandboxes

This page provides the basic commands for interacting with and managing a sandbox.

## Connect to a Sandbox

`aenv connect` opens an interactive shell inside the sandbox and attaches your terminal. `aenv cn` is its short alias:

```bash
aenv connect <sandbox-id>
aenv cn <sandbox-id>
```

If a sandbox is paused, `aenv connect` will automatically resume it.

## Execute a Command

`aenv exec` runs one non-interactive command, streams its 
output to your local terminal, and exits with the remote 
command's exit code. It does not attach an interactive shell.
Flags intended for the remote command that collide with aenv's 
own flags can be escaped with a leading `--`.

```bash
aenv exec <sandbox-id> ls -la /
aenv exec <sandbox-id> -- command-with-aenv-like-flags --timeout 10
```

## Upload Files

`aenv upload` copies a local file or directory into a running sandbox:

Usage:

```bash
aenv upload <sandbox-id> <local-path> <remote-path> [options]
```

Example:

```bash
aenv upload 018f0d93-aaaa-bbbb-cccc-0123456789ab ./config.json /workspace/config.json
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the destination sandbox. |
| `<local-path>` | Required | Local file or directory to upload. |
| `<remote-path>` | Required | Destination inside the sandbox. Directory paths must be absolute. |
| `--user <user>` | None | Resolves a relative remote file path from this user's home directory and sets the uploaded file's owner. It is not supported for directory uploads. |

## Download Files

`aenv download` copies a file or directory from a running sandbox to your local machine:

Usage:

```bash
aenv download <sandbox-id> <remote-path> [local-path] [options]
```

Example:

```bash
aenv download 018f0d93-aaaa-bbbb-cccc-0123456789ab /workspace/result.txt ./result.txt
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the sandbox to download from. |
| `<remote-path>` | Required | File or directory inside the sandbox. Directory paths must be absolute. |
| `[local-path]` | Current directory | Local destination file or directory. |
| `--user <user>` | None | Resolves a relative remote file path from this user's home directory. It is not supported for directory downloads. |
| `--force` | Disabled | Replaces conflicting local files. Without it, the download stops instead of overwriting them. |

## Pause and Resume

Pausing saves the sandbox's current runtime state and stops its 
microVM. While it is paused, programs inside it do not run, services do not handle requests,
and the sandbox releases its CPU and memory resources. Its saved state remains
in storage so the same sandbox can be resumed later.

After resume, the filesystem, running processes, environment variables, and
in-memory data are restored to the state captured at pause time. Programs
continue from that saved state instead of starting again from the beginning.

```bash
aenv pause <sandbox-id>
aenv resume <sandbox-id>
aenv resume <sandbox-id> --timeout 600
```

`aenv resume` accepts `--timeout <seconds>`, which defaults to 300 seconds and
sets the new TTL from resume time.

By default, the sandbox automatically pauses when it reaches its TTL. See
[Auto-Eviction](./auto-eviction.md) for how the deadline is set and how to delete
instead of pause.

## Persistent Snapshots

A snapshot is a durable, reusable checkpoint of a running sandbox. Creating one
does not replace the sandbox: the source returns to Running after capture, and
the snapshot can later launch one or more new sandboxes.

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-base
```

The resulting snapshot appears in `aenv snapshot list` and can be started with
`aenv start <snapshot-id-or-name>`. See [Snapshots](../snapshots/index.md) for its
parameters and lifecycle.

## Fork

Forking clones a running sandbox into independent child sandboxes on the same
node. The source is briefly paused while its state is captured, then returns to
Running. Children inherit the source filesystem, memory, network policy,
security mode, and CPU/memory/disk configuration. All children use one captured
state, but each child can succeed or fail independently.

```bash
curl -X POST \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{"count": 3, "timeout": 600}' \
  http://127.0.0.1:8000/sandboxes/<sandbox-id>/fork
```

| Field | Default | Description |
|---|---|---|
| `count` | `1` | Number of children to create; minimum 1, maximum 100. |
| `timeout` | Source sandbox's TTL duration | TTL for each child, measured from the fork time. |

A successful request returns an array with one result for each requested child.
Each entry contains either a `sandbox` object—including its `sandboxID`—or an
`error` explaining why that individual child failed. It is not a plain list of
IDs. A non-201 response means the request failed before any child was attempted.
See the [API Reference](../../api/index.md) for the complete fork request and
response schemas.

## View Resource Metrics

Get the retained metrics for one sandbox:

```bash
curl -H "X-API-Key: $AENV_API_KEY" \
  "$AENV_URL/sandboxes/$SANDBOX_ID/metrics?start=1700000000&end=1700003600"
```

`start` and `end` are optional Unix timestamps in seconds and define an inclusive
time range. When omitted, the endpoint returns all retained samples for the
sandbox in timestamp order.

Get the latest metric for several sandboxes:

```bash
curl -H "X-API-Key: $AENV_API_KEY" \
  "$AENV_URL/sandboxes/metrics?sandbox_ids=$SANDBOX_ID,$OTHER_SANDBOX_ID"
```

`sandbox_ids` is required and accepts a comma-separated list of up to 100 unique
sandbox IDs. The response contains the latest available metric for each requested
running sandbox. Sandboxes without an available metric are omitted.

Each metric contains:

| Field | Description |
| --- | --- |
| `timestampUnix` | Sample time as Unix seconds. |
| `cpuCount` | Number of CPU cores. |
| `cpuUsedPct` | CPU usage percentage. |
| `memUsed` | Memory used in bytes. |
| `memTotal` | Total memory in bytes. |
| `memCache` | Cached memory in bytes. |
| `diskUsed` | Disk space used in bytes. |
| `diskTotal` | Total disk space in bytes. |

See the [API Reference](../../api/index.md) for complete request, response, and
error schemas.

Configure collection under `[orchestrator]` in your config file:

```toml
[orchestrator]
metrics_interval_secs = 15
metrics_retention_secs = 3600
```

`metrics_interval_secs` sets the collection interval in seconds; `0` disables
collection. `metrics_retention_secs` sets how long samples remain available.

### Prometheus guest resource metrics

Opt in on each runtime node to export the same cached guest CPU and memory
samples through its existing `/metrics` endpoint:

```toml
[orchestrator]
metrics_interval_secs = 15
metrics_retention_secs = 3600
metrics_prometheus_enabled = true
metrics_prometheus_max_age_secs = 60
```

Export is disabled by default and independent of `observability.enabled`, which
controls the node/admin APIs. Setting `metrics_interval_secs = 0` also disables
guest Prometheus export. `/metrics` is outside API-key authentication: enabling
this option exposes sandbox IDs and their guest usage to anyone who can access
the node endpoint. Restrict access to your monitoring network.

All the following metrics are gauges with one label, `sandbox_id`:

| Metric | Meaning |
| --- | --- |
| `agentenv_sandbox_guest_cpu_usage_percent` | Unmodified envd `cpu_used_pct`, identical to API `cpuUsedPct`; no conversion to used cores or cumulative CPU seconds. |
| `agentenv_sandbox_guest_cpu_count` | envd `cpu_count`, identical to API `cpuCount`. |
| `agentenv_sandbox_guest_memory_used_bytes` | envd `mem_used`, identical to API `memUsed`. |
| `agentenv_sandbox_guest_memory_total_bytes` | envd `mem_total`, identical to API `memTotal`. |
| `agentenv_sandbox_guest_memory_cache_bytes` | envd `mem_cache`, identical to API `memCache`. |
| `agentenv_sandbox_guest_sample_timestamp_seconds` | Guest-reported sample time in Unix seconds, exported as a value rather than a Prometheus sample timestamp. |
| `agentenv_sandbox_guest_sample_available` | `1` when a running sandbox has a fresh sample from its current runtime; otherwise `0`. |

These describe the guest, not configured resource allocation, host Firecracker
RSS/PSS, or physical memory attribution across shared snapshot pages. The memory
fields preserve envd's accounting; do not add or subtract cached memory without
checking the guest agent's definitions. CPU normalization is inherited from the
deployed envd version rather than inferred from the vCPU count.

Scrapes read the node-local cache and never request new guest samples, resume a
sandbox, or refresh its TTL. Sample freshness uses elapsed host monotonic time
since collection, so guest clock skew does not control expiration. At
`metrics_prometheus_max_age_secs` (or history retention, if shorter), usage and
sample-time series stop being emitted and availability becomes `0`. A failed
collection leaves the previous sample available only until that deadline.
Paused/deleted sandboxes have no active guest series on the next scrape after
the transition is observed; resume and fork require a sample from the current
runtime. A transition concurrent with a scrape may appear in the following
scrape. JSON API history retention and query behavior are unchanged.

Scrape every runtime node directly, not a load-balanced gateway or a node Service
that selects a different replica each time. For a static deployment:

```yaml
scrape_configs:
  - job_name: agentenv-runtime
    scrape_interval: 15s
    static_configs:
      - targets: ["node-a:8000"]
        labels:
          node_id: node-a
      - targets: ["node-b:8000"]
        labels:
          node_id: node-b
```

For Kubernetes, discover each runtime Pod endpoint individually and relabel its
node name; do not scrape only the Service's virtual IP. Prometheus target labels
provide `instance` and node/cluster identity without extra sandbox labels.

Example PromQL:

```promql
# Per-sandbox guest CPU percent (already a gauge; do not apply rate()).
agentenv_sandbox_guest_cpu_usage_percent

# Guest used-memory fraction, following envd's used/total definition.
agentenv_sandbox_guest_memory_used_bytes
  / agentenv_sandbox_guest_memory_total_bytes

# Running sandboxes without a fresh sample; use a suitable alert `for` duration.
agentenv_sandbox_guest_sample_available == 0
```

There are seven active time series per sandbox with a sample, and one for a
running sandbox without one, per target. Export keeps no per-sandbox handles in
the global recorder, but Prometheus retains historical series according to its
own retention. High sandbox churn therefore increases TSDB cardinality. Avoid
adding arbitrary user metadata as metric labels. Encoding is linear in running
sandbox count; tune collection/scrape intervals and measure overhead at your
deployment's scale.

## Manage Sandboxes

List sandboxes:

```bash
aenv list  # alias: aenv ls
aenv list --output json
```

`--output` accepts `table` or `json`. It
defaults to a table in an interactive terminal and JSON when output is piped or
redirected.

Delete a sandbox:

```bash
aenv delete <sandbox-id>  # alias: aenv rm
```

Deletion is permanent, but snapshots
previously created from the sandbox are unaffected.
