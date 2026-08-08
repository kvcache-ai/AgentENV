# Configuration Reference

AgentENV reads configuration from a TOML file. The default path is `config/default.toml`. Override it with:

```bash
export AENV_CONFIG_PATH=/path/to/config.toml
# or
cargo run --bin server -- --config /path/to/config.toml
```

## Global Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `home_path` | string | `"/var/lib/aenv"` | Base directory for local AgentENV state. Overridden by `AENV_HOME_PATH` |
| `runtime_path` | string | `"/run/aenv"` | Base directory for transient namespace and daemon-socket state. Overridden by `AENV_RUNTIME_PATH` |
| `deps_path` | string | `"$AENV_HOME/deps"` | Root directory for auto-downloaded runtime assets. Overridden by `AENV_DEPS_PATH` |
| `virtualization_mode` | `"kvm"` or `"pvm"` | `"kvm"` | Virtualization mode for this node. Keep the default unless following the [PVM Deployment](../deployment/pvm.md) guide. Overridden by `AENV_VIRTUALIZATION_MODE` |

Snapshots and paused sandboxes can only be restored in the mode in which they
were created.

`$AENV_HOME` is a literal placeholder in state-path values, not a shell
environment variable. AgentENV replaces it with the resolved `home_path` after
applying `AENV_HOME_PATH`; `ublk.daemon_socket_path` additionally supports
`$AENV_RUNTIME`, which resolves to `runtime_path`. Relative paths without these
placeholders are resolved against the directory containing the configuration
file.

Packaged runtime dependency versions and download URLs live in
`config/deps_manifest.toml`. Only the dependencies for the selected mode are
installed. User configuration should contain runtime behavior and explicit
local path overrides, not the default dependency catalog.

## `[firecracker]`

Firecracker VM binary and boot configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Optional Firecracker release override for auto-download |
| `url` | string | manifest value | Optional download URL template override with `{version}` and `{arch}` placeholders |
| `binary_path` | string | derived from manifest/config version | Explicit path to a local `firecracker` binary. Setup skips the Firecracker download and requires this to be a readable, non-empty, executable regular file |
| `boot_args` | string | `"console=ttyS0 reboot=k panic=1 pci=off init=/init …"` | Kernel command line arguments. The shipped default also includes DAMON memory-reclaim parameters; see `config/default.toml` for the full value. |
| `allowed_extra_boot_args_prefixes` | array of strings | `[]` | Allowed prefixes for `extraBootArgs` on cold-start sandboxes. If empty, no request-provided extra boot args are appended |
| `socket_timeout_secs` | integer | `3` | Max seconds to wait for the Firecracker API socket |
| `socket_poll_ms` | integer | `1` | Poll interval (ms) for checking socket availability |
| `work_dir` | string | `"$AENV_HOME/firecracker-work"` | Parent directory for per-sandbox Firecracker work directories. These dirs contain runtime sockets, symlinks, local logs, and writable OverlayBD upper layer data such as `overlaybd/upper.data` and `overlaybd/upper.index` |
| `serial_dir` | string | `"$AENV_HOME/logs/serial"` | Directory for persistent Firecracker serial output (per-sandbox subdirectories) |
| `log_level` | string | unset (disabled) | Optional Firecracker log level (`Error`, `Warning`, `Info`, `Debug`, `Trace`, case-insensitive). When set to a non-empty value, Firecracker's own logging is enabled and written to a `firecracker.log` file in each sandbox's log directory (alongside the serial output). Empty/unset disables it |

## `[kernel]`

Linux kernel image for microVMs.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Optional kernel version override for auto-download |
| `url` | string | manifest value | Optional download URL template override with `{version}` placeholder |
| `image_path` | string | derived from manifest/config version | Explicit path to a local `vmlinux.bin`. Setup skips the kernel download and requires this to be a readable, non-empty regular file |

## `[tools]`

Tools drive image used to boot the AgentENV control plane inside each microVM.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | manifest value | Immutable SemVer release of the complete tools drive; custom distributions should use a unique prerelease such as `0.1.0-custom.1` |
| `url` | string | manifest value | Optional OCI image URL template override with a `{version}` placeholder; requires an explicit `version` when set |
| `drive_path` | string | unset | Local tools ext4 source imported into the versioned dependency directory; requires an explicit `version` |
| `control_plane_port` | integer | `49983` | Port used by envd inside the guest |

Snapshots and paused sandboxes keep using the tools drive version they were
created with. Launch does not download missing releases: operators must install
the recorded version under `<deps_path>/tools/<version>/tools.ext4` before
restore. Setup retains previously installed versions until they are removed
manually.

## Template Rootfs Images

User-visible rootfs images are selected at the template API layer.
`POST /v2/templates/{templateID}/builds/{buildID}` accepts an optional
`fromImage` field:

- omitted: use `[image.resolver].default_image`
- full OCI reference: use the supplied image
- short name: normalize standard Docker Hub forms such as `ubuntu:24.04`
  and `node:20`

## `[image.resolver]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `default_image` | string | `ubuntu:24.04` | Image used when template builds omit `fromImage` |
| `search_registries` | array of strings | `["docker.io", "ghcr.io"]` | Registries tried when resolving short image references |
| `allowed_registries` | array of strings | unset (no restriction) | Whitelist of registry hosts (e.g. `docker.io`, `registry.example.com:5000`). **Omitting the key** imposes no restriction; an **explicit empty list `[]`** denies every registry. When set to a non-empty list, only references whose registry host is in the list resolve; any other host is rejected as a client (4xx, `ImageReferenceError`) error. See *How the three registry settings interact* below. |
| `try_referrers_overlaybd_prefixes` | array of strings | `[]` | Image reference prefixes for which AgentENV tries OCI Referrers API via `regctl` for an overlaybd-native artifact before converting a standard OCI image locally. Prefixes are matched with simple `starts_with`; include the trailing slash yourself, for example `registry.example.com/` or `registry.example.com/team/`. Requires `regctl` on `PATH`; lookup failures fall back to the source image. |

### How the three registry settings interact

Image resolution runs in two phases, and the three keys act at different points:

1. **`search_registries` — completion.** Only used for *short / unqualified*
   references (e.g. `ubuntu`). Each entry is prefixed to the name to build a
   list of fully-qualified candidates (`docker.io/library/ubuntu:latest`,
   `ghcr.io/ubuntu:latest`, …). Fully-qualified references skip this step.
2. **`allowed_registries` — gating.** Applied right after candidates are built,
   to *both* fully-qualified references and the candidates expanded from
   `search_registries`. Candidates whose registry host is not whitelisted are
   dropped; if none remain, the reference is rejected with a 4xx error. In
   effect the resolvable set of short-name hosts is the **intersection** of
   `search_registries` and `allowed_registries` — e.g. searching `docker.io`
   and `ghcr.io` while only allowing `ghcr.io` resolves short names to
   `ghcr.io` only.
3. **`try_referrers_overlaybd_prefixes` — per-candidate optimization.** Runs
   later, while resolving an *already-permitted* candidate: after its manifest
   is fetched, AgentENV may query the OCI Referrers API on the **same**
   registry/repository for an overlaybd-native artifact. Because referrer
   lookups never leave the source image's own host, they are implicitly
   covered by `allowed_registries` — no separate whitelist entry is needed for
   referrers.

   Two referrer `artifactType`s are recognized, in this order:

   | `artifactType` | Produced by |
   |-----|-----|
   | `application/vnd.containerd.overlaybd.native.v1+json` | accelerated-container-image (`obdconv`) |
   | `application/vnd.azure.artifact.streaming.v1` | Azure Container Registry artifact streaming (`az acr artifact-streaming create`) |

   Both point at an overlaybd-native manifest; only the discovery label
   differs. The referrer manifest is re-validated after it is fetched, so a
   referrer that is not actually overlaybd-native is rejected rather than
   used. Turbo-OCI referrers
   (`application/vnd.containerd.overlaybd.turbo.v1+json`) are never selected —
   AgentENV's overlaybd runtime does not implement the turbo read path.

   To stream from ACR, add your registry (with the trailing slash) to the
   list, e.g. `try_referrers_overlaybd_prefixes = ["myregistry.azurecr.io/"]`.

## `[image.cache]`

Node-local cache root for resolved and converted user images.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `root_dir` | string | `"$AENV_HOME/image-cache"` | Root directory for AgentENV image-cache artifacts |
| `capacity_gb` | integer | `100` | Budget for capacity-driven eviction of local commit bytes. Enforced only when `[image.cache.gc].enabled` is `true`: the background GC evicts least-recently-used source configs once usage crosses the high watermark, down to the low watermark. Unset = no capacity cap. |

## `[image.cache.gc]`

Background hard-commit garbage collection for the image cache. When enabled,
each pass reconciles metadata from the on-disk source configs and then deletes
hard-commit objects that are no longer rooted by source configs, held by
image-cache leases, or referenced by the in-process running set. Committed
snapshots are durable SnapshotRepository state and do not pin ImageCache
commits. With `capacity_gb` set, GC first evicts least-recently-used source
configs over the high watermark so hard-commit GC can reclaim what they unrooted.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `true` | Enable the background image-cache GC task |
| `interval_secs` | integer | `1800` | Seconds between GC passes (a value `<= 0` falls back to the default) |
| `min_age_secs` | integer | `600` | Minimum time since last use before a source config is eligible for capacity eviction (the LRU floor) |
| `high_watermark_ratio` | float | `0.95` | Begin capacity eviction once local commit bytes exceed `capacity_gb` × this ratio. Clamped to `(0, 1]` |
| `low_watermark_ratio` | float | `0.70` | Evict down to `capacity_gb` × this ratio once the high watermark trips. Clamped to `(0, high_watermark_ratio]` |

Capacity-driven eviction runs only when `[image.cache].capacity_gb` is set;
otherwise the GC still reclaims unreachable commits but performs no watermark
eviction.

## `[image.cache.remote_blocks]`

Overlaybd registryfs_v2 remote block cache settings. The directory is always
`<image.cache.root_dir>/remote-blocks`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_size_gb` | integer | `10` | Maximum size of the overlaybd remote block cache in GiB. This value is written to generated overlaybd `cacheConfig.cacheSizeGB` |

Resolved image data is cached under:

```text
<image.cache.root_dir>/
  commits/
    <sha256-commit-digest>/
      overlaybd.commit
                  # full overlaybd commit store shared by OCI conversion and download
  indexes/        # OCI layer + conversion context -> overlaybd commit descriptor
  remote-blocks/  # overlaybd-native remote block cache
  configs/         # resolved image configs
    <slug>-<hash>-image.json
```

## `[sandbox_proxy]`

Optional host-based data-plane routing for sandbox services.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `domains` | array of strings | `[]` | DNS domains accepted by the server for host-based proxy URLs shaped like `{port}-{sandboxID}.{domain}`. The first configured domain is returned in sandbox create/detail responses as `domain`. |

When `domains` is empty, the server still supports `/proxy` and routing-header
proxy requests, but does not classify requests by `Host`. Domains are normalized
to lowercase, deduplicated, and must be valid DNS names. The configured order is
preserved because `domains[0]` is the advertised sandbox domain.

Environment variable override:

- `AENV_SANDBOX_PROXY_DOMAINS`

## `[network.egress]`

Node-level sandbox egress guardrails. These rules are installed before
per-sandbox `allowOut` / `denyOut` rules, so sandbox API requests cannot
override them.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `always_denied_cidrs` | array of IPv4 CIDR strings | `["10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16"]` | Destination CIDRs that are always rejected from sandboxes before user egress policy is evaluated. Deployments can remove selected RFC1918 ranges when sandbox egress to those destinations is required. |

## `[network.internal]`

AgentENV-internal sandbox address plan. Change these only when the defaults
overlap with host or deployment network ranges.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `host_interaction_cidr` | IPv4 CIDR | `10.11.0.0/16` | Per-slot host interaction address pool. Must contain at least 32768 addresses. |
| `veth_cidr` | IPv4 CIDR | `10.12.0.0/16` | Per-slot namespace veth pair pool. Must contain at least 65536 addresses. |

The two configured CIDRs must not overlap each other or AgentENV's fixed VM tap
link `169.254.0.20/30`. These networks are also treated as reserved sandbox
egress destinations regardless of `always_denied_cidrs`.

## `[machine]`

Default VM resources for sandboxes.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mem_size_mib` | integer | `1024` | Guest RAM in MiB |
| `vcpu_count` | integer | `2` | Number of virtual CPUs |

## `[envd]`

In-guest `envd` daemon settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | `"0.5.15"` | Expected envd version baked into the tools drive image |
| `init_timeout_secs` | integer | `60` | Max seconds to wait for envd to become ready after VM start |
| `poll_ms` | integer | `3` | Poll interval (ms) for envd health check retries |

## `[orchestrator]`

Sandbox lifecycle management.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auto_evict_interval_ms` | integer | `1000` | Poll interval (ms) for background timeout eviction |
| `default_sandbox_timeout_secs` | integer | `15` | Default keep-alive timeout for sandboxes |
| `auto_resume_min_sandbox_timeout_secs` | integer | `300` | When a data-plane request targets a non-running sandbox, automatically resume it (if auto-resume is enabled) and refresh its timeout for no-less than this duration |
| `persisted_sandbox_store_path` | string | `"$AENV_HOME/persisted-sandboxes"` | Directory for persisted sandbox state |

## `[pool]`

Shared process-wide warm-pool defaults used by network slots, block devices, and pre-spawned Firecracker processes. Pools prewarm to the low watermark, then grow the refill target geometrically toward the high watermark when real acquisitions drain the pool.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `low_watermark` | integer | `2` | Initial lower bound for all enabled warm-resource pools |
| `high_watermark` | integer | `64` | Maximum idle target for all enabled warm-resource pools |

Component sections:

| Section | Key | Type | Default | Description |
|---------|-----|------|---------|-------------|
| `[pool.network]` | `maintenance_enabled` | boolean | `true` | Enable the background network-slot maintenance worker |
| `[pool.block]` | `enabled` | boolean | `true` | Enable the ublk overlaybd warm-device pool |
| `[pool.block]` | `startup_prewarm` | boolean | `true` | Prewarm block devices after the first reusable image shape is known |
| `[pool.firecracker]` | `enabled` | boolean | `true` | Enable pre-spawned Firecracker processes for snapshot resume |
| `[pool.firecracker]` | `maintenance_enabled` | boolean | `true` | Enable the background Firecracker process maintenance worker |
| `[pool.firecracker]` | `startup_prewarm` | boolean | `true` | Spawn warm Firecracker entries up to the low watermark during server startup |
| `[pool.firecracker]` | `fill_concurrency` | integer | `4` | Maximum number of warm Firecracker processes created concurrently by one maintenance refill batch |

Validation rules:

- `low_watermark <= high_watermark`
- `[pool.firecracker].fill_concurrency > 0`

## `[node_identity]`

Stable identity fields for this node. These values appear in node API responses
and scheduler heartbeats.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `node_id` | string | hostname-derived | Stable node identifier returned by the admin/node APIs |
| `cluster_id` | string (UUID) | nil UUID | Logical cluster identifier included in node snapshots |
| `service_instance_id` | string | generated UUID | Unique process/service instance identifier for the current node runtime |

## `[observability]`

Node-level observability and host metrics collection.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | Enable the node/admin observability service. When disabled, `/nodes` returns an empty list and `/nodes/{nodeID}` returns `404` |

When observability is enabled, host CPU/memory/disk metrics are collected at request time. CPU percent is computed from two samples; the first node metrics request waits about 100ms to return a measured value.

## `[observability.scheduler_report]`

Optional scheduler heartbeat reporting for multi-node control plane integration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable periodic scheduler heartbeat reporting. Requires `[cluster].scheduler_endpoint` |
| `interval_secs` | integer | `5` | Heartbeat report interval in seconds |

Environment variable overrides:

- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED`
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`
- `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS`

`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` overrides `[cluster].scheduler_endpoint` for the reporter process only.

## `[cluster]`

Shared cluster-level service endpoints.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `scheduler_endpoint` | string | unset | gRPC endpoint for the scheduler, for example `"http://127.0.0.1:9090"`. Used by scheduler heartbeat reporting and P2P peer discovery. |

## `[p2p]`

Project-wide artifact transport configuration. The transport is disabled by default. When enabled, it is used by the overlaybd P2P HTTP facade and by snapshot publication/runtime resolution as an optional artifact visibility and acceleration path.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable the P2P artifact transport. When false, AgentENV uses `DisabledP2pTransport`, so lookups miss and publishes are no-ops. |
| `transport` | string | `"iroh"` | Transport backend. Supported values are `"disabled"` and `"iroh"`. Ignored while `enabled = false`. |
| `store_dir` | string | `"$AENV_HOME/p2p/store"` | Local store used by the transport backend. Relative explicit paths are resolved against the config file directory. |
| `listen_addr` | string | `"0.0.0.0:0"` | Optional local listen address for the embedded transport endpoint. Port `0` lets the OS choose a free port. |
| `lookup_timeout_ms` | integer | `5000` | Timeout for one artifact catalog lookup against a peer. |
| `fetch_timeout_ms` | integer | `30000` | Timeout for fetching one artifact from a peer. |
| `peer_discovery_refresh_interval_secs` | integer | `5` | Interval for refreshing peer endpoints from scheduler. Values below one second are clamped to one second. |

## `[custom_extension]`

Custom extension service configuration. When `url` is unset, the integration is fully disabled. See [Custom Extension](../concepts/custom-extension.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `url` | string | unset | HTTP base URL of the custom extension service. When set, AgentENV invokes sandbox lifecycle hooks under `POST {url}/sandbox-hook/*`. |
| `timeout_ms` | integer | `5000` | Timeout for each custom extension HTTP call, in milliseconds. |

## `[snapshot]`

Snapshot storage/build configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `local_cache_path` | string | `"$AENV_HOME/snapshot-local-cache"` | Manager-owned node-local snapshot artifact/cache root. Relative explicit paths are resolved against the config file directory. |
| `repository_backend` | string | `"posix_fs"` | Snapshot repository backend. Supported values: `"posix_fs"` and `"oss"` |
| `p2p_enabled` | boolean | `true` | When enabled, the snapshot manager publishes committed snapshots to the P2P transport and attempts to resolve from it before falling back to the repository backend. |

Environment variable overrides:

- `AENV_SNAPSHOT_LOCAL_CACHE_PATH`

## `[snapshot.image_publish]`

Source-registry image publication. Only takes effect when `snapshot.repository_backend = "oss"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | When enabled, publishing a snapshot also pushes its rootfs as an OverlayBD-native OCI image tag `agentenv-snapshot-{snapshot_id}` to the original source registry. Requires source images to be OverlayBD-native in that registry and push credentials in the Docker config (`~/.docker/config.json`). Existing remote layers are referenced by digest; only new delta layers are uploaded. The published reference is exposed as `imageRef` in snapshot APIs. Memory and VM-state artifacts always remain in the snapshot repository. |

## `[backend.posix_fs]`

POSIX filesystem-backed snapshot repository configuration. This section is used when `snapshot.repository_backend = "posix_fs"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `snapshot_store` | string | `"$AENV_HOME/snapshot-store"` | Root directory for durable committed snapshot repository state. Relative explicit paths are resolved against the config file directory. |

Environment variable overrides:

- `AENV_SNAPSHOT_STORE`

## `[backend.oss]`

OSS-backed snapshot repository configuration. This section is required when `snapshot.repository_backend = "oss"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `endpoint` | string | none | OSS endpoint URL, for example `"https://oss-cn-hangzhou.aliyuncs.com"` |
| `bucket` | string | none | OSS bucket name used for committed snapshot state |
| `prefix` | string | empty | Optional object key prefix under the bucket |
| `credential_process` | string | unset | External command used to fetch OSS credentials. Use a plain executable-plus-args form without shell expansion, pipes, or command substitution so it behaves consistently across AgentENV and overlaybd credential consumers |
| `access_key_id` | string | unset | Static OSS access key ID. Required when `credential_process` is not set |
| `access_key_secret` | string | unset | Static OSS access key secret. Required when `credential_process` is not set |
| `security_token` | string | unset | Optional session token paired with static access key credentials |
| `region` | string | none | Region passed to the S3-compatible object-store client; required for current OSS backend |
| `cache_max_size_gb` | integer | `10` | Maximum size of the node-local OSS artifact cache in GiB |

Notes:

- `credential_process` and static access key settings are mutually exclusive in practice; when `credential_process` is set, the backend ignores static credential fields.
- `credential_process` should be written as a portable argv-style command line. Avoid `$VAR`, backticks, `$(...)`, pipes, and shell builtins.
- Although the config section is still named `oss`, the runtime path is implemented via a shared S3-compatible client, so `region` must be configured.

Other path override:

- `AENV_DEPS_PATH`

Setup sysctl tuning is host-level setup. It is skipped before reading `/proc/sys`
when the server detects that it is running inside a container. Set
`AENV_FORCE_SYSCTL_TUNING=1` only for a privileged container with writable
host sysctls; otherwise configure these kernel parameters on the host.

## `[protoc]`

Protobuf compiler metadata for code generation lives in
`config/deps_manifest.toml`, not `config.toml`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `version` | string | `"33.4"` | protoc release version |
| `url` | string | GitHub release URL | Download URL template with `{version}` and `{platform}` placeholders |

## `[ublk]`

Userspace block device configuration. Rootfs is served through an OverlayBD-backed ublk device managed by `uvm-ublk-daemon`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `daemon_binary_path` | string | `"$AENV_HOME/ublk/uvm-ublk-daemon"` | Path to the `uvm-ublk-daemon` binary |
| `daemon_socket_path` | string | `"$AENV_RUNTIME/ublk-daemon.sock"` | Unix socket path used by the daemon |
| `daemon_log_path` | string | `"$AENV_HOME/logs/ublk-daemon.log"` | File path for daemon logs; deployments are responsible for rotation and retention |
| `daemon_metrics_listen_addr` | string | `"0.0.0.0:9103"` | HTTP listen address for daemon Prometheus metrics; empty string disables it |

Environment variable override:

- `AENV_UBLK_DAEMON_BINARY_PATH`
- `AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR`

## `[ublk.overlaybd]`

OverlayBD configuration for ublk. Legacy `enabled` and `device_type` keys are ignored.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `global_config_path` | string | `"$AENV_HOME/overlaybd/overlaybd-global.json"` | Path to overlaybd global config JSON (see note below). Relative explicit paths are resolved against the config file directory. |
| `read_only` | boolean | `false` | When set to `true`, materializes the rootfs without a writable upper |
| `runtime_upper_mode` | string | `"hybridLogStructured"` | Runtime upper format for newly materialized writable rootfs OverlayBD images. Supported values are `"logStructured"`, `"hybridLogStructured"`, and `"sparse"`. Existing source uppers keep their own mode |
| `allow_shrink` | boolean | `false` | Allows an explicit cold-start `diskSizeMB` smaller than the source rootfs. Explicit sizes use MiB and must be divisible by 1024. Growth is always allowed; snapshot resume never resizes. |
| `resize_timeout_secs` | integer | `120` | Timeout in seconds for the cold-start OverlayBD resize tool. Must be greater than zero. |
| `download_enable` | boolean | `false` | Enables overlaybd layer-level background download for remote layers |
| `p2p_lookup_timeout_ms` | integer | `300` | Timeout for one foreground Overlaybd descriptor lookup through the localhost P2P HTTP facade. Timeout is treated as a cacheable miss. |
| `p2p_fetch_range_timeout_ms` | integer | `2000` | Timeout for one foreground Overlaybd range fetch through the localhost P2P HTTP facade before falling back to the origin registry. |

### `global_config_path` and auto-generated config

The file at the configured default path
`$AENV_HOME/overlaybd/overlaybd-global.json` is **auto-generated** by the server
at startup. The generated JSON incorporates several TOML settings —
`[image.cache].root_dir`, `[image.cache.remote_blocks].max_size_gb`,
`download_enable`, `[backend.oss]` credentials, and Docker registry credentials
detected from `~/.docker/config.json` — into a single overlaybd runtime config
file.

The server regenerates the file at `global_config_path` on every startup, so
these TOML settings always take effect automatically — any manual edits to the
generated file are overwritten on the next startup. To keep customizations,
make them through the TOML settings, not by editing the generated JSON.

## `[memory_snapshot]`

Memory snapshot overlaybd configuration. The server auto-generates the file at
the default path on every startup.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `overlaybd_global_config_path` | string | `"$AENV_HOME/overlaybd/mem-overlaybd-global.json"` | Path to the overlaybd global config used for the memory-snapshot ublk backend. Regenerated at startup (manual edits are overwritten); change only to relocate the generated file. |
| `direct_overlaybd` | bool | `true` | Create memory overlaybd layers directly from Firecracker dirty memory ranges via `process_vm_readv`, skipping the intermediate `mem.bin` file. Set `AGENTENV_MEMORY_SNAPSHOT_DIRECT_OVERLAYBD=false` to force the legacy `mem.bin` conversion path. |
| `track_dirty_pages` | bool | `false` | Enable Firecracker KVM dirty-page tracking for memory snapshots. When true, `direct_overlaybd` must also be `true`; PVM is temporarily disabled because this combination has not been tested. Set `AGENTENV_MEMORY_SNAPSHOT_TRACK_DIRTY_PAGES=true` to enable it. |
| `compression_enabled` | bool | `false` | Enable compression for memory snapshot layers. When disabled, `compression_algorithm` is still parsed but has no effect. This setting affects only memory layers; the physical file name remains `overlaybd.commit`. |
| `compression_algorithm` | string | `"lz4"` | Compression algorithm for memory snapshot layers. Valid values are only `lz4` and `zstd`. |

## `[memory_snapshot.background_download]`

Background download settings dedicated to remote memory-snapshot OverlayBD layers.
They do not change the general rootfs or attached-drive defaults. All fields are
serialized into the generated memory OverlayBD global config. Each remote layer
is filled block by block into the node-local remote file cache: the cache-owned
background-download scheduler registers one task per remote layer (deduplicated
by blob, shared across sandboxes) and downloads only chunks still missing
from the entry bitmap — each source request fetches `block_size` bytes
(aligned to whole cache blocks) and publishes the chunk's cache blocks as
soon as they land. Submission is never rejected under load — tasks run as
scheduler capacity allows, with at most `maxConcurrentFiles` layer tasks
concurrently per file-cache backend (from the generated overlaybd download
config, default 8)
and at most `concurrency` chunk reads in parallel per layer, subject to the
scheduler's `max_inflight_blocks` cap. Downloads of a
sandbox-bound device start only after envd is ready (plus `delay`), with a 20s
fallback if the ready signal is lost; while foreground remote reads are in
flight, background block reads yield to a small guaranteed floor instead of
competing at full speed. The generated memory
config leaves throttling off (`maxMBps = 0`); image configs that carry a positive
`maxMBps` keep their historical shared rate limit across the block tasks.
A completed cache block becomes visible to foreground reads as soon as it is
committed to the cache bitmap; there is no staging file, no full-file digest
check, and no switch-to-local, so a failed or canceled block simply stays
uncached and is fetched on demand by foreground reads or a later retry. The
cache is a bounded working set: blocks may be evicted under capacity pressure
and are then re-fetched on demand.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable` | boolean | `true` | Enables background download for remote memory-snapshot layers. |
| `delay` | integer | `0` | Delay in seconds after envd is ready before background download begins (downloads never start before envd readiness; a 20s fallback applies if the ready signal is lost). |
| `delay_extra` | integer | `1` | Exclusive upper bound for random extra delay. The default `1` ensures `delay = 0` adds no jitter. |
| `try_cnt` | integer | `5` | Retry count, with the same semantics as OverlayBD `DownloadConfig.tryCnt`. |
| `block_size` | integer | `16777216` | Background download chunk size in bytes (16 MiB): one source request fetches a chunk of this size, aligned down to whole cache blocks. The cache keeps its own smaller block size for foreground reads, so background downloads keep large-request throughput while foreground keeps fine-grained on-demand reads. Peak scratch per active layer download is `block_size × concurrency`. |
| `concurrency` | integer | `4` | Maximum number of in-flight block remote reads within a single remote layer. `1` keeps the historical serial behavior. Must be greater than zero. |
| `max_inflight_blocks` | integer | `16` | Cap on concurrently downloading chunks enforced by each file-cache backend's download scheduler, shared by every concurrent layer download on that backend; bounds total scratch memory to `max_inflight_blocks` × the download chunk size (`block_size`). The value is fixed when the backend is created from the global config; a per-image `download` override never resizes the scheduler-owned cap (the first mismatch per scheduler is logged as `max_inflight_blocks_override_ignored`). Must be greater than zero. |
