# Starting a Sandbox

Start a sandbox from a reusable template or snapshot, directly from an OCI image,
or from a multi-service Compose project.

## From a Template or Snapshot

Before using a warm start, create a [template](../templates/index.md) or a
[snapshot](../snapshots/index.md). You can then start any number of sandboxes from its alias or ID:

Usage:

```bash
aenv start <template-or-snapshot> [options]
```

Example:

```bash
# Start by alias
aenv start my-python-template

# Start by ID
aenv start 018f0d93-aaaa-bbbb-cccc-0123456789ab
```

Warm-start options:

| Argument or option | Default | Description |
|---|---|---|
| `<template-or-snapshot>` | Required | Template or snapshot ID or alias. |
| `--timeout <seconds>` | `300` | Set the sandbox TTL. The sandbox auto-pauses when it reaches the TTL; see [Auto-Eviction](./auto-eviction.md). |
| `--volume <mount-path>=<volume>` | None | Mount a persistent volume by ID or name. Repeat the option to mount multiple volumes. |
| `-d`, `--detach` | Off | Print the sandbox ID and exit instead of attaching an interactive shell. |

Without `--detach`, `aenv start` waits for the sandbox to become ready and then
attaches an interactive shell. CPU, memory, and disk settings are inherited
from the template or snapshot and cannot be overridden on a warm start. The CLI
always enables secure sandbox authentication and manages the envd access token
automatically; see [Secure Sandbox Authentication](../authentication/secure-sandbox.md).

To retrieve the current state and configuration of one sandbox, use the HTTP
API:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/sandboxes/<sandbox-id>
```

## Cold Start from an OCI Image

A cold start resolves an OCI image directly and prepares a fresh writable root filesystem at runtime:

Usage:

```bash
aenv start --cold <image> [options]
```

Example:

```bash
aenv start --cold ubuntu:24.04
aenv start --cold ubuntu:24.04 --cpu 4 --memory 4096 --disk-size-mb 65536
```

Cold-start options:

| Argument or option | Default | Description |
|---|---|---|
| `<image>` | Required | External OCI image reference. |
| `--cold` | Required for an OCI image | Cold start directly from `<image>`. |
| `--timeout <seconds>` | `300` | Set the sandbox TTL. The sandbox auto-pauses when it reaches the TTL; see [Auto-Eviction](./auto-eviction.md). |
| `--cpu <count>` | `[machine].vcpu_count` from your AgentENV config file | Set the sandbox's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set sandbox memory. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Source image virtual size | Set root filesystem size. The value must be greater than zero and divisible by 1024 MiB. Alias: `--disk-mb`. |
| `--volume <mount-path>=<volume>` | None | Mount a persistent volume by ID or name. Repeat the option to mount multiple volumes. |
| `-d`, `--detach` | Off | Print the sandbox ID and exit instead of attaching an interactive shell. |

Cold-started sandboxes also use secure sandbox authentication by default.

The AgentENV config file is `config/default.toml` by default, or the file
specified by `AENV_CONFIG_PATH`.

An OverlayBD-native image can start without downloading
the complete image first; its filesystem data is loaded from the registry on
demand. See [On-Demand Loading](../../getting-started/on-demand-loading.md).

Growth of the disk size is allowed by
default. Shrinking below the source image size requires
`ublk.overlaybd.allow_shrink = true` in your AgentENV config file. Resizing
applies only when creating a fresh writable root filesystem, not to read-only
images, images with an existing upper layer, or snapshot resume. Sandbox
responses report the effective size as `diskSizeMB`.

## Compose projects

`POST /sandboxes-compose` runs an image-only Docker Compose project inside one
Firecracker sandbox. The node resolves each source image through `ImageResolver`.
Each service gets its own writable attached drive, including services using the
same image. Inside the VM, `containerd-plain-snapshotter` registers those mounted
root filesystems with Docker while preserving image configuration (entrypoint,
command, environment, user, working directory, healthcheck, and volumes).

### Enable the runtime

The initial runtime supports Linux x86-64/KVM. Build from the repository root and
publish to a registry accessible to the node. The image build rejects other
target architectures. Docker and Compose downloads are verified against pinned
SHA-256 digests; overriding their version build arguments also requires updating
the corresponding `DOCKER_SHA256` or `COMPOSE_SHA256` argument:

```sh
docker build -f compose-image/Dockerfile -t REGISTRY/agentenv-compose:1 .
docker push REGISTRY/agentenv-compose:1
cd services
go build -o aenv-compose-plan ./compose/cmd
```

Install `aenv-compose-plan` on the node's PATH, or configure an absolute path:

```toml
[compose]
base_image = "REGISTRY/agentenv-compose:1"
planner_binary = "/usr/local/bin/aenv-compose-plan"
```

Environment overrides are `AENV_COMPOSE_BASE_IMAGE` and
`AENV_COMPOSE_PLANNER_BINARY`. Sandbox creation is disabled when no base image is
set.
The server Docker image and release bundle include the planner. Extracting a
bundle alone does not add it to PATH; install it or set the absolute path above.

The dedicated image includes Docker 28.4.0, Compose 2.39.4, their containerd/runc,
and the plain snapshotter. The Linux `aenv` binary also provides the hidden
`compose guest-init` and `compose guest-start` commands used by the guest.
The guest does not require Python.
`/init` supervises the runtime with tini. It uses an
explicit containerd socket and fails if Docker does not select `plain`. The
existing tools drive and envd are unchanged. The guest kernel must support
containers: cgroup v2, namespaces, veth, bridge, conntrack, iptables, and NAT.
Docker 28 also requires `CONFIG_IP_NF_RAW=y` (and `CONFIG_IP6_NF_RAW=y` for IPv6).
The current bundled 6.1.175 kernel lacks these options. Build a compatible kernel
with the optional Docker target (compilation runs as a non-root user), then set
its path on the Compose node:

```sh
docker build --platform linux/amd64 -f compose-image/Dockerfile \
  --target kernel-output --output type=local,dest=compose-kernel .
```

```toml
[kernel]
image_path = "/absolute/path/compose-kernel/vmlinux-compose-6.1.175"
```

The kernel target pins the Firecracker guest configuration and Linux version, enabling
PCI support required for ACPI initialization in vanilla Linux, plus the two
raw-table options. It does not replace the host kernel or update other nodes.
The source archive is verified against a pinned SHA-256 digest. Parallel kernel
build jobs default to the available CPU count, capped at roughly one job per
GiB of available memory; pass `--build-arg JOBS=4` to override this heuristic.
Keep the resulting kernel consistent across nodes restoring the same
sandbox snapshots. Do not disable Docker's raw-table protections as a workaround.

### Create a sandbox

`aenv compose up` creates one sandbox running an image-only Compose project. The
command waits for the services to become running or healthy, prints only the
sandbox ID to stdout, and exits without attaching a shell. Each invocation
creates a new sandbox.

```sh
cat > compose.yaml <<'YAML'
services:
  web:
    image: nginx:1.27-alpine
    ports: ["8080:80"]
  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 1s
      timeout: 1s
      retries: 30
    volumes: ["data:/data"]
volumes:
  data: {}
YAML
aenv compose up -f compose.yaml --cpu 2 --memory 2048 --timeout 600
```

| Flag | Description |
|------|-------------|
| `-f, --file <PATH>` | Compose YAML or JSON file (default: `compose.yaml`); `-` reads stdin. Maximum 1 MiB. |
| `--env <KEY=VALUE>` | Explicit interpolation variable; repeatable, with the last value winning. Empty values are allowed. |
| `--profile <NAME>` | Enable an optional Compose profile; repeatable. |
| `--timeout <secs>` | Sandbox TTL after readiness (default: 300). |
| `--startup-timeout <secs>` | Total startup budget, including image resolution and health checks (1–300; default: 300). |
| `--cpu <count>` | CPU cores. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory in MiB. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Root filesystem size, at least 1024 and divisible by 1024 MiB. Alias: `--disk-mb`. |

After `aenv auth`, the CLI reads the file, waits for Compose readiness, and prints
the sandbox ID. Only `--env` values are used for interpolation; local environment
variables and `.env` files are not loaded. Resource defaults come from the server.
The HTTP request allows an additional 60 seconds beyond the startup budget for
cleanup. Startup failures produce a non-zero exit status and the API error on
stderr. Manage the returned sandbox with `aenv exec`, `connect`, `pause`,
`resume`, `snapshot`, and `delete`. The same endpoint remains available directly:

```sh
jq -n --rawfile compose compose.yaml \
  '{compose:$compose,cpuCount:2,memoryMB:2048,timeout:600,startupTimeout:300}' \
  | curl --max-time 330 -fsS "$AENV_API_URL/sandboxes-compose" \
      -H "X-API-Key: $AENV_API_KEY" -H 'Content-Type: application/json' --data-binary @-
```

The response is the usual sandbox object and `x-agentenv-sandbox-id` header.
Access published TCP ports through the existing sandbox proxy; they are published
inside the VM. Port 49983 is reserved for envd. Services can reach one another by
Compose service name on their bridge network.

`composeEnv` supplies interpolation variables; `profiles` selects optional
services. The node does not read its process environment, `.env`, or local files.
Interpolation runs once: literal `$$` and dollars introduced by `composeEnv`
survive the guest's second Compose load. Image pulls happen on the node using
its configured registry credentials; the guest uses local aliases and
`--pull never --no-build`.

`startupTimeout` is 1–300 seconds (default 300), covering planning, image
resolution, VM boot, registration, and Compose readiness. Cleanup may extend
the response time. In-flight host resource acquisitions finish before rollback
so their devices and processes can be reclaimed; no further startup stage begins
after the deadline is observed. The gateway reserves at least 330 seconds for this route.
`timeout` is the sandbox TTL after readiness (default 300); `autoPause` defaults
to true. Compose creation enables secure envd access; use the returned token
when executing commands through envd.

The API publishes `Running` only after `docker compose up --wait` succeeds.
Services without healthchecks must be running; services with healthchecks must
be healthy. Dependencies use Compose's normal `depends_on` semantics. A failure
or deadline expiry destroys the partial sandbox through the existing lifecycle
cleanup. Guest runtime logs are under `/var/log/agentenv-compose`; normalized
Compose and source image configurations are under `/var/lib/agentenv-compose`.

### Supported scope

- One container per service, 1–24 selected services, maximum 1 MiB Compose source.
- Maximum 2 MiB HTTP request and 4 MiB complete guest startup plan, including
  resolved image metadata. YAML expansion and variable interpolation have bounded
  budgets; YAML nesting is limited to 128 levels. Oversized plans are rejected
  before VM allocation.
- Image references, commands, entrypoints, environment, healthchecks, dependencies,
  profiles, local named volumes, tmpfs, bridge networks, fixed published TCP ports.
  Conflicting published ports across selected services are rejected before startup.
- `network_mode: service:<name>` shares another selected service's network
  namespace. `cap_add` permits `SYS_PTRACE` inside the sandbox for debugging tasks.
- Existing sandbox pause/resume, capture, fork, and deletion. VM memory and disk
  snapshots preserve the running Docker/containerd/snapshotter processes; restore
  does not register images again or repeat Compose initialization.

Inline builds at the runtime API, replicas/scaling, host bind mounts, external files (`env_file`, `include`,
`extends`, `label_file`), secrets/configs, external networks/volumes, custom volume
drivers, host networking/devices, privileged mode, and UDP publishing are rejected.
Named volumes live in the sandbox root disk; they are not AgentENV managed volumes.

The plain snapshotter keeps active snapshot metadata in memory. Restarting
Docker/containerd/the snapshotter independently is unsupported. Recreating a
container reuses its service's writable rootfs rather than discarding its writes.
To reset the application, create a new sandbox. This initial integration depends
on the snapshotter introduced in upstream PR #318.

### Validate an installation

On an isolated test node with `aenv` installed, run:

```sh
cargo test -p aenv --bin aenv commands::compose
AENV_CLI=/absolute/path/aenv python3 compose-image/test_e2e.py GuestRuntimeProcessTests
AENV_API_URL=http://127.0.0.1:8001 AENV_API_KEY=... \
  python3 compose-image/test_e2e.py
```

The test checks same-image service isolation, health dependencies, DNS, published
ports, interpolation, pause/resume, fork, snapshot restore, and failure cleanup.
It deletes its sandboxes and snapshots and uses temporary CLI credentials.
