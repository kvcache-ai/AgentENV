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
| `--volume <mount-path>=<volume>` | None | Mount a persistent volume by ID or name. Repeat the option to mount multiple volumes. Not allowed when the snapshot already has volume mounts. |
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
| `<image>` | Required | External OCI image reference, published image digest (`sha256:...`), or image tag (`aenv/name:tag`). |
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

## Build reusable images

To build a standalone image, run `aenv build --image --name my-app ./app`. The
command prints the published `sha256:...` reference on stdout; build progress
goes to stderr. Use that reference, or a tag such as `aenv/my-app:latest`, in a
sandbox `image` field, or cold start it with `aenv start --cold sha256:...`.
Dockerfile, build arguments, secrets, cache, progress, and timeout flags work as
for template builds. Startup overrides and sandbox resource flags do not apply
to image builds; worker resources come from the server configuration.

Images are referenced by the SHA256 of its canonical OverlayBD
image description (`sha256:...`), rather than its original OCI manifest digest.
The description binds the ordered layer digests, platform, and OCI runtime
configuration, including entrypoint, command, environment, and healthcheck.
Descriptions live at `images/descriptions/{digest}.json`; layer data uses the
same content-addressed managed-layer storage as snapshots. Node-local paths are
materialized on resolution and are not part of the image identity.

Builders and runtime nodes must use the same repository. Every compatible node
with repository access can resolve an image independently of the build node's
cache. Local cache eviction does not remove published images. With a POSIX
backend, cross-node use requires a shared filesystem. Publication completes
before status becomes `ready`; failed publication does not expose a usable build
result. Uploaded immutable objects may remain for reuse after a failed build.

Image builds use the template build API. `POST /v3/templates` with
`"imageOnly": true` creates the build; `name` (optionally `name:tag`) and `tags`
name the image, and no template is created. The builder, status, and logs
endpoints under `/templates/{templateID}/builds/{buildID}` then work as for a
Dockerfile template build, and a ready status reports `imageDigest`. The build
record lives at `images/builds/{buildID}.json`, so any node reports the
status after the worker exits. `DELETE /templates/{templateID}` removes the
build record and logs and keeps the published image and its tags. Upgrade the
server, gateway, and CLI together to use image builds.

### Tag images

An image tag is a mutable `name:tag` pointer to one image built under that
name. Reference it as `aenv/name:tag` wherever a `sha256:...` image reference
is accepted, for example `aenv start --cold aenv/my-app:v1`. Omitting `:tag`
means `latest`. The name uses lowercase letters and digits joined by single
`.`, `_`, or `-`; the tag uses letters, digits, `.`, `_`, and `-` and does not
start with `.` or `-`. Each is at most 128 characters. Short names under
`aenv/` always refer to tags; reference an OCI image in a registry's `aenv`
namespace by its full name, such as `docker.io/aenv/app:v1`.

Tags belong to their image name, like template tags. A build points the tag in
`--name NAME:TAG` and every `--tag TAG` at its image, or `latest` if neither is
given. Tags live at `images/tags/{name}/{tag}.json` in the shared repository.

A tag resolves to its digest when a sandbox, volume, or template build uses it.
Sandboxes, snapshots, and node caches record only the digest, so moving a tag
later does not change existing workloads. Each tag resolution reads one more
repository object than a digest reference.

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
target architectures:

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
The server Docker image and release bundle include the planner.

The guest kernel needs `CONFIG_CGROUPS`, `CONFIG_NAMESPACES`, `CONFIG_VETH`,
`CONFIG_BRIDGE`, `CONFIG_NF_CONNTRACK`, `CONFIG_IP_NF_IPTABLES`,
`CONFIG_IP_NF_NAT`, `CONFIG_IP_NF_RAW`, `CONFIG_IP6_NF_RAW`, and `CONFIG_PCI`.
Build a compatible kernel with the optional Docker target and configure its path:

```sh
docker build --platform linux/amd64 -f compose-image/Dockerfile \
  --target kernel-output --output type=local,dest=compose-kernel .
```

```toml
[kernel]
image_path = "/absolute/path/compose-kernel/vmlinux-compose-6.1.175"
```

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
| `--cpu <count>` | CPU cores. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory in MiB. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Root filesystem size, at least 1024 and divisible by 1024 MiB. Alias: `--disk-mb`. |

After `aenv auth`, the CLI reads the file, waits for Compose readiness, and prints
the sandbox ID. Only `--env` values are used for interpolation; local environment
variables and `.env` files are not loaded. Resource defaults come from the server.
Startup failures produce a non-zero exit status and the API error on stderr.
Manage the returned sandbox with `aenv exec`, `connect`, `pause`, `resume`,
`snapshot`, and `delete`.

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

Planning is limited to 30 seconds; guest image registration and Compose readiness
have a separate 300-second budget. The gateway allows at least 600 seconds per
request, and the CLI waits up to 660 seconds to leave room for image preparation,
VM startup, and cleanup. These request limits are not a total startup deadline:
after a request times out, sandbox creation may still finish in the background;
check the sandbox list before retrying.
`timeout` is the sandbox TTL after readiness (default 300); `autoPause` defaults
to true. Use the returned token when executing commands through envd.

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
- `network_mode: service:<name>` shares another selected service's network
  namespace. `cap_add` permits `SYS_PTRACE` inside the sandbox for debugging tasks.
- Existing sandbox pause/resume, capture, fork, and deletion. VM memory and disk
  snapshots preserve the running Docker/containerd/snapshotter processes; restore
  does not register images again or repeat Compose initialization.

Inline builds at the runtime API, replicas/scaling, host bind mounts, external files (`env_file`, `include`,
`extends`, `label_file`), secrets/configs, external networks/volumes, custom volume
drivers, host networking/devices, privileged mode, and UDP publishing are rejected.
Named volumes live in the sandbox root disk; they are not AgentENV managed volumes.
