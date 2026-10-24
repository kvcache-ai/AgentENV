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
set; build planning only requires the planner.
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

### Build service images

If services specify `build`, first use the CLI's Compose build mode:

```sh
aenv build --compose compose.yaml --output compose.built.yaml
aenv compose up -f compose.built.yaml
```

`aenv build --compose` builds the active services' Dockerfiles with the remote
BuildKit builder, one image build (`POST /v3/templates` with `imageOnly`) per
service, as `aenv build --image` does. The server converts each finished
image to OverlayBD and publishes its layers and description to the configured
OSS or POSIX repository. No destination registry is required. It does not start the service
containers or create runnable VM templates. Services that already specify only
`image` are retained. The runtime API continues to accept image-only
projects; it does not receive local build contexts.

Like Docker Compose, each service's image is named `PROJECT-SERVICE` and tagged
`latest`, so it can also be referenced as `aenv/PROJECT-SERVICE:latest`. The
project defaults to the name of the Compose file's directory; set it with
`--name`. Both names are lowercased, and other characters become single `-`
separators. Each build moves the `latest` tag; the output file still pins
every built service by digest. Each service build leaves a build record, which
`DELETE /templates/{buildID}` removes while keeping the image and its tags.

The CLI requests a build plan from `POST /sandboxes-compose/plan` on the server.
This authenticated endpoint accepts `compose`, `composeEnv`, `profiles`, and
`harbor`, and returns normalized Compose plus per-service build instructions.
It has a 30-second planning budget, reads no local files, and allocates no builders
or sandboxes. Only the server needs `aenv-compose-plan`; upgrade it together with
the CLI. Build contexts are subsequently uploaded from the client to BuildKit.

| Flag | Description |
|------|-------------|
| `--compose <path>` | Local Compose YAML or JSON file. |
| `--name <project>` | Project name prefixing service image names; defaults to the Compose file's directory name. |
| `--image-repository <registry/repository>` | Optional: additionally push images to this registry and reference the pushed tags instead of the repository image. See "Registry distribution" below. |
| `--output <path>` | Output file; defaults to `compose.built.yaml` beside the input. Must not already exist. |
| `--env KEY=VALUE` | Explicit interpolation/build argument environment; repeatable, last value wins. Process environment and `.env` files are not loaded. |
| `--profile <name>` | Select optional services; repeatable. Selected profiles are resolved into the output. |
| `--harbor` | Apply Harbor's main-service build and keepalive defaults before normalizing a task's Compose override file. Explicit task settings take precedence. |
| `--registry-insecure` | Permit HTTP or untrusted TLS for pushes to a development registry. Runtime registry access must be configured separately. |

Shared build flags `--no-cache`, `--buildctl`, `--progress`, and `--timeout` also
apply (`--timeout` is per service). Put per-service build arguments in the Compose
file. Supported build fields are `context`, `dockerfile`, `args`, `target`, and
`no_cache`, including the `build: ./directory` shorthand. Contexts must be local
directories; relative contexts resolve beside the Compose file and Dockerfiles
resolve relative to their build context. As in Compose, a context may lie
outside the Compose file's directory; the CLI uploads every context the file
names. Images are built for the builder's platform; the Compose runtime
requires x86-64.

The input file is unchanged. The output is JSON, which Compose accepts as YAML,
and contains no `build` or `pull_policy: build` entries. It is written only after
every service succeeds; partial failures may leave published images in the repository. Unsupported Compose features and missing build files are rejected
before allocating builders. The server must support the image build API.

#### Registry distribution

To share images with consumers outside the shared AgentENV repository or use
registry retention, pass
`--image-repository REGISTRY/REPOSITORY`. Each build then additionally pushes a
unique tag, and the built file references those tags instead of OverlayBD description digests.
The repository must be reachable from both the builder VM and the runtime node.
BuildKit uses the CLI user's Docker registry credentials for push. Runtime nodes
need their own pull credentials. For private registry addresses, the node's
`network.egress.always_denied_cidrs` must permit builder access. The registry
cannot run on the builder's own node: the node rejects connections that VMs open
to the host's addresses.
`--registry-insecure` changes TLS handling only; it does not bypass the node's
network policy. The CLI installer bundles `aenv-buildctl`; a local Docker daemon
is not required.

For Terminal-Bench 4.0 tasks, use the original environment directory, and name
the project after the task because every task's Compose file is in `environment/`:

```sh
aenv build --compose tasks/freight-dispatch-shift/environment/docker-compose.yaml \
  --harbor --name freight-dispatch-shift --output freight.built.yaml
aenv compose up -f freight.built.yaml --cpu 4 --memory 8192
```

`--harbor` supplies `main.build.context: .` when neither image nor build is set,
and `main.command: [sh, -c, sleep infinity]` when command is omitted, matching
Harbor's build base configuration. It prepares the task environment; it does not
run Harbor agents, inject verifier files, or grade benchmark solutions.

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

### Validate Compose builds

To test original Terminal-Bench 4.0 Compose environments on an isolated node:

```sh
git clone --depth 1 --branch v4.0.0 https://github.com/harbor-framework/terminal-bench.git
AENV_API_URL=http://127.0.0.1:8001 AENV_API_KEY=... \
  python3 compose-image/test_e2e.py terminal-bench --tasks terminal-bench/tasks \
  --results tb4-results
```

The script tests all tasks containing `environment/docker-compose.yaml`, or
repeat `--task NAME` to select tasks. It uses `aenv build --harbor` followed by
`aenv compose up`, checks service health and execution in `main`, records timings
and source hashes, and deletes each sandbox. Built-file reports are retained.
This validates environment startup, not benchmark solution scores.

With the image catalog API installed, set `AENV_IMAGE_CATALOG_TEST=1` to also
verify that deleting a shared service image preserves running services, snapshots,
pause/resume, and forks. Standalone image/catalog tests run with `make test-buildkit`.
