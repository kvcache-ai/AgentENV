# Starting a Sandbox

Start a sandbox from a reusable template or snapshot, directly from an OCI image,
or from a multi-service [Compose project](#compose-projects).

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
| `--image-mount <mount-path>=<image>` | None | Attach a writable drive from an image; see [Attach image drives](#attach-image-drives). Repeatable. Not allowed when the snapshot already has volume mounts or attached drives. |
| `--image-mount-ro <mount-path>=<image>` | None | Attach a read-only drive from an image. Repeatable. Not allowed when the snapshot already has volume mounts or attached drives. |
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
| `--image-mount <mount-path>=<image>` | None | Attach a writable drive from an image; see [Attach image drives](#attach-image-drives). Repeatable. |
| `--image-mount-ro <mount-path>=<image>` | None | Attach a read-only drive from an image. Repeatable. |
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

## Attach image drives

Warm and cold starts can attach image filesystems as drives at guest paths:

```bash
aenv start my-template \
  --image-mount /workspace=aenv/repo:main \
  --image-mount-ro /mnt/models=aenv/models:v1
```

The CLI sends these as `attachedDrives` entries, on `POST /v2/sandboxes` for
warm starts and on `POST /sandboxes-cold` for cold starts. Each entry has a
`driveID`, a `source.image`, and optional `mountPath`, `subPath`, `readOnly`,
and `diskSizeMB`. The API defaults `readOnly` to `true`; the CLI sets it to
`false` for `--image-mount` and derives `driveID` from the mount path, such as
`image_mnt_models` for `/mnt/models`. The image accepts the same references as
a cold-start `<image>`.

An attached drive belongs to its sandbox and is removed with it. Writes are not
kept elsewhere; use a [volume](../volumes/index.md) for data that must outlive
the sandbox.

| Operation | Attached drives |
|---|---|
| Snapshot | Captures each drive's current state into the snapshot. |
| Launch from a snapshot | Restores the captured drives. Providing `attachedDrives`, including an empty array, is rejected when the snapshot has volume mounts or attached drives. |
| Fork | Gives each child its own copy of the drive state. |
| Pause and resume | Keeps the drives and their writes. |

On a warm start, attached drives and new volumes share the snapshot's reserved
drive slots, so their combined count cannot exceed the slots it has free.
Snapshots captured before reserved slots existed have none.

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

`aenv compose up` runs an image-only Docker Compose project inside one
sandbox. It uses only the existing sandbox API: the CLI plans the project
locally, starts a Compose guest image, cold or from a
[template](#start-from-a-template), with one writable
[image drive](#attach-image-drives) per service, and starts the services
through envd. Services that use the same source image still get separate
drives. Inside the VM, `containerd-plain-snapshotter` registers each mounted
root filesystem with Docker and keeps its image configuration (entrypoint,
command, environment, user, working directory, healthcheck, and volumes).

### Prepare the guest image and planner

The Compose runtime supports Linux x86-64 nodes. The CLI bundles the guest image
source from `images/compose`. Without `--base-image`, the first
`aenv compose up` builds it like `aenv build --image` with the server's
[template builder](../templates/creating.md) and publishes it as
`aenv/aenv-compose:<fingerprint>`, where the fingerprint covers the bundled
source. Later runs, from any node sharing the repository, reuse it, and a CLI
with a changed guest image builds a new one. The first build takes a few minutes
and needs the bundled `aenv-buildctl`; the builder must reach Docker Hub and
`download.docker.com`.

To use your own image instead, build `images/compose` and pass it with
`--base-image`:

```bash
aenv build --image --name my-compose images/compose
aenv compose up --base-image aenv/my-compose:latest -f compose.yaml
```

The CLI installers place the planner, `aenv-compose-plan`, next to `aenv`. For a
source build, run `go -C services build -o bin/aenv-compose-plan ./compose/cmd`
and pass the result with `--planner`, or copy it next to `aenv`.

Docker in the guest needs netfilter, bridge, and veth support, which the default
KVM guest kernel provides. A node that downloaded `vmlinux-6.18` before the
kernel asset was rebuilt with iptables support keeps the old file; remove
`<deps_path>/kernel/vmlinux-6.18/vmlinux.bin` so the node downloads it again.
Image drives also need the block device pool (`[pool.block]`, enabled by
default).

### Create a sandbox

```bash
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

The command waits until every service is running, or healthy if it has a
healthcheck, prints only the sandbox ID on stdout, and exits without attaching a
shell. Each invocation creates a new sandbox. Manage it with `aenv exec`,
`connect`, `pause`, `resume`, `snapshot`, and `delete`.

| Argument or option | Default | Description |
|---|---|---|
| `--base-image <image>` | Bundled image, built on first use | Compose guest image built from `images/compose/Dockerfile`; cold-starts the sandbox. |
| `--template [<template>]` | None | Warm-start from a template of the guest image; without a name, the bundled template, built on first use. See [Start from a template](#start-from-a-template). |
| `-f`, `--file <path>` | `compose.yaml` | Compose YAML or JSON file; `-` reads stdin. Maximum 1 MiB. |
| `--env <KEY=VALUE>` | None | Interpolation variable. Repeatable; the last value wins. Empty values are allowed. |
| `--profile <name>` | None | Enable an optional Compose profile. Repeatable. |
| `--timeout <seconds>` | `300` | Sandbox TTL once the services are ready. |
| `--startup-timeout <seconds>` | `300` | Time allowed for Docker readiness, image registration, and `docker compose up --wait`. |
| `--cpu <count>` | `[machine].vcpu_count` | Set the sandbox's vCPU count. Cold start only. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` | Set sandbox memory. Cold start only. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Guest image virtual size | Set the root filesystem size, which holds named volumes. Must be divisible by 1024 MiB. Cold start only. Alias: `--disk-mb`. |
| `--planner <path>` | `aenv-compose-plan` next to `aenv` | Compose planner executable. |

`aenv compose up` runs these steps:

1. Run the planner locally. It validates and normalizes the project; an
   unsupported project fails here without creating a sandbox.
2. Create the sandbox with `POST /sandboxes-cold`, or `POST /v2/sandboxes` with
   `--template`. Service `N` gets the writable attached drive `compose_N`,
   mounted at `/mnt/compose_N`. If the bundled image is not published yet, the
   server rejects the cold start before creating a sandbox; the CLI builds the
   image and retries once.
3. Read the service image configurations, which the node publishes in instance
   metadata, and register each drive under the placeholder image name the
   planner assigned.
4. Run `docker compose up --wait --pull never --no-build` with a clean
   environment, passing the rendered project on stdin.
5. Set the sandbox TTL to `--timeout`.

The sandbox is `Running` during startup. If a step fails, or you press Ctrl-C,
the CLI deletes the sandbox and exits non-zero. If `aenv` is killed instead, the
sandbox expires after `--startup-timeout` plus 60 seconds and then auto-pauses;
delete it with `aenv delete`. Guest daemon logs are under
`/var/log/agentenv-compose`.

Only `--env` values are used for interpolation; the local environment and `.env`
files are not loaded. Interpolation runs once, so literal `$$` and dollars in
`--env` values reach the containers unchanged. The node pulls service images
with its own registry credentials. Published TCP ports are reachable through the
sandbox proxy, and services reach one another by service name.

Compose sandboxes support pause and resume, snapshots, and forks. VM snapshots
keep the running Docker, containerd, and snapshotter processes, so restoring
one does not register images or start Compose again. A fork gives each child its
own copy of the service drives.

### Start from a template

You can also start projects from a template of the guest image:

```bash
aenv compose up --template -f compose.yaml --timeout 600
```

Without a name, `--template` uses the bundled template
`aenv-compose-<fingerprint>`, which the first run builds like `aenv build` with
the server's template builder. To use your own template, build it from
`images/compose` and pass its name:

```bash
aenv build --name my-compose --cpu 2 --memory 2048 images/compose
aenv compose up --template my-compose -f compose.yaml
```

A template start restores a captured guest instead of booting the image. For
small projects with cached images, it is not noticeably faster than a cold
start: sandbox creation is faster, but the first Docker operations after a
restore are slower. Compared with a cold start:

- The template fixes CPU, memory, and root disk size, so `--cpu`, `--memory`,
  and `--disk-size-mb` are rejected. Build another template for other sizes.
- Each service takes one of the template's free drive slots. The template has
  `[volume].max_volume_count` slots (4 by default), taken from the configuration
  of the node that built it. A project with more services fails before a sandbox
  is created; use a cold start, or rebuild the template after raising the
  setting.
- A CLI with a changed guest image builds a new bundled template; rebuild your
  own templates yourself.

### Supported scope

- 1–24 selected services with one container each. The rendered project is at
  most 4 MiB, and YAML nesting at most 128 levels.
- Every service needs `image`; `build`, `scale`, and `deploy.replicas` above 1
  are rejected. Only `linux/amd64` platforms are accepted.
- Named volumes with the local driver, `tmpfs`, and bind mounts from absolute
  guest paths. Named volumes live on the sandbox root disk; they are not
  AgentENV [volumes](../volumes/index.md).
- Bridge networks without custom IPAM, and `network_mode` of `none`, `host`,
  `bridge`, or `service:<name>`.
- Fixed published TCP ports bound to all guest addresses. Port 49983 is
  reserved for envd.
- Keys that read planner-host files or external resources (`env_file`,
  `label_file`, `include`, `extends`, `secrets`, `configs`) and other unlisted
  keys are rejected. Guest-only options such as `privileged`, `cap_add`, and
  `sysctls` pass through, because the VM is the isolation boundary.
