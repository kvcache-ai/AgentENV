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

`aenv compose up` runs a Docker Compose project inside one sandbox. It uses
only the existing sandbox API: the CLI plans the project locally, builds the
services that declare [`build`](#build-service-images), cold starts a Compose guest image with one writable
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
`connect`, `pause`, `resume`, `snapshot`, and `delete`. As with Docker Compose,
containers are named `PROJECT-SERVICE-1`.

The project options behave as in Docker Compose:

| Argument or option | Default | Description |
|---|---|---|
| `-f`, `--file <path>` | `compose.yaml`, `compose.yml`, `docker-compose.yml` or `docker-compose.yaml` in the current or a parent directory, with its override file | Compose file. Repeatable; later files are merged into earlier ones. `-` reads stdin. |
| `--project-directory <dir>` | First Compose file's directory; current directory for stdin | Project directory, which relative paths and the default `.env` file resolve against. |
| `--env-file <path>` | `.env` in the project directory | Interpolation environment file. Repeatable. |
| `-p`, `--project-name <name>` | Compose `name`, `COMPOSE_PROJECT_NAME`, or the project directory's name | Project name. Also prefixes built service image names. `--name` is an alias. |
| `--profile <name>` | `COMPOSE_PROFILES` | Enable an optional Compose profile. Repeatable. |
| `--env <KEY=VALUE>` | None | Set a process environment variable for the project, overriding the inherited one. Repeatable; the last value wins. Empty values are allowed. |

The other options configure the sandbox:

| Argument or option | Default | Description |
|---|---|---|
| `--base-image <image>` | Bundled image, built on first use | Compose guest image built from `images/compose/Dockerfile`. |
| `--no-build` | Off | Do not build services; use their published `aenv/PROJECT-SERVICE:latest` images. |
| `--timeout <seconds>` | `300` | Sandbox TTL once the services are ready. |
| `--startup-timeout <seconds>` | `300` | Time allowed for Docker readiness, image registration, and `docker compose up --wait`. |
| `--cpu <count>` | `[machine].vcpu_count` | Set the sandbox's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` | Set sandbox memory. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Guest image virtual size | Set the root filesystem size, which holds named volumes. Must be divisible by 1024 MiB. Alias: `--disk-mb`. |
| `--planner <path>` | `aenv-compose-plan` next to `aenv` | Compose planner executable. |

`aenv compose up` runs these steps:

1. Run the planner locally. It loads the project with
   [compose-go](https://github.com/compose-spec/compose-go) as Docker Compose
   does, then checks that the sandbox can run it; an unsupported project fails
   here without creating a sandbox.
2. Build the services that declare `build`, as described in
   [Build service images](#build-service-images), unless `--no-build` is set.
3. Create the sandbox with `POST /sandboxes-cold`. Service `N` gets the writable
   attached drive `compose_N`, mounted at `/mnt/compose_N`. If the bundled
   image is not published yet, the server rejects the request before creating
   a sandbox; the CLI builds the image and retries once.
4. Read the service image configurations, which the node publishes in instance
   metadata, and register each drive under a placeholder image name.
5. Run `docker compose up --wait --pull never --no-build` with a clean
   environment and the project name, passing the rendered project on stdin.
6. Set the sandbox TTL to `--timeout`.

The sandbox is `Running` during startup. If a step fails, or you press Ctrl-C,
the CLI deletes the sandbox and exits non-zero. If `aenv` is killed instead, the
sandbox expires after `--startup-timeout` plus 60 seconds and then auto-pauses;
delete it with `aenv delete`. Guest daemon logs are under
`/var/log/agentenv-compose`.

The project is loaded on the CLI machine exactly as `docker compose config`
loads it: `include`, `extends`, multiple files, `!reset` and `!override`
tags, `.env` and `env_file` files, and interpolation from the process
environment all behave as in Docker Compose. The rendered project contains the
resolved values, so the guest neither reads these files nor interpolates again:
literal `$$` and dollars in variable values reach the containers unchanged. The
node pulls service images with its own registry credentials. Published TCP ports are reachable through the
sandbox proxy, and services reach one another by service name.

Compose sandboxes support pause and resume, snapshots, and forks. VM snapshots
keep the running Docker, containerd, and snapshotter processes, so restoring
one does not register images or start Compose again. A fork gives each child its
own copy of the service drives.

### Build service images

If services specify `build`, `aenv compose up` builds them before creating the
sandbox, as Docker Compose does, and starts each from the image it just built.
To build without starting a sandbox, use `aenv compose build`, which writes an
image-only Compose file:

```bash
aenv compose up
aenv compose build
aenv compose up -f compose.built.yaml
aenv compose up --no-build
```

`aenv compose up` builds on every run; BuildKit's cache shortens repeated
builds. With `--no-build`, it skips building and starts each service from
`aenv/PROJECT-SERVICE:latest`, which an earlier `aenv compose build` or
`aenv compose up` with the same project name published. If that image does not
exist, the server rejects the request before creating a sandbox.
`aenv compose up` builds with the default BuildKit options and `no_cache` from
the Compose file; for `--no-cache`, `--timeout`, or registry pushes, run
`aenv compose build` first.

Building uses only existing APIs. The CLI runs the planner locally, then builds
each active service that declares `build` with the server's
[template builder](../templates/creating.md), as `aenv build --image` does. The
server converts each image to OverlayBD and publishes it to the shared
repository; no registry is required. Services that specify only `image` are
kept unchanged. `aenv compose build` does not start a sandbox.

Like Docker Compose, each service's image is named `PROJECT-SERVICE` and tagged
`latest`, so it can also be referenced as `aenv/PROJECT-SERVICE:latest`. The
project name is chosen as in Docker Compose; set it with `-p`. Service names are
lowercased, and other characters become single `-` separators. The output file
of `aenv compose build` pins every built service by digest. Each service build
leaves a build record, which `DELETE /templates/{buildID}` removes while keeping
the image and its tags.

`aenv compose build` accepts the project options of `aenv compose up`, including
`--planner`, and these options:

| Option | Default | Description |
|---|---|---|
| `--output <path>` | `compose.built.yaml` in the project directory | Image-only Compose file to write. Must not already exist. |
| `--image-repository <registry/repository>` | None | Also push each image to a unique tag in this repository and reference that tag. |
| `--registry-insecure` | Off | Allow HTTP or untrusted TLS for `--image-repository` pushes. |
| `--no-cache`, `--buildctl`, `--progress`, `--timeout` | As for `aenv build` | BuildKit options. `--timeout` applies to each service. |

The builder uses the build fields `context`, `dockerfile`, `args`, `target`, and
`no_cache`. `secrets`, `ssh`, `additional_contexts`, `dockerfile_inline`,
`entitlements`, and `privileged` are rejected; the other build fields are
ignored with a warning. Contexts must be local directories. As in Compose,
relative contexts resolve against the project directory, Dockerfiles resolve
against their context, and a context may lie outside the project directory; the
CLI uploads every context the project names. Arguments without a value take the
variable from the project environment, or keep their Dockerfile default. Images
are built for the builder's platform; the Compose runtime requires x86-64.

The input files are not modified. The output is JSON, which Compose also accepts
as YAML, and contains no `build` or `pull_policy` entries. Like the output of
`docker compose config`, it contains resolved environment values, which may
include secrets from `.env` or the process environment. Unsupported Compose
features and missing contexts or Dockerfiles are rejected before any builder is
allocated. Services are built one at a time; `aenv compose build` writes the
output, and `aenv compose up` creates the sandbox, only after every build
succeeds. Images built before a failure remain in the repository.

With `--image-repository REGISTRY/REPOSITORY`, each build also pushes a unique
tag, and the output references those tags instead of digests. Use this to share
images with consumers outside the AgentENV repository. BuildKit pushes with the
CLI user's Docker registry credentials, while runtime nodes pull with their own.
The registry must be reachable from both the builder VM and the runtime node;
for private addresses, the node's `network.egress.always_denied_cidrs` must
permit builder access, and the registry cannot run on the builder's own node.

### Supported scope

Any project that Docker Compose loads is loaded the same way. The sandbox then
runs it with these limits:

- 1–24 selected services with one container each. The rendered project is at
  most 4 MiB. `jobs`, `models`, and `provider` services are rejected.
- `scale` and `deploy.replicas` above 1 are rejected. Only `linux/amd64`
  platforms are accepted. A `pre_start` hook must use its service's image.
- Named volumes with the local driver, `tmpfs`, and bind mounts from guest
  paths. A bind source is a guest path unless the project spells it relative
  (`./`, `../`, `~`) or it lies in the project directory or an included
  project's directory; such local paths are rejected. Named volumes live on
  the sandbox root disk; they are not AgentENV [volumes](../volumes/index.md).
- Bridge networks, and `network_mode` of `none`, `host`, `bridge`, or
  `service:<name>`.
- Fixed published TCP ports bound to all guest addresses. Port 49983 is
  reserved for envd.
- External volumes, networks, configs, and secrets, and configs and secrets
  read from files or environment variables, are rejected; inline config
  `content` works.
- `develop` is ignored with a warning. Guest-only options such as
  `privileged`, `cap_add`, `devices`, and `sysctls` pass through, because the
  VM is the isolation boundary.
