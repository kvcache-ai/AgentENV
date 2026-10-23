# Starting a Sandbox

There are two ways to start a sandbox: warm start from a reusable template or
snapshot, or cold start directly from an OCI image.

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
| `--image-mount <mount-path>=<image>` | None | Mount a writable copy of an image owned by the sandbox; see [Mount images](#mount-images). Repeatable. |
| `--image-mount-ro <mount-path>=<image>` | None | Mount an image read-only. Repeatable. |
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
| `--image-mount <mount-path>=<image>` | None | Mount a writable copy of an image owned by the sandbox; see [Mount images](#mount-images). Repeatable. |
| `--image-mount-ro <mount-path>=<image>` | None | Mount an image read-only. Repeatable. |
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

## Mount images

Warm and cold starts can mount image filesystems at guest paths as writable
copies by default, or read-only when explicitly requested:

```bash
aenv start my-template \
  --image-mount /workspace=aenv/repo:main \
  --image-mount-ro /mnt/models=aenv/models:v1
```

The API takes `imageMounts` entries with `image`, `mountPath`, and optional
`readOnly` (default `false`) on `POST /v2/sandboxes` and
`POST /sandboxes-cold`. The CLI uses v2 for warm starts. Sandbox responses list
them under `imageMounts`. Mount size equals the image's virtual size. Images exceeding `volume.max_size_mb` (default 256 GiB) are rejected with
HTTP 400.

An image mount takes a published image digest (`sha256:...`), an image tag
(`aenv/name:tag`), or an OverlayBD-native image reference. These reference
existing layers, so mounting copies no image data. A standard OCI image that
needs node-local conversion is rejected; publish it first, for example with
`aenv build --image` and a `FROM <image>` Dockerfile, or create a
[volume](../volumes/create-and-mount.md) from it.

Each image mount is an exclusive volume that belongs to the sandbox. It counts
toward `volume.max_volume_count` together with `volumeMounts`, is hidden from
`GET /volumes`, and is deleted with the sandbox without capturing its writes.
To keep data beyond the sandbox, use a volume instead.

| Operation | Image mounts |
|---|---|
| Snapshot | Captures each mount's current state, as for volumes. |
| Launch from a snapshot | Recreates the mounts as copies owned by the new sandbox. Providing explicit `volumeMounts` or `imageMounts`, including empty arrays, is rejected when the snapshot has volume or image mounts. Omit both fields to restore the captured mounts. |
| Fork | Gives each child its own copy. |
| Pause and resume | Keeps the mounts and their writes. |

Snapshots reference the image's layers rather than its digest or tag, so moving
the tag later does not change them.

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
