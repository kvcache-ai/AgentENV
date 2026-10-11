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

## Build reusable images

To build a standalone image, run `aenv build --image ./app`. The command prints
the published `sha256:...` reference on stdout; build progress goes to stderr.
Use that reference in a sandbox `image` field. Dockerfile, build
arguments, secrets, cache, progress, and timeout flags work as for template
builds. Template naming, startup overrides, and sandbox resource flags do not
apply to image builds; worker resources come from the server configuration.

Images are referenced by the SHA256 of its canonical OverlayBD
image description (`sha256:...`), rather than its original OCI manifest digest.
The description binds the ordered layer digests, platform, and OCI runtime
configuration, including entrypoint, command, environment, and healthcheck.
Descriptions live at `catalog/images/{digest}.json`; layer data uses the same
content-addressed managed-layer storage as snapshots. Node-local paths are
materialized on resolution and are not part of the image identity.

Builders and runtime nodes must use the same repository. Every compatible node
with repository access can resolve an image independently of the build node's
cache. Local cache eviction does not remove published images. With a POSIX
backend, cross-node use requires a shared filesystem. Publication completes
before status becomes `ready`; failed publication does not expose a usable build
result. Uploaded immutable objects may remain for reuse after a failed build.

Image builds have an independent API:

- `POST /images/builds` accepts `{ "timeout": 3600 }` and returns `buildID` and
  the required BuildKit `imageName`.
- `GET /images/builds/{buildID}/builder` upgrades to the BuildKit WebSocket tunnel.
- `GET /images/builds/{buildID}` reports `waiting`, `building`, `ready`, or `error`;
  successful results include the published description's `imageDigest`.
- `GET /images/builds/{buildID}/logs` accepts bounded `offset` and `limit` queries.
- `DELETE /images/builds/{buildID}` releases the worker, result, and diagnostics.
  Published images and their shared layers are retained.

Build status and logs survive worker cleanup and server restart until explicit
deletion. The node's build journal retains results and heartbeat routing without
occupying a build slot. Image builds do not create template records. The existing
template builder API continues to publish runnable VM snapshots. Upgrade the
server, gateway, and CLI together to use the image API.

### Manage published images

Published images have a separate resource API, using the same API-key authentication:

| Request | Result |
| --- | --- |
| `GET /images?limit=100` | Digest-ordered summaries with platform and layer count; `limit` is 1–100. |
| `GET /images?limit=100&nextToken=...` | Continue using the preceding response's `nextToken`; omission means the end. |
| `GET /images/{imageDigest}` | The digest and complete immutable OverlayBD description, including OCI runtime configuration. |
| `DELETE /images/{imageDigest}` | Remove the description; returns `204` even if already absent. |

Images and volumes share layer publication and local config generation. Their
catalog records and lifecycles are independent. Image management can run on any
node using the shared repository; build progress and logs still route to the
build node. BuildKit's `imageName` identifies its temporary export and is not a
user-managed image tag.

Deleting an image prevents new resolutions, including local cache hits. Existing
workloads, pause/resume, forks, and snapshots retain their captured layers. Layer
objects remain in shared storage, so deletion does not immediately reclaim disk
or OSS space. A concurrent or later build can publish the same digest again.
Completed build results remain historical records even if their image is deleted.
Pagination is not a frozen view: concurrent publication and deletion may change
later pages; continue while `nextToken` is present, including after an empty page.
