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

To build a standalone image, run `aenv build --image ./app`. The command prints
the published `sha256:...` reference on stdout; build progress goes to stderr.
Use that reference in a sandbox `image` field, or cold start it with
`aenv start --cold sha256:...`. Dockerfile, build
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

Image builds use the `/images/builds` API and do not create template records.
Build status survives worker cleanup and server restart until
`DELETE /images/builds/{buildID}`, which keeps published images and their layers.
Upgrade the server, gateway, and CLI together to use the image API.

### Tag images

An image tag is a mutable `name:tag` pointer to one published image. Reference
it as `aenv/name:tag` wherever a `sha256:...` image reference is accepted,
for example `aenv start --cold aenv/my-app:v1`. Omitting `:tag` means
`latest`. The name uses lowercase letters and digits joined by single `.`, `_`,
or `-`; the tag uses letters, digits, `.`, `_`, and `-` and does not start with
`.` or `-`. Each is at most 128 characters. Short names under `aenv/` always
refer to tags; reference an OCI image in a registry's `aenv` namespace by its
full name, such as `docker.io/aenv/app:v1`.

`aenv build --image ./app --tag my-app:v1` tags the image after it is
published; repeat `--tag` for more tags. Stdout still contains only the digest.
`PUT /images/tags/{tag}` with `{"imageDigest": "sha256:..."}` creates a tag or
moves it to another published image, and `GET /images/tags/{tag}` returns the
digest it points to. `GET /images/tags?limit=100` lists tags ordered by
`name:tag`, paged with `nextToken` like `GET /images`. `DELETE /images/tags/{tag}`
removes a tag, returns `204` even if it is already absent, and never deletes the
image. Tags live at `catalog/image-tags/{name}:{tag}.json` in the shared
repository.

A tag resolves to its digest when a sandbox, volume, or template build uses it.
Sandboxes, snapshots, and node caches record only the digest, so moving a tag
later does not change existing workloads. Each tag resolution reads one more
repository object than a digest reference.

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
node using the shared repository; build status still routes to the build
node. BuildKit's `imageName` identifies its temporary export and is not a
user-managed image tag.

Deleting an image prevents new resolutions, including local cache hits. Existing
workloads, pause/resume, forks, and snapshots retain their captured layers. Layer
objects remain in shared storage, so deletion does not immediately reclaim disk
or OSS space. A concurrent or later build can publish the same digest again.
Completed build results remain historical records even if their image is deleted.
Pagination is not a frozen view: concurrent publication and deletion may change
later pages; continue while `nextToken` is present, including after an empty page.
Deleting an image leaves tags that point to it in place; resolving such a tag
fails until it is moved to a published image.
