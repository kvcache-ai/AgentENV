# Templates

A template is a reusable starting point for launching sandboxes. Build or
import it once, then use it to create sandboxes whenever you need the same
software and configuration.

## Create Your Template

There are two ways to create a template: `aenv pull` imports an OCI image directly, and `aenv build` runs Dockerfile instructions inside a temporary build sandbox.

Some defaults below come from your AgentENV config file. This is
`config/default.toml` by default, or the file specified by
`AENV_CONFIG_PATH`.

### aenv pull

Pull an existing OCI image as a template and optionally give it a memorable name:

Usage:

```bash
aenv pull <image> [options]
```

Example:

```bash
aenv pull ubuntu:24.04
aenv pull ubuntu:24.04 --name my-base
```

`--name` is optional. Without it, AgentENV uses the image repository name. A template name can
be used anywhere a template ID is accepted.

| Argument or option | Default | Description |
|---|---|---|
| `<image>` | Required | OCI image reference. Short names such as `ubuntu:24.04` and full references are supported. |
| `--name <name>` | Image repository name | Assign a human-readable template name. |
| `--cpu <count>` | `[machine].vcpu_count` from your config file | Set the template's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set the template's memory. Aliases: `--memory-mb`, `--mem`. |
| `--start-cmd <cmd>` | None | Run a command before capturing the template snapshot. |
| `--ready-cmd <cmd>` | `sleep 20` when `--start-cmd` is set; otherwise none | Poll a shell command every two seconds until it exits successfully. |
| `--probe <port>` | None | Wait for TCP on `localhost:<port>`. Cannot be combined with `--ready-cmd`. |
| `-d`, `--detach` | Off | Submit the build and return immediately instead of waiting. |
| `--timeout <seconds>` | No timeout | Limit how long the CLI waits for the build. Cannot be combined with `--detach`. |

`Env`, `WorkingDir`, and `User` are automatically inherited from the OCI image config. See [Runtime Configuration](#runtime-configuration) for the full field list.

### aenv build

Build a Dockerfile with BuildKit inside a temporary microVM, then convert the
result to OverlayBD and capture a template. The CLI and full AgentENV installers
include `buildctl` v0.33.0. Source builds can use `--buildctl` to select a compatible
client. Docker and a staging registry are not required on the CLI machine.

```bash
aenv build <context> --name <name> [options]
# From the repository root:
aenv build . -f deploy/docker/Dockerfile.agentenv --name aenv
```

| Argument or option | Default | Description |
|---|---|---|
| `<context>` | Required | Local context directory, as with `docker build`. |
| `-f, --file <path>` | `<context>/Dockerfile` | Dockerfile path; explicit relative paths resolve from the current directory. |
| `--name <name>` | Required | Assign the template name. |
| `--cpu <count>` | `[machine].vcpu_count` from your config file | Set the template's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set the template's memory. Aliases: `--memory-mb`, `--mem`. |
| `--start-cmd <command>` | Image `ENTRYPOINT`/`CMD` | Override template startup; an empty string disables startup. |
| `--ready-cmd <command>` | Image `HEALTHCHECK`, or the normal startup delay | Override the command that must succeed before snapshot capture. |
| `--build-arg KEY=VALUE` | None | Build argument; repeatable. |
| `--secret <spec>` | None | Native BuildKit secret mounts; repeatable. |
| `--no-cache` | False | Disable instruction cache; cache mounts retain their usual BuildKit semantics. |
| `--buildctl <path>` | `buildctl` | Local client executable. |
| `--progress <format>` | `auto` | Three-stage bar on terminals, plain logs when redirected. `plain` selects plain logs; `tty` selects BuildKit's native display. |
| `--timeout <seconds>` | 3600 | Builder preparation and Dockerfile build deadline; the CLI allows 10 additional minutes for publication. |

`COPY` and `ADD` resolve from the context directory, independently of the
Dockerfile's location. Local directories are supported; URL and stdin contexts
are not supported. The final Dockerfile stage is always published. Select base
images with `FROM` (or `ARG` used by `FROM`) and startup with `ENTRYPOINT`/`CMD`.
`--start-cmd` and `--ready-cmd` override startup and readiness independently.
There are no image, stage-selection, SSH, or builder-resource overrides in the
build CLI.

Managed builder settings belong to the server configuration:

```toml
[template_build]
builder_image = "docker.io/moby/buildkit:v0.33.0"
builder_cpu_count = 16
builder_memory_mb = 32768
cache_size_mb = 262144
```

The 256 GiB disk is the persistent `/var/lib/buildkit` data volume, where image
layers, build contexts, and cache mounts live. Its capacity applies when creating
the cache; changing the setting does not resize an existing volume. These
resources are separate from the resulting template's CPU and memory.

BuildKit handles multi-stage builds, `COPY`, `ADD`, `.dockerignore`, cache mounts,
and Dockerfile syntax. The CLI remains connected during the build, streams build
progress, and exits nonzero on failure. The server provisions and releases an
internal worker for each template build. The first Dockerfile build prepares a
reusable builder snapshot in a private namespace of the configured snapshot
repository. Nodes sharing that repository restore the same builder and attach a
new cache volume before starting BuildKit. Concurrent first requests on a node
share initialization. Simultaneous first builds on different nodes may both
prepare a builder; subsequent builds reuse the published snapshot. The CLI uses
only template and build IDs; workers are absent from public sandbox listings and endpoints. Cancellation
and deadlines release the worker and discard its incomplete cache child. A hard client failure
is covered by the build deadline, and server restart recovers unfinished builds
and cache reservations from a durable journal. A publication already accepted by the
server may finish after the CLI is interrupted; its status remains available:

```bash
aenv template watch my-template
```

`aenv template watch` has no timeout option. Stop the
local watch with `Ctrl-C`; the remote build continues.

The node reads the completed image directly from the builder by SHA-256 digest.
It verifies the transferred bytes and converts only missing layers, reusing the
same content-addressed OverlayBD cache as registry image imports. The completed
image never travels through the CLI. Only the final image configuration becomes
template configuration; intermediate stages and build-time arguments are not
template environment variables.

Publishing also boots the image and runs its startup command. An image
can compile and convert successfully but fail this step if its entrypoint needs
devices absent from the guest. For example, `deploy/docker/Dockerfile.agentenv`
starts the host AgentENV server, which requires `/dev/kvm`; it cannot run as a
template in a guest without nested KVM support. Use `--start-cmd` to select another
startup command, or `--start-cmd "" --ready-cmd true` to capture without starting
the image's application or running its health check.

Numeric Dockerfile `USER` values are resolved to guest account names for envd.
A missing numeric account gets an entry with the requested UID/GID; existing
accounts are preserved. The guest image still needs `/bin/sh` for envd process
execution, including exec-form Dockerfile commands.

Caches are shared across template names and nodes using the configured snapshot
repository. Each build clones the latest immutable cache seed into its own
writable volume. Sequential builds inherit the preceding build's accumulated
instruction cache and `RUN --mount=type=cache` data. Concurrent builds can fork
the same seed without waiting for each other; the last successfully published
cache becomes the next seed. Sibling cache additions are not merged.

Cache volumes use normal volume publication, uploading only missing layers.
Cache publication adds cleanup time, but a failed cache upload does not fail an
otherwise successful template build or replace the previous seed. Old cache
volumes are removed after active children release their leases. BuildKit's
garbage collector manages cache contents. Cache sharing uses the repository's
existing API-key trust boundary. Earlier per-template and per-node caches are
not imported automatically. Builder-template upgrade handling is not automatic.
Registry credentials come from the local BuildKit session and normal Docker
credential configuration.

The additive API is template-scoped:

1. `POST /templates/builds` accepts `template` and optional `startCmd`, `readyCmd`,
   and `timeout`. It returns template/build IDs. Worker settings come from the
   node configuration.
2. Poll the existing `GET /templates/{templateID}/builds/{buildID}/status` endpoint:
   `waiting` means the builder template or worker is preparing; `building`
   means it can accept a build.
3. `GET /templates/{templateID}/builds/{buildID}/builder` opens a binary
   WebSocket carrying native BuildKit traffic.
4. `POST /templates/{templateID}/builds/{buildID}/builder` accepts the completed
   image `digest` and starts server-owned import and publication.
5. `DELETE /templates/{templateID}/builds/{buildID}/builder` cancels an unsubmitted
   build and waits for worker cleanup. Accepted publication continues server-side.

These endpoints require the API key and route to the owning node through the
gateway. Status polling uses the active build's node binding; after the binding
expires, it uses the shared template repository as before. Image import has a
one-hour deadline; metadata is limited to 4 MiB per
blob and images to 1024 layers and 64 GiB of compressed data. Existing template
endpoints retain their current behavior and report final publication status.

By default, image `ENTRYPOINT` and `CMD` are combined for startup through `/bin/sh -c`.
Dockerfile `HEALTHCHECK` supplies the readiness command before snapshot capture;
shell checks honor Dockerfile `SHELL`, and `HEALTHCHECK NONE` disables the check.
It uses AgentENV's readiness polling and deadline, not Docker's health-monitoring
intervals or restart behavior. Without a check, startup uses the normal template
readiness delay. `--start-cmd` and `--ready-cmd` (API fields `startCmd` and `readyCmd`)
override these commands independently without modifying the image configuration.
Images must satisfy AgentENV's normal guest runtime requirements;
`EXPOSE` and `VOLUME` are metadata, not Docker runtime services.

### Runtime Configuration

Both methods read the same set of OCI image config fields. For `aenv pull`, these come from the image config or flags; for `aenv build`, they are set by the
corresponding Dockerfile instructions executed during the build. The following fields from the [OCI image-spec config object](https://github.com/opencontainers/image-spec/blob/main/config.md) are recognised:

| OCI field | Dockerfile instruction | Runtime effect |
|-----------|------------------------|----------------|
| `Env` | `ENV` | Environment variables injected into every sandbox process |
| `WorkingDir` | `WORKDIR` | Default working directory |
| `User` | `USER` | Default user |
| `Entrypoint` / `Cmd` | `ENTRYPOINT` / `CMD` | Mapped to `startCmd` for `aenv build`; use `--start-cmd` explicitly for `aenv pull` |
| `ExposedPorts` | `EXPOSE` | Stored as metadata only |
| `Volumes` | `VOLUME` | Stored as metadata only |
| `Labels` | `LABEL` | Stored as metadata only |

## Manage Templates

### List templates

```bash
aenv template list        # alias: aenv template ls
aenv template list --output json
```

Displays all templates with their ID, name, build status, CPU, memory, disk
size, and last-updated timestamp. `--output` accepts `table` or `json`. It
defaults to `table` in an interactive terminal and `json` when output is piped
or redirected.

### List template builds

List the complete build history for one template:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/templates/<template-id>
```

### Check a template alias

Check whether an alias exists and resolve it to a template ID:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/templates/aliases/<alias>
```

### Delete a template

```bash
aenv template delete <template-id-or-name>   # alias: aenv template rm
```

## Relationship to Snapshots

Templates are the API and UX layer. Snapshots are the durable runtime layer.

- A template build publishes one committed snapshot.
- A template ID or alias resolves to one committed snapshot.
- A sandbox created from a template resumes from that snapshot.

If you want the storage and runtime model underneath templates, see
[Snapshots](./snapshots.md).
