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
aenv build <dockerfile> --name <name> [options]
```

| Argument or option | Default | Description |
|---|---|---|
| `<dockerfile>` | Required | Path to the Dockerfile. |
| `--name <name>` | Required | Assign the template name. |
| `--image <ref>` | Dockerfile `FROM` | Override the first stage's base image. Alias: `--user-image`. |
| `--cpu <count>` | `[machine].vcpu_count` from your config file | Set the template's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set the template's memory. Aliases: `--memory-mb`, `--mem`. |
| `--context <dir>` | Dockerfile directory | Directory sent through BuildKit's native context protocol. |
| `--target <stage>` | Final stage | Stage to publish. |
| `--build-arg KEY=VALUE` | None | Build argument; repeatable. |
| `--secret <spec>` / `--ssh <spec>` | None | Native BuildKit secret and SSH forwarding; repeatable. |
| `--cache-volume <name>` | Derived from template name | Persistent exclusive volume mounted at `/var/lib/buildkit`. |
| `--cache-size <MiB>` | 16384 | Size when creating a new cache volume. |
| `--no-cache` | False | Disable instruction cache; cache mounts retain their usual BuildKit semantics. |
| `--builder-cpu` / `--builder-memory` | 2 / 2048 MiB | Builder resources, independent of template resources. |
| `--builder-image <ref>` | `docker.io/moby/buildkit:v0.33.0` | Image containing `buildkitd`, `buildctl`, and its OCI runtime. |
| `--buildctl <path>` | `buildctl` | Local client executable. |
| `--progress <format>` | `auto` | Three-stage bar on terminals, plain logs when redirected. `plain` selects plain logs; `tty` selects BuildKit's native display. |
| `--timeout <seconds>` | 3600 | Dockerfile build deadline; the CLI allows 10 additional minutes for provisioning and publication. |
| `--start-cmd` / `--ready-cmd` | Image command / default readiness | Override startup/readiness; an empty start command disables startup. |

BuildKit handles multi-stage builds, `COPY`, `ADD`, `.dockerignore`, cache mounts,
and Dockerfile syntax. The CLI remains connected during the build, streams build
progress, and exits nonzero on failure. The server provisions and releases an
internal worker for each template build. The CLI uses only template and build
IDs; workers are absent from public sandbox listings and endpoints. Cancellation
and deadlines release the worker while retaining its cache. A hard client failure
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

Caches survive builder replacement. Use the same `--cache-volume` when building
successive template names. Template names remain unique, as in the existing
template API. Concurrent builds need separate exclusive volumes. Remove unused
caches with `aenv volume delete <name>`; BuildKit manages contents within each
cache. Registry credentials come from the local BuildKit session and normal
Docker credential configuration.

The additive API is template-scoped:

1. `POST /templates/builds` accepts `template` plus optional builder resources,
   cache settings, `timeout`, `startCmd`, and `readyCmd`. It returns template/build IDs.
2. Poll the existing `GET /templates/{templateID}/builds/{buildID}/status` endpoint:
   `waiting` means the worker is preparing; `building` means it can accept a build.
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

Image `ENTRYPOINT` and `CMD` are combined for startup through `/bin/sh -c`.
Images must satisfy AgentENV's normal guest runtime requirements; OCI
`HEALTHCHECK`, `EXPOSE`, and `VOLUME` are metadata, not Docker runtime services.

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
