# Templates

A template is the user-facing wrapper around a committed snapshot. Instead of
booting a fresh VM and installing software every time, you build a template
once and later create sandboxes from it in milliseconds.

## How Templates Work

1. **Define**: specify an overlaybd-backed base rootfs and ordered steps such as `run`, `env`, and `workdir`.
2. **Build**: AgentENV boots a temporary sandbox and executes those steps inside it.
3. **Finalize**: optional startup commands can be started and checked for readiness before the snapshot is captured.
4. **Publish**: the result is committed as a snapshot in the snapshot repository.
5. **Launch**: creating a sandbox from a template resolves the committed snapshot and resumes from it.

## Create Your Template

There are two ways to create a template: `aenv pull` imports an OCI image directly, and `aenv build` runs Dockerfile instructions inside a temporary build sandbox.

### aenv pull

Pulling an existing OCI image as a template:

```bash
aenv pull ubuntu:24.04
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Template name. Defaults to the repository segment of the image |
| `--start-cmd <cmd>` | Command to run before capturing the snapshot |
| `--ready-cmd <cmd>` | Shell command polled every 2 s until exit 0; gates snapshot capture |
| `--probe <port>` | Readiness check: wait for TCP on `localhost:<port>` |
| `-d`, `--detach` | Submit the build and return without waiting |
| `--timeout <secs>` | Maximum time to wait for the build to complete |

`Env`, `WorkingDir`, and `User` are automatically inherited from the OCI image config. See [Runtime Configuration](#runtime-configuration) for the full field list.

### aenv build

> ⚠️ **Experimental** — Not recommended for production use.

Build a template by running Dockerfile instructions inside a temporary sandbox:

```bash
aenv build ./Dockerfile
```

| Flag | Description |
|------|-------------|
| `-t`, `--tag <name>` | Template name. Defaults to the parent directory name of the Dockerfile |
| `--image <ref>` | Override the `FROM` image |

Supported Dockerfile instructions:

| Instruction | Behavior |
|-------------|----------|
| `FROM` | Base image (overridable with `--image`) |
| `RUN` | Shell command executed inside the build sandbox |
| `ENV` | Set an environment variable |
| `ARG` | Set a build-time variable |
| `WORKDIR` | Create the directory if needed and set it as the working directory |
| `USER` | Set the default user |
| `ENTRYPOINT` | Becomes the template `startCmd` |
| `CMD` | Becomes `startCmd` if no `ENTRYPOINT` is present |
| `EXPOSE` / `VOLUME` / `LABEL` | Accepted but stored as metadata only |

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

### Aliases

Each template is identified by a UUID. Pass `--name` or `-t` at creation time
to assign a human-readable alias:

```bash
aenv pull ubuntu:24.04 --name my-base
aenv build ./Dockerfile -t my-service
```

The alias can be used wherever a template ID is accepted:

```bash
aenv start my-base
aenv template delete my-service
```

## Manage Templates

### List templates

```bash
aenv template list        # alias: aenv template ls
```

Displays all templates with their ID, name, build status, CPU, memory, disk size, and last-updated timestamp.

### Delete a template

```bash
aenv template delete <template-id-or-name>   # alias: aenv template rm
```

### Watch a build

`aenv build` always detaches immediately after submitting. Use `watch` to follow the build status until it succeeds or fails:

```bash
aenv template watch <template-id-or-name>
```

### Start a sandbox from a template

```bash
aenv start <template-id-or-name>      # start and attach an interactive shell
aenv start -d <template-id-or-name>   # detach: print sandbox ID and exit
```

## Relationship to Snapshots

Templates are the API and UX layer. Snapshots are the durable runtime layer.

- A template build publishes one committed snapshot.
- A template ID or alias resolves to one committed snapshot.
- A sandbox created from a template resumes from that snapshot.

If you want the storage and runtime model underneath templates, see
[Snapshots](./snapshots.md).
