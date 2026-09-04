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

> ⚠️ **Experimental** — Not recommended for production use.

Build a template by running Dockerfile instructions inside a temporary sandbox:

```bash
aenv build <dockerfile> --name <name> [options]
```

| Argument or option | Default | Description |
|---|---|---|
| `<dockerfile>` | Required | Path to the Dockerfile. |
| `--name <name>` | Required | Assign the template name. |
| `--image <ref>` | First concrete `FROM` image | Override the base image. If the Dockerfile has no usable `FROM`, `[image.resolver].default_image` from your config file is used. Alias: `--user-image`. |
| `--cpu <count>` | `[machine].vcpu_count` from your config file | Set the template's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set the template's memory. Aliases: `--memory-mb`, `--mem`. |

`aenv build` and `aenv pull --detach` submit the build and return immediately. Watch it until it
succeeds or fails by passing the template name or ID:

```bash
aenv template watch my-template
```

`aenv template watch` has no timeout option. Stop the
local watch with `Ctrl-C`; the remote build continues.

Supported Dockerfile instructions:

| Instruction | Behavior |
|-------------|----------|
| `FROM` | Base image (overridable with `--image`) |
| `RUN` | Shell command executed inside the build sandbox |
| `ENV` | Set an environment variable |
| `ARG` | Set a build-time variable |
| `WORKDIR` | Create the directory if needed and set it as the working directory |
| `USER` | Set the default user (a missing named account is created at the end of the build) |
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
