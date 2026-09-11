# Create Your Template

There are two ways to create a template: `aenv pull` imports an existing OCI image directly, while `aenv build` runs a Dockerfile with
BuildKit.

Some defaults below come from `config/default.toml`, or the file selected by
`AENV_CONFIG_PATH`.

## aenv pull

Pull an existing OCI image as a template and optionally give it a memorable name:

```bash
aenv pull <image> [options]
```

Examples:

```bash
aenv pull ubuntu:24.04
aenv pull ubuntu:24.04 --name my-base
```

`--name` is optional. Without it, AgentENV uses the image repository name. A
template name can be used anywhere a template ID is accepted.

| Argument or option | Default | Description |
|---|---|---|
| `<image>` | Required | OCI image reference. Short names such as `ubuntu:24.04` and full references are supported. |
| `--name <name>` | Image repository name | Assign a human-readable template name. |
| `--cpu <count>` | `[machine].vcpu_count` from your config file | Set the template's vCPU count. Alias: `--cpu-count`. |
| `--memory <MiB>` | `[machine].mem_size_mib` from your config file | Set the template's memory. Aliases: `--memory-mb`, `--mem`. |
| `--start-cmd <cmd>` | None | Run a command before capturing the template snapshot. |
| `--ready-cmd <cmd>` | `sleep 20` when `--start-cmd` is set; otherwise none | Poll a shell command every two seconds until it exits successfully. |
| `--probe <port>` | None | Wait for TCP on `localhost:<port>`. Cannot be combined with `--ready-cmd`. |
| `-d, --detach` | Off | Submit the build and return immediately instead of waiting. |
| `--timeout <seconds>` | No timeout | Limit how long the CLI waits for the build. Cannot be combined with `--detach`. |

When using `--detach`, monitor the build with `aenv template watch <template>`.

## aenv build

Build a Dockerfile with BuildKit inside a temporary microVM, then convert the
result to OverlayBD and capture a template. Docker and a staging registry are
not required on the CLI machine.

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
| `--no-cache` | False | Rebuild without cached instructions. BuildKit also resets cache mounts used by those instructions. |
| `--buildctl <path>` | `aenv-buildctl` beside `aenv` | Local client executable. |
| `--progress <format>` | `auto` | Three-stage bar on terminals, plain logs when redirected. `plain` selects plain logs; `tty` selects BuildKit's native display. |
| `--timeout <seconds>` | 3600 | Builder preparation and Dockerfile build deadline; the CLI allows 10 additional minutes for publication. |

### Build Context and Dockerfile

`COPY` and `ADD` resolve from the context directory, independently of the
Dockerfile's location. Local directories are supported; URL and stdin contexts
are not supported. BuildKit supports multi-stage builds, `.dockerignore`, cache
mounts, and standard Dockerfile syntax. The final Dockerfile stage is always
published.

There are no image, stage-selection, SSH, or builder-resource overrides in the
build CLI. Select base images with `FROM`, including an `ARG` used by `FROM`.

### Startup and Readiness

By default, image `ENTRYPOINT` and `CMD` determine template startup, and a
Dockerfile `HEALTHCHECK` supplies the readiness command before snapshot capture.
Shell health checks honor Dockerfile `SHELL`, and `HEALTHCHECK NONE` disables
the check. Without a health check, startup uses the normal template readiness
delay.

`--start-cmd` and `--ready-cmd` override startup and readiness independently
without modifying the image configuration. Publishing boots the image and runs
the selected startup command. An image can therefore compile successfully but
fail during publication if its entrypoint requires devices that are unavailable
in the guest. To capture the image without starting its application or running
its health check, use:

```bash
aenv build <context> --name <name> --start-cmd "" --ready-cmd true
```

### Build Capacity

Managed builder settings belong to the server configuration:

```toml
[template_build]
max_concurrent_builds = 4
builder_image = "docker.io/moby/buildkit:v0.33.0"
builder_cpu_count = 16
builder_memory_mb = 32768
cache_size_mb = 65536
```

Each node admits at most `max_concurrent_builds` managed builds, including
preparation, image publication, and cleanup. When the per-node limit is reached,
builder preparation returns HTTP 429. Retry the build when capacity is
available.

`cache_size_mb` controls the persistent BuildKit cache volume. Its capacity is
set when the cache is created, so changing the setting does not resize an
existing cache. It must be at least 1024 MiB and cannot exceed
`volume.max_size_mb`. A Dockerfile build is rejected if the configured cache
capacity exceeds that volume limit; setting a smaller volume limit does not
prevent the server from starting or serving other APIs. Builder CPU, memory,
and cache capacity are independent of the CPU and memory assigned to the
resulting template.

### Monitor a Build

`aenv build` remains connected, streams progress, and exits nonzero on failure.
Once BuildKit succeeds, AgentENV continues importing and publishing the template
even if the CLI is interrupted. Check its status by template name or ID:

```bash
aenv template watch my-template
```

`aenv template watch` has no timeout option. Stop the local watch with `Ctrl-C`;
the remote build continues. It reports the following statuses:

| Status | Meaning |
| --- | --- |
| `waiting` | The build has been submitted and is waiting to start. |
| `building` | The template is currently being built or published. |
| `ready` | The build succeeded and the template can start sandboxes. |
| `error` | The build failed. `aenv template watch` reports the failure reason when available. |

## Runtime Configuration

Image configuration affects every sandbox created from the template. `aenv pull`
retains the source image configuration, while `aenv build` produces it from the
final Dockerfile stage. The table below distinguishes fields that affect the
sandbox runtime automatically from fields that require explicit CLI options.

| Image config field | Dockerfile instruction | Runtime effect |
|-----------|------------------------|----------------|
| `Env` | `ENV` | Environment variables available to sandbox processes. |
| `WorkingDir` | `WORKDIR` | Default working directory. |
| `User` | `USER` | Default user. |
| `Entrypoint` / `Cmd` | `ENTRYPOINT` / `CMD` | Determines startup for `aenv build`; `aenv pull` requires `--start-cmd` to configure startup explicitly. |
| `Healthcheck` | `HEALTHCHECK` | Determines readiness for `aenv build`; `--ready-cmd` overrides it. |

`EXPOSE`, `VOLUME`, and `LABEL` are retained as image metadata but do not create
ports, mount volumes, or otherwise configure the sandbox runtime.
