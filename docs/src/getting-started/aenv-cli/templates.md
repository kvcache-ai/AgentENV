# Templates

`aenv templates` is an alias for `aenv template`, including its `list`,
`watch`, and `delete` subcommands.

## `aenv pull <image>`

Create a template from an OCI image. Waits for the build to complete by default.

```bash
aenv pull ubuntu:22.04
aenv pull ubuntu:22.04 --name my-ubuntu
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Override the template name. Defaults to the image's repository segment. |
| `--cpu <count>` | CPU cores for the template. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory for the template. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `--start-cmd <cmd>` | Shell command to run inside the sandbox before capturing the template snapshot |
| `--ready-cmd <cmd>` | Shell command polled until it exits 0. Defaults to `/agentenv/bin/busybox sleep 20` when `--start-cmd` is set; otherwise unset. |
| `--probe <PORT>` | Wait until `localhost:<PORT>` accepts TCP connections. Conflicts with `--ready-cmd`. |
| `-d, --detach` | Submit the build and return immediately without waiting |
| `--timeout <SECS>` | Maximum seconds to wait for the build to complete. No timeout by default. Conflicts with `--detach`. |

## `aenv build <context> --name <name>`

Create a template from a local Dockerfile using BuildKit in an isolated
microVM. The command waits until the template is ready.

```bash
aenv build . --name my-app
aenv build . -f deploy/docker/Dockerfile.agentenv --name aenv
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Required template name. |
| `--cpu <count>` | CPU cores for the template. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory for the template. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `-f, --file <path>` | Dockerfile path. Defaults to `<context>/Dockerfile`; explicit relative paths resolve from the current directory. |
| `--start-cmd <command>` | Override the image `ENTRYPOINT`/`CMD`; an empty string disables startup. |
| `--ready-cmd <command>` | Override the image `HEALTHCHECK` with a command that must succeed before capture. |
| `--build-arg KEY=VALUE` | Build argument; repeatable. |
| `--secret <spec>` | BuildKit secret mount; repeatable. |
| `--no-cache` | Rebuild without cached instructions or their cache mounts. |
| `--buildctl <path>` | Select the local BuildKit client executable. |
| `--progress <auto\|plain\|tty>` | Build progress format. Defaults to `auto`. |
| `--timeout <seconds>` | Build deadline, from 1 to 86400 seconds. Defaults to 3600; the CLI allows 10 additional minutes for provisioning and publication. |

## `aenv build --compose <path>`

Build the active services' Dockerfiles with the remote BuildKit builder and push
their OCI images to a registry. This mode does not start the service containers
or create runnable VM templates. Services that already specify only `image` are
retained. Start the resulting project separately:

```bash
aenv build --compose compose.yaml \
  --image-repository registry.example.com/team/images --output compose.built.yaml
aenv compose up -f compose.built.yaml
```

The repository must be reachable from both the builder VM and the runtime node.
Each build uses unique tags in that repository; existing service `image` values
are replaced for services with `build`. BuildKit uses the CLI user's Docker
registry credentials for push. Runtime nodes need their own pull credentials.
For private registry addresses, the node's `network.egress.always_denied_cidrs`
must permit builder access. `--registry-insecure` changes TLS handling only;
it does not bypass the node's network policy.
The CLI installer bundles `aenv-buildctl` and `aenv-compose-plan`; a local Docker
daemon is not required. The server must support image-only builder sessions.

| Flag | Description |
|------|-------------|
| `--compose <path>` | Local Compose YAML or JSON file. |
| `--image-repository <registry/repository>` | Required destination, without a tag or digest. |
| `--output <path>` | Output file; defaults to `compose.built.yaml` beside the input. Must not already exist. |
| `--env KEY=VALUE` | Explicit interpolation/build argument environment; repeatable, last value wins. Process environment and `.env` files are not loaded. |
| `--profile <name>` | Select optional services; repeatable. Selected profiles are resolved into the output. |
| `--harbor` | Apply Harbor's main-service build and keepalive defaults before normalizing a task's Compose override file. Explicit task settings take precedence. |
| `--registry-insecure` | Permit HTTP or untrusted TLS for pushes to a development registry. Runtime registry access must be configured separately. |
| `--compose-planner <path>` | Override the bundled Compose planner executable. |

Shared build flags `--no-cache`, `--buildctl`, `--progress`, and `--timeout` also
apply (`--timeout` is per service). Put per-service build arguments in the Compose
file. Supported build fields are `context`, `dockerfile`, `args`, `target`, and
`no_cache`, including the `build: ./directory` shorthand. Contexts must be local
directories; relative contexts resolve beside the Compose file and Dockerfiles
resolve relative to their build context. Images target `linux/amd64`.

The input file is unchanged. The output is JSON, which Compose accepts as YAML,
and contains no `build` or `pull_policy: build` entries. It is written only after
every service succeeds; partial failures may leave successfully pushed images in
the repository. Remove those using your registry's retention policy. Unsupported
Compose features and missing build files are rejected before allocating builders.

For Terminal-Bench 4.0 tasks, use the original environment directory:

```bash
aenv build --compose tasks/freight-dispatch-shift/environment/docker-compose.yaml \
  --harbor --image-repository registry.example.com/team/tb4 --output freight.built.yaml
aenv compose up -f freight.built.yaml --cpu 4 --memory 8192
```

`--harbor` supplies `main.build.context: .` when neither image nor build is set,
and `main.command: [sh, -c, sleep infinity]` when command is omitted, matching
Harbor's build base configuration. It prepares the task environment; it does not
run Harbor agents, inject verifier files, or grade benchmark solutions.

## `aenv template list`

List all templates. Alias: `aenv template ls`, `aenv templates list`.

```bash
aenv template list
aenv template list --output json
```

| Flag | Description |
|------|-------------|
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

## `aenv template watch <template>`

Watch a template build until it succeeds or fails. Accepts either a template name/alias or a template UUID.

```bash
aenv template watch my-ubuntu
aenv template watch <template-id>
```

## `aenv template delete <template>`

Delete a template by name or ID. Alias: `aenv template rm`.

```bash
aenv template delete my-ubuntu
aenv template delete <template-id>
```
