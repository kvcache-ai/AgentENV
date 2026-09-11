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
| `--ready-cmd <cmd>` | Shell command polled until it exits 0. Defaults to `sleep 20` when `--start-cmd` is set; otherwise unset. |
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
