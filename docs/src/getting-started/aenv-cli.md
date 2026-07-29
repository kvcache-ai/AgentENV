# aenv CLI Reference

`aenv` is the native CLI for AgentENV. It wraps the HTTP API and envd gRPC endpoints into a developer-friendly interface for managing templates, sandboxes, and snapshots.

## Installation

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install-cli.sh | bash
```

Or build from source (requires Rust):

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
make install-aenv
```

---

## Authentication

### `aenv auth`

Save the server URL and API key. Credentials are stored at `~/.config/aenv/credentials` (mode `0600`).

```bash
aenv auth
# AENV server URL [http://localhost:8000]: The address of the AgentENV server
# API key: dummy (Any non-empty string works for local development.)
```

---

## Templates

### `aenv pull <image>`

Create a template from an OCI image. Waits for the build to complete by default.

```bash
aenv pull ubuntu:22.04
aenv pull ubuntu:22.04 --name my-ubuntu
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Override the template name |
| `--start-cmd <cmd>` | Shell command to run inside the sandbox before capturing the template snapshot |
| `--ready-cmd <cmd>` | Shell command used to wait until the sandbox is ready (polled until it exits 0) |
| `--probe <PORT>` | Wait until `localhost:<PORT>` accepts TCP connections |
| `-d, --detach` | Submit the build and return immediately without waiting |
| `--timeout <SECS>` | Maximum seconds to wait for the build to complete |

### `aenv build <dockerfile>`

Create a template from a local Dockerfile.

```bash
aenv build ./Dockerfile
aenv build ./Dockerfile -t my-app
aenv build ./Dockerfile --image ghcr.io/myorg/base:latest
```

| Flag | Description |
|------|-------------|
| `-t, --tag <name>` | Template name. Defaults to the parent directory name |
| `--image <image>` | Override the `FROM` image used as the rootfs base |

### `aenv template list`

List all templates. Alias: `aenv template ls`, `aenv templates list`.

```bash
aenv template list
aenv template list --output json
```

### `aenv template watch <template>`

Watch a template build until it succeeds or fails. Accepts either a template name/alias or a template UUID.

```bash
aenv template watch my-ubuntu
aenv template watch <template-id>
```

### `aenv template delete <template>`

Delete a template by name or ID. Alias: `aenv template rm`.

```bash
aenv template delete my-ubuntu
aenv template delete <template-id>
```

---

## Sandboxes

### `aenv start <target>`

Start a sandbox and attach an interactive shell. `<target>` is a template name or template UUID.

```bash
aenv start my-ubuntu
aenv start --cold ubuntu:24.04              # start directly from an OCI image
```

| Flag | Description |
|------|-------------|
| `--cold` | Start directly from an external OCI image instead of a template |
| `--timeout <secs>` | Sandbox TTL in seconds (default: 300) |
| `--cpu-count <n>` / `--cpu` | CPU cores — only valid with `--cold` |
| `--memory-mb <n>` / `--mem` | Memory in MiB — only valid with `--cold` |
| `-d, --detach` | Print the sandbox ID and exit without attaching a shell |

`<target>` accepts a template UUID, template name, or (with `--cold`) an OCI image reference.

### `aenv pause <sandbox-id>`

Pause a running sandbox. The sandbox state is preserved and can be resumed later.

```bash
aenv pause <sandbox-id>
```

### `aenv resume <sandbox-id>`

Resume a paused sandbox.

```bash
aenv resume <sandbox-id>
```

| Flag | Description |
|------|-------------|
| `--timeout <secs>` | TTL in seconds from now (default: 300) |

### `aenv timeout <sandbox-id> <seconds>`

Set or extend the sandbox expiration to `<seconds>` from now.

```bash
aenv timeout <sandbox-id> 600
```

### `aenv connect <sandbox-id>`

Attach an interactive shell to a running or paused sandbox. Alias: `aenv cn`.

```bash
aenv connect <sandbox-id>
```

Resumes the sandbox if paused before attaching.

### `aenv exec <sandbox-id> <command> [args...]`

Run a one-shot command in a sandbox and stream its output.

```bash
aenv exec <sandbox-id> ls -la /
```

### `aenv list`

List all sandboxes. Alias: `aenv ls`.

```bash
aenv list
```

Outputs a table on a TTY and JSON when piped. Override with `--output table|json`.

### `aenv delete <sandbox-id>`

Kill and delete a sandbox. Alias: `aenv rm`.

```bash
aenv delete <sandbox-id>
aenv rm <sandbox-id>
```

---

## Snapshots

### `aenv snapshot create <sandbox-id>`

Capture a persistent snapshot from a running sandbox. The snapshot can be used as a template to start new sandboxes with `aenv start`.

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-base
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Snapshot name or alias |

When source-registry image publication is enabled on the server, the command also prints the published OverlayBD-native image reference on an `Image:` line; that tag can be used directly as a `userImage`.

### `aenv snapshot list`

List persistent snapshots. Alias: `aenv snapshot ls`, `aenv snap ls`.

```bash
aenv snapshot list
aenv snapshot list --sandbox-id <sandbox-id>
```

| Flag | Description |
|------|-------------|
| `--sandbox-id <id>` | Filter snapshots by source sandbox ID |
| `--output <format>` | Output format: `table` (default on TTY) or `json` |

The table output includes an `IMAGE REF` column (`-` when no image was published); JSON output includes the optional `imageRef` field.

To delete a snapshot, use `aenv template delete <snapshot-id>` or `aenv template delete <name>` — snapshots share the same underlying store as templates and are deleted through the same command.