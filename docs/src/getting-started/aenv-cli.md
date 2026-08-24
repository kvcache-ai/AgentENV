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
# API key: <the server's configured or generated API key>
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
| `--name <name>` | Override the template name. Defaults to the image's repository segment. |
| `--cpu <count>` | CPU cores for the template. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory for the template. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `--start-cmd <cmd>` | Shell command to run inside the sandbox before capturing the template snapshot |
| `--ready-cmd <cmd>` | Shell command polled until it exits 0. Defaults to `sleep 20` when `--start-cmd` is set; otherwise unset. |
| `--probe <PORT>` | Wait until `localhost:<PORT>` accepts TCP connections. Conflicts with `--ready-cmd`. |
| `-d, --detach` | Submit the build and return immediately without waiting |
| `--timeout <SECS>` | Maximum seconds to wait for the build to complete. No timeout by default. Conflicts with `--detach`. |

### `aenv build <dockerfile> --name <name>`

Create a template from a local Dockerfile.

```bash
aenv build ./Dockerfile --name my-app
aenv build ./Dockerfile --name my-app --image ghcr.io/myorg/base:latest
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Required template name |
| `--cpu <count>` | CPU cores for the template. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory for the template. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `--image <image>` | Override the rootfs base. Defaults to the first concrete `FROM` image, then the server's `[image.resolver].default_image` if none is usable. Alias: `--user-image`. |

`aenv build` submits the build and returns immediately. Use
`aenv template watch <template>` to wait for completion.

### `aenv template list`

List all templates. Alias: `aenv template ls`, `aenv templates list`.

```bash
aenv template list
aenv template list --output json
```

| Flag | Description |
|------|-------------|
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

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

Start a sandbox and attach an interactive shell. `<target>` accepts a template
or snapshot ID or alias, or an OCI image reference with `--cold`.

```bash
aenv start my-ubuntu
aenv start --secure my-ubuntu               # require token-authenticated envd access
aenv start --cold ubuntu:24.04              # start directly from an OCI image
```

| Flag | Description |
|------|-------------|
| `--cold` | Start directly from an external OCI image instead of a template |
| `--secure` | Require token authentication for envd control communication |
| `--timeout <secs>` | Sandbox TTL in seconds (default: 300) |
| `--cpu <count>` | CPU cores; only valid with `--cold`. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory in MiB; only valid with `--cold`. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Root filesystem size; only valid with `--cold`. Defaults to the source image's virtual size; an explicit value must be at least 1024 and divisible by 1024 MiB. Alias: `--disk-mb`. |
| `-d, --detach` | Print the sandbox ID and exit without attaching a shell |

CPU, memory, and disk overrides are supported only for cold starts.

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
| `--timeout <secs>` | TTL in seconds from now (default: 300). Must be longer than the sandbox's current remaining TTL. |

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

### `aenv upload <sandbox-id> <local-path> <remote-path>`

Upload a local file or directory to a sandbox through envd. Files are streamed
individually, and missing remote directories are created automatically.

```bash
aenv upload <sandbox-id> ./config.json /workspace/config.json
aenv upload <sandbox-id> ./config.json /workspace/
aenv upload <sandbox-id> ./project /workspace/
aenv upload <sandbox-id> ./project /workspace/app
aenv upload --user app <sandbox-id> ./config.json config.json
```

| Flag | Description |
|------|-------------|
| `--user <user>` | Resolve relative remote file paths as this user and set the uploaded file's owner |

For directory uploads, the remote path must be absolute and `--user` is not
supported. If the remote destination ends in `/` or already exists as a
directory, the local directory name is appended. Otherwise the destination is
used as the new directory root. Hidden files and empty directories are copied;
symbolic links and special files are rejected.

Upload copies file contents and directory structure only. It does **not**
preserve host ownership or group, permissions (including executable bits),
timestamps, ACLs, extended attributes, or hard-link relationships. Destination
metadata is assigned by envd and the sandbox filesystem.

### `aenv download <sandbox-id> <remote-path> [local-path]`

Download a file or directory from a sandbox through envd.

```bash
aenv download <sandbox-id> /workspace/result.txt ./result.txt
aenv download <sandbox-id> /workspace/result.txt
aenv download <sandbox-id> /workspace/result.txt ./output/
aenv download <sandbox-id> /workspace/project ./backup/
aenv download --user app --force <sandbox-id> result.txt ./result.txt
```

| Flag | Description |
|------|-------------|
| `--user <user>` | Resolve relative remote file paths from this user's home directory |
| `--force` | Replace conflicting local files |

When the local path is omitted, the remote name is used in the current
directory. When the local path names an existing directory or ends in `/`, the
remote name is appended automatically. The resulting local parent directory
must already exist. Directory downloads require an absolute remote path and do
not support `--user`. Existing directories are merged; unrelated files remain,
while conflicting files require `--force`. Each file is written through a
temporary file and moved into place only after that file succeeds. Symbolic
links and special files are rejected. Downloads do **not** preserve remote
ownership or group, permissions (including executable bits), timestamps, ACLs,
extended attributes, or hard-link relationships.

### `aenv list`

List all sandboxes. Alias: `aenv ls`.

```bash
aenv list
```

| Flag | Description |
|------|-------------|
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

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
| `--name <name>` | Snapshot name or alias. If omitted, the generated snapshot ID identifies the snapshot. |

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
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

The table output includes an `IMAGE REF` column (`-` when no image was published); JSON output includes the optional `imageRef` field.

To delete a snapshot, use `aenv template delete <snapshot-id>` or `aenv template delete <name>` — snapshots share the same underlying store as templates and are deleted through the same command.
