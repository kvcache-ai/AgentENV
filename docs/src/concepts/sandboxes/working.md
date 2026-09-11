# Working with Sandboxes

This page provides the basic commands for interacting with and managing a sandbox.

## Connect to a Sandbox

`aenv connect` opens an interactive shell inside the sandbox and attaches your terminal. `aenv cn` is its short alias:

```bash
aenv connect <sandbox-id>
aenv cn <sandbox-id>
```

If a sandbox is paused, `aenv connect` will automatically resume it.

## Execute a Command

`aenv exec` runs one non-interactive command, streams its 
output to your local terminal, and exits with the remote 
command's exit code. It does not attach an interactive shell.
Flags intended for the remote command that collide with aenv's 
own flags can be escaped with a leading `--`.

```bash
aenv exec <sandbox-id> ls -la /
aenv exec <sandbox-id> -- command-with-aenv-like-flags --timeout 10
```

## Upload Files

`aenv upload` copies a local file or directory into a running sandbox:

Usage:

```bash
aenv upload <sandbox-id> <local-path> <remote-path> [options]
```

Example:

```bash
aenv upload 018f0d93-aaaa-bbbb-cccc-0123456789ab ./config.json /workspace/config.json
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the destination sandbox. |
| `<local-path>` | Required | Local file or directory to upload. |
| `<remote-path>` | Required | Destination inside the sandbox. Directory paths must be absolute. |
| `--user <user>` | None | Resolves a relative remote file path from this user's home directory and sets the uploaded file's owner. It is not supported for directory uploads. |

## Download Files

`aenv download` copies a file or directory from a running sandbox to your local machine:

Usage:

```bash
aenv download <sandbox-id> <remote-path> [local-path] [options]
```

Example:

```bash
aenv download 018f0d93-aaaa-bbbb-cccc-0123456789ab /workspace/result.txt ./result.txt
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the sandbox to download from. |
| `<remote-path>` | Required | File or directory inside the sandbox. Directory paths must be absolute. |
| `[local-path]` | Current directory | Local destination file or directory. |
| `--user <user>` | None | Resolves a relative remote file path from this user's home directory. It is not supported for directory downloads. |
| `--force` | Disabled | Replaces conflicting local files. Without it, the download stops instead of overwriting them. |

## Pause and Resume

Pausing saves the sandbox's current runtime state and stops its 
microVM. While it is paused, programs inside it do not run, services do not handle requests,
and the sandbox releases its CPU and memory resources. Its saved state remains
in storage so the same sandbox can be resumed later.

After resume, the filesystem, running processes, environment variables, and
in-memory data are restored to the state captured at pause time. Programs
continue from that saved state instead of starting again from the beginning.

```bash
aenv pause <sandbox-id>
aenv resume <sandbox-id>
aenv resume <sandbox-id> --timeout 600
```

`aenv resume` accepts `--timeout <seconds>`, which defaults to 300 seconds and
sets the new TTL from resume time.

By default, the sandbox automatically pauses when it reaches its TTL. See
[Auto-Eviction](./auto-eviction.md) for how the deadline is set and how to delete
instead of pause.

## Persistent Snapshots

A snapshot is a durable, reusable checkpoint of a running sandbox. Creating one
does not replace the sandbox: the source returns to Running after capture, and
the snapshot can later launch one or more new sandboxes.

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-base
```

The resulting snapshot appears in `aenv snapshot list` and can be started with
`aenv start <snapshot-id-or-name>`. See [Snapshots](../snapshots/index.md) for its
parameters and lifecycle.

## Fork

Forking clones a running sandbox into independent child sandboxes on the same
node. The source is briefly paused while its state is captured, then returns to
Running. Children inherit the source filesystem, memory, network policy,
security mode, and CPU/memory/disk configuration. All children use one captured
state, but each child can succeed or fail independently.

```bash
curl -X POST \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{"count": 3, "timeout": 600}' \
  http://127.0.0.1:8000/sandboxes/<sandbox-id>/fork
```

| Field | Default | Description |
|---|---|---|
| `count` | `1` | Number of children to create; minimum 1, maximum 100. |
| `timeout` | Source sandbox's TTL duration | TTL for each child, measured from the fork time. |

A successful request returns an array with one result for each requested child.
Each entry contains either a `sandbox` object—including its `sandboxID`—or an
`error` explaining why that individual child failed. It is not a plain list of
IDs. A non-201 response means the request failed before any child was attempted.
See the [API Reference](../../api/index.md) for the complete fork request and
response schemas.

## Manage Sandboxes

List sandboxes:

```bash
aenv list  # alias: aenv ls
aenv list --output json
```

`--output` accepts `table` or `json`. It
defaults to a table in an interactive terminal and JSON when output is piped or
redirected.

Delete a sandbox:

```bash
aenv delete <sandbox-id>  # alias: aenv rm
```

Deletion is permanent, but snapshots
previously created from the sandbox are unaffected.
