# Sandboxes

## `aenv start <target>`

Start a sandbox and attach an interactive shell. `<target>` accepts a template
or snapshot ID or alias, or an OCI image reference with `--cold`.

```bash
aenv start my-ubuntu
aenv start --cold ubuntu:24.04              # start directly from an OCI image
```

Sandboxes started by `aenv` always require token-authenticated envd access. The
CLI obtains and manages the access token automatically.

| Flag | Description |
|------|-------------|
| `--cold` | Start directly from an external OCI image instead of a template or snapshot |
| `--timeout <secs>` | Sandbox TTL in seconds (default: 300) |
| `--cpu <count>` | CPU cores; only valid with `--cold`. Defaults to `[machine].vcpu_count` on the server. Alias: `--cpu-count`. |
| `--memory <MiB>` | Memory in MiB; only valid with `--cold`. Defaults to `[machine].mem_size_mib` on the server. Aliases: `--memory-mb`, `--mem`. |
| `--disk-size-mb <MiB>` | Root filesystem size; only valid with `--cold`. Defaults to the source image's virtual size; an explicit value must be at least 1024 and divisible by 1024 MiB. Alias: `--disk-mb`. |
| `--volume <MOUNT_PATH=VOLUME>` | Mount a persistent volume by ID or name. Available for warm and cold starts; repeatable. |
| `-d, --detach` | Print the sandbox ID and exit without attaching a shell |

CPU, memory, and disk overrides are supported only for cold starts.

## `aenv pause <sandbox-id>`

Pause a running sandbox. The sandbox state is preserved and can be resumed later.

```bash
aenv pause <sandbox-id>
```

## `aenv resume <sandbox-id>`

Resume a paused sandbox.

```bash
aenv resume <sandbox-id>
```

| Flag | Description |
|------|-------------|
| `--timeout <secs>` | TTL in seconds from now (default: 300). Must be longer than the sandbox's current remaining TTL. |

## `aenv timeout <sandbox-id> <seconds>`

Set or extend the sandbox expiration to `<seconds>` from now.

```bash
aenv timeout <sandbox-id> 600
```

## `aenv connect <sandbox-id>`

Attach an interactive shell to a running or paused sandbox. Alias: `aenv cn`.

```bash
aenv connect <sandbox-id>
```

Resumes the sandbox if paused before attaching.

## `aenv exec <sandbox-id> <command> [args...]`

Run a one-shot command in a sandbox and stream its output.

Use a leading `--` when flags for the remote command could be interpreted as
`aenv` flags.

```bash
aenv exec <sandbox-id> ls -la /
aenv exec <sandbox-id> -- command-with-aenv-like-flags --timeout 10
```

## `aenv upload <sandbox-id> <local-path> <remote-path>`

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

## `aenv download <sandbox-id> <remote-path> [local-path]`

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

## `aenv list`

List all sandboxes. Alias: `aenv ls`.

```bash
aenv list
```

| Flag | Description |
|------|-------------|
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

## `aenv delete <sandbox-id>`

Kill and delete a sandbox. Alias: `aenv rm`.

```bash
aenv delete <sandbox-id>
aenv rm <sandbox-id>
```
