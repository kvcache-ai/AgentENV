# Volumes

## `aenv volume create <name>`

Create an independent persistent volume. Volumes default to 65536 MiB (64 GiB)
and `exclusive` mode.

```bash
aenv volume create workspace
aenv volume create models --mode ro --image ghcr.io/example/models:latest
aenv volume create job-workspace --from-volume workspace
```

| Flag | Description |
|------|-------------|
| `--size-mb <MiB>` | Volume size (default: 65536). A copy-on-write fork must use the same size as its source. |
| `--mode <exclusive\|ro>` | Access mode (default: `exclusive`). Exclusive volumes are writable by one sandbox; read-only volumes can be shared. |
| `--from-volume <volume>` | Create a copy-on-write fork from an existing volume ID or name. Conflicts with `--image`. |
| `--image <image>` | Initialize the volume from an OCI image. Conflicts with `--from-volume`. |

We recommend creating an exclusive fork for each sandbox instead of mounting a
shared writable volume directly:

```bash
aenv volume create job-data --mode exclusive --from-volume dataset-base
aenv start ubuntu --volume /workspace/data=job-data
```

See [Volumes](../../concepts/volumes/index.md) for access-mode semantics, lifecycle
behavior, automatic sandbox fork and snapshot handling, and complete CLI
examples.

## `aenv volume list`

List persistent volumes. Alias: `aenv volume ls`.

```bash
aenv volume list
aenv volume list --output json
```

| Flag | Description |
|------|-------------|
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

## `aenv volume inspect <volume>`

Inspect a volume by ID or name. The command prints the complete volume record
as formatted JSON.

```bash
aenv volume inspect job-data
```

## `aenv volume delete <volume>`

Delete a volume by ID or name.

```bash
aenv volume delete job-data
```

A mounted volume cannot be deleted.

