# Create and Mount Volumes

## Create a Volume

`aenv volume create` creates an empty volume, a copy-on-write fork of an
existing volume, or a volume initialized from an OCI image.

Usage:

```bash
aenv volume create <name> [options]
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<name>` | Required | Unique name for the new volume. |
| `--size-mb <MiB>` | `65536` | Volume size in MiB. The value must be greater than zero. |
| `--mode <exclusive\|ro>` | `exclusive` | Access mode for the new volume. |
| `--from-volume <volume>` | None | Create a copy-on-write fork from an existing volume ID or name. Conflicts with `--image`. |
| `--image <image>` | None | Initialize the volume from an OCI image. Conflicts with `--from-volume`. |

### Access Modes

A volume's mode is selected when the volume is created and cannot be changed.

| Mode | Writable | Mount concurrency | Intended use |
| --- | --- | --- | --- |
| `exclusive` | Yes | One sandbox | Per-sandbox workspaces, caches, databases, and mutable state |
| `ro` | No | Multiple sandboxes | Shared datasets, models, tools, and other immutable inputs |

An exclusive volume is reserved by its mounted sandbox. Another sandbox cannot
mount or delete it until that reservation is released. A read-only volume can
be mounted by multiple sandboxes at the same time, but guest writes fail.

### Create an Empty Volume

Create an empty volume by omitting both `--from-volume` and `--image`:

```bash
aenv volume create workspace --size-mb 65536 --mode exclusive
```

Both options in this example use their default values, so
`aenv volume create workspace` creates the same volume.

### Fork an Existing Volume

Use `--from-volume` to create a copy-on-write fork from an existing volume ID
or name:

```bash
aenv volume create job-data --mode exclusive --from-volume dataset-base
```

The fork captures the source volume's state at creation time. Later changes to
the fork do not change the source or another fork. It must have the same size
as its source; if the source has a custom size, pass that size with
`--size-mb`. An exclusive source must be unmounted before a fork is created,
while a read-only source may remain mounted.

> **Recommended workflow: fork before use**
>
> Treat a shared volume as an immutable, read-only base and create one
> exclusive fork for each sandbox that needs to modify it. This gives every
> sandbox an independent writable volume without copying all source data
> eagerly.
>
> ```bash
> aenv volume create job-a-data --mode exclusive --from-volume dataset-base
> aenv volume create job-b-data --mode exclusive --from-volume dataset-base
> ```
>
> This is an explicit volume operation. Mounting a volume does not change its
> mode or automatically fork it.

### Create a Volume from an OCI Image

Use `--image` to initialize a volume with the contents of an OCI image:

```bash
aenv volume create models-base \
  --mode ro \
  --image registry.example.com/team/models:latest
```

For a standard OCI image, AgentENV downloads and converts its
layers when the volume is created. For an OverlayBD-native
image, AgentENV keeps the remote layer references and does not
download the layer contents during volume creation. Blocks are
fetched from the OCI registry on demand when the mounted volume
is read, and then retained in the local remote-block cache.

## Mount Volumes when Starting a Sandbox

Use `aenv start --volume MOUNT_PATH=VOLUME_ID_OR_NAME` to mount a volume while
creating a sandbox. The option works for both warm and cold starts and can be
repeated:

```bash
aenv start ubuntu \
  --volume /workspace=workspace \
  --volume /models=models-base
```

Mount paths must be absolute guest paths other than `/`, and cannot overlap another mount.

