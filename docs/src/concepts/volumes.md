# Volumes

> **Beta:** The volume feature is still in beta. For workloads that can use a
> drive supplied only when cold-starting a sandbox, the `attachedDrives`
> feature on `POST /sandboxes-cold` is more extensively tested. See the
> [API reference](../api/index.md) for the extra-drive request schema.

Volumes are persistent block filesystems managed independently from sandboxes.
A volume has a stable ID, a unique name, a fixed size, and an access mode. You
can mount it at an absolute guest path when creating a sandbox, then reuse its
contents after that sandbox is deleted.

The default volume size is 65536 MiB (64 GiB). By default, one sandbox may
mount up to four volumes and each volume may be at most 262144 MiB (256 GiB).
Administrators can change these limits in the [`[volume]` configuration](../configuration/reference.md#volume).

## Access Modes

A volume's mode is selected when the volume is created and cannot be changed.

| Mode | Writable | Mount concurrency | Intended use |
| --- | --- | --- | --- |
| `exclusive` | Yes | One sandbox | Per-sandbox workspaces, caches, databases, and mutable state |
| `ro` | No | Multiple sandboxes | Shared datasets, models, tools, and other immutable inputs |

An exclusive volume is reserved by its mounted sandbox. Another sandbox cannot
mount or delete it until that reservation is released. A read-only volume can
be mounted by multiple sandboxes at the same time, but guest writes fail.

## Recommended Workflow: Fork Before Use

Treat a shared volume as an immutable base and create a copy-on-write fork for
each sandbox that needs to modify it. This gives every sandbox an independent
exclusive volume without copying all source data eagerly.

```text
read-only base volume
        |
        +-- exclusive fork for sandbox A
        +-- exclusive fork for sandbox B
```

Forks capture the source volume state at creation time. Later changes to a fork
do not change the source or another fork. The fork must have the same size as
its source. An exclusive source must be unmounted before a public fork is
created; a read-only source may remain shared.

## Sandbox Forks and Snapshots

AgentENV automatically carries mounted volumes through sandbox fork and
snapshot operations. You do not need to fork or snapshot each mounted volume
separately.

### Fork a sandbox

When a sandbox is forked, AgentENV processes every mounted volume for every
child sandbox:

- Each mounted `exclusive` volume gets an independent copy-on-write volume
  fork. The child mounts the new volume at the same guest path and can modify
  it without changing the source sandbox's volume.
- Each mounted `ro` volume remains mounted from the same read-only volume. It
  is safe to share because neither the source nor a child can modify it.

This behavior is separate from manually creating a reusable volume fork with
`aenv volume create --from-volume`.

### Snapshot a sandbox

When a sandbox is snapshotted, every mounted volume is included automatically.
The volume snapshot records its layers, size, mount path, and access mode.
Starting a sandbox from that snapshot creates new volumes with the captured
contents and mounts them at the same paths. An exclusive volume remains
exclusive; a read-only volume remains read-only.

Volume snapshots are independent from their source volumes. Deleting a source
volume does not remove volume data already committed into a sandbox snapshot.

## CLI Examples

### Create and mount an empty volume

```bash
aenv volume create workspace --mode exclusive --size-mb 65536
aenv start ubuntu --volume /workspace=workspace
```

`--volume` accepts `MOUNT_PATH=VOLUME_ID_OR_NAME` and can be repeated. Mount
paths must be absolute, cannot be `/`, and cannot overlap another mount.

```bash
aenv start ubuntu \
  --volume /workspace=workspace \
  --volume /models=models-base
```

### Create a volume from an OCI image

Use `--image` to initialize a volume with the contents of an OCI image:

```bash
aenv volume create models-base \
  --mode ro \
  --image registry.example.com/team/models:latest
```

For a standard OCI image, AgentENV downloads and converts its layers when the
volume is created. For an OverlayBD-native image, AgentENV keeps the remote
layer references and does not download the layer contents during volume
creation. Blocks are fetched from the OCI registry on demand when the mounted
volume is read, and then retained in the local remote-block cache.

### Create a reusable base and fork it

First populate an exclusive seed volume:

```bash
aenv volume create dataset-seed --mode exclusive
SANDBOX_ID=$(aenv start ubuntu -d --volume /data=dataset-seed)
aenv exec "$SANDBOX_ID" sh -lc 'printf "%s\n" training-data > /data/input.txt'
aenv delete "$SANDBOX_ID"
```

Create a read-only base from the seed, then create one exclusive fork per
sandbox:

```bash
aenv volume create dataset-base --mode ro --from-volume dataset-seed
aenv volume create job-42-data --mode exclusive --from-volume dataset-base
aenv start ubuntu --volume /data=job-42-data
```

These commands use the default 64 GiB size for every volume. When the source
has a custom size, pass the same `--size-mb` value while creating its fork.

### Share a read-only volume

```bash
SANDBOX_A=$(aenv start ubuntu -d --volume /models=models-base)
SANDBOX_B=$(aenv start ubuntu -d --volume /models=models-base)
```

Both sandboxes can read `/models`. Neither sandbox can modify it.

### Inspect lifecycle state

```bash
aenv volume list
aenv volume inspect job-42-data
aenv volume delete job-42-data
```

The volume status controls whether it can be mounted:

| Status | Meaning |
| --- | --- |
| `ready` | The volume can be mounted. |
| `uploading` | Publication is in progress; the volume is temporarily unavailable. |
| `failed` | Publication failed; the volume is unavailable until recovered. |

Deleting a mounted volume returns a conflict. Delete its sandbox first, then
delete the volume.

## HTTP API Examples

Set the server URL and API key before running these examples:

```bash
export AENV_URL=http://127.0.0.1:8000
export AENV_API_KEY=<api-key>
```

Create a read-only base volume:

```bash
curl -fsS -X POST "$AENV_URL/volumes" \
  -H "X-API-Key: $AENV_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dataset-base",
    "sizeMB": 65536,
    "mode": "ro",
    "image": "ghcr.io/example/dataset:latest"
  }'
```

Create an exclusive copy-on-write fork:

```bash
curl -fsS -X POST "$AENV_URL/volumes" \
  -H "X-API-Key: $AENV_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "job-42-data",
    "sizeMB": 65536,
    "mode": "exclusive",
    "fromVolume": "dataset-base"
  }'
```

Mount the fork into a sandbox:

```bash
curl -fsS -X POST "$AENV_URL/sandboxes" \
  -H "X-API-Key: $AENV_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "templateID": "ubuntu",
    "volumeMounts": {
      "/workspace/data": "job-42-data"
    }
  }'
```

List and inspect volumes:

```bash
curl -fsS "$AENV_URL/volumes" -H "X-API-Key: $AENV_API_KEY"
curl -fsS "$AENV_URL/volumes/job-42-data" -H "X-API-Key: $AENV_API_KEY"
```
