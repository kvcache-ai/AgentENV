# Use a Snapshot Rootfs as an OCI Image

Capturing a snapshot and starting it with `aenv start <snapshot>` restores the complete snapshot state. If
you only need the captured root filesystem, you can instead publish it as an
OCI image and cold-start sandboxes from that image.

## Publish an Image When Creating a Snapshot

AgentENV can automatically create and publish an OCI image of the snapshot
rootfs whenever you create a snapshot. For snapshots backed by OSS and created
from an OverlayBD-native OCI image, enable automatic publication in your
AgentENV config file (`config/default.toml`, or the file selected by
`AENV_CONFIG_PATH`):

```toml
[snapshot]
repository_backend = "oss"

[snapshot.image_publish]
enabled = true
```

Create the snapshot:

```bash
aenv snapshot create <sandbox-id> --name my-checkpoint
```

The command prints the published image reference when publication succeeds.
Use that reference to cold-start a sandbox:

```bash
aenv start --cold registry.example.com/team/app:agentenv-snapshot-<snapshot-id>
```

## Export an Existing Snapshot Disk Image

You can manually export the rootfs (or an attached drive via `--image-idx`) of
an existing snapshot as an OCI image using the unified `aenv-snapshot` CLI
(included in the runtime Docker image at `/usr/local/bin/aenv-snapshot`, or
built from source with `cargo build -p agentenv --bin aenv-snapshot`):

```bash
aenv-snapshot export-image <snapshot-id-or-alias> \
  --target-repository registry.example.com/team/app \
  --tag release-1
```

`aenv-snapshot export-image` prints a JSON object on stdout (including
`image_ref`), and supports inspecting snapshots (`aenv-snapshot info <snapshot>`)
and rendering read-only OverlayBD configs (`aenv-snapshot build-image-config <snapshot>`).

| Argument or option | Default | Description |
| --- | --- | --- |
| `<snapshot-id-or-alias>` | Required | Snapshot whose disk image is exported. |
| `--target-repository <registry/repository>` | Required | Destination OCI repository including the registry host. |
| `--image-idx <index>` | `0` | Disk image to export: `0` for rootfs, `1..=N` for attached drives in committed order. |
| `--tag <tag>` | `latest` | Tag for the exported image. |
| `--config <path>` | `AENV_CONFIG_PATH`, then the default config path | AgentENV config used to locate the snapshot directory and repository. |

The exported image can be stored in an OCI registry, shared independently of
the AgentENV snapshot repository, and used to cold-start new sandboxes. When
exporting the rootfs (`--image-idx 0`), the exported image inherits the
captured root filesystem and OCI runtime configuration (such as `Env` and
`WorkingDir`), but not the snapshot's memory or running-process state; attached
drive exports (`--image-idx 1..=N`) publish the drive layers with a minimal OCI
config.

```bash
IMAGE_REF=$(aenv-snapshot export-image <snapshot-id-or-alias> \
  --target-repository registry.example.com/team/app \
  --tag release-1 | jq -r '.image_ref')

aenv start --cold "$IMAGE_REF"
```

### Compatibility Wrapper (`aenv-snapshot-image`)

For scripts written against earlier releases, `aenv-snapshot-image` remains
available via `make build-snapshot-image` and shares the same underlying export
implementation. When migrating scripts from `aenv-snapshot-image` to
`aenv-snapshot export-image`, note two interface differences:

1. **Target repository**: `aenv-snapshot-image` allows omitting
   `--target-repository` (inferring the repository from the snapshot's recorded
   publication or unique external source), whereas `aenv-snapshot export-image`
   requires `--target-repository` explicitly.
2. **Stdout format**: `aenv-snapshot-image` writes a single bare image reference
   line to stdout (directly usable in `IMAGE=$(aenv-snapshot-image ...)`),
   whereas `aenv-snapshot export-image` writes structured JSON.

```bash
make build-snapshot-image
sudo install -m 0755 target/debug/aenv-snapshot-image /usr/local/bin/aenv-snapshot-image

IMAGE=$(aenv-snapshot-image <snapshot-id-or-alias> \
  --target-repository registry.example.com/team/app \
  --tag release-1)
aenv start --cold "$IMAGE"
```
