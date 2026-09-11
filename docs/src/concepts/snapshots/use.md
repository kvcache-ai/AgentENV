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

## Export an Existing Snapshot Rootfs

You can manually export the rootfs of an existing snapshot as an OCI image.
This is done with `aenv-snapshot-image`, which is not included in the regular
AgentENV installation packages and must first be built and installed from the
repository:

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
make build-snapshot-image
sudo install -m 0755 target/debug/aenv-snapshot-image /usr/local/bin/aenv-snapshot-image
```

Export the rootfs:

```bash
aenv-snapshot-image <snapshot-id-or-alias> \
  --target-repository registry.example.com/team/app \
  --tag release-1
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<snapshot-id-or-alias>` | Required | Snapshot whose rootfs is exported. |
| `--target-repository <registry/repository>` | Inferred from the snapshot | Destination OCI repository. Specify it when a unique source repository cannot be inferred. |
| `--tag <tag>` | `latest` for an explicit destination; otherwise `snapshot-<snapshot-id>` | Tag for the exported image. |
| `--config <path>` | `AENV_CONFIG_PATH`, then the default config path | AgentENV config used to locate the snapshot repository. |

The exported image can be stored in an OCI registry, shared independently of
the AgentENV snapshot repository, and used to cold-start new sandboxes. The new
sandbox inherits the exported root filesystem and OCI runtime configuration,
but not the snapshot's memory or running-process state.

You can capture it and start a sandbox from the resulting image:

```bash
aenv-snapshot-image <snapshot-id-or-alias> \
  --target-repository registry.example.com/team/app \
  --tag release-1

aenv start --cold <image_ref>
```
