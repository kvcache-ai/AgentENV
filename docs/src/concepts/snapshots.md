# Snapshots

A snapshot is a reusable checkpoint of a sandbox. It preserves the sandbox's
filesystem and runtime state so that you can later start a new sandbox from the
same point instead of rebuilding the environment and rerunning setup work.

- **Templates** are stored as snapshots. A template build commits one snapshot;
  the template ID is an alias that resolves to it.
- **Sandboxes** launch by resuming from a snapshot.
- **Running sandboxes** can produce new snapshots, capturing their current
  state for later reuse or branching.

## Create a Snapshot from a Running Sandbox

Capture the current state of a running sandbox:

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-checkpoint
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the running sandbox to capture. |
| `--name <name>` | None | Assigns a human-readable alias. If omitted, use the generated snapshot ID returned by the command. |

The source sandbox continues running after the snapshot is created. You can
then pass either the snapshot ID or its alias to `aenv start`:

```bash
aenv start my-checkpoint
# Or:
aenv start <snapshot-id>
```

This creates a separate sandbox with a new sandbox ID. It inherits the captured
filesystem, running processes, memory state, environment variables, runtime
configuration, and resource settings.

To retrieve information about one snapshot by ID or alias, use the HTTP API:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/snapshots/<snapshot-id-or-alias>
```

## Use a Snapshot Rootfs as an OCI Image

Starting with `aenv start <snapshot>` restores the complete snapshot state. If
you only need the captured root filesystem, you can instead publish it as an
OCI image and cold-start sandboxes from that image.

### Publish an Image When Creating a Snapshot

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

### Export an Existing Snapshot Rootfs

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

## Manage Snapshots

### List Snapshots

```bash
aenv snapshot list
aenv snapshot list --sandbox-id <sandbox-id>
```

`aenv snapshot ls` is an alias for `aenv snapshot list`.

| Option | Default | Description |
| --- | --- | --- |
| `--sandbox-id <sandbox-id>` | All snapshots | Shows only snapshots created from the specified sandbox, including after that sandbox is deleted. |
| `--output <table\|json>` | Table in an interactive terminal; JSON when redirected | Selects the output format. |

### Start a Sandbox from a Snapshot

```bash
aenv start <snapshot-id-or-name>
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<snapshot-id-or-name>` | Required | Snapshot ID or alias to start from. |
| `--timeout <seconds>` | `300` | Sets the sandbox TTL; see [Auto-Eviction](sandboxes.md#auto-eviction). |
| `-d`, `--detach` | Disabled | Prints the new sandbox ID without attaching an interactive shell. |

The CLI always enables secure sandbox authentication and manages the envd
access token automatically.

### Delete a Snapshot

Snapshots and templates share the same catalog. Delete a snapshot by passing
its required ID or alias to the template delete command:

```bash
aenv template delete <snapshot-id-or-name>
```

## Optional P2P Visibility

P2P visibility lets nodes discover and fetch committed snapshot artifacts from
peers. The snapshot repository remains the durable source of truth, so P2P is
an optional distribution path rather than a replacement for snapshot storage.

Enable both the node-wide P2P transport and snapshot publication in your
AgentENV config file (`config/default.toml`, or the file selected by
`AENV_CONFIG_PATH`):

```toml
[p2p]
enabled = true

[snapshot]
p2p_enabled = true
```

See the [Configuration Reference](../configuration/reference.md#p2p)
for the optional transport, storage-directory, address, and timeout settings.
