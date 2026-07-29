# Snapshots

Snapshots are the durable runtime primitive in AgentENV. Everything else is
built on top of them:

- **Templates** are stored as snapshots. A template build commits one snapshot;
  the template ID is an alias that resolves to it.
- **Sandboxes** launch by resuming from a snapshot.
- **Running sandboxes** can produce new snapshots, capturing their current
  state for later reuse or branching.

## Create a Snapshot from a Running Sandbox

Capture the current state of a live sandbox:

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-checkpoint
```

Pass `--name` to assign a human-readable alias. The alias can then be used with
`aenv start` or any command that accepts a snapshot ID.

The sandbox transitions through `Running → Snapshotting → Running` during
capture; the sandbox continues running after the snapshot is committed.
Recoverable failures leave the sandbox running and surface the error to the
caller. Terminal failures — where the runtime was mutated past safe resume —
tear down the sandbox.

## OverlayBD Image Publication

When the snapshot repository backend is `oss` and `[snapshot.image_publish]` is
enabled, publishing a snapshot also writes its rootfs back to the original
OverlayBD-native OCI registry as an image tag:

```text
Created snapshot 018f0d93-aaaa-bbbb-cccc-0123456789ab
Image: registry.example.com/team/app:agentenv-snapshot-018f0d93-aaaa-bbbb-cccc-0123456789ab
```

The tag lives in the same registry and repository as the source image.
Publication is incremental: layers already present in that repository (the
base image and previously published snapshot deltas) are referenced by digest
and never re-uploaded; only new runtime delta layers are pushed. The result is
an OverlayBD-native OCI image that can be used directly as a `userImage` to
cold boot new sandboxes.

Notes:

- The image contains only the root filesystem. Memory state and `vm_state.bin`
  remain in the snapshot repository, so resuming with full memory state still
  requires starting from the snapshot itself.
- The published reference is returned as `imageRef` in snapshot create/GET/list
  API responses and shown by `aenv snapshot create` and `aenv snapshot list`.
- Snapshots created from non-OverlayBD-native source images, or created while
  publication is disabled, do not produce an `imageRef`.
- AgentENV never overwrites an existing generated snapshot tag. Registry
  administrators can still move or replace tags through external tooling.
- Deleting a snapshot also attempts to delete its published manifest from the
  registry.

## Manage Snapshots

### List Snapshots

List all snapshots, optionally filtered by source sandbox:

```bash
aenv snapshot list                           # alias: aenv snapshot ls
aenv snapshot ls --sandbox-id <sandbox-id>
```

The `--sandbox-id` filter works even after the source sandbox has been deleted.

### Start Sandboxes from Snapshots

Start a new sandbox from a snapshot:

```bash
aenv start <snapshot-id-or-name>
```

### Delete Snapshots

Snapshots share the same catalog as templates. To delete a snapshot, use the
template delete command with the snapshot ID or alias:

```bash
aenv template delete <snapshot-id-or-name>
```

## Optional P2P Visibility

When `[p2p].enabled = true`, published snapshot artifacts are advertised
through the node-to-node P2P transport after the repository commit succeeds.
This makes new artifacts discoverable to peer nodes before slow backend reads
become the bottleneck:

- `vm_state.bin` and `firecracker-manifest.json` are advertised under
  snapshot-scoped keys.
- Overlaybd commit layers are advertised under the shared overlaybd layer keys
  (`overlaybd-layer/v1/sha256:<digest>`), so overlaybd runtime reads can find
  them through the overlaybd P2P facade.

P2P does not change the committed snapshot model. The snapshot repository
remains the source of truth, and failed P2P publication does not roll back a
successful snapshot publish.

## Storage

### Three-layer model

It helps to treat these as three different layers rather than one combined store:

**1. Builder staging**

This is a manager-owned temporary workspace used while executing one build. It
contains local snapshot artifacts captured from the build sandbox, for example:

```text
<local_cache_path>/
  snapshots/<id>/
    vm_state.bin
    mem_image.json
    rootfs/
      image.json
    drives/<drive-id>/
      image.json
```

This directory is a `TemplateBuilder` implementation detail. It is not the
durable committed record.

**2. Committed snapshot repository**

Once a build is published, the durable state lives in the snapshot repository:

```text
<snapshot_store>/
  repository/
    catalog/
      aliases/
    snapshots/<id>/
      snapshot.json
      firecracker-manifest.json
      vm_state.bin
    managed-layers/
      <digest>.overlaybd.commit
```

This repository is the source of truth wrapped by the template API.

**3. Node-local runtime cache**

Before a sandbox is launched, AgentENV derives runnable overlaybd configs from
the committed snapshot and materializes them in a node-local runtime cache:

```text
<node_local_runtime_cache>/
  runtime/<id>/
    memory/
      image.json
    rootfs/
      image.json
    drives/<drive-id>/
      image.json
```

These are launch-time derived runtime inputs, not committed metadata.

### What a committed snapshot contains

Each committed snapshot is split across two durable files:

**`snapshot.json`** contains:

- **`context`** — the runtime configuration baked in at build time: environment
  variables, working directory, user, exposed ports, volumes, and labels.
  Sandboxes launched from this snapshot inherit these values.
- **`rootfs_layers`**, **`attached_drives[].layers`**, **`memory_layers`** —
  overlaybd layer references, either as managed local layers (deduplicated by content digest) or as external OCI registry references.
- Startup configuration.

**`firecracker-manifest.json`** contains launch-time metadata not expressed as
overlay layer lists, including `memory.virtual_size`, `rootfs.virtual_size`,
and per-drive metadata.

The following files exist only as local build artifacts or node-local derived
configs and are not committed:

- `rootfs/image.json`
- `drives/<id>/image.json`
- `mem_image.json`
- `upper.data`
- `upper.index`

### Runtime resolution

Before launch, the committed state is resolved into node-local paths:

- `memory_layers` become runtime `memory/image.json`
- `rootfs.layers` become runtime `rootfs/image.json`
- `attached_drives[].layers` become runtime `drives/<id>/image.json`
- `firecracker-manifest.json` is loaded and hydrated with node-local absolute paths
- committed `vm_state.bin` becomes the runnable vm-state path
