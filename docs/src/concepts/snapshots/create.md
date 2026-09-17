# Create a Snapshot from a Running Sandbox

Capture the current state of a running sandbox:

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-checkpoint
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the running sandbox to capture. |
| `--name <name>` | None | Assigns a human-readable alias. If omitted, use the generated snapshot ID returned by the command. |

The source sandbox continues running after the snapshot is created. Continue
with [Use a Snapshot](./use.md) to restore it or publish its rootfs as an OCI
image.

You can then pass either the snapshot ID or its alias to `aenv start`:

```bash
aenv start my-checkpoint
# Or:
aenv start <snapshot-id>
```

This creates a separate sandbox with a new sandbox ID. It inherits the captured
filesystem, running processes, memory state, environment variables, runtime
configuration, and resource settings.

The HTTP endpoint `POST /sandboxes/{sandboxID}/snapshots` also accepts
`{"snapshotType":"local"}` to create an owner-local snapshot. Omitting
`snapshotType` selects `distributed`. Local snapshots do not accept `name`.
Use `POST /snapshots/{snapshotID}/promote` to publish an existing Local snapshot
as Distributed without recapturing the VM. `POST /sandboxes` requires a Distributed
snapshot, so promote a Local snapshot before using it to create a sandbox.
Repeating promotion returns the same Distributed snapshot.
