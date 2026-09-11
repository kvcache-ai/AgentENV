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
