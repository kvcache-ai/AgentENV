# Snapshots

A snapshot is a reusable checkpoint of a sandbox. It preserves the sandbox's
filesystem and runtime state so that you can later start a new sandbox from the
same point instead of rebuilding the environment and rerunning setup work.

- **Templates** are stored as snapshots. A template build commits one snapshot;
  the template ID is an alias that resolves to it.
- **Warm-started sandboxes** restore the state of a committed template or snapshot.
- **Running sandboxes** can produce new snapshots, capturing their current
  state for later reuse or branching.

## What a Snapshot Preserves

A snapshot preserves:

- VM state and memory, allowing running processes to continue from the captured point.
- The root filesystem and its changes.
- Attached-drive contents and attachment metadata.
- Mounted-volume contents, mount paths, sizes, and access modes. Starting from
  the snapshot creates new volumes from the captured contents rather than
  reusing the source volume identities.
- CPU, memory, and disk settings, together with command context such as
  environment variables, working directory, user, and startup commands.

---

Where to Go Next:

- [Create a Snapshot](./create.md) — capture a reusable checkpoint from a running sandbox.
- [Manage Snapshots](./manage.md) — list, inspect, and delete snapshots.
- [Rootfs as an OCI Image](./use.md) — publish or export only the captured root filesystem.
- [Optional P2P Visibility](./p2p.md) — accelerate committed snapshot artifact distribution between nodes.
