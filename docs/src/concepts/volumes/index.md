# Volumes

> **Beta:** The volume feature is still in beta. For workloads that can use a
> drive supplied only when cold-starting a sandbox, the `attachedDrives`
> feature on `POST /sandboxes-cold` is more extensively tested. See the
> [API reference](../../api/index.md) for the extra-drive request schema.

Volumes are persistent block filesystems managed independently from sandboxes.
A volume has a stable ID, a unique name, a fixed size, and an access mode. You can
mount it at an absolute guest path when creating a sandbox.

Deleting a sandbox does not delete its volumes. Before stopping a running
sandbox, AgentENV commits the latest writes from its writable volumes so they
can be mounted by another sandbox later. If this commit cannot be completed
safely, AgentENV prevents the incomplete volume data from being mounted.

Pausing is different: it preserves the complete sandbox state, including its
memory and root filesystem, for a later resume.

The default volume size is 65536 MiB (64 GiB). By default, one sandbox may
mount up to four volumes and each volume may be at most 262144 MiB (256 GiB).
Administrators can change these limits in the [`[volume]` configuration](../../configuration/reference.md#volume).

---

Where to Go Next:

- [Create and Mount Volumes](./create-and-mount.md) — create empty, forked, or image-backed volumes and mount them in sandboxes.
- [Manage Volumes](./manage.md) — list, inspect, and delete volumes.
- [Sandbox Forks and Snapshots](./sandbox-forks-and-snapshots.md) — understand how mounted volumes behave during sandbox fork and snapshot operations.
