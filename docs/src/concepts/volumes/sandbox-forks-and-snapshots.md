# Volumes in Sandbox Forks and Snapshots

AgentENV automatically carries mounted volumes through sandbox fork and
snapshot operations. You do not need to fork or snapshot each mounted volume
separately.

## Fork a Sandbox

When a sandbox is forked, AgentENV processes every mounted volume for every
child sandbox:

- Each mounted `exclusive` volume gets an independent copy-on-write volume
  fork. The child mounts the new volume at the same guest path and can modify
  it without changing the source sandbox's volume.
- Each mounted `ro` volume remains mounted from the same read-only volume. It
  is safe to share because neither the source nor a child can modify it.

This behavior is separate from manually creating a reusable volume fork with
`aenv volume create --from-volume`.

## Snapshot a Sandbox

When a sandbox is snapshotted, every mounted volume is included automatically.
The volume snapshot records its layers, size, mount path, and access mode.
Starting a sandbox from that snapshot creates new volumes with the captured
contents and mounts them at the same paths. An exclusive volume remains
exclusive; a read-only volume remains read-only.

Volume snapshots are independent from their source volumes. Deleting a source
volume does not remove volume data already committed into a sandbox snapshot.

