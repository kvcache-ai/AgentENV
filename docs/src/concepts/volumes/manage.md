# Manage Volumes

This page provides the basic commands for managing a volume.

## List Volumes

`aenv volume list` lists all volumes.

```bash
aenv volume list  # Alias: aenv volume ls
```

`--output` accepts `table` or `json`. It defaults to `table` in an interactive
terminal and `json` when output is piped or redirected.

## Inspect a Volume

`aenv volume inspect <volume>` prints the complete record for a volume ID or
name as formatted JSON.

The volume status controls whether it can be mounted:

| Status | Meaning |
| --- | --- |
| `ready` | The volume can be mounted. |
| `uploading` | Publication is in progress; the volume is temporarily unavailable. |
| `failed` | Publication failed; the volume is unavailable until recovered. |

## Delete a Volume

`aenv volume delete <volume>` deletes a volume by ID or name.

A mounted volume cannot be deleted. Delete its sandbox first, then delete the
volume.

