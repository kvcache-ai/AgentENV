# Manage Snapshots

This page provides the basic commands for managing a snapshot.

## Get a Snapshot

Retrieve information about one snapshot by ID or alias with the HTTP API:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/snapshots/<snapshot-id-or-alias>
```

## List Snapshots

```bash
aenv snapshot list
aenv snapshot list --sandbox-id <sandbox-id>
```

`aenv snapshot ls` is an alias for `aenv snapshot list`.

| Option | Default | Description |
| --- | --- | --- |
| `--sandbox-id <sandbox-id>` | All snapshots | Shows only snapshots created from the specified sandbox, including after that sandbox is deleted. |
| `--output <table\|json>` | Table in an interactive terminal; JSON when redirected | Selects the output format. |

## Delete a Snapshot

Delete a sandbox snapshot by ID or alias with the HTTP API:

```bash
curl -X DELETE -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/snapshots/<snapshot-id-or-alias>
```

Success returns `204`. Missing snapshots, already deleted snapshots, and
template builds return `404`; delete template builds through the template API.

The template delete command also accepts a snapshot ID or alias.

```bash
aenv template delete <snapshot-id-or-name>
```

## Inspect and Collect Stored Snapshots

Use `aenv-snapshot` on a host with access to the Node's metadata and artifact
repository. Disk index 0 selects the root filesystem. Higher indices select
attached drives in the order shown by `info`.

```bash
aenv-snapshot info <snapshot-id-or-alias>
aenv-snapshot build-image-config <snapshot-id-or-alias> --image-idx 0
aenv-snapshot export-image <snapshot-id-or-alias> --target-repository <registry/repository>
aenv-snapshot gc --limit 64
```

Deleting a SQL-managed snapshot retires it immediately. The `gc` command
reclaims retired snapshot artifacts once their pins are released and collects
unreferenced layers in batches. Use the same database and repository settings
as the Node.
