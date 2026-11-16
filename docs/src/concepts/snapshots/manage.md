# Manage Snapshots

This page provides the basic commands for managing a snapshot.

## Get a Snapshot

Retrieve information about one snapshot by ID or alias with the HTTP API:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/snapshots/<snapshot-id-or-alias>
```

The response includes `sourceSandboxMetadata` when the snapshot contains user
metadata saved from its source sandbox at capture time. Creation and list
responses include the same field. For example, a caller that stores
`{"network": "private"}` in sandbox metadata can recover that value from the
snapshot after deleting the source sandbox.

The map contains user metadata, not AgentENV's internal network configuration.
Applications can read it to choose restore settings. AgentENV does not apply
these values automatically when starting a sandbox from the snapshot. An empty
map is returned as `{}`. The field is omitted when no source metadata was saved,
including older snapshots that lack it.

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

Snapshots and templates share the same catalog. Delete a snapshot by passing
its required ID or alias to the template delete command:

```bash
aenv template delete <snapshot-id-or-name>
```
