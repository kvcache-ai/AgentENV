# Snapshots

`aenv snap` is an alias for `aenv snapshot`, including its `create` and `list`
subcommands.

## `aenv snapshot create <sandbox-id>`

Capture a persistent snapshot from a running sandbox. The snapshot can be used as a template to start new sandboxes with `aenv start`.

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-base
```

| Flag | Description |
|------|-------------|
| `--name <name>` | Snapshot name or alias. If omitted, the generated snapshot ID identifies the snapshot. |

When source-registry image publication is enabled on the server, the command
also prints the published OverlayBD-native image reference on an `Image:` line;
that reference can be passed to `aenv start --cold <image-reference>`.

## `aenv snapshot list`

List persistent snapshots. Alias: `aenv snapshot ls`, `aenv snap ls`.

```bash
aenv snapshot list
aenv snapshot list --sandbox-id <sandbox-id>
```

| Flag | Description |
|------|-------------|
| `--sandbox-id <id>` | Filter snapshots by source sandbox ID |
| `--output <table\|json>` | Output format. Defaults to table on a TTY and JSON when redirected. |

The table output includes an `IMAGE REF` column (`-` when no image was published); JSON output includes the optional `imageRef` field.

To delete a snapshot, use `aenv template delete <snapshot-id>` or `aenv template delete <name>` — snapshots share the same underlying store as templates and are deleted through the same command.

