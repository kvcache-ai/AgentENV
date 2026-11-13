# Snapshot directory

`agentenv-metadata` keeps sandbox snapshot identity and lifecycle state in
SQLite or PostgreSQL. The Node and operator tools use the same client through
an in-process channel or a standalone TCP service.

The database records snapshot IDs, aliases, placement, owners, timestamps and
source sandbox IDs. The artifact repository stores `snapshot.json` alongside
the VM state and backend manifest. The record contains the captured configuration
and, for Distributed snapshots, the published layer inventory. Local snapshots
retain runnable files on their owner Node. The owner retires its previous Local
entries when it starts with a fresh runtime.

Publication and promotion register the layer graph and snapshot heads in one
transaction. Promotion retains the snapshot ID and captured configuration.
Retirement hides the ID and reserves its alias. A SQL retirement also hides a
legacy record for the same snapshot. Node reads support existing repository
documents, and template builds continue to use the document catalog.

## Configure

Embedded SQLite is the single-node default. The Node initializes its schema on
startup. Use the same PostgreSQL database or TCP metadata service on all Nodes
sharing a snapshot repository. Run `server --migrate-database` for an embedded
PostgreSQL deployment before starting the Nodes. Supply the database URL through
`AENV_DATABASE_POSTGRES_URL` and select it with `AENV_DATABASE_BACKEND=postgres`.

To run a separate metadata process, migrate its database, start the service,
and set `AENV_META_ADDRESS=127.0.0.1:9230` on clients that can reach it.

```sh
aenv-metadata --sqlite-path /var/lib/aenv/metadata.sqlite3 db migrate
aenv-metadata --sqlite-path /var/lib/aenv/metadata.sqlite3 serve --listen 127.0.0.1:9230
```

TCP defaults to loopback and is intended for a trusted connection. The
`db status` and `db check` commands inspect applied migrations. Recreate
disposable databases created with the previous draft schema. Once deployed,
keep applied migrations immutable and add forward-only migration files.

## Layer retention and collection

The directory records managed layer UUIDs, their managed predecessors and each
snapshot's chain tips. Registry layers keep their original URLs. New SQL-managed
rootfs, memory and attached-drive layers use the `snapshot-layers` prefix.
Existing snapshots, templates, volumes and published images retain their shared
`managed-layers` objects. Captured volume references stay in the snapshot
configuration. Their shared backing layers are outside the snapshot collector.

Restoring or exporting a Distributed snapshot from SQL holds a pin. A paused
sandbox retains its source pin across a Node restart. Run
`aenv-snapshot gc --limit 64` with the Node's database and repository settings
to collect one batch of unpinned retired snapshots and unreferenced managed
leaves. Multiple passes may be required to collect a chain from leaf to root.

Use the [catalog importer](../../tools/snapshot-catalog-import/README.md) for
a staged handoff of an existing OSS directory.

## Test

The directory integration test uses SQLite and exercises the same contract on
PostgreSQL when `AENV_TEST_DATABASE_URL` points to an empty disposable database.

```sh
cargo test -p agentenv-metadata
```
