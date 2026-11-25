# Snapshot directory

`agentenv-metadata` keeps sandbox snapshot identity and lifecycle state in
SQLite or PostgreSQL. The Node and operator tools use the same client through
an in-process channel or a standalone TCP service. The Node commits a new sandbox
snapshot or promotion to the file catalog before mirroring the change in SQL.
A failed SQL write propagates to the caller and leaves the file commit available
for a retry.

The database records snapshot IDs, aliases, placement, owners, timestamps and
source sandbox IDs. The artifact repository stores `snapshot.json` alongside
the VM state and backend manifest. New content files contain the captured configuration
and, for Distributed snapshots, the published layer inventory. Local snapshots
retain runnable files on their owner Node. The owner retires its previous Local
entries when it starts with a fresh runtime.

Publication and promotion register the layer graph and snapshot heads in one
transaction. Promotion retains the snapshot ID and captured configuration.
Retirement removes the file entry and alias before retiring the SQL identity.
The retired SQL identity also hides outdated copies of the repository record.
Template builds continue to use the document catalog.

## Configure

Embedded SQLite is the single-node default. The Node initializes its schema on
startup. Use the same PostgreSQL database or TCP metadata service on all Nodes
sharing a snapshot repository. Run `server --migrate-database` for an embedded
PostgreSQL deployment before starting the Nodes. Supply the database URL through
`AENV_DATABASE_POSTGRES_URL` and select it with `AENV_DATABASE_BACKEND=postgres`.

By default, the Node and snapshot CLI prefer the repository catalog.
Set `[database].read_from_sql = true` or `AENV_DATABASE_READ_FROM_SQL=true`
to prefer SQL. Both modes can read snapshots written by previous releases,
including SQL snapshots without a repository mirror. Keep the same artifact
repository and database when updating an RC installation.

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

## Layer retention

The directory records managed layer UUIDs, their managed predecessors and each
snapshot's chain tips. Registry layers keep their original URLs. Managed rootfs,
memory and attached-drive layers live under `managed-layers`. Captured volume
references stay in the snapshot configuration.

The Metadata process owns the complete collection pass. Switch all Nodes that
use the repository to SQL reads and drain any existing consumers without pins
before running GC. Run `aenv-snapshot gc --limit 64` with SQL reads enabled and
the Node's metadata connection settings to collect retired Distributed snapshots
and newly unreferenced managed leaves wave by wave under a single-leader lock.
With a TCP service, the client sends the limit and the service uses its own
repository configuration. Start the service with `aenv-metadata serve --config PATH`
or set `AENV_CONFIG_PATH`; database administration does not require a repository.
Upgrade the Metadata service and GC CLI together when using TCP.

To build the standalone executable from source, run
`cargo build --locked -p agentenv --bin aenv-metadata`. The metadata library
depends on the storage operations needed by the collector. The executable
connects those operations to the same POSIX and OSS implementations as the Node.

## Test

The directory integration test uses SQLite and exercises the same contract on
PostgreSQL when `AENV_TEST_DATABASE_URL` points to an empty disposable database.

```sh
cargo test -p agentenv-metadata
```
