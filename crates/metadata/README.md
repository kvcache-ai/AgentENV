# Unified SQL Metadata Catalog

`agentenv-metadata` is the sole metadata catalog in SQLite or PostgreSQL for
snapshots, ready templates, internal builders, template build jobs, persistent
volumes (including BuildKit cache seeds), published images, image tags, shared
managed layers, and GC pins. Runtime nodes and operator CLIs talk to the same
service either in-process or over TCP (`aenv-metadata serve`).

The SQL schema consists of eight tables: `snapshots`, `volumes`, `images`,
`image_tags`, `build_jobs`, `layers`, `layer_heads`, and `snapshot_pins`. The artifact
repository stores immutable blobs (`snapshot.json`, `vm_state.bin`,
`firecracker-manifest.json`, optional `memory-startup.pack`, build logs, and
content-addressed layers under `managed-layers/`). Local snapshots retain
runnable files on their owner node, which retires its previous Local entries
when starting with a fresh runtime.

Snapshot publication, promotion, volume creation/updates, and image publication
register the managed layer graph and resource heads in one transaction.
Retirement marks the SQL identity retired and releases its alias so another
snapshot can reuse the alias while pinned readers continue using existing
artifacts.

## Configure

Embedded SQLite is the single-node default; the node initializes its schema on
startup. Use one shared PostgreSQL database or standalone TCP metadata service
for all nodes sharing a repository. The three modes (`backend = "sqlite"`,
`backend = "postgres"` with `postgres_url`, and `meta_address`) are mutually
exclusive.

For direct PostgreSQL deployments, run `server --migrate-database` or
`aenv-metadata --backend postgres db migrate` before starting runtime nodes.
Supply the connection string through `AENV_DATABASE_POSTGRES_URL` and select it
with `AENV_DATABASE_BACKEND=postgres`.

Upgrading from a pre-0.3.0 file-catalog deployment is an offline cutover: drain
(resume or delete) pre-0.3.0 paused sandboxes before stopping old nodes, apply
schema migrations, then run `aenv-snapshot import --all --dry-run` followed by
`aenv-snapshot import --all` (with `--skip-local` if old Local records remain)
before starting upgraded nodes. Runtime nodes never read un-imported legacy
repository documents or legacy paused-sandbox formats.

To run a separate metadata process, migrate its database, start the service,
and set `AENV_META_ADDRESS=127.0.0.1:9230` on clients that can reach it:

```sh
aenv-metadata --sqlite-path /var/lib/aenv/metadata.sqlite3 db migrate
aenv-metadata --sqlite-path /var/lib/aenv/metadata.sqlite3 serve --listen 127.0.0.1:9230
```

TCP defaults to loopback and is intended for trusted networks. The `db status`
and `db check` commands inspect applied migrations without creating or mutating
the database. Once deployed, keep applied migrations immutable and add
forward-only migration files.

## Layer Retention and Garbage Collection

The catalog records managed layer UUIDs, their managed predecessors, and every
live resource's chain tips in `layers` and `layer_heads`. External registry
layers keep their original URLs. All managed rootfs, memory, attached-drive,
volume, and published-image layers live under `managed-layers/`.

The Metadata process owns the complete collection pass. Run
`aenv-snapshot gc --limit 64` with the node's metadata connection settings to
reap retired unpinned Distributed snapshots and delete unreferenced managed
layer leaves wave by wave under a single-leader lock. Over TCP, the client sends
the limit and `aenv-metadata serve` uses its own repository configuration
(`--config PATH` or `AENV_CONFIG_PATH`). Database administration commands do not
require a repository configuration.

Use `aenv-snapshot import --all [--dry-run] [--skip-local]` to audit and import
all pre-0.3.0 snapshots, templates, builders (including legacy
`template-build/builder` sub-repositories), volumes, published images, image
tags, and `imageOnly` build jobs, or `aenv-snapshot import <snapshot-id>...`
(or stdin) for selected Distributed snapshots.

The runtime image includes `aenv-metadata`. To build it from source, run
`cargo build --locked -p agentenv --bin aenv-metadata`.

## Test

The integration suite runs against SQLite by default and runs the same contract
against PostgreSQL when `AENV_TEST_DATABASE_URL` points to an empty disposable
database:

```sh
cargo test -p agentenv-metadata
```
