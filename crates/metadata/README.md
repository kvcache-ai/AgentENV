# Snapshot directory

`agentenv-metadata` owns the snapshot directory in PostgreSQL or SQLite. Clients
use the same `MetaClient` through an in-process channel or TCP. The Node's
production snapshot path is not switched to this service by this change.

## Directory and content

The directory separates three concepts:

- `SnapshotRecord` carries identity and lifecycle state: ID, alias, placement,
  owner, creation and update times.
- `SnapshotConfig` carries the captured sandbox's source, resource sizes,
  launch context, startup command, runtime versions, source image
  configurations, virtualization mode, volume references and custom extension parameters. Local and Distributed records
  use the same configuration, stored in SQL's `metadata` column. Promotion preserves
  the captured configuration.
- `SnapshotManifest` is the published artifact inventory: rootfs, memory and
  attached-drive layers, plus disk image publication references. Distributed
  records include it; Local records have no published inventory; durable volume references stay in the capture configuration. They retain their
  usable files on the owner Node.

The initial migration creates four tables. `snapshots` stores directory state
and captured metadata. `layers` stores managed UUID ancestry and availability. `snapshot_heads` links snapshots to their
managed chain tips. `snapshot_pins` reserves runtime-use protection for the next
stage. No retention deadline is stored.

The caller uploads Distributed content to deterministic repository locations
keyed by snapshot ID. Repository writes must create absent content, accept
identical retries and reject conflicting overwrites. The directory derives
managed layer relationships from the supplied inventory without contacting
artifact storage. A returned `SnapshotEntry` contains directory state and captured
metadata; it does not itself retain usable files.

## Operations

- `register_snapshot(record)` registers a new identity according to
  `record.snapshot_type`. Local records retain public metadata, owner,
  resources and launch context, with no alias or published manifest.
  Distributed records require completed immutable artifact upload and atomically
  register the managed layers and heads. Aliases are validated
  before writing, including RPC input.
- Each registration uses a fresh snapshot ID. The primary key and alias unique
  constraint enforce uniqueness. Another identity cannot take an occupied alias,
  including one belonging to a retired snapshot. There is no rebinding or name
  release. Timestamps are supplied by the caller.
- `promote_snapshot(record)` changes an existing Local snapshot to Distributed
  after the caller uploads immutable content. It locks the Local row by ID,
  preserves the stored `created_at` and captured `SnapshotConfig`, binds
  `record.alias` if supplied, clears `owner_node_id`, updates `updated_at`, and
  commits the directory change and managed heads together. An
  unknown ID returns `None`. A retired ID returns `Unavailable`. An occupied
  alias rolls back the promotion with `AliasConflict`. Once Distributed,
  retrying by ID with a matching alias returns the stored entry and timestamps.
  A different alias returns `SnapshotConflict`.
- `retire_snapshot(id)` permanently hides an identity and reserves its alias.
  Retiring an unknown ID records a hidden retired row so a late registration
  cannot recreate it. Existing rows only flip `retired`, keeping their alias,
  heads and timestamps. Repeated retirement succeeds.
- `get_snapshot(reference)` looks up an ID first, then an alias. Retired identities remain
  distinguishable from missing identities through `SnapshotEntry::is_visible`;
  an unknown-ID retirement tombstone does not shadow another snapshot's
  UUID-shaped alias, and retired IDs must not trigger fallback to a legacy directory.
- `list_snapshots(filter)` returns visible entries, ordered by creation time descending
  then ID ascending. ID sets, exact ID/alias, alias prefix, source sandbox, source kind and template status filters retain AND semantics;
  `snapshot_ids: None` is unrestricted and `Some([])` matches nothing. The database
  applies the filters before returning metadata. Source sandbox IDs are nonempty
  text without NUL. The source filter uses an expression index on `metadata.source`.
  Empty ID sets and impossible alias prefixes return without querying SQL.

For Local entries, `into_local_record` restores the captured configuration with
`manifest: None`. For Distributed entries, the caller reads the immutable
`SnapshotManifest` inventory from its repository and supplies it to
`into_record(manifest)` to reconstruct the `SnapshotRecord`. The application-side `MetadataCatalog` connects these operations to the snapshot lifecycle
and enforces immutable POSIX/OSS manifests. Default Node/CLI wiring still uses the
repository-document directory; pre-created template builds retain their existing path.

## UUID prefix forest

For each rootfs, memory, attached-drive and captured-volume chain, only managed layers enter the
forest. An external layer is skipped without breaking the managed predecessor:
`[A, External, B]` projects to `B -> A`. Shared prefixes and heads are deduplicated.
The producer owns the contract that a managed UUID has a fixed predecessor and
canonical digest. Missing, invalid or nil UUIDs are rejected at publication;
legacy identity repair belongs at the import boundary.

Publication locks existing layer rows in UUID order and inserts missing identities
in parent-first order. Existing static bindings are not compared on each publish.
If another transaction fills a previously missing identity, the entire publication
retries so it observes and locks that layer's state. Non-available layers reject
publication. Snapshot, layer and head writes commit together or roll back together.

Pin/release, physical GC and coordination between uploads and GC follow
separately. A Local directory entry does not keep files alive or make them
available on another node.

## Run

```sh
cargo run -p agentenv-metadata -- --sqlite-path /var/lib/aenv/metadata.sqlite3 db migrate
cargo run -p agentenv-metadata -- --sqlite-path /var/lib/aenv/metadata.sqlite3 serve --listen 127.0.0.1:9230
```

For PostgreSQL, use `--backend postgres` and `AENV_DATABASE_POSTGRES_URL` with
an existing database. `db migrate` initializes SQLx's migration history and
applies the bundled schema. `db status` and `db check` inspect history read-only.
`db check` requires all migrations to be applied. Run one SQLite migrator at a
time.

Recreate disposable databases initialized with the earlier draft migration.
After deployment, keep applied migrations immutable and add forward-only
`<version>_<description>.sql` files. SQLx verifies applied checksums.

Apply migrations before starting the service. Set the listen address with
`--listen` or `AENV_META_ADDRESS`. TCP defaults to loopback and is intended
for a trusted internal connection.

For an embedded service, open `MetaServer` using `DatabaseConfig`, obtain a
client through `transport::embedded`, and close the server after its consumers
finish. The standalone process stops accepting requests on shutdown, waits for
accepted requests to finish, then closes its database connections. If a
registration response is lost, query its snapshot ID to check whether it committed.

The existing directory integration test uses SQLite by default. Set
`AENV_TEST_DATABASE_URL` to an **empty disposable PostgreSQL database** to run
the same contract there as well. It creates the schema and leaves its records
for the owner of that disposable database to clean up.
