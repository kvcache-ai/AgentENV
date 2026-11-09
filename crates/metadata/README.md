# Snapshot directory

`agentenv-metadata` owns the snapshot directory in PostgreSQL or SQLite. Clients
use the same `MetaClient` through an in-process channel or TCP. The Node's
production snapshot path is not switched to this service by this change.

## Directory and content

The initial migration creates all four first-release tables. `snapshots` stores
identity, permanent alias ownership, placement, directory metadata, retirement
state and an internal `committed_digest`. `layers` stores each managed UUID's fixed
predecessor, canonical digest and availability. `snapshot_heads` links snapshots
to the tips of their managed chains. `snapshot_pins` represents runtime-use
protection; pin and release are reserved for a later stage. There is no extra retention
period or `reclaim_after` column.

Distributed content is not copied into SQL. The caller uploads immutable content
at a deterministic repository location keyed by snapshot ID and passes the
structured `SnapshotRecord` to the directory. Because a fixed path does not by
itself prevent overwrites, the repository write path must enforce create-if-absent
or idempotent same-content semantics and reject conflicting overwrites before
committing to the directory. The metadata service computes a canonical digest of
the committed payload (recursively sorting object keys and preserving array order)
and derives managed layer relationships from the supplied chains without reading
files or contacting artifact storage. A `SnapshotEntry` returns directory
information, not a complete runnable snapshot.

## Operations

- `register_snapshot(record)` rejects records being deleted and registers a new identity according to
  `record.snapshot_type`. Local records retain public metadata, owner,
  resources and launch context, with no alias or published rootfs, memory or
  attached-drive references. Local volume-snapshot references retain their managed
  layer relationships.
  Distributed records require completed immutable artifact upload and atomically
  register the committed digest, managed layers and heads. Aliases are validated
  before writing, including RPC input.
- Retrying the same complete request (matching identity, alias, placement,
  source, resources, creation and update timestamps, and canonical committed
  digest) returns the original result. Different directory metadata or a
  different committed payload returns `SnapshotConflict`. Another identity
  cannot take an occupied alias, including one belonging to a retired snapshot.
  There is no rebinding or name release. Timestamps are supplied by the caller
  and must remain stable on retries.
- `promote_snapshot(record)` changes an existing Local snapshot to Distributed
  after the caller uploads immutable content. It accepts the target Distributed
  `SnapshotRecord`, verifies under lock that `created_at_unix_ms` and `source`
  match the existing Local record, binds `record.alias` if supplied, updates
  `resources` and `updated_at_unix_ms`, drops Local-only metadata, and commits
  the canonical committed digest and managed heads together. An unknown ID
  returns `None`. A retired ID returns `Unavailable`. An occupied alias rolls
  back the promotion with `AliasConflict`. Once Distributed, retrying the same
  complete request returns the existing entry; any differing field or payload
  returns `SnapshotConflict`.
- `retire_snapshot(id)` permanently hides an identity and reserves its alias.
  Retiring an unknown ID records a hidden retired row so a late registration
  cannot recreate it. Repeated retirement succeeds. Retirement retains heads
  and committed digests for existing users and eventual cleanup.
- `get_snapshot(reference)` looks up an ID first, then an alias. Retired identities remain
  distinguishable from missing identities through `SnapshotEntry::is_visible`;
  they must not trigger fallback to a legacy directory.
- `list_snapshots(filter)` returns visible entries, ordered by creation time descending
  then ID ascending. Alias prefix, IDs, exact ID or alias, source kind, source
  sandbox ID and template build status filters combine with AND semantics;
  `snapshot_ids: None` is unrestricted and `Some([])` matches nothing.

For Local entries, `into_local_record` restores the saved public record. For
Distributed entries, directory metadata retains `resources`, so the caller reads
the immutable `CommittedSnapshot` from its repository and supplies it to
`into_record(committed)` to reconstruct the `SnapshotRecord`. The application-side `MetadataCatalog` pairs this directory with a repository,
provides atomic non-overwriting manifest storage, and connects registration,
promotion, listing and retirement to `SnapshotManager`. It is available through
explicit dependency injection; default Node/CLI configuration still uses
`RepositoryCatalog`. Pre-created template build transitions remain on that existing
directory. Metadata-backed publication finishes any startup recording before
committing its immutable manifest; the document-backed path retains asynchronous
startup-manifest attachment.

## UUID prefix forest

For each rootfs, memory, attached-drive and volume-snapshot chain, only managed
layers enter the forest. An external layer is skipped without breaking the managed predecessor:
`[A, External, B]` projects to `B -> A`. Shared prefixes and heads are deduplicated.
The producer owns the contract that a managed UUID has a fixed predecessor and
canonical digest. Missing, invalid or nil UUIDs are rejected at publication;
legacy identity repair belongs at the import boundary.

Publication locks existing layer availability and inserts missing identities in
parent-first order. Existing static bindings are not compared on each publish.
If another transaction fills a previously missing identity, the entire publication
retries so it observes and locks that layer's state. Non-available layers reject
publication. Snapshot, layer and head writes commit together or roll back together.

Pin/release, physical GC and coordination between uploads and GC follow separately. A Local directory entry does not keep files alive or make them
available on another node.

## Run

```sh
cargo run -p agentenv-metadata -- --sqlite-path /var/lib/aenv/metadata.sqlite3 db migrate
cargo run -p agentenv-metadata -- --sqlite-path /var/lib/aenv/metadata.sqlite3 serve --listen 127.0.0.1:9230
```

For PostgreSQL, use `--backend postgres` and `AENV_DATABASE_POSTGRES_URL`.
Database inspection and migration commands retain the behavior documented in
[migrations/README.md](migrations/README.md). Service startup requires migrations
to have been applied. Set the listen address with `--listen` or `AENV_META_ADDRESS`. TCP defaults to loopback and is intended for a trusted
internal connection; this stage does not add authentication or TLS.

For an embedded service, open `MetaServer` using `DatabaseConfig`, obtain a
client through `transport::embedded`, and close the server after its consumers
finish. The standalone process stops accepting requests on shutdown, waits for
accepted requests to finish, then closes its database connections. As with any
RPC, a lost response does not prove that a transaction was not committed;
retry the same `SnapshotRecord` or read the directory entry by ID.

The existing directory integration test uses SQLite by default. Set
`AENV_TEST_DATABASE_URL` to an **empty disposable PostgreSQL database** to run
the same contract there as well. It creates the schema and leaves its records
for the owner of that disposable database to clean up.
