# Metadata migrations

The metadata crate embeds one initial migration for PostgreSQL and one for
SQLite. Each creates the first-release `snapshots`, `layers`, `snapshot_heads`
and `snapshot_pins` schema. Snapshot rows contain directory metadata and an
internal canonical digest of the published payload; the full Distributed content
stays in the artifact repository at a deterministic path keyed by snapshot ID.
Layers store the UUID prefix forest, and heads retain its chain tips. Pins
reserve the schema for runtime-use protection in the next stage.

There is no `reclaim_after` field or time-based retention setting. Schema
creation is not split by implementation stage; pin/release, retirement and
physical collection operations are introduced separately.

This initial migration replaces the unmerged development-only single-table
draft. Disposable test databases created from that draft must be recreated;
its SQLx checksum is intentionally not accepted as this schema. Do not bypass
history validation or rewrite checksums in a database whose data must be kept.

Apply bundled migrations with `aenv-metadata db migrate`. This initializes
SQLx's migration history and applies the four-table schema.
`aenv-metadata db status` and `aenv-metadata db check` inspect history without
creating a database or applying changes. A missing SQLite file has the initial
migration pending, so `check` fails until `migrate` succeeds. PostgreSQL requires
an existing database. The directory service checks migration history at startup
and never migrates implicitly.

After deployment, add forward-only files named `<version>_<description>.sql`
without changing applied files. SQLx records migration history and checksums.
Run one SQLite migrator at a time.
