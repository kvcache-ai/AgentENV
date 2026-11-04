# Metadata migrations

The metadata crate embeds separate PostgreSQL and SQLite migration bundles.
This database foundation intentionally starts with empty bundles: snapshot
directory tables and their storage semantics are introduced with the directory
service that consumes them.

Apply bundled migrations with `aenv-metadata db migrate`. This initializes
SQLx's migration history even when no business migration is present.
`aenv-metadata db status` and `aenv-metadata db check` inspect history without
creating a database or applying changes. An empty migration bundle has no
pending migrations and therefore does not require a history table.

After deployment, add forward-only files named `<version>_<description>.sql`
without changing applied files. SQLx records migration history and checksums.
Run one SQLite migrator at a time.
