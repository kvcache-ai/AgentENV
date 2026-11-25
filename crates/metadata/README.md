# Metadata database administration

Use `aenv-metadata db migrate` to apply the bundled SQLite or PostgreSQL schema.
`db status` and `db check` inspect migration history without creating a database
or applying changes. Run one SQLite migrator at a time.

After deployment, add forward-only migrations. SQLx validates the versions and
checksums of applied migrations.
