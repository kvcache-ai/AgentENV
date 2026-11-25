# Snapshot directory

The directory records snapshot identity, placement, alias, managed layer ancestry
and snapshot heads in SQLite or PostgreSQL. Registration and Local promotion
commit layer references with the snapshot. Retiring a snapshot releases its
alias and retains its identity so delayed requests cannot recreate it.

Use `aenv-metadata db migrate` to apply the bundled schema before starting the
service. `db status` and `db check` inspect migration history without applying
changes. SQLx validates the checksums of applied migrations. Run one SQLite
migrator at a time.

Clients can use an in-process directory or connect to `aenv-metadata serve`
over TCP. Both transports use the same directory operations.
