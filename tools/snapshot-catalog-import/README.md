# Import selected legacy OSS snapshots

This standalone tool adds selected Distributed Linux Firecracker snapshots to an existing,
migrated AgentENV database. Run it from the same checkout as the destination release.
It preserves the legacy catalog, aliases and runtime artifacts. It writes
`artifacts/<id>/snapshot.json` with only `config` and `manifest`, then registers the
snapshot metadata and managed-layer relationships through the normal directory API.

```sh
cargo build --locked --release --manifest-path tools/snapshot-catalog-import/Cargo.toml
```

Use the Node's OSS endpoint, bucket, region and repository prefix. Set
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN` for temporary
credentials, or set `SNAPSHOT_IMPORT_CREDENTIAL_PROCESS` to the Node's credential command.
Before importing, initialize PostgreSQL with `aenv-metadata --backend postgres db migrate`
or SQLite with `aenv-metadata --sqlite-path PATH db migrate`.

```bash
# Set AENV_DATABASE_POSTGRES_URL to the target database.
tools/snapshot-catalog-import/target/release/snapshot-catalog-import \
  --endpoint "$OSS_ENDPOINT" --bucket "$OSS_BUCKET" --region "$OSS_REGION" \
  --prefix "$OSS_PREFIX" \
  --snapshot-id 019a0000-0000-7000-8000-000000000001 \
  --snapshot-id 019a0000-0000-7000-8000-000000000002
```

For SQLite, pass `--sqlite-path PATH` instead of a PostgreSQL URL. At least one
`--snapshot-id` is required; repeated IDs are deduplicated. Supply the IDs that
need to remain available, including any selected through business pin or tag records.
The tool does not discover those records or create SQL pins on their behalf.

For each ID, the tool reads its legacy record, the backend manifest and
VM-state object metadata. It checks only the referenced managed layers, downloading
each distinct layer once per run to verify its digest and read its OverlayBD UUID.
The largest layer must fit in the temporary directory. Legacy External references
pointing at this repository's managed prefix become Managed; other references keep
their original URLs. Existing UUIDs and sizes must agree with the actual objects.
The SQL directory identifies legacy layers with a nil physical UUID by digest.
Imports from `managed-layers` retain that location outside the snapshot collector.
Local snapshots must be promoted on their owners before they can be imported.

Existing target records and pins are left in place. An existing selected ID is reused
only when its directory metadata and content match. Retired IDs, alias conflicts,
conflicting content and unavailable managed layers return errors. The tool does not
withdraw other records, overwrite conflicting content or restore GC state. A failed
batch retains its completed registrations; rerun the same explicit IDs to continue.
There is no full-directory scan, shadow database, source-freeze step or cutover action.
Coordinate the selected snapshots' lifecycle with the import, and preserve the existing
protection of artifact users before enabling physical GC.

## Integration test

Create a disposable `snapshot-catalog-import-test` bucket on a local S3-compatible
server and set `IMPORT_TEST_S3_ENDPOINT` and the static credentials above. The existing
test uses a generated prefix, real sealed OverlayBD layers and a temporary SQLite
database. Set `IMPORT_TEST_POSTGRES_URL` to a disposable empty database to run the same
selected-import checks on PostgreSQL; the fixture seeds existing records and a pin.

```sh
cargo test --locked --manifest-path tools/snapshot-catalog-import/Cargo.toml \
  --test import -- --include-ignored
```
