# Hand off a legacy OSS snapshot catalog

Build this one-time tool from the same checkout as the destination AgentENV release.

```sh
cargo build --locked --release --manifest-path tools/snapshot-catalog-import/Cargo.toml
```

The tool imports ready Distributed Linux Firecracker snapshots from a legacy
OSS catalog. It preserves IDs, aliases, captured configuration and the original
artifact paths. It writes `artifacts/<id>/snapshot.json` and uses the normal
metadata registration transaction to populate the shadow directory and layer
graph. Template builds remain in the repository catalog.

Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN` for
temporary credentials, or set `SNAPSHOT_IMPORT_CREDENTIAL_PROCESS` to the credential
command used by the Node. Use the Node's endpoint, bucket, region and repository prefix.
Keep the local plan private. It caches source documents and audited object identities.

```bash
importer=(
  tools/snapshot-catalog-import/target/release/snapshot-catalog-import
  --endpoint "$OSS_ENDPOINT" --bucket "$OSS_BUCKET" --region "$OSS_REGION"
  --prefix "$OSS_PREFIX" --plan ./snapshot-import.json
)

# Point AENV_DATABASE_POSTGRES_URL at a new database kept away from runtime traffic.
aenv-metadata --backend postgres db migrate

# Optional read-only audit, followed by the online preload.
"${importer[@]}" prepare
"${importer[@]}" stage

# After the legacy write fence, while reads still go to OSS.
"${importer[@]}" apply --source-frozen
"${importer[@]}" verify --source-frozen
```

For SQLite, initialize the file with `aenv-metadata --sqlite-path PATH db migrate`
and pass `--sqlite-path PATH` to `stage`, `apply` and `verify`. Use a direct or
session-pooled PostgreSQL connection so the importer can hold its advisory lock.

## Online preload

1. Run `stage` while OSS still serves reads and writes. It initializes only an empty,
   migrated database and binds it to this repository. Give one importer exclusive
   use of the shadow database and snapshot record paths until handoff.
2. Re-run `stage` with the same plan as needed. The tool lists object metadata and
   reuses audited bytes for unchanged keys. Each newly referenced managed layer is
   downloaded once to check SHA-256 and read the OverlayBD UUID. The largest layer
   must fit in the temporary directory.
3. Check the counts for Local and deferred inventory items. A publication seen
   between its record and alias writes can be deferred while the rest is staged.
   Promote remaining Local snapshots on their owners before cutover. Unknown
   record formats and conflicting managed identities block import. Layers that
   point to this repository's managed prefix are tracked even when legacy records
   called them External. Other repository references retain their original URLs.

## Final handoff

1. Queue new directory writes and drain all legacy publishes, promotes and deletes
   for this bucket/prefix, including old owners, background jobs and operator
   tools. Keep reads on OSS. `--source-frozen` declares that deployment-side fence.
2. Run `apply` to refresh the final inventory. It keeps unchanged registrations,
   withdraws obsolete shadow rows and registers the delta. Old aliases become
   available for their final owners. It audits late layers if their bytes were not
   staged, and refuses conflicting snapshot records or changes to audited VM state.
3. Run `verify` to check the frozen inventory, SQL records and aliases, record
   digests, layer graph and heads. Success seals the importer marker. Switch reads
   to SQL, then release queued writes there. Check snapshot restores using the
   target Node's registry credentials and runtime dependencies before enabling GC.

An interrupted stage or apply can be rerun against the same unsealed shadow. Use
the deployment's time budget for the combined apply and verify. If it expires
before reads switch, stop the importer, keep OSS authoritative and release writes
there. Resume online staging before the next attempt. If verification already
sealed the shadow and cutover is abandoned, use a new shadow on the next attempt.

Only the shadow SQL rows are withdrawn. Original OSS objects are left in place
by the importer. Keep physical GC off until old artifact readers have drained or
their inputs are protected, and any other physical-delete authority for the prefix
has been fenced.

## Integration test

Create a disposable `snapshot-catalog-import-test` bucket on a local S3-compatible
server and set `IMPORT_TEST_S3_ENDPOINT` and the static credentials above. The
test uses a generated prefix, real sealed OverlayBD layers and a temporary SQLite
database. Set `IMPORT_TEST_POSTGRES_URL` to a disposable empty database to exercise
the same cutover on PostgreSQL.

```sh
cargo test --locked --manifest-path tools/snapshot-catalog-import/Cargo.toml \
  --test import -- --include-ignored
```
