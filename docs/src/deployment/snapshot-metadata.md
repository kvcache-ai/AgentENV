# Snapshot Metadata & Multi-Node Storage

Starting in v0.3.0, all durable catalog metadata lives in a unified SQL metadata
database (`snapshots`, `volumes`, `images`, `image_tags`, `build_jobs`, `layers`,
`layer_heads`, and `snapshot_pins`), while the artifact repository stores immutable
blobs (`snapshot.json`, `vm_state.bin`, `firecracker-manifest.json`, optional
`memory-startup.pack`, build logs, and content-addressed layers under
`managed-layers/`).

Every runtime node in a multi-node cluster must share **both**:

1. **One shared metadata database** (direct PostgreSQL or a standalone
   `aenv-metadata serve` service), and
2. **One shared artifact repository** (a shared POSIX filesystem or the same
   OSS bucket and prefix).

If nodes use their local default SQLite file, each node sees only the snapshots,
templates, volumes, and published images created locally even when all nodes
point at the same POSIX or OSS artifact store.

## Deployment Modes

Configure `[database]` in `config/default.toml` (or the corresponding
`AENV_DATABASE_*` / `AENV_META_ADDRESS` environment variables). The three modes
are mutually exclusive—mixing `meta_address` with PostgreSQL or custom SQLite
settings, or setting `postgres_url` when `backend = "sqlite"`, fails startup
validation immediately.

| Topology | Configuration | Migration owner | Typical use |
| --- | --- | --- | --- |
| **Single-node SQLite (default)** | `backend = "sqlite"`, `sqlite_path = "$AENV_HOME/metadata.sqlite3"` | Node initializes and migrates on startup | Single-host installs, local development (`local-dev` overlay) |
| **Multi-node direct PostgreSQL** | `backend = "postgres"`, `postgres_url = "postgresql://..."` | Explicit migration step (`aenv-metadata db migrate` or `server --migrate-database`) before nodes start | Docker Compose, Kubernetes `default` / `ha` (external PostgreSQL), Kubernetes `postgres` overlay |
| **Multi-node standalone Metadata service** | Nodes set `meta_address = "agentenv-metadata:9230"`; `aenv-metadata serve` owns `postgres_url` | `aenv-metadata --backend postgres db migrate` before starting `aenv-metadata serve` | Clusters that centralize PostgreSQL credentials and connection pooling (`metadata` overlay) |

### Docker Compose

`deploy/docker-compose.yml` starts a `postgres:17.4` container, runs
`aenv-metadata db migrate` once before starting `agentenv-a` and `agentenv-b`,
and points both nodes directly at the shared PostgreSQL database and shared
POSIX `agentenv-snapshot-store` volume.

### Kubernetes

1. **External PostgreSQL (default `make k8s-apply` and `K8S_OVERLAY=ha`)**:
   Configure a shared POSIX or OSS repository in `config/default.toml` (or via
   a Kustomize patch) and pass your PostgreSQL connection URL:

   ```bash
   AENV_DATABASE_POSTGRES_URL="postgresql://agentenv:secret@db.example.internal:5432/agentenv" \
     make k8s-apply
   ```

   On subsequent applies, `deploy/k8s/run.sh` reuses the existing
   `Secret/agentenv-database` if `AENV_DATABASE_POSTGRES_URL` is omitted. Each
   runtime Pod's `database-migrate` initContainer runs `/server --migrate-database`
   (protected by PostgreSQL advisory locks) before starting the server.

2. **Optional in-cluster PostgreSQL overlay (`K8S_OVERLAY=postgres`)**:
   Provisions a single-replica `postgres:17.4` StatefulSet backed by a
   PersistentVolumeClaim (`10Gi` `ReadWriteOnce`) and wires runtime nodes
   directly to `postgres:5432`:

   ```bash
   K8S_OVERLAY=postgres bash deploy/k8s/run.sh apply
   ```

3. **Optional standalone Metadata service overlay (`K8S_OVERLAY=metadata`)**:
   Runs the in-cluster PostgreSQL StatefulSet plus an `agentenv-metadata`
   Deployment and ClusterIP Service (`agentenv-metadata:9230`). Only
   `agentenv-metadata` holds `AENV_DATABASE_POSTGRES_URL` and runs schema
   migrations; runtime Pods connect over TCP via `AENV_META_ADDRESS`:

   ```bash
   K8S_OVERLAY=metadata bash deploy/k8s/run.sh apply
   ```

   > When running `aenv-snapshot gc` through a standalone `agentenv-metadata`
   > service backed by a POSIX repository, mount the shared POSIX snapshot store
   > into `Deployment/agentenv-metadata` at the same path configured in
   > `agentenv.toml` (OSS repositories need only credentials/config).

4. **Local single-node k3s development (`K8S_OVERLAY=local-dev`)**:
   Uses node-local SQLite so single-node development does not require a
   PostgreSQL instance. Do not use `local-dev` for multi-node sharing.

## Schema Migrations vs. Legacy Repository Import

Database schema migrations and offline legacy catalog import are **two distinct
steps**:

### 1. Database Schema Migrations (DDL)

Schema migrations create or advance the SQL tables (`snapshots`, `volumes`,
`images`, `image_tags`, `build_jobs`, `layers`, `layer_heads`, `snapshot_pins`) and
validate migration checksums. Nodes connected to PostgreSQL or a remote
`aenv-metadata` service never run DDL automatically during normal startup.

```bash
# Apply forward migrations to PostgreSQL
aenv-metadata --backend postgres --postgres-url "$AENV_DATABASE_POSTGRES_URL" db migrate

# Verify migration history without mutating the database
aenv-metadata --backend postgres --postgres-url "$AENV_DATABASE_POSTGRES_URL" db check
```

Equivalent node-config wrapper (honors `AENV_CONFIG_PATH` and `AENV_DATABASE_*`):

```bash
server --migrate-database
```

### 2. Legacy Repository Import (Upgrading Pre-0.3.0 POSIX or OSS Repositories)

v0.3.0 runtime nodes do **not** read pre-0.3.0 file-catalog documents
(`catalog/records/`, `volumes/records/`, `template-build/cache-head.json`,
`template-build/builder/`, `images/descriptions/`, `images/tags/`, and
`images/builds/`) or pre-0.3.0 paused-sandbox state (`persisted-sandboxes/`).

Before stopping pre-0.3.0 nodes for upgrade:

1. **Drain paused sandboxes**: resume or delete any paused sandboxes on old
   nodes (or snapshot them first if their state must survive the upgrade).
   v0.3.0 stores paused sandboxes under `$AENV_HOME/sandboxes` with sparse
   memory/rootfs uppers and does not read legacy paused state.
2. **Apply database schema migrations** (step 1 above).
3. **Run `aenv-snapshot import` offline** against the shared repository before
   starting upgraded nodes:

```bash
# 1. Validate legacy documents, layer SHA-256 digests, and SQL state without writing
aenv-snapshot import --all --dry-run

# 2. Import all legacy snapshots, ready templates, volumes, published images,
#    image tags, and imageOnly build jobs (add --skip-local to skip old Local snapshots;
#    BuildKit caches and builder templates are recreated automatically on demand)
aenv-snapshot import --all --skip-local
```

To import only specific Distributed snapshot IDs instead of scanning the full
repository, pass snapshot IDs as positional arguments or pipe them on stdin:

```bash
aenv-snapshot import <snapshot-id>...
```

Imported resources preserve content-addressed layer deduplication under
`managed-layers/` and register every live resource head in `layer_heads` so
universal garbage collection protects shared layers across snapshots, templates,
volumes, and published images.

## Garbage Collection & Pin Reconciliation

- **SQL `snapshot_pins` table** prevents `aenv-snapshot gc` from reclaiming retired
  snapshots and managed layers still referenced by running or paused sandboxes
  across the cluster.
- **Node-local `<snapshot.local_root>/pins.db/` (RocksDB)** records which SQL
  pins belong to each node so restarted nodes can reconcile and release orphaned
  pins after crashes.
- Run garbage collection on demand or on a schedule against the shared database
  and repository:

  ```bash
  aenv-snapshot gc --limit 64
  ```

## Multi-Node Verification Checklist

After deploying two or more runtime nodes against a shared PostgreSQL database
and shared POSIX/OSS repository:

1. Create a sandbox on **Node A** and publish a Distributed snapshot with an
   alias (e.g. `aenv snapshot create <sandbox-id> --name cross-node-check`).
2. Query the snapshot by alias on **Node B** (`GET /snapshots/cross-node-check`)
   and confirm the returned snapshot ID matches Node A.
3. Start a new sandbox from `cross-node-check` on **Node B** and confirm the
   VM restores cleanly from the shared repository and writes a pin row into
   `snapshot_pins`.
