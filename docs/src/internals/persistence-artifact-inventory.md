# Persistence Artifact Inventory

This document lists AgentENV artifacts that can remain on disk or in object storage beyond a single function call. It is organized by the module that owns each artifact's lifecycle.

## Path Roots

| Root | Default | Owner | Notes |
| --- | --- | --- | --- |
| `home_path` | `/var/lib/aenv` | `src/cfg.rs` | Base for paths containing the literal `$AENV_HOME` placeholder. `AENV_HOME_PATH` overrides it before placeholder expansion. |
| `runtime_path` | `/run/aenv` | `src/cfg.rs`, `src/sandbox/network/*` | Base for transient namespace mount points and daemon sockets. `AENV_RUNTIME_PATH` overrides it. |
| `deps_path` | `$AENV_HOME/deps` | `src/cfg.rs`, `src/setup/*` | Base for downloaded runtime dependencies. `AENV_DEPS_PATH` can place these rebuildable assets outside `home_path`. |
| Managed envd access-token seed | `$AENV_HOME/secrets/sandbox-access-token-hash-seed` | `src/sandbox/access.rs` | Node-local secret used when `[sandbox].access_token_hash_seed` is unset. It must be preserved with persisted secure sandboxes. |
| Firecracker sandbox work dirs | `$AENV_HOME/firecracker-work` with `agentenv-fc-` children | `src/sandbox/firecracker/*` | Per-sandbox runtime directories for sockets, symlinks, ublk runtime dirs, local logs, and writable OverlayBD upper layer data (`overlaybd/upper.data`, `overlaybd/upper.index`). An explicit `[firecracker].work_dir` overrides the root. |
| `firecracker.serial_dir` | `$AENV_HOME/logs/serial` | `src/sandbox/firecracker/*` | Durable Firecracker stdout/stderr root, grouped by sandbox ID. An explicit `[firecracker].serial_dir` overrides the root. |
| `managed_snapshot_root` | `<firecracker-work-base>/managed-snapshots` | `src/sandbox/firecracker/*` | In-process live snapshot artifact root used to keep captured snapshots alive until publish or drop. |
| `persisted_sandbox_store_path` | `$AENV_HOME/persisted-sandboxes` | `src/orchestrator/persistence/*` | Durable paused sandbox records and artifacts. |
| `snapshot_store` | `$AENV_HOME/snapshot-store` | `src/snapshot/repository/*` | Durable committed snapshot repository root. The configured backend uses `<snapshot_store>/repository`. Relative explicit paths are resolved against the config file directory. |
| `snapshot.local_cache_path` | `$AENV_HOME/snapshot-local-cache` | `src/snapshot/artifact_cache.rs`, runtime resolvers | Node-local cache for materialized runtime artifacts. Relative explicit paths are resolved against the config file directory. |
| `image.cache.root_dir` | `$AENV_HOME/image-cache` | `src/image/*`, overlaybd runtime | Node-local image cache root. Contains `configs/`, `indexes/`, `commits/`, and `remote-blocks/`. |
| `p2p.store_dir` | `$AENV_HOME/p2p/store` | `src/p2p/*`, `src/cfg.rs` | Local store for P2P artifact transport backends. Relative explicit paths are resolved against the config file directory. |
| `image.cache.remote_blocks` | `<image.cache.root_dir>/remote-blocks` | overlaybd runtime config | Remote block cache root. Overlaybd also stores `premerged-index/` under this cache dir. Its size limit comes from `image.cache.remote_blocks.max_size_gb`. |
| `ublk.daemon_socket_path` | `$AENV_RUNTIME/ublk-daemon.sock` | `src/sandbox/ublk/*`, `storage/ublk-daemon/*` | Unix socket used for server-to-daemon IPC. |
| `ublk.daemon_log_path` | `$AENV_HOME/logs/ublk-daemon.log` | `storage/ublk-daemon/*` | Daemon log file supplied during config normalization. An explicit path overrides the default. |

## Setup And Config

Owned by `src/setup/*` and `src/cfg.rs`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Firecracker binary | `<deps_path>/firecracker/{version}/firecracker` | Firecracker executable | VM process runtime | Created during setup when missing. Old versions are retained until manually removed. |
| CPU template helper | `<deps_path>/firecracker/{version}/cpu-template-helper` | Optional Firecracker helper executable | Detects host CPU config for cluster-wide CPU intersection | Extracted from Firecracker package when present. |
| Kernel image | `<deps_path>/kernel/{version}/vmlinux.bin` | Guest kernel | VM boot source | Downloaded during setup. Old versions are retained until manually removed. |
| Tools drive | `<deps_path>/tools/{version}/tools.ext4` | Read-only ext4 image with envd/tools | Firecracker root drive shared by sandboxes and pinned by snapshots through its immutable release version | Extracted from OCI or imported from `tools.drive_path` during setup. Old versions are retained until manually removed. |
| Overlaybd tools | `<deps_path>/overlaybd/bin/*`, `<deps_path>/overlaybd/lib/*` | `overlaybd-create`, `overlaybd-apply`, `overlaybd-commit`, `overlaybd-resize`, libraries | OCI-to-overlaybd conversion and packaging | Installed during setup when release metadata does not match. |
| Overlaybd release metadata | `<deps_path>/overlaybd/tools-release.json` | Installed overlaybd release identifier | Detects whether tools need reinstalling | Rewritten on setup when release changes. |
| Overlaybd package downloads | `<deps_path>/overlaybd/downloads/*` | Downloaded package archives | Setup cache for overlaybd release packages | Kept after install; no automatic GC. |
| Generated overlaybd config | `$AENV_HOME/overlaybd/overlaybd-global.json`, `$AENV_HOME/overlaybd/mem-overlaybd-global.json` | Runtime global config, cache path, credentials config | Configures overlaybd runtime and memory snapshot overlaybd access | Rewritten during setup/startup. |
| Managed envd access-token seed | `$AENV_HOME/secrets/sandbox-access-token-hash-seed` | 32 random bytes encoded as lowercase hexadecimal | Derives stable per-sandbox envd access tokens when no explicit seed is configured | Atomically created with mode `0600` during normal startup and reused thereafter. Must not be deleted while secure sandboxes are persisted. |
| Overlaybd runtime log | `<deps_path>/overlaybd/overlaybd.log` | Overlaybd runtime logs | Debugging | Appended by overlaybd runtime; no automatic GC. |

## Firecracker Sandbox

### Runtime

Owned by `src/sandbox/firecracker/*`.

| Artifact                     | Location                                                     | Contents                                                  | Purpose                                                      | Lifecycle                                                    | Rebuildable                                                  |
| ---------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Managed live snapshot root   | `<firecracker-work-base>/managed-snapshots/{sandbox_id}/{uuid}/...` | Firecracker snapshot artifacts kept alive in-process      | Holds captured running-sandbox snapshots until publish, and supports in-process pause state | Created by `FirecrackerSandbox::pause()` or `snapshot()`. Removed when `PersistentSnapshotRootGuard` drops. | No.                                                          |
| Snapshot VM state            | snapshot artifact dir `vm_state.bin`                         | Firecracker VM state                                      | Pause/resume and snapshot publish input                      | Created by Firecracker `create_snapshot`. Owner depends on caller: persister, managed root, or repository publish input. | No.                                                          |
| Raw memory diff              | snapshot artifact dir `mem.bin`                              | Sparse Firecracker diff memory dump                       | Input to memory overlaybd conversion and source for virtual memory size | Created by Firecracker `create_snapshot`. Removed best-effort after the memory overlaybd layer is committed. | Temporary input only; not reconstructed after cleanup. |
| Memory overlaybd layer       | snapshot artifact dir `mem_overlaybd/overlaybd.commit`       | Sealed overlaybd layer for memory pages                   | Runtime memory restore and publish input                     | Created by packaging `mem.bin`. Imported into repository managed layers during publish. | No, unless raw `mem.bin` is still present.                   |
| Memory image config          | snapshot artifact dir `mem_image.json`                       | Overlaybd image config stacking memory layers             | Resume paused sandbox and publish memory layers              | Written after memory conversion. Later repository resolvers regenerate runtime memory configs from committed layers. | Yes after publish; no for paused sandbox unless layers are known. |
| Rootfs snapshot config       | snapshot artifact dir `rootfs/image.json`                    | Overlaybd image config for captured rootfs state          | Resume paused sandbox and publish rootfs layers              | Staged from live runtime config after restack/seal.          | Yes only from associated layers and metadata.                |
| Rootfs snapshot layer        | snapshot artifact dir `rootfs/snapshot.commit`               | Sealed writable upper from live rootfs                    | Captures disk writes since previous lower stack              | Created by ublk daemon restack for writable overlaybd rootfs. | No.                                                          |
| Inherited runtime layers     | snapshot artifact dir `rootfs/inherited-layers/{index}/{source-file}` | Snapshot-owned hard links or copies of inherited runtime-created lower suffixes | Removes dependence on previous managed snapshot or persisted sandbox artifact roots | Created during pause when inherited lowers come from the managed snapshot root or another sandbox/generation under the same persisted `artifacts` root. | No.                                                          |
| Attached-drive snapshot dirs | snapshot artifact dir `drives/{drive_id}/...`                | Per-drive image config and snapshot layer                 | Captures writable attached-drive state                       | Created alongside rootfs snapshot for each drive.            | No.                                                          |
| Firecracker work dir         | configured work root, or consumed pool tempfile dir       | API socket, symlinks, runtime dirs, local logs            | Firecracker CWD for a sandbox                                | Created when sandbox handle is built, or moved from a warm pool entry; removed by owning `TempDir`. | Yes, except live state.                                      |
| Firecracker serial logs      | `firecracker.serial_dir/{sandbox_id}/*`, or warm pool work dir logs | Firecracker stdout/stderr                                 | Debugging                                                    | Opened on spawn and appended. Warm logs are relocated when a warm process is consumed. Configured serial output is not automatically GC'd. | No, but disposable.                                          |
| Firecracker logger output    | `firecracker.log` in the same per-sandbox log directory as the serial logs | Firecracker internal logger (`PUT /logger`)               | Debugging                                                    | Only created when `firecracker.log_level` is set to a non-empty level. Written by Firecracker itself; not automatically GC'd. | No, but disposable.                                          |

### Firecracker Pool

Owned by `src/sandbox/firecracker/pool.rs`.

| Artifact           | Location                                                     | Contents                                               | Purpose                             | Lifecycle                                                    |
| ------------------ | ------------------------------------------------------------ | ------------------------------------------------------ | ----------------------------------- | ------------------------------------------------------------ |
| Warm pool work dir | system tempfile dir | Firecracker socket and warm process logs               | Pre-spawned Firecracker process CWD | Created by pool maintenance as a `TempDir`. Removed when warm entry is cleaned up, or moved into the consuming sandbox and later dropped there. |
| Warm pool logs     | warm pool work dir `firecracker-stdout.log`, `firecracker-stderr.log` | Firecracker output before the warm process is consumed | Debugging warm startup              | Created on warm spawn. Relocated into sandbox log path when consumed if no explicit stdout/stderr override. |

## Extra Drives

Owned by `src/sandbox/extra_drive.rs` and Firecracker snapshot code.

| Artifact                      | Location                                           | Contents                                         | Purpose                                                      | Lifecycle                                                    | Rebuildable                               |
| ----------------------------- | -------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------- |
| Extra-drive runtime dir       | sandbox work dir `extra-drive-runtime-{drive_id}/` | Runtime `image.json`, upper files, result file   | ublk runtime for attached drive                              | Created when preparing extra drives. Released on rollback/stop; work dir cleanup removes files. | Not while running; otherwise unnecessary. |
| Extra-drive symlink           | sandbox work dir `extra-drive-{drive_id}`          | Symlink to `/dev/ublkbN` device path             | Firecracker drive attachment path                            | Created after ublk runtime device creation. Removed on rollback or work dir cleanup. | Yes.                                      |
| Extra-drive snapshot artifact | snapshot artifact dir `drives/{drive_id}/...`      | Captured drive overlaybd config and commit layer | Preserve attached-drive writable state across pause/resume/publish | Created during sandbox snapshot/pause. Later owned by persister, managed root, or repository publish flow. | No.                                       |

## Image Resolver

Owned by `src/image/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Image config cache | `<image.cache.root_dir>/configs/*-image.json` | Digest-qualified overlaybd image configs for resolved user images | Avoids repeating OCI manifest classification and config generation | Written on image resolve. Regenerated if invalid or if referenced local lowers are missing. No unified GC today. |
| Image metadata sidecar | `<image.cache.root_dir>/configs/*.metadata.json` | Base env/workdir metadata from OCI image config | Preserves image launch context beside cached image config | Written after image config. Rebuilt if missing while image config is usable. |
| Overlaybd commit cache | `<image.cache.root_dir>/commits/{digest-slug}/overlaybd.commit` | Content-addressed overlaybd commit layers from standard OCI conversion, and target dirs for remote overlaybd-native layers | Node-local reusable layer store for user image layers | Standard OCI conversion writes commits. Remote overlaybd-native configs point `dir` here for runtime population. No unified GC today. |
| OCI conversion index | `<image.cache.root_dir>/indexes/{source-digest}/...json` | Mapping from OCI source layer/context to overlaybd commit digest and size | Skips repeated layer conversion when converted commits exist | Written after successful standard OCI conversion. No unified GC today. |
| Temporary OCI pull/conversion work | process temp dir | OCI layout and per-layer conversion workspace | Intermediate input for standard OCI conversion | Owned by `TempDir`; removed after conversion scope exits. |

## Snapshot Repository

Owned by `src/snapshot/repository/*` and `src/snapshot/types/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Snapshot records | POSIX: `<snapshot_store>/repository/catalog/records/{id}.json`; OSS: `catalog/records/{id}.json` | Snapshot ID, alias, source, resources, build status, committed logical metadata | Durable user-visible snapshot/template metadata | Created before template build or at publish. Updated when committed or errored. Deleted by snapshot delete. | No. |
| Snapshot aliases | POSIX: `<snapshot_store>/repository/catalog/aliases/{alias}`; OSS: `catalog/aliases/{alias}.json` | Alias-to-snapshot binding | Name lookup for templates/snapshots | Bound during create/publish with conflict checks. Deleted with record cleanup. | Partially, from records if aliases are still recorded. |
| Firecracker manifest | POSIX: `<snapshot_store>/repository/snapshots/{id}/firecracker-manifest.json`; OSS: `artifacts/{id}/firecracker-manifest.json` | Firecracker snapshot shape, virtual sizes, attached-drive metadata; path fields are hydrated at runtime | Runtime manifest template for launching committed snapshots | Persisted during publish. Removed with per-snapshot artifacts. | Partially, but kept as durable artifact. |
| VM state | POSIX: `<snapshot_store>/repository/snapshots/{id}/vm_state.bin`; OSS: `artifacts/{id}/vm_state.bin` | Firecracker VM state snapshot | Required to resume committed snapshots | Copied/uploaded during publish. Removed with per-snapshot artifacts. | No. |
| Managed snapshot layers | POSIX: `<snapshot_store>/repository/managed-layers/{digest}.overlaybd.commit`; OSS: `managed-layers/{digest}` | Content-addressed rootfs, attached-drive, and memory overlaybd commit layers | Shared immutable layer storage for committed snapshots | Imported during publish by descriptor or by hashing local descriptor-less layers. Usually not removed with a single snapshot. | Not from metadata alone; can be re-fetched only if source still exists. |
| Source-registry publications | `SnapshotRecord.committed.disk_publications` plus remote OCI registry objects | Published rootfs/attached-drive image refs and manifest digests | Lets compatible snapshot deltas live in their source registry | Created during OSS publish when image publishing is enabled. Rolled back on failure or delete when possible. | Registry-owned; record is required to locate. |

Repository records are the logical source of truth for committed snapshots. Build-time and runtime `image.json` files should not become committed truth unless they contain data that cannot be derived from committed metadata.

## Snapshot Runtime Resolution

Owned by `src/snapshot/artifact_cache.rs`, `src/snapshot/runtime_support.rs`, and backend runtime resolvers.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Runtime rootfs config | `<snapshot_local_cache_path>/runtime/{snapshot_id}/rootfs/image.json` | Node-local overlaybd config for committed rootfs layers | Launch committed snapshot on the current node | Materialized during snapshot resolve and pinned by `RunnableSnapshot` lease. LRU-evictable after unpinned. | Yes. |
| Runtime memory config | `<snapshot_local_cache_path>/runtime/{snapshot_id}/memory/image.json` | Node-local overlaybd config for committed memory layers | Provides memory backend image to Firecracker resume | Materialized during snapshot resolve and pinned by lease. LRU-evictable after unpinned. | Yes. |
| Runtime attached-drive configs | `<snapshot_local_cache_path>/runtime/{snapshot_id}/drives/{drive_id}/image.json` | Node-local overlaybd configs for attached drives | Launch committed snapshot with attached drives | Materialized during snapshot resolve and pinned by lease. LRU-evictable after unpinned. | Yes. |
| OSS cached VM state | `<snapshot_local_cache_path>/artifacts/{id}/vm_state.bin` | Downloaded VM state object | Avoids repeated object-store download while pinned/cached | Downloaded by OSS resolver through `LocalArtifactCache`. LRU-evictable after unpinned. | Yes, by downloading again. |
| POSIX VM state reference | Repository `vm_state.bin` path | Direct path into POSIX repository | Avoids copying VM state into local runtime cache | Checked during resolve. Lifetime follows repository artifact. | No; repository artifact is source. |

`LocalArtifactCache` owns pinning, in-flight materialization deduplication, and LRU eviction for files it manages. It does not own the durable snapshot repository.

## Orchestrator Persistence

Owned by `src/orchestrator/persistence/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Paused sandbox record DB | `<persisted_sandbox_store_path>/records.db` | RocksDB keyed by sandbox ID; values are compact JSON records containing version, lifecycle, sandbox metadata, artifact root, and backend state | Restores paused sandboxes after server restart | Record written after pause succeeds. Marked `resuming` before resume. Rolled back on resume failure. Deleted on resume/delete. |
| Paused sandbox artifact generation | `<persisted_sandbox_store_path>/artifacts/{sandbox_id}/{uuid}/...` | Firecracker snapshot artifacts generated by `pause_to_dir` | Durable artifacts for one paused sandbox generation | Allocated before pause, populated by backend, referenced by paused record. Removed during explicit delete or `load_all` orphan cleanup. |

Paused sandbox artifacts are not committed snapshot repository artifacts. They are owned by the sandbox persister and should not be shared as snapshot truth.

## P2P Artifact Transport

Owned by `src/p2p/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Iroh blob store | `<p2p.store_dir>/iroh` | `iroh-blobs` content-addressed data and internal metadata | Local store for published artifacts and cached fetches | Created on P2P transport init, grows on publish and successful remote fetch. These blobs are collected by GC after unpublish removes those tags. |
| P2P catalog DB | `<p2p.store_dir>/iroh/catalog.db` | RocksDB keyed by artifact key; values are compact JSON artifact descriptors | Lets peers resolve stable keys into descriptors for this node | Loaded on startup. Individual entries are upserted on publish and after successful remote fetch, and deleted on unpublish. |

Snapshot publication writes best-effort P2P catalog entries after repository commit:

- `snapshot/v1/artifacts/{snapshot_id}/vm_state.bin`
- `snapshot/v1/artifacts/{snapshot_id}/firecracker-manifest.json`
- `overlaybd-layer/v1/sha256:<digest>` for referenced rootfs, memory, and attached-drive overlaybd commit layers

Those P2P entries are optional copies, not committed snapshot truth. Clearing `<p2p.store_dir>` can reduce peer-to-peer availability and force peers back to OSS or origin-registry reads, but it must not make a committed snapshot invalid by itself.

## Ublk Daemon Runtime

Owned by `storage/ublk-daemon/*` and `src/sandbox/ublk/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Overlaybd runtime image config | caller-provided runtime dir `image.json` | Rewritten overlaybd config with runtime-relative lower and upper paths | Opens ublk overlaybd target | Materialized during `CreateOverlaybdRuntimeDevice`. Removed during rollback or work dir cleanup. | Yes from source image config while not running. |
| Runtime upper data | runtime dir `upper.data` | Writable overlaybd upper data file | Stores live writes for writable devices | Created for writable runtime if source config has no existing upper. Restacked into snapshot layer on pause/snapshot. | No while running. |
| Runtime upper index | runtime dir `upper.index` | Log-structured upper index | Resolves writes in `upper.data` | Created with log-structured writable upper. Restacked into sealed snapshot layer. | No while running. |
| Runtime result file | runtime dir `result.txt` | Overlaybd result file path | Overlaybd runtime convention | Created/used by overlaybd runtime. Removed during cleanup. | Yes. |
| Ublk daemon socket | default `$AENV_RUNTIME/ublk-daemon.sock` | Unix socket | IPC between AgentENV server and ublk daemon | Created when daemon starts; removed/replaced by process lifecycle. | Yes. |
| Ublk daemon log | configured `ublk.daemon_log_path`, default `$AENV_HOME/logs/ublk-daemon.log` | Daemon logs | Debugging | Appended by the daemon; the deployment owns rotation and retention. | No, but disposable. |

## Overlaybd Storage

Owned by `storage/overlaybd/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Remote block cache | configured `cacheConfig.cacheDir`, derived from `<image.cache.root_dir>/remote-blocks` | Cached registryfs_v2 block ranges | Speeds remote overlaybd-native layer reads | Managed by overlaybd cache settings. | Yes. |
| Premerged index cache | `cacheConfig.cacheDir/premerged-index/*.pmidx` | Serialized merged read-only lower index | Speeds opening repeated lower stacks | Written asynchronously on read-only open. Pruned by size limit derived from cache size. | Yes. |
| Sealed overlaybd commit files | Various owner paths: image cache, snapshot dirs, repository managed layers | Overlaybd layer data and index trailer | Immutable lower layers for block devices and memory images | Lifecycle is owned by the module that stores the file. Overlaybd only defines the format and open/merge behavior. | Depends on owner. |

## Ownership Rules

- Snapshot repository artifacts are durable user-visible state. Do not delete them from node-local GC code.
- Paused sandbox artifacts are durable only for the paused sandbox that owns them. Do not treat them as committed snapshots.
- Runtime `image.json` files under snapshot local cache or ublk runtime dirs are derived artifacts. They should be rebuildable from committed metadata or source image configs.
- Node-local `image-cache` artifacts are disposable cache, but content-addressed commits may be expensive to regenerate. The metadata-backed GC only reclaims commits no longer rooted by on-disk source configs, held by image-cache leases, or referenced by the in-process running set. Committed snapshots are durable SnapshotRepository state and do not pin ImageCache commits. See `[image.cache.gc]` in the configuration reference for scheduling.
- P2P store contents are node-local and optional. Unpublish removes the local catalog entry and related blobs; clearing the store only affects peer-to-peer sharing and may require republishing or refetching artifacts.
- Logs and dependency downloads are operational artifacts. They are not part of sandbox or snapshot correctness, but may require separate retention policy outside image-cache GC.
