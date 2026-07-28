# Template Builder and Testing

This document explains how the current template-facing build API maps onto the
snapshot-first internals, how committed snapshots move through the
repository/runtime boundary, and what the relevant tests validate.

## 1) Public Surface vs Internal Model

### User-facing builder API

- `TemplateBuildSpec` (`src/template/build_spec.rs`)
  - Describes a declarative build request.
  - Builder helpers include:
    - `from_existing_rootfs(path)`
    - `from_overlaybd_configs(global_config_path, image_config_path)`
    - `run(cmd)`
    - `env(key, value)`
    - `workdir(path)`
    - `apt(packages)`
    - `alias(alias)`
    - `resources(cpu_count, memory_mib)`
  - The current builder implementation accepts overlaybd-backed fresh builds and
    rebuilds from committed snapshots. Fresh ext4-rootfs builds are not wired
    through `TemplateBuilder`.

- `TemplateBuilder` (`src/template/builder.rs`)
  - Preserves the external template semantics for build / rebuild flows, while
    committed snapshot lifecycle operations live in `SnapshotManager`.
  - Main methods:
    - `new()`
    - `with_local_store_root(path)`
    - `build_and_publish(snapshot_manager, config).await`
    - `rebuild_and_publish(snapshot_manager, config, snapshot_id, base_snapshot).await`

### Snapshot-first internals

- `StoredSnapshot` / `RunnableSnapshot` (`src/snapshot/types/`)
  - `StoredSnapshot` is the durable committed manifest state.
  - It stores snapshot metadata plus logical rootfs / attached-drive layer
    descriptions and memory layers.
  - Snapshot publication also persists `firecracker-manifest.json` for
    launch-time metadata such as rootfs/memory virtual size and attached-drive
    `read_only` flags.
  - It does not store fixed local artifact locations such as `mem_image.json`
    or runtime-derived `rootfs/image.json`.
  - `RunnableSnapshot` is a node-local resolved view with concrete artifact
    paths and runtime-ready overlaybd image configs.

- `SnapshotRepository` / `SnapshotRuntimeResolver`
  (`src/snapshot/repository/interfaces.rs`)
  - `SnapshotRepository` owns committed durable state.
  - `SnapshotRuntimeResolver` turns committed state into node-local runnable
    paths.

## 2) Build and Publish Flow

`TemplateBuilder::build_and_publish(...)` does the following:

1. Prepare the build base from either:
   - a committed snapshot (`rebuild_and_publish`)
   - overlaybd configs (`from_overlaybd_configs`)
2. Start a temporary Firecracker sandbox with the requested CPU/memory.
3. Execute template steps in order.
4. Probe `envd`, kernel, and firecracker versions from the running guest and executable.
5. Pause the sandbox and export a `FirecrackerSnapshotManifest` carrying metadata for repository publication
6. Publish those local artifacts into the configured snapshot repository.

The important boundary is:

- the builder API describes what to build
- local build artifacts are manager-owned temporary outputs
- committed snapshot records store logical durable state
- backend layout conventions determine where committed files and shared layers live
- runtime resolution turns committed snapshot state into node-local runnable paths

## 3) Repository Layout

The default backend is the POSIX filesystem backend. Given a backend root like
`/path/to/store/repository`:

- committed snapshot manifest:
  - `/path/to/store/repository/snapshots/<id>/snapshot.json`
- committed fixed-layout files:
  - `/path/to/store/repository/snapshots/<id>/vm_state.bin`
  - `/path/to/store/repository/snapshots/<id>/firecracker-manifest.json`
- shared managed layers:
  - `/path/to/store/repository/managed-layers/<digest>.overlaybd.commit`
- alias bindings:
  - `/path/to/store/repository/catalog/aliases/<alias>`
  - `/path/to/store/repository/catalog/aliases/<alias>.lock`

Alias locking is per-alias, not a single global `aliases.lock` file.

`snapshot.json` intentionally does not repeat local build-artifact paths such
as `mem_image.json` or `rootfs/image.json`. Runtime code derives node-local
image configs from:

- repository root
- snapshot id
- attached drive id
- managed layer digest

`firecracker-manifest.json` complements this by persisting launch-time virtual
size and attached-drive mode metadata without embedding node-local runtime
paths.

## 4) Load and Launch Flow

Typical runtime usage is:

1. `SnapshotManager::load_committed(id_or_alias).await`
2. `SnapshotManager::resolve_runnable(stored).await`
3. `FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())`
4. `sandbox.start().await`

If you want the two snapshot-manager calls combined, use
`SnapshotManager::load_runnable(id_or_alias).await`.

`resolve_runnable(...)` is where backend-neutral committed state becomes
node-local launch inputs:

- `memory_layers` -> runtime `memory/image.json`
- `rootfs.layers` -> runtime `rootfs/image.json`
- `attached_drives[].layers` -> runtime `drives/<id>/image.json`
- `attached_drives[].read_only` -> runtime mount mode for `drives/<id>`
- `firecracker-manifest.json` -> runtime manifest hydrated with node-local
  artifact paths
- committed `vm_state.bin` -> runnable vm-state path

## 5) Minimal Example

```rust
use agentenv::cfg::ConfigManager;
use agentenv::image::ImageResolver;
use agentenv::sandbox::{FirecrackerSandbox, SandboxExecutor, SandboxLaunchConfig};
use agentenv::snapshot::SnapshotManager;
use agentenv::template::{TemplateBuildSpec, TemplateBuilder};

let builder = TemplateBuilder::new();
let snapshot_manager = SnapshotManager::new()?;
let config = ConfigManager::global()?.config();
let image_resolver = ImageResolver::new(config);
let image_config = image_resolver
  .resolve(image_resolver.default_image())
  .await?
  .overlaybd_config_path;

let alias = "my-template-v1";

builder
    .build_and_publish(
        &snapshot_manager,
        TemplateBuildSpec::new()
            .from_overlaybd_config(image_config)
            .alias(alias)
            .resources(1, 128)
            .run("mkdir -p /workspace")
            .workdir("/workspace")
            .env("MARK", "ready")
            .run("printf '%s' \"$MARK\" > mark.txt"),
    )
    .await?;

let runnable = snapshot_manager
    .load_runnable(alias)
    .await?
    .expect("template should exist");

let mut sandbox =
    FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())?;
sandbox.start().await?;

let out = sandbox.run_command("cat", &["/workspace/mark.txt"]).await?;
assert_eq!(out.exit_code, 0);
assert_eq!(out.stdout.trim(), "ready");

sandbox.stop().await?;
```

## 6) State Transition

The template-facing state transition is:

1. local build artifacts
2. committed `snapshot.json` + `firecracker-manifest.json` + fixed files + managed layers
3. node-local runtime-derived image configs
4. Firecracker resume inputs

## 7) What the Tests Validate

`tests/integration/snapshot.rs` covers:

- building and publishing from overlaybd configs
- loading committed snapshots by alias / id through the snapshot manager
- resolving committed snapshots into runnable snapshots
- launching Firecracker sandboxes from resolved snapshots
- rebuilding from committed snapshots while preserving base state
- listing and deleting committed snapshots through the template API surface

`tests/integration/snapshot_attached_drive.rs` covers overlaybd attached-drive handling
across sandbox capture, publish, resolve, launch, and rebuild flows, including:

- committed metadata preserving `readOnly`
- runtime mount mode for readonly and writable drives
- rebuild keeping attached-drive metadata stable

`crates/e2e-tests/tests/snapshot_oss_e2e_test.rs` covers OSS-backed snapshot publication,
resolution, alias cleanup, missing managed-layer failure modes, and deletion.

Repository/backend unit tests under `src/snapshot/repository/backends/` cover:

- artifact import
- alias handling
- committed snapshot deletion
- runtime cache behavior

## 8) Common Failure Areas

- overlaybd base config paths are missing or invalid
- `ublk` is disabled while using overlaybd build bases
- Linux host prerequisites for Firecracker, the selected KVM/PVM mode, or
  network namespaces are missing
- repository alias conflicts during publish
- runtime resolver cannot materialize local paths from committed artifacts
