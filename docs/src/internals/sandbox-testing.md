# Sandbox and Test Flow Guide

This document describes the public sandbox API, the global `ConfigManager` under
`AgentENV/src/cfg.rs`, and what `AgentENV/tests/integration/fc.rs`,
`AgentENV/tests/integration/process.rs`, `AgentENV/tests/integration/snapshot.rs`, and
`AgentENV/tests/integration/orchestrator.rs` validate.

## 1) Sandbox Public Interface

### Types and responsibilities

- `FirecrackerSandboxConfig` (`AgentENV/src/sandbox/firecracker/config.rs`)
  - **Purpose**: describes a fresh VM boot (kernel + tools drive + user image).
  - **Key fields**
    - `firecracker_binary`: path to the `firecracker` executable.
    - `kernel_image`: path to `vmlinux.bin`.
    - `tools_drive_version`: immutable tools drive identity. The node resolves it to
      `<deps_path>/tools/<version>/tools.ext4` before mounting it as `/dev/vda`.
    - `user_image_config`: overlaybd config for the writable user image (`/dev/vdb`).
    - `boot_args`: kernel boot args (e.g. `console=ttyS0 ... init=/init`).
    - `vcpu_count`, `mem_size_mib`: VM size.
    - `runtime_policy`: socket/envd timeouts and poll intervals.
    - `firecracker_stdout_path`, `firecracker_stderr_path`: capture FC logs.
      If unset, logs default to `work_dir/logs/firecracker-stdout.log` and
      `work_dir/logs/firecracker-stderr.log`.
    - `envd_version`: expected envd version for the guest image.
    - `env_vars`: optional default environment variables injected after envd init.
    - `ublk_config`: optional ublk-backed rootfs configuration.
  - **Convenience**
    - `from_global_config()` builds a config from `ConfigManager` using the
      internal default template rootfs image. The image must already be resolved
      under `<image-cache-root>/configs`.
    - `from_global_config_with_user_image(...)` builds a config from an already
      resolved overlaybd image config.
    - Runtime launches from committed snapshots are built through
      `FirecrackerSandbox::from_snapshot(&RunnableSnapshot, &SandboxLaunchConfig)`.
    - `uid`/`gid`: File ownership (default `0`).

- `ProcessOpts` (`AgentENV/src/sandbox/process.rs`)
  - **Purpose**: Options for starting a process inside the sandbox.
  - **Key fields**
    - `envs`: `HashMap<String, String>` for process environment variables.
    - `cwd`: Optional working directory.
    - `timeout`: Optional max time to wait for completion.
  - **Builder methods**: `with_envs()`, `with_cwd()`, `with_timeout()`.

- `ProcessOutput` (`AgentENV/src/sandbox/process.rs`)
  - **Purpose**: Result of a completed process execution.
  - **Fields**: `stdout`, `stderr`, `exit_code`.

- `ProcessHandle` (`AgentENV/src/sandbox/process.rs`)
  - **Purpose**: Handle to a running process inside the sandbox.
  - **Key methods**
    - `pid()`: Returns the PID inside the guest VM.
    - `wait().await`: Waits for exit and collects all output.
    - `send_stdin(data).await`: Sends bytes to stdin.
    - `send_signal(signal).await`: Sends a signal (e.g. SIGTERM).
    - `kill().await`: Kills the process with `SIGKILL`.

- `FirecrackerSnapshotConfig` (`AgentENV/src/sandbox/firecracker/config.rs`)
  - **Purpose**: describes how to resume a VM from snapshot + memory + base disk.
  - **Key fields**
    - `vm_state_path`: Firecracker VM state file.
    - `mem_overlaybd_config`: runtime memory overlay image config.
    - `base_rootfs_path`: writable disk image or overlaybd rootfs image config
      paired with that snapshot.
    - `firecracker_binary`: FC binary to use for resuming.
    - `runtime_policy`: socket/envd timeouts and poll intervals.
    - `envd_version`: guest envd version metadata.
    - `env_vars`: optional default environment variables restored on start.
  - **Notes**
    - `FirecrackerSnapshotConfig` returned by `pause()` owns a tempdir (keeps files alive).
    - `get_rootfs_path()` resolves the snapshot rootfs path used by runtime tests.

- `SandboxBackend` and `SandboxExecutor` (`AgentENV/src/sandbox/backend.rs`)
  - **Purpose**: public traits for sandbox lifecycle and process execution.
  - **Key methods**
    - `SandboxBackend`: `start`, `start_nowait`, `wait_for_ready`, `pause`, `resume`, `stop`.
    - `SandboxExecutor`: `run_command`, `run_command_with_opts`, `start_process`.

- `FirecrackerSandbox` (`AgentENV/src/sandbox/firecracker/sandbox.rs`)
  - **Purpose**: lifecycle controller for a single Firecracker instance.
  - **Main methods (detailed)**
    - `FirecrackerSandbox::new(FirecrackerSandboxConfig)`
      - Creates a sandbox handle for a fresh boot.
      - Does not start Firecracker; it only prepares the object.
      - Each sandbox gets its own temporary work directory.
    - `FirecrackerSandbox::resume_from_snapshot_config(&FirecrackerSnapshotConfig)`
      - Creates a new sandbox handle and immediately starts it from a snapshot config.
      - Uses `vm_state_path`, the memory overlay image config, and
        `base_rootfs_path` from the config.
      - Returns a running sandbox after Firecracker and envd are ready.
    - `FirecrackerSandbox::from_snapshot(&RunnableSnapshot, &SandboxLaunchConfig)`
      - Creates a sandbox handle from committed snapshot state resolved for a node.
      - Uses snapshot `vm_state.bin` plus node-local materialized
        `memory/image.json` and `rootfs/image.json`.
      - Does not start Firecracker; call `start().await` afterward.
    - `start().await`
      - Spawns the Firecracker process, waits for the API socket, starts the VM,
        then waits for guest-level envd readiness.
    - `pause().await`
      - Sends `PATCH /vm` to pause the VM.
      - Creates snapshot artifacts including `vm_state.bin`, `mem_image.json`,
        `mem_overlaybd/overlaybd.commit`, and rootfs snapshot state.
      - Returns a `FirecrackerSnapshotConfig` that owns the tempdir holding these files.
    - `resume().await`
      - Resumes a paused VM in-place by sending `PATCH /vm` with `Resumed`.
      - Use this when you want to keep the same sandbox instance.
    - `stop().await`
      - Stops Firecracker, releases network resources, and cleans up optional ublk state.
    - `run_command(cmd, args).await`
      - Runs a command inside the sandbox via envd gRPC and waits for it to
        complete. Returns `ProcessOutput` with stdout, stderr, and exit code.
    - `run_command_with_opts(cmd, args, opts).await`
      - Same as `run_command` but accepts `ProcessOpts` for setting env vars,
        working directory, and execution timeout.
    - `start_process(cmd, args, opts).await`
      - Starts a long-running process inside the sandbox. Returns a
        `ProcessHandle` that can stream output, send stdin, or kill the process.
    - `firecracker_stdout_path()` / `firecracker_stderr_path()`
      - Resolved log paths used by this sandbox (including defaults).
    - `work_rootfs_path()`
      - Returns the path to the per-instance writable rootfs inside the work
        directory. Useful for inspecting or copying disk state after a run.

### What happens under the hood (important for correct usage)

- Each `FirecrackerSandbox` uses its own temp work directory.
- Kernel is used via the configured host path.
- When resuming, the writable rootfs and memory image are materialized into the
  work dir so each instance can mutate independently. For overlaybd-backed
  snapshot-backed template launches, this means recreating runtime image configs and fresh
  writable uppers from the committed layer stack.
- Rootfs copies use copy-on-write helpers where available.
- Firecracker runs with `current_dir = work_dir`, so relative paths work.
- Firecracker logs default to `work_dir/logs/*` unless explicit log paths are configured.
- `start()` waits for both the API socket and envd readiness.
- Although the Firecracker process will be killed on drop, explicit `stop()` is recommended.

### Minimal usage example

```rust
use agentenv::sandbox::{FirecrackerSandbox, FirecrackerSandboxConfig, SandboxExecutor};

let mut cfg = FirecrackerSandboxConfig::new(
    "/path/to/firecracker".into(),
    "/path/to/vmlinux.bin".into(),
    "/path/to/rootfs.ext4".into(),
);
cfg.boot_args = Some("console=ttyS0 reboot=k panic=1 pci=off init=/init".into());

let mut sandbox = FirecrackerSandbox::new(cfg)?;
sandbox.start().await?;

// Run a command inside the VM
let output = sandbox.run_command("echo", &["hello"]).await?;
assert_eq!(output.exit_code, 0);
println!("{}", output.stdout);

let snapshot = sandbox.pause().await?;
sandbox.stop().await?;

let mut resumed_sandbox = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
resumed_sandbox.stop().await?;
```

### Runtime prerequisites

- Linux host
- `/dev/kvm` accessible by the current user, with the host modules matching
  `virtualization_mode` (KVM by default; PVM requires x86_64 and `kvm_pvm`)
- `debugfs` (e2fsprogs) if you use init injection or disk-inspection helpers

## 2) Global Config Manager (`src/cfg.rs`)

The tests and benchmarks use a shared global config manager based on
`config/default.toml`.

### Config file format

`AgentENV/config/default.toml` (override via `AENV_CONFIG_PATH` or `--config`)

```toml
[firecracker]
boot_args = "your-kernel-boot-args"
binary_path = "/path/to/firecracker"
socket_timeout_secs = 3
socket_poll_ms = 20
# Optional override; defaults to $AENV_HOME/firecracker-work.
# work_dir = "/path/to/firecracker-work"
# Optional override; defaults to $AENV_HOME/logs/serial.
# serial_dir = "/path/to/serial"
# Optional; enables Firecracker's own logging when set to a non-empty level.
# log_level = "Info"

[kernel]
image_path = "/path/to/vmlinux.bin"

[tools]
version = "0.1.0-custom.1"
drive_path = "/path/to/tools.ext4"

[machine]
mem_size_mib = 128
vcpu_count = 1

[envd]
version = "0.5.15"
init_timeout_secs = 30
poll_ms = 10

[orchestrator]
auto_evict_interval_ms = 1000
default_sandbox_timeout_secs = 15

[snapshot]
repository_backend = "posix_fs"
local_cache_path = "$AENV_HOME/snapshot-local-cache"

[backend.posix_fs]
snapshot_store = "$AENV_HOME/snapshot-store"

[ublk]
enabled = false
```

### TOML parameter reference

- `[firecracker]`
  - `boot_args`: Kernel command line. The runtime appends `init=/init` if not
    already present (tools drive provides the init script).
  - `binary_path`: Absolute or relative path to `firecracker`.
  - `socket_timeout_secs`: Max time to wait for the Firecracker API socket.
  - `socket_poll_ms`: Poll interval for checking socket existence.
  - `work_dir`: Optional override for the per-sandbox Firecracker working
    directories. These directories include runtime sockets, symlinks, local
    logs, and writable OverlayBD upper layer data (`overlaybd/upper.data` and
    `overlaybd/upper.index`). Defaults to `$AENV_HOME/firecracker-work`.
  - `serial_dir`: Optional override for the persistent output directory.
    Serial output is written under a per-sandbox subdirectory and not removed
    by the sandbox. Defaults to `$AENV_HOME/logs/serial`.
  - `log_level`: Optional Firecracker log level (`Error`, `Warning`, `Info`,
    `Debug`, `Trace`, case-insensitive). When set to a non-empty value,
    Firecracker's own logging is enabled and written to a `firecracker.log`
    file in the sandbox's log directory (the same directory used for serial
    output). If omitted or empty, Firecracker logging is disabled.

- `[kernel]`
  - `image_path`: Optional local kernel image path (`vmlinux.bin`). If omitted,
    AgentENV derives it from `deps_path` and the bundled dependency manifest.

- `[tools]`
  - `drive_path`: Optional local tools drive source. When set, an explicit `version` is required and setup imports it into the immutable version directory.

- Template rootfs image
  - User-visible rootfs images are selected when starting template builds through the optional `fromImage` field.
  - If `fromImage` is omitted, AgentENV uses `[image.resolver].default_image`.
  - Standard Docker Hub shortnames such as `ubuntu:24.04` and `node:20`
    are normalized before image resolution.
  - Resolved image configs are cached under
    `<image-cache-root>/configs/<slug>-<hash>-image.json`; local layer
    products are cached under `<image-cache-root>/layers/`; overlaybd-native
    remote block reads are cached under `<image-cache-root>/remote-blocks/`.
  - Registry authentication uses docker credentials from
    `~/.docker/config.json` or `$DOCKER_CONFIG/config.json`.
- `[machine]`
  - `mem_size_mib`: Guest RAM size (default: 128).
  - `vcpu_count`: Number of vCPUs (default: 1).

- `[envd]`
  - `version`: Expected envd version baked into the runtime image.
  - `init_timeout_secs`: Max time to wait for the in-guest envd daemon to become
    ready after VM start (default: 30).
  - `poll_ms`: Poll interval for envd health-check retries (default: 10).

- `[orchestrator]`
  - `auto_evict_interval_ms`: Poll interval for background timeout eviction.
  - `default_sandbox_timeout_secs`: Default keep-alive timeout used by the orchestrator.
  - `auto_resume_min_sandbox_timeout_secs`: When a data-plane request targets a non-running sandbox, automatically resume it (if auto-resume is enabled) and refresh its timeout for no-less than this duration.

- `[pool]`
  - `low_watermark`: Shared lower bound for warm resources. Defaults to `2`.
  - `high_watermark`: Shared upper bound target for warm resources. Defaults to `64`.
  - `[pool.network].maintenance_enabled`: Enables the background network-slot maintenance worker. Defaults to `true`.
  - `[pool.block].enabled`: Enables the ublk overlaybd warm-device pool.
  - `[pool.firecracker].enabled`: Enables pre-spawned Firecracker processes for snapshot resume.

- `[snapshot]`
  - `local_cache_path`: Manager-owned node-local snapshot artifact/cache root. Defaults to `$AENV_HOME/snapshot-local-cache`.
  - `repository_backend`: Snapshot repository backend. Defaults to `posix_fs`.

- `[backend.posix_fs]`
  - `snapshot_store`: Committed snapshot store root. Defaults to `$AENV_HOME/snapshot-store`.

- `[ublk]`
  - `enabled`: Enables ublk-backed rootfs handling.
  - `daemon_binary_path`: Optional path to `uvm-ublk-daemon`.
  - `daemon_socket_path`: Optional unix socket path for daemon RPCs.
  - `daemon_log_path`: Optional log file path for the daemon process.
  - `device_type`: `cow` or `overlaybd`.

- `[ublk.overlaybd]`
  - `global_config_path`: path to the generated overlaybd runtime config.
    Required for `overlaybd` device_type. Per-image configs are derived from
    template build `fromImage` values and live under
    `<image.cache.root_dir>/configs`.

### Defaults and optional fields

- `[firecracker].binary_path`: Optional. If omitted, AgentENV derives it from `deps_path` and the bundled dependency manifest.
- `[firecracker].boot_args`: Optional. Defaults to `console=ttyS0 reboot=k panic=1 pci=off`.
- `[firecracker].socket_timeout_secs`: Optional. Defaults to 3 seconds.
- `[firecracker].socket_poll_ms`: Optional. Defaults to 20 ms.
- `[firecracker].work_dir`: Optional override. Defaults to `$AENV_HOME/firecracker-work`.
- `[firecracker].serial_dir`: Optional override. Defaults to `$AENV_HOME/logs/serial`.
- `[kernel].image_path`: Optional. If omitted, AgentENV derives it from `deps_path` and the bundled dependency manifest.
- `[tools].drive_path`: Optional local source override; requires an explicit `[tools].version` when set.
- `[tools].url`: Optional registry override; requires an explicit `[tools].version` when set.
- `[machine]`: Optional. If omitted, defaults are 128 MiB RAM and 1 vCPU.
- `[envd]`: Optional, but template and sandbox flows expect a valid `version`.
- `[envd].init_timeout_secs`: Optional. Defaults to 30 seconds.
- `[envd].poll_ms`: Optional. Defaults to 10 ms.
- `[orchestrator]`: Optional. If omitted, orchestrator runtime defaults are used.
- `[pool]`: Optional. If omitted, network pool maintenance is enabled with low watermark 2 and high watermark 64; block and Firecracker process pools are disabled unless enabled in their component subsections.
- `[snapshot]`: Optional. If omitted, `repository_backend` defaults to `posix_fs` and the local cache root derives from `AENV_HOME`.
- `[backend.posix_fs]`: Optional. If omitted, the POSIX snapshot store defaults to `$AENV_HOME/snapshot-store`.
- `[ublk]`: Optional. If omitted, ublk is disabled.

### Environment-based configuration

The config loader reads the full TOML file from the following sources:

- `AENV_CONFIG_PATH`
- `--config <path>`
- `config/default.toml`

Runtime image and binary paths are configured in the TOML file itself.

### Required fields

- `[kernel].image_path`
- `[tools].version` when `[tools].drive_path` or `[tools].url` is overridden
- `[envd].version`

If the `[orchestrator]` section is present, it must include
`default_sandbox_timeout_secs`.

### ConfigManager (recommended entry point)

The helper is strict. If requirements are not met (missing files, invalid
config), it returns an error and the test fails.

**Path resolution order**
1) `AENV_CONFIG_PATH` environment variable
2) `--config` CLI flag
3) `config/default.toml` (default)

### Helper methods

- `ConfigManager::global()` -> `Result<&'static ConfigManager>`
  - Lazily initializes and returns a process-wide singleton.
- `ConfigManager::new()` -> `Result<ConfigManager>`
  - Loads config using the default resolution order.
- `ConfigManager::new_from_path(path)` -> `Result<ConfigManager>`
  - Loads a specific TOML file.
- `ConfigManager::config()` -> `&AppConfig`
  - Returns the parsed application config.
- `ConfigManager::global_config()` -> `Result<&'static AppConfig>`
  - Returns the global parsed config directly.
- `ConfigManager::get_orchestrator_config()` -> `Option<OrchestratorConfig>`
  - Returns orchestrator settings when configured.
- `AppConfig::resolved_snapshot_store()` -> `PathBuf`
  - Resolves the effective committed snapshot store root.
- `AppConfig::resolved_snapshot_local_cache_path()` -> `PathBuf`
  - Resolves the effective manager-owned node-local snapshot cache root.

### Typical test setup flow

```rust
use agentenv::sandbox::{FirecrackerSandbox, FirecrackerSandboxConfig, SandboxExecutor};

let sandbox_config = FirecrackerSandboxConfig::from_global_config()?;

let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
sandbox.start().await?;

// Run commands via envd gRPC
let output = sandbox.run_command("echo", &["hello"]).await?;
assert_eq!(output.exit_code, 0);

let snapshot = sandbox.pause().await?;
sandbox.stop().await?;

let mut resumed_sandbox = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
resumed_sandbox.stop().await?;
```

## 3) What the Integration Tests Validate

### `integration/fc.rs`

- `microvm_startup_moves_to_running`
  Boots a fresh VM and verifies that:
  - Firecracker starts and accepts API calls.
  - Kernel + rootfs can be configured without error.
  - The VM can be cleanly shut down afterward.

- `microvm_pause_transitions_to_paused`
  Validates pause + snapshot creation by:
  - Pausing a running VM via FC API.
  - Creating `vm_state.bin`, `mem_image.json`, and
    `mem_overlaybd/overlaybd.commit`.
  - Ensuring those files exist on disk.

- `microvm_resume_from_pause_transitions_to_running`
  Validates in-place resume by:
  - Pausing a VM.
  - Resuming it in the same sandbox instance.
  - Confirming the control flow completes without error.

- `microvm_resume_transitions_to_running`
  Validates resume into a new sandbox by:
  - Pausing and snapshotting a VM.
  - Shutting down the original sandbox.
  - Resuming from the snapshot in a new sandbox instance.

- `snapshot_preserves_disk_state_on_resume`
  Confirms disk state is preserved by:
  - Writing a marker file via `run_command`.
  - Pausing and resuming from the snapshot.
  - Reading the marker file after resume and asserting it persists.

- `snapshot_chain_survives_after_parent_snapshot_handle_is_dropped`
  Validates multi-level snapshot chains by:
  - Creating a snapshot, resuming, creating a second snapshot.
  - Dropping the first snapshot handle.
  - Resuming from the second snapshot and verifying disk state.
  - Also tests `pause_to_dir` with snapshot-owned inherited layer adoption.

- `microvm_can_access_internet`
  Confirms guest networking works before and after snapshot resume.

- `multiple_resumes_have_independent_disk_state`
  Confirms multiple resumptions are isolated by:
  - Resuming multiple VMs from a single snapshot sequentially.
  - Each resumed VM verifies the shared marker and writes its own file.
  - Verifying each snapshot has independent state.

### `integration/process.rs`

- `run_command_captures_stdout`
  Runs `echo hello world` and verifies stdout contains the expected string.

- `run_command_captures_stderr`
  Runs a shell command that writes to stderr and verifies stderr capture.

- `run_command_reports_exit_code`
  Runs `exit 42` and verifies the exit code is reported correctly.

- `run_command_with_opts_sets_env_and_cwd`
  Verifies that environment variables and working directory options are honoured.

- `run_command_handles_timeout`
  Verifies long-running commands fail when a timeout is configured.

- `run_command_with_unbounded_output`
  Verifies excessive output is rejected instead of buffering forever.

- `start_process_interactive_stdin`
  Starts `cat`, sends data via stdin, kills the process, and verifies the echoed
  output.

- `start_process_send_signal`
  Starts `sleep 300`, sends SIGTERM, and verifies the process terminates with a
  non-zero exit code.

- `run_command_after_snapshot_resume`
  Pauses a sandbox, resumes from a snapshot config, and verifies `run_command` works on
  the resumed instance.

### `integration/snapshot.rs`

- `built_snapshot_can_be_loaded_by_alias_and_launched`
  Builds a template-backed snapshot, loads it by alias, launches a sandbox from
  the resolved runnable snapshot, and verifies captured runtime state.

- `listed_committed_snapshot_can_be_loaded_and_resolved`
  Verifies `SnapshotManager::list_committed()` returns usable committed snapshots
  that can be loaded and resolved into runnable paths.

- `loaded_runnable_snapshot_by_alias_can_be_resolved`
  Verifies `SnapshotManager::load_runnable()` resolves alias-based lookups into
  runnable snapshot state in one step.

- `delete_committed_snapshot_by_alias_removes_it_from_repository`
  Verifies deleting a committed snapshot by alias removes the durable record.

- `snapshot_rebuild_from_committed_snapshot_preserves_base_state`
  Verifies rebuilding from a committed snapshot preserves the base filesystem
  state while adding new changes.

- `rebuild_merges_env_metadata_and_list_filter_can_select_by_id`
  Verifies rebuilds merge environment metadata correctly and that committed
  snapshot listing can filter by snapshot id.

### `integration/orchestrator.rs`

- `orchestrator_lifecycle`
  Verifies the orchestrator can create, fetch, list, filter, keep alive, and
  delete template-backed sandboxes while keeping proxy lookup state in sync.

## 4) End-to-End Execution Checklist

1. Provide Firecracker binary, kernel, tools drive, overlaybd runtime, and ublk daemon. These are automatically downloaded when the server starts, or you can run `cargo run --bin server -- --setup-only` to provision runtime dependencies independently.
2. Provision host access once as root with `server --setup-host --runtime-user
   <user> --runtime-group <group>`. The group is the runtime service group: it
   owns AgentENV state and receives ublk device access. Normal server startup
   performs validation only and never invokes `sudo`.
3. Host setup installs a udev rule for `/dev/ublk-control`, `/dev/ublkc*`, and `/dev/ublkb*`, so the runtime group can access the control and dynamic device nodes.
4. Update `config/default.toml` paths, or point `AENV_CONFIG_PATH` to a custom config file.
5. Ensure `/dev/kvm` is accessible by the runtime user and the configured
   virtualization mode matches the host modules.
6. If you run template tests, ensure the host can run `regctl` (server setup installs it automatically from the `[regclient]` manifest entry) and access the registry for template `fromImage` resolution.
7. Run `scripts/tests/e2e/run_e2e.sh` for API-level E2E coverage. The runner exports `E2E_TEMPLATE_USER_IMAGE`. Suite `05_template_lifecycle.sh` also creates a template build with `E2E_SHORT_USER_IMAGE` to verify short-name image resolution.
8. Run `make test-agent-integration` to run the `agentenv` integration test modules in `tests/integration/` as a non-root user with the required capabilities, plus the Docker/MinIO-backed OSS snapshot repository test (`crates/e2e-tests/tests/snapshot_oss_e2e_test.rs`).
9. Run `make test-ublk` to run the `uvm-ublk` and `overlaybd` storage tests, including the Docker/MinIO-backed OSS backend test (`storage/overlaybd/tests/oss_backend_minio.rs`).

## 4.1) Firecracker Client and Runtime Upgrade Checklist

When bumping AgentENV to a patched Firecracker runtime, update both the generated
client API and the binary that setup downloads:

1. Replace `thirdparty/firecracker-client/firecracker.yaml` with the target
   Firecracker OpenAPI spec.
2. Run `make firecracker-client` from the repository root and review generated
   changes under `thirdparty/firecracker-client/`.
3. Build the patched Firecracker release binary on a Linux host. Package it as a
   gzip tar archive named `firecracker-{version}-{arch}.tgz` containing a
   `firecracker` executable.
4. Update the `[firecracker]` entry in `config/deps_manifest.toml` with the new
   version and a download URL template that supports `{version}` and `{arch}`.
5. Before relying on the URL in tests, verify it is a direct download:

   ```bash
   curl -L "<url>" -o /tmp/firecracker.tgz
   tar -tzf /tmp/firecracker.tgz
   ```

   The archive listing should include `firecracker`.
6. Run setup against a clean or temporary Firecracker dependency directory so it
   really downloads the archive:

   ```bash
   cargo run --bin server -- --config /path/to/config.toml --setup-only
   ```

7. Validate the Rust side after code generation:

   ```bash
   cargo fmt --check -p firecracker_client -p agentenv
   cargo check -p firecracker_client
   cargo check -p agentenv
   cargo test -p agentenv --lib
   ```

## 5) Running Custom Scripts

### Run commands via envd

Once the sandbox is started (with a rootfs that includes the envd daemon), you
can execute commands inside the VM using the process API:

```rust
// Simple command
let output = sandbox.run_command("ls", &["-la", "/tmp"]).await?;
println!("exit={} stdout={}", output.exit_code, output.stdout);

// Command with options
use agentenv::sandbox::ProcessOpts;
let opts = ProcessOpts::new()
    .with_cwd("/tmp")
    .with_envs([("KEY".into(), "value".into())].into());
let output = sandbox.run_command_with_opts("env", &[], opts).await?;

// Interactive / long-running process
let mut handle = sandbox.start_process("cat", &[], ProcessOpts::default()).await?;
handle.send_stdin(b"hello\n").await?;
handle.kill().await?;
let output = handle.wait().await?;
```

The process API uses envd's gRPC `ProcessClient` under the hood. It requires
the envd daemon to be running inside the guest VM.

### Run different scripts for multiple snapshot resumes

`/init` is not re-executed after resume, so per-instance behavior must come
from per-instance disk contents.

A typical pattern:
1) Start a base VM and `pause()` to create a snapshot.
2) For each instance, copy the snapshot rootfs and write a per-instance file
   into it (for example using `debugfs`).
3) Create a `FirecrackerSnapshotConfig` that points to that per-instance rootfs and resume.

The guest must keep a long-running dispatcher (started during the initial
boot) that watches for these per-instance files and executes them after resume.
