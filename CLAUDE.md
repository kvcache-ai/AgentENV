# AgentENV Agent Guide

`AGENTS.md` links to this file (`CLAUDE.md`); edit this shared source.
AgentENV is a Rust workspace running snapshot-capable Firecracker sandboxes with
an E2B-compatible API. Gateway and scheduler form a separate Go module in `services/`.

## Behavioral Guidelines

Behavioral guidelines to reduce common LLM coding mistakes, combined with the
project-specific instructions below.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks,
use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- Read the affected module and its tests. Use the code map and linked design
  docs to locate ownership boundaries.
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them; don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes,
simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it; don't delete it.
- Preserve unrelated work already present in the checkout.

When your changes create orphans:

- Remove imports, variables, and functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" -> "Write tests for invalid inputs, then make them pass."
- "Fix the bug" -> "Write a test that reproduces it, then make it pass."
- "Refactor X" -> "Ensure tests pass before and after."

For multi-step tasks, state a brief plan:

```text
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
3. [Step] -> verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it
work") require constant clarification.

Run the narrowest relevant checks during development, then the applicable
repository checks below. Report what ran and any checks blocked by missing
hardware, permissions, dependencies, or credentials.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer
rewrites due to overcomplication, and clarifying questions come before
implementation rather than after mistakes.

## Core Concepts

- **Snapshot/template:** A committed checkpoint contains VM state and references
  to memory, rootfs, attached-drive, and volume layers. Capture leaves the source
  running; launching it creates a new sandbox. Pause/resume retains the existing
  ID through separate paused-sandbox persistence. Template builds commit snapshots.
- **Volume:** An independently managed block filesystem that survives sandbox
  deletion. `exclusive` permits one writable mount; `ro` permits shared read-only
  mounts. `volumeMounts` references managed volumes; cold-start `attachedDrives`
  uses the same device machinery with a different ownership lifecycle.
- **OverlayBD/ublk:** OverlayBD stacks immutable lower layers and an optional writable
  upper; sealing turns changes into a reusable lower. `image.json` describes the
  stack, while layers hold data. ublk exposes it as a host block device for guest
  disks or memory restore; filesystem images and memory images are distinct.
- **OSS:** The S3-compatible repository backend, including Alibaba OSS.
  `snapshot.repository_backend` selects `oss` or `posix_fs`. Repositories own
  durable records and managed layers; external layers may stay in OCI registries.
  Runtime configs and download caches are derived data. P2P only accelerates access.

## Build and Validate

Run from the repository root. [Makefile](Makefile), [Cargo.toml](Cargo.toml), and
[rust-toolchain.toml](rust-toolchain.toml) define current targets and tooling.

```bash
make                                      # Rust build
make fmt                                  # workspace formatting check
make clippy                               # all targets/features, -D warnings
make test-unit                            # selected unit and capability tests
make test                                 # agent, envd, and storage suites
make test-agent-integration                # includes snapshot OSS E2E tests
make test-ublk                             # includes OverlayBD and MinIO tests
cargo test -p agentenv --lib test_name      # focused unit test
make -C services test                      # gateway and scheduler tests
make -C services fmt-check vet             # Go lint
make start-server                         # build/install daemon and run server
```

For Rust changes, run formatting, clippy, and relevant tests; aggregate test
targets do not cover every crate. For `services/` changes, also run Go tests;
use `go test ./...` inside `services/` for shared packages. Validate both languages
for scheduler protobuf changes. Documentation-only changes need diff/link/command
checks, not runtime suites.

Run builds/tests as a non-root user. Capability-dependent Make targets use
[scripts/run-with-capabilities.sh](scripts/run-with-capabilities.sh) to delegate
`CAP_NET_ADMIN` and `CAP_SYS_ADMIN` through `sudo`/`setpriv`. Reuse the Makefile's
runner/environment setup for filtered privileged tests. VM tests require Linux,
`/dev/kvm`, ublk, and network namespaces. `make test-unit` also includes ublk tests;
some suites need network access or object-storage services.

Use isolated `AENV_TEST_STATE_DIR` state and `AENV_TEST_DEPS_PATH` dependencies,
never a live server's state. Set `AENV_CONFIG_PATH` for non-default test configs.

## Configuration and Conventions

- Configuration lives in `src/cfg.rs` and `config/default.toml`; select a file with
  `AENV_CONFIG_PATH` or `--config`. `AENV_HOME_PATH` holds state/caches,
  `AENV_RUNTIME_PATH` transient runtime state, and `AENV_DEPS_PATH` downloaded assets.
- `server --setup-host --runtime-user <user> --runtime-group <group>` runs as root
  to configure host access/sysctls. `server --setup-only` provisions packages/assets
  without VM access. Normal startup downloads assets and validates prerequisites.
- KVM is default; PVM requires x86_64 and host-provided `kvm_pvm`. AgentENV does not
  install/load it. Select the mode with `AENV_VIRTUALIZATION_MODE`.
- Use Rust 2021, existing style, and tracing initialized only in binaries:
  `info` for lifecycle, `debug` for internals, `warn` for recoverable issues,
  `error` for unrecoverable failures. Update schemas/config/docs with contract changes.
- Use Conventional Commit prefixes (`feat:`, `fix:`, `refactor:`, `ci:`, `chore:`).
  Push to a fork; open PRs against `https://github.com/kvcache-ai/AgentENV/`.
  Never push branches directly upstream. See [CONTRIBUTING.md](CONTRIBUTING.md).
- Use the [PR template](.github/pull_request_template.md) for pull requests and
  the appropriate [issue template](.github/ISSUE_TEMPLATE/) for issues, including
  CLI-created submissions. Fill required sections and report checks accurately.

## Code Map

| Area | Start here |
| --- | --- |
| API and lifecycle | `src/api/impls/`, `src/orchestrator/service.rs`, `src/sandbox/backend.rs` |
| Firecracker and drives | `src/sandbox/firecracker/`, `src/sandbox/ublk/`, `src/sandbox/extra_drive.rs` |
| Snapshots and templates | `src/snapshot/`, `src/template/` |
| Volumes | `src/volume.rs`, `src/api/impls/volumes.rs`, `src/api/impls/sandbox.rs` |
| Layered storage | `storage/overlaybd/`, `storage/ublk/`, `storage/ublk-daemon/`, `storage/util/` |
| Images and P2P | `src/image/`, `src/p2p/`, `src/overlaybd/p2p/` |
| Control plane and metrics | `services/`, `src/observability/` |
| CLI, guest, tooling | `crates/aenv/`, `thirdparty/envd/`, `adev/` |
| Tests | `tests/`, `crates/e2e-tests/`, `crates/test-support/`, `scripts/tests/` |

Reuse shared helpers: `src/local_store.rs` (`LocalKvStore`, explicit
`LocalStoreDurability::{Memory, Wal, Sync}`), `crates/object-store-operator/`
(S3/credentials), `crates/linux-cap/`, `crates/shell-util/`, and `crates/warm-pool/`.
`storage/uffd-core/` is reference-only and excluded from the workspace.

## Constraints to Preserve

- **Lifecycle:** Recoverable capture/fork errors restore `Running`; terminal
  errors require teardown. Graceful shutdown persists sandboxes as `Paused`.
  Keep `CapturedSandboxSnapshot` alive until repository publication completes.
- **Storage:** Commit logical layer references, not node-local paths. Cache GC
  must not delete durable artifacts or live writable uppers. Use
  `UblkDeviceManager`; preserve shared-memory device reference counts and COW.
- **Capture:** Preserve sealing/restacking and attached-drive state. Template-build
  compression (`[template_build]`) is separate from ordinary memory compression
  (`[memory_snapshot]`); ordinary captures keep rootfs layers raw.
- **Volumes:** Keep reservations/readiness in `VolumeManager` and the repository.
  Reject deletion while mounted and new mounts while `uploading`/`failed`.
  Fork exclusive volumes per child, share read-only volumes, and roll back failed
  child allocations. Snapshots store `volume_snapshots` separately from attached
  drives; launches recreate volumes unless explicit `volumeMounts` replaces them.
  Deleting a source volume must not invalidate committed snapshots.
- **Registries:** Use configured `regctl` and `ImageResolver`. Preserve lazy reads
  for OverlayBD-native images. Private registries use the runtime user's Docker
  credentials. Preserve OSS object-storage/source-registry publication policies;
  `aenv-snapshot-image` exports rootfs only and must verify registry conflicts.
- **P2P:** Depend on `P2pTransport`. Reuse layer identity from
  `src/overlaybd/p2p/artifact.rs`. Publish snapshot artifacts after repository commit,
  best-effort.
  OSS resolves fixed artifacts P2P-first; POSIX must not repair missing files from
  peers. Scheduler stores artifact-to-node hints only.
- **Scheduler:** Assignments/heartbeats own bindings; lifecycle events are
  best-effort and do not establish bindings. Fork events identify each child.
  Redis query-only replicas serve existing-sandbox lookups; scheduling still
  requires the primary.
- **Extensions:** Start/patch failures fail the operation; stop is best-effort,
  including pause. Pair start/stop with a per-runtime `sandboxInstanceId`.
  Forward patches verbatim, store returned full params, and preserve them across
  snapshots/resume. Empty params work with hooks disabled; non-empty params do not.

## Generated Code

Edit schemas and regenerate; include both changes. Handwritten `thirdparty/`
code is editable; generated clients/stubs are machine-managed.

| Schema | Regenerate |
| --- | --- |
| `src/api/openapi.yml` | `make agentenv-server` |
| `src/custom_extension_api/openapi.yml` | `make custom-extension-client` |
| `thirdparty/firecracker-client/firecracker.yaml` | `make firecracker-client` |
| `thirdparty/envd/http-client/envd.yaml` | `make envd-http-client` |
| `services/api/proto/scheduler.proto` | `make -C services proto`; Rust regenerates via `build.rs` |

`cargo adev codegen` runs all OpenAPI generators. Prune orphaned custom-extension
models after schema removals. Firecracker upgrades also require matching binary
packaging, `config/deps_manifest.toml`, and the
[sandbox testing checklist](docs/src/internals/sandbox-testing.md).

## Details by Topic

- [Architecture](docs/src/internals/architecture.md) and [persistence ownership](docs/src/internals/persistence-artifact-inventory.md).
- [Snapshots](docs/src/concepts/snapshots.md), [volumes](docs/src/concepts/volumes.md), and [template testing](docs/src/internals/template-builder-testing.md).
- [P2P](docs/src/internals/p2p-design.md), [control plane](services/README.md), and [custom extensions](docs/src/concepts/custom-extension.md).
- [Configuration](docs/src/configuration/reference.md) and [environment variables](docs/src/configuration/env-vars.md).

Keep this guide concise; put implementation details in the linked docs or code.
