# Nested KVM: setup and testing

This guide tests x86_64 KVM sandboxes containing a QEMU guest. Run commands from
the AgentENV repository root on a dedicated Intel nested-KVM host. Hardware
coverage so far is Intel; AMD needs a separate run.

## 1. Prepare the node

Use a host with working nested virtualization and access to `/dev/kvm`. Install
Python 3.11+, QEMU x86 system binaries, static BusyBox at `/bin/busybox`, cpio,
and Bash. Use a fresh test work directory with at least 5 GiB free.

The default KVM dependency catalog downloads Firecracker
`1.15.1-nestedvirt-v1` and its matching `cpu-template-helper` for x86_64 or
aarch64. Both programs are installed under
`deps_path/firecracker/1.15.1-nestedvirt-v1/`. Leave `[firecracker].version`,
`url` and `binary_path` unset to use these managed defaults; explicit
configuration and environment overrides still take precedence.
The x86_64 KVM default downloads the prebuilt Linux 6.18 uncompressed vmlinux
ELF from the Firecracker release. It includes built-in KVM, MP-table CPU discovery
and virtio MMIO command-line discovery; users do not need to compile it.
Leave `[kernel].image_path`, `version` and `url` unset to use this default.
The aarch64 KVM and PVM kernel defaults are unchanged.
Supply a **bzImage for QEMU's inner guest** separately for this hardware test.
Linux 6.18 is the selected default, not a minimum version required by nested KVM.

Edit the test node's existing TOML configuration:

| Field | Value |
| --- | --- |
| `[kernel].image_path` | Leave unset to download the default outer vmlinux; set only for a custom kernel. |
| `[firecracker].boot_args` | Keep the complete existing string and append `no-kvmapf`. |
| `[pool.block].enabled` | `true`. |

For a custom Firecracker build, set its version and binary path, and place the
matching helper at `deps_path/firecracker/<version>/cpu-template-helper`.
`no-kvmapf` is the workaround used in the Intel validation; retain existing boot
and DAMON arguments.

Start a dedicated AgentENV server on the same host as the test with this config.
Isolate its state using
`AENV_HOME_PATH` and `AENV_RUNTIME_PATH`, select its dependencies with
`AENV_DEPS_PATH`, and use non-overlapping network pools. Bind the API to loopback
(the commands below use port 18090) and set `[sandbox_proxy].domains`, for
example to `["nested.local"]`. Configure the test node's regctl wrapper to pass
`--host reg=127.0.0.1:18091,tls=disabled` for the local image registry.

The runner expects the server to be running. Pass the same TOML to the server
and test: `--config` selects one file, with schema defaults and environment
overrides. Ensure that file contains the effective complete boot arguments.
Build fresh templates: changing kernel paths or boot arguments does not update
the kernel memory and arguments captured in existing snapshots.

## 2. Prepare the image and run the API test

In a Python virtual environment, install the SDK, then run one command:

```sh
python3 -m pip install -r scripts/tests/nestedvirt-requirements.txt
python3 scripts/tests/nestedvirt-smoke.py run \
  http://127.0.0.1:18090 /absolute/path/test-home/secrets/api-key \
  /absolute/path/nestedvirt-work --inner-kernel /absolute/path/bzImage \
  --config /absolute/path/node.toml --kernel-release EXPECTED_UNAME_R --rounds 3
```

Replace `EXPECTED_UNAME_R` with the outer kernel's exact `uname -r` value.
The runner prepares a fresh temporary OCI image, starts its loopback registry
(default port 18091; override with `--port` and update the node's regctl setting),
executes the API test, then closes the registry and removes temporary files.
It retains results under `nestedvirt-work/evidence/`.

`nestedvirt-guest.sh` supplies the image init, the inner counter program and
QEMU start/stop operations. QEMU runs with `accel=kvm`. The image contains QEMU,
its libraries, Bash, static BusyBox, the supplied inner kernel and its initrd.

The runner reads the API key file, builds a two-vCPU/512-MiB template, creates
a source sandbox and two fork children, then pauses and reconnects the source.
It tests each of these stages three times:

| Stage | Recovery check |
| --- | --- |
| `unused`: no inner VM yet | A fresh inner VM can start. |
| `initialized`: QEMU started with `-S` | QMP still reports KVM enabled and the guest stopped in `prelaunch`. |
| `running`: inner guest executing | Its counter continues advancing across fork and pause/resume. |

Each stage also starts and stops a fresh inner VM in all three sandboxes.
The initialized case replaces the stopped inner VM; resuming that same inner
VM is not covered. QMP listens on guest loopback port 4444.

Require successful script exit and nine unique phase/round records in
`api-results.json`. `api-commands.jsonl` records guest commands, kernel release,
boot arguments and counter growth. Results are published after sandbox/template
cleanup succeeds. Retain evidence with the source revisions, config and asset
checksums. Each run prepares a fresh fixture. Stop the dedicated test server
after the run.

## 3. Additional checks

**Old v9 snapshot recovery.** `orchestrator_v9_snapshot_upgrade` in
`tests/integration/orchestrator.rs` uses the current AgentENV test binary with
old and new Firecracker processes. It saves a v9 sandbox, invalidates its saved
executable path, restores with the configured current runtime, checks envd and
saves again as v1000. Testing an upgrade from an old AgentENV binary is separate.

Prepare `v9-node.toml` and `current-node.toml` with their respective runtimes and
matching helpers. Keep state, storage, network, guest kernel, boot arguments and
other guest assets the same, using absolute paths. After the normal integration
host/dependency setup, build the daemon with `make build-ublk PROFILE=debug`:

```sh
AENV_HOME_PATH=/absolute/path/isolated-test/home \
AENV_RUNTIME_PATH=/absolute/path/isolated-test/run \
AENV_DEPS_PATH=/absolute/path/test-deps \
AENV_UBLK_DAEMON_BINARY_PATH="$PWD/target/debug/uvm-ublk-daemon" \
AENV_CONFIG_PATH=/absolute/path/current-node.toml \
AENV_TEST_V9_CONFIG=/absolute/path/v9-node.toml \
scripts/run-with-capabilities.sh cargo test -p agentenv \
  --test orchestrator_integration orchestrator::orchestrator_v9_snapshot_upgrade \
  -- --exact --ignored --nocapture
```

**Hosted CI** checks script syntax and OCI fixture rebuilding through
`python3 scripts/tests/nestedvirt-smoke.py check-fixture` (also with `-O`).
This check uses temporary files and a loopback registry; it requires neither
KVM nor the E2B SDK. The hardware matrix runs explicitly on a nested-KVM host.
The v9 test is compiled but
ignored by default because it requires both runtime versions.
