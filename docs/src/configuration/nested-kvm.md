# Nested KVM with a Linux 6.18 guest

An x86_64 AgentENV node can expose `/dev/kvm` inside its sandboxes using
`kvcache-ai/firecracker` at `c7e1cceb6bf643f239ddac804a535ceb8ac93cb1`
and a Linux 6.18 guest kernel with built-in KVM. The host must expose VMX or
SVM and provide working nested virtualization. Intel VMX is the validated
hardware target, with two outer vCPUs and 512 MiB outer memory.

## Install matching assets

Build Firecracker and `cpu-template-helper` together from that source checkout.
Build the guest kernel using `tools/nestedvirt/build-kernel.sh` in the matching
Firecracker checkout. The guest configuration includes KVM, MP-table CPU
discovery, virtio MMIO command-line device discovery, DAMON reclaim and page
reporting. Keep the generated configuration, build commands, compiler versions
and artifact checksums with the deployment.

The guest source archive is `linux-6.18.tar.xz`, with SHA-256
`9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b`.
The integration candidate uses generated configuration SHA-256
`70699be91477f23a86be3fc15d0d138bd8f3ccbe26def6898570d1406739f55a`.
Record source commits and any local modifications separately from binary
versions. A source export without Git history has a limited delivery identity.
Matching current source files to a commit does not establish the original
compilation inputs or guarantee identical hashes after rebuilding.

Install the assets at the paths in [the optional configuration](../../../config/nestedvirt.toml),
or adapt those paths for the dedicated node.

```text
/opt/agentenv-nestedvirt/deps/firecracker/1.15.1-nestedvirt-c7e1cce/firecracker
/opt/agentenv-nestedvirt/deps/firecracker/1.15.1-nestedvirt-c7e1cce/cpu-template-helper
/opt/agentenv-nestedvirt/vmlinux-6.18-agentenv-nestedvirt-x86_64
```

`[firecracker].binary_path` selects Firecracker. The helper is resolved from
`deps_path/firecracker/<version>/cpu-template-helper`, so both executables
must use the same version directory. `[kernel].image_path` selects the
uncompressed guest ELF. Provision the other AgentENV dependencies through the
normal installation procedure. These guest assets require no host kernel upgrade.

## Select the optional configuration

The optional file includes `no-kvmapf` in the complete outer guest command
line. Disabling outer guest KVM asynchronous page faults is a functional
workaround verified with the selected Intel assets. The exact failure mechanism
and performance effects remain unproven. AMD and ARM behavior are unverified.

`--config` selects one TOML file. It does not merge that file with
`config/default.toml`. Omitted fields use schema defaults and applicable
environment overrides. `boot_args` replaces the entire string, so the example
retains the shipped boot and DAMON reclaim settings and appends `no-kvmapf`.
Review this repeated command line when ordinary defaults change.

For an existing dedicated node, preserve its network, storage and other node
settings when adding the optional fields. Adapt the independent home, runtime
and dependency paths. Check environment overrides, especially
`AENV_HOME_PATH`, `AENV_RUNTIME_PATH` and `AENV_DEPS_PATH`, before starting it.
Select the prepared file using `--config` or `AENV_CONFIG_PATH`.

Build **new templates** and launch new sandboxes from them. Check the kernel
version, both guest CPUs, `/dev/kvm` and the complete `/proc/cmdline` in those
sandboxes. Existing templates, fork snapshots and paused sandboxes keep their
captured kernel memory and startup arguments. Changing a kernel path or
startup arguments does not upgrade them. Snapshot compatibility with other
Firecracker branches is not established.

To restore the previous deployment, stop the dedicated node normally and
restore its saved configuration and asset selections. To disable the workaround
while retaining nested KVM, remove `no-kvmapf` from the complete command line.
Both changes require new templates. The original setting stalled inner
execution in the tested API path.

## Validate snapshots and the normal API

Run `tools/nestedvirt/raw_smoke.py` from the matching Firecracker checkout.
Then run the AgentENV smoke against a dedicated node and a new template. Each
path requires three rounds for each of `unused`, `initialized` and `running`.
QEMU explicitly uses KVM. Two successive execution counter samples must grow
before running-stage capture and after fresh inner startup.

The normal API test creates two fork children, pauses and reconnects the source,
and starts a fresh inner VM in all three sandboxes. Its running stage also
checks that existing inner execution advances across fork and pause/connect.
The initialized stage replaces the restored stopped `-S` inner instance before
starting the fresh one. Continuing that same initialized inner instance is
outside this test's coverage.

Prepare the real probe root filesystem using Firecracker's
`tools/nestedvirt/prepare_probe.py`. It includes static BusyBox, real Bash,
QEMU with its shared libraries, an inner kernel and initrd, `/start-inner` and
`/stop-inner`. Use Python 3.11 or later in a dedicated virtual environment.

```sh
python3 -m pip install -r scripts/tests/nestedvirt-requirements.txt
python3 scripts/tests/serve-nestedvirt-fixture.py /absolute/path/nestedvirt-work --port 18091
```

The fixture binds loopback and serves a real OCI image. Configure only the
test node's regctl to use HTTP for this registry, through a dedicated wrapper
passing `--host reg=127.0.0.1:18091,tls=disabled`. Set a proxy domain such as
`nested.local` in `[sandbox_proxy].domains`, bind the test API to loopback, and
use network pools that do not overlap other runtimes. In another terminal
using the same virtual environment, run the following command.

```sh
python3 scripts/tests/nestedvirt-api-smoke.py \
  http://127.0.0.1:18090 /absolute/path/test-home/secrets/api-key \
  127.0.0.1:18091/nested:latest /absolute/path/evidence --rounds 3
```

The test uses the official E2B client for guest commands and reads the managed
API key without printing it. Require successful process exit and all nine
unique phase/round records in `api-results.json`. Inspect `api-commands.jsonl`
for kernel arguments, counter growth and successful guest command completion.
Retain raw results and serial logs with the exact config, source and asset
identities. Stop the fixture and dedicated test node normally after testing.
Rebuild the fixture and template when selected assets or workload files change.

Performance, precise APF completion mechanics, AMD runtime behavior and cluster
CPU-template intersections require separate evidence before making those claims.
