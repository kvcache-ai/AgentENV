# Intel IAA Compression (QPL)

AgentENV can compress published snapshot layers with the
[Intel Query Processing Library (QPL)](https://www.intel.com/content/www/us/en/developer/tools/query-processing-library/overview.html).
QPL offloads DEFLATE compression and decompression to the Intel In-Memory
Analytics Accelerator (IAA), which is built into 4th Gen and later Intel Xeon
Scalable processors. This moves the CPU cost of compressing layers on publish,
and of decompressing them when sandboxes read them, onto the accelerator.

Using QPL takes two opt-ins: AgentENV must be built with the `qpl` feature,
and the node configuration must select the `qpl` compression algorithm.

> [!NOTE]
> Prebuilt release binaries and Docker images are built without the `qpl`
> feature. Build AgentENV from source to use QPL.

## How It Works

QPL is one of the algorithms of
[`[snapshot.publish_compression]`](../configuration/reference.md#snapshotpublish_compression).
Local layers always stay raw. When a snapshot is published, memory layers and
incremental read-write layers are rewritten as ZFile layers in which every
4 KiB block is one raw DEFLATE stream compressed by QPL.

- The server compresses layers when it publishes snapshots.
- `uvm-ublk-daemon` decompresses blocks on demand when a sandbox reads a
  QPL-compressed layer.
- Runs of blocks are submitted to IAA up to 32 at a time, so the accelerator
  stays busy while the calling thread waits.

Each process creates a pool of QPL jobs the first time it uses QPL. If a
hardware job can be created, the pool uses IAA (the hardware path). Otherwise
it uses QPL's software implementation on the CPU (the software path) for the
life of the process. Both paths produce and read the same data, so the
software path is a functional fallback rather than a performance mode.

## Compatibility

QPL layers use ZFile compression codec 3, which is specific to AgentENV:

- Only AgentENV builds with the `qpl` feature can read them, on either path.
  Upstream OverlayBD and AgentENV builds without the feature cannot, and a
  resume from such a layer fails on those nodes.
- Every node that may resume a snapshot published with `qpl` must be built
  with the feature. Do not select `qpl` in a cluster that also runs nodes
  without it.
- When `[snapshot.image_publish]` is enabled, QPL layers are also pushed to
  the source registry, where other OverlayBD consumers cannot read them.
- Changing `algorithm` later affects only new publications. Snapshots already
  published with `qpl` still need QPL-enabled nodes.

## Requirements

Build host:

- x86_64.
- QPL 1.9.0 installed as a shared library with its pkg-config file
  (`qpl.pc`). AgentENV's bindings match QPL 1.9.0 exactly, and the build
  fails if pkg-config finds another version.
- QPL's build prerequisites when building it from source: CMake 3.16.3 or
  newer, NASM 2.15 or newer, and a C++17 compiler.

Every runtime node:

- `libqpl.so.1` from the same QPL 1.9.0 installation, and `libstdc++`.

Runtime nodes that use the hardware path additionally need:

- A 4th Gen or later Intel Xeon Scalable processor with IAA devices.
- Intel VT-d enabled in the BIOS and `intel_iommu=on,sm_on` on the kernel
  command line.
- The `idxd` driver with shared virtual memory support
  (`CONFIG_INTEL_IDXD_SVM`). Linux 5.18 supports IAA 1.0 and Linux 6.3 supports
  IAA 2.0; AgentENV already requires Linux 6.8.
- `accel-config` and its library `libaccel-config`, which QPL loads at
  runtime.
- Enabled IAA work queues configured as described in
  [Step 2](#step-2-configure-iaa-work-queues).

## Enable QPL Compression

### Step 1: Install QPL 1.9.0

Install QPL 1.9.0 on the build host and on every runtime node. If your
distribution ships QPL 1.9.0 as a shared library with `qpl.pc`, install that
package. Otherwise build it from source:

```bash
git clone --branch v1.9.0 https://github.com/intel/qpl.git
cmake -S qpl -B qpl/build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr \
  -DQPL_LIBRARY_TYPE=SHARED \
  -DQPL_BUILD_TESTS=OFF \
  -DQPL_BUILD_EXAMPLES=OFF
cmake --build qpl/build --parallel "$(nproc)"
sudo cmake --install qpl/build
```

Confirm that pkg-config finds it:

```bash
pkg-config --modversion qpl
```

The command must print `1.9.0`. If you install QPL under another prefix, add
the directory that contains `qpl.pc` to `PKG_CONFIG_PATH` when building, and
make `libqpl.so.1` visible to the dynamic loader on runtime nodes.

### Step 2: Configure IAA Work Queues

Skip this step if the node only uses the software path.

IAA devices are disabled at boot. List them:

```bash
lspci -d 8086:0cfe
ls -d /sys/bus/dsa/devices/iax*
```

Configure every IAA device with one shared work queue that uses all of the
device's engines:

```bash
for dev in /sys/bus/dsa/devices/iax*; do
  iax=$(basename "$dev")
  id=${iax#iax}
  size=$(cat "$dev/max_work_queues_size")
  for engine in $(seq 0 $(($(cat "$dev/max_engines") - 1))); do
    sudo accel-config config-engine "$iax/engine$id.$engine" -g 0
  done
  sudo accel-config config-wq "$iax/wq$id.0" -g 0 -m shared -y user -d user \
    -n agentenv -s "$size" -t "$size" -p 10 -b 1
  sudo accel-config enable-device "$iax"
  sudo accel-config enable-wq "$iax/wq$id.0"
done
```

Keep these settings:

- **`-b 1` (`block_on_fault=1`) is required.** IAA writes decompressed blocks
  directly into buffers whose pages may not be resident yet, and the work
  queue must let the device wait for the kernel to fault them in.
- **Shared mode (`-m shared`).** QPL only uses shared work queues.
- **Every IAA device.** Batched QPL jobs use only the devices on the NUMA node
  of the calling thread, so do not leave a NUMA node without an enabled work
  queue.

Give the AgentENV runtime user access to the work queue device nodes, for
example:

```bash
sudo chown -R <runtime-user> /dev/iax
```

The configuration does not survive a reboot. Save it once, and load it at
boot before AgentENV starts, for example from a systemd unit:

```bash
sudo accel-config save-config -s /etc/accel-config/agentenv.conf
sudo accel-config load-config -c /etc/accel-config/agentenv.conf -e
```

### Step 3: Build AgentENV with QPL

Pass `QPL=1` to the build targets. It adds the `qpl` feature to both the
server, which compresses layers, and `uvm-ublk-daemon`, which decompresses
them:

```bash
make release QPL=1
API_ADDR=0.0.0.0:8000 make start-server-release QPL=1
```

See [Manual Compile](./manual-compile.md) for the rest of the source build.

### Step 4: Select QPL in the Configuration

In the AgentENV config file (`config/default.toml`, or the file selected by
`AENV_CONFIG_PATH`):

```toml
[snapshot.publish_compression]
enabled = true
algorithm = "qpl"
```

A build without the `qpl` feature refuses to start with this setting. The
job pool sizes and the decompression mode are under
[`[qpl]`](../configuration/reference.md#qpl); the defaults suit most nodes.

### Step 5: Verify

Each process creates its job pool on first use, so the log lines below appear
on the first snapshot publication (server) or the first read of a QPL layer
(`uvm-ublk-daemon`), not at startup:

- `QPL job pool ready` with `path=Hardware`: IAA is in use.
- `IAA hardware path unavailable; QPL job pool uses the software path`: no
  usable work queue was found. Check Step 2 and the device node permissions.
- `cannot lock QPL buffers in memory`: raise `RLIMIT_MEMLOCK` for the
  AgentENV processes.

## References

- [Intel QPL overview](https://www.intel.com/content/www/us/en/developer/tools/query-processing-library/overview.html)
- [Intel QPL installation guide](https://intel.github.io/qpl/documentation/get_started_docs/installation.html)
- [Intel IAA user guide](https://www.intel.com/content/www/us/en/content-details/780887/intel-in-memory-analytics-accelerator-intel-iaa.html)
- [accel-config (idxd-config)](https://github.com/intel/idxd-config)
