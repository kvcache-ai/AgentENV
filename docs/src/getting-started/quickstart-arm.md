# ARM (aarch64) Quick Start

Build and run AgentENV from source on ARM servers (AWS Graviton, Ampere Altra,
Kunpeng, etc.). Pre-built binaries are not yet published for aarch64; follow the
source-build steps below.

## Prerequisites

- Linux aarch64 host with KVM enabled (`/dev/kvm`)
- Rust toolchain (stable)
- Docker with Buildx (for building the tools drive)
- ~25 GB disk, ~8 GB RAM

### Ubuntu / Debian

```bash
sudo apt-get install -y \
    gcc g++ make protobuf-compiler libprotobuf-dev \
    clang libclang-dev dpkg jq e2fsprogs \
    iptables iproute2 umoci zstd curl libaio1t64
```

### openEuler / CentOS / RHEL

```bash
sudo dnf install -y \
    gcc gcc-c++ make protobuf-compiler protobuf-devel \
    clang clang-devel dpkg jq e2fsprogs \
    iptables iproute umoci zstd curl libaio
```

> **ublk**: enabled by default (overlaybd snapshots + on-demand loading).
> Works on Ubuntu/Debian ARM out of the box. openEuler kernels ship with
> `CONFIG_BLK_DEV_UBLK` disabled — if you need full functionality on
> openEuler, rebuild the host kernel with ublk enabled.

## 1. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

If behind a firewall, download the script manually and use a mirror:

```bash
curl -o rustup-init.sh https://sh.rustup.rs
chmod +x rustup-init.sh
RUSTUP_UPDATE_ROOT=https://mirrors.aliyun.com/rustup/rustup \
RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup \
./rustup-init.sh -y
source "$HOME/.cargo/env"
```

## 2. Build the ARM tools drive

The pre-built tools drive image is currently amd64-only. Build the ARM variant:

```bash
cd tools-image
make build ARCH=arm64
# output: out/tools-0.1.0-arm64.ext4
cd ..
```

## 3. Build AgentENV

```bash
ulimit -n 65536
make release
```

The first build takes 20–40 minutes (`librocksdb-sys` compiles a large C++
codebase). On memory-constrained hosts use `CARGO_BUILD_JOBS=2 make release`.

## 4. Configure

Create an ARM configuration:

```bash
cp config/default.toml config/arm.toml
```

Edit `config/arm.toml`:

```toml
# Path to the ARM tools drive built in step 2.
[tools]
drive_path = "/path/to/AgentENV/tools-image/out/tools-0.1.0-arm64.ext4"
version = "0.1.0"

# ARM uses MMIO, not PCI.
[firecracker]
boot_args = "console=ttyS0 reboot=k panic=1 init=/init"
```

> **ublk (overlaybd storage)**: ublk is enabled by default and works on ARM hosts
> where the kernel has `CONFIG_BLK_DEV_UBLK` (Ubuntu ARM, Debian ARM, etc.).
> If your host kernel lacks ublk (e.g. openEuler default), disable it:
> ```toml
> [ublk]
> enabled = false
> ```

## 5. First-run setup

```bash
# One-time host provisioning (KVM group, network sysctls).
sudo ./target/release/server \
    --config config/arm.toml \
    --setup-host \
    --runtime-user "$USER" \
    --runtime-group "$(id -gn)"

# Download runtime dependencies (Firecracker aarch64, kernel, overlaybd, regctl).
./target/release/server --config config/arm.toml --setup-only
```

If the server cannot reach GitHub, download these manually and place them at the
expected paths:

| Asset | Destination |
|-------|-------------|
| [firecracker-1.15.1-patch-v1-aarch64.tgz](https://github.com/kvcache-ai/firecracker/releases/download/aenv-deps/firecracker-1.15.1-patch-v1-aarch64.tgz) | Extract to `/var/lib/aenv/deps/firecracker/1.15.1-patch-v1/firecracker` |
| [vmlinux-6.1.175-aarch64](https://github.com/kvcache-ai/firecracker/releases/download/aenv-deps/vmlinux-6.1.175-aarch64) | `/var/lib/aenv/deps/kernel/vmlinux-6.1.175/aarch64/vmlinux.bin` |
| [regctl-linux-arm64](https://github.com/regclient/regclient/releases/download/v0.11.5/regctl-linux-arm64) | `/var/lib/aenv/deps/regctl/v0.11.5/regctl` (chmod +x) |
| [overlaybd aarch64 .deb](https://github.com/containerd/overlaybd/releases/tag/v1.0.18) | Extract with `dpkg-deb -x`, copy `opt/overlaybd/` to `/var/lib/aenv/deps/overlaybd/` |

## 6. Start the server

```bash
sudo -E env API_ADDR=0.0.0.0:8000 \
    ./target/release/server --config config/arm.toml
```

Verify:

```bash
curl http://127.0.0.1:8000/health
# HTTP 204 No Content
```

## Troubleshooting

### `/dev/kvm` not accessible

```bash
sudo modprobe kvm
sudo usermod -aG kvm "$USER"
# re-login
ls -l /dev/kvm
```

### `unsupported Linux distribution`

openEuler / CentOS / RHEL hosts may need the distro-recognition patch
(see `src/setup/packages.rs`). Ensure you are on the `feat/arm-support` branch.

### Too many open files

```bash
ulimit -n 65536
```

### protoc errors

```bash
sudo dnf install -y protobuf-compiler protobuf-devel   # RPM
sudo apt-get install -y protobuf-compiler               # DEB
```

### Firecracker pool prime timed out

Harmless warning on first start. The pool warms lazily on first sandbox creation.
