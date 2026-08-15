# PVM Deployment

> [!NOTE]
> **Use this guide when standard KVM is unavailable**, which commonly happens on cloud VMs where nested virtualization is not exposed. If standard KVM already works, use the [Quick Start](../getting-started/quickstart.md) instead.

> [!WARNING]
> This feature is **EXPERIMENTAL**. The PVM feature has not yet been merged into the mainline Linux kernel, and the forked kernel may not receive the same level of testing and security updates as the mainline kernel.

PVM, originally proposed in the paper [*PVM: Efficient Shadow Paging for Deploying Secure Containers in Cloud-native Environment*](https://dl.acm.org/doi/10.1145/3600006.3613158), is an alternative virtualization mode that can provide the KVM-compatible interface required by AgentENV without relying on conventional nested virtualization. After the PVM host environment is installed, AgentENV still uses `/dev/kvm` to create Firecracker microVMs.

Compared with a normal KVM deployment, a PVM deployment adds two host-level steps:

1. Install and boot a PVM-capable host kernel.
2. Load the PVM virtualization module before starting AgentENV.

AgentENV then uses its PVM-specific Firecracker and guest-kernel artifacts.

## Before You Begin

PVM is not enabled by changing only an AgentENV configuration value. The host must first be prepared with a compatible PVM kernel.

You need:

- An x86_64 Linux server.
- Root access.
- Permission to install a host kernel and reboot the server.
- A DEB-based or RPM-based Linux distribution supported by the published PVM host-kernel packages, or the ability to build the kernel from source.
- Linux kernel 6.8 or newer for the remaining AgentENV requirements.

AgentENV does **not** replace the running host kernel automatically. Prebuilt PVM host-kernel packages are published separately in the [`kvcache-ai/linux` releases](https://github.com/kvcache-ai/linux/releases). You must install the appropriate package, reboot into that kernel, and verify the PVM module before installing AgentENV.

> [!WARNING]
> Before changing kernels on a production server, confirm that you have console access or another recovery path in case the new kernel does not boot.

## Host and Guest Kernel Compatibility

The PVM host kernel and the kernel running inside the AgentENV microVM must use compatible PVM ABIs. An incompatible pair may prevent the guest from booting or cause unexpected runtime failures.

For the most predictable setup, use host and guest kernels built from the same PVM kernel version. The PVM guest kernel packaged by AgentENV is based on the [`pvm-612` branch of `virt-pvm/linux`](https://github.com/virt-pvm/linux/tree/pvm-612), at Linux version **6.12.33**. The matching prebuilt host packages are published in [`kvcache-ai/linux` release `pvm-kernel-6.12.33`](https://github.com/kvcache-ai/linux/releases/tag/pvm-kernel-6.12.33).

The host and guest require different kernel configuration options:

- **Host kernel:** enable `CONFIG_KVM_PVM=m`.
- **Guest kernel:** enable `CONFIG_PVM_GUEST`.

## How PVM Fits into AgentENV

An AgentENV node runs in exactly one virtualization mode:

| Mode | AgentENV setting | Host state |
|------|------------------|------------|
| Standard KVM | `virtualization_mode = "kvm"` | Standard KVM modules; `kvm_pvm` is not loaded |
| PVM | `virtualization_mode = "pvm"` | PVM-capable host kernel with `kvm_pvm` loaded |

The modes are intentionally isolated:

- Dependency provisioning installs only the selected Firecracker and guest kernel.
- Snapshots record the mode in which they were captured.
- Persisted paused sandboxes record their mode.
- A node refuses to restore state created in the other mode.

Do not point KVM and PVM nodes at the same persisted-sandbox directory. If nodes share a snapshot repository, ensure workloads resume only on nodes using the mode in which the snapshot was created.

## Step 1: Install a PVM-Capable Host Kernel

Use the package format for your distribution. AgentENV only requires the kernel image and modules package. The separately published headers/development package is not required unless you need to build external kernel modules on the host.

### Debian and Ubuntu

Download the kernel image:

```bash
curl -fLO \
  https://github.com/kvcache-ai/linux/releases/download/pvm-kernel-6.12.33/linux-image-6.12.33_6.12.33-7_amd64.deb
```

Install it and refresh the bootloader:

```bash
sudo dpkg -i linux-image-6.12.33_6.12.33-7_amd64.deb
sudo update-grub
```

If another installed kernel has a higher version, select Linux `6.12.33` from the bootloader's advanced options or configure it as the default boot entry before rebooting.

### RPM-Based Distributions (Fedora, RHEL, CentOS, TencentOS)

Download the kernel package:

```bash
curl -fLO \
  https://github.com/kvcache-ai/linux/releases/download/pvm-kernel-6.12.33/kernel-6.12.33_g91e9c9be4472-2.x86_64.rpm
```

Install it:

```bash
sudo rpm -ivh --oldpackage kernel-6.12.33_g91e9c9be4472-2.x86_64.rpm
```

On systems using `grubby`, select the installed PVM kernel:

```bash
sudo grubby --set-default /boot/vmlinuz-6.12.33-g91e9c9be4472
sudo grubby --default-kernel
```

### Build from Source

If the published packages are not compatible with the distribution, build the host kernel from the [`pvm-612` branch](https://github.com/kvcache-ai/linux/tree/pvm-612). Enable `CONFIG_KVM_PVM=m`, install the kernel and modules, and configure the bootloader according to the distribution's kernel-build documentation.

### Reboot and Verify

Reboot the host:

```bash
sudo reboot
```

After reconnecting, confirm that the expected kernel is active:

```bash
uname -r
```

Expected output:

```text
# DEB package
6.12.33

# RPM package
6.12.33-g91e9c9be4472
```

If `uname -r` reports the previous kernel, update the bootloader selection and reboot again before continuing.

## Step 2: Load and Verify the PVM Module

Load the module:

```bash
sudo modprobe kvm_pvm
```

Verify that it is loaded:

```bash
lsmod | grep kvm_pvm
test -d /sys/module/kvm_pvm
```

Verify that the KVM-compatible device is now available:

```bash
ls -l /dev/kvm
```

The important result of the host setup is:

- `/sys/module/kvm_pvm` exists.
- `/dev/kvm` exists.
- The AgentENV runtime account can open `/dev/kvm` for reading and writing.

After confirming that the module loads successfully, configure the kernel to load it automatically at boot (different distributions may use different directories):

```bash
echo kvm_pvm | sudo tee /etc/modules-load.d/kvm-pvm.conf
```

## Step 3: Install AgentENV in PVM Mode

### Option A: Install Script

On Ubuntu 24.04:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh \
  | sudo AENV_VIRTUALIZATION_MODE=pvm bash

sudo systemctl start aenv
```

The installer:

- Downloads `aenv-server-linux-x86_64-pvm.tar.gz`.
- Installs the PVM Firecracker and guest-kernel artifacts.
- Writes `AENV_VIRTUALIZATION_MODE="pvm"` to `/etc/default/aenv`.
- Configures the service account's access to `/dev/kvm`.

It does not install or load the PVM host kernel or `kvm_pvm`.

### Option B: Docker

Use the dedicated PVM image:

```bash
docker pull ghcr.io/kvcache-ai/aenv-server:latest-pvm

docker run --rm -it --name aenv-server \
  --device /dev/kvm \
  --privileged \
  -v /dev:/dev \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest-pvm
```

The image sets `AENV_VIRTUALIZATION_MODE=pvm` by default and contains only the PVM runtime artifacts.

### Option C: Build from Source

Select PVM for both dependency provisioning and server startup:

```bash
export AENV_VIRTUALIZATION_MODE=pvm

cargo run --bin server -- --setup-only
make start-server
```

The server generates and persists the API key under
`$AENV_HOME/secrets/api-key` on its first normal startup.

You can also set the mode in the TOML configuration:

```toml
virtualization_mode = "pvm"
```

The environment variable takes precedence over the TOML value.

Memory snapshot dirty-page tracking (`memory_snapshot.track_dirty_pages = true`) is temporarily disabled in PVM mode because this combination has not been tested.

To build a PVM Docker image:

```bash
docker build \
  --build-arg AENV_VIRTUALIZATION_MODE=pvm \
  -f deploy/docker/Dockerfile.agentenv \
  -t aenv:pvm .
```

## Step 4: Verify AgentENV

For an install-script deployment, verify the persisted mode:

```bash
grep AENV_VIRTUALIZATION_MODE /etc/default/aenv
```

Expected output:

```text
AENV_VIRTUALIZATION_MODE="pvm"
```

Inspect startup status and logs:

```bash
sudo systemctl status aenv
sudo journalctl -u aenv -f
```

Verify the API:

```bash
curl http://127.0.0.1:8000/health
```

Once the server is healthy, template creation and sandbox operations are the same as in the standard [Quick Start](../getting-started/quickstart.md).

## Multi-Node Deployment

Use a consistent virtualization mode within a runtime pool.

For Docker Compose:

- Use the PVM runtime image.
- Export `AENV_VIRTUALIZATION_MODE=pvm`.
- Prepare the host before starting the Compose stack.

For Kubernetes:

- Label x86_64 worker nodes that boot the PVM kernel.
- Load `kvm_pvm` on each selected node.
- Use the PVM AgentENV image.
- Set `AENV_VIRTUALIZATION_MODE=pvm` in the runtime DaemonSet.
- Add a node selector or affinity rule so PVM Pods cannot run on KVM nodes.

Avoid mixing KVM and PVM nodes in a pool that schedules from a shared set of snapshots unless the scheduler also enforces virtualization-mode affinity.

## Troubleshooting

### `PVM virtualization mode is only supported on x86_64 hosts`

The current machine architecture is unsupported. Deploy the PVM node on an x86_64 server or use standard KVM.

### `PVM mode requires the kvm_pvm host module to be loaded`

The AgentENV mode is set to PVM, but the host module is not active.

Check the running kernel and try loading the module:

```bash
uname -r
sudo modprobe kvm_pvm
lsmod | grep kvm_pvm
```

If `modprobe` reports that the module cannot be found, the server is not running a compatible PVM host kernel.

### `/dev/kvm` is missing after loading `kvm_pvm`

Confirm that `kvm_pvm` loaded successfully and review the kernel log:

```bash
lsmod | grep kvm_pvm
sudo dmesg | tail -n 100
```

If the module is loaded but `/dev/kvm` is still absent, verify the PVM kernel installation and boot parameters with the kernel provider.

### `/dev/kvm` is not accessible

Check ownership and service-account groups:

```bash
ls -l /dev/kvm
id aenv
```

After changing group membership, restart the service or user session.

### AgentENV downloads or uses standard KVM artifacts

Confirm the mode is present in the service environment:

```bash
grep AENV_VIRTUALIZATION_MODE /etc/default/aenv
```

Then rerun provisioning:

```bash
sudo AENV_VIRTUALIZATION_MODE=pvm \
  AENV_CONFIG_PATH=/var/lib/aenv/config/config.toml \
  AENV_HOME_PATH=/var/lib/aenv \
  /usr/local/bin/server --setup-only
```

### Snapshot or paused-sandbox mode mismatch

The persisted state was created in the other virtualization mode. Restore it on a node using its original mode, or rebuild the workload and capture a new snapshot in the target mode.
