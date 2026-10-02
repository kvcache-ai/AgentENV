#!/usr/bin/env bash
# Build a guest kernel for Docker 28 without disabling its network protections.
# Prerequisites: curl, xz, make, gcc, flex, bison, bc, libelf-dev, libssl-dev.
set -euo pipefail
if [[ $EUID -eq 0 ]]; then
    echo "Build the kernel as a non-root user." >&2
    exit 1
fi
work_dir="${1:?usage: build-kernel.sh OUTPUT_DIRECTORY}"
mkdir -p "$work_dir"
work_dir="$(cd "$work_dir" && pwd)"
version=6.1.175
kernel_sha256=37e984a9e449eb15023b3063c3fd6f64fdd1d83af5bb6a944cd7244f40beca85
config_commit=9fbc98e4dc05d03e1f00902ba4bb796395465099
cd "$work_dir"
curl -fSL --retry 3 "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-${version}.tar.xz" -o "linux-${version}.tar.xz"
printf '%s  %s\n' "$kernel_sha256" "linux-${version}.tar.xz" | sha256sum -c -
tar -xf "linux-${version}.tar.xz"
mkdir -p build
curl -fsSL --retry 3 "https://raw.githubusercontent.com/kvcache-ai/firecracker/${config_commit}/resources/guest_configs/microvm-kernel-ci-x86_64-6.1.config" -o build/.config
# The upstream config targets patched Amazon Linux. Vanilla Linux needs PCI
# enabled for ACPI initialization, even with Firecracker's pci=off boot argument.
# https://github.com/firecracker-microvm/firecracker/issues/4881
"linux-${version}/scripts/config" --file build/.config \
    --enable PCI --enable IP_NF_RAW --enable IP6_NF_RAW
make -C "linux-${version}" O="$work_dir/build" olddefconfig
# Reserve roughly 1 GiB per compiler job; JOBS remains an explicit override.
jobs="${JOBS:-}"
if [[ -z "$jobs" ]]; then
    jobs=$(nproc)
    memory_jobs=$(awk '/^MemAvailable:/ {print int($2 / 1048576)}' /proc/meminfo)
    memory_jobs=${memory_jobs:-1}
    if (( memory_jobs < 1 )); then
        memory_jobs=1
    fi
    if (( jobs > memory_jobs )); then
        jobs=$memory_jobs
    fi
fi
make -C "linux-${version}" O="$work_dir/build" -j"$jobs" vmlinux
cp build/vmlinux "$work_dir/vmlinux-compose-${version}"
echo "Configure [kernel].image_path = \"$work_dir/vmlinux-compose-${version}\" on Compose nodes."
