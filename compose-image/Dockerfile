# syntax=docker/dockerfile:1
# Build from the repository root: docker build -f compose-image/Dockerfile .
FROM rust:1-bookworm AS runtime
WORKDIR /src
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
ENV CARGO_HOME=/home/builder/.cargo
COPY . .
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev pkg-config libssl-dev \
    && useradd --create-home builder && chown -R builder:builder /src
USER builder
RUN cargo build --locked --release -p aenv && strip target/release/aenv

FROM golang:1.25.0-bookworm AS snapshotter
WORKDIR /src
COPY containerd-plain-snapshotter/go.mod containerd-plain-snapshotter/go.sum ./
RUN go mod download
COPY containerd-plain-snapshotter/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /plain-snapshotter .

# Optional reproducible kernel build; ordinary image builds skip this target.
FROM debian:bookworm AS kernel
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl xz-utils make gcc libc6-dev flex bison bc libelf-dev libssl-dev \
    && useradd --create-home builder
USER builder
WORKDIR /home/builder
ARG JOBS
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
RUN <<'KERNEL'
work_dir="$PWD"
version=6.1.175
kernel_sha256=37e984a9e449eb15023b3063c3fd6f64fdd1d83af5bb6a944cd7244f40beca85
config_commit=9fbc98e4dc05d03e1f00902ba4bb796395465099
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
KERNEL

FROM scratch AS kernel-output
COPY --from=kernel /home/builder/vmlinux-compose-6.1.175 /

FROM debian:bookworm-slim
ARG TARGETARCH
ARG DOCKER_VERSION=28.4.0
ARG DOCKER_SHA256=21516934188f06d0e5f232cbde8112701f6d82899016240bc7a5d619f6b0059c
ARG COMPOSE_VERSION=2.39.4
ARG COMPOSE_SHA256=7af95166a730b87e172d4fc9aefea8725d3c6c7327d59149267b452114ddb7d4
RUN test "$TARGETARCH" = amd64 || { echo 'Compose runtime requires linux/amd64' >&2; exit 1; }
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl iptables iproute2 util-linux procps libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --set iptables /usr/sbin/iptables-legacy \
    && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
      -o /tmp/docker.tgz \
    && echo "${DOCKER_SHA256}  /tmp/docker.tgz" | sha256sum -c - \
    && tar -xzf /tmp/docker.tgz -C /usr/local/bin --strip-components=1 \
    && rm /tmp/docker.tgz \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && echo "${COMPOSE_SHA256}  /usr/local/lib/docker/cli-plugins/docker-compose" | sha256sum -c - \
    && chmod 755 /usr/local/lib/docker/cli-plugins/docker-compose
COPY --from=snapshotter /plain-snapshotter /usr/local/bin/plain-snapshotter
COPY <<'CONTAINERD' /etc/containerd/config.toml
version = 2
root = "/var/lib/containerd"
state = "/run/containerd"
disabled_plugins = ["io.containerd.grpc.v1.cri"]

[grpc]
  address = "/run/containerd/containerd.sock"

[proxy_plugins.plain]
  type = "snapshot"
  address = "/run/containerd-plain-snapshotter/snapshotter.sock"
CONTAINERD
COPY <<'DOCKER' /etc/docker/daemon.json
{
  "features": {"containerd-snapshotter": true},
  "storage-driver": "plain",
  "containerd": "/run/containerd/containerd.sock",
  "exec-opts": ["native.cgroupdriver=cgroupfs"],
  "log-driver": "json-file",
  "log-opts": {"max-size": "10m", "max-file": "2"},
  "default-address-pools": [{"base": "172.28.0.0/16", "size": 24}]
}
DOCKER
COPY <<'INIT' /init
#!/bin/sh
exec /usr/local/bin/docker-init -- /usr/local/bin/aenv compose guest-init
INIT
COPY --from=runtime /src/target/release/aenv /usr/local/bin/aenv
RUN chmod 755 /init /usr/local/bin/aenv
CMD ["/init"]
