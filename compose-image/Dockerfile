# syntax=docker/dockerfile:1
# Build from the repository root with the snapshotter as context:
#   docker build -f compose-image/Dockerfile containerd-plain-snapshotter
FROM golang:1.25.0-bookworm AS snapshotter
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -o /plain-snapshotter .

FROM debian:bookworm-slim
ARG TARGETARCH
ARG DOCKER_VERSION=28.4.0
ARG DOCKER_SHA256=21516934188f06d0e5f232cbde8112701f6d82899016240bc7a5d619f6b0059c
ARG COMPOSE_VERSION=2.39.4
ARG COMPOSE_SHA256=7af95166a730b87e172d4fc9aefea8725d3c6c7327d59149267b452114ddb7d4
RUN test "$TARGETARCH" = amd64 || { echo 'Compose runtime requires linux/amd64' >&2; exit 1; }
# Docker's iptables rules are validated with the legacy backend on the AgentENV guest kernel.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl iptables iproute2 procps \
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
# Sandbox ingress targets the guest address, so Docker's raw-table direct-access
# filter adds no isolation here and would require CONFIG_IP_NF_RAW in the guest kernel.
COPY <<'DOCKER' /etc/docker/daemon.json
{
  "features": {"containerd-snapshotter": true},
  "storage-driver": "plain",
  "allow-direct-routing": true,
  "containerd": "/run/containerd/containerd.sock",
  "exec-opts": ["native.cgroupdriver=cgroupfs"],
  "log-driver": "json-file",
  "log-opts": {"max-size": "10m", "max-file": "2"},
  "default-address-pools": [{"base": "172.28.0.0/16", "size": 24}]
}
DOCKER
# AgentENV's tools drive mounts cgroup2, starts envd and then executes /init as
# PID 1. The server then runs /usr/bin/env, /usr/local/bin/docker and
# /usr/local/bin/plain-snapshotter through envd and requires the plain storage
# driver; keep these in sync with src/orchestrator/compose.rs.
COPY --chmod=755 <<'INIT' /init
#!/bin/sh
# Re-execute under tini so orphaned container processes are reaped.
[ "$$" -eq 1 ] && exec /usr/local/bin/docker-init -- "$0"
# Start the daemons only once, as tini's child: a second run would replace the
# live snapshotter socket.
[ "$PPID" -eq 1 ] || { echo 'agentenv-compose: /init must run as the guest init' >&2; exit 1; }
# Processes started by docker exec inherit the daemons' umask.
umask 022
log=/var/log/agentenv-compose
mkdir -p "$log" && chmod 700 "$log"
start() {
    name=$1 socket=$2
    shift 2
    "$@" >>"$log/$name.log" 2>&1 </dev/null &
    tries=600
    until [ -S "$socket" ]; do
        if [ "$tries" -eq 0 ]; then
            echo "agentenv-compose: $name did not create $socket; see $log/$name.log" >&2
            return 1
        fi
        tries=$((tries - 1))
        sleep 0.1
    done
}
# Never restart the daemons: the snapshotter keeps active snapshots in memory,
# and VM snapshots preserve the running processes.
start snapshotter /run/containerd-plain-snapshotter/snapshotter.sock /usr/local/bin/plain-snapshotter &&
    start containerd /run/containerd/containerd.sock /usr/local/bin/containerd --config /etc/containerd/config.toml &&
    start docker /var/run/docker.sock /usr/local/bin/dockerd --config-file /etc/docker/daemon.json
# Stay alive even if a daemon fails: PID 1 exiting would panic the guest and take
# envd and these logs with it. The server reports Docker readiness failures.
while :; do wait; sleep 3600; done
INIT
CMD ["/init"]
