# syntax=docker/dockerfile:1
# Guest base image for Compose sandboxes. Build from the repository root:
#   docker build -f compose-image/Dockerfile .
# The server runs /usr/bin/env, /usr/local/bin/docker and
# /usr/local/bin/plain-snapshotter through envd and requires the plain storage
# driver; keep these in sync with src/orchestrator/compose.rs.
FROM golang:1.25.0-bookworm AS snapshotter
WORKDIR /src
COPY containerd-plain-snapshotter/go.mod containerd-plain-snapshotter/go.sum ./
RUN go mod download
COPY containerd-plain-snapshotter/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /plain-snapshotter .

FROM docker:latest
ARG TARGETARCH
RUN test "$TARGETARCH" = amd64 || { echo 'Compose runtime requires linux/amd64' >&2; exit 1; }
# docker:latest omits containerd; /init runs it with the plain snapshotter proxy plugin.
RUN wget -O /tmp/docker.tgz "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
    && tar -xzf /tmp/docker.tgz -C /usr/local/bin --strip-components=1 --no-same-owner docker/containerd \
    && rm /tmp/docker.tgz \
    && containerd --version
# Docker's iptables rules are validated with the legacy backend on the AgentENV guest kernel.
RUN for t in iptables ip6tables; do for s in '' -save -restore; do \
      ln -sf "$(command -v "$t-legacy$s")" "$(command -v "$t$s")"; \
    done; done \
    && iptables --version | grep -q legacy
COPY --from=snapshotter /plain-snapshotter /usr/local/bin/plain-snapshotter
COPY compose-image/containerd.toml /etc/containerd/config.toml
# Sandbox ingress targets the guest address, so Docker's raw-table direct-access
# filter adds no isolation here and would require CONFIG_IP_NF_RAW in the guest kernel.
COPY compose-image/daemon.json /etc/docker/daemon.json
# AgentENV's tools drive mounts cgroup2, starts envd and then executes /init as PID 1.
COPY --chmod=755 compose-image/init /init
ENTRYPOINT []
CMD ["/init"]
