# Build from the repository root: docker build -f compose-image/Dockerfile .
FROM rust:1-bookworm AS runtime
WORKDIR /src
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
ENV CARGO_HOME=/home/builder/.cargo
COPY . .
RUN useradd --create-home builder && chown -R builder:builder /src
USER builder
RUN cargo build --locked --release -p aenv-compose-runtime --features runtime --bins

FROM golang:1.25.0-bookworm AS snapshotter
WORKDIR /src
COPY containerd-plain-snapshotter/go.mod containerd-plain-snapshotter/go.sum ./
RUN go mod download
COPY containerd-plain-snapshotter/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /plain-snapshotter .

FROM debian:bookworm-slim
ARG TARGETARCH
ARG DOCKER_VERSION=28.4.0
ARG DOCKER_SHA256=21516934188f06d0e5f232cbde8112701f6d82899016240bc7a5d619f6b0059c
ARG COMPOSE_VERSION=2.39.4
ARG COMPOSE_SHA256=7af95166a730b87e172d4fc9aefea8725d3c6c7327d59149267b452114ddb7d4
RUN test "$TARGETARCH" = amd64 || { echo 'Compose runtime requires linux/amd64' >&2; exit 1; }
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl iptables iproute2 util-linux procps \
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
COPY compose-image/containerd.toml /etc/containerd/config.toml
COPY compose-image/daemon.json /etc/docker/daemon.json
COPY compose-image/init /init
COPY --from=runtime /src/target/release/aenv-compose-init /usr/local/bin/aenv-compose-init
COPY --from=runtime /src/target/release/aenv-compose-start /usr/local/bin/aenv-compose-start
RUN chmod 755 /init /usr/local/bin/aenv-compose-init /usr/local/bin/aenv-compose-start
CMD ["/init"]
