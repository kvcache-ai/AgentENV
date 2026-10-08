# syntax=docker/dockerfile:1.7

# AgentENV envd tools drive.
#
# This image is a reproducible build recipe for the tools drive that
# AgentENV attaches to each Firecracker guest as /dev/vda. The guest boots from
# this drive, pivots into the user rootfs on /dev/vdb, then keeps /agentenv
# bind-mounted so the compiled envd binary and BusyBox helpers remain available.
#
# The final artifact stage is a scratch image containing the tools rootfs.
# AgentENV downloads and converts each immutable release locally to OverlayBD.
# Normal server startup does not rebuild this Dockerfile; this source exists
# so the runtime artifact can be audited and reproduced.
#
# Build example:
#
#   make -C tools-image TOOLS_VERSION=0.1.2 ENVD_REF=2026.17
#
# Important build args:
#   TOOLS_VERSION        Immutable AgentENV tools drive release version.
#   ENVD_REF             Tag/branch/commit to build from e2b-dev/infra.
#   ENVD_UPSTREAM_REPO   envd source repository. Override for forks.
#   GO_VERSION           Go toolchain used to compile envd.
#   BUSYBOX_IMAGE        BusyBox image copied into the guest tools rootfs.

ARG BUSYBOX_IMAGE=busybox:1.37.0-musl
ARG DEBIAN_VERSION=bookworm
ARG GO_VERSION=1.25.9
ARG TOOLS_VERSION=0.1.2

# ---------------------------------------------------------------------------
# Stage 1: compile envd from upstream source.
#
# The tools drive build pins an upstream ref, compiles a static envd binary,
# and injects the resolved git commit into the binary for `envd -commit`.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_VERSION} AS envd-builder

ARG ENVD_REF=2026.17
ARG ENVD_UPSTREAM_REPO=https://github.com/e2b-dev/infra.git
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN set -eux; \
    git init infra; \
    cd infra; \
    git remote add origin "$ENVD_UPSTREAM_REPO"; \
    git fetch --depth 1 origin "$ENVD_REF"; \
    git checkout --detach FETCH_HEAD

WORKDIR /src/infra/packages/envd

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    set -eux; \
    case "$TARGETARCH" in \
      amd64|arm64) ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    commit_sha="$(git -C /src/infra rev-parse --short HEAD)"; \
    CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
      go build \
        -ldflags "-X=main.commitSHA=${commit_sha} -s -w" \
        -o /out/envd \
        .; \
    chmod 0755 /out/envd; \
    build_arch="$(go env GOARCH)"; \
    if [ "$TARGETARCH" = "$build_arch" ]; then \
      /out/envd -version; \
      /out/envd -commit; \
    else \
      echo "Skipping envd execution smoke test for cross-built ${TARGETARCH} binary on ${build_arch} builder"; \
    fi

# ---------------------------------------------------------------------------
# Stage 2: assemble the guest-visible tools rootfs.
#
# This is not a normal Linux distribution rootfs. It only contains the minimal
# bootstrap files needed before and after pivot_root:
#   /init                         PID 1 before pivoting into the user rootfs
#   /agentenv/pivot-init          post-pivot bootstrap
#   /agentenv/envd                compiled control-plane daemon
#   /agentenv/bin/busybox         stable helper binary for init scripts
# ---------------------------------------------------------------------------
FROM --platform=${TARGETPLATFORM} ${BUSYBOX_IMAGE} AS rootfs

ARG TOOLS_VERSION

COPY --chmod=0755 init /tools-rootfs/init
COPY --chmod=0755 pivot-init /tools-rootfs/agentenv/pivot-init
COPY --chmod=0755 --from=envd-builder /out/envd /tools-rootfs/agentenv/envd

RUN set -eux; \
    mkdir -p \
      /tools-rootfs/dev \
      /tools-rootfs/etc \
      /tools-rootfs/mnt/user \
      /tools-rootfs/proc \
      /tools-rootfs/sys/fs/cgroup \
      /tools-rootfs/agentenv/bin; \
    cp /bin/busybox /tools-rootfs/agentenv/bin/busybox; \
    printf '%s\n' "$TOOLS_VERSION" > /tools-rootfs/agentenv/tools-drive-version; \
    ln -sf /proc/self/mounts /tools-rootfs/etc/mtab; \
    chmod 0755 /tools-rootfs/agentenv/bin/busybox; \
    /tools-rootfs/agentenv/bin/busybox --list | grep -qx 'runsv' || \
      { echo "BusyBox runsv applet missing; rebuild with CONFIG_RUNSV=y" >&2; exit 1; }; \
    /tools-rootfs/agentenv/bin/busybox --list | grep -qx 'svlogd' || \
      { echo "BusyBox svlogd applet missing; rebuild with CONFIG_SVLOGD=y" >&2; exit 1; }; \
    /tools-rootfs/agentenv/bin/busybox --list | grep -qx 'nice' || \
      { echo "BusyBox nice applet missing; rebuild with CONFIG_NICE=y" >&2; exit 1; }; \
    /tools-rootfs/agentenv/bin/busybox --list | grep -qx 'ionice' || \
      { echo "BusyBox ionice applet missing; rebuild with CONFIG_IONICE=y" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Stage 3: export contract.
#
# Export the rootfs directly. The format label selects AgentENV's stable
# local OverlayBD conversion contract.
# ---------------------------------------------------------------------------
FROM scratch AS artifact

ARG TOOLS_VERSION
LABEL org.opencontainers.image.title="AgentENV tools drive" \
      org.opencontainers.image.version="${TOOLS_VERSION}" \
      io.agentenv.tools-drive.format="oci-rootfs-v1"

COPY --from=rootfs /tools-rootfs/ /
# This artifact image is never meant to run. Keep an invalid command so an
# accidental `docker run` fails immediately instead of appearing useful.
CMD ["/nonexistent"]
