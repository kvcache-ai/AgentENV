# AgentENV envd Tools Drive

This directory contains the source for the envd tools drive attached to every
Firecracker guest as `/dev/vda`.

This source is here so contributors can inspect and reproduce the tools drive
that AgentENV consumes as a prebuilt runtime asset. Normal `make start-server`
does not rebuild this image; launch downloads and converts the configured tools
release from `config/deps_manifest.toml` when it is missing locally, unless
`tools.drive_path` points at a local ext4 file.

The build is intentionally self-contained:

1. Clone and compile `envd` from `e2b-dev/infra` at `ENVD_REF`.
2. Assemble the guest tools rootfs with BusyBox, `/init`, `/agentenv/pivot-init`,
   and `/agentenv/envd`.
3. Export the tools rootfs as an OCI image with the
   `io.agentenv.tools-drive.format=oci-rootfs-v1` label.

Requirements:

- Docker with the Buildx plugin available (`docker buildx version`).
- The default `GO_VERSION` is aligned with the default upstream `ENVD_REF`.
  Override it from `docker buildx build` only if the selected envd source
  supports a different Go toolchain.

## Local Build

From this directory:

```bash
make
```

From the repository root:

```bash
make -C tools-image
```

The image is loaded into the local Docker image store as:

```text
agentenv-tools:<TOOLS_VERSION>
```

## Versioning

`TOOLS_VERSION` is the SemVer release of the complete drive, including envd,
BusyBox, and the init scripts. Published versions are immutable: any byte-level
change requires a new version. `ENVD_REF` remains the upstream `e2b-dev/infra`
ref used to compile envd and is not necessarily the same string as the version
reported by the binary.

Official releases use normal versions such as `0.1.0`. Custom distributions use
a prerelease identifier that is unique within the AgentENV deployment, such as
`0.1.0-custom.1`. Do not publish different drive contents under the same
version.

AgentENV's runtime config records the expected in-guest envd version:

```toml
[envd]
version = "..."
```

After building a tools drive, use the build log to find the value printed by:

```bash
/out/envd -version
```

That is the value that should match `[envd].version`. For cross-architecture
builds, the Dockerfile skips executing the target binary in the builder; run
`docker run --rm --entrypoint /agentenv/envd <image> -version` on a matching Linux
host instead. The build also prints, for native builds:

```bash
/out/envd -commit
```

which identifies the upstream commit baked into the binary.

## Configuration

The build accepts these Make variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TOOLS_VERSION` | `0.1.2` | Immutable SemVer release of the complete tools drive |
| `ENVD_REF` | `2026.17` | Tag, branch, or fetchable commit to build from the envd upstream repository |
| `ENVD_UPSTREAM_REPO` | `https://github.com/e2b-dev/infra.git` | Repository containing `packages/envd` |
| `ARCH` | host architecture, normalized to `amd64` or `arm64` | Target architecture |
| `PUBLISH_PLATFORMS` | `linux/amd64,linux/arm64` | Platforms included in the published OCI image |
| `IMAGE` | `agentenv-tools:${TOOLS_VERSION}` | Local or remote image tag |
| `DOCKER` | `docker` | Docker CLI command |

Examples:

```bash
make TOOLS_VERSION=0.1.2 ENVD_REF=2026.17 ARCH=amd64

make \
  ENVD_UPSTREAM_REPO=https://github.com/e2b-dev/infra.git \
  TOOLS_VERSION=0.1.2 \
  ENVD_REF=2026.17 \
  ARCH=amd64
```

## Publish

The `publish` target builds a multi-platform artifact and pushes it to `IMAGE`.
It accepts SemVer releases and prereleases without build metadata, requires the
image tag to match that version, and refuses to overwrite a tag found by its
preflight check. That check is not atomic: the registry must enforce immutable
tags to prevent concurrent or external publishers from replacing a release.

```bash
make publish \
  TOOLS_VERSION=0.1.2 \
  ENVD_REF=2026.17 \
  IMAGE=ghcr.io/kvcache-ai/agentenv-tools:0.1.2

make publish \
  TOOLS_VERSION=0.1.1-custom.1 \
  ENVD_REF=2026.17 \
  IMAGE=registry.example.com/custom/agentenv-tools:0.1.1-custom.1
```

The **Publish Tools Image** workflow also publishes a selected version. Its
`tools_version` default only prefills the publication form; it does not select
the runtime's default tools version.

After publishing and validating a release (for example `0.1.2`), update
`[tools].version` in `config/deps_manifest.toml` to that version, then build and
roll out AgentENV through the existing deployment process. The manifest is
embedded in the binary; `server --setup-only` prepares the default tools release
for dependency bundles. Deployments without an explicit `[tools].version`
override then use the new release for new sandboxes and templates. Existing
snapshots and paused sandboxes retain their recorded version.

OCI rootfs tools images require a runtime with `oci-rootfs-v1` support. Complete
the runtime rollout before changing a shared configuration to such a release.
Runtime releases do not need a new tools version unless the tools contents
change. Git revisions and OCI digests remain release provenance; snapshots
persist only `TOOLS_VERSION`.

AgentENV converts v1 images locally with the pinned OverlayBD
`v1.0.18-aenv.1` converter and 64GiB sparse geometry; publishers do not need to
build OverlayBD artifacts. Converted layers stay in the node's versioned tools
directory, independently of image-cache eviction. Cold nodes reconstruct old
releases from the tools URL; keep those releases immutable and available while
snapshots use them. Tools are not uploaded to snapshot storage. Native OverlayBD
images also work and automatically download tools layers in the background.

## Local Verification

To test a custom tools release, publish it to a registry reachable by the
AgentENV nodes and select it:

```toml
[tools]
version = "0.1.1-custom.1"
url = "registry.example.com/custom/agentenv-tools:{version}"
```

Images without the format label retain the legacy `/tools.ext4` wrapper
convention. Existing local ext4 files can still be imported with
`tools.drive_path` and an explicit version; changing their contents requires a
new version.

Root filesystem resizing is performed by the host-side `overlaybd-resize`
binary installed from the OverlayBD package under `deps_path`; it is not part
of this guest tools drive.
