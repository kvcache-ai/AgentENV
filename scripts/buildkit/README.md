# BuildKit Integration Check

`aenv build` runs BuildKit in a temporary AgentENV microVM. Context travels over
an authenticated WebSocket connection; the node imports the resulting image
directly from BuildKit's content store into its existing OverlayBD cache.

Run the integration check against one authenticated, configured AgentENV runtime
node (not a gateway distributing builds between nodes):

```bash
BUILDCTL_BIN=/path/to/aenv-buildctl make test-buildkit
```

The test creates uniquely named templates and uses the shared managed cache. It
checks multi-stage builds, ignored files, changed files, cache persistence across
template names and replacement builders, unchanged rebuilds, failed builds,
concurrent builds and cancellation, and Dockerfile HEALTHCHECK readiness.
On Linux with `script` installed, it also checks the interactive progress bar.
Its trap removes its templates and sandboxes, retaining the managed cache. It requires `buildctl` (included
by the AgentENV installers) and an AgentENV host with working VM prerequisites.
The CLI reads its ordinary
credentials, including an alternate `XDG_CONFIG_HOME` when configured.
The single-node E2E CI job runs both scripts with a 2-vCPU, 2048-MiB builder
and an 8192-MiB cache.

`test-users.sh` checks numeric root, an existing non-root UID, a UID without an
account, and an explicit UID/GID pair during template startup and after restore.

Builder image, CPU, memory, and disk size come from `[template_build]` on the
server. Defaults are 16 vCPUs, 32768 MiB memory, and 262144 MiB cache disk; smaller
test hosts can override these in their server config. The fixture uses a unique
cache-mount ID per test run, seeds it before `COPY`, and requires its marker after
a file update. This checks both instruction cache and mutable-cache reuse.

Sandbox deletion freezes and seals writable volumes before stopping Firecracker,
without capturing VM state. Builders and ordinary sandboxes share this path.
The Rust integration test
`snapshot_attached_drive::frozen_volumes_survive_deletion_without_vm_snapshot`
checks multiple volumes, thaw after a partial freeze failure, subPath mounts,
non-root default users, and dirty-file preservation without journal recovery in
a replacement VM.

See [template builds](../../docs/src/concepts/templates.md#aenv-build) for CLI
flags and the additive API. BuildKit's protocol and Dockerfile implementation
are provided by [upstream BuildKit](https://github.com/moby/buildkit).
