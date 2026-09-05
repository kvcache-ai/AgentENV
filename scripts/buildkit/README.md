# BuildKit Integration Check

`aenv build` runs BuildKit in a temporary AgentENV microVM. Context travels over
an authenticated WebSocket connection; the node imports the resulting image
directly from BuildKit's content store into its existing OverlayBD cache.

Run the integration check against an authenticated, configured AgentENV server:

```bash
AENV_BIN=target/debug/aenv BUILDCTL_BIN=buildctl bash scripts/buildkit/test.sh
```

The test creates uniquely named templates and an exclusive cache volume. It
checks multi-stage builds, ignored files, changed files, cache persistence across
replacement builders, unchanged rebuilds, failed builds, and template startup.
On Linux with `script` installed, it also checks the interactive progress bar.
Its trap removes only the resources it creates. It requires `buildctl` (included
by the AgentENV installers) and an AgentENV host with working VM prerequisites.
The CLI reads its ordinary
credentials, including an alternate `XDG_CONFIG_HOME` when configured.

For manual experiments, start with a fresh cache volume. The fixture seeds a
cache mount in an earlier instruction and requires that marker after `COPY`,
so a file update must preserve both the cached instruction and mutable cache.

See [template builds](../../docs/src/concepts/templates.md#aenv-build) for CLI
flags and the additive API. BuildKit's protocol and Dockerfile implementation
are provided by [upstream BuildKit](https://github.com/moby/buildkit).
