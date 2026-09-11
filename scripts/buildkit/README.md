# BuildKit integration tests

Run the suite against an authenticated, otherwise idle AgentENV node or gateway:

```bash
BUILDCTL_BIN=/path/to/aenv-buildctl make test-buildkit
```

The suite uses the CLI's credentials (`aenv auth`, or an isolated
`XDG_CONFIG_HOME`) and requires Python 3.11+, BuildKit's client, and working VM
prerequisites. Observability must be enabled so it can check worker counts.
Before each test, the suite waits for every node to report no running, starting,
or paused sandboxes so stale gateway heartbeats cannot enter the cleanup baseline.
Builds create unique template names; cleanup deletes their templates and
sandboxes, retaining the managed cache. Logs remain in the printed temporary
directory when a check fails.

`test.py` contains named tests for:

- Multi-stage output, `.dockerignore`, changed context files, and startup readiness.
- Instruction-cache hits and invalidation, plus persistent cache mounts across
  workers. Random markers in the output distinguish cache reuse from reexecution;
  `--no-cache` must rerun instructions and reset the affected cache mounts,
  following the pinned BuildKit version.
- Concurrent workers producing independent runnable templates from the same seed.
- Automatic publication after the client is killed immediately following a successful solve.
- Failed solves, cancellation, deadlines, terminal API status, and subsequent
  cache reuse. Node counts must return to their initial values, and only one ready
  read-only cache seed may remain after workers and retired volumes are released.
- Worker isolation, authentication, startup overrides, numeric users, external
  Dockerfiles, secret mounts, and terminal/nonterminal progress.

The numeric-user cases also run alone with `make test-buildkit-users`. To run one
other case, invoke the Python test directly with the built CLI:

```bash
AENV_BIN=target/debug/aenv BUILDCTL_BIN=/path/to/aenv-buildctl \
  python3 scripts/buildkit/test.py BuildKitTests.test_concurrent_builds
```

The single-node and Docker Compose E2E jobs both run the full suite, including
build traffic through the gateway in Compose. They use
[`test-config.toml`](test-config.toml), with 2-vCPU, 2048-MiB builders and 8192-MiB
cache disks. [`install-buildctl.sh`](install-buildctl.sh) downloads the pinned
client and verifies its release digest. It defaults to the host platform and
accepts an explicit platform for cross-platform release packaging.

[`package-cli.sh`](../release/package-cli.sh) uses the same downloader to produce
a CLI archive containing `aenv`, its private `aenv-buildctl`, and `manifest.json`.
The release workflow publishes one archive per supported platform plus
`SHA256SUMS`. Both installers consume those archives without downloading BuildKit
separately. `make test-unit` exercises the packager and installers with fixture
release assets, including checksum failures and incomplete bundles.

Builder settings are under `[template_build]`; they do not change the resulting
template's resources. Worker deletion uses the orchestrator's normal volume
freeze, capture, stop, and release path. Cache capture is optional, while failed
stops retain ownership for cleanup to retry.

See [template builds](../../docs/src/concepts/templates.md#aenv-build) for CLI
flags, cache ownership, and the API.
