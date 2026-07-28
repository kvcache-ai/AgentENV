# Crabbox

[Crabbox](https://crabbox.sh/)
([openclaw/crabbox](https://github.com/openclaw/crabbox)) is a sandbox client
for edit–sync–run loops from a laptop, CI job, or coding agent. Its built-in
`e2b` provider can use AgentENV's E2B-compatible control plane and host-based
sandbox data plane.

No AgentENV-side plugin or repository-local wrapper is required. The deployment
does need the optional sandbox proxy domain described below; setting only
`E2B_API_URL` is not enough for a Crabbox run.

## When to use Crabbox

| Client | Best for |
|--------|----------|
| **[aenv CLI](../getting-started/aenv-cli.md)** | Interactive shells, pause/resume, template management |
| **[Crabbox](https://crabbox.sh/)** | Repo sync + remote command execution for agents and automation |
| **[E2B SDK](./e2b.md)** | Embedding sandbox create/run/kill in application code |

Use Crabbox when the workflow is “sync this checkout, run a command in an
AgentENV sandbox, and stream the output.” Prefer `aenv` for interactive attach,
snapshot operations, and cluster administration.

## How the connection works

Crabbox uses two routes:

1. Lifecycle calls such as create, list, connect, and delete go to
   `CRABBOX_E2B_API_URL` (or `E2B_API_URL`).
2. File upload and process calls go to
   `https://{port}-{sandboxID}.{domain}`.

AgentENV returns the first configured `[sandbox_proxy].domains` entry in create,
connect, and detail responses. Crabbox uses that advertised domain for the
second route.

Crabbox's `e2b` provider does not read `E2B_SANDBOX_URL`, so the routing-header
setup used by the E2B SDK cannot replace the host-based route. In particular, a
plain `http://127.0.0.1:8000` API URL can support `doctor` and `list`, but
`warmup` and `run` also need an HTTPS sandbox proxy domain.

## Configure AgentENV routing

Choose a DNS name dedicated to sandbox traffic, for example
`sandbox.agentenv.example.com`.

For a single node, configure the server:

```bash
export AENV_SANDBOX_PROXY_DOMAINS=sandbox.agentenv.example.com
make start-server
```

For the Docker Compose or Kubernetes multi-node helpers, configure the shared
gateway/runtime value:

```bash
export SANDBOX_PROXY_DOMAINS=sandbox.agentenv.example.com
make deploy-up
```

The deployment must also provide:

- wildcard DNS for `*.sandbox.agentenv.example.com` pointing to the AgentENV
  server or gateway;
- a wildcard TLS certificate for that name;
- a TLS load balancer or reverse proxy that preserves the original `Host` and
  forwards requests to AgentENV.

The server and gateway accept only explicitly configured domains. See
[Proxy](../concepts/proxy.md), [Environment Variables](../configuration/env-vars.md),
and the relevant deployment guide for more detail.

## Install and configure Crabbox

Install the upstream CLI:

```bash
brew install openclaw/tap/crabbox
# See https://crabbox.sh/ for other platforms.
crabbox --version
```

Point Crabbox's control plane at the AgentENV API and select an existing
template:

```bash
export CRABBOX_E2B_API_URL=https://agentenv.example.com
export CRABBOX_E2B_API_KEY=e2b_000000
export CRABBOX_E2B_TEMPLATE=ubuntu
```

`CRABBOX_E2B_*` values take precedence over the corresponding `E2B_*` values.
`E2B_API_KEY` or `CRABBOX_E2B_API_KEY` must be non-empty because Crabbox checks
for it. AgentENV does not currently enforce that key, so keep the API on a
trusted network even when TLS is enabled.

AgentENV normally advertises the configured sandbox proxy domain. If an
intermediary strips the `domain` response field, set the same value explicitly:

```bash
export CRABBOX_E2B_DOMAIN=sandbox.agentenv.example.com
```

### Project config

Install the checked-in example with private permissions and adjust the template:

```bash
install -m 600 config/crabbox.example.yaml .crabbox.yaml
```

```yaml
provider: e2b
target: linux
e2b:
  template: ubuntu
  workdir: crabbox
```

Keep API destinations and credentials in explicit environment variables or
trusted user configuration. Crabbox intentionally refuses to send inherited
credentials to a destination supplied only by repository configuration.
Crabbox also requires loaded configuration files to be private (`0600`), which
is why the example uses `install` rather than a plain `cp`.

## Run against AgentENV

```bash
crabbox doctor --provider e2b

# One shot: create, sync, run, and release.
crabbox run --provider e2b --e2b-template ubuntu -- make test-unit

# Keep a warm sandbox for repeated edit/run loops.
crabbox warmup --provider e2b --e2b-template ubuntu
lease=<slug-or-cbx-id-from-warmup>
crabbox status --provider e2b --id "$lease" --wait
crabbox run --provider e2b --id "$lease" --shell 'make test-unit'
crabbox stop --provider e2b "$lease"
crabbox list --provider e2b --json
```

Under the hood, Crabbox:

1. creates an AgentENV sandbox from the selected template;
2. archive-syncs the Git-managed working set into the sandbox workdir;
3. runs the command through envd's process API and streams stdout/stderr;
4. deletes a one-shot sandbox on release, unless retention was requested.

## Notes and limits

- The `e2b` provider is a delegated-run path, not an SSH lease.
- Use a dedicated `e2b.workdir`; Crabbox rejects broad roots such as `/`,
  `/home`, and `/tmp`.
- Pause/resume, fork, and snapshot APIs remain AgentENV-native. Use `aenv` or
  the HTTP API for those operations.
- Crabbox's E2B sandbox timeout is capped at one hour.
- AgentENV currently does not enforce authorization. Do not expose its control
  or sandbox data plane directly to the public internet.

## Upstream CLI verification

The upstream Crabbox binary was also exercised against
[Islo](https://islo.dev) through Crabbox's separate `islo` provider:

```bash
islo api-key create crabbox-agentenv-smoke --show
export ISLO_API_KEY='…'

crabbox doctor --provider islo
crabbox list --provider islo --json
crabbox run --provider islo --no-sync -- echo crabbox-islo-ok
```

This verifies the upstream delegated-run CLI path; it does not replace an
AgentENV E2B smoke test. AgentENV usage remains on `--provider e2b` with the
control-plane and wildcard-domain setup above.

## Related docs

- [E2B integration](./e2b.md) — SDK setup and shared environment variables
- [Proxy](../concepts/proxy.md) — routing headers and host-based URLs
- [aenv CLI](../getting-started/aenv-cli.md) — interactive AgentENV workflows
- `config/crabbox.example.yaml` — checked-in project configuration
- [Crabbox E2B provider](https://crabbox.sh/providers/e2b.html) — upstream
  provider flags, auth, and limits
- [Crabbox Islo provider](https://crabbox.sh/providers/islo.html) — external CLI
  verification provider
