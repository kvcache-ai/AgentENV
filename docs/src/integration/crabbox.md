# Crabbox

[Crabbox](https://crabbox.sh/) ([openclaw/crabbox](https://github.com/openclaw/crabbox)) is the recommended sandbox client for AgentENV when you want an edit–sync–run loop against Firecracker sandboxes from a laptop, CI job, or coding agent.

AgentENV exposes an E2B-compatible HTTP API. Crabbox’s built-in `e2b` provider talks to that API — install the upstream CLI, point it at your server, and run. No AgentENV-side code changes and no repo-local wrapper script.

## When to use Crabbox

| Client | Best for |
|--------|----------|
| **[aenv CLI](../getting-started/aenv-cli.md)** | Interactive shells, pause/resume, template management |
| **[Crabbox](https://crabbox.sh/)** | Repo sync + remote command execution, agent/automation workflows, auditable run evidence |
| **[E2B SDK](./e2b.md)** | Embedding sandbox create/run/kill in application code |

Use Crabbox when the workflow is “sync this checkout, run a command in an AgentENV sandbox, stream the output.” Prefer `aenv` for interactive attach, snapshot operations, and day-to-day cluster ops.

## Install

```bash
brew install openclaw/tap/crabbox
# or: https://crabbox.sh/ for other platforms
crabbox --version
```

## Point Crabbox at AgentENV

1. Run an AgentENV server ([Quick Start](../getting-started/quickstart.md)).
2. Ensure a template exists (`aenv pull …` / `aenv template list`).
3. Export the same E2B-compatible env vars used by the [E2B SDK](./e2b.md) (see also [Environment Variables](../configuration/env-vars.md)):

```bash
# Single-node example
export E2B_API_URL=http://127.0.0.1:8000
export E2B_API_KEY=e2b_000000
export CRABBOX_E2B_TEMPLATE=ubuntu   # AgentENV template id or name
```

Crabbox also accepts `CRABBOX_E2B_API_URL` / `CRABBOX_E2B_API_KEY` (these take precedence over `E2B_*`). Plain HTTP is allowed only for localhost / loopback; remote deployments should terminate TLS and use `https://…`.

### Project config

Copy the checked-in example and adjust the template name:

```bash
cp config/crabbox.example.yaml .crabbox.yaml
```

```yaml
provider: e2b
target: linux
e2b:
  apiUrl: http://127.0.0.1:8000
  template: ubuntu          # AgentENV template id or name
  workdir: crabbox          # dedicated subdirectory inside the sandbox
```

Keep keys in the environment — do not commit secrets into `.crabbox.yaml`.

## Run against AgentENV

```bash
crabbox doctor --provider e2b

# one-shot: create sandbox, sync tree, run, release
crabbox run --provider e2b --e2b-template ubuntu -- make test-unit

# warm lease for repeated agent / edit loops
crabbox warmup --provider e2b --e2b-template ubuntu
lease=<slug-or-id-from-warmup>
crabbox run --provider e2b --id "$lease" --shell 'make test-unit'
crabbox status --provider e2b --id "$lease" --wait
crabbox stop --provider e2b "$lease"
crabbox list --provider e2b --json
```

What happens under the hood:

1. Crabbox creates an AgentENV sandbox through the E2B-compatible control plane.
2. It archive-syncs the local working tree into the sandbox workdir.
3. It runs the command via the sandbox process API and streams stdout/stderr.
4. On release (unless kept), it deletes the sandbox.

## Notes and limits

- Crabbox uses the delegated `e2b` provider path: no SSH lease into the Firecracker guest.
- Prefer a dedicated `e2b.workdir` subdirectory; Crabbox rejects broad system roots such as `/` or `/tmp`.
- Pause/resume, fork, and snapshot APIs remain AgentENV-native — use `aenv` or the HTTP API for those. Crabbox covers create → sync → run → stop.
- AgentENV currently does not enforce authorization. Do not expose the API publicly; keep Crabbox pointed at a trusted network endpoint.

## Verified with Islo

openclaw/crabbox was exercised against [Islo](https://islo.dev) (`crabbox --provider islo`) to validate the delegated-sandbox client path before recommending it for AgentENV’s E2B API.

```bash
islo api-key create crabbox-agentenv-smoke --show
export ISLO_API_KEY='…'

crabbox doctor --provider islo
crabbox list --provider islo --json
crabbox run --provider islo --no-sync -- echo crabbox-islo-ok
```

AgentENV usage stays on `--provider e2b` + `E2B_API_URL`. Islo is only the external verification provider.

## Related docs

- [E2B integration](./e2b.md) — SDK and shared env vars
- [aenv CLI](../getting-started/aenv-cli.md) — interactive AgentENV workflows
- `config/crabbox.example.yaml` — checked-in Crabbox config example at the repository root
- [Crabbox E2B provider](https://crabbox.sh/providers/e2b.html) — provider flags, auth, and gotchas
- [Crabbox Islo provider](https://crabbox.sh/providers/islo.html) — verification provider
- [crabbox.sh](https://crabbox.sh/) — product overview and install
