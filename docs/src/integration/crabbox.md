# Crabbox

[Crabbox](https://crabbox.sh/)
([openclaw/crabbox](https://github.com/openclaw/crabbox)) is a third-party
sandbox client for sync-and-run loops from a laptop, CI job, or coding agent.
Its built-in `e2b` provider talks to AgentENV's E2B-compatible API, so no
AgentENV-side plugin or wrapper is required.

## Configuration

```bash
export CRABBOX_E2B_API_URL=https://agentenv.example.com
export CRABBOX_E2B_API_KEY=e2b_000000
export CRABBOX_E2B_TEMPLATE=ubuntu   # AgentENV template id or name

crabbox doctor --provider e2b
crabbox run --provider e2b -- make test-unit
```

## Sandbox routing requirement

Crabbox's `e2b` provider does not read `E2B_SANDBOX_URL`. It reaches sandboxes
over host-based URLs shaped like `https://{port}-{sandboxID}.{domain}`, using the
domain AgentENV advertises from `[sandbox_proxy].domains`. A loopback
`CRABBOX_E2B_API_URL` is therefore enough for `doctor` and `list`, but `warmup`
and `run` also need a configured sandbox proxy domain with wildcard DNS and TLS.
See [Proxy](../concepts/proxy.md) and
[Environment Variables](../configuration/env-vars.md) for that setup.

If an intermediary strips the `domain` field from AgentENV's sandbox response,
set `CRABBOX_E2B_DOMAIN` to the same value.

Installation, provider flags, and limits are documented upstream at
<https://crabbox.sh/providers/e2b.html>.
