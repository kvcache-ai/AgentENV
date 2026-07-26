# E2B CLI

The [E2B CLI](https://github.com/e2b-dev/e2b) can be pointed at a local AgentENV server for template and sandbox management.

## Setup

Install the CLI:

```bash
npm install -g @e2b/cli
```

Set environment variables. See [Environment Variables](../configuration/env-vars.md) for values per deployment mode.

```bash
# Single-node example
export E2B_API_URL=http://127.0.0.1:8000
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=e2b_000000
export E2B_ACCESS_TOKEN=dummy
```

> For local development, any non-empty value works for `E2B_API_KEY` and `E2B_ACCESS_TOKEN`. The `template list` command uses the access-token path, while sandbox lifecycle commands use the API-key path.

## Commands

### Templates

```bash
# List all templates
e2b template list --format json
```

### Sandboxes

```bash
# Create a sandbox (detached)
e2b sandbox create <template-id> --detach

# List running sandboxes
e2b sandbox list --state running --format json

# Execute a command inside a sandbox
e2b sandbox exec <sandbox-id> -- echo hello world

# Pause a sandbox
e2b sandbox pause <sandbox-id>

# Kill a sandbox
e2b sandbox kill <sandbox-id>
```
