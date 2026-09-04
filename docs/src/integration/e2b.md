# E2B

## E2B SDK

AgentENV exposes an E2B-compatible API, so the official [E2B SDK](https://github.com/e2b-dev/e2b) works out of the box.

### General Settings

Set environment variables to point at your AgentENV server. See [Environment Variables](../configuration/env-vars.md) for values per deployment mode.

```bash
# Single-node example
export E2B_API_URL=http://127.0.0.1:8000
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=${AENV_API_KEY}
```

AgentENV returns `trafficAccessToken` when `network.allowPublicTraffic` is false
and (for secure sandboxes)
`envdAccessToken` for envd control traffic. These credentials have different
headers and trust boundaries: use `e2b-traffic-access-token` for private
application routes and `X-Access-Token` only for envd. Public application
routes require neither token.

### TypeScript SDK

#### Setup

Install the SDK:

```bash
npm install e2b
```

#### Usage

```typescript
import { Sandbox } from "e2b";

// Create a sandbox from a template
const sandbox = await Sandbox.create("<template-id>", {
  apiKey: process.env.E2B_API_KEY,
});

// List running sandboxes
const running = Sandbox.list({
  apiKey: process.env.E2B_API_KEY,
  limit: 20,
  query: { state: ["running"] },
});
console.log(await running.nextItems());

// Run a command inside the sandbox
sandbox.commands.run("echo hello world");

// Pause the sandbox
await Sandbox.Pause(sandbox.sandboxId, {
  apiKey: process.env.E2B_API_KEY,
});

// Kill the sandbox
await sandbox.kill();
```

Replace `<template-id>` with a template that exists in your local template store. Use `e2b template list` or `GET /v2/templates` to see available templates.

### Python SDK

#### Setup

Install the SDK:

```bash
pip install e2b
```

#### Usage

```python
from e2b import Sandbox, SandboxQuery, SandboxState

# Reuse the environment variables set in your shell:
# E2B_API_URL / E2B_SANDBOX_URL / E2B_API_KEY

# Create a sandbox from a template
sandbox = Sandbox.create("<template-id>")

# List running sandboxes
running = Sandbox.list(
    limit=20,
    query=SandboxQuery(state=[SandboxState.RUNNING]),
)
print(running.next_items())

# Run a command inside the sandbox
result = sandbox.commands.run("echo hello world")
print(result.stdout, end="")

# Pause the sandbox
sandbox.beta_pause()

# Kill the sandbox
sandbox.kill()
```

## E2B CLI

AgentENV is compatible with the E2B CLI, but we recommend using the
[aenv CLI](../getting-started/aenv-cli.md) for AgentENV workflows.
