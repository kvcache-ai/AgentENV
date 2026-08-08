# E2B

## E2B SDK

AgentENV exposes an E2B-compatible API, so the official [E2B SDK](https://github.com/e2b-dev/e2b) works out of the box.

### General Settings

Set environment variables to point at your AgentENV server. See [Environment Variables](../configuration/env-vars.md) for values per deployment mode.

```bash
# Single-node example
export E2B_API_URL=http://127.0.0.1:8000
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=e2b_000000
export E2B_ACCESS_TOKEN=dummy
```

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

### Template builds

The SDK's template builder works against AgentENV, including Dockerfiles with `COPY`:

```python
import asyncio
from e2b import AsyncTemplate, Template

template = Template(file_context_path=".").from_dockerfile(
    """
    FROM ubuntu:24.04
    COPY requirements.txt /opt/app/requirements.txt
    RUN apt-get update && apt-get install -y python3
    """
)
asyncio.run(AsyncTemplate.build(template=template, alias="my-template"))
```

How `COPY` works: for each `COPY` instruction the SDK requests an upload link (`GET /templates/{templateID}/files/{hash}`), `PUT`s a tar archive of the matching context files to the returned bearer upload URL, and references the archive by `filesHash` when it starts the build. AgentENV stores the archives in the snapshot repository (shared across nodes) and extracts them inside the build sandbox.

Requirements and behavior notes:

- The base image must provide `/bin/bash` (already required for `RUN` steps) and `tar` for `COPY` steps.
- Copied files are owned by `root:root` like Docker's `COPY` default. `COPY --chown=user:group` resolves names against the image's own `/etc/passwd` and `/etc/group` and applies only to the files the copy creates; an unknown user fails the build.
- Write directory destinations with a trailing slash (`COPY app.py /opt/`). Docker's special case of copying a single file onto an existing directory named without a trailing slash (`COPY app.py /opt`) is not supported and fails the build with a clear error.
- Rebuilding an existing alias is not currently supported. Use a new alias (or remove the existing template first) until the alias subsystem is refactored.
- Any TLS-terminated, gateway-fronted, or multi-hop deployment must set `template_build.public_base_url` (or `AENV_TEMPLATE_BUILD_PUBLIC_BASE_URL`) to the external origin clients reach, for example `https://agentenv.example.com`. When it is unset, the upload URL is derived from the request `Host` header with plain `http`, and because that URL carries a bearer token in its query string the upload either fails against an HTTPS-only listener or sends the token and the whole build context in cleartext. Direct-node and bundled-gateway deployments can keep the default.

## E2B CLI

AgentENV is compatible with the E2B CLI, but we recommend using the
[aenv CLI](../getting-started/aenv-cli.md) for AgentENV workflows.
