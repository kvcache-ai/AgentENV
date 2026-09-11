# API Authentication

The API key authenticates requests that create and manage AgentENV resources,
including templates, sandboxes, and snapshots:

```bash
curl http://127.0.0.1:8000/sandboxes \
  -H "X-API-Key: <api-key>"
```

## Get the API Key

A runtime node checks these API-key sources in order:

1. `AENV_API_KEY`
2. `/run/secrets/api-key`
3. `$AENV_HOME/secrets/api-key`

The gateway checks only `AENV_API_KEY` and `/run/secrets/api-key`. The gateway
and every runtime node in one deployment must use the same key.

### Provide an API Key

To provide your own key, generate a value containing 32 to 256 URL-safe
characters and make it available through one of the locations above. For
example, generate an E2B-compatible key and set it through the environment:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
```

Use the same explicitly generated key for the gateway and every runtime node in
a multi-node deployment. Docker Compose can provide it through its shared
managed-secret volume, while Kubernetes stores it in `Secret/agentenv-auth`.
See the corresponding deployment guide for setup instructions.

### Use an Automatically Generated API Key

If a runtime node finds no key in any of the three locations, normal server
startup generates an E2B-compatible key and atomically stores it at
`$AENV_HOME/secrets/api-key`. Read that file after the server starts and use its
value when configuring the CLI or another API client.

Automatic generation is convenient for a normal single-node deployment. The
gateway never generates a key, so a multi-node deployment must make one shared
key available to the gateway and all runtime nodes.

## Configure the CLI

Run `aenv auth`, enter the AgentENV server URL, and paste the API key obtained
above. Press Enter to accept the default local URL. The API key input is hidden:

```text
$ aenv auth
AENV server URL [http://localhost:8000]: http://localhost:8000
API key: <paste-api-key-here>
Credentials saved.
```

E2B-compatible SDKs read the same AgentENV API key from `E2B_API_KEY`:

```bash
export E2B_API_KEY="<paste-api-key-here>"
```

## Unauthenticated Endpoints

`GET /health` and node `GET /metrics` are outside API-key authentication. The
gateway exposes Prometheus metrics on a separate metrics listener. Protect
these endpoints with the network and authentication controls used by your
monitoring deployment.

## Rotate the API Key

Changing `AENV_API_KEY` invalidates existing API clients but does not change
the credentials of existing sandboxes.

