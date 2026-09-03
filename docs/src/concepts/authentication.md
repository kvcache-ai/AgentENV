# Authentication

AgentENV uses three credentials for three separate kinds of access:

| Credential | Protects | When required | Header |
| --- | --- | --- | --- |
| API key | AgentENV lifecycle and management APIs | All authenticated control-plane requests | `X-API-Key` |
| `trafficAccessToken` | Services exposed by a sandbox | `allowPublicTraffic: false` | `e2b-traffic-access-token` |
| `envdAccessToken` | envd operations such as command execution and file access | `secure: true` | `X-Access-Token` |

These credentials are not interchangeable. The API key belongs to the AgentENV
deployment; the other two tokens belong to an individual sandbox.

## API Authentication

The API key authenticates requests that create and manage AgentENV resources,
including templates, sandboxes, and snapshots:

```bash
curl http://127.0.0.1:8000/sandboxes \
  -H "X-API-Key: <api-key>"
```

### Get the API Key

A runtime node checks these API-key sources in order:

1. `AENV_API_KEY`
2. `/run/secrets/api-key`
3. `$AENV_HOME/secrets/api-key`

The gateway checks only `AENV_API_KEY` and `/run/secrets/api-key`. The gateway
and every runtime node in one deployment must use the same key.

#### Provide an API Key

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

#### Use an Automatically Generated API Key

If a runtime node finds no key in any of the three locations, normal server
startup generates an E2B-compatible key and atomically stores it at
`$AENV_HOME/secrets/api-key`. Read that file after the server starts and use its
value when configuring the CLI or another API client.

Automatic generation is convenient for a normal single-node deployment. The
gateway never generates a key, so a multi-node deployment must make one shared
key available to the gateway and all runtime nodes.

### Configure the CLI

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

### Unauthenticated Endpoints

`GET /health` and node `GET /metrics` are outside API-key authentication. The
gateway exposes Prometheus metrics on a separate metrics listener. Protect
these endpoints with the network and authentication controls used by your
monitoring deployment.

### Rotate the API Key

Changing `AENV_API_KEY` invalidates existing API clients but does not change
the credentials of existing sandboxes.

## Application Ingress Authentication

Application ingress is traffic sent through the AgentENV proxy to a service
running inside a sandbox. It is independent from API and envd authentication.

With `allowPublicTraffic: true`, which is the default, application ingress does
not require an AgentENV credential.

To create a private sandbox, set `allowPublicTraffic: false`. The creation
response includes that sandbox's `trafficAccessToken`; capture it for later
proxy requests:

```bash
SANDBOX_RESPONSE=$(curl -sS -X POST http://127.0.0.1:8000/sandboxes \
  -H "X-API-Key: <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "templateID": "my-template",
    "network": {
      "allowPublicTraffic": false
    }
  }')

SANDBOX_ID=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.sandboxID')
TRAFFIC_ACCESS_TOKEN=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.trafficAccessToken')
```

Send that token in `e2b-traffic-access-token` when accessing an application in
the sandbox:

```bash
curl http://127.0.0.1:8000/proxy/ \
  -H "x-agentenv-sandbox-id: $SANDBOX_ID" \
  -H "x-agentenv-target-port: 8080" \
  -H "e2b-traffic-access-token: $TRAFFIC_ACCESS_TOKEN"
```

Each sandbox has its own token, and forked sandboxes receive independent
credentials. AgentENV removes the traffic token before forwarding the request
to the sandbox application. See [Proxy](./proxy.md#access-control)
for the complete proxy routing and access-control behavior.

## Secure Sandbox Authentication

A secure sandbox requires an `envdAccessToken` for envd control operations,
including interactive connections, command execution, and file upload or
download. This does not protect services exposed by the sandbox; application
ingress uses `trafficAccessToken` as described above.

### Enable Secure Mode

Set `secure: true` in the sandbox
creation request. For example, create a secure sandbox and capture
the token returned in the response:

```bash
SANDBOX_RESPONSE=$(curl -sS -X POST http://127.0.0.1:8000/sandboxes \
  -H "X-API-Key: <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "templateID": "my-template",
    "secure": true
  }')

SANDBOX_ID=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.sandboxID')
ENVD_ACCESS_TOKEN=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.envdAccessToken')
```

Use the envd port `49983`, and send the token in `X-Access-Token`:

```bash
curl http://127.0.0.1:8000/proxy/health \
  -H "x-agentenv-sandbox-id: $SANDBOX_ID" \
  -H "x-agentenv-target-port: 49983" \
  -H "X-Access-Token: $ENVD_ACCESS_TOKEN"
```

The `aenv` CLI always requests secure sandbox authentication and handles the
envd access token automatically:

```bash
aenv start <template-or-snapshot>
```

For `connect`, `exec`, `upload`, and
`download`, it obtains the token through the authenticated AgentENV API and
adds `X-Access-Token` to the subsequent envd request.

### Secure-Mode Lifecycle

Secure mode and its credentials remain valid across pause, server restart, and
resume. Forked sandboxes receive independent credentials.

## Sandbox Access-Token Seed

AgentENV derives both `trafficAccessToken` and `envdAccessToken` from the
sandbox identity and a random access-token seed. This seed is independent of
the AgentENV API key.

By default, no manual configuration is required. When a seed is not configured,
AgentENV generates one on first startup and persists it at
`$AENV_HOME/secrets/sandbox-access-token-hash-seed`. Later startups reuse the
same value.

To provide your own seed, generate one value:

```bash
ACCESS_TOKEN_SEED="$(openssl rand -hex 32)"
```

Then configure it using one of the following methods.

Set the environment variable before starting AgentENV:

```bash
export AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED="$ACCESS_TOKEN_SEED"
```

Or set it in `config/default.toml`, or in the configuration file selected by
`AENV_CONFIG_PATH`:

```toml
[sandbox]
access_token_hash_seed = "<generated-seed>"
```

Kubernetes deployments store the shared value under the
`sandbox-access-token-hash-seed` key in `Secret/agentenv-runtime-secrets`.

Preserve the seed across upgrades. Changing it rotates both sandbox token types
and requires all runtime nodes to be updated together.

## Transport Security

Authentication does not encrypt traffic. Use HTTPS termination, a VPN,
loopback, or a trusted private network to protect API keys and sandbox tokens
in transit.
