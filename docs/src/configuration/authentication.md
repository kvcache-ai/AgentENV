# Authentication

AgentENV uses one shared API key for a single-tenant deployment. The gateway
and every runtime node must use the same value.

| Credential | Scope | Header |
|---|---|---|
| API key | AgentENV lifecycle and management APIs | `X-API-Key` |
| `trafficAccessToken` | Application ingress when `allowPublicTraffic` is `false` | `e2b-traffic-access-token` |
| `envdAccessToken` | Direct envd access for secure sandboxes | `X-Access-Token` |

The credentials are not interchangeable. Public application ingress and envd
in insecure sandboxes need no AgentENV credential. `Authorization` remains an
application header and does not authenticate AgentENV. On sandbox routes,
`X-API-Key` is also treated as application data unless it exactly matches the
AgentENV API key, in which case it is removed to avoid forwarding the platform
credential.

`GET /health` and node `GET /metrics` are outside API-key authentication. The
gateway exposes Prometheus metrics on its separate metrics listener. Protect
these endpoints with the network and authentication controls used by your
Prometheus deployment. They are distinct from E2B's authenticated sandbox
metrics API.

E2B SDK users set `E2B_API_KEY` to the AgentENV API key. Sandbox credentials
are derived from the sandbox ID and
`AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED`, independently of the API key.

## Key Resolution

A runtime node checks these sources in order:

1. `AENV_API_KEY`
2. `/run/secrets/api-key`
3. `$AENV_HOME/secrets/api-key`

If none exists, normal server startup generates and atomically stores a key at
the managed path. The gateway checks only the first two sources and never
generates a key.

Custom keys must contain 32 to 4096 URL-safe characters. Generated keys use an
E2B-compatible `e2b_` prefix. For example:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
```

Docker Compose shares one managed-secret volume between runtime nodes and
mounts it read-only on the gateway. Kubernetes stores the key in
`Secret/agentenv-auth`. See the corresponding deployment guide for commands to
read or supply those values. Multi-node deployments must also share one
`AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` across runtime nodes.

## Security and Rotation

API-key authentication does not encrypt traffic. Use HTTPS termination, a VPN,
loopback, or a trusted private network.

Changing `AENV_API_KEY` invalidates API clients without changing sandbox
credentials. Changing `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` rotates both
sandbox token types. Apply either change to all relevant processes together.
