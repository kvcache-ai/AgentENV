# Proxy

The reverse proxy lets you reach services running inside a sandbox from outside. It supports HTTP requests, SSE streams, and WebSocket connections.

## Endpoints

- `ANY /proxy` forwards to `/` inside the sandbox
- `ANY /proxy/{path}` forwards to `/{path}` inside the sandbox
- Header-routed requests on otherwise unmatched paths forward the original path
  unchanged. This lets clients use the same base URL for API and sandbox data
  traffic when they send routing headers.
- When sandbox proxy domains are configured, host-based URLs shaped like
  `{port}-{sandboxID}.{domain}` forward the original path without routing
  headers.

Query strings are forwarded unchanged.

## Required Headers

Each proxied request must identify the target sandbox and port:

| Header | Description |
|--------|-------------|
| `x-agentenv-sandbox-id` | Sandbox UUID to route to |
| `x-agentenv-target-port` | Port of the service inside the sandbox |

E2B-compatible aliases are also accepted:

| Header | Alias for |
|--------|-----------|
| `e2b-sandbox-id` | `x-agentenv-sandbox-id` |
| `e2b-sandbox-port` | `x-agentenv-target-port` |

These routing headers are stripped before the request is forwarded to the sandbox.

## Access Control

Proxy authentication is independent from AgentENV API authentication:

- Public application ingress (`allowPublicTraffic: true`, the default) requires
  no AgentENV credential.
- Private application ingress (`allowPublicTraffic: false`) requires the
  sandbox's `trafficAccessToken` in `e2b-traffic-access-token`.
- Secure envd traffic requires the sandbox's `envdAccessToken` in
  `X-Access-Token`. Insecure envd traffic has no envd token.

`X-API-Key` authenticates AgentENV control-plane APIs only. It does not grant
access to private application ingress or secure envd. A matching platform key
is stripped on proxy requests; other `X-API-Key` values remain available to
sandbox applications. AgentENV also strips the traffic token, and forwards
`X-Access-Token` only to the matching secure envd port.

Host-based proxy requests derive both values from `Host`, for example
`http://8080-<sandbox-uuid>.sandbox.example.com/health` targets port `8080`.
The configured domain must route to the AgentENV server in single-node mode or
to the gateway in multi-node mode. Host-based proxy traffic is always treated as
data-plane traffic; lifecycle and other control-plane APIs should use the base
API host.
