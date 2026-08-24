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

For example, start an HTTP server on port `8080` inside a running sandbox:

```bash
aenv exec <sandbox-id> sh -c 'echo "Hello from AgentENV" > /tmp/index.html'
aenv exec <sandbox-id> python3 -m http.server 8080 --directory /tmp
```

Access the service through the AgentENV proxy:

```bash
curl http://127.0.0.1:8000/proxy/index.html \
  -H "x-agentenv-sandbox-id: <sandbox-id>" \
  -H "x-agentenv-target-port: 8080"
```

## Header-Based Routing

When using `/proxy` or routing an otherwise unmatched path with headers,
identify the target sandbox and port with these headers:

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

| Traffic | When it applies | Required credential |
| --- | --- | --- |
| Public application ingress | The sandbox uses `allowPublicTraffic: true`, which is the default. | None |
| Private application ingress | The sandbox uses `allowPublicTraffic: false`. | `e2b-traffic-access-token: <trafficAccessToken>` |
| Secure envd traffic | The sandbox was created with secure communication enabled. | `X-Access-Token: <envdAccessToken>` |

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
