# Application Ingress Authentication

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
to the sandbox application. See [Proxy](../sandboxes/proxy.md#access-control)
for the complete proxy routing and access-control behavior.
