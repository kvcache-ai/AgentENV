# Authentication

AgentENV uses one shared API key for a single-tenant deployment. The gateway
and every runtime node must receive the same `AENV_API_KEY`.

Clients authenticate API requests with:

```text
X-API-Key: <AENV_API_KEY>
```

`Authorization`, `X-Admin-Token`, and `X-Team-ID` do not authenticate
AgentENV. The `Authorization` header is left unchanged when a request is
proxied into a sandbox, so applications inside a sandbox can use it normally.
`GET /health` is public for load balancer and container health checks.

E2B SDK users only set `E2B_API_KEY` to the same value. AgentENV returns a
sandbox-scoped access token in sandbox responses, and current E2B SDKs use it
for sandbox traffic automatically. Users do not need to create, copy, or store
a second token.

## Installation Methods

The native installer generates a 256-bit key once, stores it in
`/etc/default/aenv` with restricted permissions, and preserves it across
upgrades. Read it locally when configuring a client:

```bash
sudo sed -n 's/^AENV_API_KEY=//p' /etc/default/aenv
```

`scripts/docker-setup.sh` does the same for Docker at
`/etc/aenv/auth.env`. The documented Docker and Compose commands consume that
file:

```bash
sudo sed -n 's/^AENV_API_KEY=//p' /etc/aenv/auth.env
```

`make k8s-apply` creates `Secret/agentenv-auth` on the first apply and reuses
the existing key on later applies. Read it with:

```bash
kubectl -n agentenv-system get secret agentenv-auth \
  -o go-template='{{index .data "AENV_API_KEY" | base64decode}}{{"\n"}}'
```

For a manual build, generate and export the key before starting the server:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
make start-server
```

To provide your own key to an installer or deployment helper, set
`AENV_API_KEY` to at least 32 URL-safe characters. In a multi-node deployment,
use exactly the same value for the gateway and every runtime node.
The generated keys use `e2b_` followed by hexadecimal characters so they pass
the E2B SDK default API-key validation. Use that format for custom keys when
you need E2B SDK compatibility.

## Transport Security

API key authentication does not encrypt HTTP traffic. Do not send the key over
an untrusted plaintext network. Keep AgentENV on loopback or a trusted private
network, use a VPN, or terminate HTTPS at a reverse proxy or load balancer.

## Rotation

Set a new `AENV_API_KEY` on the gateway and every runtime node, then restart
them. Existing clients must switch to the new value. Previously issued
sandbox-scoped tokens stop working when the key changes.
