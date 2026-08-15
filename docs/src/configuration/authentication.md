# Authentication

AgentENV uses one shared API key for a single-tenant deployment. The gateway
and every runtime node in a cluster must resolve the same key.

Clients authenticate API requests with:

```text
X-API-Key: <AENV_API_KEY>
```

`Authorization`, `X-Admin-Token`, and `X-Team-ID` do not authenticate
AgentENV. The `Authorization` header is left unchanged when a request is
proxied into a sandbox, so applications inside a sandbox can use it normally.
`GET /health` is public for load balancer and container health checks.

E2B SDK users set `E2B_API_KEY` to the same value. Sandbox create responses
include an independent `trafficAccessToken`; send it as
`e2b-traffic-access-token` on application proxy requests. The token is scoped to
the sandbox and is not accepted for control-plane API calls.

For secure sandboxes, `envdAccessToken` is a separate credential for envd
control traffic and must be sent as `X-Access-Token` only when targeting the
envd control-plane port. It is absent for insecure sandboxes.

## Key Resolution

On normal startup, a runtime node uses the first available source:

1. `AENV_API_KEY`
2. `/run/secrets/api-key`
3. `$AENV_HOME/secrets/api-key`

If neither an environment value nor an external secret exists, the server
generates a 256-bit key and atomically stores it in the managed path with
`0600` permissions. It reuses that key on later starts. Dependency and host
setup modes do not create a key.

The gateway uses `AENV_API_KEY` or `/run/secrets/api-key`; it never generates a
key because every gateway and runtime node in a cluster must share one.

## Installation Methods

For a native installation, start the service once and read the managed key:

```bash
sudo cat /var/lib/aenv/secrets/api-key
```

When upgrading an installation that already has `AENV_API_KEY` in
`/etc/default/aenv`, the installer preserves that entry and the server keeps
using it. Fresh installations leave key creation to the server.

For a single Docker container, no auth volume is required. The server creates
the key in its writable container layer:

```bash
docker exec aenv-server cat /workspace/env/secrets/api-key
```

Removing the container removes this generated key. Supply an explicit key or
mount a secret at `/run/secrets/api-key` when it must remain stable across
container replacements.

The checked-in Compose deployment mounts one named volume read-write on both
runtime nodes and read-only at `/run/secrets` on the gateway. Concurrent node
startup is safe: atomic creation makes both nodes converge on the same key.
Read it with:

```bash
docker compose -f deploy/docker-compose.yml exec -T agentenv-a \
  cat /workspace/env/secrets/api-key
```

`docker compose down` preserves the key. `docker compose down -v` removes the
auth volume, so the next startup generates a new key.

`make k8s-apply` creates `Secret/agentenv-auth` on the first apply and reuses
the existing key on later applies. Read it with:

```bash
kubectl -n agentenv-system get secret agentenv-auth \
  -o go-template='{{index .data "AENV_API_KEY" | base64decode}}{{"\n"}}'
```

For a single-node manual build, start the server and read
`$AENV_HOME/secrets/api-key`. To provide your own key instead, export it before
startup:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
make start-server
```

Custom keys must contain at least 32 URL-safe characters. In a multi-node
deployment, use exactly the same value for the gateway and every runtime node.
The generated keys use `e2b_` followed by hexadecimal characters so they pass
the E2B SDK default API-key validation. Use that format for custom keys when
you need E2B SDK compatibility.

Docker Compose secrets can supply a pre-existing key without another AgentENV
configuration variable. In an override file, define a file-backed secret and
mount it with `target: api-key` on the gateway and every runtime node. Compose
then exposes the standard `/run/secrets/api-key` path. Compose secret sources
must already exist, so the named-volume setup remains the zero-configuration
default that allows Rust to generate the key during startup.

## Transport Security

API key authentication does not encrypt HTTP traffic. Do not send the key over
an untrusted plaintext network. Keep AgentENV on loopback or a trusted private
network, use a VPN, or terminate HTTPS at a reverse proxy or load balancer.

## Rotation

Set a new `AENV_API_KEY` on the gateway and every runtime node, or replace the
shared secret file, then restart them. Existing clients must switch to the new
value. Previously issued
`trafficAccessToken` values stop working when the key changes.
`envdAccessToken` values are unaffected and rotate only when the optional envd
seed changes.
