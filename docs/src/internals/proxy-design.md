# AgentENV Proxy Design and Usage

This document describes the per-node reverse proxy that forwards requests into individual sandboxes. For the distributed routing layer (gateway to scheduler to node), see [System Architecture](./architecture.md).

## Scope

Each AgentENV node runs a reverse proxy that accepts requests on its API server
and forwards them to sandbox services over the sandbox interaction network. In a
multi-node deployment, the gateway resolves sandbox ownership through the
scheduler and forwards data-plane requests to the owning node's proxy surface.

Current entrypoints:

- `ANY /proxy`
- `ANY /proxy/{*proxy_path}`
- Any otherwise unmatched path that carries sandbox routing headers
- Host-based sandbox proxy requests when `[sandbox_proxy].domains` is configured:
  `{port}-{sandboxID}.{domain}`

Implementation references:

- `src/api/proxy.rs`
- `src/orchestrator/service.rs`
- `src/orchestrator/proxy.rs`

## Routing Contract

Each proxied request must identify:

- Sandbox ID
- Target port inside the sandbox service plane

Accepted headers:

- `x-agentenv-sandbox-id`
- `x-agentenv-target-port`
- E2B-compatible aliases:
  - `e2b-sandbox-id`
  - `e2b-sandbox-port`

Validation:

- Sandbox ID must be a valid UUID format.
- Target port must parse as `u16` and be greater than `0`.

Authorization is evaluated by the owning runtime after route parsing:

- Control-plane routes require the deployment `X-API-Key` and do not accept
  sandbox credentials.
- Node Prometheus `/metrics` and health `/health` are public to application
  auth and should be protected separately at the deployment boundary.
- Non-envd application routes require `e2b-traffic-access-token` only when the
  sandbox has private ingress (`allowPublicTraffic: false`).
- The envd port requires `X-Access-Token` only for secure sandboxes.
- `X-API-Key` is never a data-plane credential.

The distributed gateway deliberately does not make sandbox authorization
decisions. It routes data-plane requests, including public ingress and insecure
envd requests with no credential, to the owning runtime. The runtime has the
sandbox metadata needed to apply the policy and performs the authoritative
token validation.

Host-based routing derives both fields from `Host`. The configured domain must
match exactly after lowercase normalization and optional trailing-dot removal.
Sandbox IDs in host routes must be valid UUIDs and the target port must fit in
`u16`.

## Runtime Route Model

AgentENV keeps an in-memory runtime route table in orchestrator.

- Route key: `SandboxId`
- Route value: `ProxyTarget` (currently host interaction IP) plus route metadata (`version`, `updated_at`)

Design rule:

- Only `Running` sandboxes publish runtime routes.
- Non-running states rely on metadata fallback (not route-table state).

Lookup behavior (`proxy_lookup_for`):

1. If runtime route exists: `Ready(target)`
2. Else read metadata:
   - no metadata: `NotFound`
   - metadata state is `Running`: `RouteMissing`
   - metadata state is `Paused`: `Paused { auto_resume }`
   - other states: `Unavailable(state)`

This keeps hot-path reads lock-light and avoids reading sandbox instance internals in API request paths.

## Paused Sandbox Auto-Resume

`/proxy` can auto-resume paused sandboxes when lifecycle policy enables it.

- Proxy route resolution does not read sandbox instance internals.
- Orchestrator lookup returns `Paused { auto_resume }`, and proxy decides behavior from that signal.
- Auto-resume is attempted once per request.
- Resume timeout update uses `EnsureMinimum(5 minutes)`:
  - effective sandbox timeout is `max(existing_timeout, 5 minutes)`
- Proxy waits up to:
  - test builds: short unit-test timeout
  - non-test runtime: `60s`

Request outcomes for paused sandboxes:

- `auto_resume = false`: `410 Gone`
- `auto_resume = true` and resume succeeds, then route becomes ready: request is forwarded normally
- `auto_resume = true` but resume/lookup fails: `502 Bad Gateway`
- `auto_resume = true` but resume wait times out: `504 Gateway Timeout`

## Lifecycle Hooks and Race Hardening

Route publication/removal is tied to lifecycle transitions.

- Create/Resume success path:
  - Persist metadata to `Running`
  - Publish runtime route only if the launching sandbox handle is still the current handle
- Pause/Delete/Rollback paths:
  - Atomically detach sandbox handle and runtime route before stop/finalization

Race protections:

- Late route publication from stale handles is blocked via pointer identity check.
- Handle detachment and route removal happen in one critical section to reduce stale-route visibility windows.
- Launch rollback supports both transitional-state rollback and running-state rollback paths.

## HTTP Forwarding Semantics

### Path and Query

- `/proxy` forwards to upstream `/`
- `/proxy/{*proxy_path}` forwards raw URI path suffix after `/proxy`
- Header-routed fallback requests forward the original request path
- Host-based requests forward the original request path
- Query string is forwarded unchanged

Important details:

- Percent-encoded path segments are preserved.
- Repeated leading slashes are preserved.
  - Example: `/proxy//api` forwards as `//api`
- Host-based proxy routing runs before Axum route matching. When a configured
  sandbox proxy host is used, the request is data-plane traffic even if its path
  resembles a control-plane API.

### Header Handling

Control-plane routing headers are stripped before forwarding upstream:

- `x-agentenv-sandbox-id`
- `x-agentenv-target-port`
- `e2b-sandbox-id`
- `e2b-sandbox-port`

Sandbox credential handling:

- `e2b-traffic-access-token` is stripped before forwarding.
- A successfully validated secure-envd `X-Access-Token` is forwarded to envd;
  otherwise that header is stripped.
- A value matching the platform `X-API-Key` is stripped; other values are
  forwarded as application headers.

Hop-by-hop headers are stripped on both request and response paths, including:

- Standard hop-by-hop headers (`Connection`, `Upgrade`, `TE`, `Trailer`, `Transfer-Encoding`, `Proxy-Authenticate`, `Proxy-Authorization`, `Keep-Alive`)
- Any extra headers nominated by `Connection`

Forwarded headers are injected:

- `x-forwarded-host`
- `x-forwarded-proto`
- `x-forwarded-method`
- `x-forwarded-uri`

### Streaming

HTTP bodies are proxied as streams (request and response), including SSE and large uploads/downloads.

## WebSocket Semantics

WebSocket upgrade is supported through the same `/proxy` endpoints.

- Client upgrade request is validated and forwarded upstream.
- Bidirectional frame bridging is established after successful upstream handshake.
- Selected subprotocol from upstream is propagated to the client.

Handshake failure behavior:

- If upstream rejects with an HTTP response (for example `401`, `403`, `404`), status/body are forwarded as-is.
- Transport or connection failures return `502 Bad Gateway`.
- Handshake timeout returns `504 Gateway Timeout`.

## Error Mapping

- `400 Bad Request`
  - Missing/invalid sandbox routing header
  - Missing/invalid target port header
  - Invalid upstream URI construction
- `404 Not Found`
  - Sandbox not found
- `410 Gone`
  - Sandbox is paused and auto-resume is disabled
  - Sandbox exists but is not proxyable in current state
- `502 Bad Gateway`
  - Upstream transport/connect failure
  - Sandbox is `Running` but runtime route is missing (`RouteMissing`)
  - Paused sandbox auto-resume failed
- `504 Gateway Timeout`
  - Paused sandbox auto-resume timed out
  - Upstream response header timeout
  - Upstream websocket handshake timeout

## Usage Examples

### HTTP

```bash
curl -i \
  -H 'e2b-traffic-access-token: <trafficAccessToken>' \
  -H 'x-agentenv-sandbox-id: <sandbox-uuid>' \
  -H 'x-agentenv-target-port: 8080' \
  'http://127.0.0.1:8000/proxy/health?full=true'
```

### E2B-compatible headers

```bash
curl -i \
  -H 'X-API-Key: test-key' \
  -H 'e2b-sandbox-id: <sandbox-uuid>' \
  -H 'e2b-sandbox-port: 8080' \
  'http://127.0.0.1:8000/proxy/status'
```

### WebSocket

Example (generic):

- URL: `ws://127.0.0.1:8000/proxy/ws/echo`
- Headers:
  - `x-agentenv-sandbox-id: <sandbox-uuid>`
  - `x-agentenv-target-port: <port>`
  - API auth headers as required by your deployment

### Host-based

```bash
curl -i \
  -H 'X-API-Key: test-key' \
  'http://8080-<sandbox-uuid>.sandbox.example.com/status'
```

## Testing Notes

Relevant test coverage exists in:

- `src/api/proxy.rs` unit tests (HTTP, SSE, large body, websocket, headers, path preservation, error mapping)
- `src/orchestrator/service.rs` unit tests (route publication/removal behavior and stale-handle guard)
- Integration lifecycle tests in `tests/integration/orchestrator.rs`
- E2E proxy suite in `scripts/tests/e2e/suites/06_proxy.sh` (header compatibility and paused auto-resume behavior)

For environment-backed integration validation, use repository-prescribed integration targets.
