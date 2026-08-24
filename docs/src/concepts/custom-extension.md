# Custom Extension

The custom extension is an optional external HTTP service that AgentENV calls during the sandbox lifecycle. It lets you implement deployment-specific behavior — for example, connecting sandboxes into a VPN, custom firewall rules, or extra mounts — without changing AgentENV itself.

The extension implements a small set of HTTP endpoints ("hooks"); AgentENV is the client. The interface is defined in [`src/custom_extension_api/openapi.yml`](https://github.com/kvcache-ai/AgentENV/blob/main/src/custom_extension_api/openapi.yml).

## Lifecycle hooks

To support the complete sandbox lifecycle, your extension should implement the
following four APIs. AgentENV sends a JSON request to
`POST {url}/sandbox-hook/<hook>` when the corresponding event occurs. Any connection error, timeout, or non-2xx response fails the corresponding sandbox operation (except `stop`, which is best-effort).

See [Minimal Extension Example](#minimal-extension-example) for an example
extension that implements all four hooks.

| Hook | When | Request | Response |
|------|------|---------|----------|
| `start-fresh` | Before a fresh sandbox boots, after its network slot is allocated | `sandboxId`, `sandboxInstanceId`, `networkNamespacePath`, `hostInteractionIp`, `customExtensionParams` | optional `extraBootArgs` appended to the kernel cmdline |
| `start-resume` | Before a sandbox resumes from a snapshot (template launch, resume after pause, fork child) | same as above | none |
| `patch-params` | When a user PATCHes the sandbox's params | `sandboxId`, `patch` (verbatim user body) | updated **full** `customExtensionParams` |
| `stop` | When the sandbox runtime is torn down, before the network slot is released | `sandboxId`, `sandboxInstanceId` | none |

Notes:

- **Instance identity.** A `sandboxId` is reused across pause/resume cycles. Every `start-fresh` / `start-resume` carries a fresh `sandboxInstanceId` identifying that runtime instance, and the subsequent `stop` carries the same value. Because `stop` is best-effort and may be reordered (e.g. a pause's `stop` arriving after the resume's `start-resume`), treat `(sandboxId, sandboxInstanceId)` as the identity of a running instance and ignore `stop` notifications whose `sandboxInstanceId` is not the latest started instance for that sandbox.
- **`stop` also fires on pause.** Pausing persists the sandbox state and then stops the VM process and releases the network namespace; the subsequent resume creates a fresh runtime and fires `start-resume`. In-place pause+resume during snapshot capture does not fire any hook (and keeps the same `sandboxInstanceId`).
- `stop` is best-effort: delivery failures are only logged, and it is also fired fire-and-forget if a started sandbox is dropped without an explicit stop.
- `networkNamespacePath` is the host path of the sandbox's netns file (e.g. `/var/run/netns/agentenv-ns-*`), so the extension can enter the namespace (e.g. `nsenter --net=...`) to set up firewall rules or VPN interfaces.
- `hostInteractionIp` is the per-runtime IPv4 address that AgentENV routes to this sandbox. It can change after pause/resume, so extensions must use the value from the current start hook rather than caching an older one.
- Concurrent `patch-params` calls to the same sandbox are not serialized; if your patch semantics are not commutative, handle concurrency in the extension.

## Connect AgentENV to an Extension

Connect AgentENV to your extension service by configuring its URL:

```toml
# config/default.toml (or your AENV_CONFIG_PATH)
[custom_extension]
url = "http://127.0.0.1:9090"
# timeout_ms = 5000   # optional, per-call timeout in milliseconds
```

`AENV_CUSTOM_EXTENSION_URL` works as well. When `url` is unset, the integration is fully disabled: no hooks are called and `customExtensionParams` must be empty.

## Use the Extension

Use `customExtensionParams` to pass extension-specific settings for a sandbox.
It is an opaque JSON object interpreted only by your extension. An absent value
and an empty object are equivalent.

### Set at Creation

Both `POST /sandboxes` and `POST /sandboxes-cold` accept
`customExtensionParams`. For example, create a sandbox from a template with VPN
settings for the extension:

```bash
curl -X POST http://127.0.0.1:8000/sandboxes \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "templateID": "my-template",
    "customExtensionParams": {
      "vpn": { "network": "team-a" }
    }
  }'
```

For a cold-start sandbox, include the same field in the cold-start request:

```bash
curl -X POST http://127.0.0.1:8000/sandboxes-cold \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "image": "docker.io/library/ubuntu:24.04",
    "customExtensionParams": {
      "vpn": { "network": "team-a" }
    }
  }'
```

### Read

Get the current params. AgentENV returns `{}` when they are empty:

```bash
curl http://127.0.0.1:8000/sandboxes/<sandbox-id>/custom-extension-params \
  -H 'X-API-Key: test-key'
```

### Patch

The request body is passed through verbatim to the extension's `patch-params` hook; its semantics are defined entirely by the extension. The hook returns the updated full params, which AgentENV stores and returns:

```bash
curl -X PATCH http://127.0.0.1:8000/sandboxes/<sandbox-id>/custom-extension-params \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{"vpn": {"network": "team-a", "peers": ["10.8.0.2", "10.8.0.3"]}}'
```

### Persistence

Params survive pause/resume and are stored into snapshots created from the sandbox. When starting from a template, a `customExtensionParams` provided at creation overrides the one stored in the snapshot; otherwise the snapshot's value is inherited.

---

## Minimal extension example

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# Latest started runtime instance per sandbox: (sandboxId, sandboxInstanceId)
# is the identity of a running instance; a stop for a superseded instance
# (e.g. arriving after a newer start) is ignored.
latest_instance: dict[str, str] = {}

@app.post("/sandbox-hook/start-fresh")
async def start_fresh(req: Request):
    body = await req.json()
    latest_instance[body["sandboxId"]] = body["sandboxInstanceId"]
    # e.g. nsenter --net={body["networkNamespacePath"]} wg-quick up ...
    return {"extraBootArgs": None}

@app.post("/sandbox-hook/start-resume")
async def start_resume(req: Request):
    body = await req.json()
    latest_instance[body["sandboxId"]] = body["sandboxInstanceId"]
    return {}

@app.post("/sandbox-hook/patch-params")
async def patch_params(req: Request):
    body = await req.json()
    # apply body["patch"] however you like, then return the full new params
    return {"customExtensionParams": body["patch"]}

@app.post("/sandbox-hook/stop")
async def stop(req: Request):
    body = await req.json()
    if latest_instance.get(body["sandboxId"]) == body["sandboxInstanceId"]:
        latest_instance.pop(body["sandboxId"], None)
        # tear down resources for this instance
    return {}
```

Any non-2xx response (or timeout) fails the corresponding sandbox operation, except for `stop`, which is always tolerated.
