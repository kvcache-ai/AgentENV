# Lifecycle Hooks

To support the complete sandbox lifecycle, your extension should implement the
following APIs. AgentENV sends a JSON request to
`POST {url}/sandbox-hook/<hook>` when the corresponding event occurs. Any connection error, timeout, or non-2xx response fails the corresponding sandbox operation (except `stop` and `event`, which are best-effort).

| Hook | When | Request | Response |
|------|------|---------|----------|
| `start-fresh` | Before a fresh sandbox boots, after its network slot is allocated | `sandboxId`, `sandboxInstanceId`, `networkNamespacePath`, `hostInteractionIp`, `customExtensionParams` | optional `extraBootArgs` appended to the kernel cmdline |
| `start-resume` | Before a sandbox resumes from a snapshot (template launch, resume after pause, fork child) | same as above | none |
| `patch-params` | When a user PATCHes the sandbox's params | `sandboxId`, `patch` (verbatim user body) | updated **full** `customExtensionParams` |
| `stop` | When the sandbox runtime is torn down, before the network slot is released | `sandboxId`, `sandboxInstanceId` | none |
| `event` | After a sandbox lifecycle change, when the webhook is enabled (optional, see [Webhook](./connect-and-use.md#webhook)) | E2B v2 `SandboxEvent` body, signature headers when a secret is set | none |

Notes:

- **Instance identity.** A `sandboxId` is reused across pause/resume cycles. Every `start-fresh` / `start-resume` carries a fresh `sandboxInstanceId` identifying that runtime instance, and the subsequent `stop` carries the same value. Because `stop` is best-effort and may be reordered (e.g. a pause's `stop` arriving after the resume's `start-resume`), treat `(sandboxId, sandboxInstanceId)` as the identity of a running instance and ignore `stop` notifications whose `sandboxInstanceId` is not the latest started instance for that sandbox.
- **`stop` also fires on pause.** Pausing persists the sandbox state and then stops the VM process and releases the network namespace; the subsequent resume creates a fresh runtime and fires `start-resume`. In-place pause+resume during snapshot capture does not fire any hook (and keeps the same `sandboxInstanceId`).
- `stop` is best-effort: delivery failures are only logged, and it is also fired fire-and-forget if a started sandbox is dropped without an explicit stop.
- `networkNamespacePath` is the host path of the sandbox's netns file (e.g. `/var/run/netns/agentenv-ns-*`), so the extension can enter the namespace (e.g. `nsenter --net=...`) to set up firewall rules or VPN interfaces.
- `hostInteractionIp` is the per-runtime IPv4 address that AgentENV routes to this sandbox. It can change after pause/resume, so extensions must use the value from the current start hook rather than caching an older one.
- Concurrent `patch-params` calls to the same sandbox are not serialized; if your patch semantics are not commutative, handle concurrency in the extension.

## Minimal Extension Example

```python
import base64
import hashlib
import hmac

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# Latest started runtime instance per sandbox: (sandboxId, sandboxInstanceId)
# is the identity of a running instance; a stop for a superseded instance
# (e.g. arriving after a newer start) is ignored.
latest_instance: dict[str, str] = {}

# Must match the webhook's signatureSecret; empty if signing is off.
SIGNATURE_SECRET = "replace-with-a-random-secret"

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

@app.post("/sandbox-hook/event")
async def event(req: Request):
    raw = await req.body()
    if SIGNATURE_SECRET:
        digest = hashlib.sha256(SIGNATURE_SECRET.encode() + raw).digest()
        expected = base64.b64encode(digest).decode().rstrip("=")
        if not hmac.compare_digest(expected, req.headers.get("e2b-signature", "")):
            return JSONResponse({"error": "bad signature"}, status_code=401)
    body = await req.json()
    return {}
```

Any non-2xx response or timeout fails the corresponding sandbox operation,
except for `stop` and `event`, which are always tolerated.
