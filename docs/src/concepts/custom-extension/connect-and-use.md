# Use the Extension

This page shows how to connect the AgenENV server to the extension and use it.

## Connect AgentENV to the Extension

Connect AgentENV to your extension service by configuring its URL:

```toml
 config/default.toml (or your AENV_CONFIG_PATH)
[custom_extension]
url = "http://127.0.0.1:9090"
 timeout_ms = 5000   # optional, per-call timeout in milliseconds
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

The request body is passed through verbatim to the extension's `patch-params`
hook; its semantics are defined entirely by the extension. The hook returns the
updated full params, which AgentENV stores and returns:

```bash
curl -X PATCH http://127.0.0.1:8000/sandboxes/<sandbox-id>/custom-extension-params \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{"vpn": {"network": "team-a", "peers": ["10.8.0.2", "10.8.0.3"]}}'
```

### Webhook

The webhook lets your extension receive sandbox lifecycle events, such as a
sandbox being created or deleted. AgentENV has exactly one webhook, and it
points at your extension's `POST {url}/sandbox-hook/event`. It exists only
while `[custom_extension].url` is set, and it is disabled until you enable it.
See [Lifecycle Hooks](./lifecycle-hooks.md) for the request your extension
receives.

Enable it and choose the events to receive:

```bash
curl -X PATCH http://127.0.0.1:8000/events/webhooks/<webhook-id> \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "enabled": true,
    "events": ["sandbox.lifecycle.created", "sandbox.lifecycle.killed"],
    "signatureSecret": "replace-with-a-random-secret"
  }'
```

Read it back with `GET /events/webhooks/<webhook-id>`. Creating and deleting
webhooks is not supported.

| Field | Meaning |
| --- | --- |
| `enabled` | Whether events are sent. Defaults to `false`. |
| `events` | The event types to send. Must not be empty. Defaults to all five. |
| `signatureSecret` | Signs each request so your extension can verify it. An empty string turns signing off. Never returned by the API. |
| `name` | A display name. |
| `url` | Always `{url}/sandbox-hook/event`. It cannot be changed; setting it returns `400`. |

`events` accepts:

| Event | Sent when |
| --- | --- |
| `sandbox.lifecycle.created` | A sandbox is created |
| `sandbox.lifecycle.killed` | A sandbox is deleted, including timeout deletion |
| `sandbox.lifecycle.paused` | A sandbox is paused, including timeout pause |
| `sandbox.lifecycle.resumed` | A paused sandbox resumes |
| `sandbox.lifecycle.forked` | A sandbox is forked, once per child |

Once enabled, your extension receives a `POST {url}/sandbox-hook/event` for each
subscribed event with these headers:

| Header | Meaning |
| --- | --- |
| `content-type` | Always `application/json` |
| `e2b-webhook-id` | The webhook that matched the event |
| `e2b-delivery-id` | Unique per delivery attempt; retries get a new value |
| `e2b-signature-version` | Always `v1` |
| `e2b-signature` | Unpadded base64 of `sha256(signatureSecret + rawBody)` |

The body has the following fields:

```json
{
  "id": "01a1211e-38db-7c53-a2e2-b40392a7688f",
  "version": "v2",
  "type": "sandbox.lifecycle.created",
  "timestamp": "2026-10-09T14:43:11.194863Z",
  "event_category": "lifecycle",
  "event_label": "create",
  "event_data": {
    "sandbox_metadata": {
      "<custom-key>": "<custom-value>"
    }
  },
  "sandbox_id": "<sandbox-id>",
  "sandbox_execution_id": "",
  "sandbox_template_id": "<template-id>",
  "sandbox_build_id": "",
  "sandbox_team_id": "00000000-0000-0000-0000-000000000000",
  "events_ttl_days": 7
}
```
- `type` is the event, and `event_label` is its short form: `create`, `kill`,
  `pause`, `resume`, or `fork`.
- `event_data.sandbox_metadata` is the sandbox's user metadata. Forked events
  also carry `event_data.source_sandbox_id`, the sandbox the child was forked
  from.
- `sandbox_template_id` is the template or snapshot the sandbox was launched
  from. Cold-start sandboxes report the resolved image.
- `e2b-signature-version` and `e2b-signature` are sent only when a
  `signatureSecret` is set. The signature is the unpadded base64 of
  `sha256(signatureSecret + rawBody)`; verify it against the raw body.
- Each event has a unique `id`. Use it to deduplicate.

The settings are shared by all nodes that use the same snapshot repository.
Events are sent once each, after the operation completes, and a failed delivery
never fails the sandbox operation.

### Persistence

Params survive pause/resume and are stored into snapshots created from the
sandbox. When starting from a template, a `customExtensionParams` provided at
creation overrides the one stored in the snapshot; otherwise the snapshot's
value is inherited.
