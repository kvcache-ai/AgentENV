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

### Persistence

Params survive pause/resume and are stored into snapshots created from the
sandbox. When starting from a template, a `customExtensionParams` provided at
creation overrides the one stored in the snapshot; otherwise the snapshot's
value is inherited.
