# Secure Sandbox Authentication

A secure sandbox requires an `envdAccessToken` for envd control operations,
including interactive connections, command execution, and file upload or
download. This does not protect services exposed by the sandbox; application
ingress uses `trafficAccessToken` as described in
[Application Ingress Authentication](./application-ingress.md).

## Enable Secure Mode

Set `secure: true` in the sandbox
creation request. For example, create a secure sandbox and capture
the token returned in the response:

```bash
SANDBOX_RESPONSE=$(curl -sS -X POST http://127.0.0.1:8000/sandboxes \
  -H "X-API-Key: <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "templateID": "my-template",
    "secure": true
  }')

SANDBOX_ID=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.sandboxID')
ENVD_ACCESS_TOKEN=$(printf '%s' "$SANDBOX_RESPONSE" | jq -r '.envdAccessToken')
```

Use the envd port `49983`, and send the token in `X-Access-Token`:

```bash
curl http://127.0.0.1:8000/proxy/health \
  -H "x-agentenv-sandbox-id: $SANDBOX_ID" \
  -H "x-agentenv-target-port: 49983" \
  -H "X-Access-Token: $ENVD_ACCESS_TOKEN"
```

The `aenv` CLI always requests secure sandbox authentication and handles the
envd access token automatically:

```bash
aenv start <template-or-snapshot>
```

For `connect`, `exec`, `upload`, and
`download`, it obtains the token through the authenticated AgentENV API and
adds `X-Access-Token` to the subsequent envd request.

## Secure-Mode Lifecycle

Secure mode and its credentials remain valid across pause, server restart, and
resume. Forked sandboxes receive independent credentials.
