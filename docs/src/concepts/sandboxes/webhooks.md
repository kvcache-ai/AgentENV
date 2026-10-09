# Lifecycle Event Webhooks

AgentENV can push sandbox state changes to HTTP endpoints you register, called
webhooks. You can register multiple webhooks, each with its own URL, event
subscription, and signing secret. Whenever a sandbox is created, paused,
resumed, forked, or deleted, AgentENV sends a signed `POST` request to every
enabled webhook subscribed to that event.

## Manage Webhooks

All requests need your API key in `X-API-Key`.

### Register

```bash
curl -X POST "$AENV_URL/events/webhooks" \
  -H "X-API-Key: $AENV_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-hook",
    "url": "https://example.com/aenv-events",
    "events": ["sandbox.lifecycle.created", "sandbox.lifecycle.killed"],
    "signatureSecret": "replace-with-a-random-secret"
  }'
```

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | Yes | A display name. Must not be empty. |
| `url` | Yes | An absolute `http` or `https` URL that receives events. |
| `events` | Yes | The event types to receive. Must not be empty. |
| `signatureSecret` | Yes | A secret you choose. AgentENV signs every request with it so you can verify the request came from AgentENV. It is never returned by the API. |
| `enabled` | No | Whether to send events. Defaults to `true`. |

`events` accepts:

| Event | Sent when |
| --- | --- |
| `sandbox.lifecycle.created` | A sandbox is created |
| `sandbox.lifecycle.killed` | A sandbox is deleted, including timeout deletion |
| `sandbox.lifecycle.paused` | A sandbox is paused, including timeout pause |
| `sandbox.lifecycle.resumed` | A paused sandbox resumes |
| `sandbox.lifecycle.forked` | A sandbox is forked, once per child |

The response is `201` with the new webhook, including its `id`:

```json
{
  "id": "01a1211d-bac5-7df0-ac65-267bc797b1cf",
  "name": "my-hook",
  "createdAt": "2026-10-09T14:42:38.917453399Z",
  "teamId": "00000000-0000-0000-0000-000000000000",
  "url": "https://example.com/aenv-events",
  "enabled": true,
  "events": ["sandbox.lifecycle.created", "sandbox.lifecycle.killed"]
}
```

From now on, sandbox lifecycle changes are pushed to your webhook as described
in [Receive Events](#receive-events).

### Update

Send only the fields to change. Any field from registration can be updated,
including `signatureSecret`.

```bash
curl -X PATCH "$AENV_URL/events/webhooks/$WEBHOOK_ID" \
  -H "X-API-Key: $AENV_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

Changes apply to the next event. Retries of an earlier event also use the
updated webhook: a disabled or deleted webhook gets no further retries.

### List

```bash
curl "$AENV_URL/events/webhooks" -H "X-API-Key: $AENV_API_KEY"
```

Returns an array of all registered webhooks.

### Check

```bash
curl "$AENV_URL/events/webhooks/$WEBHOOK_ID" -H "X-API-Key: $AENV_API_KEY"
```

Returns the information of one webhook, or `404` if it does not exist.

### Delete

```bash
curl -X DELETE "$AENV_URL/events/webhooks/$WEBHOOK_ID" \
  -H "X-API-Key: $AENV_API_KEY"
```

Returns `200`. The webhook stops receiving events, and its delivery history is
removed.

## Receive Events

Each event is a `POST` to your webhook URL with these headers:

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

## View Deliveries

Every delivery attempt is recorded for 7 days. Two endpoints read this history.

### Deliveries

Lists delivery attempts grouped by event, newest first:

```bash
curl "$AENV_URL/events/webhooks/$WEBHOOK_ID/deliveries?limit=10" \
  -H "X-API-Key: $AENV_API_KEY"
```

| Query parameter | Meaning |
| --- | --- |
| `limit` | Groups per page, 1 to 100. Defaults to 25. |
| `cursor` | The `nextCursor` from the previous page. |
| `orderAsc` | `true` for oldest first. Defaults to `false`. |
| `start`, `end` | Only attempts in `[start, end)`, as RFC 3339 date-times. |
| `deliveryStatus` | `success`, `failed`, or both, comma-separated. |
| `eventType` | Only these event types, comma-separated. |

Each group lists the event's attempts with the request sent, the response
status, headers, and truncated body, and the error class and message on failure.
`nextCursor` is `null` on the last page. Keep the same filters and order when
passing it back.

### Stats

Summarizes delivery attempts in hourly buckets:

```bash
curl "$AENV_URL/events/webhooks/$WEBHOOK_ID/stats" \
  -H "X-API-Key: $AENV_API_KEY"
```

`start` and `end` select the range and default to the last 24 hours. The
response has the total and failed attempt counts and the minimum, average, and
maximum duration in milliseconds, both overall and per hour. Stats count
attempts, so an event retried 3 times counts 3 times.