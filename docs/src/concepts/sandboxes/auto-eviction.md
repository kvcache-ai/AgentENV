# Auto-Eviction

Every running sandbox has a time-to-live (TTL). The TTL establishes an
expiration deadline so a sandbox cannot occupy CPU and memory indefinitely.
When the TTL is reached,
AgentENV automatically pauses or deletes the sandbox so those resources can be
reclaimed.

## Behavior at Expiration

When a sandbox reaches its TTL, AgentENV performs its configured timeout
action:

- **Delete** (`autoPause: false`, the default): permanently remove the sandbox.
- **Pause** (`autoPause: true`): preserve the sandbox so it can be resumed
  later. Its snapshot occupies storage until it is resumed or deleted.

The timeout action is selected when the sandbox is created. The `aenv start`
command uses the default action, `autoPause: false`. To pause on expiration
instead, create the sandbox through the API with `autoPause: true`.

Warm start from a template or snapshot:

```bash
curl -X POST \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "templateID": "my-template",
    "timeout": 600,
    "autoPause": true
  }' \
  http://127.0.0.1:8000/sandboxes
```

Cold start from an OCI image:

```bash
curl -X POST \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "image": "ubuntu:24.04",
    "timeout": 600,
    "autoPause": true
  }' \
  http://127.0.0.1:8000/sandboxes-cold
```

## Set or Extend the Deadline

`aenv start --timeout <seconds>` sets the initial TTL. If an automatically
paused sandbox is needed again, `aenv resume --timeout <seconds>` resumes it and
sets a new TTL from the resume time. Both commands default to 300 seconds.

For a running sandbox, replace its deadline with an exact number of seconds
from now:

```bash
aenv timeout <sandbox-id> 600
```

This sets the deadline to 600 seconds from the time the command is sent.
Calling it again replaces the previous deadline, so it can either extend or
shorten the remaining time.

To keep a running sandbox alive without shortening a later existing deadline,
use the refresh API:

```bash
curl -X POST \
  -H 'X-API-Key: test-key' \
  -H 'Content-Type: application/json' \
  -d '{"duration": 600}' \
  http://127.0.0.1:8000/sandboxes/<sandbox-id>/refreshes
```

Refresh does not shorten the remaining TTL if the current deadline is
later. Refresh applies only to a running sandbox; resume a paused sandbox
first. If `duration` is omitted, the server's default sandbox timeout is used.

`aenv connect` resumes a paused sandbox when it connects and ensures that its
TTL is at least the default 300 seconds.

