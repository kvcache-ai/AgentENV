# Custom Extension

The custom extension is an optional external HTTP service that AgentENV calls
during the sandbox lifecycle. It lets you implement deployment-specific
behavior—such as connecting sandboxes to a VPN, applying custom firewall rules,
or adding mounts—without changing AgentENV itself.

The extension implements a small set of HTTP endpoints called hooks, and
AgentENV acts as the client. The interface is defined in
[`src/custom_extension_api/openapi.yml`](https://github.com/kvcache-ai/AgentENV/blob/main/src/custom_extension_api/openapi.yml).

---

Where to Go Next:

- [Lifecycle Hooks](./lifecycle-hooks.md) — implement the four lifecycle endpoints.
- [Use the Extension](./connect-and-use.md) — configure AgentENV and manage extension-specific Sandbox parameters.
