# Authentication

AgentENV uses three credentials for three separate kinds of access:

| Credential | Protects | When required | Header |
| --- | --- | --- | --- |
| API key | AgentENV lifecycle and management APIs | All authenticated control-plane requests | `X-API-Key` |
| `trafficAccessToken` | Services exposed by a sandbox | `allowPublicTraffic: false` | `e2b-traffic-access-token` |
| `envdAccessToken` | envd operations such as command execution and file access | `secure: true` | `X-Access-Token` |

These credentials are not interchangeable. The API key belongs to the AgentENV
deployment; the other two tokens belong to an individual sandbox.

> **Notice:** Authentication verifies credentials but does not encrypt traffic.
> Use HTTPS termination, a VPN, loopback, or a trusted private network to
> protect API keys and sandbox tokens in transit.

---

Where to Go Next:

- [API Authentication](./api-authentication.md) — authenticate lifecycle and management API requests.
- [Application Ingress Authentication](./application-ingress.md) — protect services exposed by a sandbox.
- [Secure Sandbox Authentication](./secure-sandbox.md) — authenticate command execution, file transfer, and other envd operations.
- [Sandbox Access-Token Seed](./access-token-seed.md) — configure the seed used to derive sandbox access tokens.
