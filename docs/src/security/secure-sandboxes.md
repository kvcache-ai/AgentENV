# Secure Sandboxes

Secure sandboxes use an envd access token for control-plane communication. This protects envd operations such as command execution and file access.

> [!NOTE]
> Secure mode protects envd control-plane operations. Application traffic uses
> the sandbox-scoped `trafficAccessToken` described in
> [Authentication](../configuration/authentication.md).

Set `secure: true` when creating a sandbox through API or E2B-compatible SDKs to enable secure mode. Or use the CLI:

```bash
aenv start --secure <template-id>
```

The API and SDKs return the sandbox's `envdAccessToken` where appropriate and attach it to envd requests automatically. The application proxy credential is independent and is sent as `e2b-traffic-access-token`. Forked sandboxes get independent credentials. Secure mode is preserved across pause, restart, and resume; legacy sandboxes remain non-secure unless created with `secure: true`.

## Access-Token Seed

A seed is a random value used to derive each sandbox's envd and traffic access tokens. This seed is optional for a standalone runtime. When it is unset, the runtime automatically creates and persists a seed under `$AENV_HOME/secrets`.
This is sufficient for normal single-node operation and does not require additional setup.

Configure the same explicit seed on the gateway and every runtime node in a clustered deployment. Generate it once and store it in the deployment's secret manager:

```bash
openssl rand -hex 32
```

Set the value as `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` on the gateway and every runtime node.
For TOML configuration, use `[sandbox].access_token_hash_seed` instead.
Container deployments may mount it at
`/run/secrets/sandbox-access-token-hash-seed`.

Preserve the seed across upgrades; changing it rotates both sandbox access tokens.

### Kubernetes

`make k8s-apply` generates and preserves the seed in `Secret/agentenv-auth`, then injects it into the gateway and runtime Pods. Set `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` before applying to supply your own value.

An external secret manager may provide the same Secret and key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: agentenv-auth
  namespace: agentenv-system
stringData:
  AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED: <shared-secret>
```
