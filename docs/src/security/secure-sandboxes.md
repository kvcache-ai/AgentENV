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

A seed is a random value used to derive the access token for each sandbox. This seed is optional. When it is unset, each runtime node automatically creates and persists a node-local seed under `$AENV_HOME/secrets`.
This is sufficient for normal single-node operation and does not require additional setup.

Configure the same explicit seed on every runtime node when the deployment needs to recover the same sandbox ID on another node in the future. Generate it once and store it in the deployment's secret manager:

```bash
openssl rand -hex 32
```

Set the value as `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` on every runtime node.
For TOML configuration, use `[sandbox].access_token_hash_seed` instead.

Preserve the seed across upgrades; changing it rotates access tokens for existing secure sandboxes.

### Kubernetes

The runtime DaemonSet reads the optional `agentenv-runtime-secrets` Secret. To configure a shared seed for all runtime Pods, create it before applying the runtime manifests:

```bash
kubectl apply -f deploy/k8s/base/namespace.yaml

AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED="$(openssl rand -hex 32)"
kubectl -n agentenv-system create secret generic agentenv-runtime-secrets \
  --from-literal="sandbox-access-token-hash-seed=${AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED}" \
  --dry-run=client -o yaml | kubectl apply -f -
unset AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED
```

Run this once for a new cluster and preserve the existing Secret during upgrades. An external secret manager may be used instead, provided it creates the same Secret name and key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: agentenv-runtime-secrets
  namespace: agentenv-system
stringData:
  sandbox-access-token-hash-seed: <shared-secret>
```

If the Secret is not created, the DaemonSet still starts and each runtime Pod uses its automatically managed node-local seed.
