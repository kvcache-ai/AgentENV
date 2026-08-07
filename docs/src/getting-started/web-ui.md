# Web UI (control plane)

The AgentENV Web UI is a Next.js console for operators and developers to manage sandboxes, snapshots, templates, and nodes through the **Gateway HTTP API**.

## Prerequisites

- A running AgentENV Gateway (single-node `:8000` or multi-node Gateway `:8080`)
- An API key header value (`X-API-Key`) accepted by the deployment
- Optional admin token (`X-Admin-Token`) for `/nodes` APIs

## Local development

```bash
cd web
pnpm install
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000). In **Settings**, set:

| Field | Example (Compose Gateway) | Example (single node) |
|---|---|---|
| Gateway URL | `http://127.0.0.1:8080` | `http://127.0.0.1:8000` |
| API key | any non-empty key | same |
| Admin token | optional | optional |

Credentials are stored in httpOnly session cookies. They are cleared on logout and, because the cookies carry no expiry, when the browser session ends.

## Docker Compose

The `web` service is defined in `deploy/docker-compose.yml` and published on host port **3000**.

```bash
docker compose -f deploy/docker-compose.yml up -d --build web
```

Then open [http://127.0.0.1:3000](http://127.0.0.1:3000) and point Settings at `http://127.0.0.1:8080` (Gateway published on the host).

Build context for the image is `web/` using `deploy/docker/Dockerfile.web`.

## Kubernetes

Kustomize base includes `agentenv-web` Deployment + Service (`deploy/k8s/base/`).

```bash
make k8s-apply
kubectl -n agentenv-system port-forward svc/agentenv-web 3000:3000
```

Default in-cluster Gateway URL env: `http://agentenv-gateway:8080`. When using the UI from a browser on your laptop via port-forward, set Settings to the Gateway URL **reachable from the Next.js server** (in-cluster service name if server-side fetches run in-pod) or use host-accessible URLs consistently.
