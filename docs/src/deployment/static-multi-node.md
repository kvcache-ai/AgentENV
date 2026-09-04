# Static Multi-Node (Without Kubernetes)

Run AgentENV across multiple physical or virtual machines without Kubernetes.
This deployment uses the Go Gateway and Scheduler with a statically configured
runtime-node list.

Static discovery is appropriate when node membership changes infrequently. The
Scheduler does not automatically register an unknown node from its heartbeat:
each runtime node must appear in `scheduler.nodes`, and changing that list
requires a Scheduler restart.

## Architecture

This example co-locates the Gateway and Scheduler on `10.0.0.10` and runs two
AgentENV runtime nodes:

| Component | Address | Purpose |
|---|---|---|
| Gateway | `10.0.0.10:8080` | Client-facing HTTP and WebSocket entry point |
| Scheduler | `10.0.0.10:9090` | gRPC placement, heartbeat, and sandbox binding service |
| Runtime node A | `10.0.0.21:8000` | Runs Firecracker sandboxes as `node-a` |
| Runtime node B | `10.0.0.22:8000` | Runs Firecracker sandboxes as `node-b` |

```mermaid
flowchart LR
    client["Client"] -->|"HTTP / WebSocket"| gateway["Gateway<br/>10.0.0.10:8080"]
    gateway -->|"gRPC"| scheduler["Scheduler<br/>10.0.0.10:9090"]
    gateway -->|"HTTP proxy"| nodeA["Runtime node A<br/>10.0.0.21:8000"]
    gateway -->|"HTTP proxy"| nodeB["Runtime node B<br/>10.0.0.22:8000"]
    nodeA -.->|"heartbeat"| scheduler
    nodeB -.->|"heartbeat"| scheduler
    scheduler -.->|"placement and lookup"| gateway
```

AgentENV authenticates HTTP requests but does not encrypt them. Use private
addresses, a VPN, or TLS termination before traffic crosses an untrusted
network.

## Prerequisites

On every runtime node:

- Linux kernel 6.8+
- `/dev/kvm` access
- root access for the AgentENV installation
- network reachability to the Scheduler
- shared storage across all runtime nodes, using either POSIXFS or OSS

On the control-plane host:

- Go 1.21 or later
- network reachability to every runtime node
- a checkout of the AgentENV repository

Allow the following TCP flows:

| Source | Destination | Port |
|---|---|---|
| Clients | Gateway | `8080` |
| Gateway | Scheduler | `9090` |
| Runtime nodes | Scheduler | `9090` |
| Gateway | Runtime nodes | `8000` |

The examples keep metrics listeners on loopback. Open their ports separately if
an external metrics collector needs them.

## 1. Install the runtime nodes

Generate one API key and one sandbox access-token seed through your normal
secret-management channel. Use the API key on the gateway and every runtime
node; use the seed only on runtime nodes:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
export AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED="$(openssl rand -hex 32)"
```

Run the installation on each runtime node:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh | sudo bash
```

Edit `/etc/default/aenv` on each machine without removing the paths written by
the installer, and add both shared values before starting the services. A
multi-node deployment must not let each node generate independent managed
secrets.

Node A uses:

```bash
API_ADDR="0.0.0.0:8000"
AENV_NODE_ID="node-a"
AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED="true"
AENV_OBSERVABILITY_SCHEDULER_ENDPOINT="http://10.0.0.10:9090"
```

Node B uses the same values except for its unique node ID:

```bash
API_ADDR="0.0.0.0:8000"
AENV_NODE_ID="node-b"
AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED="true"
AENV_OBSERVABILITY_SCHEDULER_ENDPOINT="http://10.0.0.10:9090"
```

The `AENV_NODE_ID` values must exactly match the corresponding IDs in the
Scheduler configuration below. Restart and verify each runtime:

```bash
sudo systemctl restart aenv
sudo systemctl status aenv
curl http://127.0.0.1:8000/health
```

## 2. Build and install the control-plane binaries

On the control-plane host:

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
make -C services build

sudo install -m 0755 services/bin/scheduler /usr/local/bin/agentenv-scheduler
sudo install -m 0755 services/bin/gateway /usr/local/bin/agentenv-gateway
sudo useradd --system --no-create-home --shell /usr/sbin/nologin agentenv-control
sudo install -d -o root -g agentenv-control -m 0750 /etc/agentenv
```

Create `/etc/agentenv/auth.env` with the API key used on the runtime nodes:

```bash
sudo install -o root -g agentenv-control -m 0640 /dev/null /etc/agentenv/auth.env
sudoedit /etc/agentenv/auth.env
```

```text
AENV_API_KEY=<same shared key>
```

If the `agentenv-control` account already exists, the `useradd` command reports
that fact and can be skipped.

## 3. Configure static discovery

Create `/etc/agentenv/control-plane.json`:

```json
{
  "log_level": "info",
  "log_format": "json",
  "scheduler": {
    "grpc_listen_addr": "0.0.0.0:9090",
    "metrics_listen_addr": "127.0.0.1:9101",
    "strategy": "round_robin",
    "report_ttl": "30s",
    "binding_ttl": "30s",
    "discovery": {
      "mode": "static"
    },
    "nodes": [
      {
        "id": "node-a",
        "endpoint": "http://10.0.0.21:8000"
      },
      {
        "id": "node-b",
        "endpoint": "http://10.0.0.22:8000"
      }
    ]
  },
  "gateway": {
    "http_listen_addr": "0.0.0.0:8080",
    "metrics_listen_addr": "127.0.0.1:9102",
    "scheduler_addr": "10.0.0.10:9090",
    "request_timeout": "90s",
    "forward_response_size": 4194304,
    "sandbox_proxy_domains": []
  }
}
```

Protect the configuration after editing it:

```bash
sudo chown root:agentenv-control /etc/agentenv/control-plane.json
sudo chmod 0640 /etc/agentenv/control-plane.json
```

Each node endpoint must be reachable from the Gateway. The Scheduler returns
that endpoint to the Gateway when it selects a node.

## 4. Run the Scheduler and Gateway with systemd

Create `/etc/systemd/system/agentenv-scheduler.service`:

```ini
[Unit]
Description=AgentENV Scheduler
Wants=network-online.target
After=network-online.target

[Service]
User=agentenv-control
Group=agentenv-control
ExecStart=/usr/local/bin/agentenv-scheduler -config /etc/agentenv/control-plane.json
Restart=on-failure
RestartSec=5
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/agentenv-gateway.service`:

```ini
[Unit]
Description=AgentENV Gateway
Wants=network-online.target
After=network-online.target agentenv-scheduler.service

[Service]
User=agentenv-control
Group=agentenv-control
EnvironmentFile=/etc/agentenv/auth.env
ExecStart=/usr/local/bin/agentenv-gateway -config /etc/agentenv/control-plane.json
Restart=on-failure
RestartSec=5
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Start both services:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now agentenv-scheduler agentenv-gateway
```

## 5. Verify the cluster

Check every network hop before creating a sandbox:

```bash
# On the control-plane host
curl http://10.0.0.21:8000/health
curl http://10.0.0.22:8000/health
curl http://127.0.0.1:8080/health

# Wait for node heartbeats, then inspect the cluster through the Gateway
export AENV_API_KEY="$(sudo sed -n 's/^AENV_API_KEY=//p' /etc/agentenv/auth.env)"
curl -H "X-API-Key: ${AENV_API_KEY}" http://127.0.0.1:8080/nodes
```

The node list should contain `node-a` and `node-b`. Point clients at the
Gateway, not directly at a runtime node:

```bash
aenv auth
# AENV server URL: http://10.0.0.10:8080
# API key: <the same shared key>
```

Sandbox create, list, lifecycle, and data-plane requests can then be routed
through the Gateway.

## Operations

Follow service logs:

```bash
sudo journalctl -u agentenv-scheduler -f
sudo journalctl -u agentenv-gateway -f

# On a runtime node
sudo journalctl -u aenv -f
```

To add, remove, rename, or change the endpoint of a static node:

1. Update `scheduler.nodes` in `/etc/agentenv/control-plane.json`.
2. Ensure the runtime's `AENV_NODE_ID` matches its configured ID.
3. Restart the Scheduler.

```bash
sudo systemctl restart agentenv-scheduler
```

Restarting the Scheduler temporarily interrupts routing that depends on its
in-memory state. Runtime heartbeats repopulate observed sandbox assignments
after the Scheduler comes back.

## Troubleshooting

### A runtime is healthy but absent from `/nodes`

- Confirm that `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED` is `true`.
- Include the `http://` scheme in
  `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`.
- Verify that `AENV_NODE_ID` exactly matches a configured
  `scheduler.nodes[].id`.
- Check that the runtime can reach Scheduler port `9090`.
- Inspect `journalctl -u aenv` for heartbeat rejection or connection errors.

Heartbeats from IDs not present in the static node list are rejected; they do
not register new nodes.

### The Gateway returns no available nodes

- Verify both runtime health endpoints from the control-plane host.
- Verify the static endpoint addresses are reachable from the Gateway.
- Check Scheduler logs for expired heartbeat reports.
- Check that host firewalls allow runtime-to-Scheduler and
  Gateway-to-runtime traffic.

### The Gateway cannot connect to the Scheduler

The Gateway's `scheduler_addr` is a gRPC target and does not use an `http://`
prefix. Runtime heartbeat configuration uses a URL and does require that
prefix:

```text
gateway.scheduler_addr = 10.0.0.10:9090
AENV_OBSERVABILITY_SCHEDULER_ENDPOINT = http://10.0.0.10:9090
```
