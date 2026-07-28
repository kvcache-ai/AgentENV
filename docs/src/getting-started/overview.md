# Getting Started

AgentENV (abbreviated as AENV) is a self-hosted sandbox runtime for AI agents. It runs isolated Firecracker microVMs and exposes an E2B-compatible HTTP API — so existing E2B SDK code works against it without modification.
The repository is available at <https://github.com/kvcache-ai/AgentENV>.

## Why AgentENV

- **Scale across diverse environments**: AENV runs massive numbers of Firecracker environments across machines and diverse OCI-compatible images, loaded on demand via overlaybd. Local disk acts as a bounded cache, retaining hot data and evicting cold, so images can exceed disk capacity while startup stays fast cluster-wide, without pre-warming every host.
- **Make idle environments inexpensive**: Snapshot-backed environments boot or resume in under 50 ms and pause in under 100 ms. Idle environments can quickly release CPU and memory, then return when new work arrives.
- **Native snapshot and fork support**: AENV snapshots memory and filesystem changes incrementally, completing in under 100 ms even under heavy disk modification. A running environment can fork into multiple independent sandboxes for parallel agent workflows. Snapshots persist to S3-compatible object storage or a shared distributed filesystem to prevent data loss.
- **Preserve performance and density over time**: AENV delivers high-performance I/O via ublk while sharing the host page cache across storage and memory-snapshot data. Memory ballooning returns reclaimable guest memory to the host, sustaining high overcommit as environments run longer and diverge.

## Features

- **Firecracker microVMs** with full Linux kernel isolation per sandbox
- **Pause and resume** with memory + disk snapshots for instant cold start
- **Layered block devices** via overlaybd + ublk for copy-on-write image sharing
- **Snapshot-backed template builder** for publishing reusable, pre-configured sandbox runtimes
- **E2B-compatible API** so existing E2B SDKs and CLIs work out of the box
- **Reverse proxy** to reach services running inside sandboxes via HTTP and WebSocket
- **Multi-node scaling** with a gateway + scheduler control plane (prototype)

## Who Is This For

AgentENV is built for teams running AI agents that need isolated execution environments: code interpreters, tool-use agents, autonomous coding agents, or any workload where you want a fresh (or cached) Linux environment per task.

## Interacting with the Server

AgentENV exposes an HTTP API. There are four ways to use it:

| Method | Best for |
|--------|----------|
| **[aenv CLI](./aenv-cli.md)** | Interactive use, scripting, local development |
| **[E2B](../integration/e2b.md)** | Application code — existing E2B-based applications work with AgentENV without modification |
| **[HTTP API](../api/index.md)** | Direct control, other languages, automation |

## Where to Go Next

- **[Quick Start](./quickstart.md)** — Install the server, run your first sandbox. Takes ~5 minutes on a supported Linux host.
- **[Deployment](../deployment/manual-compile.md)** — Build from source, Docker Compose multi-node, or Kubernetes.
- **[PVM Deployment](../deployment/pvm.md)** — Use AgentENV on a server where standard KVM is unavailable.
