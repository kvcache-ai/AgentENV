# Summary

# Getting Started

- [Overview](./getting-started/overview.md)
- [Quick Start](./getting-started/quickstart.md)
- [On-Demand Loading](./getting-started/on-demand-loading.md)
- [aenv CLI Reference](./getting-started/aenv-cli/index.md)
  - [Authentication](./getting-started/aenv-cli/authentication.md)
  - [Sandboxes](./getting-started/aenv-cli/sandboxes.md)
  - [Templates](./getting-started/aenv-cli/templates.md)
  - [Snapshots](./getting-started/aenv-cli/snapshots.md)
  - [Volumes](./getting-started/aenv-cli/volumes.md)
  - [Shell Completion](./getting-started/aenv-cli/shell-completion.md)

# Deployment

- [Docker (Single Node)](./deployment/docker.md)
- [Docker Compose (Multi-Node Simulation)](./deployment/docker-compose.md)
- [Static Multi-Node (Without Kubernetes)](./deployment/static-multi-node.md)
- [Kubernetes (Multi-Node)](./deployment/kubernetes.md)
- [Manual Compile (Single Node)](./deployment/manual-compile.md)
- [PVM Deployment (When KVM Is Unavailable)](./deployment/pvm.md)

# Use Cases

- [Run Python Code](./use-cases/python.md)
- [Train Terminal-Bench-2 with Miles](./use-cases/miles.md)

# Core Concepts

- [How AgentENV Works](./concepts/overview.md)
- [Authentication](./concepts/authentication/index.md)
  - [API Authentication](./concepts/authentication/api-authentication.md)
  - [Application Ingress Authentication](./concepts/authentication/application-ingress.md)
  - [Secure Sandbox Authentication](./concepts/authentication/secure-sandbox.md)
  - [Sandbox Access-Token Seed](./concepts/authentication/access-token-seed.md)
- [Sandboxes](./concepts/sandboxes/index.md)
  - [Starting a Sandbox](./concepts/sandboxes/starting.md)
  - [Working with Sandboxes](./concepts/sandboxes/working.md)
  - [Auto-Eviction](./concepts/sandboxes/auto-eviction.md)
  - [Networking](./concepts/sandboxes/networking.md)
  - [Proxy](./concepts/sandboxes/proxy.md)
- [Templates](./concepts/templates/index.md)
  - [Create Your Template](./concepts/templates/creating.md)
  - [Manage Templates](./concepts/templates/managing.md)
- [Snapshots](./concepts/snapshots/index.md)
  - [Create a Snapshot](./concepts/snapshots/create.md)
  - [Manage Snapshots](./concepts/snapshots/manage.md)
  - [Rootfs as an OCI Image](./concepts/snapshots/use.md)
  - [Optional P2P Visibility](./concepts/snapshots/p2p.md)
- [Volumes](./concepts/volumes/index.md)
  - [Create and Mount Volumes](./concepts/volumes/create-and-mount.md)
  - [Manage Volumes](./concepts/volumes/manage.md)
  - [Sandbox Forks and Snapshots](./concepts/volumes/sandbox-forks-and-snapshots.md)
- [Custom Extension](./concepts/custom-extension/index.md)
  - [Lifecycle Hooks](./concepts/custom-extension/lifecycle-hooks.md)
  - [Use the Extension](./concepts/custom-extension/connect-and-use.md)

# API Reference

- [API Reference](./api/index.md)

# Integration

- [E2B](./integration/e2b.md)

# Troubleshooting

- [Common Issues](./troubleshooting/common-issues.md)

# Configuration

- [Configuration Reference](./configuration/reference.md)
- [Environment Variables](./configuration/env-vars.md)

---

# Developer Internals

- [System Architecture](./internals/architecture.md)
- [Sandbox Network Architecture](./internals/networking.md)
- [Sandbox Internals and Testing](./internals/sandbox-testing.md)
- [Template Builder and Testing](./internals/template-builder-testing.md)
- [BuildKit Template Builds](./internals/buildkit-template-builds.md)
- [Persistence Artifact Inventory](./internals/persistence-artifact-inventory.md)
- [Proxy Design](./internals/proxy-design.md)
- [Distributed Control Plane](./internals/services.md)
- [P2P Artifact Transport](./internals/p2p-design.md)
