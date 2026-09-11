# aenv CLI Reference

`aenv` is the native CLI for AgentENV. It wraps the HTTP API and envd gRPC
endpoints into a developer-friendly interface for managing templates,
sandboxes, persistent volumes, and snapshots.

## Installation

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install-cli.sh | bash
```

Or build from source (requires Rust):

```bash
git clone https://github.com/kvcache-ai/AgentENV.git
cd AgentENV
make install-aenv
```


