# Optional P2P Visibility

P2P visibility lets nodes discover and fetch committed snapshot artifacts from
peers. The snapshot repository remains the durable source of truth, so P2P is
an optional distribution path rather than a replacement for snapshot storage.

Enable both the node-wide P2P transport and snapshot publication in your
AgentENV config file (`config/default.toml`, or the file selected by
`AENV_CONFIG_PATH`):

```toml
[p2p]
enabled = true

[snapshot]
p2p_enabled = true
```

See the [Configuration Reference](../../configuration/reference.md#p2p)
for the optional transport, storage-directory, address, and timeout settings.
