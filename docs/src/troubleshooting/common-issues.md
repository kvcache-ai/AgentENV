# Common Issues

## `/dev/kvm` not accessible

**Symptom**: Server fails to start with a KVM-related error.

**Solution**: Ensure your host has hardware virtualization enabled (Intel VT-x or AMD-V) and that `/dev/kvm` is readable by the current user. On most systems:

```bash
sudo usermod -aG kvm $USER
# Log out and back in for the group change to take effect
```

## Permission denied for network operations

**Symptom**: Sandbox creation fails with network namespace or iptables errors.

**Solution**: The server requires both `CAP_NET_ADMIN` and `CAP_SYS_ADMIN` in
its effective, permitted, and inheritable sets. The installed systemd unit
configures these automatically. For a source checkout, use `make start-server`
or `scripts/run-with-capabilities.sh <server-binary>`; do not run the whole
server as root. Also verify that the runtime account belongs to the `kvm` group
and can open `/dev/ublk-control`.

## Sandbox namespaces are missing from `ip netns list`

**Symptom**: Sandboxes are running, but `ip netns list` does not show their
network namespaces and `ip netns exec <name>` cannot find them.

**Solution**: AgentENV stores namespace mount points under
`$AENV_RUNTIME_PATH/netns` instead of `/var/run/netns`; the installed service
uses `/run/aenv/netns`. Inspect that directory directly and enter a namespace
by path when needed:

```bash
sudo nsenter --net=/run/aenv/netns/agentenv-ns-<slot> ip addr
```

## Config file not found

**Symptom**: `Error: config file not found`

**Solution**: The server looks for `config/default.toml` by default. Either run from the repository root or set `AENV_CONFIG_PATH`:

```bash
export AENV_CONFIG_PATH=/path/to/your/config.toml
```

## Port already in use

**Symptom**: `Address already in use` when starting the server.

**Solution**: Another process is using port 8000. Either stop it or change the listen address:

```bash
API_ADDR=0.0.0.0:8001 make start-server
```

## Sandbox creation timeout

**Symptom**: `POST /sandboxes` returns a timeout error.

**Solution**: Check that runtime assets (Firecracker binary, kernel, rootfs) have been downloaded. The server auto-provisions them on first start, but network issues can cause failures. Run `cargo run --bin server -- --setup-only` to provision manually and see detailed errors.

Also check `[envd].init_timeout_secs` in your config. The default is 60 seconds. If the rootfs image is large, the in-guest envd daemon may need more time to initialize.

## Crabbox cannot reach AgentENV

**Symptom**: `crabbox doctor --provider e2b` or
`crabbox run --provider e2b` fails with auth, DNS, TLS, or connection errors.

**Solution**:

1. Install the upstream CLI: `brew install openclaw/tap/crabbox` (see
   [crabbox.sh](https://crabbox.sh/)).
2. Set the AgentENV control-plane URL, a non-empty key, and an existing template:

```bash
export CRABBOX_E2B_API_URL=https://agentenv.example.com
export CRABBOX_E2B_API_KEY=e2b_000000
export CRABBOX_E2B_TEMPLATE=<template-id-or-name>
```

3. Confirm `crabbox doctor --provider e2b` succeeds. If it fails, verify the API
   URL is reachable and was set explicitly in the environment. Crabbox refuses
   inherited credentials paired only with a repository-configured endpoint.
4. If `doctor` succeeds but `warmup` or `run` fails, verify AgentENV advertises a
   sandbox domain:

   ```bash
   export AENV_SANDBOX_PROXY_DOMAINS=sandbox.agentenv.example.com
   ```

   Wildcard DNS and TLS for `*.sandbox.agentenv.example.com` must route to the
   AgentENV server or gateway. Crabbox does not read `E2B_SANDBOX_URL`; a plain
   loopback API URL alone cannot carry its file and process traffic.
5. Install the example with private permissions if you want project template
   and workdir defaults:

   ```bash
   install -m 600 config/crabbox.example.yaml .crabbox.yaml
   ```

   If doctor reports `permissions 0644 want 0600`, run
   `chmod 600 .crabbox.yaml`.

See [Crabbox integration](../integration/crabbox.md).

> TODO: Expand with more common issues as they are reported.
