# Common Issues

## `/dev/kvm` is missing or inaccessible

**Symptom**: The server cannot find or open `/dev/kvm`.

**Solution**: First check whether standard KVM is available:

```bash
ls -l /dev/kvm
```

If the device exists, ensure it is readable and writable by the runtime user.
On most systems:

```bash
sudo usermod -aG kvm $USER
# Log out and back in for the group change to take effect
```

If the cloud server does not expose standard KVM, follow
[PVM Deployment](../deployment/pvm.md). That guide covers the additional host
setup needed before starting AgentENV.

## The configured virtualization mode does not match the host

**Symptom**: Startup reports that KVM cannot run while the PVM module is
loaded, or that PVM requires additional host setup.

**Solution**: Normal installations should use the default KVM mode. If the
host was prepared for PVM, follow [PVM Deployment](../deployment/pvm.md) and
ensure the service environment contains:

```bash
AENV_VIRTUALIZATION_MODE=pvm
```

## Permission denied for network operations

**Symptom**: Sandbox creation fails with network namespace or iptables errors.

**Solution**: The server requires `CAP_NET_ADMIN`, `CAP_NET_RAW`, and `CAP_SYS_ADMIN` in
its effective, permitted, and inheritable sets. The installed systemd unit
configures these automatically. For a source checkout, use `make start-server`
or `scripts/run-with-capabilities.sh <server-binary>`; do not run the whole
server as root. Also verify that the runtime account belongs to the `kvm`
group, can open `/dev/kvm`, and can open `/dev/ublk-control`.

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

> TODO: Expand with more common issues as they are reported.
