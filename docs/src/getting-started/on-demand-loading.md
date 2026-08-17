# On-Demand Loading from Shared Storage

AgentENV loads images on demand via overlaybd. Local disk acts as a bounded cache,
retaining hot data and evicting cold data so nodes do not need to pre-warm every
image or keep a complete copy of every snapshot.

## Prerequisites

- Storage connectivity should be as fast as possible. Use at least a 1 Gbps
  network; 10 Gbps or faster is strongly recommended.

## 1. Open the configuration file

If AgentENV is running as a systemd service, open the installed configuration:

```bash
sudo vim /var/lib/aenv/config/config.toml  # Or use the path to your config file.
```

If AgentENV is running in Docker, download and edit the default configuration:

```bash
curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/config/default.toml -o config.toml
vim config.toml
```

## 2. Configure shared storage

AgentENV supports two shared storage backends: POSIXFS and OSS. Choose one of
the following options.

### Option A: POSIXFS

Enable the POSIXFS backend, set the shared snapshot location, and increase the
remote-block cache:

```toml
[snapshot]
repository_backend = "posix_fs"

[backend.posix_fs]
snapshot_store = "/mnt/aenv-snapshots"

[image.cache.remote_blocks]
max_size_gb = 100
```

### Option B: OSS

Enable the OSS backend, configure the OSS connection, and increase the
remote-block cache:

```toml
[snapshot]
repository_backend = "oss"

[backend.oss]
endpoint = "YOUR_ENDPOINT"
bucket = "YOUR_BUCKET"
region = "YOUR_REGION"
prefix = "YOUR_PREFIX"
cache_max_size_gb = 100
access_key_id = "YOUR_ACCESS_KEY_ID"
access_key_secret = "YOUR_ACCESS_KEY_SECRET"

[image.cache.remote_blocks]
max_size_gb = 100
```

If your provider requires or prefers virtual-host bucket addressing — for
example [Tigris](https://www.tigrisdata.com/docs/) or a Cloudflare R2
deployment — also set `addressing_style = "virtual"`; see the
[configuration reference](../configuration/reference.md#backendoss) for details.

## 3. Apply the configuration

If AgentENV is running as a systemd service, restart it:

```bash
sudo systemctl restart aenv
```

If AgentENV is running in Docker, stop the current container and recreate it with
the updated configuration and shared snapshot directory:

```bash
# The /mnt/aenv-snapshots mount is required only when using POSIXFS.
docker stop CONTAINER_ID_OR_NAME
docker run --rm -it --name aenv-server \
  --device /dev/kvm --privileged -v /dev:/dev \
  -v "$PWD/config.toml:/workspace/config/default.toml:ro" \
  -v /mnt/aenv-snapshots:/mnt/aenv-snapshots \
  -p 8000:8000 \
  ghcr.io/kvcache-ai/aenv-server:latest
```

Once configured, AgentENV loads templates and snapshots from shared storage by
default.
