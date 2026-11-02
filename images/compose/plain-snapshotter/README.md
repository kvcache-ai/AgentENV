# containerd-plain-snapshotter

一个极简的 containerd snapshotter 插件，让你可以直接将主机上已有的 rootfs 目录作为 Docker 容器的根文件系统运行，无需镜像层解包、overlay 或任何文件拷贝。

## 解决什么问题

正常情况下，Docker 运行容器需要先拉取镜像，将每一层解压，再通过 overlayfs 叠加成最终的 rootfs。如果你已经有一个现成的 rootfs 目录（比如通过 debootstrap、buildroot、或者直接从其他地方拷贝过来的），想直接用它启动容器，Docker 并不原生支持这种用法。

plain snapshotter 就是为了解决这个问题：它在容器创建时直接把你的目录 bind-mount 进去，零拷贝、零解压。

## 工作原理

### 架构

```
Docker/containerd  ←gRPC→  plain-snapshotter (proxy plugin)
                              ↓
                         config.json (chain ID → rootfs 路径)
                              ↓
                         bind-mount 到你的 rootfs 目录
```

plain snapshotter 作为 containerd 的 **proxy snapshot plugin** 运行，通过 Unix socket 提供 gRPC 服务。

### 注册流程 (`register`)

Docker 要求容器必须关联一个镜像，所以我们需要先在 Docker 中创建一个"占位镜像"：

```
plain-snapshotter register my-app /path/to/rootfs
```

这个命令做三件事：

1. **创建占位镜像** — 构造一个 Docker 镜像 tar（老格式），包含一个 UUID 占位 layer 和镜像 config，通过 `docker load` 加载。这个镜像的文件系统内容不会被使用，运行时由 snapshotter 替换为你指定的 rootfs 目录。
2. **获取 chain ID** — 通过 `docker inspect` 获取镜像的层 chain ID（`sha256:xxx`）。
3. **写入映射** — 将 `chain ID → rootfs 路径` 写入 `config.json`。

如果需要保留原始镜像的 metadata（ENV、ENTRYPOINT、CMD 等），可以通过 `--image-metadata` 指定一个镜像配置 JSON 文件：

```
plain-snapshotter register --image-metadata /tmp/image-config.json my-app /path/to/rootfs
```

不指定 `--image-metadata` 时使用空配置，镜像不包含任何 metadata。

### 运行流程 (`docker run`)

当你执行 `docker run my-app` 时：

1. Docker 让 containerd 创建容器，containerd 调用 snapshotter 的 `Prepare(key, parent=chain_ID)`
2. snapshotter 在 config.json 中查找 parent 的 chain ID
3. 命中 → 返回一个 bind-mount 到你配置的 rootfs 目录
4. containerd 用这个 mount 作为容器的根文件系统
5. 容器直接运行在你的原始目录上

### 关键设计

- **零拷贝**：rootfs 目录通过 bind-mount 直接挂载，不做任何文件复制
- **内存 + 配置文件**：运行时 snapshot 状态存在内存中（重启清空），chain ID 映射持久化在 config.json 中
- **命名空间适配**：Docker 使用带前缀的 snapshot key（如 `moby/2/sha256:xxx`），snapshotter 会自动剥离前缀进行匹配

## 快速开始

### 1. 编译

```bash
go build -o plain-snapshotter .
```

### 2. 配置 containerd

在 `/etc/containerd/config.toml` 的 `[proxy_plugins]` 下添加：

```toml
[proxy_plugins.plain]
  type = "snapshot"
  address = "/run/containerd-plain-snapshotter/snapshotter.sock"
```

### 3. 配置 Docker

编辑 `/etc/docker/daemon.json`，将 plain 设为默认 snapshotter：

```json
{
  "features": {
    "containerd-snapshotter": true
  },
  "storage-driver": "plain"
}
```

### 4. 启动服务

```bash
# 启动 snapshotter
sudo ./plain-snapshotter

# 重启 containerd 和 Docker
sudo systemctl restart containerd
sudo systemctl restart docker
```

### 5. 注册并运行

```bash
# 基本注册：将 rootfs 目录关联为 Docker 镜像（无 metadata）
sudo ./plain-snapshotter register my-busybox ./test/rootfs

# 带 metadata 注册：从已有镜像提取配置，保留 ENV/ENTRYPOINT/CMD 等
docker inspect --format '{{json .Config}}' nginx:latest | jq . > /tmp/nginx-config.json
sudo ./plain-snapshotter register --image-metadata /tmp/nginx-config.json my-nginx /path/to/nginx-rootfs

# 验证 metadata 是否保留
docker inspect --format '{{json .Config.Env}}' my-nginx:latest

# Docker run
docker run --rm my-busybox /bin/sh -c "echo hello"

# Docker Compose
cat > docker-compose.yml <<EOF
services:
  app:
    image: my-busybox
    command: ["/bin/sh", "-c", "echo hello from compose"]
EOF
docker compose up
```

## 命令参考

### 服务模式（默认）

```bash
plain-snapshotter [flags]
  -address string   gRPC socket 路径 (默认 "/run/containerd-plain-snapshotter/snapshotter.sock")
  -root string      状态目录 (默认 "/var/lib/containerd-plain-snapshotter")
```

### 注册模式

```bash
plain-snapshotter register [flags] <镜像名> <rootfs路径>
  -root string      状态目录，须与服务模式一致 (默认 "/var/lib/containerd-plain-snapshotter")
  -image-metadata string    镜像配置 JSON 文件，包含 ENV/ENTRYPOINT/CMD 等 metadata（可选）
```

`-image-metadata` 接受的 JSON 格式支持两种：

- **裸 container config**（推荐）— 即 `docker inspect --format '{{json .Config}}' <image>` 的输出，包含 `Env`、`Entrypoint`、`Cmd`、`WorkingDir` 等字段
- **完整 image config** — 包含 `architecture`、`os`、`rootfs` 等顶层字段的完整 OCI image config，`rootfs` 部分会被自动替换

## 配置文件

`config.json` 位于 `<root>/config.json`，格式为：

```json
{
  "sha256:abc123...": "/absolute/path/to/rootfs"
}
```

key 是镜像层的 chain ID，value 是 rootfs 目录的绝对路径。由 `register` 命令自动维护，一般不需要手动编辑。

## 注意事项

- 容器直接读写你的 rootfs 目录，多个容器共享同一个 rootfs 时请注意数据冲突
- snapshotter 重启后内存中的 snapshot 状态会丢失，但 config.json 中的映射不受影响
- 仅支持通过 `register` 注册的单层镜像，不支持常规的多层 Docker 镜像拉取

