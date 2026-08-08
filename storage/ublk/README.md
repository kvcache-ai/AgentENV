# ublk

This is a ublk server binary, similar to [libublk-rs](https://github.com/ublk-org/libublk-rs), but targeted at the tokio async runtime.

## Prerequisite

This ublk daemon is only tested on Ubuntu 24.04, with Ubuntu kernel version as 6.8.0-87-generic and 6.17.0-1007-oem.

You'd better make sure your kernel version is 6.8.

## Build

```rust
cargo build

# Or release mode
# cargo build --release

# Check help
./target/debug/uvm-ublk --help
```

## Running

`uvm-ublk` attempts to load the `ublk` kernel module automatically when it starts. If automatic loading fails, please load it manually following the below instructions, or rerun `cargo run --bin server -- --setup-only` to refresh both module state and device permissions.

```
# First insert kernel module
modprobe ublk_drv ublks_max=65536

# Then prepare OverlayBD global and image configs
GLOBAL_CONFIG=/path/to/overlaybd-global.json
IMAGE_CONFIG=/path/to/image.json
UBLK_ID=1

# This will start a command in current shell
./target/debug/uvm-ublk create --nr-queues 1 --depth 16 "$UBLK_ID" overlaybd \
    --global-config "$GLOBAL_CONFIG" \
    --image-config "$IMAGE_CONFIG"

# Then you can access /dev/ublkb1 in another shell

# To remove the ublk device
./target/debug/uvm-ublk delete 1
```

Note that the `ublks_max=65536` module parameter is mainly used for lower-version kernels (e.g., 6.8.0). If your kernel version is higher (e.g., 6.17), you do not need this.
