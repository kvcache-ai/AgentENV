# uvm-ublk

Async Tokio/io_uring userspace block device (`ublk`) library crate backing [`uvm-ublk-daemon`](../ublk-daemon) and AgentENV's OverlayBD runtime block devices.

## Prerequisites

Tested on Ubuntu 24.04 with Linux kernel 6.8+ (`ublk_drv` module with `ublks_max=65536` on 6.8 kernels).

## Daemon Usage

Runtime device creation, deletion, and crash recovery are served by `uvm-ublk-daemon` (provisioned automatically by `cargo run --bin server -- --setup-host` and `make build-ublk`).
