use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use std::io::Write;
use std::path::PathBuf;
use tracing_log::log::LevelFilter;

use storage_util::io_ring::spawn_io_ring_worker;
use uvm_ublk::{
    delete_dev, setup_tracing, wait_for_ublk_dev, OverlaybdTarget, OverlaybdTargetConfig,
    UVMUblkCtrlBuilder, UVMUblkDevBuilder,
};

#[derive(Debug, Subcommand)]
enum Device {
    Overlaybd(OverlaybdTargetConfig),
}

#[derive(Debug, Parser)]
#[command(about = "create or delete the uvm ublk device.")]
enum Command {
    /// Create a new device, the current process will start to serve the IO request.
    #[clap(visible_alias("add"))]
    Create {
        #[command(flatten)]
        args: CreateArgs,
        #[command(subcommand)]
        device: Device,
    },
    /// Delete the device with specific id.
    #[clap(visible_alias("del"))]
    Delete {
        /// The device id that want to delete.
        dev_id: u32,
    },
    /// Recovery a previously started but stopped device.
    /// (This is a future feature, has not been implemented yet).
    Recovery,
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// The number of queues, about this device. Each queue will corresponds to
    /// a worker thread and an io uring.
    #[arg(long, default_value_t = 1)]
    nr_queues: u16,
    /// The depth of the queues, which is the concurrency within each queue.
    #[arg(long, default_value_t = 16)]
    depth: u16,
    /// The max io buffer for each slot with the queue. For example, if the
    /// depth is 16, and io_buf_size_kb is 256, then each queue will allocate
    /// 4 MB buffer, 256 KB for each slot within the queue.
    #[arg(long, default_value_t = 256)]
    io_buf_size_kb: u32,
    /// The log level, possible values includes: "off", "error", "warn", "info", "debug", "trace".
    #[arg(long, default_value = "info")]
    log_level: LevelFilter,
    /// Store the path to the pid file of the device.
    #[arg(long)]
    pid_file: Option<PathBuf>,
    /// The device id. it is the user's responsibility to prevent
    /// id conflict (if `dev_id` already exists, the process will exit).
    dev_id: u32,
}

async fn create_device(args: CreateArgs, device: Device) -> Result<()> {
    if args.nr_queues != 1 {
        bail!("currently only support single queue");
    }
    let (ctrl_ring, _) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
    let ctrl = UVMUblkCtrlBuilder::new()
        .nr_queues(args.nr_queues)
        .depth(args.depth)
        .max_io_buf_bytes(args.io_buf_size_kb * 1024)
        .dev_id(args.dev_id)
        .name("overlaybd-blk")
        .build(ctrl_ring)
        .context("build ctrl")?;
    match device {
        Device::Overlaybd(dev_args) => {
            let tgt = OverlaybdTarget::from_config(&dev_args)
                .await
                .expect("create overlaybd target");
            let mut dev = UVMUblkDevBuilder::new(ctrl)
                .set_target(tgt)
                .build()
                .await
                .expect("create overlaybd dev");
            dev.start().await.expect("start overlaybd dev");

            wait_for_ublk_dev(dev.dev_id()).expect("wait for ublkb device to show up");
            println!("ready");
            std::io::stdout().flush().expect("flush ready notification");
            dev.wait_for_bg_tasks().await;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    //
    // NOTE: why we use fork:
    // We need a notification method for user (the one that spawn uvm-ublk binary) that the devices
    // has prepared. Of course, the caller can poll the existence of /dev/ublkb<id>, but that might
    // not be a good idea.
    // When this process exited, the block device should be ready.
    let cmd = Command::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    rt.block_on(async {
        match cmd {
            Command::Create { args, device } => {
                setup_tracing(None, args.log_level).context("setup tracing log")?;
                create_device(args, device).await
            }
            Command::Delete { dev_id } => {
                setup_tracing(None, LevelFilter::Info).context("setup tracing log")?;
                let (ring, _) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
                delete_dev(ring, dev_id).await
            }
            Command::Recovery => {
                unimplemented!();
            }
        }
    })
}
