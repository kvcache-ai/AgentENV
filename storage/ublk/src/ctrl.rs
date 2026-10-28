use anyhow::{Context, Error, Result};
use io_uring::{opcode::UringCmd80, types};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use storage_util::io_ring::IoRingHandle;

use crate::ublk_caps;

const CTRL_PATH: &str = "/dev/ublk-control";
const UBLK_MODULE_PATH: &str = "/sys/module/ublk_drv";
const MODPROBE_ATTEMPTS: &[(&str, &[&str])] = &[
    ("modprobe", &["ublk_drv"]),
    ("sudo", &["-n", "modprobe", "ublk_drv"]),
];

/// Returns `true` if the `ublk_drv` kernel module is currently loaded.
pub fn ublk_module_loaded() -> bool {
    Path::new(UBLK_MODULE_PATH).exists()
}

/// Returns `true` if ublk is usable: the module is loaded (or can be loaded)
/// and the control device is accessible.
pub fn ublk_available() -> bool {
    if !ublk_module_loaded() {
        // Best-effort silent load attempt
        for &(prog, args) in MODPROBE_ATTEMPTS {
            if run_command(prog, args) && ublk_module_loaded() {
                break;
            }
        }
    }
    ublk_module_loaded() && Path::new(CTRL_PATH).exists()
}

fn run_command(program: &str, args: &[&str]) -> bool {
    match Command::new(program).args(args).status() {
        Ok(status) if status.success() => true,
        Ok(status) => {
            tracing::debug!(
                command = format!("{} {}", program, args.join(" ")),
                status = %status,
                "command exited unsuccessfully while preparing ublk"
            );
            false
        }
        Err(err) => {
            tracing::debug!(
                command = format!("{} {}", program, args.join(" ")),
                error = %err,
                "command failed while preparing ublk"
            );
            false
        }
    }
}

/// Ensure the `ublk_drv` kernel module is loaded, attempting `modprobe` if not.
///
/// Tries in order: bare `modprobe`, then non-interactive `sudo -n modprobe`.
pub fn load_ublk_module() -> Result<()> {
    if ublk_module_loaded() {
        return Ok(());
    }

    tracing::info!("ublk kernel module not loaded; attempting automatic load");
    for command in MODPROBE_ATTEMPTS {
        if run_command(command.0, command.1) && ublk_module_loaded() {
            tracing::info!("loaded ublk kernel module");
            return Ok(());
        }
    }

    anyhow::bail!(
        "ublk kernel module is not loaded and automatic loading failed. \
         Try `sudo modprobe ublk_drv` or rerun `cargo run --bin server -- --setup-only`.",
    )
}

fn ctrl_open_error(err: std::io::Error) -> Error {
    let message = match err.kind() {
        std::io::ErrorKind::NotFound if !ublk_module_loaded() => format!(
            "failed to open {CTRL_PATH}: ublk kernel module may not be loaded. \
               Try `sudo modprobe ublk_drv` or rerun `cargo run --bin server -- --setup-only`"
        ),
        std::io::ErrorKind::NotFound => format!(
            "failed to open {CTRL_PATH}: the ublk control device is missing. \
               Ensure the ublk device node is present and rerun `cargo run --bin server -- --setup-only` if needed"
        ),
        std::io::ErrorKind::PermissionDenied => format!(
            "failed to open {CTRL_PATH}: permission denied. Ensure the current user has access \
               to the ublk control device, or rerun `cargo run --bin server -- --setup-only` to refresh permissions"
        ),
        _ => format!("failed to open {CTRL_PATH}"),
    };
    Error::new(err).context(message)
}

fn open_ctrl_file() -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(CTRL_PATH)
        .map_err(ctrl_open_error)
}

pub struct UVMUblkCtrlBuilder<'a> {
    name: &'a str,
    nr_queues: u16,
    /// The IO depth of each queue
    depth: u16,
    /// Maximum size of each IO buffer size
    io_buf_bytes: u32,
    /// The flags for the ublk device
    ctrl_flags: u64,
    dev_id: u32,
    /// Stored by the kernel in `ublksrv_flags` and returned by GET_DEV_INFO.
    owner_tag: u64,
}

impl<'a> Default for UVMUblkCtrlBuilder<'a> {
    fn default() -> Self {
        Self {
            name: "unknown",
            nr_queues: 1,
            depth: 16,
            io_buf_bytes: 256 * 1024,
            ctrl_flags: 0,
            dev_id: u32::MAX,
            owner_tag: 0,
        }
    }
}

impl<'a> UVMUblkCtrlBuilder<'a> {
    /// By default, the io buf size is 256 KB
    pub fn new() -> Self {
        Self::default()
    }

    pub fn name(mut self, name: &'a str) -> Self {
        self.name = name;
        self
    }

    pub fn nr_queues(mut self, nr_queues: u16) -> Self {
        self.nr_queues = nr_queues;
        self
    }

    /// Set the IO depth of each queue.
    pub fn depth(mut self, depth: u16) -> Self {
        self.depth = depth;
        self
    }

    pub fn max_io_buf_bytes(mut self, io_buf_bytes: u32) -> Self {
        self.io_buf_bytes = io_buf_bytes;
        self
    }

    pub fn zero_copy(mut self, zc: bool) -> Self {
        if zc {
            self.ctrl_flags |= ublk_sys::UBLK_F_AUTO_BUF_REG as u64;
        } else {
            self.ctrl_flags &= !(ublk_sys::UBLK_F_AUTO_BUF_REG as u64);
        }
        self
    }

    /// Keep the device recoverable when its ublk server exits: I/O queues
    /// until a new server attaches with user recovery, and outstanding I/O
    /// is reissued to it.
    pub fn user_recovery(mut self, enabled: bool) -> Self {
        if enabled {
            self.ctrl_flags |= ublk_caps::USER_RECOVERY_FLAGS;
        } else {
            self.ctrl_flags &= !ublk_caps::USER_RECOVERY_FLAGS;
        }
        self
    }

    /// Tag the device with a value of the ublk server's choosing. The kernel
    /// keeps it in `ublksrv_flags` for the device's lifetime, unlike
    /// `ublksrv_pid`, which it resets when the server exits, so a later server
    /// can tell which leftover devices belong to it.
    pub fn owner_tag(mut self, tag: u64) -> Self {
        self.owner_tag = tag;
        self
    }

    /// Later when add device, we try to use exact the `id` as its device id.
    pub fn dev_id(mut self, id: u32) -> Self {
        self.dev_id = id;
        self
    }

    pub fn build(self, ring: IoRingHandle<io_uring::squeue::Entry128>) -> Result<UVMUblkCtrl> {
        load_ublk_module()?;
        let name = self.name.to_string();
        let dev_info = ublk_sys::ublksrv_ctrl_dev_info {
            nr_hw_queues: self.nr_queues,
            queue_depth: self.depth,
            max_io_buf_bytes: self.io_buf_bytes,
            // for create purpose, we assign u32::MAX, the kernel code
            // will check this
            dev_id: self.dev_id,
            ublksrv_pid: nix::unistd::getpid().as_raw(),
            flags: self.ctrl_flags,
            ublksrv_flags: self.owner_tag,
            ..Default::default()
        };
        let ctrl_file = open_ctrl_file()?;
        Ok(UVMUblkCtrl {
            name,
            dev_info,
            ctrl_file,
            ring,
        })
    }
}

pub struct UVMUblkCtrl {
    pub name: String,
    pub dev_info: ublk_sys::ublksrv_ctrl_dev_info,
    pub ctrl_file: std::fs::File,
    pub ring: IoRingHandle<io_uring::squeue::Entry128>,
}

#[repr(C)]
union CtrlCmd {
    data: [u8; 80],
    cmd: ublk_sys::ublksrv_ctrl_cmd,
}

impl UVMUblkCtrl {
    /// UBLK_U_CMD_GET_FEATURES
    pub async fn get_features(&self) -> Result<u64> {
        let mut features = 0u64;
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                addr: (&raw mut features) as u64,
                len: std::mem::size_of::<u64>() as u16,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_GET_FEATURES,
        )
        .cmd(unsafe { cmd.data })
        .build();

        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send get_features ctrl cmd to ring")?;
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res))
                .context("get_features ctrl request");
        }
        Ok(features)
    }

    /// UBLK_U_CMD_UPDATE_SIZE
    pub fn update_size(
        &self,
        new_sectors: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let dev_id = self.dev_info.dev_id;
        let fd = self.ctrl_file.as_raw_fd();
        let ring = self.ring.clone();

        async move {
            let cmd = CtrlCmd {
                cmd: ublk_sys::ublksrv_ctrl_cmd {
                    dev_id,
                    queue_id: u16::MAX,
                    data: [new_sectors],
                    ..Default::default()
                },
            };
            let sqe = UringCmd80::new(types::Fd(fd), ublk_caps::UBLK_U_CMD_UPDATE_SIZE)
                .cmd(unsafe { cmd.data })
                .build();

            let res = ring
                .send_io(sqe)
                .await
                .context("send update_size ctrl cmd to ring")?;
            if res != 0 {
                return Err(std::io::Error::from_raw_os_error(-res))
                    .context("update_size ctrl request");
            }
            Ok(())
        }
    }

    /// UBLK_U_CMD_ADD_DEV
    pub async fn add_dev(&mut self) -> Result<()> {
        // The ADD_DEV command needs: dev_info.dev_id == ublksrv_ctrl_cmd.dev_id.
        // The builder defaults to u32::MAX, which asks the kernel to assign an ID.
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                addr: (&raw mut self.dev_info) as u64,
                len: std::mem::size_of::<ublk_sys::ublksrv_ctrl_dev_info>() as u16,
                dev_id: self.dev_info.dev_id,
                // NOTE: the ADD_DEV command will check queue_id == (u16)-1
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_ADD_DEV,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send add_dev ctrl cmd to ring")?;
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res)).context("add_dev ctrl request");
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(dev_id=self.dev_info.dev_id), err(Debug))]
    pub async fn del_dev(&mut self) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                dev_id: self.dev_info.dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_DEL_DEV,
        )
        .cmd(unsafe { cmd.data })
        .build();
        loop {
            let res = self
                .ring
                .send_io(sqe.clone())
                .await
                .context("send del_dev ctrl cmd to ring")?;
            match res {
                0 => break,
                errno if errno == -libc::EINTR => {
                    tracing::info!("del_dev encounter EINTR, just retry");
                    continue;
                }
                errno => {
                    return Err(std::io::Error::from_raw_os_error(-errno))
                        .context("del_dev ctrl request")
                }
            }
        }
        Ok(())
    }

    pub async fn start_dev(&mut self) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                data: [nix::unistd::getpid().as_raw() as u64],
                dev_id: self.dev_info.dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_START_DEV,
        )
        .cmd(unsafe { cmd.data })
        .build();
        loop {
            let res = self
                .ring
                .send_io(sqe.clone())
                .await
                .context("send start_dev ctrl cmd to ring")?;
            match res {
                0 => break,
                // if encounter EINTR, just retry
                errno if errno == -libc::EINTR => continue,
                errno => {
                    return Err(std::io::Error::from_raw_os_error(-errno))
                        .context("start_dev ctrl request")
                }
            }
        }
        Ok(())
    }

    pub async fn stop_dev(&mut self) -> Result<()> {
        let dev_id = self.dev_info.dev_id;
        tracing::debug!(dev_id, "begin to stop_dev");
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_STOP_DEV,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send stop_dev ctrl cmd to ring")?;
        tracing::info!(dev_id, res, "stop_dev ctrl cmd completed");
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res)).context("stop_dev ctrl request");
        }
        Ok(())
    }

    pub async fn set_params(&mut self, param: ublk_sys::ublk_params) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                addr: (&raw const param) as u64,
                len: std::mem::size_of::<ublk_sys::ublk_params>() as u16,
                dev_id: self.dev_info.dev_id,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_SET_PARAMS,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send set_params ctrl cmd to ring")?;
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res)).context("set_params ctrl request");
        }
        Ok(())
    }

    /// UBLK_U_CMD_GET_PARAMS
    pub async fn get_params(&self) -> Result<ublk_sys::ublk_params> {
        let mut params = ublk_sys::ublk_params {
            len: std::mem::size_of::<ublk_sys::ublk_params>() as u32,
            ..Default::default()
        };
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                addr: (&raw mut params) as u64,
                len: std::mem::size_of::<ublk_sys::ublk_params>() as u16,
                dev_id: self.dev_info.dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_GET_PARAMS,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send get_params ctrl cmd to ring")?;
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res)).context("get_params ctrl request");
        }
        Ok(params)
    }

    /// UBLK_U_CMD_START_USER_RECOVERY
    ///
    /// The kernel returns `EBUSY` until the previous ublk server has released
    /// `/dev/ublkcN` and the device is quiesced, so retry until `timeout`.
    pub async fn start_user_recovery(&mut self, timeout: Duration) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                dev_id: self.dev_info.dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_START_USER_RECOVERY,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let res = self
                .ring
                .send_io(sqe.clone())
                .await
                .context("send start_user_recovery ctrl cmd to ring")?;
            match res {
                0 => return Ok(()),
                errno if errno == -libc::EINTR => continue,
                errno if errno == -libc::EBUSY && tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                errno => {
                    return Err(std::io::Error::from_raw_os_error(-errno))
                        .context("start_user_recovery ctrl request")
                }
            }
        }
    }

    /// UBLK_U_CMD_END_USER_RECOVERY
    ///
    /// Like START_DEV, this completes once every queue has fetched its
    /// requests, and makes the current process the device's ublk server.
    pub async fn end_user_recovery(&mut self) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                data: [nix::unistd::getpid().as_raw() as u64],
                dev_id: self.dev_info.dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_END_USER_RECOVERY,
        )
        .cmd(unsafe { cmd.data })
        .build();
        loop {
            let res = self
                .ring
                .send_io(sqe.clone())
                .await
                .context("send end_user_recovery ctrl cmd to ring")?;
            match res {
                0 => return Ok(()),
                errno if errno == -libc::EINTR => continue,
                errno => {
                    return Err(std::io::Error::from_raw_os_error(-errno))
                        .context("end_user_recovery ctrl request")
                }
            }
        }
    }

    pub async fn get_dev_info(&mut self) -> Result<()> {
        let cmd = CtrlCmd {
            cmd: ublk_sys::ublksrv_ctrl_cmd {
                addr: (&raw mut self.dev_info) as u64,
                len: std::mem::size_of::<ublk_sys::ublksrv_ctrl_dev_info>() as u16,
                dev_id: self.dev_info.dev_id,
                ..Default::default()
            },
        };
        let sqe = UringCmd80::new(
            types::Fd(self.ctrl_file.as_raw_fd()),
            ublk_sys::UBLK_U_CMD_GET_DEV_INFO,
        )
        .cmd(unsafe { cmd.data })
        .build();
        let res = self
            .ring
            .send_io(sqe)
            .await
            .context("send get_dev_info ctrl cmd to ring")?;
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(-res))
                .context("get_dev_info ctrl request");
        }
        Ok(())
    }

    pub fn get_cdev_path(&self) -> PathBuf {
        PathBuf::from(format!("/dev/ublkc{}", self.dev_info.dev_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_recovery_toggles_recovery_and_reissue_flags() {
        let recovery =
            (ublk_sys::UBLK_F_USER_RECOVERY | ublk_sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;
        let zero_copy = ublk_sys::UBLK_F_AUTO_BUF_REG as u64;

        let builder = UVMUblkCtrlBuilder::new()
            .zero_copy(true)
            .user_recovery(true);
        assert_eq!(builder.ctrl_flags, recovery | zero_copy);

        let builder = builder.user_recovery(false);
        assert_eq!(builder.ctrl_flags, zero_copy);
    }
}
