//! A separate process enforces each funded runtime's deadline. It owns a
//! pidfd, so PID reuse cannot redirect a kill. Loss of the owning server's
//! socket also stops execution. This mode starts before the server's Tokio
//! runtime, config, cgroups or network subsystems are initialized.

use crate::{ExecutionLease, LeaseState};
use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

pub const MODE: &str = "--execution-watchdog";
const MAX_FRAME: usize = 4096;

#[derive(Serialize, Deserialize)]
struct Reply {
    error: Option<String>,
}

pub struct Watchdog {
    channel: UnixStream,
    child: Option<Child>,
}

impl Watchdog {
    pub fn start(executable: &Path, pid: i32, lease: ExecutionLease) -> Result<Self> {
        lease.remaining(SystemTime::now())?;
        let (channel, child_channel) = UnixStream::pair()?;
        channel.set_read_timeout(Some(Duration::from_secs(2)))?;
        channel.set_write_timeout(Some(Duration::from_secs(2)))?;
        let child_fd: OwnedFd = child_channel.into();
        let child = Command::new(executable)
            .arg(MODE)
            .arg(pid.to_string())
            .stdin(Stdio::from(child_fd))
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("start independent execution watchdog")?;
        let mut result = Self {
            channel,
            child: Some(child),
        };
        result.renew(lease)?;
        Ok(result)
    }

    pub fn renew(&mut self, lease: ExecutionLease) -> Result<()> {
        send(&mut self.channel, &lease)?;
        let reply: Reply = receive(&mut self.channel)?;
        if let Some(error) = reply.error {
            bail!("watchdog refused execution lease: {error}");
        }
        Ok(())
    }

    pub fn exited(&mut self) -> bool {
        self.child
            .as_mut()
            .is_none_or(|child| !matches!(child.try_wait(), Ok(None)))
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.channel.shutdown(std::net::Shutdown::Both);
        if let Some(mut child) = self.child.take() {
            // Reap without blocking a runtime worker on process exit.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
    }
}

fn send<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_FRAME {
        bail!("watchdog frame is too large");
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}

fn receive<T: DeserializeOwned>(stream: &mut UnixStream) -> Result<T> {
    let mut size = [0; 4];
    stream.read_exact(&mut size)?;
    let size = u32::from_be_bytes(size) as usize;
    if size == 0 || size > MAX_FRAME {
        bail!("invalid watchdog frame size");
    }
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Linux pidfds name the actual process rather than a recyclable PID.
pub struct ProcessHandle(OwnedFd);

impl ProcessHandle {
    #[cfg(target_os = "linux")]
    pub fn open(pid: i32) -> Result<Self> {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("open runtime pidfd");
        }
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd as i32) }))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn open(_pid: i32) -> Result<Self> {
        bail!("runtime enforcement requires Linux pidfds");
    }

    #[cfg(target_os = "linux")]
    pub fn kill(&self) -> Result<()> {
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.0.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn kill(&self) -> Result<()> {
        bail!("runtime enforcement requires Linux pidfds");
    }

    pub fn exited(&self) -> bool {
        let mut fd = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        unsafe { libc::poll(&mut fd, 1, 0) > 0 && fd.revents != 0 }
    }
}

struct StopOnExit(Arc<ProcessHandle>);
impl Drop for StopOnExit {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

/// The parent passes the private duplex control socket as stdin. No listener
/// or public endpoint can grant execution time to this process.
pub fn run(pid: i32) -> Result<()> {
    let target = StopOnExit(Arc::new(ProcessHandle::open(pid)?));
    let fd = unsafe { libc::dup(libc::STDIN_FILENO) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut channel = unsafe { UnixStream::from_raw_fd(fd) };
    channel.set_read_timeout(Some(Duration::from_secs(1)))?;
    channel.set_write_timeout(Some(Duration::from_secs(1)))?;
    let first: ExecutionLease = receive(&mut channel)?;
    let state = Arc::new(Mutex::new(LeaseState::new(
        first,
        SystemTime::now(),
        Instant::now(),
    )?));
    // Deadline enforcement never waits for control-channel I/O. A partial
    // renewal frame or a wedged parent cannot stall the cutoff.
    let timer_target = Arc::clone(&target.0);
    let timer_state = Arc::clone(&state);
    std::thread::spawn(move || loop {
        if timer_target.exited() {
            std::process::exit(0);
        }
        let expired = timer_state
            .lock()
            .map(|lease| lease.expired(SystemTime::now(), Instant::now()))
            .unwrap_or(true);
        if expired {
            let _ = timer_target.kill();
            std::process::exit(0);
        }
        std::thread::sleep(Duration::from_millis(10));
    });
    send(&mut channel, &Reply { error: None })?;
    loop {
        if target.0.exited()
            || state
                .lock()
                .unwrap()
                .expired(SystemTime::now(), Instant::now())
        {
            return Ok(());
        }
        let mut fds = [
            libc::pollfd {
                fd: channel.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: target.0 .0.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let remaining = state.lock().unwrap().lease().remaining(SystemTime::now())?;
        let timeout = remaining.as_millis().clamp(1, 100) as i32;
        let polled = unsafe { libc::poll(fds.as_mut_ptr(), 2, timeout) };
        if polled < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error.into());
        }
        if fds[1].revents != 0 {
            return Ok(());
        }
        if fds[0].revents != 0 {
            let next = receive(&mut channel)?;
            let result = state
                .lock()
                .unwrap()
                .renew(next, SystemTime::now(), Instant::now());
            send(
                &mut channel,
                &Reply {
                    error: result.err().map(|error| error.to_string()),
                },
            )?;
        }
    }
}
