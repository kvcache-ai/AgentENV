#![cfg(target_os = "linux")]
use runtime_policy::{
    unix_millis,
    watchdog::{ProcessHandle, Watchdog, MODE},
    ExecutionLease,
};
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::{fd::OwnedFd, unix::net::UnixStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime},
};
use uuid::Uuid;
const PROBE: &str = env!("CARGO_BIN_EXE_runtime-policy-probe");
struct Cleanup(Child);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn lease(ms: u64) -> ExecutionLease {
    ExecutionLease {
        activation_id: Uuid::now_v7(),
        operation_id: Uuid::now_v7(),
        sequence: 0,
        expires_at_unix_ms: unix_millis(SystemTime::now()).unwrap() + ms,
    }
}
fn await_exit(process: &ProcessHandle, deadline: Instant) {
    while !process.exited() {
        assert!(
            Instant::now() < deadline,
            "runtime continued beyond its funded cutoff"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn expiry_and_lost_control_channel_stop_the_exact_process() {
    for close in [false, true] {
        let target = Cleanup(Command::new("sleep").arg("60").spawn().unwrap());
        let process = ProcessHandle::open(target.0.id() as i32).unwrap();
        let watchdog = Watchdog::start(
            Path::new(PROBE),
            target.0.id() as i32,
            lease(if close { 10_000 } else { 500 }),
        )
        .unwrap();
        let mut watchdog = Some(watchdog);
        if close {
            watchdog.take();
        }
        await_exit(&process, Instant::now() + Duration::from_secs(2));
    }
}
#[test]
fn stalled_api_owner_cannot_stall_execution_deadline() {
    let mut owner = Cleanup(Command::new(PROBE).stdout(Stdio::piped()).spawn().unwrap());
    let mut line = String::new();
    BufReader::new(owner.0.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let process = ProcessHandle::open(line.trim().parse().unwrap()).unwrap();
    assert_eq!(unsafe { libc::kill(owner.0.id() as i32, libc::SIGSTOP) }, 0);
    await_exit(&process, Instant::now() + Duration::from_secs(2));
}
#[test]
fn partial_renewal_frame_cannot_hold_the_deadline_thread() {
    let target = Cleanup(Command::new("sleep").arg("60").spawn().unwrap());
    let process = ProcessHandle::open(target.0.id() as i32).unwrap();
    let (mut parent, child) = UnixStream::pair().unwrap();
    parent
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let child: OwnedFd = child.into();
    let _watchdog = Cleanup(
        Command::new(PROBE)
            .arg(MODE)
            .arg(target.0.id().to_string())
            .stdin(Stdio::from(child))
            .spawn()
            .unwrap(),
    );
    let payload = serde_json::to_vec(&lease(500)).unwrap();
    parent
        .write_all(&(payload.len() as u32).to_be_bytes())
        .unwrap();
    parent.write_all(&payload).unwrap();
    let mut size = [0; 4];
    parent.read_exact(&mut size).unwrap();
    let mut reply = vec![0; u32::from_be_bytes(size) as usize];
    parent.read_exact(&mut reply).unwrap();
    parent.write_all(&100u32.to_be_bytes()).unwrap();
    parent.write_all(b"{").unwrap();
    await_exit(&process, Instant::now() + Duration::from_secs(2));
}
