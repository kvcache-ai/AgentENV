#![cfg(feature = "runtime")]

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn start(budget: &str) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_aenv-compose-start"))
        .arg(budget)
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn wait(mut child: std::process::Child) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("startup process did not exit without stdin EOF");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

#[test]
fn missing_input_obeys_deadline_without_eof() {
    assert!(wait(start("0.05")).contains("deadline exceeded"));
}

#[test]
fn malformed_frame_exits_without_eof() {
    let mut child = start("2");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"not-json\n")
        .unwrap();
    assert!(wait(child).contains("invalid Compose startup plan"));
}

#[test]
fn signal_cancels_waiting_for_input() {
    let child = start("30");
    std::thread::sleep(Duration::from_millis(100));
    // SAFETY: this PID belongs to the child created by this test.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    assert!(wait(child).contains("received SIGTERM"));
}

#[test]
fn rejects_invalid_timeout() {
    for budget in ["0", "-1", "NaN", "inf", "invalid"] {
        assert!(wait(start(budget)).contains("timeout"));
    }
}
