/// Stop the whole owner process before unwinding can reclaim live I/O resources.
/// Recovery requires the deployment to stop its children before restarting it.
pub fn fatal_exit(reason: &str) -> ! {
    // Bypass Rust logging locks and atexit hooks. Only try a nonblocking pipe/socket
    // write: a regular log file can block on disk I/O even with O_NONBLOCK.
    unsafe {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if libc::fstat(libc::STDERR_FILENO, stat.as_mut_ptr()) == 0
            && matches!(
                stat.assume_init().st_mode & libc::S_IFMT,
                libc::S_IFIFO | libc::S_IFSOCK
            )
        {
            let flags = libc::fcntl(libc::STDERR_FILENO, libc::F_GETFL);
            if flags >= 0
                && libc::fcntl(libc::STDERR_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK) == 0
            {
                for bytes in [
                    b"fatal runtime error: ".as_slice(),
                    &reason.as_bytes()[..reason.len().min(512)],
                    b"\n",
                ] {
                    libc::write(libc::STDERR_FILENO, bytes.as_ptr().cast(), bytes.len());
                }
            }
        }
        libc::_exit(70);
    }
}

/// Abort before the protected value is dropped unless its operation completed.
/// Unlike a retained-resource guard, cancellation ends the process immediately.
pub fn abort_on_drop<T>(value: T, reason: &'static str) -> AbortOnDrop<T> {
    AbortOnDrop {
        value: Some(value),
        reason,
    }
}

pub struct AbortOnDrop<T> {
    value: Option<T>,
    reason: &'static str,
}

impl<T> AbortOnDrop<T> {
    pub fn complete(mut self) -> T {
        self.value.take().expect("completion consumes the guard")
    }
}

impl<T> std::ops::Deref for AbortOnDrop<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("guard is pending")
    }
}

impl<T> std::ops::DerefMut for AbortOnDrop<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("guard is pending")
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if self.value.is_some() {
            fatal_exit(self.reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::path::PathBuf;
    use std::task::{Context, Waker};

    #[test]
    fn abandoned_cleanup_exits_before_resource_destructors() {
        const MODE: &str = "AENV_TEST_ABANDONED_CLEANUP";
        const ROOT: &str = "AENV_TEST_CLEANUP_ROOT";
        if let Ok(mode) = std::env::var(MODE) {
            let root = PathBuf::from(std::env::var(ROOT).unwrap());
            let directory = tempfile::Builder::new()
                .prefix("backing")
                .rand_bytes(0)
                .tempdir_in(root)
                .unwrap();
            let resources = abort_on_drop(directory, "required cleanup was interrupted");
            let mut cleanup = Box::pin(async move {
                assert!(resources.path().exists());
                if mode == "panic" {
                    panic!("cleanup panicked");
                }
                std::future::pending::<()>().await;
                drop(resources.complete());
            });
            if std::env::var(MODE).unwrap() != "unpolled" {
                assert!(cleanup
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending());
            }
            drop(cleanup);
            panic!("abandoned cleanup did not terminate process");
        }
        for mode in ["unpolled", "polled", "panic"] {
            let root = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "fail_stop::tests::abandoned_cleanup_exits_before_resource_destructors",
                    "--nocapture",
                ])
                .env(MODE, mode)
                .env(ROOT, root.path())
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(70),
                "{mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(root.path().join("backing").is_dir(), "{mode}");
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_path_buf();
        drop(abort_on_drop(directory, "completed cleanup").complete());
        assert!(!path.exists());
    }
}
