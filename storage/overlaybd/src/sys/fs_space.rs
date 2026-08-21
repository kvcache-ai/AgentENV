//! Filesystem space accounting.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use nix::errno::Errno;

use super::{SysError, SysResult};

/// Space accounting for the filesystem holding a path, in bytes.
///
/// Returning derived byte counts rather than a `statvfs` keeps the platform's
/// integer widths out of callers: Darwin's `f_blocks` and `f_bavail` are 32-bit
/// where Linux's are 64-bit, so multiplying them by the fragment size has a
/// different result type per target. Normalizing once here means the arithmetic
/// is written once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FsSpace {
    /// Total size of the filesystem.
    pub capacity_bytes: u64,
    /// Bytes available to an unprivileged process.
    pub avail_bytes: u64,
}

/// Query the filesystem holding `path`.
///
/// `statvfs` exists on every target this crate builds for — a syscall wrapper on
/// Linux, a libc wrapper over `statfs` on Darwin — so unlike the other
/// capabilities in [`super`] this one needs no per-platform arm, only the width
/// normalization above. Verified against `df` on both: `f_frsize * f_blocks`
/// reproduces the reported size exactly.
///
/// # Two portability traps, both deliberately avoided here
///
/// - **Use `f_frsize`, never `f_bsize`.** The block counts are in `f_frsize`
///   units on both platforms, but `f_bsize` means "preferred I/O size" and
///   diverges wildly: measured 4096 on Linux against **1048576** on macOS.
///   Multiplying the counts by `f_bsize` would overstate macOS capacity 256x.
/// - **Darwin's counts are 32-bit.** `f_blocks` and `f_bavail` are 4 bytes
///   there against 8 on Linux, so with a 4096-byte `f_frsize` this saturates at
///   2^32 * 4096 = 16 TiB. A cache directory on a larger volume would report
///   wrong numbers on macOS; switching the Darwin arm to `statfs`, whose counts
///   are 64-bit, is the fix if that ever matters.
///
/// Darwin's `man 3 statvfs` also warns that "portable applications must not
/// depend on" the structure being filled in, which is part of why the result is
/// narrowed to two derived numbers rather than exposed as a `statvfs`.
// `useless_conversion` is allowed because it is only useless on *one* target:
// these fields are already `u64` on Linux but 32-bit on Darwin, so removing the
// conversions as clippy suggests would break the macOS build.
#[allow(clippy::useless_conversion)]
pub fn fs_space(path: &Path) -> SysResult<FsSpace> {
    let cpath =
        CString::new(path.as_os_str().as_bytes()).map_err(|_| SysError::Errno(Errno::EINVAL))?;

    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `cpath` is a valid NUL-terminated path and `statvfs` fully
    // initializes `st` when it returns 0.
    let ret = unsafe { libc::statvfs(cpath.as_ptr(), st.as_mut_ptr()) };
    if ret != 0 {
        return Err(SysError::last());
    }
    // SAFETY: `statvfs` returned 0, so `st` is initialized.
    let st = unsafe { st.assume_init() };

    // `u64::from` rather than `as`: it compiles for both the 32-bit Darwin and
    // 64-bit Linux field types while still refusing a lossy conversion if a
    // future target widens something unexpectedly.
    let frsize = u64::from(st.f_frsize);
    Ok(FsSpace {
        capacity_bytes: frsize.saturating_mul(u64::from(st.f_blocks)),
        avail_bytes: frsize.saturating_mul(u64::from(st.f_bavail)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_space_reports_nonzero_capacity_for_tempdir() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let space = fs_space(dir.path()).expect("statvfs on tempdir");
        assert!(space.capacity_bytes > 0, "capacity should be positive");
        assert!(
            space.avail_bytes <= space.capacity_bytes,
            "available ({}) must not exceed capacity ({})",
            space.avail_bytes,
            space.capacity_bytes
        );
    }

    /// Doubles as the check that [`SysError::last`] really reads `errno` on this
    /// platform: asserting the exact code, not merely that some error came back,
    /// is what proves `nix::errno::Errno::last()` works here. It is only correct
    /// because every caller of `SysError::last` in this module invokes it
    /// immediately after the failing syscall, before anything can clobber
    /// `errno`.
    #[test]
    fn fs_space_fails_with_enoent_for_missing_path() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let missing = dir.path().join("no-such-directory");
        let err = fs_space(&missing).expect_err("statvfs on missing path must fail");
        match err {
            SysError::Errno(errno) => assert_eq!(
                errno,
                Errno::ENOENT,
                "statvfs on a missing path must report ENOENT, got {errno}"
            ),
            SysError::Unsupported(op) => {
                panic!("statvfs is available on all targets, got Unsupported({op})")
            }
        }
    }
}
