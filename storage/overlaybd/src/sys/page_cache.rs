//! Dropping page-cache residency for a file range.

use std::os::fd::BorrowedFd;

use super::SysResult;

/// Hint that the page cache backing `[offset, offset + len)` can be dropped.
///
/// A `len` of zero means "through end of file", following the Linux
/// `posix_fadvise` convention.
///
/// This is advisory: it never changes what a subsequent read returns, only
/// whether that read has to touch the device. macOS has no equivalent and
/// returns [`super::SysError::Unsupported`], so callers that treat eviction as a
/// hint should map that variant to success rather than propagating it.
pub fn evict_page_cache(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
    imp::evict_page_cache(fd, offset, len)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use nix::errno::Errno;

    use super::super::{to_off_t, SysError, SysResult};

    pub(super) fn evict_page_cache(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
        let offset = to_off_t(offset)?;
        let len = to_off_t(len)?;
        // SAFETY: `fd` is a live borrowed descriptor and `posix_fadvise` only
        // reads its scalar arguments.
        let ret =
            unsafe { libc::posix_fadvise(fd.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
        if ret == 0 {
            return Ok(());
        }
        // `posix_fadvise` returns the error number directly instead of setting
        // `errno`.
        Err(SysError::Errno(Errno::from_raw(ret)))
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::os::fd::BorrowedFd;

    use super::super::{SysError, SysResult};

    /// Darwin has no `posix_fadvise`. `F_NOCACHE` changes the caching policy for
    /// all future I/O on the descriptor rather than evicting a range that is
    /// already resident, so it is not a substitute; `madvise` only applies to a
    /// mapping the caller owns. Report the gap and let callers decide.
    pub(super) fn evict_page_cache(_fd: BorrowedFd<'_>, _offset: u64, _len: u64) -> SysResult<()> {
        Err(SysError::Unsupported("evict_page_cache"))
    }
}
