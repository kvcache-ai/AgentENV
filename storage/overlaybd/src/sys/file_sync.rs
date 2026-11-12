//! Synchronizing a file and its shared memory mapping.

use std::os::fd::BorrowedFd;

use storage_util::MMapRegion;

use super::SysResult;

/// Synchronize mapped writes before publishing metadata that references them.
///
/// Linux uses `fsync`. macOS first synchronizes the entire mapping with
/// `msync(MS_SYNC)`, then issues `F_FULLFSYNC` on the file. Ordinary `fsync` on
/// macOS can leave mmap pages dirty; `msync` alone does not provide the full
/// synchronization needed for newly allocated sparse-file ranges.
///
/// The caller must supply a descriptor for the file backing `region` and
/// coordinate writes, truncation, and hole punching so that data being
/// published is not changed during synchronization. Both borrows must remain
/// valid for the entire call. This operation blocks; async callers should run
/// it on a blocking thread while keeping the resources and guards alive.
///
/// Any synchronization failure is returned without falling back to a weaker
/// operation. Unsupported platforms return [`super::SysError::Unsupported`].
pub fn sync_mapped_file(fd: BorrowedFd<'_>, region: &MMapRegion) -> SysResult<()> {
    imp::sync_mapped_file(fd, region)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use storage_util::MMapRegion;

    use super::super::{SysError, SysResult};

    pub(super) fn sync_mapped_file(fd: BorrowedFd<'_>, _region: &MMapRegion) -> SysResult<()> {
        // SAFETY: `fd` is a live borrowed descriptor.
        if unsafe { libc::fsync(fd.as_raw_fd()) } != 0 {
            return Err(SysError::last());
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use storage_util::MMapRegion;

    use super::super::{SysError, SysResult};

    pub(super) fn sync_mapped_file(fd: BorrowedFd<'_>, region: &MMapRegion) -> SysResult<()> {
        // SAFETY: `region` keeps the nonempty, page-aligned shared mapping
        // alive for this call. msync does not invalidate the mapping.
        if unsafe { libc::msync(region.as_ptr().cast(), region.len(), libc::MS_SYNC) } != 0 {
            return Err(SysError::last());
        }
        // SAFETY: `fd` is a live borrowed descriptor; F_FULLFSYNC takes no
        // additional argument.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_FULLFSYNC) } != 0 {
            return Err(SysError::last());
        }
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::os::fd::BorrowedFd;

    use storage_util::MMapRegion;

    use super::super::{SysError, SysResult};

    pub(super) fn sync_mapped_file(_fd: BorrowedFd<'_>, _region: &MMapRegion) -> SysResult<()> {
        Err(SysError::Unsupported("sync_mapped_file"))
    }
}
