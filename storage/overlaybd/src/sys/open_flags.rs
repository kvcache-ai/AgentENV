//! Open-time flags, and post-open setup, that differ by platform.

use std::fs::File;

use super::SysResult;

/// Alignment requirements for a file opened with direct I/O.
#[derive(Clone, Copy)]
pub struct DirectIoAlignment {
    pub memory: usize,
    pub offset: usize,
}

impl Default for DirectIoAlignment {
    fn default() -> Self {
        // Preserve the existing contract when the OS cannot report file limits.
        Self {
            memory: 512,
            offset: 512,
        }
    }
}

/// Query the already-open file, without resolving its backing device or path.
pub fn direct_io_alignment(file: &File) -> SysResult<DirectIoAlignment> {
    imp::direct_io_alignment(file)
}

/// The open flag requesting cache-bypassing I/O, or `None` on platforms that
/// express it after open instead (see [`enable_direct_io`]).
pub fn direct_io_open_flag() -> Option<i32> {
    imp::DIRECT_IO_OPEN_FLAG
}

/// Finish enabling cache-bypassing I/O on a freshly opened file.
///
/// Call this after `open` whenever direct I/O was requested — on every
/// platform. Linux already got what it needed from [`direct_io_open_flag`] and
/// this is a no-op there; macOS does all of its work here.
///
/// # Platform differences
///
/// macOS has no `O_DIRECT`. The analogue is `fcntl(F_NOCACHE)`, which is weaker
/// in three ways worth knowing about:
///
/// 1. It is applied after open rather than being an open flag — hence this
///    function existing at all.
/// 2. It imposes no alignment requirements on offsets, lengths or buffers
///    (measured: a 100-byte write at offset 1 succeeds). Callers still get the
///    Linux alignment rules enforced, so that `direct_io` means one thing
///    everywhere and unaligned I/O cannot pass on macOS only to fail on Linux.
/// 3. It only keeps *new* pages out of the unified buffer cache; pages already
///    resident are still served from it. So unlike `O_DIRECT` this is **not** a
///    way to read around the cache and observe on-disk state.
///
/// Point 3 is fine for why this is used here — avoiding a second copy of data
/// overlaybd already caches itself — but would silently defeat anyone reaching
/// for direct I/O to bypass caching for correctness.
pub fn enable_direct_io(file: &File) -> SysResult<()> {
    imp::enable_direct_io(file)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::fs::File;

    use super::super::SysResult;

    pub(super) const DIRECT_IO_OPEN_FLAG: Option<i32> = Some(libc::O_DIRECT);

    pub(super) fn direct_io_alignment(file: &File) -> SysResult<super::DirectIoAlignment> {
        use std::os::fd::AsRawFd;

        use nix::errno::Errno;

        use super::super::SysError;

        let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
        // SAFETY: file owns a live descriptor, the empty path is NUL terminated,
        // and stat points to writable storage for the complete statx result.
        let ret = unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                libc::STATX_DIOALIGN,
                stat.as_mut_ptr(),
            )
        };
        if ret != 0 {
            return match Errno::last() {
                Errno::ENOSYS | Errno::EINVAL | Errno::EOPNOTSUPP => Ok(Default::default()),
                errno => Err(SysError::Errno(errno)),
            };
        }
        // SAFETY: statx succeeded and initialized the output.
        let stat = unsafe { stat.assume_init() };
        if stat.stx_mask & libc::STATX_DIOALIGN == 0 {
            return Ok(Default::default());
        }
        if stat.stx_dio_mem_align == 0 || stat.stx_dio_offset_align == 0 {
            return Err(SysError::Unsupported("direct_io on this file"));
        }
        let memory = usize::try_from(stat.stx_dio_mem_align)
            .map_err(|_| SysError::Unsupported("direct_io alignment does not fit usize"))?;
        let offset = usize::try_from(stat.stx_dio_offset_align)
            .map_err(|_| SysError::Unsupported("direct_io alignment does not fit usize"))?;
        if !memory.is_power_of_two() || !offset.is_power_of_two() {
            return Err(SysError::Unsupported(
                "direct_io alignment is not a power of two",
            ));
        }
        Ok(super::DirectIoAlignment { memory, offset })
    }

    /// `O_DIRECT` was set at open time, so there is nothing left to do.
    pub(super) fn enable_direct_io(_file: &File) -> SysResult<()> {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::fs::File;
    use std::os::fd::AsRawFd;

    use super::super::{SysError, SysResult};

    pub(super) fn direct_io_alignment(_file: &File) -> SysResult<super::DirectIoAlignment> {
        Ok(Default::default())
    }

    /// Darwin has no open-time flag for this; see [`enable_direct_io`].
    pub(super) const DIRECT_IO_OPEN_FLAG: Option<i32> = None;

    pub(super) fn enable_direct_io(file: &File) -> SysResult<()> {
        // SAFETY: `file` owns a live descriptor and `F_NOCACHE` takes an int by
        // value.
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if ret != -1 {
            return Ok(());
        }
        Err(SysError::last())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::fs::File;

    use super::super::{SysError, SysResult};

    pub(super) fn direct_io_alignment(_file: &File) -> SysResult<super::DirectIoAlignment> {
        Ok(Default::default())
    }

    pub(super) const DIRECT_IO_OPEN_FLAG: Option<i32> = None;

    pub(super) fn enable_direct_io(_file: &File) -> SysResult<()> {
        Err(SysError::Unsupported("direct_io"))
    }
}
