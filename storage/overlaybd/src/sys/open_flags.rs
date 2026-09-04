//! Open-time flags, and post-open setup, that differ by platform.

use std::fs::File;

use super::SysResult;

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

    pub(super) const DIRECT_IO_OPEN_FLAG: Option<i32> = None;

    pub(super) fn enable_direct_io(_file: &File) -> SysResult<()> {
        Err(SysError::Unsupported("direct_io"))
    }
}
