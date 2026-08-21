//! Reserving storage for a byte range of a file without changing its size.

use std::os::fd::BorrowedFd;

use super::SysResult;

/// Ask the filesystem to allocate storage for `[offset, offset + len)`, keeping
/// the file's logical size unchanged.
///
/// This is the inverse of [`release_space_hint`](super::release_space_hint), and
/// unlike that one it is **not** a hint: `Ok(())` means the blocks are reserved,
/// so a later store into the range cannot fail for want of space.
///
/// The motivating caller is the full-file cache, which writes refilled blocks
/// through a shared mmap over a sparse file. Storing into an unallocated hole
/// makes the kernel allocate at page-fault time, where there is no way to report
/// `ENOSPC` to the faulting instruction — Linux turns it into `SIGBUS` and kills
/// the process. Reserving up front moves that failure to a plain error return.
///
/// # Platform support
///
/// Callers must handle [`SysError::Unsupported`](super::SysError::Unsupported)
/// as "nothing was reserved", not as a failure: **macOS cannot do this at all**
/// (see below), so treating it as an error would break every caller on that
/// platform rather than degrading it. This is why `Unsupported` is a separate
/// variant instead of an errno.
///
/// Linux also reports `Unsupported` when the filesystem lacks `fallocate`
/// (`EOPNOTSUPP`/`ENOSYS`) — tmpfs and some network filesystems. Reserving is
/// then impossible for the same reason, and the caller degrades identically.
pub fn reserve_space(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
    if len == 0 {
        return Ok(());
    }
    imp::reserve_space(fd, offset, len)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use nix::errno::Errno;

    use super::super::{to_off_t, SysError, SysResult};

    pub(super) fn reserve_space(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
        let offset = to_off_t(offset)?;
        let len = to_off_t(len)?;
        // KEEP_SIZE so the reservation never moves the file length: callers size
        // their file up front and validate it on reload.
        //
        // SAFETY: `fd` is a live borrowed descriptor and `fallocate` only reads
        // its scalar arguments.
        let ret =
            unsafe { libc::fallocate(fd.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, offset, len) };
        if ret == 0 {
            return Ok(());
        }
        match Errno::last() {
            // The filesystem has no fallocate. Distinguished from a real
            // failure because the caller has to keep working without a
            // reservation, exactly as it must on macOS.
            Errno::EOPNOTSUPP | Errno::ENOSYS => Err(SysError::Unsupported("reserve_space")),
            errno => Err(SysError::Errno(errno)),
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::os::fd::BorrowedFd;

    use super::super::{SysError, SysResult};

    /// Darwin has no equivalent of `fallocate(FALLOC_FL_KEEP_SIZE)`, so this
    /// always reports `Unsupported`.
    ///
    /// **Do not try to implement this with `F_PREALLOCATE`.** That call cannot
    /// name an interior offset at all: with `F_PEOFPOSMODE` the kernel rejects
    /// any non-zero `fst_offset` outright — `bsd/kern/kern_descrip.c` reads
    ///
    /// ```text
    /// case F_PEOFPOSMODE:
    ///         if (alloc_struct.fst_offset != 0) { error = EINVAL; goto outdrop; }
    /// ```
    ///
    /// and `man 2 fcntl` documents the same `EINVAL`. The other position mode,
    /// `F_VOLPOSMODE`, is not a file offset either despite the name: it is a
    /// physical placement hint that becomes `blockHint` for the volume
    /// allocator. Both modes only extend allocation past the fork's end, which
    /// is why portable Rust wrappers (`fs2`, `fs3`, `fs4`) expose `allocate`
    /// with no offset parameter. `posix_fallocate` does not exist on Darwin.
    ///
    /// Realizing an interior range therefore requires writing to it — the
    /// approach Chromium's `AllocateFileRegion` and SQLite's `fcntlSizeHint`
    /// fall back to (read a byte per block, write it back when the block reads
    /// as zero). That trades one syscall pair per block and dirties the page
    /// cache, so it belongs to the caller that can judge the cost, not to a
    /// primitive that silently performs I/O. Reporting `Unsupported` keeps the
    /// choice where the information is.
    pub(super) fn reserve_space(_fd: BorrowedFd<'_>, _offset: u64, _len: u64) -> SysResult<()> {
        Err(SysError::Unsupported("reserve_space"))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::os::fd::BorrowedFd;

    use super::super::{SysError, SysResult};

    pub(super) fn reserve_space(_fd: BorrowedFd<'_>, _offset: u64, _len: u64) -> SysResult<()> {
        Err(SysError::Unsupported("reserve_space"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::os::fd::AsFd;
    use std::os::unix::fs::MetadataExt;

    /// A sparse file of `len` logical bytes with nothing allocated.
    fn sparse_file(len: u64) -> (tempfile::TempDir, File) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("reserve.bin"))
            .expect("create file");
        file.set_len(len).expect("set_len");
        (dir, file)
    }

    #[test]
    fn zero_length_is_a_noop() {
        let (_dir, file) = sparse_file(4096);
        reserve_space(file.as_fd(), 0, 0).expect("zero length must succeed");
        assert_eq!(file.metadata().expect("stat").len(), 4096);
    }

    /// On a platform that supports reserving, storage is actually allocated and
    /// the logical size is untouched. Where it is unsupported the call must say
    /// so through `Unsupported` rather than a bare errno, since callers branch
    /// on exactly that to keep working.
    #[test]
    fn reserving_allocates_without_resizing() {
        let len = 64 * 1024;
        let (_dir, file) = sparse_file(len);
        let blocks_before = file.metadata().expect("stat").blocks();

        match reserve_space(file.as_fd(), 0, len) {
            Ok(()) => {
                let meta = file.metadata().expect("stat");
                assert_eq!(meta.len(), len, "logical size must be preserved");
                assert!(
                    meta.blocks() > blocks_before,
                    "storage should be reserved: {} -> {} blocks",
                    blocks_before,
                    meta.blocks()
                );
            }
            Err(err) => assert!(
                err.is_unsupported(),
                "a platform without range reservation must report Unsupported, got {err}"
            ),
        }
    }

    /// Reserving an interior range must not change the file's logical size
    /// either — the caller sized the file already and validates it on reload.
    #[test]
    fn reserving_an_interior_range_keeps_the_logical_size() {
        let len = 64 * 1024;
        let (_dir, file) = sparse_file(len);

        match reserve_space(file.as_fd(), 16 * 1024, 16 * 1024) {
            Ok(()) => {}
            Err(err) => assert!(err.is_unsupported(), "unexpected failure: {err}"),
        }
        assert_eq!(
            file.metadata().expect("stat").len(),
            len,
            "logical size must be preserved"
        );
    }
}
