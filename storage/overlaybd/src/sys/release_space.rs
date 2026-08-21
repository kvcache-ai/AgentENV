//! Returning a file's storage to the filesystem without shrinking the file.

use std::os::fd::BorrowedFd;

use nix::errno::Errno;

use super::{SysError, SysResult};

/// Ask the filesystem to release the storage backing `[offset, offset + len)`,
/// keeping the file's logical size unchanged.
///
/// This is a hint **about space, not about contents**. Two things it explicitly
/// does not promise:
///
/// - That the range reads back as zeros. Linux's `fallocate(PUNCH_HOLE)` does
///   zero it as a side effect, macOS's `F_PUNCHHOLE` only operates on whole
///   blocks so partial edges keep their old bytes. Callers needing zeros must
///   arrange it themselves, and both current callers do: `LSMTFile::discard_range`
///   records the range in its index with `SegmentMapping::zeroed`, and
///   `CacheEntry` eviction clears its bitmap before calling, so later reads miss
///   and refill from the source either way.
/// - That any particular amount is released. A range not covering a whole
///   filesystem block releases nothing and still returns `Ok`.
///
/// What it *does* keep is diagnostics: best-effort applies to coverage, not to
/// errors. A syscall that is attempted and fails — `EIO`, `EPERM`, `ENOSPC` —
/// is still reported, so a cache eviction that cannot reclaim space does not
/// silently look like it worked.
///
/// # Granularity
///
/// Linux accepts any range and frees the whole blocks inside it.
///
/// macOS requires both offset and length to be multiples of the filesystem block
/// size (`man 2 fcntl`: "Holes must be aligned to file system block
/// boundaries"), returning `EINVAL` otherwise, so the range is shrunk *inward*
/// to block boundaries — never outward, which would discard data the caller
/// never asked about.
///
/// Worth knowing for macOS: LSMT's own alignment is 512 bytes
/// (`lsmt::file::types::ALIGNMENT`) while an APFS block is 4096 and its
/// allocation granularity is 16 KiB. A 512-byte discard therefore releases
/// nothing at all, and only ranges of 4096 or more, aligned to 4096, release
/// anything. That is within contract, but it means fine-grained discards reclaim
/// no space on macOS.
///
/// # Errors
///
/// A range whose end does not fit in a `u64` is rejected with `EOVERFLOW` rather
/// than clamped. Clamping would turn a malformed request into a shorter
/// well-formed one and release blocks the caller never named, and the macOS arm
/// additionally rounds the offset upward, which on a near-`u64::MAX` offset
/// panics in debug and wraps to zero in release — releasing from the start of
/// the file. Both are worse than refusing the call.
pub fn release_space_hint(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
    if len == 0 {
        return Ok(());
    }
    if offset.checked_add(len).is_none() {
        return Err(SysError::Errno(Errno::EOVERFLOW));
    }
    imp::release_space_hint(fd, offset, len)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use super::super::{to_off_t, SysError, SysResult};

    pub(super) fn release_space_hint(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
        let offset = to_off_t(offset)?;
        let len = to_off_t(len)?;
        // SAFETY: `fd` is a live borrowed descriptor and `fallocate` only reads
        // its scalar arguments.
        let ret = unsafe {
            libc::fallocate(
                fd.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset,
                len,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use nix::errno::Errno;

    use super::super::{to_off_t, SysError, SysResult};

    /// Fallback when `fstat` reports a block size that cannot be used for the
    /// alignment arithmetic below.
    const DEFAULT_BLOCK_SIZE: u64 = 4096;

    /// Filesystem block size for `fd`, which `F_PUNCHHOLE` requires both the
    /// offset and the length to be a multiple of.
    fn block_size(fd: BorrowedFd<'_>) -> SysResult<u64> {
        let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `fd` is live and `fstat` fully initializes `st` when it
        // returns 0.
        let ret = unsafe { libc::fstat(fd.as_raw_fd(), st.as_mut_ptr()) };
        if ret != 0 {
            return Err(SysError::last());
        }
        // SAFETY: `fstat` returned 0, so `st` is initialized.
        let st = unsafe { st.assume_init() };
        let blk = u64::try_from(st.st_blksize).unwrap_or(0);
        // The alignment arithmetic below assumes a power of two.
        Ok(if blk.is_power_of_two() {
            blk
        } else {
            DEFAULT_BLOCK_SIZE
        })
    }

    pub(super) fn release_space_hint(fd: BorrowedFd<'_>, offset: u64, len: u64) -> SysResult<()> {
        let blk = block_size(fd)?;
        let end = offset
            .checked_add(len)
            .ok_or(SysError::Errno(Errno::EOVERFLOW))?;

        // Shrink inward: start rounds up, end rounds down. Rounding either the
        // other way would release blocks outside the requested range.
        //
        // Rounding up is checked separately from the range: an offset within
        // `blk - 1` of `u64::MAX` has no next block boundary even though
        // `offset + len` itself fits. Unchecked, that panics in debug and wraps
        // to 0 in release, which would then punch from the start of the file.
        let aligned_offset = offset
            .checked_next_multiple_of(blk)
            .ok_or(SysError::Errno(Errno::EOVERFLOW))?;
        let aligned_end = end / blk * blk;
        if aligned_end <= aligned_offset {
            // The request covers no whole block, so there is nothing to release.
            // This is a hint about space, so releasing none of it is success.
            return Ok(());
        }

        let arg = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: to_off_t(aligned_offset)?,
            fp_length: to_off_t(aligned_end - aligned_offset)?,
        };
        // SAFETY: `fd` is live and `F_PUNCHHOLE` reads a single `fpunchhole_t`
        // through the pointer, which stays valid for the call.
        let ret = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PUNCHHOLE, &arg) };
        if ret != -1 {
            return Ok(());
        }
        Err(SysError::last())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::os::fd::BorrowedFd;

    use super::super::SysResult;

    /// No way to release a range here. Since this is only ever a hint about
    /// space, reporting success while releasing nothing is the contract rather
    /// than a violation of it.
    pub(super) fn release_space_hint(
        _fd: BorrowedFd<'_>,
        _offset: u64,
        _len: u64,
    ) -> SysResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::os::fd::AsFd;
    use std::os::unix::fs::{FileExt, MetadataExt};

    /// A file of `len` bytes, fully written so every block is allocated.
    fn written_file(len: usize) -> (tempfile::TempDir, File) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("release.bin"))
            .expect("create file");
        file.write_all_at(&vec![0xAB_u8; len], 0).expect("fill");
        file.sync_all().expect("sync");
        (dir, file)
    }

    #[test]
    fn zero_length_is_a_noop() {
        let (_dir, file) = written_file(4096);
        release_space_hint(file.as_fd(), 0, 0).expect("zero length must succeed");
        assert_eq!(file.metadata().expect("stat").len(), 4096);
    }

    /// A range that runs past `u64::MAX` is refused instead of being clamped to
    /// something well-formed. Both offsets below also sit within a block of
    /// `u64::MAX`, where rounding the offset up to the next block boundary
    /// overflows on its own — unchecked that panics in debug and wraps to 0 in
    /// release, which would release from the start of the file.
    #[test]
    fn an_overflowing_range_is_refused() {
        let (_dir, file) = written_file(4096);
        for (offset, len) in [(u64::MAX, 1), (u64::MAX - 16, 4096)] {
            let err = release_space_hint(file.as_fd(), offset, len)
                .expect_err("a range past u64::MAX must be refused");
            assert!(
                !err.is_unsupported(),
                "expected an errno, got {err} for offset={offset} len={len}"
            );
        }
        assert_eq!(
            file.metadata().expect("stat").len(),
            4096,
            "a refused call must not touch the file"
        );
    }

    /// A range too small to cover a whole block releases nothing and still
    /// succeeds — that is the point of the "hint" in the name. Contents are
    /// deliberately not asserted: Linux zeroes such a range as a side effect of
    /// `fallocate` while macOS leaves it untouched, and this interface promises
    /// neither.
    #[test]
    fn sub_block_range_succeeds_without_changing_file_size() {
        let (_dir, file) = written_file(4096);
        release_space_hint(file.as_fd(), 13, 499).expect("sub-block range must succeed");
        assert_eq!(
            file.metadata().expect("stat").len(),
            4096,
            "logical size must be preserved"
        );
    }

    /// The whole point: an aligned range covering entire blocks actually returns
    /// storage, while the file keeps its logical size.
    #[test]
    fn aligned_whole_file_range_releases_storage() {
        let len = 64 * 1024;
        let (_dir, file) = written_file(len);
        let blocks_before = file.metadata().expect("stat").blocks();
        assert!(blocks_before > 0, "a fully written file must have blocks");

        release_space_hint(file.as_fd(), 0, len as u64).expect("aligned range must succeed");
        file.sync_all().expect("sync");

        let meta = file.metadata().expect("stat");
        assert_eq!(meta.len(), len as u64, "logical size must be preserved");
        assert!(
            meta.blocks() < blocks_before,
            "storage should be released: {} -> {} blocks",
            blocks_before,
            meta.blocks()
        );
    }

    /// Releasing an interior range leaves the surrounding data alone.
    #[test]
    fn aligned_interior_range_keeps_surrounding_data() {
        let len = 16 * 1024;
        let (_dir, file) = written_file(len);

        release_space_hint(file.as_fd(), 4096, 4096).expect("interior range must succeed");

        let mut head = [0u8; 16];
        file.read_exact_at(&mut head, 0).expect("read head");
        assert_eq!(head, [0xAB; 16], "data before the range must survive");

        let mut tail = [0u8; 16];
        file.read_exact_at(&mut tail, 8192).expect("read tail");
        assert_eq!(tail, [0xAB; 16], "data after the range must survive");
    }
}
