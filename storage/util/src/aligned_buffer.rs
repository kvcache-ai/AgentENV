use std::alloc::{alloc, dealloc, Layout};
use std::io::{self, Error, ErrorKind};
use std::ops::Range;
use std::ptr::NonNull;

/// A heap-allocated byte buffer whose starting address is aligned to a given power-of-two
/// boundary.
///
/// This is required by Linux O_DIRECT I/O, which demands that the memory address, file offset,
/// and transfer length are all multiples of the logical block size (typically 512 bytes).
///
/// # Safety
/// The backing allocation is managed manually. `AlignedBuffer` is `Send` but not `Sync`;
/// callers must not share a reference across threads without external synchronisation.
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
    /// Optionally narrow the visible slice to a sub-range of the allocation.
    /// `AsRef` / `AsMut` honour this range.
    range: Range<usize>,
}

impl AlignedBuffer {
    /// Allocate `len` bytes with the given `align` (must be a power of two).
    pub fn new(len: usize, align: usize) -> io::Result<Self> {
        let layout = Layout::from_size_align(len, align).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid Layout for AlignedBuffer: {err:?}"),
            )
        })?;
        if layout.size() == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "zero sized AlignedBuffer",
            ));
        }
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr)
            .ok_or_else(|| Error::other("AlignedBuffer alloc returned null pointer"))?;
        Ok(Self {
            ptr,
            layout,
            range: 0..len,
        })
    }

    /// Length of the currently visible slice (respects any active sub-range).
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    /// Returns `true` if the visible slice is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.range.start >= self.range.end
    }

    /// Narrow the visible slice to `range` (analogous to `&buf[start..end]`).
    pub fn into_sub_range(mut self, range: Range<usize>) -> io::Result<Self> {
        if range.start >= range.end || range.end > self.layout.size() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "invalid subrange {range:?} for AlignedBuffer (size = {})",
                    self.layout.size()
                ),
            ));
        }
        self.range = range;
        Ok(self)
    }
}

impl AsRef<[u8]> for AlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(self.range.start), self.len()) }
    }
}

impl AsMut<[u8]> for AlignedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(self.range.start), self.len())
        }
    }
}

impl std::fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len())
            .field("align", &self.layout.align())
            .finish_non_exhaustive()
    }
}

// SAFETY: the buffer is uniquely owned; no shared references exist across threads.
unsafe impl Send for AlignedBuffer {}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            unsafe {
                dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_rw() {
        let mut buf = AlignedBuffer::new(512, 512).expect("alloc 512-byte aligned buffer");
        assert_eq!(buf.len(), 512);
        assert!(!buf.is_empty());

        // Write a pattern and read it back.
        buf.as_mut().fill(0xAB);
        assert!(buf.as_ref().iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_ptr_is_aligned() {
        for &align in &[512usize, 4096usize] {
            let buf = AlignedBuffer::new(align, align).expect("alloc aligned buffer");
            let ptr = buf.as_ref().as_ptr() as usize;
            assert_eq!(
                ptr % align,
                0,
                "buffer pointer {ptr:#x} is not aligned to {align}"
            );
        }
    }

    #[test]
    fn test_into_sub_range() {
        let mut buf = AlignedBuffer::new(1024, 512).expect("alloc 1024-byte buffer");
        // Fill with two distinct halves.
        buf.as_mut()[..512].fill(0x11);
        buf.as_mut()[512..].fill(0x22);

        // Sub-range spanning parts of both halves.
        let sub = buf.into_sub_range(256..768).expect("valid sub-range");
        assert_eq!(sub.len(), 512);
        assert!(sub.as_ref()[..256].iter().all(|&b| b == 0x11));
        assert!(sub.as_ref()[256..].iter().all(|&b| b == 0x22));
    }

    #[test]
    fn test_into_sub_range_invalid() {
        let buf = AlignedBuffer::new(512, 512).expect("alloc buffer");
        // start >= end
        assert!(buf.into_sub_range(256..256).is_err());

        let buf = AlignedBuffer::new(512, 512).expect("alloc buffer");
        // end exceeds allocation size
        assert!(buf.into_sub_range(0..513).is_err());
    }

    #[test]
    fn test_zero_size_rejected() {
        let err = AlignedBuffer::new(0, 512).expect_err("zero-size should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
