use anyhow::{anyhow, bail, Context, Result};
use io_uring::{opcode, types};
use std::alloc::{alloc, dealloc, Layout};
use std::cell::OnceCell;
use std::ops::Range;
use std::rc::Rc;

use storage_util::io_ring::AsyncIoRing;

pub struct AutoRegBuffer {
    inner: Rc<AutoRegBufferInner>,
}

struct AutoRegBufferInner {
    data: ublk_sys::ublk_auto_buf_reg,
    pub ring: AsyncIoRing,
    /// The size of this buffer, it will be filled according to
    /// [ublk_sys::ublksrv_io_desc] after ublk server get IO request.
    size: usize,
}

impl Drop for AutoRegBufferInner {
    fn drop(&mut self) {
        let _ = self.ring.release_sparse_buffer_index(self.data.index as _);
    }
}

impl AutoRegBuffer {
    pub fn new(ring: AsyncIoRing, size: usize) -> Result<Self> {
        let data = ublk_sys::ublk_auto_buf_reg {
            index: ring
                .occupy_sparse_buffer_index()
                .context("get sparse buf index")? as u16,
            flags: 0,
            ..Default::default()
        };
        Ok(Self {
            inner: Rc::new(AutoRegBufferInner { data, ring, size }),
        })
    }

    pub fn as_ublk_auto_buf_reg(&self) -> &ublk_sys::ublk_auto_buf_reg {
        &self.inner.data
    }

    /// Read the data from io_ring_file_idx into buffer.
    /// - `offset`: file offset
    /// - `len`: number of bytes to read.
    /// - `buf_offset`: offset of the buffer.
    async unsafe fn read_all_from_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
        len: usize,
        buf_offset: u64,
    ) -> std::io::Result<usize> {
        let mut done = 0;
        while done < len {
            let entry = opcode::ReadFixed::new(
                types::Fixed(io_ring_file_idx),
                (buf_offset + done as u64) as _,
                (len - done) as _,
                self.inner.data.index,
            )
            .offset(offset + done as u64)
            .build();
            let res = self.inner.ring.send(entry)?.await;
            if res == 0 {
                break;
            } else if res < 0 {
                if res == -libc::EINTR {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            done += res as usize;
        }
        Ok(done)
    }

    /// Write the data from buffer into io_ring_file_idx.
    /// - `offset`: file offset
    /// - `len`: number of bytes to write.
    /// - `buf_offset`: offset of the buffer.
    async unsafe fn write_all_into_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
        len: usize,
        buf_offset: u64,
    ) -> std::io::Result<usize> {
        let mut done = 0;
        while done < len {
            let entry = opcode::WriteFixed::new(
                types::Fixed(io_ring_file_idx),
                (buf_offset + done as u64) as _,
                (len - done) as _,
                self.inner.data.index,
            )
            .offset(offset + done as u64)
            .build();
            let res = self.inner.ring.send(entry)?.await;
            if res == 0 {
                break;
            } else if res < 0 {
                if res == -libc::EINTR {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            done += res as usize;
        }
        Ok(done)
    }
}

pub struct UserBufferInner {
    ptr: *mut u8,
    layout: Layout,
    /// The index within io uring
    buf_idx: OnceCell<u32>,
    pub ring: AsyncIoRing,
}

impl UserBufferInner {
    pub fn new(ring: AsyncIoRing, size: usize, align: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, align).context("invalid layout")?;
        if layout.size() == 0 {
            bail!("UserBufferInner size cannot be zero");
        }
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            bail!("allocate UserBufferInner failed, size = {size} align = {align}");
        }
        let mut this = Self {
            ptr,
            layout,
            buf_idx: OnceCell::new(),
            ring: ring.clone(),
        };
        // register the buffer to io ring
        let idx = ring.register_buffer(this.as_mut())?;
        this.buf_idx
            .set(idx)
            .map_err(|_| anyhow!("cannot set buf_idx of UserBufferInner"))?;
        Ok(this)
    }

    /// Return a **mutable** sub slice started from `offset`, and len = `len`.
    ///
    /// # Panic
    /// Panic if try to access a sub slice out of range.
    ///
    /// # Safety
    /// Users should make sure there at most one that access the sub slice in the requested range.
    #[allow(clippy::mut_from_ref)]
    pub fn subslice_mut(&self, offset: u64, len: usize) -> &mut [u8] {
        let offset = offset as usize;
        assert!(
            offset + len <= self.len(),
            "offset {offset} len = {len} out of range"
        );
        unsafe {
            let ptr = self.ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, len)
        }
    }

    /// Return a sub slice started from `offset`, span `len`.
    ///
    /// # Panic
    /// Panic if try to access a sub slice out of range.
    ///
    /// # Safety
    /// Users should make sure there are no others that mutablely access
    /// the requested range via [subslice_mut](Self::subslice_mut).
    pub fn subslice(&self, offset: u64, len: usize) -> &[u8] {
        let offset = offset as usize;
        assert!(
            offset + len <= self.len(),
            "offset {offset} len = {len} out of range"
        );
        unsafe {
            let ptr = self.ptr.add(offset);
            std::slice::from_raw_parts(ptr.cast_const(), len)
        }
    }

    pub fn len(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for UserBufferInner {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            unsafe {
                dealloc(self.ptr, self.layout);
            }
        }
        if let Some(buf_idx) = self.buf_idx.get().copied() {
            let _ = self.ring.unregister_buffer(buf_idx);
        }
    }
}

impl AsMut<[u8]> for UserBufferInner {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

/// A buffer that has been registered into io uring,
/// and setup its alignment, suitable for IO request.
pub struct UserBuffer {
    inner: Rc<UserBufferInner>,
}

impl UserBuffer {
    pub fn new(ring: AsyncIoRing, size: usize, align: usize) -> Result<Self> {
        let inner = UserBufferInner::new(ring, size, align)?;
        Ok(Self {
            inner: Rc::new(inner),
        })
    }

    /// Return a **mutable** sub slice started from `offset`, and len = `len`.
    ///
    /// # Panic
    /// Panic if try to access a sub slice out of range.
    ///
    /// # Safety
    /// Users should make sure there at most one that access the sub slice in the requested range.
    #[allow(clippy::mut_from_ref)]
    pub fn subslice_mut(&self, offset: u64, len: usize) -> &mut [u8] {
        self.inner.subslice_mut(offset, len)
    }

    /// Return a sub slice started from `offset`, span `len`.
    ///
    /// # Panic
    /// Panic if try to access a sub slice out of range.
    ///
    /// # Safety
    /// Users should make sure there are no others that mutablely access
    /// the requested range via [subslice_mut](Self::subslice_mut).
    pub fn subslice(&self, offset: u64, len: usize) -> &[u8] {
        self.inner.subslice(offset, len)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // SAFETY: after the UserBuffer has successfully created, it must
    // register the buffer into uring.
    pub fn uring_buf_idx(&self) -> std::io::Result<u16> {
        let idx =
            self.inner.buf_idx.get().copied().ok_or_else(|| {
                std::io::Error::other("io buffer is not registered with io_uring")
            })?;
        Ok(idx as u16)
    }

    /// Read the data from io_ring_file_idx into buffer.
    /// - `offset`: file offset
    /// - `len`: number of bytes to read.
    /// - `buf_offset`: offset of the buffer.
    async unsafe fn read_all_from_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
        len: usize,
        buf_offset: u64,
    ) -> std::io::Result<usize> {
        let buf_idx = self.uring_buf_idx()?;
        let mut done = 0;
        while done < len {
            let entry = opcode::ReadFixed::new(
                types::Fixed(io_ring_file_idx),
                self.inner.ptr.add(buf_offset as usize + done),
                (len - done) as _,
                buf_idx,
            )
            .offset(offset + done as u64)
            .build();
            let res = self.inner.ring.send(entry)?.await;
            if res == 0 {
                break;
            } else if res < 0 {
                if res == -libc::EINTR {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            done += res as usize;
        }
        Ok(done)
    }

    /// Write the data from buffer into io_ring_file_idx.
    /// - `offset`: file offset
    /// - `len`: number of bytes to write.
    /// - `buf_offset`: offset of the buffer.
    async unsafe fn write_all_into_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
        len: usize,
        buf_offset: u64,
    ) -> std::io::Result<usize> {
        let buf_idx = self.uring_buf_idx()?;
        let mut done = 0;
        while done < len {
            let entry = opcode::WriteFixed::new(
                types::Fixed(io_ring_file_idx),
                self.inner.ptr.add(buf_offset as usize + done),
                (len - done) as _,
                buf_idx,
            )
            .offset(offset + done as u64)
            .build();
            let res = self.inner.ring.send(entry)?.await;
            if res == 0 {
                break;
            } else if res < 0 {
                if res == -libc::EINTR {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            done += res as usize;
        }
        Ok(done)
    }

    /// Copy from `src` into this user buffer at `offset`.
    ///
    /// Return error if it will copy out of range.
    pub fn copy_from(&self, src: &[u8], offset: u64) -> Result<()> {
        if offset as usize + src.len() > self.len() {
            bail!(
                "try copy exceed the user buffer, offset = {offset}, len = {}",
                src.len()
            );
        }
        self.subslice_mut(offset, src.len()).copy_from_slice(src);
        Ok(())
    }
}

pub enum IOBuffer {
    /// The UBLK_F_AUTO_BUF_REG feature of ublk
    AutoReg(AutoRegBuffer),
    /// User buffer means allocated at user space, and registered into io uring
    User(UserBuffer),
}

/// A subslice of the IOBuffer
pub struct IOBufferView {
    pub buffer: IOBuffer,
    pub range: Range<usize>,
}

impl IOBuffer {
    pub fn uring_buf_idx(&self) -> std::io::Result<u16> {
        match self {
            IOBuffer::AutoReg(auto_buf) => Ok(auto_buf.inner.data.index),
            IOBuffer::User(user_buf) => user_buf.uring_buf_idx(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            IOBuffer::AutoReg(auto_buf) => auto_buf.inner.size,
            IOBuffer::User(user_buf) => user_buf.len(),
        }
    }

    fn dup(&mut self) -> Self {
        match self {
            IOBuffer::AutoReg(auto_buf) => IOBuffer::AutoReg(AutoRegBuffer {
                inner: auto_buf.inner.clone(),
            }),
            IOBuffer::User(user_buf) => IOBuffer::User(UserBuffer {
                inner: user_buf.inner.clone(),
            }),
        }
    }

    pub fn ring(&self) -> &AsyncIoRing {
        match self {
            Self::AutoReg(auto_buf) => &auto_buf.inner.ring,
            Self::User(user_buf) => &user_buf.inner.ring,
        }
    }

    /// Returns two views of the io buffer, the first is [0, mid), the second
    /// is [mid, len).
    ///
    /// Returns `None` if mid > buffer length.
    pub fn split_at(&mut self, mid: usize) -> Option<(IOBufferView, IOBufferView)> {
        if mid > self.len() {
            return None;
        }
        let first = IOBufferView {
            buffer: self.dup(),
            range: (0..mid),
        };
        let second = IOBufferView {
            buffer: self.dup(),
            range: (mid..self.len()),
        };
        Some((first, second))
    }
}

impl IOBufferView {
    pub fn io_ring(&self) -> &AsyncIoRing {
        self.buffer.ring()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Size of this view of buffer.
    pub fn len(&self) -> usize {
        self.range.end.saturating_sub(self.range.start)
    }

    /// Returns two sub views of this view, the first is [0, offset), the second
    /// is [offset, len).
    ///
    /// Returns `None` if offset >= view length.
    pub fn split_at(self, mid: usize) -> Option<(IOBufferView, IOBufferView)> {
        if mid > self.len() {
            return None;
        }
        let IOBufferView { mut buffer, range } = self;
        let first = IOBufferView {
            buffer: buffer.dup(),
            range: range.start..range.start + mid,
        };
        let second = IOBufferView {
            buffer,
            range: range.start + mid..range.end,
        };
        Some((first, second))
    }

    /// Write the data from file into this io buffer view, filling the entire io buffer.
    /// - `io_ring_file_idx`: The index of the file registerred to io uring.
    /// - `offset`: The file offset to read, in bytes.
    pub async fn read_all_from_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
    ) -> std::io::Result<usize> {
        match &self.buffer {
            IOBuffer::AutoReg(auto_buf) => unsafe {
                auto_buf
                    .read_all_from_fixed(
                        io_ring_file_idx,
                        offset,
                        self.len(),
                        self.range.start as u64,
                    )
                    .await
            },
            IOBuffer::User(user_buf) => unsafe {
                user_buf
                    .read_all_from_fixed(
                        io_ring_file_idx,
                        offset,
                        self.len(),
                        self.range.start as u64,
                    )
                    .await
            },
        }
    }

    /// Write the all data in this io buffer view into file.
    /// - `io_ring_file_idx`: The index of the file registerred to io uring.
    /// - `offset`: The file offset to write, in bytes.
    pub async fn write_all_into_fixed(
        &self,
        io_ring_file_idx: u32,
        offset: u64,
    ) -> std::io::Result<usize> {
        match &self.buffer {
            IOBuffer::AutoReg(auto_buf) => unsafe {
                auto_buf
                    .write_all_into_fixed(
                        io_ring_file_idx,
                        offset,
                        self.len(),
                        self.range.start as u64,
                    )
                    .await
            },
            IOBuffer::User(user_buf) => unsafe {
                user_buf
                    .write_all_into_fixed(
                        io_ring_file_idx,
                        offset,
                        self.len(),
                        self.range.start as u64,
                    )
                    .await
            },
        }
    }
}
