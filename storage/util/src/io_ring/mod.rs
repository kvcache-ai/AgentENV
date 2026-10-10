use io_uring::opcode;
use std::future::Future;
use std::os::fd::RawFd;
use std::pin::Pin;

mod uring;
mod worker;

use uring::AsyncIoRingSQE;
pub use uring::{AsyncIoRing, AsyncIoRingBuilder, RingFuture};
pub use worker::{spawn_io_ring_worker, IoRingHandle, IoRingWorker, RegisterableBuf, URING};

/// Abstracts over "submit an io_uring entry and get back a result"
/// This trait is dyn compatible.
pub trait IoUringSubmitterBox {
    /// Submit an io_uring SQE and return the raw result (i32)
    fn submit_box<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<i32>> + 'a>>;
}

/// Similar to [IoUringSubmitterBox], but return a Send Future.
/// This trait is dyn compatible.
pub trait IoUringSubmitterSendBox: Send + Sync {
    /// Submit an io_uring SQE and return the raw result (i32)
    fn submit_box<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<i32>> + Send + 'a>>;
}

impl<T: IoUringSubmitterSendBox> IoUringSubmitterBox for T {
    fn submit_box<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<i32>> + 'a>> {
        self.submit_box(entry)
    }
}

/// This trait is not dyn compatible, but it can keep the Send-ness of the Future
pub trait IoUringSubmitter {
    /// Submit an io_uring SQE and return the raw result (i32)
    fn submit<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> impl Future<Output = std::io::Result<i32>> + 'a;
}

impl<'b> IoUringSubmitter for dyn IoUringSubmitterBox + 'b {
    fn submit<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> impl Future<Output = std::io::Result<i32>> + 'a {
        self.submit_box(entry)
    }
}

// impl IoUringSubmitter for &(dyn IoUringSubmitterSendBox + '_) {
impl<'b> IoUringSubmitter for dyn IoUringSubmitterSendBox + 'b {
    fn submit<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> impl Future<Output = std::io::Result<i32>> + 'a {
        self.submit_box(entry)
    }
}

/// Try to fill the `buf` as much as possible, by reading from fixed file
/// - `file_io_ring_idx`: Indicates the file which has been registered onto io uring.
///
/// Return the number of bytes that read.
pub async fn read_exact_at_fixed<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    file_io_ring_idx: u32,
    mut buf: &mut [u8],
    mut offset: u64,
) -> std::io::Result<usize> {
    let mut bytes_read = 0;
    while !buf.is_empty() {
        let entry = opcode::Read::new(
            io_uring::types::Fixed(file_io_ring_idx),
            buf.as_mut_ptr(),
            buf.len() as _,
        )
        .offset(offset)
        .build();
        let res = submitter.submit(entry).await?;
        if res == 0 {
            break;
        }
        if res > 0 {
            buf = &mut buf[res as usize..];
            offset += res as u64;
            bytes_read += res as usize;
        } else if res == -libc::EINTR {
            continue;
        } else {
            return Err(std::io::Error::from_raw_os_error(-res));
        }
    }
    Ok(bytes_read)
}

/// Try to write the `buf` into the file as much as possible.
/// - `file_io_ring_idx`: Indicates the file which has been registered onto io uring.
///
/// Return the number of bytes that written.
pub async fn write_exact_at_fixed<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    file_io_ring_idx: u32,
    mut buf: &[u8],
    mut offset: u64,
) -> std::io::Result<usize> {
    let mut bytes_written = 0;
    while !buf.is_empty() {
        let entry = opcode::Write::new(
            io_uring::types::Fixed(file_io_ring_idx),
            buf.as_ptr(),
            buf.len() as _,
        )
        .offset(offset)
        .build();
        let res = submitter.submit(entry).await?;
        if res == 0 {
            break;
        }
        if res > 0 {
            buf = &buf[res as usize..];
            offset += res as u64;
            bytes_written += res as usize;
        } else if res == -libc::EINTR {
            continue;
        } else {
            return Err(std::io::Error::from_raw_os_error(-res));
        }
    }
    Ok(bytes_written)
}

/// Try to fill the `buf` as much as possible from file descriptor.
/// Similar to [read_exact_at_fixed], expect the `fd` is not
/// an index to sparse file table, but a file descriptor.
///
/// Return the number of bytes that read.
pub async fn read_exact_at<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    fd: RawFd,
    mut buf: &mut [u8],
    mut offset: u64,
) -> std::io::Result<usize> {
    let mut bytes_read = 0;
    while !buf.is_empty() {
        let entry = opcode::Read::new(io_uring::types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
            .offset(offset)
            .build();
        let res = submitter.submit(entry).await?;
        if res == 0 {
            break;
        }
        if res > 0 {
            buf = &mut buf[res as usize..];
            offset += res as u64;
            bytes_read += res as usize;
        } else if res == -libc::EINTR {
            continue;
        } else {
            return Err(std::io::Error::from_raw_os_error(-res));
        }
    }
    Ok(bytes_read)
}

pub async fn read_exact_at_aligned<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    fd: RawFd,
    mut buf: &mut [u8],
    mut offset: u64,
    alignment: usize,
) -> std::io::Result<usize> {
    let mut total = 0;
    while !buf.is_empty() {
        let entry = opcode::Read::new(io_uring::types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
            .offset(offset)
            .build();
        let res = submitter.submit(entry).await?;
        if res <= 0 {
            if res == 0 {
                break;
            }
            if res == -libc::EINTR {
                continue;
            }
            return Err(std::io::Error::from_raw_os_error(-res));
        }
        let n = res as usize;
        total += n;
        if n < buf.len() && !n.is_multiple_of(alignment) {
            break;
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(total)
}

/// Try to write the `buf` into the file as much as possible.
/// Similar to [write_exact_at_fixed], expect the `fd` is not
/// an index to sparse file table, but a file descriptor.
///
/// Return the number of bytes that written.
pub async fn write_exact_at<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    fd: std::os::fd::RawFd,
    mut buf: &[u8],
    mut offset: u64,
) -> std::io::Result<usize> {
    let mut bytes_written = 0;
    while !buf.is_empty() {
        let entry = opcode::Write::new(io_uring::types::Fd(fd), buf.as_ptr(), buf.len() as _)
            .offset(offset)
            .build();
        let res = submitter.submit(entry).await?;
        if res == 0 {
            break;
        }
        if res > 0 {
            buf = &buf[res as usize..];
            offset += res as u64;
            bytes_written += res as usize;
        } else if res == -libc::EINTR {
            continue;
        } else {
            return Err(std::io::Error::from_raw_os_error(-res));
        }
    }
    Ok(bytes_written)
}

pub async fn write_exact_at_aligned<S: IoUringSubmitter + ?Sized>(
    submitter: &S,
    fd: RawFd,
    mut buf: &[u8],
    mut offset: u64,
    alignment: usize,
) -> std::io::Result<usize> {
    let mut total = 0;
    while !buf.is_empty() {
        let entry = opcode::Write::new(io_uring::types::Fd(fd), buf.as_ptr(), buf.len() as _)
            .offset(offset)
            .build();
        let res = submitter.submit(entry).await?;
        if res <= 0 {
            if res == 0 {
                break;
            }
            if res == -libc::EINTR {
                continue;
            }
            return Err(std::io::Error::from_raw_os_error(-res));
        }
        let n = res as usize;
        total += n;
        if n < buf.len() && !n.is_multiple_of(alignment) {
            break;
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(total)
}
