use std::io::IoSliceMut;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::sys::uio::{process_vm_readv, RemoteIoVec};
use nix::unistd::Pid;
use overlaybd::virtual_file::VirtualFile;

/// Reads memory from a remote process using `process_vm_readv`.
pub(super) struct ProcessVmReader {
    pid: Pid,
}

impl ProcessVmReader {
    pub(super) fn new(pid: Pid) -> Self {
        Self { pid }
    }

    pub(super) fn read_exact_remote(&self, remote_addr: u64, dst: &mut [u8]) -> Result<()> {
        let base = usize::try_from(remote_addr)
            .with_context(|| format!("remote address {remote_addr:#x} does not fit usize"))?;
        self.read_exact_remote_vectored(
            &mut [RemoteIoVec {
                base,
                len: dst.len(),
            }],
            dst,
        )
    }

    pub(super) fn read_exact_remote_vectored(
        &self,
        mut remote_iovs: &mut [RemoteIoVec],
        mut remaining: &mut [u8],
    ) -> Result<()> {
        // Linux limits a process_vm_readv call to 1024 remote iovecs.
        const MAX_IOVECS: usize = 1024;
        while !remaining.is_empty() {
            let count = remote_iovs.len().min(MAX_IOVECS);
            let read = match process_vm_readv(
                self.pid,
                &mut [IoSliceMut::new(remaining)],
                &remote_iovs[..count],
            ) {
                Ok(read) => read,
                Err(nix::errno::Errno::EINTR) => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("process_vm_readv pid {}", self.pid.as_raw()));
                }
            };
            if read == 0 {
                bail!(
                    "process_vm_readv for pid {} returned 0 bytes",
                    self.pid.as_raw()
                );
            }
            remaining = &mut remaining[read..];
            let mut consumed = read;
            while consumed > 0 {
                let first = &mut remote_iovs[0];
                if consumed < first.len {
                    first.base += consumed;
                    first.len -= consumed;
                    break;
                }
                consumed -= first.len;
                remote_iovs = &mut remote_iovs[1..];
            }
        }
        Ok(())
    }
}

#[async_trait]
impl VirtualFile for ProcessVmReader {
    /// `offset` is a host virtual address (HVA), not a file offset.
    /// This adapter reads synchronously; owned-buffer callers can offload
    /// `read_exact_remote` to a blocking worker.
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut data = vec![0u8; len];
        self.read_exact_remote(offset, &mut data)?;
        Ok(Bytes::from(data))
    }

    /// `offset` is a host virtual address (HVA), not a file offset.
    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.read_exact_remote(offset, dst)?;
        Ok(dst.len())
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("ProcessVmReader does not support write_at")
    }

    async fn size(&self) -> Result<u64> {
        bail!("ProcessVmReader does not have a meaningful size")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_current_process_memory() -> Result<()> {
        let source = b"process-vm-reader";
        let reader = ProcessVmReader::new(Pid::this());

        let bytes = reader.read_at(source.as_ptr() as u64, source.len()).await?;
        assert_eq!(&bytes[..], source);

        let mut dst = vec![0u8; source.len()];
        let read = reader
            .read_at_into(source.as_ptr() as u64, &mut dst)
            .await?;
        assert_eq!(read, source.len());
        assert_eq!(dst, source);

        Ok(())
    }

    #[test]
    fn vectored_reads_preserve_order_across_the_kernel_iovec_limit() -> Result<()> {
        let source = (0..2050)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut ranges = source
            .iter()
            .rev()
            .map(|byte| RemoteIoVec {
                base: byte as *const u8 as usize,
                len: 1,
            })
            .collect::<Vec<_>>();
        let mut output = vec![0; source.len()];
        ProcessVmReader::new(Pid::this()).read_exact_remote_vectored(&mut ranges, &mut output)?;
        assert_eq!(output, source.into_iter().rev().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn vectored_reads_report_an_unreadable_range_after_a_partial_read() {
        let source = b"readable";
        let mut ranges = [
            RemoteIoVec {
                base: source.as_ptr() as usize,
                len: source.len(),
            },
            RemoteIoVec { base: 0, len: 4096 },
        ];
        let mut output = vec![0; source.len() + 4096];
        assert!(ProcessVmReader::new(Pid::this())
            .read_exact_remote_vectored(&mut ranges, &mut output)
            .is_err());
        assert_eq!(&output[..source.len()], source);
    }
}
