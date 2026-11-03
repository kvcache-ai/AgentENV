use std::io::IoSliceMut;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::sys::uio::{process_vm_readv, RemoteIoVec};
use nix::unistd::Pid;
use overlaybd::virtual_file::VirtualFile;

/// Reads memory from a remote process using `process_vm_readv`.
pub(crate) struct ProcessVmReader {
    pid: Pid,
}

impl ProcessVmReader {
    pub(crate) fn new(pid: Pid) -> Self {
        Self { pid }
    }

    // TODO: `process_vm_readv` is synchronous and currently runs on the Tokio
    // worker polling `VirtualFile::read_at_into`. Offloading it without an
    // extra memory copy requires an owned-buffer interface or a dedicated
    // blocking worker that owns the destination buffer for the request.
    fn read_exact_remote(&self, remote_addr: u64, dst: &mut [u8]) -> Result<()> {
        if dst.is_empty() {
            return Ok(());
        }

        let mut remote_addr = remote_addr;
        let mut remaining = dst;

        while !remaining.is_empty() {
            let remote_base = usize::try_from(remote_addr).with_context(|| {
                format!(
                    "remote address {remote_addr:#x} for pid {} does not fit usize",
                    self.pid.as_raw()
                )
            })?;
            let remote_iovs = [RemoteIoVec {
                base: remote_base,
                len: remaining.len(),
            }];
            let read = {
                let mut local_iovs = [IoSliceMut::new(remaining)];
                match process_vm_readv(self.pid, &mut local_iovs, &remote_iovs) {
                    Ok(read) => read,
                    Err(nix::errno::Errno::EINTR) => continue,
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "process_vm_readv pid {} remote address {remote_addr:#x}",
                                self.pid.as_raw()
                            )
                        });
                    }
                }
            };

            if read == 0 {
                bail!(
                    "process_vm_readv for pid {} returned 0 bytes at {remote_addr:#x}",
                    self.pid.as_raw()
                );
            }
            if read > remaining.len() {
                bail!(
                    "process_vm_readv for pid {} returned {} bytes for {} byte buffer at {remote_addr:#x}",
                    self.pid.as_raw(),
                    read,
                    remaining.len()
                );
            }

            remote_addr = remote_addr.checked_add(read as u64).with_context(|| {
                format!(
                    "process_vm_readv remote address overflow for pid {}",
                    self.pid.as_raw()
                )
            })?;
            remaining = &mut remaining[read..];
        }
        Ok(())
    }
}

#[async_trait]
impl VirtualFile for ProcessVmReader {
    /// `offset` is a host virtual address (HVA), not a file offset.
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
}
