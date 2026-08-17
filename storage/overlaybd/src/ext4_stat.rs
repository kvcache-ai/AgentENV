//! Best-effort ext4 usage probe over a read-only view of a block device.
//!
//! Reads only the superblock and reports used bytes, computed as
//! `(blocks_count - free_blocks_count) * block_size` — the same quantity `df`
//! prints as `Used`. This is a best-effort observability probe: no journal
//! replay, no checksum verification, and the on-disk counters may lag a
//! mounted filesystem by a checkpoint. Anything that is not a plausible ext4
//! view yields an error so callers can simply skip the metric.

use std::sync::Arc;

use anyhow::{ensure, Context, Result};

use crate::io::virtual_file::VirtualFile;

const SUPERBLOCK_OFFSET: u64 = 1024;
const SUPERBLOCK_LEN: usize = 1024;
const EXT4_MAGIC: u16 = 0xEF53;
const FEATURE_INCOMPAT_64BIT: u32 = 0x0080;

fn le16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().expect("le16"))
}

fn le32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("le32"))
}

/// Read the ext4 superblock from `file` and return the filesystem's used
/// bytes (`df`'s "Used" column). Errors when the view is not a plausible ext4.
pub async fn ext4_used_bytes(file: &Arc<dyn VirtualFile>) -> Result<u64> {
    let sb = file
        .read_at(SUPERBLOCK_OFFSET, SUPERBLOCK_LEN)
        .await
        .context("read ext4 superblock")?;
    ensure!(sb.len() == SUPERBLOCK_LEN, "short read on ext4 superblock");
    ensure!(
        le16(&sb, 0x38) == EXT4_MAGIC,
        "not an ext4 filesystem (bad superblock magic)"
    );
    let block_size = 1024u64
        .checked_shl(le32(&sb, 0x18))
        .context("ext4 block size shift overflow")?;
    let mut blocks_count = u64::from(le32(&sb, 0x04));
    let mut free_blocks = u64::from(le32(&sb, 0x0C));
    if le32(&sb, 0x60) & FEATURE_INCOMPAT_64BIT != 0 {
        blocks_count |= u64::from(le32(&sb, 0x150)) << 32;
        free_blocks |= u64::from(le32(&sb, 0x154)) << 32;
    }
    ensure!(
        blocks_count > 0 && free_blocks <= blocks_count,
        "implausible ext4 counters: blocks={blocks_count} free={free_blocks}"
    );
    (blocks_count - free_blocks)
        .checked_mul(block_size)
        .context("ext4 used size overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalFile;
    use crate::test_utils::test_io_ring;

    fn put16(buf: &mut [u8], offset: usize, value: u16) {
        buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put32(buf: &mut [u8], offset: usize, value: u32) {
        buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// A 2 KiB image whose second half is a synthetic ext4 superblock.
    fn fake_image(block_shift: u32, blocks: u64, free: u64, use_64bit: bool) -> Vec<u8> {
        let mut img = vec![0u8; 2048];
        let sb = &mut img[1024..2048];
        put16(sb, 0x38, EXT4_MAGIC);
        put32(sb, 0x18, block_shift);
        put32(sb, 0x04, blocks as u32);
        put32(sb, 0x0C, free as u32);
        if use_64bit {
            put32(sb, 0x60, FEATURE_INCOMPAT_64BIT);
            put32(sb, 0x150, (blocks >> 32) as u32);
            put32(sb, 0x154, (free >> 32) as u32);
        }
        img
    }

    async fn open_image(dir: &tempfile::TempDir, bytes: &[u8]) -> Arc<dyn VirtualFile> {
        let path = dir.path().join("dev.img");
        std::fs::write(&path, bytes).unwrap();
        Arc::new(LocalFile::open_ro(&path, test_io_ring()).await.unwrap())
    }

    #[tokio::test]
    async fn reads_used_bytes_from_superblock() {
        let dir = tempfile::tempdir().unwrap();
        // 4 KiB blocks, 1000 blocks total, 250 free → used = 750 * 4096.
        let file = open_image(&dir, &fake_image(2, 1000, 250, false)).await;
        assert_eq!(ext4_used_bytes(&file).await.unwrap(), 750 * 4096);
    }

    #[tokio::test]
    async fn reads_64bit_counters() {
        let dir = tempfile::tempdir().unwrap();
        let blocks = 5u64 << 30;
        let free = 1u64 << 30;
        let file = open_image(&dir, &fake_image(2, blocks, free, true)).await;
        assert_eq!(
            ext4_used_bytes(&file).await.unwrap(),
            (blocks - free) * 4096
        );
    }

    #[tokio::test]
    async fn rejects_non_ext4_view() {
        let dir = tempfile::tempdir().unwrap();
        let file = open_image(&dir, &vec![0u8; 2048]).await;
        assert!(ext4_used_bytes(&file).await.is_err());
    }
}
