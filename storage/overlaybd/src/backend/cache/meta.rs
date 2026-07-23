use anyhow::anyhow;
use anyhow::{ensure, Context, Result};
use nix::errno::Errno;
use roaring::RoaringBitmap;
use std::io::Write;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};
use std::task::Waker;
use std::time::{SystemTime, UNIX_EPOCH};
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use super::{DATA_FILE_NAME, META_FILE_NAME, META_MAGIC, META_VERSION};

// ---------------------------------------------------------------------------
// EntryPaths
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct EntryPaths {
    pub(crate) dir: PathBuf,
    pub(crate) data_path: PathBuf,
    pub(crate) meta_path: PathBuf,
}

impl EntryPaths {
    pub(crate) fn new(root: &Path, cache_id: &str) -> Self {
        let dir = root.join(cache_id);
        Self {
            data_path: dir.join(DATA_FILE_NAME),
            meta_path: dir.join(META_FILE_NAME),
            dir,
        }
    }
}

// ---------------------------------------------------------------------------
// BlockLoadState — per-block inflight deduplication (mirrors local_cache.rs)
// ---------------------------------------------------------------------------

pub(crate) struct BlockLoadState {
    pub(crate) wakers: Vec<Waker>,
}

#[derive(Clone, FromBytes, IntoBytes, Immutable)]
#[repr(C, packed)]
pub(crate) struct CacheMetaDiskHeader {
    pub(crate) magic: U32,
    pub(crate) version: U32,
    pub(crate) source_size: U64,
    pub(crate) block_size: U64,
    pub(crate) last_access_unix_nanos: U64,
    pub(crate) hits: U64,
    pub(crate) misses: U64,
    pub(crate) refills: U64,
}

impl Default for CacheMetaDiskHeader {
    fn default() -> Self {
        Self {
            magic: U32::new(META_MAGIC),
            version: U32::new(META_VERSION),
            source_size: Default::default(),
            block_size: Default::default(),
            last_access_unix_nanos: Default::default(),
            hits: Default::default(),
            misses: Default::default(),
            refills: Default::default(),
        }
    }
}

/// Compact binary metadata persisted to meta.bin
/// On-disk format (all little-endian):
///
/// ```text
/// [u32 magic][u32 version]
/// [u64 source_size][u64 block_size]
/// [u64 last_access_unix_nanos]
/// [u64 hits][u64 misses][u64 refills]
/// [u32 key_len][u8 × key_len]
/// [u8 array: serialized bitmap]  (RoaringBitmap serialized)
/// [u32 crc32]
/// ```
pub(crate) struct CacheMetaDisk {
    pub(crate) header: CacheMetaDiskHeader,
    /// Cache key, an identifier of this cache
    pub(crate) key: String,
    pub(crate) bitmap: RoaringBitmap,
}

impl CacheMetaDisk {
    /// Serialize to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>> {
        let key_bytes = self.key.as_bytes();
        // roughly guess the size
        let size = 64 + 4 + self.bitmap.len() as usize * 4 + key_bytes.len() + 4;
        let mut out = Vec::with_capacity(size);
        out.write_all(self.header.as_bytes())?;
        // write key
        let key_len = u32::try_from(key_bytes.len()).context("key too long")?;
        out.write_all(&key_len.to_le_bytes())?;
        out.write_all(key_bytes)?;
        // write bitmap
        self.bitmap
            .serialize_into(&mut out)
            .context("failed to serialize bitmap")?;

        let checksum = crc32c::crc32c(&out);
        out.write_all(&checksum.to_le_bytes())?;

        Ok(out)
    }

    /// Parse from bytes.
    fn from_bytes(raw: &[u8]) -> Result<Self> {
        ensure!(
            raw.len() >= std::mem::size_of::<CacheMetaDiskHeader>() + 4 + 4,
            "meta.bin too short for header"
        );

        // SAFETY: we have already check the size of raw
        let (header_bytes, data) = raw.split_at(std::mem::size_of::<CacheMetaDiskHeader>());
        let header = CacheMetaDiskHeader::read_from_bytes(header_bytes)
            .map_err(|_| anyhow!("read CacheMetaDiskHeader failed"))?;
        ensure!(
            header.magic == META_MAGIC,
            "meta.bin magic mismatch: expected 0x{META_MAGIC:08X}, got 0x{:08X}",
            header.magic
        );
        ensure!(
            header.version == META_VERSION,
            "meta.bin version mismatch: expected {META_VERSION}, got {}",
            header.version
        );

        // SAFETY: we have already check the size of raw
        let (key_len_bytes, data) = data.split_at(4);
        let key_len = u32::from_le_bytes(
            key_len_bytes
                .try_into()
                .context("read meta key_len failed")?,
        ) as usize;
        // key data and crc32
        ensure!(
            data.len() >= key_len + 4,
            "meta.bin truncated: key extends past end"
        );
        // SAFETY: we have already check the size of data
        let (key_bytes, data) = data.split_at(key_len);
        let key = std::str::from_utf8(key_bytes)
            .context("meta key is not valid UTF-8")?
            .to_string();

        // the last u32 (4B) is crc
        let (bitmap_bytes, crc_bytes) = data.split_at(data.len() - 4);
        let bitmap = RoaringBitmap::deserialize_from(bitmap_bytes)
            .context("failed to deserialize bitmap")?;

        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().context("read meta crc failed")?);
        let computed = crc32c::crc32c(&raw[..raw.len() - 4]);
        ensure!(
            stored_crc == computed,
            "meta CRC mismatch: stored 0x{stored_crc:08X}, computed 0x{computed:08X}"
        );

        Ok(Self {
            header,
            key,
            bitmap,
        })
    }

    /// Write atomically to `path` using crash-safe ordering:
    ///   1. Write tmp file
    ///   2. fsync tmp file
    ///   3. Rename tmp → target
    ///
    /// The caller is responsible for syncing the data file *before* calling
    /// this method if crash-safe data↔metadata consistency is required.
    pub(crate) async fn write_to_path(&self, meta_path: &Path) -> Result<()> {
        let raw = self.to_bytes()?;
        let dir = meta_path
            .parent()
            .context("meta_path has no parent directory")?;
        let tmp = dir.join(format!("meta.tmp-{}", uuid::Uuid::new_v4()));

        // Write and fsync the tmp file.
        let mut f = tokio::fs::File::create(&tmp).await?;
        <tokio::fs::File as tokio::io::AsyncWriteExt>::write_all(&mut f, &raw).await?;
        f.sync_all().await?;
        drop(f);

        match std::fs::rename(&tmp, meta_path) {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = std::fs::remove_file(&tmp);
                Err(err.into())
            }
        }
    }

    /// Load from `path`. Returns `None` if file does not exist.
    pub(crate) async fn load_from_path(path: &Path) -> Result<Self> {
        let raw = tokio::fs::read(path).await?;
        Self::from_bytes(&raw)
    }
}

/// Return a timestamp, represent the nano seconds elapsed from UNIX_EPOCH
pub(crate) fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Similar to fsync
pub(crate) async fn fsync<F: AsFd>(f: F) -> Result<()> {
    let fd = f.as_fd().as_raw_fd();
    tokio::task::spawn_blocking(move || {
        let ret = unsafe { libc::fsync(fd) };
        if ret == 0 {
            Ok(())
        } else {
            Err(Errno::last())
        }
    })
    .await
    .context("join tokio spawn_blocking task during fsync failed")?
    .context("fsync failed")
}

#[cfg(test)]
mod meta_tests {
    use super::*;

    #[test]
    fn roundtrip_cache_meta_bin() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0);
        bitmap.insert(42);
        bitmap.insert(1000);

        let meta = CacheMetaDisk {
            key: "test-key".to_string(),
            header: CacheMetaDiskHeader {
                source_size: U64::new(1024 * 1024),
                block_size: U64::new(4096),
                last_access_unix_nanos: U64::new(123456789),
                hits: U64::new(10),
                misses: U64::new(5),
                refills: U64::new(3),
                ..Default::default()
            },
            bitmap: bitmap.clone(),
        };

        let bytes = meta.to_bytes().unwrap();
        let restored = CacheMetaDisk::from_bytes(&bytes).unwrap();

        assert_eq!(restored.key, "test-key");
        assert_eq!(restored.header.source_size, 1024 * 1024);
        assert_eq!(restored.header.block_size, 4096);
        assert_eq!(restored.header.last_access_unix_nanos, 123456789);
        assert_eq!(restored.header.hits, 10);
        assert_eq!(restored.header.misses, 5);
        assert_eq!(restored.header.refills, 3);
        assert!(restored.bitmap.contains(0));
        assert!(restored.bitmap.contains(42));
        assert!(restored.bitmap.contains(1000));
        assert!(!restored.bitmap.contains(1));
    }

    #[test]
    fn corrupt_magic_rejected() {
        let meta = CacheMetaDisk {
            key: "k".to_string(),
            header: CacheMetaDiskHeader {
                ..Default::default()
            },
            bitmap: RoaringBitmap::new(),
        };
        let mut bytes = meta.to_bytes().unwrap();
        bytes[0] = 0xFF;
        assert!(CacheMetaDisk::from_bytes(&bytes).is_err());
    }

    #[test]
    fn corrupt_crc_rejected() {
        let meta = CacheMetaDisk {
            key: "k".to_string(),
            header: CacheMetaDiskHeader {
                ..Default::default()
            },
            bitmap: RoaringBitmap::new(),
        };
        let mut bytes = meta.to_bytes().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(CacheMetaDisk::from_bytes(&bytes).is_err());
    }

    #[tokio::test]
    async fn write_and_load_from_path() {
        let tmp = tempfile::tempdir().unwrap();
        let meta_path = tmp.path().join("meta.bin");

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(7);

        let meta = CacheMetaDisk {
            key: "file-key".to_string(),
            header: CacheMetaDiskHeader {
                source_size: U64::new(8192),
                block_size: U64::new(4096),
                last_access_unix_nanos: U64::new(999),
                hits: U64::new(1),
                misses: U64::new(2),
                refills: U64::new(3),
                ..Default::default()
            },
            bitmap,
        };
        meta.write_to_path(&meta_path).await.unwrap();

        let loaded = CacheMetaDisk::load_from_path(&meta_path)
            .await
            .expect("should load");
        assert_eq!(loaded.key, "file-key");
        assert_eq!(loaded.header.source_size, 8192);
        assert!(loaded.bitmap.contains(7));
        assert!(!loaded.bitmap.contains(0));
    }

    #[tokio::test]
    async fn load_nonexistent_returns_err() {
        let tmp = tempfile::tempdir().unwrap();
        let meta_path = tmp.path().join("does_not_exist.bin");
        assert!(CacheMetaDisk::load_from_path(&meta_path).await.is_err());
    }
}
