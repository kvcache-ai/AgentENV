mod bk_download;
mod cached_fs;
mod full_file_cache;
mod meta;
#[cfg(test)]
mod tests;

pub(crate) use bk_download::BkDownloadSubmitError;

use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

pub use cached_fs::{CachedFs, CachedFsSource, LocalFsSource};
pub use full_file_cache::cache_pool::{
    CacheListType, CachePoolStat, CacheStats, CachedFileStats, FileCacheBackend,
    FileCacheBackendOptions,
};
// `CacheAdvice` is re-exported alongside `CachedFile` because it is the argument
// type of the public `CachedFile::fadvise`: without it here, that method cannot
// be called from outside the crate at all, since `full_file_cache` is private.
pub use full_file_cache::cache_store::{CacheAdvice, CachedFile};

const GIB: u64 = 1024 * 1024 * 1024;
const META_MAGIC: u32 = 0x4f42_4348; // "OBCH"
const META_VERSION: u32 = 1;
const DEFAULT_CACHE_DIR: &str = "/opt/overlaybd/registry_cache";
const PAGE_SIZE: u64 = 4096;
const DEFAULT_BLOCK_SIZE: u64 = 256 * 1024;
const DEFAULT_CAPACITY_BYTES: u64 = 4 * GIB;
const DEFAULT_DISK_AVAIL_BYTES: u64 = 128 * 1024 * 1024;
const DATA_FILE_NAME: &str = "data";
const META_FILE_NAME: &str = "meta.bin";
const MAX_PREFETCH_SIZE: u64 = 32 * 1024 * 1024;
const MAX_FREE_SPACE_BYTES: u64 = 50 * GIB;
const EVICTION_MARK_BYTES: u64 = 5 * GIB;
const WATERMARK_RATIO: u64 = 90;
const DEFAULT_EVICTION_PERIOD: Duration = Duration::from_secs(1);
const DEFAULT_CHECKPOINT_PERIOD: Duration = Duration::from_secs(300);
const DEFAULT_MAX_REFILLING: u32 = 128;

pub const O_CACHE_ONLY: u32 = 0x0100_0000;
pub const RW_V2_CACHE_ONLY: u32 = 0x0000_0004;

pub type CacheFnTransFunc = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

fn div_round_up(n: u64, d: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    (n - 1) / d + 1
}

fn align_down(v: u64, a: u64) -> u64 {
    if a == 0 {
        return v;
    }
    (v / a) * a
}

fn align_up(v: u64, a: u64) -> u64 {
    if a == 0 || v == 0 {
        return v;
    }
    div_round_up(v, a).saturating_mul(a)
}

fn cache_key_digest(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let out = hasher.finalize();
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
