use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

const DIGEST_BUFFER_SIZE: usize = 128 * 1024;

/// Content identity and integrity information for a local file.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileDigest {
    /// File size in bytes, counted from the same stream used for hashing.
    pub size: u64,
    /// SHA-256 digest of the file bytes, formatted as `sha256:<hex>`.
    pub sha256: String,
}

impl FileDigest {
    /// Asynchronously opens `path`, streams its contents, and returns its size plus `sha256:<hex>` digest.
    pub async fn describe(path: &Path) -> io::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        let (hasher, size) = hash_reader_async(&mut file).await?;
        Ok(Self {
            size,
            sha256: format!("sha256:{:x}", hasher.finalize()),
        })
    }

    /// Opens `path`, streams its contents, and returns its size plus `sha256:<hex>` digest.
    pub fn describe_blocking(path: &Path) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;
        let (hasher, size) = hash_reader(&mut file)?;
        Ok(Self {
            size,
            sha256: format!("sha256:{:x}", hasher.finalize()),
        })
    }
}

/// Returns the raw 32-byte SHA-256 digest for in-memory bytes.
pub fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Returns the lowercase hexadecimal SHA-256 digest for in-memory bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Returns an OCI-style SHA-256 digest string (`sha256:<hex>`) for in-memory bytes.
pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

/// Whether `value` is a lowercase OCI-style SHA-256 digest (`sha256:<64 hex>`).
pub fn is_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// Copies all bytes from `source` to `destination` while returning the source SHA-256 hex digest and byte count.
pub fn copy_with_sha256_hex(
    mut source: impl Read,
    mut destination: impl Write,
) -> io::Result<(String, u64)> {
    let mut hasher = Sha256::new();
    let mut copied = 0_u64;
    let mut buffer = [0_u8; DIGEST_BUFFER_SIZE];

    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        destination.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        copied += read as u64;
    }

    Ok((format!("{:x}", hasher.finalize()), copied))
}

fn hash_reader(mut reader: impl Read) -> io::Result<(Sha256, u64)> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; DIGEST_BUFFER_SIZE];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }

    Ok((hasher, size))
}

async fn hash_reader_async(
    mut reader: impl tokio::io::AsyncRead + Unpin,
) -> io::Result<(Sha256, u64)> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; DIGEST_BUFFER_SIZE]; // Use a heap buffer to avoid large stack allocations in async contexts.

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }

    Ok((hasher, size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn describe_file_reports_stable_digest() {
        let temp = tempfile::NamedTempFile::new().expect("temp");
        std::fs::write(temp.path(), b"artifact").expect("write");
        let described = FileDigest::describe(temp.path()).await.expect("describe");
        assert_eq!(described.size, 8);
        assert_eq!(
            described.sha256,
            "sha256:c7c5c1d70c5dec4416ab6158afd0b223ef40c29b1dc1f97ed9428b94d4cadb1c"
        );
        let described_blocking =
            FileDigest::describe_blocking(temp.path()).expect("describe_blocking");
        assert_eq!(described_blocking, described);
    }
}
