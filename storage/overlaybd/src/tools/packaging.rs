use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::io::AsyncReadExt;

use crate::backend::local::LocalFile;
use crate::io::virtual_file::VirtualFile;
use crate::lsmt::file::{
    compact_to, create_file_rw, create_mappings_from_sparse, CommitArgs, LayerInfo,
};

const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;
/// Package an ext4 file as an overlaybd lower layer (commit + index).
pub async fn package_ext4_as_overlaybd(
    source: &Path,
    output: &Path,
    index: &Path,
    chunk_size: Option<usize>,
) -> Result<()> {
    let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);

    let parent = output
        .parent()
        .context("output path should have a parent directory")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create output directory failed: {}", parent.display()))?;

    let lower_tmp = output.with_extension("commit.tmp");
    let index_tmp = index.with_extension("index.tmp");
    let build_result = async {
        let virtual_size = tokio::fs::metadata(source)
            .await
            .with_context(|| format!("stat source rootfs failed: {}", source.display()))?
            .len();
        let data_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let index_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&index_tmp)
                .with_context(|| format!("create temp index failed: {}", index_tmp.display()))?,
        );
        let lsmt = create_file_rw(LayerInfo::new(data_file, Some(index_file), virtual_size))
            .await
            .context("create overlaybd rw layer failed")?;
        let mut input = tokio::fs::File::open(source)
            .await
            .with_context(|| format!("open source rootfs failed: {}", source.display()))?;
        let mut offset = 0u64;
        let mut buffer = vec![0u8; chunk_size];

        loop {
            let n = input
                .read(&mut buffer)
                .await
                .with_context(|| format!("read source rootfs failed: {}", source.display()))?;
            if n == 0 {
                break;
            }
            let written = lsmt
                .write_at(offset, &buffer[..n])
                .await
                .with_context(|| format!("write overlaybd lower failed at offset {offset}"))?;
            if written != n {
                bail!(
                    "short write while packaging overlaybd lower: expected {}, wrote {}",
                    n,
                    written
                );
            }
            offset += n as u64;
        }

        if offset != virtual_size {
            bail!(
                "packaged overlaybd lower size mismatch: expected {}, wrote {}",
                virtual_size,
                offset
            );
        }

        lsmt.close_seal()
            .await
            .context("seal overlaybd lower failed")?;
        tokio::fs::rename(&lower_tmp, output)
            .await
            .with_context(|| {
                format!("move sealed lower into place failed: {}", output.display())
            })?;
        tokio::fs::rename(&index_tmp, index)
            .await
            .with_context(|| format!("move sealed index into place failed: {}", index.display()))?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
        let _ = tokio::fs::remove_file(&index_tmp).await;
    }

    build_result
}

/// Compact a raw file into the destination owned by `commit_args`.
pub async fn package_raw_as_overlaybd_with_args(
    source: &Path,
    commit_args: CommitArgs,
) -> Result<()> {
    let virtual_size = tokio::fs::metadata(source)
        .await
        .with_context(|| format!("stat source raw file failed: {}", source.display()))?
        .len();
    let source_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(source)
            .with_context(|| format!("open source raw file failed: {}", source.display()))?,
    );
    let mappings = create_mappings_from_sparse(&source_file, 0)
        .await
        .with_context(|| format!("scan sparse raw file failed: {}", source.display()))?;
    let src_layers = vec![source_file];
    compact_to(&src_layers, &mappings, virtual_size, commit_args)
        .await
        .with_context(|| format!("compact raw file as overlaybd failed: {}", source.display()))
}

/// Package a raw file as a sealed overlaybd lower layer.
pub async fn package_raw_as_overlaybd(source: &Path, output: &Path) -> Result<()> {
    let parent = output
        .parent()
        .context("output path should have a parent directory")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create output directory failed: {}", parent.display()))?;

    let lower_tmp = output.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let mut commit_args = CommitArgs::new(output_file);
        commit_args.concurrency = 32;
        package_raw_as_overlaybd_with_args(source, commit_args).await?;
        tokio::fs::rename(&lower_tmp, output)
            .await
            .with_context(|| {
                format!("move sealed lower into place failed: {}", output.display())
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }

    build_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsmt::file::open_file_ro;
    use std::os::unix::fs::FileExt;
    use tempfile::TempDir;

    const SECTOR: u64 = 512;

    /// Create a `len`-byte file, then write `data_at` ranges into it, leaving the
    /// rest unwritten. Whether the unwritten parts stay holes is up to the
    /// filesystem — that is precisely the variable these tests must tolerate.
    fn sparse_source(dir: &TempDir, len: u64, data_at: &[(u64, u8)]) -> std::path::PathBuf {
        let path = dir.path().join("source.raw");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(len).unwrap();
        for &(offset, fill) in data_at {
            file.write_all_at(&vec![fill; SECTOR as usize], offset)
                .unwrap();
        }
        file.sync_all().unwrap();
        path
    }

    /// Read the whole sealed layer back in sector-aligned chunks.
    async fn read_layer(path: &Path, len: u64) -> Vec<u8> {
        let layer = open_file_ro(Arc::new(LocalFile::open_ro(path).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            layer.size().await.unwrap(),
            len,
            "virtual size must survive"
        );

        let chunk = 1024 * 1024;
        let mut out = Vec::with_capacity(len as usize);
        let mut offset = 0u64;
        while offset < len {
            let want = chunk.min((len - offset) as usize);
            out.extend_from_slice(&layer.read_at(offset, want).await.unwrap());
            offset += want as u64;
        }
        out
    }

    /// The property that makes it safe to run this packager where allocation is
    /// speculative: however much the filesystem allocated beyond what was
    /// written, the packaged layer reads back byte-for-byte identical to the
    /// source. Copying an allocated-but-never-written range costs space, never
    /// correctness, because it is read out of the source, where it reads as
    /// zeros.
    #[tokio::test]
    async fn package_raw_preserves_source_content_whatever_the_extent_map_says() {
        let dir = TempDir::new().unwrap();
        let len = 8 * 1024 * 1024;
        let source = sparse_source(
            &dir,
            len,
            &[(0, 0xAA), (1024 * 1024, 0xBB), (len - SECTOR, 0xCC)],
        );
        let output = dir.path().join("layer.commit");

        package_raw_as_overlaybd(&source, &output).await.unwrap();

        assert_eq!(
            read_layer(&output, len).await,
            std::fs::read(&source).unwrap(),
            "packaged layer must read back identical to the source"
        );

        // Only assert the space saving where the filesystem actually guarantees
        // it. APFS allocates across gaps below a ~16-20 MiB threshold, so this
        // 8 MiB source is legitimately fully allocated there.
        #[cfg(target_os = "linux")]
        {
            let packaged = std::fs::metadata(&output).unwrap().len();
            assert!(
                packaged < len / 2,
                "sparse scan should have skipped the holes, but packaged {packaged} of {len}"
            );
        }
    }

    /// A source with nothing written at all: the scan yields no mappings, and
    /// every absent mapping reads back as zeros.
    #[tokio::test]
    async fn package_raw_handles_a_source_with_no_data_at_all() {
        let dir = TempDir::new().unwrap();
        let len = 2 * 1024 * 1024;
        let source = sparse_source(&dir, len, &[]);
        let output = dir.path().join("layer.commit");

        package_raw_as_overlaybd(&source, &output).await.unwrap();

        assert!(
            read_layer(&output, len).await.iter().all(|&b| b == 0),
            "an all-holes source must read back as all zeros"
        );
    }

    /// The block arithmetic in `create_mappings_from_sparse` truncates, so an
    /// unaligned source size would drop its trailing partial sector from the
    /// index and read back as zeros. That must be an error, not silent loss.
    #[tokio::test]
    async fn package_raw_rejects_a_source_whose_size_is_not_sector_aligned() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("source.raw");
        std::fs::write(&path, vec![0xAB; 1000]).unwrap();

        let err = package_raw_as_overlaybd(&path, &dir.path().join("layer.commit"))
            .await
            .expect_err("an unaligned source size must be rejected");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("unaligned data extent"),
            "expected an unaligned-extent error, got: {rendered}"
        );
    }
}
