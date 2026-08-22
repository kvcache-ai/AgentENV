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
