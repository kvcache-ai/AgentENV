use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use ring::digest::{Context as DigestContext, SHA256};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;
use zerocopy::FromBytes;

use crate::backend::local::LocalFile;
use crate::backend::switch::new_switch_file;
use crate::backend::tar::new_tar_file_adaptor;
use crate::config::{LayerConfig, OverlaybdCompactOutput};
use crate::io::virtual_file::VirtualFile;
use crate::lsmt::file::{LSMTReadOnlyFile, LayerDescriptor};
use crate::lsmt::format::HeaderTrailer;

use super::output::create_commit_args;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerFile {
    pub path: PathBuf,
    pub uuid: Uuid,
    pub descriptor: Option<LayerDescriptor>,
    is_sparse_rw: bool,
}

impl LayerFile {
    pub fn dense(path: PathBuf, uuid: Uuid, descriptor: Option<LayerDescriptor>) -> Self {
        Self {
            path,
            uuid,
            descriptor,
            is_sparse_rw: false,
        }
    }

    pub fn sealed_sparse(path: PathBuf, uuid: Uuid) -> Self {
        Self {
            path,
            uuid,
            descriptor: None,
            is_sparse_rw: true,
        }
    }

    /// Read a sealed file's identity and format, retaining any already-known byte descriptor.
    pub async fn from_path(path: PathBuf, descriptor: Option<LayerDescriptor>) -> Result<Self> {
        let file = open_layer_file(&path).await?;
        let header = file
            .read_at(0, std::mem::size_of::<HeaderTrailer>())
            .await?;
        let header = HeaderTrailer::read_from_bytes(&header)
            .map_err(|_| anyhow::anyhow!("invalid layer header: {}", path.display()))?;
        let layer = LSMTReadOnlyFile::open(file).await?;
        Ok(Self {
            path,
            uuid: layer.get_uuid(0)?,
            descriptor,
            is_sparse_rw: header.is_sparse_rw(),
        })
    }
}

impl From<&LayerFile> for LayerConfig {
    fn from(layer: &LayerFile) -> Self {
        Self {
            file: layer.path.display().to_string(),
            uuid: if layer.uuid.is_nil() {
                String::new()
            } else {
                layer.uuid.to_string()
            },
            digest: layer
                .descriptor
                .as_ref()
                .map(|d| d.digest.clone())
                .unwrap_or_default(),
            size: layer.descriptor.as_ref().map(|d| d.size).unwrap_or(0),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestackResult {
    pub sealed_upper: LayerFile,
    pub compacted_lowers: Option<(Range<usize>, LayerFile)>,
}

async fn open_layer_file(path: &Path) -> Result<Arc<dyn VirtualFile>> {
    let file = new_tar_file_adaptor(Arc::new(LocalFile::open_ro(path)?)).await?;
    Ok(new_switch_file(file, true, path.to_str()).await?)
}

/// Re-encode one sealed layer without changing its logical identity.
pub async fn export_sealed_layer(
    source: &Path,
    destination: Arc<dyn VirtualFile>,
    output: OverlaybdCompactOutput,
) -> Result<(Uuid, LayerDescriptor)> {
    let layer = LSMTReadOnlyFile::open(open_layer_file(source).await?).await?;
    let uuid = layer.get_uuid(0)?;
    let args = create_commit_args(destination, output, true).await?;
    let descriptor = layer
        .commit_preserving_metadata(args)
        .await?
        .context("layer export did not return a descriptor")?;
    Ok((uuid, descriptor))
}

/// Create an export destination only when sparse bytes need a dense representation.
pub async fn export_sparse_layer(
    source: &LayerFile,
    destination: impl FnOnce() -> Result<PathBuf>,
    output: OverlaybdCompactOutput,
) -> Result<Option<LayerFile>> {
    if !source.is_sparse_rw {
        return Ok(None);
    }
    let path = destination()?;
    let (uuid, descriptor) = export_sealed_layer(
        &source.path,
        Arc::new(LocalFile::open_rw(&path, false)?),
        output,
    )
    .await?;
    ensure!(
        source.uuid.is_nil() || source.uuid == uuid,
        "layer identity changed: {}",
        source.path.display()
    );
    Ok(Some(LayerFile::dense(path, uuid, Some(descriptor))))
}

/// Return uploadable bytes and their descriptor, reusing complete files without rewriting them.
pub async fn prepare_layer_file(
    source: &LayerFile,
    destination: impl FnOnce() -> Result<PathBuf>,
    output: OverlaybdCompactOutput,
) -> Result<LayerFile> {
    if let Some(exported) = export_sparse_layer(source, destination, output).await? {
        return Ok(exported);
    }
    let descriptor = match &source.descriptor {
        Some(descriptor) => {
            ensure!(
                tokio::fs::metadata(&source.path).await?.len() == descriptor.size,
                "layer size differs from its descriptor: {}",
                source.path.display()
            );
            descriptor.clone()
        }
        None => {
            let mut input = tokio::fs::File::open(&source.path).await?;
            let mut digest = DigestContext::new(&SHA256);
            let mut bytes = vec![0; 1024 * 1024];
            let mut size = 0;
            loop {
                let count = input.read(&mut bytes).await?;
                if count == 0 {
                    break;
                }
                digest.update(&bytes[..count]);
                size += count as u64;
            }
            LayerDescriptor {
                digest: format!("sha256:{}", hex::encode(digest.finish())),
                size,
            }
        }
    };
    Ok(LayerFile::dense(
        source.path.clone(),
        source.uuid,
        Some(descriptor),
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::lsmt::file::LSMTFile;

    #[tokio::test]
    async fn file_preparation_exports_sparse_and_reuses_tar_bytes() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source_path = root.path().join("source.commit");
        let writer =
            LSMTFile::create(Arc::new(LocalFile::new(&source_path)?), None, 4096, true).await?;
        writer.write_at(0, &[42; 4096]).await?;
        writer.close_seal().await?;
        let uuid = writer.get_uuid(0)?;
        drop(writer);
        let original = std::fs::read(&source_path)?;
        let source = LayerFile::from_path(source_path.clone(), None).await?;
        let destination = tempfile::NamedTempFile::new_in(root.path())?;
        let dense = prepare_layer_file(
            &source,
            || Ok(destination.path().to_path_buf()),
            OverlaybdCompactOutput::Raw,
        )
        .await?;
        assert_eq!(dense.uuid, uuid);
        assert_eq!(std::fs::read(source_path)?, original);
        let dense_bytes = std::fs::read(&dense.path)?;
        assert_eq!(
            dense.descriptor.as_ref().unwrap().digest,
            format!(
                "sha256:{}",
                hex::encode(ring::digest::digest(&SHA256, &dense_bytes))
            )
        );

        let archive_path = root.path().join("layer.tar");
        let mut archive = tar::Builder::new(std::fs::File::create(&archive_path)?);
        let mut header = tar::Header::new_ustar();
        header.set_size(dense_bytes.len() as u64);
        header.set_mode(0o644);
        archive.append_data(&mut header, "overlaybd.commit", dense_bytes.as_slice())?;
        archive.finish()?;
        drop(archive);
        let archive_file = LayerFile::from_path(archive_path.clone(), None).await?;
        let prepared = prepare_layer_file(
            &archive_file,
            || panic!("dense file must not allocate an export"),
            OverlaybdCompactOutput::Raw,
        )
        .await?;
        assert_eq!(prepared.path, archive_path);
        assert_eq!(prepared.uuid, uuid);
        let bytes = std::fs::read(&prepared.path)?;
        let descriptor = prepared.descriptor.unwrap();
        assert_eq!(descriptor.size, bytes.len() as u64);
        assert_eq!(
            descriptor.digest,
            format!(
                "sha256:{}",
                hex::encode(ring::digest::digest(&SHA256, &bytes))
            )
        );
        let reader = LSMTReadOnlyFile::open(open_layer_file(&prepared.path).await?).await?;
        assert_eq!(reader.read_at(0, 4096).await?.as_ref(), &[42; 4096]);
        Ok(())
    }
}
