use anyhow::{Context, Result};

#[cfg(feature = "full")]
use std::path::Path;
#[cfg(feature = "full")]
use std::sync::Arc;

#[cfg(feature = "full")]
use crate::backend::local::LocalFile;
#[cfg(feature = "full")]
use crate::image::image_file::ImageFile;
#[cfg(feature = "full")]
use crate::image::image_service::ImageService;
#[cfg(feature = "full")]
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "full")]
use crate::lsmt::file::CommitArgs;

#[cfg(feature = "full")]
pub async fn export_upper_as_snapshot_layer(
    _image_service: &ImageService,
    image: &ImageFile,
    output_layer_path: &Path,
) -> Result<()> {
    if image.is_read_only().await {
        return Ok(());
    }

    let output_file: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(output_layer_path).with_context(|| {
            format!(
                "create overlaybd snapshot output failed: {}",
                output_layer_path.display()
            )
        })?);
    image
        .export_upper_as_sealed(CommitArgs::new(output_file))
        .await
        .with_context(|| {
            format!(
                "export overlaybd writable upper as snapshot failed: {}",
                output_layer_path.display()
            )
        })?;
    Ok(())
}

#[cfg(all(test, feature = "full"))]
mod tests {
    use super::export_upper_as_snapshot_layer;
    use crate::backend::local::LocalFile;
    use crate::config::{GlobalConfig, UpperMode};
    use crate::image::helper::prepare_runtime_upper;
    use crate::image::image_service::ImageService;
    use crate::io::virtual_file::VirtualFile;
    use crate::lsmt::file::{create_file_rw, LSMTReadOnlyFile, LayerInfo};
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn write_global_config(temp: &TempDir) -> anyhow::Result<PathBuf> {
        let path = temp.path().join("global.json");
        let mut cfg = GlobalConfig {
            enable_audit: false,
            nr_io_rings: 1,
            ..GlobalConfig::default()
        };
        cfg.cache_config.cache_type = "file".to_string();
        cfg.cache_config.cache_dir = temp.path().join("cache").display().to_string();
        std::fs::write(&path, serde_json::to_vec_pretty(&cfg)?)?;
        Ok(path)
    }

    async fn create_sealed_lower(
        path: &Path,
        index_path: &Path,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path)?);
        let index_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(index_path)?);
        let args = LayerInfo::new(data_file, Some(index_file), payload.len() as u64);
        let lower = create_file_rw(args).await?;
        lower.write_at(0, payload).await?;
        lower.close_seal().await?;
        Ok(())
    }

    fn write_image_config(
        temp: &TempDir,
        lower_path: &Path,
        read_only: bool,
    ) -> anyhow::Result<PathBuf> {
        let path = temp.path().join("image.json");
        let upper = if read_only {
            json!({})
        } else {
            json!({
                "index": temp.path().join("upper.index"),
                "data": temp.path().join("upper.data"),
                "target": "",
                "gzipIndex": ""
            })
        };
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "lowers": [{
                    "file": lower_path
                }],
                "upper": upper,
                "resultFile": temp.path().join("result.txt"),
            }))?,
        )?;
        Ok(path)
    }

    #[tokio::test]
    async fn export_snapshot_layer_creates_sealed_output_for_writable_image() -> anyhow::Result<()>
    {
        let temp = tempfile::tempdir()?;
        let global_path = write_global_config(&temp)?;
        let lower_path = temp.path().join("lower.commit");
        let lower_index = temp.path().join("lower.index");
        create_sealed_lower(&lower_path, &lower_index, &[0u8; 8192]).await?;
        let image_path = write_image_config(&temp, &lower_path, false)?;
        prepare_runtime_upper(
            &temp.path().join("upper.data"),
            Some(&temp.path().join("upper.index")),
            8192,
            UpperMode::LogStructured,
        )?;

        let image_service = ImageService::from_config_path(&global_path).await?;
        let image = image_service.create_image_file(&image_path).await?;
        image.write_at(0, &[0xAB; 4096]).await?;
        image.sync().await?;

        let snapshot_path = temp.path().join("snapshot.commit");
        export_upper_as_snapshot_layer(&image_service, &image, &snapshot_path).await?;

        let snapshot_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&snapshot_path)?);
        let snapshot = LSMTReadOnlyFile::open(snapshot_file).await?;
        let content = snapshot.read_at(0, 4096).await?;
        assert_eq!(content.as_ref(), &[0xAB; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn export_snapshot_layer_skips_output_for_read_only_image() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let global_path = write_global_config(&temp)?;
        let lower_path = temp.path().join("lower.commit");
        let lower_index = temp.path().join("lower.index");
        create_sealed_lower(&lower_path, &lower_index, &[0x11; 4096]).await?;
        let image_path = write_image_config(&temp, &lower_path, true)?;

        let image_service = ImageService::from_config_path(&global_path).await?;
        let image = image_service.create_image_file(&image_path).await?;
        let snapshot_path = temp.path().join("readonly-snapshot.commit");

        export_upper_as_snapshot_layer(&image_service, &image, &snapshot_path).await?;

        assert!(!snapshot_path.exists());
        Ok(())
    }
}
