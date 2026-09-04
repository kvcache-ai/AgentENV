use super::local::LocalFile;
use super::tar::new_tar_file_adaptor;
use crate::compression::zfile::{is_zfile, zfile_open_ro_vfile};
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn try_open_zfile(
    file: Arc<dyn VirtualFile>,
    verify: bool,
    file_path: Option<&str>,
) -> Result<Arc<dyn VirtualFile>> {
    let detected = match file_path {
        Some(path) => is_zfile(file.clone())
            .await
            .context(format!("check file type `{path}` failed"))?,
        None => is_zfile(file.clone()).await?,
    };
    if detected == 1 {
        return match file_path {
            Some(path) => zfile_open_ro_vfile(file, verify)
                .await
                .context(format!("open zfile `{path}` failed")),
            None => zfile_open_ro_vfile(file, verify).await,
        };
    }
    Ok(file)
}

pub struct SwitchFile {
    m_file: Option<Arc<dyn VirtualFile>>,
    m_local_file: RwLock<Option<Arc<dyn VirtualFile>>>,
    m_filepath: RwLock<Option<String>>,
}

impl fmt::Debug for SwitchFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwitchFile").finish_non_exhaustive()
    }
}

impl SwitchFile {
    fn from_opened(file: Arc<dyn VirtualFile>, local: bool, file_path: Option<&str>) -> Self {
        let local_file = if local { Some(file.clone()) } else { None };
        let source_file = if local { None } else { Some(file) };
        Self {
            m_file: source_file,
            m_local_file: RwLock::new(local_file),
            m_filepath: RwLock::new(file_path.map(str::to_owned)),
        }
    }

    async fn current_file(&self) -> Result<Arc<dyn VirtualFile>> {
        if let Some(file) = self.m_local_file.read().await.clone() {
            return Ok(file);
        }
        if let Some(file) = self.m_file.clone() {
            return Ok(file);
        }
        bail!("switch file has no active backing file")
    }

    pub async fn filepath(&self) -> Option<String> {
        self.m_filepath.read().await.clone()
    }

    /// Mark this switch file as a local file, all later IO will goto local.
    pub async fn set_switch_file(&self, filepath: impl AsRef<Path>) -> Result<()> {
        let path = filepath.as_ref();
        let display = path.to_string_lossy().into_owned();
        *self.m_filepath.write().await = Some(display.clone());

        let file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path).context(format!("open commit file `{display}` failed"))?,
        );
        let file = new_tar_file_adaptor(file)
            .await
            .context(format!("open commit file as tar file `{display}` failed"))?;
        let file = try_open_zfile(file, false, Some(&display)).await?;

        *self.m_local_file.write().await = Some(file);
        Ok(())
    }
}

pub async fn new_switch_file(
    source: Arc<dyn VirtualFile>,
    local: bool,
    file_path: Option<&str>,
) -> Result<Arc<SwitchFile>> {
    let mut retry = 1u8;
    loop {
        match try_open_zfile(source.clone(), !local, file_path).await {
            Ok(file) => return Ok(Arc::new(SwitchFile::from_opened(file, local, file_path))),
            Err(_e) if retry > 0 => retry -= 1,
            Err(e) => {
                return Err(match file_path {
                    Some(path) => e.context(format!("open source file as zfile `{path}` failed")),
                    None => e,
                })
            }
        }
    }
}

#[async_trait]
impl VirtualFile for SwitchFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.current_file().await?.read_at(offset, len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.current_file().await?.read_at_into(offset, dst).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.current_file().await?.write_at(offset, data).await
    }

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        self.current_file()
            .await?
            .write_bytes_at(offset, data)
            .await
    }

    async fn size(&self) -> Result<u64> {
        self.current_file().await?.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.current_file().await?.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.current_file().await?.sync().await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        self.current_file().await?.seek_data(offset).await
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        self.current_file().await?.seek_hole(offset).await
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.current_file().await?.evict_range(offset, len).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.current_file().await?.evict_all().await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        self.current_file().await?.fgetxattr(name).await
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        self.current_file().await?.flistxattr().await
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        self.current_file()
            .await?
            .fsetxattr(name, value, flags)
            .await
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        self.current_file().await?.fremovexattr(name).await
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let file = self.current_file().await?;
            file.read_at_with_ctx(ctx, offset, len).await
        })
    }

    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let file = self.current_file().await?;
            file.read_at_into_with_ctx(ctx, offset, dst).await
        })
    }

    #[cfg(feature = "io-uring")]
    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let file = self.current_file().await?;
            file.write_at_with_ctx(ctx, offset, data).await
        })
    }

    #[cfg(feature = "io-uring")]
    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let file = self.current_file().await?;
            file.write_bytes_at_with_ctx(ctx, offset, data).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::local::LocalFile;
    use super::super::tar::new_tar_file_create;
    use super::*;
    use crate::compression::zfile::{zfile_compress, CompressArgs, CompressOptions};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    struct MemoryFile {
        data: Mutex<Vec<u8>>,
    }

    impl MemoryFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Mutex::new(data),
            }
        }
    }

    #[async_trait]
    impl VirtualFile for MemoryFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            let data = self.data.lock().await;
            if len == 0 || offset >= data.len() as u64 {
                return Ok(Bytes::new());
            }
            let end = offset.saturating_add(len as u64).min(data.len() as u64) as usize;
            Ok(Bytes::copy_from_slice(&data[offset as usize..end]))
        }

        async fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
            let mut data = self.data.lock().await;
            let end = usize::try_from(offset.saturating_add(buf.len() as u64))
                .context("write range overflow")?;
            if end > data.len() {
                data.resize(end, 0);
            }
            data[offset as usize..end].copy_from_slice(buf);
            Ok(buf.len())
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.lock().await.len() as u64)
        }

        async fn truncate(&self, size: u64) -> Result<()> {
            let mut data = self.data.lock().await;
            let size = usize::try_from(size).context("truncate size overflow")?;
            data.resize(size, 0);
            Ok(())
        }

        async fn sync(&self) -> Result<()> {
            Ok(())
        }
    }

    async fn write_all_at(
        file: &(dyn VirtualFile + Send + Sync),
        mut data: &[u8],
        mut offset: u64,
    ) -> Result<()> {
        while !data.is_empty() {
            let written = file.write_at(offset, data).await?;
            if written == 0 {
                bail!("write_at returned zero bytes");
            }
            data = &data[written..];
            offset = offset
                .checked_add(written as u64)
                .context("offset overflow")?;
        }
        Ok(())
    }

    async fn read_all(file: &(dyn VirtualFile + Send + Sync)) -> Result<Vec<u8>> {
        let size = file.size().await? as usize;
        let mut offset = 0u64;
        let mut out = Vec::with_capacity(size);
        while out.len() < size {
            let chunk = file.read_at(offset, size - out.len()).await?;
            if chunk.is_empty() {
                bail!("short read while collecting file contents");
            }
            offset = offset
                .checked_add(chunk.len() as u64)
                .context("offset overflow")?;
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    async fn create_plain_tar_file(path: &Path, data: &[u8]) -> Result<()> {
        let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path)?);
        let tar = new_tar_file_create(file).await?;
        write_all_at(tar.as_ref(), data, 0).await?;
        tar.close().await
    }

    async fn create_zfile_tar_file(path: &Path, data: &[u8]) -> Result<()> {
        let src: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(data.to_vec()));
        let dst: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path)?);
        let tar = new_tar_file_create(dst).await?;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, tar.clone(), &args).await?;
        tar.close().await
    }

    fn sample_bytes(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|idx| seed.wrapping_add((idx % 251) as u8))
            .collect()
    }

    #[tokio::test]
    async fn test_new_switch_file_passthrough_source() {
        let source_data = sample_bytes(0x11, 4096);
        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data.clone()));

        let switch = new_switch_file(source, false, Some("http://registry/blob"))
            .await
            .expect("open switch file");

        assert_eq!(
            read_all(switch.as_ref()).await.expect("read switch source"),
            source_data
        );
        assert_eq!(
            switch.filepath().await.expect("filepath"),
            "http://registry/blob"
        );
    }

    #[tokio::test]
    async fn test_set_switch_file_to_plain_local_tar() {
        let dir = tempdir().expect("create tempdir");
        let local_path = dir.path().join("plain.commit");

        let source_data = sample_bytes(0x21, 4096);
        let local_data = sample_bytes(0x61, 8192);
        create_plain_tar_file(&local_path, &local_data)
            .await
            .expect("create local tar file");

        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data.clone()));
        let switch = new_switch_file(source, false, Some("http://registry/blob"))
            .await
            .expect("open switch file");

        assert_eq!(
            read_all(switch.as_ref()).await.expect("read before switch"),
            source_data
        );

        switch
            .set_switch_file(&local_path)
            .await
            .expect("switch to local tar");

        assert_eq!(
            read_all(switch.as_ref()).await.expect("read after switch"),
            local_data
        );
        assert_eq!(
            switch.filepath().await.expect("filepath"),
            local_path.to_string_lossy().as_ref()
        );
    }

    #[tokio::test]
    async fn test_set_switch_file_to_local_tar_zfile() {
        let dir = tempdir().expect("create tempdir");
        let local_path = dir.path().join("zfile.commit");

        let source_data = sample_bytes(0x33, 4096);
        let local_data = sample_bytes(0x77, 16 * 1024 + 123);
        create_zfile_tar_file(&local_path, &local_data)
            .await
            .expect("create local tar zfile");

        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data));
        let switch = new_switch_file(source, false, Some("http://registry/blob"))
            .await
            .expect("open switch file");

        switch
            .set_switch_file(&local_path)
            .await
            .expect("switch to local zfile");

        assert_eq!(
            switch.size().await.expect("zfile size"),
            local_data.len() as u64
        );
        assert_eq!(
            read_all(switch.as_ref())
                .await
                .expect("read decompressed local"),
            local_data
        );
    }

    #[tokio::test]
    async fn test_set_switch_file_failure_keeps_current_source() {
        let dir = tempdir().expect("create tempdir");
        let missing_path = dir.path().join("missing.commit");

        let source_data = sample_bytes(0x45, 4096);
        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data.clone()));
        let switch = new_switch_file(source, false, Some("http://registry/blob"))
            .await
            .expect("open switch file");

        let error = switch
            .set_switch_file(&missing_path)
            .await
            .expect_err("missing file must fail");
        let error_msg = format!("{error:#}");
        assert!(
            error_msg.contains("open commit file") || error_msg.contains("No such file"),
            "unexpected error: {error_msg}"
        );
        assert_eq!(
            read_all(switch.as_ref())
                .await
                .expect("read after failed switch"),
            source_data
        );
        assert_eq!(
            switch.filepath().await.expect("filepath"),
            missing_path.to_string_lossy().as_ref()
        );
    }

    #[tokio::test]
    async fn test_set_switch_file_failure_keeps_current_local_file() {
        let dir = tempdir().expect("create tempdir");
        let local_path = dir.path().join("plain.commit");
        let missing_path = dir.path().join("missing.commit");

        let source_data = sample_bytes(0x52, 4096);
        let local_data = sample_bytes(0x83, 8192);
        create_plain_tar_file(&local_path, &local_data)
            .await
            .expect("create local tar file");

        let source: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(source_data));
        let switch = new_switch_file(source, false, Some("http://registry/blob"))
            .await
            .expect("open switch file");
        switch
            .set_switch_file(&local_path)
            .await
            .expect("switch to local tar");
        assert_eq!(
            read_all(switch.as_ref())
                .await
                .expect("read switched local"),
            local_data
        );

        let error = switch
            .set_switch_file(&missing_path)
            .await
            .expect_err("missing file must fail");
        let error_msg = format!("{error:#}");
        assert!(
            error_msg.contains("open commit file") || error_msg.contains("No such file"),
            "unexpected error: {error_msg}"
        );
        assert_eq!(
            read_all(switch.as_ref())
                .await
                .expect("read local after failed re-switch"),
            local_data
        );
        assert_eq!(
            switch.filepath().await.expect("filepath"),
            missing_path.to_string_lossy().as_ref()
        );
    }

    #[tokio::test]
    async fn test_new_switch_file_local_mode_opens_zfile() {
        let dir = tempdir().expect("create tempdir");
        let local_path = dir.path().join("local-zfile.commit");
        let local_data = sample_bytes(0x58, 12 * 1024 + 97);
        create_zfile_tar_file(&local_path, &local_data)
            .await
            .expect("create tar zfile");

        let local: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_ro(&local_path).expect("open local"));
        let local = new_tar_file_adaptor(local).await.expect("adapt local tar");
        let switch = new_switch_file(local, true, Some(local_path.to_string_lossy().as_ref()))
            .await
            .expect("open local switch file");

        assert_eq!(switch.size().await.expect("size"), local_data.len() as u64);
        assert_eq!(
            read_all(switch.as_ref()).await.expect("read local mode"),
            local_data
        );
    }
}
