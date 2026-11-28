use super::local::LocalFile;
use super::tar::new_tar_file_adaptor;
use crate::compression::zfile::{is_zfile, zfile_open_ro_vfile};
#[cfg(feature = "io-uring")]
use crate::io::vfile_io::CtxRead;
use crate::io::vfile_io::{DirectRead, FileReader};
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{timeout_at, Instant};

const REOPEN_TIMEOUT: Duration = Duration::from_secs(60);
const REOPEN_INTERVAL: Duration = Duration::from_millis(250);

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

async fn open_zfile(
    file: Arc<dyn VirtualFile>,
    verify: bool,
    path: Option<&str>,
    mut retries: u8,
) -> Result<Arc<dyn VirtualFile>> {
    loop {
        match try_open_zfile(file.clone(), verify, path).await {
            Ok(file) => return Ok(file),
            Err(_) if retries > 0 => retries -= 1,
            Err(error) => return Err(error),
        }
    }
}

struct SwitchState {
    local_file: Option<Arc<dyn VirtualFile>>,
    filepath: Option<PathBuf>,
    direct_io: bool,
    local_size: Option<u64>,
}

/// A remote source that can switch to an immutable local lower. Local paths
/// must continue naming the same content: a disconnected local handle is
/// reopened at that path, retaining the decoded size and opening options.
pub struct SwitchFile {
    m_file: Option<Arc<dyn VirtualFile>>,
    state: RwLock<SwitchState>,
    reopen_gate: Arc<Mutex<Instant>>,
}

impl fmt::Debug for SwitchFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwitchFile").finish_non_exhaustive()
    }
}

impl SwitchFile {
    async fn from_opened(
        file: Arc<dyn VirtualFile>,
        local: bool,
        file_path: Option<&Path>,
        direct_io: bool,
    ) -> Result<Self> {
        let local_size = if local {
            Some(file.size().await?)
        } else {
            None
        };
        let local_file = if local { Some(file.clone()) } else { None };
        let source_file = if local { None } else { Some(file) };
        Ok(Self {
            m_file: source_file,
            state: RwLock::new(SwitchState {
                local_file,
                filepath: file_path.map(Path::to_path_buf),
                direct_io,
                local_size,
            }),
            reopen_gate: Arc::new(Mutex::new(Instant::now())),
        })
    }

    /// Open an immutable local lower. The pathname must keep naming the same
    /// content across remounts; it must never be reused for a different layer.
    pub async fn open_local(path: &Path, direct_io: bool) -> Result<Arc<Self>> {
        let file = Self::open_local_file(path, direct_io, 1).await?;
        Ok(Arc::new(
            Self::from_opened(file, true, Some(path), direct_io).await?,
        ))
    }

    async fn open_local_file(
        path: &Path,
        direct_io: bool,
        zfile_retries: u8,
    ) -> Result<Arc<dyn VirtualFile>> {
        let display = path.to_string_lossy().into_owned();
        let file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::builder()
                .write(false)
                .create(false)
                .direct_io(direct_io)
                .open(path)
                .with_context(|| format!("open commit file `{display}` failed"))?,
        );
        let file = new_tar_file_adaptor(file)
            .await
            .with_context(|| format!("open commit file as tar file `{display}` failed"))?;
        open_zfile(file, false, Some(&display), zfile_retries).await
    }

    async fn current_file(&self) -> Result<Arc<dyn VirtualFile>> {
        if let Some(file) = self.state.read().await.local_file.clone() {
            return Ok(file);
        }
        if let Some(file) = self.m_file.clone() {
            return Ok(file);
        }
        bail!("switch file has no active backing file")
    }

    async fn is_current_file(&self, file: &Arc<dyn VirtualFile>) -> Result<bool> {
        Ok(Arc::ptr_eq(&self.current_file().await?, file))
    }

    pub async fn filepath(&self) -> Option<String> {
        self.state
            .read()
            .await
            .filepath
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
    }

    /// Mark this switch file as a local file, all later IO will goto local.
    pub async fn set_switch_file(&self, filepath: impl AsRef<Path>) -> Result<()> {
        let _gate = self.reopen_gate.lock().await;
        let path = filepath.as_ref();
        // Keep the buffered mode previously used by LocalFile::open_ro.
        let direct_io = false;
        let file = Self::open_local_file(path, direct_io, 0).await?;
        let local_size = Some(file.size().await?);
        // Publish the file and its successful pathname together. A failed
        // switch must not redirect a later reopen to the failed candidate.
        *self.state.write().await = SwitchState {
            local_file: Some(file),
            filepath: Some(path.to_path_buf()),
            direct_io,
            local_size,
        };
        Ok(())
    }

    async fn reopen_after_error(
        &self,
        failed: &Arc<dyn VirtualFile>,
        error: anyhow::Error,
        deadline: &mut Option<Instant>,
    ) -> Result<()> {
        if !disconnected(&error) {
            return Err(error);
        }
        let (path, direct_io, expected_size) = {
            let state = self.state.read().await;
            let (Some(_), Some(path)) = (&state.local_file, &state.filepath) else {
                return Err(error);
            };
            (path.clone(), state.direct_io, state.local_size)
        };
        let deadline = *deadline.get_or_insert_with(|| Instant::now() + REOPEN_TIMEOUT);
        if !self.is_current_file(failed).await? {
            return Ok(());
        }
        let mut next_attempt = match timeout_at(deadline, self.reopen_gate.clone().lock_owned())
            .await
        {
            Ok(gate) => gate,
            Err(_) => return Err(error).context("timed out waiting to reopen disconnected lower"),
        };
        check_retry_deadline(Some(deadline))?;
        // Path/options only change with a new file generation under the gate.
        // Rechecking that generation validates the snapshot above as well.
        if !self.is_current_file(failed).await? {
            return Ok(());
        }
        let mut last_error = error;
        loop {
            // Throttle across generations too: metadata can still be cached
            // on a dead mount even when every subsequent cold read fails.
            tokio::time::sleep_until((*next_attempt).min(deadline)).await;
            if Instant::now() >= deadline {
                return Err(last_error).context("timed out reopening disconnected lower");
            }
            *next_attempt = Instant::now() + REOPEN_INTERVAL;
            // Probing tar/ZFile also does synchronous reads. Keep the whole
            // candidate construction off the ublk queue's single thread.
            // The worker owns the gate until it finishes, even if this caller
            // times out or is cancelled. Timing out its JoinHandle does not
            // cancel IO or allow another candidate to start in parallel.
            let reopen_path = path.clone();
            let runtime = tokio::runtime::Handle::current();
            let worker = tokio::task::spawn_blocking(move || {
                let opened = runtime.block_on(async {
                    // The caller may time out while this worker is queued.
                    check_retry_deadline(Some(deadline))?;
                    let file = Self::open_local_file(&reopen_path, direct_io, 0).await?;
                    ensure!(
                        Some(file.size().await?) == expected_size,
                        "reopened immutable lower changed size: {}",
                        reopen_path.display()
                    );
                    Ok::<_, anyhow::Error>(file)
                });
                (opened, next_attempt)
            });
            let (opened, gate) = match timeout_at(deadline, worker).await {
                Ok(joined) => joined.context("join disconnected lower reopen")?,
                Err(_) => return Err(last_error).context("timed out reopening disconnected lower"),
            };
            next_attempt = gate;
            match opened {
                Ok(file) => {
                    self.state.write().await.local_file = Some(file);
                    tracing::info!(path = %path.display(), "reopened disconnected lower");
                    return Ok(());
                }
                Err(error) if disconnected(&error) || not_found(&error) => {
                    // ENOENT is transient only after an old handle proved
                    // disconnected: rebind may briefly expose an empty path.
                    last_error = error;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn read_bytes<R: FileReader>(&self, reader: R, offset: u64, len: usize) -> Result<Bytes> {
        let mut deadline = None;
        loop {
            check_retry_deadline(deadline)?;
            let file = self.current_file().await?;
            match reader.read_bytes(file.as_ref(), offset, len).await {
                Ok(bytes) => return Ok(bytes),
                // Allocate the larger recovery future only on failure.
                Err(error) => {
                    Box::pin(self.reopen_after_error(&file, error, &mut deadline)).await?
                }
            }
        }
    }

    async fn read_into<R: FileReader>(
        &self,
        reader: R,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        let mut deadline = None;
        loop {
            check_retry_deadline(deadline)?;
            let file = self.current_file().await?;
            match reader.read(file.as_ref(), offset, dst).await {
                Ok(size) => return Ok(size),
                Err(error) => {
                    Box::pin(self.reopen_after_error(&file, error, &mut deadline)).await?
                }
            }
        }
    }
}

fn disconnected(error: &anyhow::Error) -> bool {
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        error.kind() == io::ErrorKind::NotConnected || error.raw_os_error() == Some(libc::ESTALE)
    })
}

fn not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|error| error.kind() == io::ErrorKind::NotFound)
}

fn check_retry_deadline(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(
            io::Error::new(io::ErrorKind::TimedOut, "lower read recovery timed out").into(),
        );
    }
    Ok(())
}

/// Wrap a remote or buffered local source. Use `SwitchFile::open_local` for
/// local files whose direct-I/O option must be retained across reopens.
pub async fn new_switch_file(
    source: Arc<dyn VirtualFile>,
    local: bool,
    file_path: Option<&str>,
) -> Result<Arc<SwitchFile>> {
    let file = open_zfile(source, !local, file_path, 1)
        .await
        .map_err(|error| match file_path {
            Some(path) => error.context(format!("open source file as zfile `{path}` failed")),
            None => error,
        })?;
    Ok(Arc::new(
        SwitchFile::from_opened(file, local, file_path.map(Path::new), false).await?,
    ))
}

#[async_trait]
impl VirtualFile for SwitchFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.read_bytes(DirectRead, offset, len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.read_into(DirectRead, offset, dst).await
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
        // Local lowers are immutable. In particular, restack must not fstat
        // the disconnected handle before the first read can reopen it.
        if let Some(size) = self.state.read().await.local_size {
            return Ok(size);
        }
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
        Box::pin(self.read_bytes(CtxRead { ctx }, offset, len))
    }

    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.read_into(CtxRead { ctx }, offset, dst))
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
pub(crate) mod tests {
    use super::super::local::LocalFile;
    use super::super::tar::new_tar_file_create;
    use super::*;
    use crate::compression::zfile::{zfile_compress, CompressArgs, CompressOptions};
    use std::sync::atomic::{AtomicI32, Ordering};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    struct MemoryFile {
        data: Mutex<Vec<u8>>,
    }

    pub(crate) struct SeverableFile {
        inner: Arc<dyn VirtualFile>,
        pub(crate) errno: AtomicI32,
    }

    impl SeverableFile {
        fn check(&self) -> Result<()> {
            match self.errno.load(Ordering::SeqCst) {
                0 => Ok(()),
                errno => Err(io::Error::from_raw_os_error(errno).into()),
            }
        }
    }

    #[async_trait]
    impl VirtualFile for SeverableFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            self.check()?;
            self.inner.read_at(offset, len).await
        }

        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            self.check()?;
            self.inner.write_at(offset, data).await
        }

        async fn size(&self) -> Result<u64> {
            self.check()?;
            self.inner.size().await
        }
    }

    pub(crate) async fn severable_local(
        path: &Path,
    ) -> Result<(Arc<SwitchFile>, Arc<SeverableFile>)> {
        let source = Arc::new(SeverableFile {
            inner: Arc::new(LocalFile::open_ro(path)?),
            errno: AtomicI32::new(0),
        });
        let tar = new_tar_file_adaptor(source.clone()).await?;
        let file = try_open_zfile(tar, false, None).await?;
        let switch = Arc::new(SwitchFile::from_opened(file, true, Some(path), false).await?);
        Ok((switch, source))
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
            "http://registry/blob"
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
            local_path.to_string_lossy().as_ref()
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

    #[tokio::test]
    async fn disconnected_local_reopens_raw_tar_and_zfile() -> Result<()> {
        let dir = tempdir()?;
        let data = sample_bytes(0x62, 32 * 1024);
        for format in ["raw", "tar", "zfile"] {
            let path = dir.path().join(format);
            match format {
                "raw" => std::fs::write(&path, &data)?,
                "tar" => create_plain_tar_file(&path, &data).await?,
                _ => create_zfile_tar_file(&path, &data).await?,
            }
            let (switch, old) = severable_local(&path).await?;
            old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
            assert_eq!(switch.size().await?, data.len() as u64);
            assert_eq!(
                switch.read_at(16384, 4096).await?.as_ref(),
                &data[16384..20480]
            );
            // A second disconnection exercises the into-buffer entry point.
            let failed = Arc::new(SeverableFile {
                inner: switch.current_file().await?,
                errno: AtomicI32::new(libc::ESTALE),
            });
            switch.state.write().await.local_file = Some(failed);
            let mut dst = vec![0; 4096];
            assert_eq!(switch.read_at_into(24576, &mut dst).await?, dst.len());
            assert_eq!(dst, data[24576..28672]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_read_waits_for_path_and_keeps_inflight_file_alive() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        let hidden = dir.path().join("hidden");
        let data = sample_bytes(0x55, 8192);
        std::fs::write(&path, &data)?;
        let (switch, old) = severable_local(&path).await?;
        let before = switch.current_file().await?;
        std::fs::rename(&path, &hidden)?;
        old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        let restore_path = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            std::fs::rename(&hidden, &path)
        };
        let (read, restored) = tokio::join!(switch.read_at(4096, 4096), restore_path);
        restored?;
        assert_eq!(read?.as_ref(), &data[4096..]);
        assert!(!Arc::ptr_eq(&before, &switch.current_file().await?));
        old.errno.store(0, Ordering::SeqCst);
        assert_eq!(before.read_at(0, 4096).await?.as_ref(), &data[..4096]);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_and_late_failures_reuse_the_reopened_generation() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        std::fs::write(&path, sample_bytes(0x44, 8192))?;
        let (switch, old) = severable_local(&path).await?;
        let failed = switch.current_file().await?;
        old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        let generations = futures_util::future::join_all((0..8).map(|_| async {
            switch
                .reopen_after_error(
                    &failed,
                    io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                    &mut None,
                )
                .await?;
            switch.current_file().await
        }))
        .await;
        let current = switch.current_file().await?;
        for generation in generations {
            assert!(Arc::ptr_eq(&current, &generation?));
        }
        let mut deadline = None;
        switch
            .reopen_after_error(
                &failed,
                io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                &mut deadline,
            )
            .await?;
        assert!(
            deadline.is_some(),
            "late failures must start the same retry budget"
        );
        assert!(Arc::ptr_eq(&current, &switch.current_file().await?));
        let expired_at = Instant::now() - Duration::from_secs(1);
        let mut expired = Some(expired_at);
        switch
            .reopen_after_error(
                &failed,
                io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                &mut expired,
            )
            .await?;
        // The caller checks before reading again; recovery must not renew
        // an expired budget just because another reader changed the file.
        assert_eq!(expired, Some(expired_at));
        Ok(())
    }

    #[tokio::test]
    async fn wrong_size_and_non_connection_errors_are_not_recovered() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        std::fs::write(&path, sample_bytes(0x22, 8192))?;
        let (switch, old) = severable_local(&path).await?;
        let before = switch.current_file().await?;
        for errno in [libc::EIO, libc::ENOENT, libc::EACCES] {
            old.errno.store(errno, Ordering::SeqCst);
            let error = switch.read_at(4096, 512).await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().raw_os_error(),
                Some(errno)
            );
            assert!(Arc::ptr_eq(&before, &switch.current_file().await?));
        }
        let replacement = dir.path().join("other");
        std::fs::write(&replacement, sample_bytes(0x33, 16384))?;
        std::fs::rename(replacement, &path)?;
        old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        assert!(switch
            .read_at(4096, 512)
            .await
            .unwrap_err()
            .to_string()
            .contains("changed size"));
        assert!(Arc::ptr_eq(&before, &switch.current_file().await?));
        Ok(())
    }

    #[tokio::test]
    async fn remote_failure_does_not_wait_for_local_switch() -> Result<()> {
        use futures_util::FutureExt;
        let source = Arc::new(SeverableFile {
            inner: Arc::new(MemoryFile::new(sample_bytes(0x11, 8192))),
            errno: AtomicI32::new(0),
        });
        let switch = new_switch_file(source.clone(), false, Some("http://registry/blob")).await?;
        let _switch_in_progress = switch.reopen_gate.lock().await;
        source.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        let error = switch
            .read_at(4096, 512)
            .now_or_never()
            .expect("a remote connection error must not wait for the local switch gate")
            .unwrap_err();
        assert!(disconnected(&error));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_budget_includes_gate_wait_and_missing_path() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        std::fs::write(&path, sample_bytes(0x77, 8192))?;
        let (switch, old) = severable_local(&path).await?;
        let failed = switch.current_file().await?;
        old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        std::fs::rename(&path, dir.path().join("hidden"))?;
        let mut deadline = Some(Instant::now() + Duration::from_millis(30));
        let error = switch
            .reopen_after_error(
                &failed,
                io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                &mut deadline,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(Arc::ptr_eq(&failed, &switch.current_file().await?));
        let _gate = switch.reopen_gate.lock().await;
        let mut deadline = Some(Instant::now() + Duration::from_millis(20));
        let error = switch
            .reopen_after_error(
                &failed,
                io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                &mut deadline,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn abandoned_reopen_keeps_worker_gate(cancel: bool) -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        let data = sample_bytes(0x38, 8192);
        std::fs::write(&path, &data)?;
        let (switch, old) = severable_local(&path).await?;
        let failed = switch.current_file().await?;
        old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
        let count_fds = || -> Result<usize> {
            Ok(std::fs::read_dir("/proc/self/fd")?
                .filter_map(std::result::Result::ok)
                .filter(|entry| std::fs::read_link(entry.path()).ok().as_ref() == Some(&path))
                .count())
        };
        let original_fds = count_fds()?;

        // With two blocking threads, this occupies one and the real candidate
        // occupies the other. Its tar probe's LocalFile::read_at offload then
        // waits in the queue. Dropping release also unblocks on test failure.
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = wait.recv();
        });
        tokio::time::timeout(Duration::from_secs(2), ready).await??;
        let caller = tokio::spawn({
            let switch = switch.clone();
            let failed = failed.clone();
            async move {
                let budget = if cancel {
                    REOPEN_TIMEOUT
                } else {
                    Duration::from_millis(500)
                };
                switch
                    .reopen_after_error(
                        &failed,
                        io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                        &mut Some(Instant::now() + budget),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while count_fds()? != original_fds + 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        if cancel {
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
        } else {
            let error = tokio::time::timeout(Duration::from_secs(2), caller)
                .await??
                .unwrap_err();
            assert!(error.to_string().contains("timed out"));
        }
        assert!(switch.reopen_gate.try_lock().is_err());
        assert!(Arc::ptr_eq(&failed, &switch.current_file().await?));

        let followers = futures_util::future::join_all((0..4).map(|_| async {
            switch
                .reopen_after_error(
                    &failed,
                    io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                    &mut Some(Instant::now() + Duration::from_millis(20)),
                )
                .await
        }));
        for result in tokio::time::timeout(Duration::from_secs(2), followers).await? {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("timed out waiting to reopen"));
        }
        assert_eq!(count_fds()?, original_fds + 1);
        assert!(switch.reopen_gate.try_lock().is_err());

        drop(release);
        tokio::time::timeout(Duration::from_secs(2), blocker).await??;
        drop(tokio::time::timeout(Duration::from_secs(2), switch.reopen_gate.lock()).await?);
        assert_eq!(count_fds()?, original_fds);
        // The real candidate completed successfully, but its abandoned caller
        // cannot publish it. A fresh read must still recover the old handle.
        assert!(Arc::ptr_eq(&failed, &switch.current_file().await?));
        let bytes =
            tokio::time::timeout(Duration::from_secs(2), switch.read_at(4096, 4096)).await??;
        assert_eq!(bytes.as_ref(), &data[4096..]);
        assert!(!Arc::ptr_eq(&failed, &switch.current_file().await?));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timed_out_reopen_keeps_worker_gate() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()?;
        runtime.block_on(abandoned_reopen_keeps_worker_gate(false))
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancelled_reopen_keeps_worker_gate() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()?;
        runtime.block_on(abandoned_reopen_keeps_worker_gate(true))
    }

    #[test]
    fn queued_reopen_expires_before_opening() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()?;
        runtime.block_on(async {
            let dir = tempdir()?;
            let path = dir.path().join("lower");
            std::fs::write(&path, sample_bytes(0x57, 8192))?;
            let (switch, old) = severable_local(&path).await?;
            let failed = switch.current_file().await?;
            old.errno.store(libc::ENOTCONN, Ordering::SeqCst);

            let mut releases = Vec::new();
            for _ in 0..2 {
                let (release, wait) = std::sync::mpsc::channel::<()>();
                let (started, ready) = tokio::sync::oneshot::channel();
                tokio::task::spawn_blocking(move || {
                    let _ = started.send(());
                    let _ = wait.recv();
                });
                // Dropping the senders also releases both threads on failure.
                releases.push(release);
                tokio::time::timeout(Duration::from_secs(2), ready).await??;
            }

            let mut deadline = Some(Instant::now() + Duration::from_millis(20));
            let error = tokio::time::timeout(
                Duration::from_secs(2),
                switch.reopen_after_error(
                    &failed,
                    io::Error::from_raw_os_error(libc::ENOTCONN).into(),
                    &mut deadline,
                ),
            )
            .await?
            .unwrap_err();
            assert!(error.to_string().contains("timed out reopening"));
            assert!(switch.reopen_gate.try_lock().is_err());

            // Run the queued worker, keeping the other thread occupied. An
            // unnecessary open would block its tar probe and retain the gate.
            drop(releases.pop());
            drop(tokio::time::timeout(Duration::from_secs(2), switch.reopen_gate.lock()).await?);
            assert!(Arc::ptr_eq(&failed, &switch.current_file().await?));
            Ok(())
        })
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reopen_preserves_direct_io_on_the_new_descriptor() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("lower");
        let data = sample_bytes(0x31, 8192);
        std::fs::write(&path, &data)?;
        let switch = SwitchFile::open_local(&path, true).await?;
        let failed = Arc::new(SeverableFile {
            inner: switch.current_file().await?,
            errno: AtomicI32::new(libc::ENOTCONN),
        });
        switch.state.write().await.local_file = Some(failed.clone());
        assert_eq!(switch.read_at(4096, 4096).await?.as_ref(), &data[4096..]);
        let mut descriptors = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            let entry = entry?;
            if std::fs::read_link(entry.path()).ok().as_ref() != Some(&path) {
                continue;
            }
            let info =
                std::fs::read_to_string(Path::new("/proc/self/fdinfo").join(entry.file_name()))?;
            let flags = info
                .lines()
                .find_map(|line| line.strip_prefix("flags:\t"))
                .unwrap();
            assert_ne!(u32::from_str_radix(flags, 8)? & libc::O_DIRECT as u32, 0);
            descriptors += 1;
        }
        assert_eq!(
            descriptors, 2,
            "both the in-flight and new descriptor must retain O_DIRECT"
        );
        Ok(())
    }

    #[cfg(feature = "io-uring")]
    #[tokio::test]
    async fn disconnected_context_reads_use_the_reopened_file() -> Result<()> {
        use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};
        let ring: AsyncIoRing = AsyncIoRingBuilder::new().sqe_entries(16).build()?;
        tokio::task::LocalSet::new()
            .run_until(async {
                let completing = tokio::task::spawn_local({
                    let ring = ring.clone();
                    async move { ring.handle_completion().await }
                });
                let submitting = tokio::task::spawn_local({
                    let ring = ring.clone();
                    async move {
                        loop {
                            ring.borrow().submit().expect("submit test reads");
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                });
                let result = async {
                    let dir = tempdir()?;
                    let path = dir.path().join("lower");
                    let data = sample_bytes(0x29, 16384);
                    std::fs::write(&path, &data)?;
                    let (switch, old) = severable_local(&path).await?;
                    old.errno.store(libc::ENOTCONN, Ordering::SeqCst);
                    assert_eq!(
                        switch
                            .read_at_with_ctx(IoCtx::new(&ring), 4096, 4096)
                            .await?
                            .as_ref(),
                        &data[4096..8192]
                    );
                    let failed = Arc::new(SeverableFile {
                        inner: switch.current_file().await?,
                        errno: AtomicI32::new(libc::ESTALE),
                    });
                    switch.state.write().await.local_file = Some(failed);
                    let mut dst = vec![0; 4096];
                    assert_eq!(
                        switch
                            .read_at_into_with_ctx(IoCtx::new(&ring), 12288, &mut dst)
                            .await?,
                        dst.len()
                    );
                    assert_eq!(dst, data[12288..]);
                    Ok(())
                }
                .await;
                submitting.abort();
                completing.abort();
                result
            })
            .await
    }
}
