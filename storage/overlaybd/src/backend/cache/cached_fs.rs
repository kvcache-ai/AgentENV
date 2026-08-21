use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::errno::Errno;
use parking_lot::RwLock;
use std::ffi::CString;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use super::full_file_cache::cache_pool::FileCacheBackend;
use super::full_file_cache::cache_store::CachedFile;
use super::O_CACHE_ONLY;
use crate::backend::local::LocalFile;
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use crate::sys;

#[async_trait]
pub trait CachedFsSource: Send + Sync {
    async fn open(&self, pathname: &str, flags: u32, mode: u32) -> Result<Arc<dyn VirtualFile>>;

    async fn mkdir(&self, _pathname: &str, _mode: u32) -> Result<()> {
        bail!("mkdir is not supported for this cached source");
    }

    async fn rmdir(&self, _pathname: &str) -> Result<()> {
        bail!("rmdir is not supported for this cached source");
    }

    async fn readlink(&self, _pathname: &str) -> Result<PathBuf> {
        bail!("readlink is not supported for this cached source");
    }

    async fn stat(&self, _pathname: &str) -> Result<std::fs::Metadata> {
        bail!("stat is not supported for this cached source");
    }

    async fn lstat(&self, _pathname: &str) -> Result<std::fs::Metadata> {
        bail!("lstat is not supported for this cached source");
    }

    async fn statfs(&self, _pathname: &str) -> Result<libc::statfs> {
        bail!("statfs is not supported for this cached source");
    }

    async fn statvfs(&self, _pathname: &str) -> Result<libc::statvfs> {
        bail!("statvfs is not supported for this cached source");
    }

    async fn opendir(&self, _pathname: &str) -> Result<Vec<String>> {
        bail!("opendir is not supported for this cached source");
    }

    async fn unlink(&self, _pathname: &str) -> Result<()> {
        bail!("unlink is not supported for this cached source");
    }

    async fn access(&self, _pathname: &str, _mode: i32) -> Result<()> {
        bail!("access is not supported for this cached source");
    }

    async fn getxattr(&self, _path: &str, _name: &str) -> Result<Vec<u8>> {
        bail!("getxattr is not supported for this cached source");
    }

    async fn listxattr(&self, _path: &str) -> Result<Vec<String>> {
        bail!("listxattr is not supported for this cached source");
    }

    async fn setxattr(&self, _path: &str, _name: &str, _value: &[u8], _flags: i32) -> Result<()> {
        bail!("setxattr is not supported for this cached source");
    }

    async fn removexattr(&self, _path: &str, _name: &str) -> Result<()> {
        bail!("removexattr is not supported for this cached source");
    }
}

#[derive(Debug, Clone)]
pub struct LocalFsSource {
    root: PathBuf,
}

impl LocalFsSource {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn resolve_path(&self, pathname: &str) -> PathBuf {
        let relative = pathname.trim_start_matches('/');
        if relative.is_empty() {
            self.root.clone()
        } else {
            self.root.join(relative)
        }
    }

    fn path_to_cstring(path: &Path) -> Result<CString> {
        CString::new(path.as_os_str().as_bytes()).context("path contains interior NUL byte")
    }
}

#[async_trait]
impl CachedFsSource for LocalFsSource {
    async fn open(&self, pathname: &str, flags: u32, mode: u32) -> Result<Arc<dyn VirtualFile>> {
        let path = self.resolve_path(pathname);
        let oflags = i32::try_from(flags).context("open flags overflow i32")?;
        let accmode = oflags & libc::O_ACCMODE;
        let mut read = accmode != libc::O_WRONLY;
        let mut write = accmode == libc::O_WRONLY || accmode == libc::O_RDWR;
        if !read && !write {
            // Keep sane default if caller passes cache control flags only.
            read = true;
            write = false;
        }
        let create = (oflags & libc::O_CREAT) != 0;
        let truncate = (oflags & libc::O_TRUNC) != 0;
        if create {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let file = LocalFile::builder()
            .read(read)
            .write(write)
            .create(create)
            .truncate(truncate)
            .mode(mode)
            .open(&path)?;
        Ok(Arc::new(file))
    }

    async fn mkdir(&self, pathname: &str, mode: u32) -> Result<()> {
        let path = self.resolve_path(pathname);
        tokio::fs::create_dir(&path).await?;
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(mode);
            tokio::fs::set_permissions(path, perm).await?;
        }
        Ok(())
    }

    async fn rmdir(&self, pathname: &str) -> Result<()> {
        let path = self.resolve_path(pathname);
        tokio::fs::remove_dir(path).await?;
        Ok(())
    }

    async fn readlink(&self, pathname: &str) -> Result<PathBuf> {
        let path = self.resolve_path(pathname);
        Ok(tokio::fs::read_link(path).await?)
    }

    async fn stat(&self, pathname: &str) -> Result<std::fs::Metadata> {
        let path = self.resolve_path(pathname);
        Ok(tokio::fs::metadata(path).await?)
    }

    async fn lstat(&self, pathname: &str) -> Result<std::fs::Metadata> {
        let path = self.resolve_path(pathname);
        Ok(tokio::fs::symlink_metadata(path).await?)
    }

    async fn statfs(&self, pathname: &str) -> Result<libc::statfs> {
        let path = self.resolve_path(pathname);
        let cpath = Self::path_to_cstring(&path)?;
        let mut st = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: cpath is a valid C string and `st` points to writable memory.
        let ret = unsafe { libc::statfs(cpath.as_ptr(), st.as_mut_ptr()) };
        if ret != 0 {
            return Err(Errno::last().into());
        }
        // SAFETY: libc::statfs succeeded and fully initialized `st`.
        Ok(unsafe { st.assume_init() })
    }

    async fn statvfs(&self, pathname: &str) -> Result<libc::statvfs> {
        let path = self.resolve_path(pathname);
        let cpath = Self::path_to_cstring(&path)?;
        let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: cpath is a valid C string and `st` points to writable memory.
        let ret = unsafe { libc::statvfs(cpath.as_ptr(), st.as_mut_ptr()) };
        if ret != 0 {
            return Err(Errno::last().into());
        }
        // SAFETY: libc::statvfs succeeded and fully initialized `st`.
        Ok(unsafe { st.assume_init() })
    }

    async fn opendir(&self, pathname: &str) -> Result<Vec<String>> {
        let path = self.resolve_path(pathname);
        let mut out = Vec::new();
        let mut rd = tokio::fs::read_dir(path).await?;
        while let Some(entry) = rd.next_entry().await? {
            out.push(entry.file_name().to_string_lossy().to_string());
        }
        out.sort_unstable();
        Ok(out)
    }

    async fn unlink(&self, pathname: &str) -> Result<()> {
        let path = self.resolve_path(pathname);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn access(&self, pathname: &str, mode: i32) -> Result<()> {
        let path = self.resolve_path(pathname);
        let cpath = Self::path_to_cstring(&path)?;
        // SAFETY: cpath is NUL-terminated and lives across the call.
        let ret = unsafe { libc::access(cpath.as_ptr(), mode) };
        if ret == 0 {
            return Ok(());
        }
        Err(Errno::last().into())
    }

    async fn getxattr(&self, path: &str, name: &str) -> Result<Vec<u8>> {
        let path = self.resolve_path(path);
        let cpath = Self::path_to_cstring(&path)?;
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        sys::getxattr(&cpath, &cname).with_context(|| format!("getxattr {name} on {path:?}"))
    }

    async fn listxattr(&self, path: &str) -> Result<Vec<String>> {
        let path = self.resolve_path(path);
        let cpath = Self::path_to_cstring(&path)?;
        sys::listxattr(&cpath).with_context(|| format!("listxattr on {path:?}"))
    }

    async fn setxattr(&self, path: &str, name: &str, value: &[u8], flags: i32) -> Result<()> {
        let path = self.resolve_path(path);
        let cpath = Self::path_to_cstring(&path)?;
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        sys::setxattr(&cpath, &cname, value, flags)
            .with_context(|| format!("setxattr {name} on {path:?}"))
    }

    async fn removexattr(&self, path: &str, name: &str) -> Result<()> {
        let path = self.resolve_path(path);
        let cpath = Self::path_to_cstring(&path)?;
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        sys::removexattr(&cpath, &cname).with_context(|| format!("removexattr {name} on {path:?}"))
    }
}

/// A lazy-opened readonly source file.
struct LazySourceFile {
    source_fs: Arc<dyn CachedFsSource>,
    pathname: String,
    mode: u32,
    file: AsyncMutex<Option<Arc<dyn VirtualFile>>>,
}

impl LazySourceFile {
    fn new(source_fs: Arc<dyn CachedFsSource>, pathname: impl Into<String>, mode: u32) -> Self {
        Self {
            source_fs,
            pathname: pathname.into(),
            mode,
            file: AsyncMutex::new(None),
        }
    }

    fn source_open_flags(&self) -> u32 {
        // NOTE: source file is read only
        libc::O_RDONLY as u32
    }

    async fn open_if_needed(&self) -> Result<Arc<dyn VirtualFile>> {
        let mut guard = self.file.lock().await;
        if let Some(file) = guard.as_ref() {
            return Ok(file.clone());
        }
        let file = self
            .source_fs
            .open(&self.pathname, self.source_open_flags(), self.mode)
            .await?;
        *guard = Some(file.clone());
        Ok(file)
    }
}

#[async_trait]
impl VirtualFile for LazySourceFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.open_if_needed().await?.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.open_if_needed().await?.write_at(offset, data).await
    }

    async fn size(&self) -> Result<u64> {
        self.open_if_needed().await?.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.open_if_needed().await?.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.open_if_needed().await?.sync().await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        self.open_if_needed().await?.seek_data(offset).await
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        self.open_if_needed().await?.seek_hole(offset).await
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.open_if_needed().await?.evict_range(offset, len).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.open_if_needed().await?.evict_all().await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        self.open_if_needed().await?.fgetxattr(name).await
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        self.open_if_needed().await?.flistxattr().await
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        self.open_if_needed()
            .await?
            .fsetxattr(name, value, flags)
            .await
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        self.open_if_needed().await?.fremovexattr(name).await
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let file = self.open_if_needed().await?;
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
            let file = self.open_if_needed().await?;
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
            let file = self.open_if_needed().await?;
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
            let file = self.open_if_needed().await?;
            file.write_bytes_at_with_ctx(ctx, offset, data).await
        })
    }
}

#[derive(Clone)]
pub struct CachedFs {
    source_fs: Arc<RwLock<Option<Arc<dyn CachedFsSource>>>>,
    file_cache_pool: Arc<RwLock<FileCacheBackend>>,
}

impl CachedFs {
    pub fn new(
        source_fs: Option<Arc<dyn CachedFsSource>>,
        file_cache_pool: FileCacheBackend,
    ) -> Self {
        Self {
            source_fs: Arc::new(RwLock::new(source_fs)),
            file_cache_pool: Arc::new(RwLock::new(file_cache_pool)),
        }
    }

    pub async fn open_with_mode(
        &self,
        pathname: &str,
        flags: u32,
        mode: u32,
    ) -> Result<Arc<CachedFile>> {
        let cflags = flags & O_CACHE_ONLY;
        let pool = self.get_pool();
        if let Some(src_fs) = self.get_source() {
            let source_mode = if mode == 0 { 0o644 } else { mode };
            let source = Arc::new(LazySourceFile::new(src_fs, pathname, source_mode));
            pool.open_src_name_with_flags(pathname, source, cflags)
                .await
        } else {
            let initial_size = pool.cached_size_by_src_name(pathname).unwrap_or(0);
            pool.open_cache_only_src_name_with_flags(pathname, initial_size, cflags | O_CACHE_ONLY)
                .await
        }
    }

    pub async fn open(&self, pathname: &str, flags: u32) -> Result<Arc<CachedFile>> {
        self.open_with_mode(pathname, flags, 0).await
    }

    pub async fn rename(&self, oldname: &str, newname: &str) -> Result<()> {
        self.get_pool().rename_src_name(oldname, newname).await
    }

    pub async fn mkdir(&self, pathname: &str, mode: u32) -> Result<()> {
        if let Some(src_fs) = self.get_source() {
            src_fs.mkdir(pathname, mode).await
        } else {
            bail!("mkdir is not supported without source fs");
        }
    }

    pub async fn rmdir(&self, pathname: &str) -> Result<()> {
        if let Some(src_fs) = self.get_source() {
            src_fs.rmdir(pathname).await
        } else {
            bail!("rmdir is not supported without source fs");
        }
    }

    pub async fn readlink(&self, pathname: &str) -> Result<PathBuf> {
        if let Some(src_fs) = self.get_source() {
            src_fs.readlink(pathname).await
        } else {
            bail!("readlink is not supported without source fs");
        }
    }

    pub async fn stat(&self, pathname: &str) -> Result<std::fs::Metadata> {
        if let Some(src_fs) = self.get_source() {
            src_fs.stat(pathname).await
        } else {
            bail!("stat is not supported without source fs");
        }
    }

    pub async fn lstat(&self, pathname: &str) -> Result<std::fs::Metadata> {
        if let Some(src_fs) = self.get_source() {
            src_fs.lstat(pathname).await
        } else {
            bail!("lstat is not supported without source fs");
        }
    }

    pub async fn statfs(&self, pathname: &str) -> Result<libc::statfs> {
        if let Some(src_fs) = self.get_source() {
            src_fs.statfs(pathname).await
        } else {
            bail!("statfs is not supported without source fs");
        }
    }

    pub async fn statvfs(&self, pathname: &str) -> Result<libc::statvfs> {
        if let Some(src_fs) = self.get_source() {
            src_fs.statvfs(pathname).await
        } else {
            bail!("statvfs is not supported without source fs");
        }
    }

    pub async fn opendir(&self, pathname: &str) -> Result<Vec<String>> {
        if let Some(src_fs) = self.get_source() {
            src_fs.opendir(pathname).await
        } else {
            bail!("opendir is not supported without source fs");
        }
    }

    pub async fn unlink(&self, pathname: &str) -> Result<()> {
        let evict_ret = self.get_pool().evict_src_name(pathname).await;
        if let Some(src_fs) = self.get_source() {
            // Align with C++: return source unlink status when source FS exists.
            return src_fs.unlink(pathname).await;
        }
        evict_ret
    }

    pub async fn access(&self, pathname: &str, mode: i32) -> Result<()> {
        if let Some(src_fs) = self.get_source() {
            return src_fs.access(pathname, mode).await;
        }
        if self.get_pool().contains_src_name(pathname) {
            Ok(())
        } else {
            bail!("cache entry not found for {pathname}");
        }
    }

    pub fn get_source(&self) -> Option<Arc<dyn CachedFsSource>> {
        self.source_fs.read().clone()
    }

    pub fn set_source(&self, source_fs: Option<Arc<dyn CachedFsSource>>) {
        *self.source_fs.write() = source_fs;
    }

    fn get_pool(&self) -> FileCacheBackend {
        self.file_cache_pool.read().clone()
    }

    pub async fn getxattr(&self, path: &str, name: &str) -> Result<Vec<u8>> {
        if let Some(src_fs) = self.get_source() {
            src_fs.getxattr(path, name).await
        } else {
            bail!("getxattr is not supported without source fs");
        }
    }

    pub async fn listxattr(&self, path: &str) -> Result<Vec<String>> {
        if let Some(src_fs) = self.get_source() {
            src_fs.listxattr(path).await
        } else {
            bail!("listxattr is not supported without source fs");
        }
    }

    pub async fn setxattr(&self, path: &str, name: &str, value: &[u8], flags: i32) -> Result<()> {
        if let Some(src_fs) = self.get_source() {
            src_fs.setxattr(path, name, value, flags).await
        } else {
            bail!("setxattr is not supported without source fs");
        }
    }

    pub async fn removexattr(&self, path: &str, name: &str) -> Result<()> {
        if let Some(src_fs) = self.get_source() {
            src_fs.removexattr(path, name).await
        } else {
            bail!("removexattr is not supported without source fs");
        }
    }
}

impl fmt::Debug for CachedFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let has_source = self.source_fs.read().is_some();
        f.debug_struct("CachedFs")
            .field("has_source", &has_source)
            .finish_non_exhaustive()
    }
}
