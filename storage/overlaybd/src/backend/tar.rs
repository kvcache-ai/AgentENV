use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub const T_BLOCKSIZE: u64 = 512;
const T_BLOCKSIZE_USIZE: usize = T_BLOCKSIZE as usize;
const TMAGIC: &[u8; 5] = b"ustar";
const TVERSION: &[u8; 2] = b"00";
const TMAGIC_EMPTY: &[u8; 5] = b"xxtar";
const TVERSION_EMPTY: &[u8; 2] = b"xx";
const GNU_LONGNAME_TYPE: u8 = b'L';
const GNU_LONGLINK_TYPE: u8 = b'K';
const PAX_HEADER: u8 = b'x';
const PAX_GLOBAL_HEADER: u8 = b'g';
const PAX_PATH: &str = "path";
const PAX_LINKPATH: &str = "linkpath";
const PAX_SIZE: &str = "size";
const REGTYPE: u8 = b'0';

// tarfile and tarfs are not complete implementation of tar.
// only for overlaybd remote blob, which stores one blob file with tar header and trailer.
// used to skip header for file I/O.

#[derive(Debug, Clone, Default)]
pub struct TarBackend;

impl TarBackend {
    pub fn new() -> Self {
        Self
    }

    pub async fn open(&self, file: Arc<dyn VirtualFile>) -> Result<Arc<dyn VirtualFile>> {
        new_tar_file_adaptor(file).await
    }

    pub async fn open_rw(&self, file: Arc<dyn VirtualFile>) -> Result<Arc<dyn VirtualFile>> {
        open_tar_file_rw(file).await
    }

    pub async fn create(&self, file: Arc<dyn VirtualFile>) -> Result<Arc<TarFile>> {
        new_tar_file_create(file).await
    }
}

pub struct TarFile {
    file: Arc<dyn VirtualFile>,
    tar: TarCore,
    base_offset: u64,
    size: AtomicU64,
    new_tar: AtomicBool,
}

impl TarFile {
    async fn new(file: Arc<dyn VirtualFile>, create: bool) -> Result<Self> {
        if create {
            mark_new_tar(file.as_ref()).await?;
        }
        let tar = TarCore::read_header(file.clone()).await?;
        let size = tar.get_size();
        let base_offset = if tar.has_pax_header() {
            3 * T_BLOCKSIZE
        } else {
            T_BLOCKSIZE
        };
        Ok(Self {
            file,
            tar,
            base_offset,
            size: AtomicU64::new(size),
            new_tar: AtomicBool::new(create),
        })
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn payload_size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_new_tar(&self) -> bool {
        self.new_tar.load(Ordering::Relaxed)
    }

    async fn rewrite_header_trailer(&self) -> Result<()> {
        if !self.is_new_tar() {
            return Ok(());
        }
        let size = self.payload_size();
        write_header_trailer(self.file.as_ref(), size).await
    }

    pub async fn close(&self) -> Result<()> {
        if self.is_new_tar() {
            self.rewrite_header_trailer().await?;
        }
        self.file.sync().await
    }
}

impl fmt::Debug for TarFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TarFile")
            .field("base_offset", &self.base_offset)
            .field("size", &self.payload_size())
            .field("has_pax_header", &self.tar.has_pax_header())
            .field("is_new_tar", &self.is_new_tar())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl VirtualFile for TarFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let size = self.payload_size();
        if len == 0 || offset >= size {
            return Ok(Bytes::new());
        }
        let max_len = size.saturating_sub(offset).min(len as u64) as usize;
        self.file.read_at(offset + self.base_offset, max_len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let size = self.payload_size();
        if dst.is_empty() || offset >= size {
            return Ok(0);
        }
        let max_len = size.saturating_sub(offset).min(dst.len() as u64) as usize;
        self.file
            .read_at_into(offset + self.base_offset, &mut dst[..max_len])
            .await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let written = self.file.write_at(offset + self.base_offset, data).await?;
        if written > 0 {
            let end = offset.saturating_add(written as u64);
            self.size.fetch_max(end, Ordering::Relaxed);
        }
        Ok(written)
    }

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        let written = self
            .file
            .write_bytes_at(offset + self.base_offset, data)
            .await?;
        if written > 0 {
            let end = offset.saturating_add(written as u64);
            self.size.fetch_max(end, Ordering::Relaxed);
        }
        Ok(written)
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let size = self.payload_size();
            if len == 0 || offset >= size {
                return Ok(Bytes::new());
            }
            let max_len = size.saturating_sub(offset).min(len as u64) as usize;
            self.file
                .read_at_with_ctx(ctx, offset + self.base_offset, max_len)
                .await
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
            let size = self.payload_size();
            if dst.is_empty() || offset >= size {
                return Ok(0);
            }
            let max_len = size.saturating_sub(offset).min(dst.len() as u64) as usize;
            self.file
                .read_at_into_with_ctx(ctx, offset + self.base_offset, &mut dst[..max_len])
                .await
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
            let written = self
                .file
                .write_at_with_ctx(ctx, offset + self.base_offset, data)
                .await?;
            if written > 0 {
                let end = offset.saturating_add(written as u64);
                self.size.fetch_max(end, Ordering::Relaxed);
            }
            Ok(written)
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
            let written = self
                .file
                .write_bytes_at_with_ctx(ctx, offset + self.base_offset, data)
                .await?;
            if written > 0 {
                let end = offset.saturating_add(written as u64);
                self.size.fetch_max(end, Ordering::Relaxed);
            }
            Ok(written)
        })
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.payload_size())
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.file.truncate(size + self.base_offset).await?;
        self.size.store(size, Ordering::Relaxed);
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        self.file.sync().await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        let size = self.payload_size();
        let physical = self.file.seek_data(offset + self.base_offset).await?;
        let Some(physical) = physical else {
            return Ok(None);
        };
        ensure!(
            physical >= self.base_offset,
            "seek_data returned offset before tar payload"
        );
        let logical = physical - self.base_offset;
        if logical >= size {
            return Ok(None);
        }
        Ok(Some(logical))
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        let size = self.payload_size();
        let physical = self.file.seek_hole(offset + self.base_offset).await?;
        let Some(physical) = physical else {
            return Ok(None);
        };
        ensure!(
            physical >= self.base_offset,
            "seek_hole returned offset before tar payload"
        );
        Ok(Some((physical - self.base_offset).min(size)))
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.file.evict_range(offset + self.base_offset, len).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.file.evict_all().await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        self.file.fgetxattr(name).await
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        self.file.flistxattr().await
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        self.file.fsetxattr(name, value, flags).await
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        self.file.fremovexattr(name).await
    }
}

#[derive(Debug, Clone)]
struct TarCore {
    header: TarHeader,
    pax: Option<PaxHeader>,
}

impl TarCore {
    async fn read_header(file: Arc<dyn VirtualFile>) -> Result<Self> {
        let mut offset = 0u64;
        let mut pax = None;

        let (mut header, mut next_offset) = read_header_internal(file.as_ref(), offset).await?;
        offset = next_offset;

        while matches!(
            header.typeflag(),
            GNU_LONGLINK_TYPE | GNU_LONGNAME_TYPE | PAX_HEADER | PAX_GLOBAL_HEADER
        ) {
            match header.typeflag() {
                PAX_HEADER => {
                    let data = read_special_file(file.as_ref(), offset, header.get_size()).await?;
                    let parsed = PaxHeader::read_pax(&data)?;
                    pax = Some(parsed);
                    offset = offset.saturating_add(align_up(header.get_size(), T_BLOCKSIZE));
                }
                PAX_GLOBAL_HEADER => {
                    let _ = read_special_file(file.as_ref(), offset, header.get_size()).await?;
                    if pax.is_none() {
                        pax = Some(PaxHeader::empty());
                    }
                    offset = offset.saturating_add(align_up(header.get_size(), T_BLOCKSIZE));
                }
                GNU_LONGNAME_TYPE | GNU_LONGLINK_TYPE => {
                    let _ = read_special_file(file.as_ref(), offset, header.get_size()).await?;
                    offset = offset.saturating_add(align_up(header.get_size(), T_BLOCKSIZE));
                }
                _ => {}
            }

            (header, next_offset) = read_header_internal(file.as_ref(), offset).await?;
            offset = next_offset;
        }

        Ok(Self { header, pax })
    }

    fn get_size(&self) -> u64 {
        match self.pax.as_ref().and_then(|pax| pax.size) {
            Some(size) if size > 0 => size,
            _ => self.header.get_size(),
        }
    }

    fn has_pax_header(&self) -> bool {
        self.pax.is_some()
    }
}

#[derive(Debug, Clone)]
struct PaxHeader {
    path: Option<String>,
    linkpath: Option<String>,
    size: Option<u64>,
    records: HashMap<String, String>,
}

impl PaxHeader {
    fn empty() -> Self {
        Self {
            path: None,
            linkpath: None,
            size: None,
            records: HashMap::new(),
        }
    }

    fn read_pax(data: &[u8]) -> Result<Self> {
        let mut header = Self::empty();

        let mut start = 0usize;
        while start < data.len() {
            if data[start] == 0 {
                break;
            }
            let space_pos = data[start..]
                .iter()
                .position(|b| *b == b' ')
                .context("invalid pax record: missing length separator")?;
            let len_bytes = &data[start..start + space_pos];
            let len_str =
                std::str::from_utf8(len_bytes).context("invalid pax record length utf8")?;
            let len = len_str
                .parse::<usize>()
                .context("invalid pax record length")?;
            ensure!(
                len >= 5 && start.saturating_add(len) <= data.len(),
                "invalid pax record length range"
            );

            let record_start = start + space_pos + 1;
            let record_end = start + len - 1;
            let record = std::str::from_utf8(&data[record_start..record_end])
                .context("invalid pax record utf8")?;
            let eq_pos = record
                .find('=')
                .context("invalid pax record: missing '='")?;
            let key = record[..eq_pos].to_string();
            let value = record[eq_pos + 1..].to_string();
            header.records.insert(key, value);
            start += len;
        }

        for (key, value) in &header.records {
            match key.as_str() {
                PAX_SIZE => {
                    header.size = Some(value.parse::<u64>().context("invalid pax size value")?);
                }
                PAX_PATH => {
                    header.path = Some(value.clone());
                }
                PAX_LINKPATH => {
                    header.linkpath = Some(value.clone());
                }
                _ => {}
            }
        }

        Ok(header)
    }
}

#[derive(Debug, Clone)]
struct TarHeader {
    raw: [u8; T_BLOCKSIZE_USIZE],
}

impl TarHeader {
    fn from_block(block: &[u8]) -> Result<Self> {
        ensure!(
            block.len() == T_BLOCKSIZE_USIZE,
            "short tar header: got {}, expected {T_BLOCKSIZE_USIZE}",
            block.len()
        );
        let mut raw = [0u8; T_BLOCKSIZE_USIZE];
        raw.copy_from_slice(block);
        Ok(Self { raw })
    }

    fn magic_matches(&self) -> bool {
        self.raw[257..262] == TMAGIC[..]
    }

    fn version_matches(&self) -> bool {
        self.raw[263..265] == TVERSION[..]
    }

    fn get_size(&self) -> u64 {
        oct_to_u64(&self.raw[124..136]).unwrap_or(0)
    }

    fn get_crc(&self) -> u64 {
        oct_to_u64(&self.raw[148..156]).unwrap_or(0)
    }

    fn typeflag(&self) -> u8 {
        self.raw[156]
    }

    fn crc_ok(&self) -> bool {
        let expect = self.get_crc();
        expect == self.crc_calc() || expect == self.signed_crc_calc() as u64
    }

    fn crc_calc(&self) -> u64 {
        let mut sum = 0u64;
        for idx in 0..T_BLOCKSIZE_USIZE {
            sum = sum.saturating_add(u64::from(self.raw[idx]));
        }
        for idx in 148..156 {
            sum = sum
                .saturating_add(u64::from(b' '))
                .saturating_sub(u64::from(self.raw[idx]));
        }
        sum
    }

    fn signed_crc_calc(&self) -> i64 {
        let mut sum = 0i64;
        for byte in self.raw {
            sum = sum.saturating_add(i64::from(byte as i8));
        }
        for idx in 148..156 {
            sum = sum
                .saturating_add(i64::from(b' ' as i8))
                .saturating_sub(i64::from(self.raw[idx] as i8));
        }
        sum
    }
}

fn oct_to_u64(field: &[u8]) -> Option<u64> {
    let trimmed = field
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .collect::<Vec<_>>();
    let s = std::str::from_utf8(&trimmed).ok()?.trim();
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(s, 8).ok()
}

fn align_up(v: u64, a: u64) -> u64 {
    if a == 0 || v == 0 {
        return v;
    }
    ((v - 1) / a + 1).saturating_mul(a)
}

fn format_pax_record(key: &str, value: &str) -> String {
    let mut size = key.len() + value.len() + 3;
    size += size.to_string().len();
    let mut record = format!("{size} {key}={value}\n");
    if record.len() != size {
        size = record.len();
        record = format!("{size} {key}={value}\n");
    }
    record
}

fn tar_header(typeflag: u8, size: u64, name: &str) -> [u8; T_BLOCKSIZE_USIZE] {
    let mut block = [0u8; T_BLOCKSIZE_USIZE];
    let name_bytes = name.as_bytes();
    block[..name_bytes.len().min(100)].copy_from_slice(&name_bytes[..name_bytes.len().min(100)]);
    write_octal_field(&mut block[124..136], size);
    block[156] = typeflag;
    block[257..262].copy_from_slice(TMAGIC);
    block[263..265].copy_from_slice(TVERSION);
    for byte in &mut block[148..156] {
        *byte = b' ';
    }
    let checksum = TarHeader { raw: block }.crc_calc();
    let chk = format!("{checksum:06o}\0 ");
    block[148..156].copy_from_slice(chk.as_bytes());
    block
}

fn write_octal_field(field: &mut [u8], value: u64) {
    for byte in field.iter_mut() {
        *byte = b'0';
    }
    if let Some(last) = field.last_mut() {
        *last = b' ';
    }
    let oct = format!("{value:o}");
    let start = field.len().saturating_sub(1 + oct.len());
    field[start..start + oct.len()].copy_from_slice(oct.as_bytes());
}

async fn mark_new_tar(file: &dyn VirtualFile) -> Result<()> {
    let record = format_pax_record("size", "0");
    let mut buf = vec![0u8; 3 * T_BLOCKSIZE_USIZE];
    let mut pax = [0u8; T_BLOCKSIZE_USIZE];
    let pax_name = b"overlaybd.pax";
    pax[..pax_name.len()].copy_from_slice(pax_name);
    pax[156] = PAX_HEADER;
    write_octal_field(&mut pax[124..136], record.len() as u64);
    buf[..T_BLOCKSIZE_USIZE].copy_from_slice(&pax);
    buf[T_BLOCKSIZE_USIZE..T_BLOCKSIZE_USIZE + record.len()].copy_from_slice(record.as_bytes());

    let mut hdr = [0u8; T_BLOCKSIZE_USIZE];
    let name = b"overlaybd.new";
    hdr[..name.len()].copy_from_slice(name);
    write_octal_field(&mut hdr[124..136], 0);
    hdr[257..262].copy_from_slice(TMAGIC_EMPTY);
    hdr[263..265].copy_from_slice(TVERSION_EMPTY);
    for byte in &mut hdr[148..156] {
        *byte = b' ';
    }
    let checksum = TarHeader { raw: hdr }.crc_calc();
    hdr[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
    buf[2 * T_BLOCKSIZE_USIZE..3 * T_BLOCKSIZE_USIZE].copy_from_slice(&hdr);

    let written = file.write_at(0, &buf).await?;
    ensure!(
        written == buf.len(),
        "short write while creating tar header: {written}/{}",
        buf.len()
    );
    Ok(())
}

async fn write_header_trailer(file: &dyn VirtualFile, payload_size: u64) -> Result<()> {
    let record = format_pax_record("size", &payload_size.to_string());
    let mut buf = vec![0u8; 3 * T_BLOCKSIZE_USIZE];
    let pax = tar_header(PAX_HEADER, record.len() as u64, "overlaybd.pax");
    buf[..T_BLOCKSIZE_USIZE].copy_from_slice(&pax);
    buf[T_BLOCKSIZE_USIZE..T_BLOCKSIZE_USIZE + record.len()].copy_from_slice(record.as_bytes());
    let reg = tar_header(REGTYPE, 0, "overlaybd.commit");
    buf[2 * T_BLOCKSIZE_USIZE..3 * T_BLOCKSIZE_USIZE].copy_from_slice(&reg);

    let written = file.write_at(0, &buf).await?;
    ensure!(
        written == buf.len(),
        "short write while updating tar header: {written}/{}",
        buf.len()
    );

    let payload_end = 3 * T_BLOCKSIZE + payload_size;
    let trailer_off = align_up(payload_end, T_BLOCKSIZE);
    let trailer = [0u8; 2 * T_BLOCKSIZE_USIZE];
    let written = file.write_at(trailer_off, &trailer).await?;
    ensure!(
        written == trailer.len(),
        "short write while updating tar trailer: {written}/{}",
        trailer.len()
    );
    file.truncate(trailer_off + 2 * T_BLOCKSIZE).await?;
    Ok(())
}

async fn read_header_internal(file: &dyn VirtualFile, mut offset: u64) -> Result<(TarHeader, u64)> {
    let mut zero_blocks = 0usize;
    loop {
        let raw = file.read_at(offset, T_BLOCKSIZE_USIZE).await?;
        ensure!(!raw.is_empty(), "unexpected EOF while reading tar header");
        let header = TarHeader::from_block(&raw)?;
        offset = offset.saturating_add(T_BLOCKSIZE);
        if header.raw.iter().all(|b| *b == 0) {
            zero_blocks += 1;
            ensure!(
                zero_blocks < 2,
                "unexpected end-of-tar marker in header stream"
            );
            continue;
        }
        return Ok((header, offset));
    }
}

async fn read_special_file(file: &dyn VirtualFile, offset: u64, size: u64) -> Result<Vec<u8>> {
    let aligned = align_up(size, T_BLOCKSIZE);
    let len =
        usize::try_from(aligned).context(format!("special tar section too large: {aligned}"))?;
    let raw = file.read_at(offset, len).await?;
    ensure!(
        raw.len() == len,
        "short read for special tar section: got {}, expected {len}",
        raw.len()
    );
    let keep = usize::try_from(size).context(format!("special tar size too large: {size}"))?;
    Ok(raw[..keep].to_vec())
}

pub async fn is_tar_file(file: &dyn VirtualFile) -> Result<bool> {
    let raw = file.read_at(0, T_BLOCKSIZE_USIZE).await?;
    if raw.len() != T_BLOCKSIZE_USIZE {
        return Ok(false);
    }
    let header = TarHeader::from_block(&raw)?;
    Ok(header.magic_matches() && header.version_matches() && header.crc_ok())
}

pub async fn new_tar_file(file: Arc<dyn VirtualFile>) -> Result<Arc<TarFile>> {
    ensure!(
        is_tar_file(file.as_ref()).await?,
        "input file is not a tar file"
    );
    Ok(Arc::new(TarFile::new(file, false).await?))
}

pub async fn new_tar_file_create(file: Arc<dyn VirtualFile>) -> Result<Arc<TarFile>> {
    Ok(Arc::new(TarFile::new(file, true).await?))
}

pub async fn open_tar_file(file: Arc<dyn VirtualFile>) -> Result<Arc<dyn VirtualFile>> {
    if is_tar_file(file.as_ref()).await? {
        Ok(new_tar_file(file).await?)
    } else {
        Ok(file)
    }
}

pub async fn open_tar_file_rw(file: Arc<dyn VirtualFile>) -> Result<Arc<dyn VirtualFile>> {
    if file.size().await? == 0 {
        Ok(new_tar_file_create(file).await?)
    } else {
        open_tar_file(file).await
    }
}

pub async fn new_tar_file_adaptor(file: Arc<dyn VirtualFile>) -> Result<Arc<dyn VirtualFile>> {
    open_tar_file(file).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    struct MemoryFile {
        data: Mutex<Vec<u8>>,
        last_seek_data: AtomicU64,
        last_seek_hole: AtomicU64,
    }

    impl MemoryFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Mutex::new(data),
                last_seek_data: AtomicU64::new(0),
                last_seek_hole: AtomicU64::new(0),
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

        async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
            self.last_seek_data.store(offset, Ordering::Relaxed);
            let size = self.data.lock().await.len() as u64;
            if offset >= size {
                return Ok(None);
            }
            Ok(Some(offset))
        }

        async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
            self.last_seek_hole.store(offset, Ordering::Relaxed);
            Ok(Some(self.data.lock().await.len() as u64))
        }
    }

    fn format_pax_record(key: &str, value: &str) -> String {
        let mut size = key.len() + value.len() + 3;
        size += size.to_string().len();
        let mut record = format!("{size} {key}={value}\n");
        if record.len() != size {
            size = record.len();
            record = format!("{size} {key}={value}\n");
        }
        record
    }

    fn write_octal_field(field: &mut [u8], value: u64) {
        for byte in field.iter_mut() {
            *byte = b'0';
        }
        if let Some(last) = field.last_mut() {
            *last = b' ';
        }
        let oct = format!("{value:o}");
        let start = field.len().saturating_sub(1 + oct.len());
        field[start..start + oct.len()].copy_from_slice(oct.as_bytes());
    }

    fn tar_header(typeflag: u8, size: u64, name: &str) -> [u8; T_BLOCKSIZE_USIZE] {
        let mut block = [0u8; T_BLOCKSIZE_USIZE];
        let name_bytes = name.as_bytes();
        block[..name_bytes.len().min(100)]
            .copy_from_slice(&name_bytes[..name_bytes.len().min(100)]);
        write_octal_field(&mut block[124..136], size);
        block[156] = typeflag;
        block[257..262].copy_from_slice(TMAGIC);
        block[263..265].copy_from_slice(TVERSION);
        for byte in &mut block[148..156] {
            *byte = b' ';
        }
        let checksum = TarHeader { raw: block }.crc_calc();
        let chk = format!("{checksum:06o}\0 ");
        block[148..156].copy_from_slice(chk.as_bytes());
        block
    }

    fn build_regular_tar(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&tar_header(b'0', payload.len() as u64, "overlaybd.commit"));
        out.extend_from_slice(payload);
        let pad = align_up(payload.len() as u64, T_BLOCKSIZE) as usize - payload.len();
        out.extend(std::iter::repeat_n(0u8, pad));
        out.extend(std::iter::repeat_n(0u8, 2 * T_BLOCKSIZE_USIZE));
        out
    }

    fn build_overlaybd_pax_tar(payload: &[u8]) -> Vec<u8> {
        let record = format_pax_record("size", &payload.len().to_string());
        let mut out = Vec::new();
        out.extend_from_slice(&tar_header(
            PAX_HEADER,
            record.len() as u64,
            "overlaybd.pax",
        ));
        out.extend_from_slice(record.as_bytes());
        let pax_pad = align_up(record.len() as u64, T_BLOCKSIZE) as usize - record.len();
        out.extend(std::iter::repeat_n(0u8, pax_pad));
        out.extend_from_slice(&tar_header(b'0', 0, "overlaybd.commit"));
        out.extend_from_slice(payload);
        let pad = align_up(payload.len() as u64, T_BLOCKSIZE) as usize - payload.len();
        out.extend(std::iter::repeat_n(0u8, pad));
        out.extend(std::iter::repeat_n(0u8, 2 * T_BLOCKSIZE_USIZE));
        out
    }

    fn build_overlaybd_global_pax_tar(payload: &[u8]) -> Vec<u8> {
        let record = format_pax_record("mtime", "0");
        let mut out = Vec::new();
        out.extend_from_slice(&tar_header(
            PAX_GLOBAL_HEADER,
            record.len() as u64,
            "overlaybd.global",
        ));
        out.extend_from_slice(record.as_bytes());
        let pax_pad = align_up(record.len() as u64, T_BLOCKSIZE) as usize - record.len();
        out.extend(std::iter::repeat_n(0u8, pax_pad));
        out.extend_from_slice(&tar_header(
            REGTYPE,
            payload.len() as u64,
            "overlaybd.commit",
        ));
        out.extend_from_slice(payload);
        let pad = align_up(payload.len() as u64, T_BLOCKSIZE) as usize - payload.len();
        out.extend(std::iter::repeat_n(0u8, pad));
        out.extend(std::iter::repeat_n(0u8, 2 * T_BLOCKSIZE_USIZE));
        out
    }

    #[tokio::test]
    async fn test_is_tar_file_detects_regular_tar_header() {
        let payload = b"hello-overlaybd-tar".to_vec();
        let file = MemoryFile::new(build_regular_tar(&payload));
        assert!(is_tar_file(&file).await.expect("detect tar"));
    }

    #[tokio::test]
    async fn test_new_tar_file_adaptor_passthrough_non_tar() {
        let file: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(b"plain-file".to_vec()));
        let adapted = new_tar_file_adaptor(file.clone())
            .await
            .expect("adapt non-tar file");
        assert!(Arc::ptr_eq(&file, &adapted));
    }

    #[tokio::test]
    async fn test_new_tar_file_rejects_non_tar() {
        let file: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(b"plain-file".to_vec()));
        let err = new_tar_file(file).await.expect_err("non-tar should fail");
        assert!(
            format!("{err}").contains("input file is not a tar file"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn test_tar_file_maps_regular_tar_payload() {
        let payload = b"regular-tar-payload".to_vec();
        let file: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(build_regular_tar(&payload)));
        let adapted = new_tar_file(file).await.expect("open tar file");

        assert_eq!(adapted.base_offset(), T_BLOCKSIZE);
        assert_eq!(
            adapted.size().await.expect("payload size"),
            payload.len() as u64
        );
        let got = adapted
            .read_at(0, payload.len())
            .await
            .expect("read payload");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_tar_file_maps_overlaybd_pax_payload() {
        let payload = b"pax-overlaybd-payload".to_vec();
        let file: Arc<dyn VirtualFile> =
            Arc::new(MemoryFile::new(build_overlaybd_pax_tar(&payload)));
        let adapted = new_tar_file(file).await.expect("open pax tar file");

        assert_eq!(adapted.base_offset(), 3 * T_BLOCKSIZE);
        assert_eq!(
            adapted.size().await.expect("payload size"),
            payload.len() as u64
        );
        let got = adapted
            .read_at(3, payload.len() - 3)
            .await
            .expect("read pax payload");
        assert_eq!(got.as_ref(), &payload[3..]);
    }

    #[tokio::test]
    async fn test_tar_file_maps_overlaybd_global_pax_payload() {
        let payload = b"global-pax-overlaybd".to_vec();
        let file: Arc<dyn VirtualFile> =
            Arc::new(MemoryFile::new(build_overlaybd_global_pax_tar(&payload)));
        let adapted = new_tar_file(file).await.expect("open global pax tar file");

        assert_eq!(adapted.base_offset(), 3 * T_BLOCKSIZE);
        assert_eq!(
            adapted.size().await.expect("payload size"),
            payload.len() as u64
        );
        let got = adapted
            .read_at(0, payload.len())
            .await
            .expect("read global pax payload");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_new_tar_file_create_writes_reopenable_tar_layout() {
        let backing = Arc::new(MemoryFile::new(Vec::new()));
        let created = new_tar_file_create(backing.clone())
            .await
            .expect("create tar file");
        assert!(created.is_new_tar());
        assert_eq!(created.base_offset(), 3 * T_BLOCKSIZE);

        let payload = b"created-payload".to_vec();
        let written = created.write_at(0, &payload).await.expect("write payload");
        assert_eq!(written, payload.len());
        assert!(!is_tar_file(backing.as_ref())
            .await
            .expect("unfinished new tar should not be visible as tar"));

        created.close().await.expect("close created tar");

        let reopened = open_tar_file(backing.clone())
            .await
            .expect("reopen created tar");
        let got = reopened
            .read_at(0, payload.len())
            .await
            .expect("read reopened payload");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_open_tar_file_rw_creates_tar_for_empty_backing() {
        let backing = Arc::new(MemoryFile::new(Vec::new()));
        let opened = open_tar_file_rw(backing.clone())
            .await
            .expect("open writable tar");

        let payload = b"rw-open-created-payload".to_vec();
        let written = opened.write_at(0, &payload).await.expect("write payload");
        assert_eq!(written, payload.len());

        assert!(!is_tar_file(backing.as_ref())
            .await
            .expect("unfinished writable tar should not be visible as tar"));
    }

    #[tokio::test]
    async fn test_open_tar_file_rw_reuses_existing_tar() {
        let payload = b"existing-rw-payload".to_vec();
        let backing: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(build_regular_tar(&payload)));
        let opened = open_tar_file_rw(backing)
            .await
            .expect("open existing tar for write path");

        let got = opened
            .read_at(0, payload.len())
            .await
            .expect("read existing tar payload");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_tar_file_seek_offsets_are_shifted() {
        let payload = vec![0x5a; 1024];
        let file = Arc::new(MemoryFile::new(build_regular_tar(&payload)));
        let adapted = new_tar_file(file.clone())
            .await
            .expect("open regular tar file");

        let data_off = adapted.seek_data(11).await.expect("seek_data");
        let hole_off = adapted.seek_hole(23).await.expect("seek_hole");

        assert_eq!(data_off, Some(11));
        assert_eq!(hole_off, Some(payload.len() as u64));
        assert_eq!(
            file.last_seek_data.load(Ordering::Relaxed),
            T_BLOCKSIZE + 11
        );
        assert_eq!(
            file.last_seek_hole.load(Ordering::Relaxed),
            T_BLOCKSIZE + 23
        );
    }
}
