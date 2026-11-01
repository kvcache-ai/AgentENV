use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, ensure, Context, Result};
use uuid::Uuid;
use zerocopy::FromBytes;

use crate::compression::zfile::{is_zfile_header, HEADER_TRAILER_SPACE as ZFILE_HEADER_SIZE};
use crate::config::LayerConfig;
use crate::lsmt::format::HeaderTrailer;

/// The names [`resolve_local_layer_path`] probes inside a layer's `dir`, in this
/// order.
///
/// Public because they are a naming convention, not an implementation detail: anything
/// that *writes* a layer has to use one of these for the result to be findable from a
/// `LayerConfig` that carries only a directory. `COMMIT_FILE_NAME` is what to write;
/// `SEALED_FILE_NAME` is only read, for layers produced before that name settled.
pub const COMMIT_FILE_NAME: &str = "overlaybd.commit";
pub const SEALED_FILE_NAME: &str = "overlaybd.sealed";

pub fn resolve_local_layer_path(layer: &LayerConfig) -> Option<PathBuf> {
    if !layer.file.is_empty() {
        if layer.file.contains("://") {
            return None;
        }
        return Some(PathBuf::from(&layer.file));
    }
    if layer.dir.is_empty() {
        return None;
    }

    let commit = Path::new(&layer.dir).join(COMMIT_FILE_NAME);
    if commit.exists() {
        return Some(commit);
    }

    let sealed = Path::new(&layer.dir).join(SEALED_FILE_NAME);
    if sealed.exists() {
        return Some(sealed);
    }

    None
}

pub fn read_overlaybd_layer_virtual_size(path: impl AsRef<Path>) -> Result<u64> {
    Ok(read_overlaybd_layer_trailer(path)?.virtual_size.get())
}

pub fn read_overlaybd_layer_uuid(path: impl AsRef<Path>) -> Result<Uuid> {
    let trailer = read_overlaybd_layer_trailer(path)?;
    let raw = trailer.uuid.split(|b| *b == 0).next().unwrap_or(&[]);
    let s = std::str::from_utf8(raw).unwrap_or("");
    Ok(Uuid::parse_str(s).unwrap_or_else(|_| Uuid::nil()))
}

pub fn should_dense_export_layer(path: &Path) -> bool {
    match read_overlaybd_layer_is_sparse_rw(path) {
        Ok(is_sparse) => is_sparse,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %format_args!("{error:#}"),
                "failed to inspect overlaybd sparse header; falling back to raw layer upload"
            );
            false
        }
    }
}

pub fn read_overlaybd_layer_is_sparse_rw(path: impl AsRef<Path>) -> Result<bool> {
    let path = path.as_ref();
    let file =
        File::open(path).with_context(|| format!("open overlaybd layer {}", path.display()))?;
    let mut header_bytes = Vec::with_capacity(HeaderTrailer::SPACE as usize);
    file.take(HeaderTrailer::SPACE)
        .read_to_end(&mut header_bytes)
        .with_context(|| format!("read overlaybd header {}", path.display()))?;

    // ZFile layers are already uploadable, including files smaller than an LSMT header.
    let zfile_header = header_bytes
        .first_chunk::<ZFILE_HEADER_SIZE>()
        .with_context(|| format!("overlaybd header is too short: {}", path.display()))?;
    if is_zfile_header(zfile_header)
        .with_context(|| format!("inspect compressed overlaybd layer {}", path.display()))?
    {
        return Ok(false);
    }

    ensure!(
        header_bytes.len() == HeaderTrailer::SPACE as usize,
        "overlaybd LSMT header is too short: {}",
        path.display()
    );
    let header = HeaderTrailer::read_from_bytes(&header_bytes[..size_of::<HeaderTrailer>()])
        .map_err(|_| anyhow!("invalid overlaybd header: {}", path.display()))?;
    ensure!(
        header.verify_magic() && header.is_header(),
        "overlaybd header mismatch for {}",
        path.display()
    );

    Ok(header.is_sparse_rw())
}

fn read_overlaybd_layer_trailer(path: impl AsRef<Path>) -> Result<HeaderTrailer> {
    let path = path.as_ref();
    let mut file =
        File::open(path).with_context(|| format!("open overlaybd layer {}", path.display()))?;
    let file_size = file
        .metadata()
        .with_context(|| format!("stat overlaybd layer {}", path.display()))?
        .len();
    ensure!(
        file_size >= HeaderTrailer::SPACE,
        "overlaybd layer {} is too small for trailer",
        path.display()
    );

    file.seek(SeekFrom::Start(file_size - HeaderTrailer::SPACE))
        .with_context(|| format!("seek overlaybd trailer {}", path.display()))?;

    let mut raw = vec![0u8; HeaderTrailer::SPACE as usize];
    file.read_exact(&mut raw)
        .with_context(|| format!("read overlaybd trailer {}", path.display()))?;

    let trailer = HeaderTrailer::read_from_bytes(&raw[..size_of::<HeaderTrailer>()])
        .map_err(|_| anyhow!("invalid overlaybd trailer: {}", path.display()))?;
    ensure!(
        trailer.verify_magic()
            && trailer.is_trailer()
            && trailer.is_data_file()
            && trailer.is_sealed(),
        "overlaybd trailer mismatch for {}",
        path.display()
    );

    Ok(trailer)
}
