use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::path::Path;

/// Copy a quiesced file without turning holes into data or allocated zeroes
/// into holes. Sparse OverlayBD uppers use that distinction as their index.
/// The caller owns destination publication, syncing, and failure cleanup.
pub async fn copy_sparse_file(source: &Path, destination: &Path) -> io::Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || copy_sparse_file_blocking(&source, &destination))
        .await
        .map_err(io::Error::other)?
}

/// Synchronous variant for callers already running on a blocking worker.
pub fn copy_sparse_file_blocking(source: &Path, destination: &Path) -> io::Result<()> {
    let mut source = File::open(source)?;
    let metadata = source.metadata()?;
    let mut destination = File::create(destination)?;
    destination.set_len(metadata.len())?;
    let mut cursor = 0;
    while cursor < metadata.len() {
        let start = match seek_extent(&source, cursor, libc::SEEK_DATA) {
            Ok(start) => start,
            Err(error) if error.raw_os_error() == Some(libc::ENXIO) => break,
            Err(error) => return Err(error),
        };
        let end = seek_extent(&source, start, libc::SEEK_HOLE)?.min(metadata.len());
        source.seek(SeekFrom::Start(start))?;
        destination.seek(SeekFrom::Start(start))?;
        let length = end - start;
        let copied = io::copy(&mut (&mut source).take(length), &mut destination)?;
        if copied != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sparse source truncated while copying",
            ));
        }
        cursor = end;
    }
    destination.set_permissions(metadata.permissions())?;
    Ok(())
}

fn seek_extent(file: &File, offset: u64, whence: libc::c_int) -> io::Result<u64> {
    let result = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, whence) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[test]
    fn sparse_copy_preserves_allocated_zeroes_holes_and_permissions() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        let mut file = File::create(&source)?;
        file.set_len(16 * 1024 * 1024)?;
        file.write_all(&[0xA5; 4096])?;
        file.seek(SeekFrom::Start(8 * 1024 * 1024))?;
        file.write_all(&[0; 4096])?;
        file.set_permissions(std::fs::Permissions::from_mode(0o640))?;
        copy_sparse_file_blocking(&source, &destination)?;
        assert_eq!(std::fs::read(&source)?, std::fs::read(&destination)?);
        let copied = File::open(&destination)?;
        assert_eq!(
            seek_extent(&copied, 4096, libc::SEEK_DATA)?,
            8 * 1024 * 1024
        );
        assert_eq!(
            seek_extent(&copied, 8 * 1024 * 1024, libc::SEEK_HOLE)?,
            8 * 1024 * 1024 + 4096
        );
        assert!(copied.metadata()?.blocks() * 512 < 1024 * 1024);
        assert_eq!(copied.metadata()?.permissions().mode() & 0o777, 0o640);
        Ok(())
    }
}
