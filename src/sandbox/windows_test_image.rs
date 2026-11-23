//! Opt-in disk-image experiment for issue #390, not an OCI image format.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowsTestDisk {
    pub disk_path: PathBuf,
    pub firmware_code: PathBuf,
    pub firmware_vars: PathBuf,
    pub disk_url: Option<String>,
}

pub(crate) async fn resolve_windows_test_disk(name: &str) -> Result<WindowsTestDisk> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "windows-test image name must contain only letters, digits, '-' or '_'"
    );
    let root = std::env::var_os("AENV_WINDOWS_TEST_IMAGE_DIR")
        .context("Windows disk experiment disabled: set AENV_WINDOWS_TEST_IMAGE_DIR")?;
    let root = PathBuf::from(root).canonicalize()?;
    let mut disk: WindowsTestDisk =
        serde_json::from_slice(&tokio::fs::read(root.join(format!("{name}.json"))).await?)?;
    for path in [
        &mut disk.disk_path,
        &mut disk.firmware_code,
        &mut disk.firmware_vars,
    ] {
        if path.is_relative() {
            *path = root.join(&*path);
        }
    }
    cache_disk(&disk.disk_path, disk.disk_url.as_deref()).await?;
    disk.disk_path = disk.disk_path.canonicalize()?;
    ensure!(disk.firmware_code.is_file(), "UEFI code file missing");
    ensure!(
        disk.firmware_vars.is_file(),
        "UEFI variables template missing"
    );
    Ok(disk)
}

async fn cache_disk(path: &Path, url: Option<&str>) -> Result<()> {
    // A trusted node-local manifest chooses both the local cache and origin;
    // clients cannot submit arbitrary download URLs or host paths via the API.
    if path.is_file() {
        return Ok(());
    }
    let url = url.context("local Windows system disk missing and disk_url is not configured")?;
    let parent = path.parent().context("disk path has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut output = tokio::fs::File::from_std(temporary.reopen()?);
    let mut response = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(1800))
        .build()?
        .get(url)
        .send()
        .await?
        .error_for_status()?;
    while let Some(chunk) = response.chunk().await? {
        output.write_all(&chunk).await?;
    }
    output.sync_all().await?;
    drop(output);
    ensure!(
        temporary.as_file().metadata()?.len() > 0,
        "empty system disk download"
    );
    // Never replace a base disk another sandbox may already be reading.
    if let Err(error) = temporary.persist_noclobber(path) {
        if error.error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn local_disk_wins_over_remote_and_missing_disk_downloads_atomically() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("disk.qcow2");
        tokio::fs::write(&path, b"local").await?;
        cache_disk(&path, Some("http://127.0.0.1:1/unreachable")).await?;
        assert_eq!(tokio::fs::read(&path).await?, b"local");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/disk", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nremote",
                )
                .await
                .unwrap();
        });
        let remote = dir.path().join("download.qcow2");
        cache_disk(&remote, Some(&url)).await?;
        server.await?;
        assert_eq!(tokio::fs::read(remote).await?, b"remote");
        let missing = dir.path().join("missing.qcow2");
        assert!(cache_disk(&missing, Some("http://127.0.0.1:1/unreachable"))
            .await
            .is_err());
        assert!(!missing.exists());
        Ok(())
    }
}
