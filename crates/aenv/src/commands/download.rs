use crate::client::Client;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Args as ClapArgs;
use futures::{Stream, StreamExt};
use std::path::{Path, PathBuf};
use tempfile::Builder;
use tokio::io::AsyncWriteExt;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv download <sandbox-id> /workspace/result.txt ./result.txt
  aenv download <sandbox-id> /workspace/result.txt
  aenv download <sandbox-id> /workspace/result.txt ./output/
  aenv download --user app --force <sandbox-id> result.txt ./result.txt")]
pub struct Args {
    /// Sandbox ID
    sandbox_id: String,
    /// Source file path inside the sandbox
    remote_path: String,
    /// Local destination file or directory. Defaults to the current directory
    local_path: Option<PathBuf>,
    /// User used by envd for relative path resolution
    #[arg(long)]
    user: Option<String>,
    /// Replace the local destination if it already exists
    #[arg(long)]
    force: bool,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    rt.block_on(run_async(client, args))
}

async fn run_async(client: Client, args: Args) -> Result<()> {
    let local_path = resolve_destination(&args.remote_path, args.local_path.as_deref())?;
    validate_destination(&local_path, args.force)?;

    let response = client
        .files(&args.sandbox_id)?
        .download(&args.remote_path, args.user.as_deref())
        .await?;
    save_stream(response.bytes_stream(), &local_path, args.force).await?;

    println!(
        "Downloaded {}:{} to {}",
        args.sandbox_id,
        args.remote_path,
        local_path.display()
    );
    Ok(())
}

fn resolve_destination(remote_path: &str, local_path: Option<&Path>) -> Result<PathBuf> {
    let remote_file_name = remote_file_name(remote_path)?;
    let local_path = local_path.unwrap_or_else(|| Path::new("."));

    match std::fs::metadata(local_path) {
        Ok(metadata) if metadata.is_dir() => Ok(local_path.join(remote_file_name)),
        Ok(_) => Ok(local_path.to_path_buf()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if local_path
                .as_os_str()
                .to_string_lossy()
                .ends_with(std::path::MAIN_SEPARATOR)
            {
                Ok(local_path.join(remote_file_name))
            } else {
                Ok(local_path.to_path_buf())
            }
        }
        Err(err) => {
            Err(err).with_context(|| format!("reading destination {}", local_path.display()))
        }
    }
}

fn remote_file_name(remote_path: &str) -> Result<&str> {
    if remote_path.is_empty() || remote_path.ends_with('/') {
        bail!("remote download source must include a file name: {remote_path}");
    }

    Path::new(remote_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .context("remote download source must include a valid UTF-8 file name")
}

fn validate_destination(destination: &Path, force: bool) -> Result<()> {
    if destination.file_name().is_none() {
        bail!(
            "local download destination must be a file path: {}",
            destination.display()
        );
    }

    let parent = destination_parent(destination);
    let parent_metadata = std::fs::metadata(parent)
        .with_context(|| format!("reading destination directory {}", parent.display()))?;
    if !parent_metadata.is_dir() {
        bail!(
            "download destination parent is not a directory: {}",
            parent.display()
        );
    }

    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.is_dir() => bail!(
            "local download destination is a directory: {}",
            destination.display()
        ),
        Ok(_) if !force => bail!(
            "local download destination already exists: {}; use --force to replace it",
            destination.display()
        ),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("reading destination {}", destination.display()))
        }
    }
}

fn destination_parent(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

async fn save_stream<S, E>(stream: S, destination: &Path, force: bool) -> Result<()>
where
    S: Stream<Item = std::result::Result<Bytes, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let parent = destination_parent(destination);
    let temp = Builder::new()
        .prefix(".aenv-download-")
        .tempfile_in(parent)
        .with_context(|| format!("creating temporary file in {}", parent.display()))?;
    let (std_file, temp_path) = temp.into_parts();
    let mut file = tokio::fs::File::from_std(std_file);

    tokio::pin!(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| anyhow!(err).context("reading download stream"))?;
        file.write_all(&chunk)
            .await
            .context("writing downloaded file")?;
    }
    file.flush().await.context("flushing downloaded file")?;
    drop(file);

    let persist_result = if force {
        temp_path.persist(destination)
    } else {
        temp_path.persist_noclobber(destination)
    };
    match persist_result {
        Ok(()) => Ok(()),
        Err(err) if err.error.kind() == std::io::ErrorKind::AlreadyExists && !force => bail!(
            "local download destination already exists: {}; use --force to replace it",
            destination.display()
        ),
        Err(err) => Err(err.error)
            .with_context(|| format!("saving downloaded file to {}", destination.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tempfile::TempDir;

    #[test]
    fn omitted_destination_uses_remote_filename_in_current_directory() {
        assert_eq!(
            resolve_destination("/workspace/result.txt", None).unwrap(),
            PathBuf::from("./result.txt")
        );
    }

    #[test]
    fn directory_destination_appends_remote_filename() {
        let temp = TempDir::new().unwrap();

        assert_eq!(
            resolve_destination("/workspace/result.txt", Some(temp.path())).unwrap(),
            temp.path().join("result.txt")
        );
    }

    #[test]
    fn trailing_slash_destination_appends_remote_filename() {
        assert_eq!(
            resolve_destination("/workspace/result.txt", Some(Path::new("output/"))).unwrap(),
            PathBuf::from("output/result.txt")
        );
    }

    #[test]
    fn explicit_destination_filename_is_preserved() {
        assert_eq!(
            resolve_destination("/workspace/result.txt", Some(Path::new("./renamed.txt"))).unwrap(),
            PathBuf::from("./renamed.txt")
        );
    }

    #[test]
    fn remote_source_requires_filename_for_inference() {
        let error = resolve_destination("/workspace/", None).unwrap_err();

        assert!(error
            .to_string()
            .contains("remote download source must include a file name"));
    }

    #[test]
    fn existing_destination_requires_force() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        std::fs::write(&destination, "old").unwrap();

        let err = validate_destination(&destination, false).unwrap_err();
        assert!(err.to_string().contains("use --force"));
        validate_destination(&destination, true).unwrap();
    }

    #[tokio::test]
    async fn save_stream_does_not_replace_existing_file_without_force() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        tokio::fs::write(&destination, b"old").await.unwrap();

        let err = save_stream(
            stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"new"))]),
            &destination,
            false,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("use --force"));
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn save_stream_replaces_existing_file_with_force() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        tokio::fs::write(&destination, b"old").await.unwrap();

        save_stream(
            stream::iter([
                Ok::<_, std::io::Error>(Bytes::from_static(b"new ")),
                Ok(Bytes::from_static(b"content")),
            ]),
            &destination,
            true,
        )
        .await
        .unwrap();

        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"new content");
    }

    #[tokio::test]
    async fn failed_stream_leaves_no_destination_or_temporary_file() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        let stream = stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("stream failed")),
        ]);

        save_stream(stream, &destination, false).await.unwrap_err();

        assert!(!destination.exists());
        let entries = std::fs::read_dir(temp.path()).unwrap().count();
        assert_eq!(entries, 0);
    }
}
