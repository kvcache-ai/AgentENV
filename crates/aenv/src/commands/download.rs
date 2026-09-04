use crate::client::files::TRANSFER_IDLE_TIMEOUT;
use crate::client::{files::EnvdFilesClient, Client};
use crate::progress::TransferProgress;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Args as ClapArgs;
use envd::filesystem::FileType;
use futures::{Stream, StreamExt};
use std::path::{Component, Path, PathBuf};
use tempfile::Builder;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv download <sandbox-id> /workspace/result.txt ./result.txt
  aenv download <sandbox-id> /workspace/result.txt
  aenv download <sandbox-id> /workspace/project ./backup/
  aenv download --user app --force <sandbox-id> result.txt ./result.txt")]
pub struct Args {
    /// Sandbox ID
    #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
    sandbox_id: String,
    /// Source file or directory path inside the sandbox
    remote_path: String,
    /// Local destination file or directory. Defaults to the current directory
    local_path: Option<PathBuf>,
    /// User whose home directory resolves relative remote file paths
    #[arg(long)]
    user: Option<String>,
    /// Replace conflicting local files
    #[arg(long)]
    force: bool,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    rt.block_on(run_async(client, args))
}

async fn run_async(client: Client, args: Args) -> Result<()> {
    let files = client.files(&args.sandbox_id)?;
    let remote_entry = files.stat(&args.remote_path, args.user.as_deref()).await?;
    let Some(remote_entry) = remote_entry else {
        bail!("remote path does not exist: {}", args.remote_path);
    };

    if remote_entry.r#type == FileType::File as i32 {
        let local_path = resolve_local_root(&args.remote_path, args.local_path.as_deref())?;
        validate_file_destination(&local_path, args.force)?;
        let total_bytes = remote_file_size(remote_entry.size, &args.remote_path)?;
        let progress = TransferProgress::new("Downloading", total_bytes)?;
        progress.set_message(args.remote_path.clone());
        let result = download_file(
            &files,
            &args.remote_path,
            args.user.as_deref(),
            &local_path,
            args.force,
            &progress,
        )
        .await;
        if result.is_ok() {
            progress.finish();
        } else {
            progress.abandon();
        }
        result?;
        println!(
            "Downloaded {}:{} to {}",
            args.sandbox_id,
            args.remote_path,
            local_path.display()
        );
        return Ok(());
    }
    if remote_entry.r#type != FileType::Directory as i32 {
        bail!(
            "remote source is not a regular file or directory: {}",
            args.remote_path
        );
    }

    if args.user.is_some() {
        bail!("--user is not supported for directory downloads");
    }
    require_absolute_remote_path(&args.remote_path)?;

    let local_root = resolve_local_root(&args.remote_path, args.local_path.as_deref())?;
    let entries = collect_remote_directory(&files, &args.remote_path).await?;
    let total_bytes = entries
        .iter()
        .filter(|entry| entry.kind == RemoteEntryKind::File)
        .try_fold(0u64, |total, entry| {
            total.checked_add(entry.size).with_context(|| {
                format!(
                    "total size overflow while collecting remote directory {}",
                    args.remote_path
                )
            })
        })?;
    preflight_directory_download(&local_root, &entries, args.force)?;
    create_local_directories(&local_root, &entries)?;
    let progress = TransferProgress::new("Downloading", total_bytes)?;

    let result = async {
        for entry in entries
            .iter()
            .filter(|entry| entry.kind == RemoteEntryKind::File)
        {
            progress.set_message(entry.relative.display().to_string());
            let destination = local_root.join(&entry.relative);
            download_file(
                &files,
                &entry.remote_path,
                None,
                &destination,
                args.force,
                &progress,
            )
            .await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if result.is_ok() {
        progress.finish();
    } else {
        progress.abandon();
    }
    result?;

    println!(
        "Downloaded directory {}:{} to {}",
        args.sandbox_id,
        args.remote_path,
        local_root.display()
    );
    Ok(())
}

async fn download_file(
    files: &EnvdFilesClient,
    remote_path: &str,
    username: Option<&str>,
    local_path: &Path,
    force: bool,
    progress: &TransferProgress,
) -> Result<()> {
    let response = files.download(remote_path, username).await?;
    save_stream(response.bytes_stream(), local_path, force, progress).await
}

fn resolve_local_root(remote_path: &str, local_path: Option<&Path>) -> Result<PathBuf> {
    let remote_file_name = remote_file_name(remote_path)?;
    let local_path = local_path.unwrap_or_else(|| Path::new("."));

    match std::fs::symlink_metadata(local_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("symbolic links are not supported: {}", local_path.display())
        }
        Ok(metadata) if metadata.is_dir() => Ok(local_path.join(remote_file_name)),
        Ok(_) => Ok(local_path.to_path_buf()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if path_ends_with_separator(local_path) {
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

fn path_ends_with_separator(path: &Path) -> bool {
    path.as_os_str()
        .as_encoded_bytes()
        .last()
        .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
}

fn remote_file_name(remote_path: &str) -> Result<&str> {
    let remote_path = remote_path.trim_end_matches('/');
    if remote_path.is_empty() {
        bail!("remote source must include a file or directory name");
    }
    remote_path
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .context("remote source must include a valid file or directory name")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteEntryKind {
    Directory,
    File,
}

#[derive(Debug)]
struct RemoteEntry {
    remote_path: String,
    relative: PathBuf,
    kind: RemoteEntryKind,
    size: u64,
}

async fn collect_remote_directory(
    files: &EnvdFilesClient,
    remote_root: &str,
) -> Result<Vec<RemoteEntry>> {
    let remote_root = normalize_remote_path(remote_root);
    let mut pending = vec![(remote_root.clone(), PathBuf::new())];
    let mut entries = Vec::new();

    while let Some((directory, relative_directory)) = pending.pop() {
        let children = files.list_dir(&directory).await?;
        for child in children {
            validate_remote_entry_name(&child.name)?;
            let remote_path = join_remote_component(&directory, &child.name);
            let relative = relative_directory.join(&child.name);
            validate_relative_path(&relative)?;
            let kind = if child.r#type == FileType::Directory as i32 {
                RemoteEntryKind::Directory
            } else if child.r#type == FileType::File as i32 {
                RemoteEntryKind::File
            } else if child.r#type == FileType::Symlink as i32 {
                bail!("symbolic links are not supported: {remote_path}");
            } else {
                bail!("special remote files are not supported: {remote_path}");
            };
            entries.push(RemoteEntry {
                remote_path: remote_path.clone(),
                relative: relative.clone(),
                kind,
                size: if kind == RemoteEntryKind::File {
                    remote_file_size(child.size, &remote_path)?
                } else {
                    0
                },
            });
            if kind == RemoteEntryKind::Directory {
                pending.push((remote_path, relative));
            }
        }
    }

    entries.sort_by(|left, right| {
        let left_key = (left.kind == RemoteEntryKind::File, &left.relative);
        let right_key = (right.kind == RemoteEntryKind::File, &right.relative);
        left_key.cmp(&right_key)
    });
    Ok(entries)
}

fn remote_file_size(size: i64, remote_path: &str) -> Result<u64> {
    u64::try_from(size)
        .with_context(|| format!("remote file has negative size {size}: {remote_path}"))
}

fn preflight_directory_download(
    local_root: &Path,
    entries: &[RemoteEntry],
    force: bool,
) -> Result<()> {
    validate_root_parent(local_root)?;
    validate_expected_directory(local_root)?;

    for entry in entries {
        let destination = local_root.join(&entry.relative);
        match entry.kind {
            RemoteEntryKind::Directory => validate_expected_directory(&destination)?,
            RemoteEntryKind::File => validate_expected_file(&destination, force)?,
        }
    }
    Ok(())
}

fn validate_root_parent(local_root: &Path) -> Result<()> {
    let parent = destination_parent(local_root);
    let metadata = std::fs::symlink_metadata(parent)
        .with_context(|| format!("reading destination directory {}", parent.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("symbolic links are not supported: {}", parent.display());
    }
    if !metadata.is_dir() {
        bail!(
            "download destination parent is not a directory: {}",
            parent.display()
        );
    }
    validate_existing_ancestors(local_root)
}

fn validate_existing_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("symbolic links are not supported: {}", current.display())
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| format!("reading {}", current.display()));
            }
        }
    }
    Ok(())
}

fn validate_expected_directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("symbolic links are not supported: {}", path.display())
        }
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => bail!(
            "local path exists and is not a directory: {}",
            path.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

fn validate_expected_file(path: &Path, force: bool) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("symbolic links are not supported: {}", path.display())
        }
        Ok(metadata) if metadata.is_dir() => {
            bail!("local path exists and is a directory: {}", path.display())
        }
        Ok(metadata) if !metadata.is_file() => {
            bail!("special local files are not supported: {}", path.display())
        }
        Ok(_) if !force => bail!(
            "local download destination already exists: {}; use --force to replace it",
            path.display()
        ),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

fn create_local_directories(local_root: &Path, entries: &[RemoteEntry]) -> Result<()> {
    std::fs::create_dir_all(local_root)
        .with_context(|| format!("creating directory {}", local_root.display()))?;
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == RemoteEntryKind::Directory)
    {
        let path = local_root.join(&entry.relative);
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating directory {}", path.display()))?;
    }
    Ok(())
}

fn validate_file_destination(destination: &Path, force: bool) -> Result<()> {
    if destination.file_name().is_none() {
        bail!(
            "local download destination must be a file path: {}",
            destination.display()
        );
    }
    validate_root_parent(destination)?;
    validate_expected_file(destination, force)
}

fn destination_parent(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

async fn save_stream<S, E>(
    stream: S,
    destination: &Path,
    force: bool,
    progress: &TransferProgress,
) -> Result<()>
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
    loop {
        let next = timeout(TRANSFER_IDLE_TIMEOUT, stream.next())
            .await
            .with_context(|| {
                format!(
                    "download made no progress for {} seconds",
                    TRANSFER_IDLE_TIMEOUT.as_secs()
                )
            })?;
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|err| anyhow!(err).context("reading download stream"))?;
        progress.inc(chunk.len() as u64);
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

fn require_absolute_remote_path(path: &str) -> Result<()> {
    if !path.starts_with('/') {
        bail!("directory transfers require an absolute remote path: {path}");
    }
    if path.split('/').any(|component| component == "..") {
        bail!("remote path must not contain '..': {path}");
    }
    Ok(())
}

fn normalize_remote_path(path: &str) -> String {
    if path == "/" {
        "/".to_string()
    } else {
        path.trim_end_matches('/').to_string()
    }
}

fn join_remote_component(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}

fn validate_remote_entry_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        bail!("invalid remote directory entry name: {name:?}");
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return Ok(());
    }
    bail!("invalid relative path: {}", path.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tempfile::TempDir;

    #[test]
    fn omitted_destination_uses_remote_name_in_current_directory() {
        assert_eq!(
            resolve_local_root("/workspace/project", None).unwrap(),
            PathBuf::from("./project")
        );
    }

    #[test]
    fn directory_destination_appends_remote_name() {
        let temp = TempDir::new().unwrap();
        assert_eq!(
            resolve_local_root("/workspace/project", Some(temp.path())).unwrap(),
            temp.path().join("project")
        );
    }

    #[test]
    fn explicit_destination_is_preserved() {
        assert_eq!(
            resolve_local_root("/workspace/project", Some(Path::new("./backup"))).unwrap(),
            PathBuf::from("./backup")
        );
    }

    #[test]
    fn nonexistent_destination_with_forward_slash_appends_remote_name() {
        assert_eq!(
            resolve_local_root("/workspace/project", Some(Path::new("backup/"))).unwrap(),
            PathBuf::from("backup/project")
        );
    }

    #[cfg(windows)]
    #[test]
    fn nonexistent_destination_with_backslash_appends_remote_name() {
        assert_eq!(
            resolve_local_root("/workspace/project", Some(Path::new(r"backup\"))).unwrap(),
            PathBuf::from(r"backup\project")
        );
    }

    #[test]
    fn remote_file_size_rejects_negative_values() {
        let error = remote_file_size(-1, "/workspace/result.txt").unwrap_err();
        assert!(error.to_string().contains("negative size -1"));
    }

    #[test]
    fn preflight_merges_directories_and_requires_force_for_files() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/file.txt"), "old").unwrap();
        let entries = vec![
            RemoteEntry {
                remote_path: "/project/nested".into(),
                relative: PathBuf::from("nested"),
                kind: RemoteEntryKind::Directory,
                size: 0,
            },
            RemoteEntry {
                remote_path: "/project/nested/file.txt".into(),
                relative: PathBuf::from("nested/file.txt"),
                kind: RemoteEntryKind::File,
                size: 3,
            },
        ];

        assert!(preflight_directory_download(&root, &entries, false).is_err());
        preflight_directory_download(&root, &entries, true).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn preflight_rejects_symlink_destinations() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let root = temp.path().join("project");
        symlink(&target, &root).unwrap();

        let error = preflight_directory_download(&root, &[], false).unwrap_err();
        assert!(error
            .to_string()
            .contains("symbolic links are not supported"));
    }

    #[test]
    fn directory_remote_path_must_be_absolute() {
        assert!(require_absolute_remote_path("/workspace/project").is_ok());
        assert!(require_absolute_remote_path("workspace/project").is_err());
    }

    #[tokio::test]
    async fn save_stream_does_not_replace_existing_file_without_force() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        tokio::fs::write(&destination, b"old").await.unwrap();

        let error = save_stream(
            stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"new"))]),
            &destination,
            false,
            &TransferProgress::new("Downloading", 3).unwrap(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("use --force"));
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn save_stream_replaces_existing_file_with_force() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        tokio::fs::write(&destination, b"old").await.unwrap();

        save_stream(
            stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"new"))]),
            &destination,
            true,
            &TransferProgress::new("Downloading", 3).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"new");
    }

    #[tokio::test]
    async fn failed_stream_leaves_no_destination_or_temporary_file() {
        let temp = TempDir::new().unwrap();
        let destination = temp.path().join("result.txt");
        let stream = stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("stream failed")),
        ]);

        save_stream(
            stream,
            &destination,
            false,
            &TransferProgress::new("Downloading", 7).unwrap(),
        )
        .await
        .unwrap_err();
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
