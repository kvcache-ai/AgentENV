use crate::client::{files::EnvdFilesClient, Client};
use crate::progress::TransferProgress;
use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use envd::filesystem::FileType;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv upload <sandbox-id> ./config.json /workspace/config.json
  aenv upload <sandbox-id> ./project /workspace/
  aenv upload --user app <sandbox-id> ./config.json config.json")]
pub struct Args {
    /// Sandbox ID
    #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
    sandbox_id: String,
    /// Local file or directory to upload
    local_path: PathBuf,
    /// Destination path inside the sandbox
    remote_path: String,
    /// User used by envd for relative file paths and file ownership
    #[arg(long)]
    user: Option<String>,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    rt.block_on(run_async(client, args))
}

async fn run_async(client: Client, args: Args) -> Result<()> {
    let metadata = std::fs::symlink_metadata(&args.local_path)
        .with_context(|| format!("reading local path {}", args.local_path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "symbolic links are not supported: {}",
            args.local_path.display()
        );
    }

    let files = client.files(&args.sandbox_id)?;
    if metadata.is_file() {
        let remote_path = resolve_remote_file_path(&args.local_path, &args.remote_path)?;
        let progress = TransferProgress::new("Uploading", metadata.len())?;
        progress.set_message(args.local_path.display().to_string());
        let result = files
            .upload(
                &args.local_path,
                &remote_path,
                args.user.as_deref(),
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
            "Uploaded {} to {}:{}",
            args.local_path.display(),
            args.sandbox_id,
            remote_path
        );
        return Ok(());
    }
    if !metadata.is_dir() {
        bail!(
            "local upload source must be a regular file or directory: {}",
            args.local_path.display()
        );
    }

    if args.user.is_some() {
        bail!("--user is not supported for directory uploads");
    }
    require_absolute_remote_path(&args.remote_path)?;

    let entries = collect_local_directory(&args.local_path)?;
    let total_bytes = entries
        .iter()
        .filter(|entry| entry.kind == LocalEntryKind::File)
        .try_fold(0u64, |total, entry| {
            total.checked_add(entry.size).with_context(|| {
                format!(
                    "total size overflow while collecting local directory {}",
                    args.local_path.display()
                )
            })
        })?;
    let remote_root =
        resolve_remote_directory_root(&files, &args.local_path, &args.remote_path).await?;
    prepare_remote_directories(&files, &remote_root, &entries).await?;
    let progress = TransferProgress::new("Uploading", total_bytes)?;

    let result = async {
        for entry in entries
            .iter()
            .filter(|entry| entry.kind == LocalEntryKind::File)
        {
            progress.set_message(entry.relative.display().to_string());
            let remote_path = join_remote(&remote_root, &entry.relative)?;
            files
                .upload(&entry.path, &remote_path, None, &progress)
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
        "Uploaded directory {} to {}:{}",
        args.local_path.display(),
        args.sandbox_id,
        remote_root
    );
    Ok(())
}

fn resolve_remote_file_path(local_path: &Path, remote_path: &str) -> Result<String> {
    if remote_path.is_empty() {
        bail!("remote upload destination cannot be empty");
    }
    if !remote_path.ends_with('/') {
        return Ok(remote_path.to_string());
    }

    let file_name = utf8_file_name(local_path, "local filename")?;
    Ok(format!("{remote_path}{file_name}"))
}

async fn resolve_remote_directory_root(
    files: &EnvdFilesClient,
    local_path: &Path,
    remote_path: &str,
) -> Result<String> {
    let source_name = utf8_file_name(local_path, "local directory name")?;
    let normalized = normalize_remote_path(remote_path);
    if remote_path.ends_with('/') {
        return Ok(join_remote_component(&normalized, source_name));
    }

    match files.stat(&normalized, None).await? {
        Some(entry) if entry.r#type == FileType::Directory as i32 => {
            Ok(join_remote_component(&normalized, source_name))
        }
        Some(_) => bail!("remote destination already exists and is not a directory: {normalized}"),
        None => Ok(normalized),
    }
}

async fn prepare_remote_directories(
    files: &EnvdFilesClient,
    remote_root: &str,
    entries: &[LocalEntry],
) -> Result<()> {
    ensure_remote_directory(files, remote_root).await?;
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == LocalEntryKind::Directory)
    {
        let path = join_remote(remote_root, &entry.relative)?;
        ensure_remote_directory(files, &path).await?;
    }
    Ok(())
}

async fn ensure_remote_directory(files: &EnvdFilesClient, path: &str) -> Result<()> {
    match files.stat(path, None).await? {
        Some(entry) if entry.r#type == FileType::Directory as i32 => Ok(()),
        Some(_) => bail!("remote path exists and is not a directory: {path}"),
        None => files.make_dir(path).await,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalEntryKind {
    Directory,
    File,
}

#[derive(Debug)]
struct LocalEntry {
    path: PathBuf,
    relative: PathBuf,
    kind: LocalEntryKind,
    size: u64,
}

fn collect_local_directory(root: &Path) -> Result<Vec<LocalEntry>> {
    let mut entries = Vec::new();
    for entry in WalkDir::new(root).follow_links(false).min_depth(1) {
        let entry = entry.with_context(|| format!("walking {}", root.display()))?;
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            bail!(
                "symbolic links are not supported: {}",
                entry.path().display()
            );
        }
        let kind = if file_type.is_dir() {
            LocalEntryKind::Directory
        } else if file_type.is_file() {
            LocalEntryKind::File
        } else {
            bail!(
                "special files are not supported: {}",
                entry.path().display()
            );
        };
        let relative = entry
            .path()
            .strip_prefix(root)
            .expect("walkdir entries are below their root")
            .to_path_buf();
        validate_relative_path(&relative)?;
        entries.push(LocalEntry {
            path: entry.path().to_path_buf(),
            relative,
            kind,
            size: if kind == LocalEntryKind::File {
                entry
                    .metadata()
                    .with_context(|| format!("reading metadata for {}", entry.path().display()))?
                    .len()
            } else {
                0
            },
        });
    }
    entries.sort_by(|left, right| {
        let left_key = (left.kind == LocalEntryKind::File, &left.relative);
        let right_key = (right.kind == LocalEntryKind::File, &right.relative);
        left_key.cmp(&right_key)
    });
    Ok(entries)
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

fn join_remote(base: &str, relative: &Path) -> Result<String> {
    let mut path = normalize_remote_path(base);
    for component in relative.components() {
        let Component::Normal(component) = component else {
            bail!("invalid relative path: {}", relative.display());
        };
        let component = component
            .to_str()
            .context("directory entries must have valid UTF-8 names")?;
        path = join_remote_component(&path, component);
    }
    Ok(path)
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        for component in path.components() {
            if let Component::Normal(component) = component {
                component
                    .to_str()
                    .context("directory entries must have valid UTF-8 names")?;
            }
        }
        return Ok(());
    }
    bail!("invalid relative path: {}", path.display())
}

fn utf8_file_name<'a>(path: &'a Path, description: &str) -> Result<&'a str> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .with_context(|| format!("{description} is missing or not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn directory_walk_includes_files_and_empty_directories() {
        let temp = TempDir::new().unwrap();
        fs::create_dir(temp.path().join("empty")).unwrap();
        fs::create_dir(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("nested/file.txt"), "content").unwrap();
        fs::write(temp.path().join(".hidden"), "hidden").unwrap();

        let entries = collect_local_directory(temp.path()).unwrap();
        assert!(entries.iter().any(|entry| {
            entry.relative == Path::new("empty") && entry.kind == LocalEntryKind::Directory
        }));
        assert!(entries.iter().any(|entry| {
            entry.relative == Path::new("nested/file.txt") && entry.kind == LocalEntryKind::File
        }));
        assert!(entries.iter().any(|entry| {
            entry.relative == Path::new(".hidden") && entry.kind == LocalEntryKind::File
        }));
    }

    #[cfg(unix)]
    #[test]
    fn directory_walk_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("target"), "content").unwrap();
        symlink("target", temp.path().join("link")).unwrap();

        let error = collect_local_directory(temp.path()).unwrap_err();
        assert!(error
            .to_string()
            .contains("symbolic links are not supported"));
    }

    #[test]
    fn directory_remote_path_must_be_absolute_and_safe() {
        assert!(require_absolute_remote_path("/workspace/project").is_ok());
        assert!(require_absolute_remote_path("workspace/project").is_err());
        assert!(require_absolute_remote_path("/workspace/../root").is_err());
    }

    #[test]
    fn file_destination_with_trailing_slash_appends_filename() {
        assert_eq!(
            resolve_remote_file_path(Path::new("Makefile"), "/root/").unwrap(),
            "/root/Makefile"
        );
    }

    #[test]
    fn remote_path_join_uses_posix_separators() {
        assert_eq!(
            join_remote("/workspace/project", Path::new("src/main.rs")).unwrap(),
            "/workspace/project/src/main.rs"
        );
    }
}
