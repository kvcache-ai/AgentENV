use crate::client::Client;
use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};

#[derive(ClapArgs)]
#[command(after_help = "Example:
  aenv upload <sandbox-id> ./config.json /workspace/config.json
  aenv upload --user app <sandbox-id> ./config.json config.json")]
pub struct Args {
    /// Sandbox ID
    sandbox_id: String,
    /// Local file to upload
    local_path: PathBuf,
    /// Destination file path inside the sandbox
    remote_path: String,
    /// User used by envd for relative path resolution and file ownership
    #[arg(long)]
    user: Option<String>,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    rt.block_on(run_async(client, args))
}

async fn run_async(client: Client, args: Args) -> Result<()> {
    let metadata = tokio::fs::metadata(&args.local_path)
        .await
        .with_context(|| format!("reading local file {}", args.local_path.display()))?;
    if !metadata.is_file() {
        bail!(
            "local upload source must be a regular file: {}",
            args.local_path.display()
        );
    }

    let remote_path = resolve_remote_path(&args.local_path, &args.remote_path)?;
    client
        .files(&args.sandbox_id)?
        .upload(&args.local_path, &remote_path, args.user.as_deref())
        .await?;

    println!(
        "Uploaded {} to {}:{}",
        args.local_path.display(),
        args.sandbox_id,
        remote_path
    );
    Ok(())
}

fn resolve_remote_path(local_path: &Path, remote_path: &str) -> Result<String> {
    if remote_path.is_empty() {
        bail!("remote upload destination cannot be empty");
    }
    if !remote_path.ends_with('/') {
        return Ok(remote_path.to_string());
    }

    let file_name = local_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("local filename is not valid UTF-8")?;
    Ok(format!("{remote_path}{file_name}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn rejects_directory_upload_source_before_request() {
        let temp = TempDir::new().unwrap();
        let err = run_async(
            Client::new("http://127.0.0.1:1", "key").unwrap(),
            Args {
                sandbox_id: "sandbox".to_string(),
                local_path: temp.path().to_path_buf(),
                remote_path: "/tmp/file".to_string(),
                user: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("local upload source must be a regular file"));
    }

    #[test]
    fn trailing_slash_remote_path_appends_local_filename() {
        assert_eq!(
            resolve_remote_path(Path::new("Makefile"), "/root/").unwrap(),
            "/root/Makefile"
        );
        assert_eq!(
            resolve_remote_path(Path::new("config.json"), "/workspace/config.json").unwrap(),
            "/workspace/config.json"
        );
    }
}
