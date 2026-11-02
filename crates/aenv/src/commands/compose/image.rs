//! The Compose guest image source in images/compose, embedded so `aenv compose
//! up` can build the image with the server's builder without a checkout.

use anyhow::{Context, Result};
use std::path::Path;

macro_rules! source {
    ($($path:literal),* $(,)?) => {
        &[$((
            $path,
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../images/compose/", $path))
                as &[u8],
        )),*]
    };
}

const SOURCE: &[(&str, &[u8])] = source!(
    ".dockerignore",
    "Dockerfile",
    "containerd.toml",
    "daemon.json",
    "init",
    "plain-snapshotter/go.mod",
    "plain-snapshotter/go.sum",
    "plain-snapshotter/main.go",
    "plain-snapshotter/snapshotter.go",
);

/// FNV-1a over every file's path, length and contents, so a changed source
/// gets a new image name.
fn fingerprint() -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (path, data) in SOURCE {
        let len = (data.len() as u64).to_le_bytes();
        for &byte in path.as_bytes().iter().chain(&[0]).chain(&len).chain(*data) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// Image name, as `aenv build --image --name` takes it.
fn image_name() -> String {
    format!("aenv-compose:{:016x}", fingerprint())
}

/// Image reference for cold starts.
pub(super) fn image_reference() -> String {
    format!("aenv/{}", image_name())
}

/// The server reports an unknown image tag as `image tag '...' was not found`.
pub(super) fn is_missing(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains(&image_name()) && message.contains("was not found")
}

fn write_source(dir: &Path) -> Result<()> {
    for (path, data) in SOURCE {
        let path = dir.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

/// Builds the image with the server's builder, reporting progress on stderr.
pub(super) fn build() -> Result<()> {
    let context = tempfile::Builder::new()
        .prefix("aenv-compose-image-")
        .tempdir()?;
    write_source(context.path())?;
    super::super::build::compose_image(context.path().to_owned(), image_name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn embeds_every_image_source_file() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../images/compose");
        let mut files: Vec<String> = walkdir::WalkDir::new(&root)
            .into_iter()
            .map(Result::unwrap)
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| {
                let path = entry.path().strip_prefix(&root).unwrap();
                path.to_str().unwrap().replace('\\', "/")
            })
            .filter(|path| !path.ends_with("README.md") && !path.ends_with(".gitignore"))
            .collect();
        files.sort();
        let mut embedded: Vec<&str> = SOURCE.iter().map(|(path, _)| *path).collect();
        embedded.sort();
        assert_eq!(files, embedded);
    }

    #[test]
    fn names_the_image_by_source_fingerprint() {
        let reference = image_reference();
        let tag = reference.strip_prefix("aenv/aenv-compose:").unwrap();
        assert_eq!(tag.len(), 16);
        assert!(tag.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(reference, image_reference());
    }

    #[test]
    fn detects_only_a_missing_managed_image() {
        let missing = anyhow::anyhow!("HTTP 400: image tag '{}' was not found", image_name());
        assert!(is_missing(&missing));
        assert!(!is_missing(&anyhow::anyhow!(
            "HTTP 400: image tag 'aenv-compose:0000000000000000' was not found"
        )));
        assert!(!is_missing(&anyhow::anyhow!("HTTP 500: {}", image_name())));
    }

    #[test]
    fn writes_the_build_context() {
        let dir = tempfile::tempdir().unwrap();
        write_source(dir.path()).unwrap();
        for (path, data) in SOURCE {
            assert_eq!(std::fs::read(dir.path().join(path)).unwrap(), *data);
        }
    }
}
