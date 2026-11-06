//! Copies the local files a Compose project binds into the guest at the same
//! absolute paths, where the rendered project's bind mounts and config and
//! secret files refer to them, as a remote Docker daemon's host would.

use anyhow::{ensure, Context, Result};
use flate2::{write::GzEncoder, Compression};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Bounds the uncompressed size of the copied files.
const MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// Where the CLI uploads the archive in the guest.
pub(super) const GUEST_ARCHIVE: &str = "/tmp/agentenv-compose-files.tar.gz";

/// A gzip tar of the bound files, named by their absolute paths without the
/// leading `/`.
pub(super) struct Archive {
    pub file: tempfile::NamedTempFile,
    /// Directories that must exist in the guest before extraction.
    pub parents: Vec<String>,
}

/// Packs the paths that exist on this machine, or returns `None` if none
/// does: like Docker, the guest Compose creates a missing bind source unless
/// it sets `create_host_path: false`. As for a bind mount, a symlinked path
/// contributes its target; symlinks inside directories stay links.
pub(super) fn pack(paths: &[PathBuf]) -> Result<Option<Archive>> {
    pack_limited(paths, MAX_BYTES)
}

fn pack_limited(paths: &[PathBuf], limit: u64) -> Result<Option<Archive>> {
    let file = tempfile::NamedTempFile::new().context("preparing the project files archive")?;
    let mut bundle = tar::Builder::new(GzEncoder::new(file.reopen()?, Compression::default()));
    bundle.follow_symlinks(false);
    let mut parents = Vec::new();
    let mut total = 0;
    for path in paths {
        let source = match path.canonicalize() {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        let name = path
            .strip_prefix("/")
            .with_context(|| format!("{} is not an absolute path", path.display()))?;
        let parent = path
            .parent()
            .and_then(Path::to_str)
            .with_context(|| format!("{} must be a UTF-8 path below /", path.display()))?;
        parents.push(parent.to_owned());
        for entry in WalkDir::new(&source).follow_links(false) {
            let entry = entry?;
            let file_type = entry.file_type();
            // Sockets, pipes and devices cannot be copied.
            if !(file_type.is_file() || file_type.is_dir() || file_type.is_symlink()) {
                continue;
            }
            if file_type.is_file() {
                total += entry.metadata()?.len();
                ensure!(
                    total <= limit,
                    "local files bound by the Compose project exceed {} MiB; bind fewer or smaller paths",
                    limit / (1024 * 1024)
                );
            }
            let relative = entry.path().strip_prefix(&source)?;
            let target = if relative.as_os_str().is_empty() {
                name.to_owned()
            } else {
                name.join(relative)
            };
            bundle
                .append_path_with_name(entry.path(), &target)
                .with_context(|| format!("packing {}", entry.path().display()))?;
        }
    }
    bundle.into_inner()?.finish()?;
    Ok((!parents.is_empty()).then_some(Archive { file, parents }))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::os::unix::fs::symlink;

    fn entries(archive: &Archive) -> Vec<(String, tar::EntryType)> {
        let mut bundle = tar::Archive::new(GzDecoder::new(archive.file.reopen().unwrap()));
        let mut result: Vec<_> = bundle
            .entries()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let path = entry
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .trim_end_matches('/')
                    .to_owned();
                (path, entry.header().entry_type())
            })
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    #[test]
    fn packs_bound_paths_under_their_absolute_names() {
        let work = tempfile::tempdir().unwrap();
        let root = work.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("site/assets")).unwrap();
        std::fs::write(root.join("site/index.html"), "index").unwrap();
        std::fs::write(root.join("site/assets/app.js"), "app").unwrap();
        symlink("index.html", root.join("site/home.html")).unwrap();
        std::fs::write(root.join("app.conf"), "conf").unwrap();
        symlink(root.join("app.conf"), root.join("linked.conf")).unwrap();
        let paths = [
            root.join("site"),
            root.join("linked.conf"),
            root.join("missing"),
        ];
        let archive = pack(&paths).unwrap().unwrap();
        let name = |path: &str| {
            root.join(path)
                .strip_prefix("/")
                .unwrap()
                .display()
                .to_string()
        };
        assert_eq!(
            entries(&archive),
            vec![
                (name("linked.conf"), tar::EntryType::Regular),
                (name("site"), tar::EntryType::Directory),
                (name("site/assets"), tar::EntryType::Directory),
                (name("site/assets/app.js"), tar::EntryType::Regular),
                (name("site/home.html"), tar::EntryType::Symlink),
                (name("site/index.html"), tar::EntryType::Regular),
            ]
        );
        let parent = root.to_str().unwrap().to_owned();
        assert_eq!(archive.parents, [parent.clone(), parent]);
    }

    #[test]
    fn skips_missing_paths_and_bounds_the_size() {
        let work = tempfile::tempdir().unwrap();
        assert!(pack(&[work.path().join("missing")]).unwrap().is_none());
        std::fs::write(work.path().join("large"), vec![0; 2048]).unwrap();
        let error = pack_limited(&[work.path().join("large")], 1024)
            .err()
            .unwrap();
        assert!(error.to_string().contains("exceed"), "{error}");
    }
}
