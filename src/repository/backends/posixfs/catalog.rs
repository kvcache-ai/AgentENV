use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::artifacts::sync_dir;

use crate::repository::{DocumentCollection, DocumentKey, RepositoryError, RepositoryResult};

/// Shared documents in a POSIX repository: one file each, written atomically
/// via temporary file + rename.
pub struct PosixFsCatalogStore {
    root: PathBuf,
}

impl PosixFsCatalogStore {
    /// Creates a document store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) fn get_document(&self, key: &DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        let path = self.root.join(key.posix_path());
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RepositoryError::backend(
                format!("read '{}'", path.display()),
                error,
            )),
        }
    }

    pub(crate) fn put_document(&self, key: &DocumentKey, bytes: &[u8]) -> RepositoryResult<()> {
        self.write_bytes(&self.root.join(key.posix_path()), bytes)
    }

    pub(crate) fn delete_document(&self, key: &DocumentKey) -> RepositoryResult<()> {
        self.remove_file_if_exists(&self.root.join(key.posix_path()))
    }

    pub(crate) fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let directory = self.root.join(collection.prefix());
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read '{}'", directory.display()),
                    error,
                ))
            }
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(format!("read '{}'", directory.display()), error)
            })?;
            let is_file = entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(format!("inspect '{}'", entry.path().display()), error)
                })?
                .is_file();
            let file_name = entry.file_name();
            let Some(id) = file_name
                .to_str()
                .and_then(|name| collection.id_from_file_name(name))
                .filter(|_| is_file)
            else {
                continue;
            };
            if after.is_none_or(|after| id > after) {
                ids.push(id.to_owned());
            }
        }
        ids.sort_unstable();
        ids.truncate(limit);
        Ok(ids)
    }

    pub(crate) fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        let mut documents = Vec::new();
        for id in self.list_documents(collection, None, usize::MAX)? {
            let key = collection
                .key(&id)
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("invalid document id '{id}'"),
                })?;
            // Removed since the listing.
            if let Some(bytes) = self.get_document(&key)? {
                documents.push((id, bytes));
            }
        }
        Ok(documents)
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> RepositoryResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write temp json '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.as_file().sync_all().map_err(|error| {
            RepositoryError::backend(format!("sync temp json '{}'", temp.path().display()), error)
        })?;
        let tmp_path = temp.path().to_path_buf();
        temp.persist(path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "persist json '{}' -> '{}'",
                    tmp_path.display(),
                    path.display()
                ),
                error.error,
            )
        })?;
        sync_dir(parent)
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => path.parent().map_or(Ok(()), sync_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }
}
