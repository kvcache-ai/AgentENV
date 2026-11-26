use std::fs;
use std::path::Path;

use super::artifacts::sync_dir;
use crate::repository::{LegacyDocumentCollection, RepositoryError, RepositoryResult};

pub(super) fn list_legacy(
    root: &Path,
    collection: LegacyDocumentCollection,
    after: Option<&str>,
    limit: usize,
) -> RepositoryResult<Vec<String>> {
    let directory = root.join(collection.prefix());
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

pub(super) fn read_legacy(
    root: &Path,
    collection: LegacyDocumentCollection,
) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
    let mut documents = Vec::new();
    for id in list_legacy(root, collection, None, usize::MAX)? {
        let key = collection
            .key(&id)
            .ok_or_else(|| RepositoryError::InvalidRequest {
                reason: format!("invalid document id '{id}'"),
            })?;
        // Removed since the listing.
        if let Some(bytes) = read_file(&root.join(key.posix_path()))? {
            documents.push((id, bytes));
        }
    }
    Ok(documents)
}

pub(super) fn read_file(path: &Path) -> RepositoryResult<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RepositoryError::backend(
            format!("read '{}'", path.display()),
            error,
        )),
    }
}

pub(super) fn remove_file_if_exists(path: &Path) -> RepositoryResult<()> {
    match fs::remove_file(path) {
        Ok(()) => path.parent().map_or(Ok(()), sync_dir),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RepositoryError::backend(
            format!("remove '{}'", path.display()),
            error,
        )),
    }
}
