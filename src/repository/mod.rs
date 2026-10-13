pub mod backends;
mod build_cache;
pub mod errors;
pub mod interfaces;
mod layout;

pub use build_cache::{BuildCacheState, BuildCacheStore};
pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{
    BlobBody, BlobLocation, DocumentChange, DocumentUpdate, LayerRole, PublishPolicy, Repository,
};
pub(crate) use layout::is_valid_volume_component;
pub use layout::{BlobKey, DocumentCollection, DocumentKey, SnapshotFile};

#[cfg(test)]
mod tests {
    use std::path::Path;

    /// The modules that build on the repository. Importing one from here
    /// would make the repository depend on its own users.
    const USERS: [&str; 4] = ["snapshot", "image", "volume", "sandbox"];

    fn rust_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }

    #[test]
    fn repository_does_not_depend_on_its_users() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/repository");
        let mut files = Vec::new();
        rust_files(&root, &mut files);
        let mut violations = Vec::new();
        for file in files {
            let source = std::fs::read_to_string(&file).unwrap();
            for user in USERS {
                if source.contains(&format!("crate::{user}::")) {
                    violations.push(format!("{} uses crate::{user}", file.display()));
                }
            }
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }
}
