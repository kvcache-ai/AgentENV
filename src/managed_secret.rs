#[cfg(unix)]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

pub(crate) enum CreateOutcome {
    Created,
    Existing(File),
}

pub(crate) fn read(path: &Path, max_len: usize) -> Result<Option<String>> {
    let parent = path.parent().context("managed secret path has no parent")?;
    match validate_directory(parent) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("validate managed secret directory {}", parent.display())
            });
        }
    }

    match open(path) {
        Ok(file) => read_file(path, file, max_len).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("open managed secret {}", path.display())),
    }
}

pub(crate) fn read_file(path: &Path, mut file: File, max_len: usize) -> Result<String> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect managed secret {}", path.display()))?;
    if !metadata.is_file() {
        bail!("managed secret {} must be a regular file", path.display());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            bail!(
                "managed secret {} must have permissions 0600, found {mode:04o}",
                path.display()
            );
        }
        let expected_uid = nix::unistd::Uid::effective().as_raw();
        if metadata.uid() != expected_uid {
            bail!(
                "managed secret {} must be owned by uid {expected_uid}, found uid {}",
                path.display(),
                metadata.uid()
            );
        }
    }

    if metadata.len() > max_len as u64 {
        bail!(
            "managed secret {} must be at most {max_len} bytes",
            path.display()
        );
    }

    let mut contents = String::with_capacity(max_len);
    Read::by_ref(&mut file)
        .take((max_len + 1) as u64)
        .read_to_string(&mut contents)
        .with_context(|| format!("read managed secret {}", path.display()))?;
    if contents.len() > max_len {
        bail!(
            "managed secret {} must be at most {max_len} bytes",
            path.display()
        );
    }
    Ok(contents)
}

pub(crate) fn create(path: &Path, contents: &[u8]) -> Result<CreateOutcome> {
    ensure_supported()?;
    let parent = path.parent().context("managed secret path has no parent")?;
    create_directory(parent)?;
    validate_directory_identity(parent).with_context(|| {
        format!(
            "validate managed secret directory ownership {}",
            parent.display()
        )
    })?;
    set_permissions(parent, 0o700)?;
    validate_directory(parent)
        .with_context(|| format!("validate managed secret directory {}", parent.display()))?;

    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary secret in {}", parent.display()))?;
    set_permissions(temporary.path(), 0o600)?;
    temporary
        .write_all(contents)
        .with_context(|| format!("write temporary secret in {}", parent.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("sync temporary secret in {}", parent.display()))?;

    match temporary.persist_noclobber(path) {
        Ok(_) => {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .with_context(|| format!("sync managed secret directory {}", parent.display()))?;
            Ok(CreateOutcome::Created)
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            validate_directory(parent).with_context(|| {
                format!("validate managed secret directory {}", parent.display())
            })?;
            open(path)
                .map(CreateOutcome::Existing)
                .with_context(|| format!("open managed secret {}", path.display()))
        }
        Err(error) => {
            Err(error.error).with_context(|| format!("persist managed secret {}", path.display()))
        }
    }
}

fn create_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        builder.mode(0o700);
    }

    builder
        .create(path)
        .with_context(|| format!("create managed secret directory {}", path.display()))
}

#[cfg(unix)]
fn open(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);

    use std::os::unix::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);

    options.open(path)
}

#[cfg(not(unix))]
fn open(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "managed secrets require Unix no-follow file semantics",
    ))
}

#[cfg(unix)]
fn ensure_supported() -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_supported() -> Result<()> {
    bail!("managed secrets require Unix no-follow file semantics")
}

fn validate_directory(path: &Path) -> io::Result<()> {
    let metadata = validate_directory_identity(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("must have permissions 0700, found {mode:04o}"),
            ));
        }
    }

    Ok(())
}

fn validate_directory_identity(path: &Path) -> io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "must be a directory and not a symbolic link",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let expected_uid = nix::unistd::Uid::effective().as_raw();
        if metadata.uid() != expected_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "must be owned by uid {expected_uid}, found uid {}",
                    metadata.uid()
                ),
            ));
        }
    }

    Ok(metadata)
}

#[cfg(unix)]
fn set_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_permissions(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}
