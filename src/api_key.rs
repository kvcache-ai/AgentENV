use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use rand::{rngs::SysRng, TryRng};
use tracing::info;

use crate::cfg::AppConfig;

const API_KEY_ENV: &str = "AENV_API_KEY";
const EXTERNAL_API_KEY_PATH: &str = "/run/secrets/api-key";
const MANAGED_API_KEY_RELATIVE_PATH: &str = "secrets/api-key";
const GENERATED_API_KEY_PREFIX: &str = "e2b_";

pub fn resolve(config: &AppConfig) -> Result<String> {
    resolve_from(
        std::env::var_os(API_KEY_ENV).as_deref(),
        Path::new(EXTERNAL_API_KEY_PATH),
        &config.home_path,
    )
}

fn resolve_from(
    explicit: Option<&OsStr>,
    external_path: &Path,
    home_path: &Path,
) -> Result<String> {
    if let Some(explicit) = explicit {
        return validate(
            explicit
                .to_str()
                .context("AENV_API_KEY must contain valid UTF-8")?,
        )
        .context("invalid AENV_API_KEY");
    }

    match read(external_path) {
        Ok(key) => {
            info!(path = %external_path.display(), "loaded API key from external secret");
            return Ok(key);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("load external API key"),
    }

    let managed_path = home_path.join(MANAGED_API_KEY_RELATIVE_PATH);
    match read(&managed_path) {
        Ok(key) => return Ok(key),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("load managed API key"),
    }

    create(&managed_path)
}

fn read(path: &Path) -> Result<String, io::Error> {
    let value = fs::read_to_string(path)?;
    validate(&value).map_err(io::Error::other)
}

fn validate(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() < 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-'))
    {
        bail!("API key must contain at least 32 URL-safe characters");
    }
    Ok(value.to_owned())
}

fn create(path: &Path) -> Result<String> {
    let parent = path
        .parent()
        .context("managed API key path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create managed secret directory {}", parent.display()))?;
    set_permissions(parent, 0o700)?;

    let mut random = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut random)
        .context("generate managed API key")?;
    let key = format!("{GENERATED_API_KEY_PREFIX}{}", hex::encode(random));

    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary API key in {}", parent.display()))?;
    set_permissions(temporary.path(), 0o600)?;
    writeln!(temporary, "{key}")?;
    temporary.as_file().sync_all()?;

    match temporary.persist_noclobber(path) {
        Ok(_) => {
            File::open(parent)?.sync_all()?;
            info!(path = %path.display(), "generated managed API key");
            Ok(key)
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            read(path).context("load concurrently generated API key")
        }
        Err(error) => {
            Err(error.error).with_context(|| format!("persist managed API key {}", path.display()))
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    const TEST_KEY: &str = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn configured_sources_take_precedence() -> Result<()> {
        let temp = TempDir::new()?;
        let external_path = temp.path().join("external");
        fs::write(&external_path, format!("{TEST_KEY}\n"))?;

        assert_eq!(
            resolve_from(Some(OsStr::new(TEST_KEY)), &external_path, temp.path())?,
            TEST_KEY
        );
        assert_eq!(resolve_from(None, &external_path, temp.path())?, TEST_KEY);
        assert!(!temp.path().join(MANAGED_API_KEY_RELATIVE_PATH).exists());
        Ok(())
    }

    #[test]
    fn managed_key_is_private_and_stable() -> Result<()> {
        let temp = TempDir::new()?;
        let missing_external = temp.path().join("missing");
        let first = resolve_from(None, &missing_external, temp.path())?;

        assert_eq!(resolve_from(None, &missing_external, temp.path())?, first);
        assert!(first.starts_with(GENERATED_API_KEY_PREFIX));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let path = temp.path().join(MANAGED_API_KEY_RELATIVE_PATH);
            assert_eq!(
                fs::metadata(path.parent().unwrap())?.permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
        }
        Ok(())
    }

    #[test]
    fn concurrent_creation_converges() -> Result<()> {
        const THREADS: usize = 8;
        let temp = TempDir::new()?;
        let home_path = Arc::new(temp.path().to_owned());
        let external_path = Arc::new(temp.path().join("missing"));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles = (0..THREADS)
            .map(|_| {
                let home_path = Arc::clone(&home_path);
                let external_path = Arc::clone(&external_path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    resolve_from(None, &external_path, &home_path)
                })
            })
            .collect::<Vec<_>>();

        let keys = handles
            .into_iter()
            .map(|handle| handle.join().expect("API key creation thread panicked"))
            .collect::<Result<Vec<_>>>()?;
        assert!(keys.iter().all(|key| key == &keys[0]));
        Ok(())
    }
}
