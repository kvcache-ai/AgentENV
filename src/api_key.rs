use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use anyhow::{bail, Context, Result};
use rand::{rngs::SysRng, TryRng};
use tracing::info;

use crate::cfg::AppConfig;
use crate::managed_secret::{self, CreateOutcome};

const API_KEY_ENV: &str = "AENV_API_KEY";
const EXTERNAL_API_KEY_PATH: &str = "/run/secrets/api-key";
const MANAGED_API_KEY_RELATIVE_PATH: &str = "secrets/api-key";
const API_KEY_MAX_LEN: usize = 4096;
const API_KEY_FILE_MAX_LEN: usize = API_KEY_MAX_LEN + 2;
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

    match read_external(external_path) {
        Ok(key) => {
            info!(path = %external_path.display(), "loaded API key from external secret");
            return Ok(key);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("load external API key"),
    }

    let managed_path = home_path.join(MANAGED_API_KEY_RELATIVE_PATH);
    match fs::symlink_metadata(&managed_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return create(&managed_path),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect managed API key {}", managed_path.display()));
        }
        Ok(_) => {}
    }
    if let Some(value) =
        managed_secret::read(&managed_path, API_KEY_FILE_MAX_LEN).context("load managed API key")?
    {
        return validate_file_contents(&value).context("invalid managed API key");
    }

    create(&managed_path)
}

fn read_external(path: &Path) -> Result<String, io::Error> {
    let value = read_bounded(File::open(path)?)?;
    validate_file_contents(&value).map_err(io::Error::other)
}

fn read_bounded(file: File) -> Result<String, io::Error> {
    let mut value = String::with_capacity(API_KEY_FILE_MAX_LEN);
    file.take((API_KEY_FILE_MAX_LEN + 1) as u64)
        .read_to_string(&mut value)?;
    if value.len() > API_KEY_FILE_MAX_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("API key file must be at most {API_KEY_FILE_MAX_LEN} bytes"),
        ));
    }
    Ok(value)
}

fn validate(value: &str) -> Result<String> {
    if !(32..=API_KEY_MAX_LEN).contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-'))
    {
        bail!("API key must contain between 32 and {API_KEY_MAX_LEN} URL-safe characters");
    }
    Ok(value.to_owned())
}

fn validate_file_contents(value: &str) -> Result<String> {
    let value = value.strip_suffix('\n').unwrap_or(value);
    validate(value.strip_suffix('\r').unwrap_or(value))
}

fn create(path: &Path) -> Result<String> {
    let mut random = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut random)
        .context("generate managed API key")?;
    let key = format!("{GENERATED_API_KEY_PREFIX}{}", hex::encode(random));

    match managed_secret::create(path, format!("{key}\n").as_bytes())? {
        CreateOutcome::Created => {
            info!(path = %path.display(), "generated managed API key");
            Ok(key)
        }
        CreateOutcome::Existing(file) => {
            let value = managed_secret::read_file(path, file, API_KEY_FILE_MAX_LEN)
                .context("load concurrently generated API key")?;
            validate_file_contents(&value).context("invalid concurrently generated API key")
        }
    }
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
