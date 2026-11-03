use std::fmt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use rand::{rngs::SysRng, TryRng};
use sha2::Sha256;
use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::managed_secret::{self, CreateOutcome};
use crate::types::SandboxId;

type HmacSha256 = Hmac<Sha256>;

const MANAGED_SEED_RELATIVE_PATH: &str = "secrets/sandbox-access-token-hash-seed";
const MANAGED_SEED_BYTES: usize = 32;
const SEED_HEX_LEN: usize = MANAGED_SEED_BYTES * 2;
const MANAGED_SEED_FILE_MAX_LEN: usize = SEED_HEX_LEN + 1;
const TRAFFIC_ACCESS_TOKEN_PREFIX: &str = "sandbox-traffic";

#[derive(Clone, PartialEq, Eq)]
pub struct EnvdAccessToken(String);

impl EnvdAccessToken {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EnvdAccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnvdAccessToken(<redacted>)")
    }
}

#[derive(Clone)]
pub struct SandboxAccessTokenGenerator {
    seed: Vec<u8>,
}

impl SandboxAccessTokenGenerator {
    pub fn new(seed: &str) -> Result<Self> {
        let seed = validate_explicit_seed(seed)?;
        Ok(Self {
            seed: seed.as_bytes().to_vec(),
        })
    }

    pub(crate) fn load_or_create(
        config: &AppConfig,
        managed_seed_must_exist: bool,
    ) -> Result<Self> {
        if let Some(seed) = config.sandbox.access_token_hash_seed.as_deref() {
            return Self::new(seed);
        }

        let managed_seed_path = config.home_path.join(MANAGED_SEED_RELATIVE_PATH);
        let seed = resolve_seed(&managed_seed_path, managed_seed_must_exist)?;

        if config.cluster.scheduler_endpoint.is_some() {
            warn!(
                path = %managed_seed_path.display(),
                "using a node-local managed sandbox access-token seed; configure AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED with the same value on every node in a clustered deployment"
            );
        }

        Self::new(&seed)
    }

    pub fn generate(&self, subject: SandboxId) -> EnvdAccessToken {
        EnvdAccessToken(self.generate_for(subject.to_string().as_bytes()))
    }

    pub fn generate_traffic(&self, subject: SandboxId) -> String {
        self.generate_for(format!("{TRAFFIC_ACCESS_TOKEN_PREFIX}-{subject}").as_bytes())
    }

    fn generate_for(&self, subject: &[u8]) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject);
        hex::encode(mac.finalize().into_bytes())
    }

    pub fn matches(&self, subject: SandboxId, candidate: &str) -> bool {
        self.matches_for(subject.to_string().as_bytes(), candidate)
    }

    pub fn matches_traffic(&self, subject: SandboxId, candidate: &str) -> bool {
        self.matches_for(
            format!("{TRAFFIC_ACCESS_TOKEN_PREFIX}-{subject}").as_bytes(),
            candidate,
        )
    }

    fn matches_for(&self, subject: &[u8], candidate: &str) -> bool {
        let mut candidate_bytes = [0_u8; 32];
        let decoded = hex::decode_to_slice(candidate, &mut candidate_bytes).is_ok();
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject);
        mac.verify_slice(&candidate_bytes).is_ok() & decoded
    }
}

fn validate_explicit_seed(seed: &str) -> Result<&str> {
    let seed = seed.trim();
    if seed.is_empty() {
        bail!("[sandbox].access_token_hash_seed must be non-empty when configured");
    }
    Ok(seed)
}

fn resolve_seed(managed_path: &Path, managed_seed_must_exist: bool) -> Result<String> {
    if let Some(contents) = managed_secret::read(managed_path, MANAGED_SEED_FILE_MAX_LEN)? {
        return validate_managed_seed(managed_path, &contents);
    }

    if managed_seed_must_exist {
        bail!(
            "managed sandbox access-token seed {} is missing while token-protected persisted sandboxes exist; restore the file or configure AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED",
            managed_path.display()
        );
    }

    create_managed_seed(managed_path)
}

fn validate_managed_seed(path: &Path, contents: &str) -> Result<String> {
    let seed = contents.strip_suffix('\n').unwrap_or(contents);
    if !is_valid_managed_seed(seed) {
        bail!(
            "managed sandbox access-token seed {} must contain exactly {SEED_HEX_LEN} lowercase hexadecimal characters, optionally followed by a newline",
            path.display()
        );
    }

    Ok(seed.to_owned())
}

fn create_managed_seed(path: &Path) -> Result<String> {
    let mut random = [0_u8; MANAGED_SEED_BYTES];
    SysRng
        .try_fill_bytes(&mut random)
        .context("generate managed sandbox access-token seed")?;
    let seed = hex::encode(random);

    match managed_secret::create(path, format!("{seed}\n").as_bytes())? {
        CreateOutcome::Created => {
            info!(path = %path.display(), "generated managed sandbox access-token seed");
            Ok(seed)
        }
        CreateOutcome::Existing(file) => validate_managed_seed(
            path,
            &managed_secret::read_file(path, file, MANAGED_SEED_FILE_MAX_LEN)?,
        ),
    }
}

fn is_valid_managed_seed(seed: &str) -> bool {
    seed.len() == SEED_HEX_LEN
        && seed
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl fmt::Debug for SandboxAccessTokenGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxAccessTokenGenerator(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    fn create_private_managed_seed_directory(path: &Path) -> Result<()> {
        fs::create_dir_all(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    #[test]
    fn generates_e2b_compatible_access_tokens() {
        let generator = SandboxAccessTokenGenerator::new("test-seed").unwrap();
        let subject = SandboxId::try_from("01936f8e-72f5-7000-8000-000000000001").unwrap();

        let envd_token = generator.generate(subject);
        let traffic_token = generator.generate_traffic(subject);

        assert_eq!(
            envd_token.expose(),
            "4f00f2a93a87c37161ae01c59b6d4f84506668113441277e9f6272dd4bfae1a7"
        );
        assert_eq!(
            traffic_token,
            "586547d7c10facb0f4871297fdbfd9d2b4376f4b02b2e1487646c1c87a293bd8"
        );
        assert!(envd_token
            .expose()
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit()));
        assert!(traffic_token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(generator.matches(subject, envd_token.expose()));
        assert!(generator.matches_traffic(subject, &traffic_token));
        assert!(!generator.matches(subject, "not-a-token"));
        assert!(!generator.matches(subject, &"0".repeat(64)));
        assert!(!generator.matches_traffic(subject, envd_token.expose()));
    }

    #[test]
    fn rejects_empty_seed_and_redacts_secrets() {
        assert!(SandboxAccessTokenGenerator::new("  ").is_err());
        let generator = SandboxAccessTokenGenerator::new("super-secret").unwrap();
        let subject = SandboxId::default();
        let token = generator.generate(subject);

        assert!(!format!("{generator:?}").contains("super-secret"));
        assert!(!format!("{token:?}").contains(token.expose()));
    }

    #[test]
    fn explicit_seed_takes_precedence_without_creating_managed_state() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let config = AppConfig {
            home_path: temp.path().to_owned(),
            sandbox: crate::cfg::SandboxConfig {
                access_token_hash_seed: Some("configured-seed".to_owned()),
                ..Default::default()
            },
            ..Default::default()
        };

        let generator = SandboxAccessTokenGenerator::load_or_create(&config, false)?;

        assert_eq!(generator.seed, "configured-seed".as_bytes());
        assert!(!managed_path.exists());
        Ok(())
    }

    #[test]
    fn managed_seed_is_private_and_stable() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);

        let first = resolve_seed(&managed_path, false)?;
        let second = resolve_seed(&managed_path, false)?;

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let directory_mode = fs::metadata(managed_path.parent().unwrap())?
                .permissions()
                .mode()
                & 0o777;
            let file_mode = fs::metadata(&managed_path)?.permissions().mode() & 0o777;
            assert_eq!(directory_mode, 0o700);
            assert_eq!(file_mode, 0o600);
        }

        let subject = SandboxId::new();
        let first_generator = SandboxAccessTokenGenerator::new(&first)?;
        let second_generator = SandboxAccessTokenGenerator::new(&second)?;
        assert_eq!(
            first_generator.generate(subject),
            second_generator.generate(subject)
        );
        Ok(())
    }

    #[test]
    fn concurrent_managed_seed_creation_converges() -> Result<()> {
        const THREADS: usize = 8;

        let temp = TempDir::new()?;
        let managed_path = Arc::new(temp.path().join(MANAGED_SEED_RELATIVE_PATH));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles = (0..THREADS)
            .map(|_| {
                let managed_path = Arc::clone(&managed_path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    resolve_seed(&managed_path, false)
                })
            })
            .collect::<Vec<_>>();

        let seeds = handles
            .into_iter()
            .map(|handle| handle.join().expect("seed creation thread panicked"))
            .collect::<Result<Vec<_>>>()?;
        assert!(seeds.iter().all(|seed| seed == &seeds[0]));
        Ok(())
    }

    #[test]
    fn empty_or_invalid_managed_seed_is_not_replaced() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;

        for contents in ["", "invalid\n"] {
            fs::write(&managed_path, contents)?;
            set_test_permissions(&managed_path, 0o600)?;

            let error = resolve_seed(&managed_path, false).unwrap_err();

            assert!(error.to_string().contains("64 lowercase hexadecimal"));
            assert_eq!(fs::read_to_string(&managed_path)?, contents);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn permissive_managed_seed_is_rejected() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        fs::write(&managed_path, format!("{}\n", "a".repeat(64)))?;
        set_test_permissions(&managed_path, 0o640)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error.to_string().contains("permissions 0600"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn managed_seed_symlink_is_rejected() -> Result<()> {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let target_path = temp.path().join("seed-target");
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        fs::write(&target_path, format!("{}\n", "a".repeat(64)))?;
        set_test_permissions(&target_path, 0o600)?;
        symlink(&target_path, &managed_path)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error.to_string().contains("open managed secret"));
        Ok(())
    }

    #[test]
    fn oversized_managed_seed_is_rejected_before_reading_contents() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        create_private_managed_seed_directory(managed_path.parent().unwrap())?;
        let file = fs::File::create(&managed_path)?;
        file.set_len(1024 * 1024)?;
        set_test_permissions(&managed_path, 0o600)?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(error.to_string().contains("must be at most 65 bytes"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn managed_seed_directory_symlink_is_rejected() -> Result<()> {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);
        let target_directory = temp.path().join("target-secrets");
        create_private_managed_seed_directory(&target_directory)?;
        symlink(&target_directory, managed_path.parent().unwrap())?;

        let error = resolve_seed(&managed_path, false).unwrap_err();

        assert!(format!("{error:#}").contains("not a symbolic link"));
        Ok(())
    }

    #[test]
    fn missing_managed_seed_is_not_recreated_for_persisted_state() -> Result<()> {
        let temp = TempDir::new()?;
        let managed_path = temp.path().join(MANAGED_SEED_RELATIVE_PATH);

        let error = resolve_seed(&managed_path, true).unwrap_err();

        assert!(error.to_string().contains("persisted sandboxes exist"));
        assert!(!managed_path.exists());
        Ok(())
    }

    #[cfg(unix)]
    fn set_test_permissions(path: &Path, mode: u32) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn set_test_permissions(_path: &Path, _mode: u32) -> Result<()> {
        Ok(())
    }
}
