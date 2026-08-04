use std::fmt;

use anyhow::{bail, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::types::SandboxId;

type HmacSha256 = Hmac<Sha256>;

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
        let seed = seed.trim();
        if seed.is_empty() {
            bail!("[sandbox].access_token_hash_seed must be configured and non-empty");
        }
        Ok(Self {
            seed: seed.as_bytes().to_vec(),
        })
    }

    pub fn generate(&self, subject: SandboxId) -> EnvdAccessToken {
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject.to_string().as_bytes());
        EnvdAccessToken(hex::encode(mac.finalize().into_bytes()))
    }

    pub fn matches(&self, subject: SandboxId, candidate: &str) -> bool {
        let mut candidate_bytes = [0_u8; 32];
        let decoded = hex::decode_to_slice(candidate, &mut candidate_bytes).is_ok();
        let mut mac =
            HmacSha256::new_from_slice(&self.seed).expect("HMAC accepts keys of any length");
        mac.update(subject.to_string().as_bytes());
        mac.verify_slice(&candidate_bytes).is_ok() & decoded
    }
}

impl fmt::Debug for SandboxAccessTokenGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxAccessTokenGenerator(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_lowercase_hex_hmac_sha256() {
        let generator = SandboxAccessTokenGenerator::new("test-seed").unwrap();
        let subject = SandboxId::try_from("01936f8e-72f5-7000-8000-000000000001").unwrap();

        let token = generator.generate(subject);

        assert_eq!(token.expose().len(), 64);
        assert_eq!(
            token.expose(),
            "4f00f2a93a87c37161ae01c59b6d4f84506668113441277e9f6272dd4bfae1a7"
        );
        assert!(token.expose().bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(token.expose(), token.expose().to_ascii_lowercase());
        assert!(generator.matches(subject, token.expose()));
        assert!(!generator.matches(subject, "not-a-token"));
        assert!(!generator.matches(subject, &"0".repeat(64)));
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
}
