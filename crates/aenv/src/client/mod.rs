pub mod files;
pub mod sandboxes;
pub mod snapshots;
pub mod templates;

use crate::auth::Credentials;
use crate::grpc::Transport;
use anyhow::{anyhow, bail, Result};
use std::time::Duration;
use ureq::Agent;

#[derive(Clone)]
pub struct Client {
    agent: Agent,
    base: String,
    api_key: String,
}

impl Client {
    pub fn from_env() -> Result<Self> {
        let creds = Credentials::load()?;
        Self::new(&creds.url, &creds.api_key)
    }

    pub fn new(url: &str, api_key: &str) -> Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build();
        Ok(Self {
            agent,
            base,
            api_key: api_key.to_string(),
        })
    }

    pub fn transport(
        &self,
        sandbox_id: &str,
        envd_access_token: Option<&str>,
    ) -> Result<Transport> {
        Transport::new(&self.base, sandbox_id, envd_access_token)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    pub fn get(&self, path: &str) -> ureq::Request {
        self.agent
            .get(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }

    pub fn post(&self, path: &str) -> ureq::Request {
        self.agent
            .post(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }

    pub fn delete(&self, path: &str) -> ureq::Request {
        self.agent
            .delete(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }
}

impl Credentials {
    pub fn load() -> Result<Self> {
        crate::auth::load()
    }
}

pub fn handle_status(resp: Result<ureq::Response, ureq::Error>) -> Result<ureq::Response> {
    match resp {
        Ok(r) => Ok(r),
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            let msg = parse_api_error(&body).unwrap_or_else(|| body.clone());
            bail!("HTTP {}: {}", code, msg.trim())
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!(t).context("transport error")),
    }
}

fn parse_api_error(body: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ApiError {
        message: Option<String>,
    }
    serde_json::from_str::<ApiError>(body)
        .ok()
        .and_then(|e| e.message)
}
