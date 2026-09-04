pub mod files;
pub mod sandboxes;
pub mod snapshots;
pub mod templates;
pub mod volumes;

use crate::auth::Credentials;
use crate::grpc::Transport;
use anyhow::{anyhow, Result};
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
        Self::new_with_timeouts(
            url,
            api_key,
            Duration::from_secs(5),
            Duration::from_secs(120),
        )
    }

    pub fn new_with_timeouts(
        url: &str,
        api_key: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(connect_timeout)
            .timeout(request_timeout)
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
            Err(format_status_error(code, &body))
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!(t).context("transport error")),
    }
}

fn format_status_error(code: u16, body: &str) -> anyhow::Error {
    let detail = parse_api_error(body)
        .or_else(|| (!body.trim().is_empty()).then(|| body.trim().to_string()));

    if code == 401 {
        return match detail {
            Some(detail) => anyhow!(
                "authentication failed (HTTP 401 Unauthorized): {detail}; run `aenv auth` to update your API key"
            ),
            None => anyhow!(
                "authentication failed (HTTP 401 Unauthorized); run `aenv auth` to update your API key"
            ),
        };
    }

    anyhow!("HTTP {}: {}", code, detail.unwrap_or_default())
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
#[cfg(test)]
mod tests {
    use super::format_status_error;

    #[test]
    fn unauthorized_error_explains_how_to_reauthenticate() {
        assert_eq!(
            format_status_error(401, "").to_string(),
            "authentication failed (HTTP 401 Unauthorized); run `aenv auth` to update your API key"
        );
    }
}
