use super::{handle_status, Client};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub struct SandboxVolumeMount {
    pub name: String,
    pub path: String,
}

fn volume_mounts_request(
    mounts: Option<HashMap<String, String>>,
) -> Option<Vec<SandboxVolumeMount>> {
    mounts.map(|mounts| {
        mounts
            .into_iter()
            .map(|(path, name)| SandboxVolumeMount { name, path })
            .collect()
    })
}

#[derive(Debug, Serialize)]
pub struct NewSandbox<'a> {
    #[serde(rename = "templateID")]
    pub template_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    pub secure: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "volumeMounts")]
    pub volume_mounts: Option<Vec<SandboxVolumeMount>>,
}

#[derive(Debug, Serialize)]
pub struct NewColdSandbox<'a> {
    pub image: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "cpuCount")]
    pub cpu_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "memoryMB")]
    pub memory_mb: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "diskSizeMB")]
    pub disk_size_mb: Option<u32>,
    pub secure: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "volumeMounts")]
    pub volume_mounts: Option<Vec<SandboxVolumeMount>>,
}

#[derive(Deserialize)]
pub struct Sandbox {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(default, rename = "envdAccessToken")]
    pub envd_access_token: Option<String>,
    #[serde(default, rename = "trafficAccessToken")]
    pub traffic_access_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NewComposeSandbox<'a> {
    pub compose: &'a str,
    #[serde(skip_serializing_if = "HashMap::is_empty", rename = "composeEnv")]
    pub compose_env: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>,
    pub timeout: u32,
    #[serde(rename = "startupTimeout")]
    pub startup_timeout: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "cpuCount")]
    pub cpu_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "memoryMB")]
    pub memory_mb: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "diskSizeMB")]
    pub disk_size_mb: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct RefreshSandbox {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u32>,
}

#[derive(Deserialize)]
pub struct SandboxDetail {
    pub state: String,
    #[serde(default, rename = "envdAccessToken")]
    pub envd_access_token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListedSandbox {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, rename = "cpuCount")]
    pub cpu_count: Option<u32>,
    #[serde(default, rename = "memoryMB")]
    pub memory_mib: Option<u32>,
    #[serde(default, rename = "diskSizeMB")]
    pub disk_size_mib: Option<u32>,
    #[serde(default, rename = "startedAt")]
    pub started_at: Option<String>,
    #[serde(default, rename = "endAt")]
    pub end_at: Option<String>,
}

impl Client {
    pub fn create_sandbox(
        &self,
        template_id: &str,
        timeout: Option<u32>,
        volume_mounts: Option<HashMap<String, String>>,
    ) -> Result<Sandbox> {
        let body = NewSandbox {
            template_id,
            timeout,
            secure: true,
            volume_mounts: volume_mounts_request(volume_mounts),
        };
        let resp = handle_status(self.post("/sandboxes").send_json(&body))?;
        let sandbox: Sandbox = resp.into_json()?;
        Ok(sandbox)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_cold_sandbox(
        &self,
        image: &str,
        timeout: Option<u32>,
        cpu_count: Option<u32>,
        memory_mb: Option<u32>,
        disk_size_mb: Option<u32>,
        volume_mounts: Option<HashMap<String, String>>,
    ) -> Result<Sandbox> {
        let body = NewColdSandbox {
            image,
            timeout,
            cpu_count,
            memory_mb,
            disk_size_mb,
            secure: true,
            volume_mounts: volume_mounts_request(volume_mounts),
        };
        let resp = handle_status(self.post("/sandboxes-cold").send_json(&body))?;
        let sandbox: Sandbox = resp.into_json()?;
        Ok(sandbox)
    }

    pub fn list_sandboxes(&self) -> Result<Vec<ListedSandbox>> {
        let resp = handle_status(self.get("/v2/sandboxes").call())?;
        Ok(resp.into_json()?)
    }

    pub fn create_compose_sandbox(&self, body: &NewComposeSandbox<'_>) -> Result<Sandbox> {
        // Compose startup can take 300 seconds, beyond the client's default
        // 120-second timeout. Allow additional time for server-side cleanup.
        let timeout = Duration::from_secs(u64::from(body.startup_timeout) + 60);
        let resp = handle_status(
            self.post("/sandboxes-compose")
                .timeout(timeout)
                .send_json(body),
        )?;
        Ok(resp.into_json()?)
    }

    pub fn delete_sandbox(&self, id: &str) -> Result<()> {
        handle_status(self.delete(&format!("/sandboxes/{}", id)).call())?;
        Ok(())
    }

    pub fn pause_sandbox(&self, id: &str) -> Result<()> {
        handle_status(self.post(&format!("/sandboxes/{}/pause", id)).call())?;
        Ok(())
    }

    pub fn sandbox_state_with_timeout(
        &self,
        id: &str,
        timeout: Duration,
    ) -> Result<Option<String>> {
        let resp = match self
            .get(&format!("/sandboxes/{}", id))
            .timeout(timeout)
            .call()
        {
            Ok(resp) => resp,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(err) => handle_status(Err(err))?,
        };
        let detail: SandboxDetail = resp.into_json()?;
        Ok(Some(detail.state))
    }

    pub fn get_sandbox(&self, id: &str) -> Result<SandboxDetail> {
        let resp = handle_status(self.get(&format!("/sandboxes/{id}")).call())?;
        Ok(resp.into_json()?)
    }

    /// `connect` resumes a paused sandbox or extends the TTL of a running one.
    pub fn connect_sandbox(&self, id: &str, timeout: u32) -> Result<Sandbox> {
        let resp = handle_status(
            self.post(&format!("/sandboxes/{}/connect", id))
                .send_json(json!({ "timeout": timeout })),
        )?;
        Ok(resp.into_json()?)
    }

    pub fn set_timeout(&self, id: &str, timeout: u32) -> Result<()> {
        handle_status(
            self.post(&format!("/sandboxes/{}/timeout", id))
                .send_json(json!({ "timeout": timeout })),
        )?;
        Ok(())
    }

    pub fn refresh_sandbox(&self, id: &str, duration: Option<u32>) -> Result<()> {
        let body = RefreshSandbox { duration };
        handle_status(
            self.post(&format!("/sandboxes/{}/refreshes", id))
                .send_json(&body),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, NewColdSandbox, NewComposeSandbox, NewSandbox, RefreshSandbox};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::time::Duration;

    #[test]
    fn compose_request_uses_auth_contract_and_overrides_default_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = Client::new_with_timeouts(
            &format!("http://{address}"),
            "test-key",
            Duration::from_secs(5),
            Duration::from_millis(20),
        )
        .unwrap();
        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut reader = BufReader::new(&mut conn);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line);
            }
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .map(str::to_owned)
                })
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            std::thread::sleep(Duration::from_millis(100));
            let response = r#"{"sandboxID":"compose-sandbox","envdAccessToken":"guest-token"}"#;
            write!(conn, "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            (headers, body)
        });
        let request = NewComposeSandbox {
            compose: "services: {app: {image: '${IMAGE}'}}",
            compose_env: [("IMAGE".to_owned(), "busybox:1.37".to_owned())].into(),
            profiles: vec!["worker".to_owned()],
            timeout: 600,
            startup_timeout: 300,
            cpu_count: Some(2),
            memory_mb: Some(2048),
            disk_size_mb: Some(8192),
        };
        let sandbox = client.create_compose_sandbox(&request).unwrap();
        let (headers, body) = server.join().unwrap();
        assert!(headers.starts_with("POST /sandboxes-compose HTTP/1.1\r\n"));
        assert!(headers
            .to_ascii_lowercase()
            .contains("x-api-key: test-key\r\n"));
        assert_eq!(
            body,
            serde_json::json!({
                "compose": request.compose,
                "composeEnv": {"IMAGE": "busybox:1.37"},
                "profiles": ["worker"],
                "timeout": 600,
                "startupTimeout": 300,
                "cpuCount": 2,
                "memoryMB": 2048,
                "diskSizeMB": 8192,
            })
        );
        assert_eq!(sandbox.sandbox_id, "compose-sandbox");
        assert_eq!(sandbox.envd_access_token.as_deref(), Some("guest-token"));
    }

    #[test]
    fn new_sandbox_serializes_template_start() {
        let body = NewSandbox {
            template_id: "base-template",
            timeout: Some(300),
            secure: true,
            volume_mounts: None,
        };

        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["templateID"], "base-template");
        assert_eq!(value["timeout"], 300);
        assert_eq!(value["secure"], true);
        assert!(value.get("cpuCount").is_none());
        assert!(value.get("memoryMB").is_none());
    }

    #[test]
    fn new_cold_sandbox_serializes_resource_overrides() {
        let body = NewColdSandbox {
            image: "ubuntu:24.04",
            timeout: Some(300),
            cpu_count: Some(2),
            memory_mb: Some(1024),
            disk_size_mb: Some(8192),
            secure: true,
            volume_mounts: None,
        };

        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["image"], "ubuntu:24.04");
        assert_eq!(value["timeout"], 300);
        assert_eq!(value["cpuCount"], 2);
        assert_eq!(value["memoryMB"], 1024);
        assert_eq!(value["diskSizeMB"], 8192);
        assert_eq!(value["secure"], true);
        assert!(value.get("templateID").is_none());
    }

    #[test]
    fn refresh_sandbox_body_serializes_optional_duration() {
        let value = serde_json::to_value(RefreshSandbox {
            duration: Some(300),
        })
        .unwrap();

        assert_eq!(value["duration"], 300);

        let empty = serde_json::to_value(RefreshSandbox { duration: None }).unwrap();
        assert!(empty.get("duration").is_none());
    }
}
