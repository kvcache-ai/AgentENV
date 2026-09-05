use super::{handle_status, Client};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub struct NewSandbox<'a> {
    #[serde(rename = "templateID")]
    pub template_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    pub secure: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "volumeMounts")]
    pub volume_mounts: Option<HashMap<String, String>>,
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
    pub volume_mounts: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct Sandbox {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(default, rename = "envdAccessToken")]
    pub envd_access_token: Option<String>,
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

#[derive(Debug, Serialize)]
struct SandboxCpuAffinityRequest<'a> {
    vcpu: &'a str,
    core: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SandboxCpuAffinity {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    pub vcpu: String,
    pub cores: String,
    #[serde(rename = "ignoredOfflineCores")]
    pub ignored_offline_cores: String,
    #[serde(rename = "boundThreadCount")]
    pub bound_thread_count: u32,
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
            volume_mounts,
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
            volume_mounts,
        };
        let resp = handle_status(self.post("/sandboxes-cold").send_json(&body))?;
        let sandbox: Sandbox = resp.into_json()?;
        Ok(sandbox)
    }

    pub fn list_sandboxes(&self) -> Result<Vec<ListedSandbox>> {
        let resp = handle_status(self.get("/v2/sandboxes").call())?;
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

    pub fn bind_cpu_affinity(
        &self,
        id: &str,
        vcpu: &str,
        core: &str,
    ) -> Result<SandboxCpuAffinity> {
        let id = uuid::Uuid::parse_str(id)
            .with_context(|| format!("invalid sandbox ID {id:?}"))?
            .to_string();
        let body = SandboxCpuAffinityRequest { vcpu, core };
        let resp = handle_status(
            self.post(&format!("/sandboxes/{id}/cpu-affinity"))
                .send_json(&body),
        )?;
        let result: SandboxCpuAffinity = resp.into_json()?;
        ensure!(
            result.sandbox_id == id,
            "CPU affinity response sandbox mismatch: requested {id}, got {}",
            result.sandbox_id
        );
        Ok(result)
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
    use super::{NewColdSandbox, NewSandbox, RefreshSandbox, SandboxCpuAffinity};
    use crate::client::Client;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    fn serve_json_once(response_body: &'static str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            let _ = request_tx.send(String::from_utf8(request).unwrap());

            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .unwrap();
        });
        (format!("http://{address}"), request_rx)
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

    #[test]
    fn bind_cpu_affinity_uses_expected_http_contract() {
        let (url, request) = serve_json_once(
            r#"{"sandboxID":"01936f8e-72f5-7000-8000-0000000000ab","vcpu":"*","cores":"2-3","ignoredOfflineCores":"","boundThreadCount":4}"#,
        );
        let client = Client::new(&url, "secret-key").unwrap();
        let result = client
            .bind_cpu_affinity("01936F8E-72F5-7000-8000-0000000000AB", "*", "2-3")
            .unwrap();
        assert_eq!(
            result,
            SandboxCpuAffinity {
                sandbox_id: "01936f8e-72f5-7000-8000-0000000000ab".into(),
                vcpu: "*".into(),
                cores: "2-3".into(),
                ignored_offline_cores: String::new(),
                bound_thread_count: 4,
            }
        );

        let request = request.recv().unwrap();
        assert!(request.starts_with(
            "POST /sandboxes/01936f8e-72f5-7000-8000-0000000000ab/cpu-affinity HTTP/1.1\r\n"
        ));
        let body = request.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({"vcpu": "*", "core": "2-3"})
        );
    }

    #[test]
    fn bind_cpu_affinity_rejects_path_special_sandbox_ids() {
        let client = Client::new("http://127.0.0.1:1", "secret-key").unwrap();

        for id in ["sandbox/other", "sandbox?admin=true"] {
            let error = client.bind_cpu_affinity(id, "*", "0").unwrap_err();
            assert!(error.to_string().contains("invalid sandbox ID"));
        }
    }
}
