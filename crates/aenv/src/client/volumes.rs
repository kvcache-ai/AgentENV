use super::{handle_status, Client};
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    #[serde(rename = "volumeID")]
    pub volume_id: String,
    pub name: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default, rename = "sizeMB")]
    pub size_mb: u64,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateVolume<'a> {
    pub name: &'a str,
    #[serde(rename = "sizeMB")]
    pub size_mb: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_volume: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<&'a str>,
}

impl Client {
    pub fn create_volume(&self, request: &CreateVolume<'_>) -> Result<Volume> {
        Ok(handle_status(self.post("/volumes").send_json(request))?.into_json()?)
    }

    pub fn list_volumes(&self) -> Result<Vec<Volume>> {
        let mut volumes = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut request = self.get("/volumes").query("limit", "100");
            if let Some(token) = next_token.as_deref() {
                request = request.query("nextToken", token);
            }
            let response = handle_status(request.call())?;
            next_token = response
                .header("x-next-token")
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(str::to_string);
            volumes.append(&mut response.into_json()?);
            if next_token.is_none() {
                return Ok(volumes);
            }
        }
    }

    pub fn get_volume(&self, reference: &str) -> Result<Volume> {
        Ok(handle_status(self.get(&format!("/volumes/{reference}")).call())?.into_json()?)
    }

    pub fn delete_volume(&self, reference: &str) -> Result<()> {
        handle_status(self.delete(&format!("/volumes/{reference}")).call())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CreateVolume;

    #[test]
    fn create_volume_serializes_issue_fields() {
        let request = CreateVolume {
            name: "my-data",
            size_mb: 2048,
            mode: Some("exclusive"),
            from_volume: Some("parent"),
            image: None,
        };
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "name": "my-data",
                "sizeMB": 2048,
                "mode": "exclusive",
                "fromVolume": "parent"
            })
        );
    }
}
