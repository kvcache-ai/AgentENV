use serde::{Deserialize, Serialize};

/// A single source image config entry, associating a guest mount path with the
/// raw (opaque) image config JSON from the source OCI image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageConfigEntry {
    /// Drive identifier for attached drives, `None` for the rootfs.
    #[serde(rename = "driveId", default, skip_serializing_if = "Option::is_none")]
    pub drive_id: Option<String>,
    /// Guest mount path for this image. By convention, `"/"` denotes the rootfs.
    #[serde(rename = "mountPath")]
    pub mount_path: String,
    /// Raw source image config JSON, preserved as-is without field-level
    /// interpretation. Typically the `"config"` object from an OCI image
    /// config document (containing `Cmd`, `Entrypoint`, `Env`, etc.).
    pub config: serde_json::Value,
}

/// Collection of source image configs resolved at sandbox creation time.
///
/// Serialises as a flat JSON array of [`ImageConfigEntry`] objects. An empty
/// collection serialises as `[]` and is skipped by `skip_serializing_if` at
/// the embedding site.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ImageConfigs(Vec<ImageConfigEntry>);

impl ImageConfigs {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add a config entry. Pass `None` for `drive_id` when adding the rootfs.
    pub fn add(
        &mut self,
        drive_id: Option<impl Into<String>>,
        mount_path: impl Into<String>,
        config: serde_json::Value,
    ) {
        self.0.push(ImageConfigEntry {
            drive_id: drive_id.map(Into::into),
            mount_path: mount_path.into(),
            config,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn entries(&self) -> &[ImageConfigEntry] {
        &self.0
    }

    /// Return the raw config associated with the rootfs image, if present.
    pub fn rootfs_config(&self) -> Option<&serde_json::Value> {
        self.0
            .iter()
            .find(|entry| entry.drive_id.is_none() && entry.mount_path == "/")
            .map(|entry| &entry.config)
    }

    /// Serialize into a `serde_json::Value` (JSON array) suitable for
    /// embedding into the MMDS extra metadata map.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Array(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_as_flat_array() {
        let mut configs = ImageConfigs::new();
        configs.add(Some("data"), "/mnt/data", json!({"Env": ["DATA=1"]}));
        configs.add(None::<String>, "/", json!({"Cmd": ["/bin/sh"]}));

        let value = serde_json::to_value(&configs).expect("serialize");
        assert_eq!(
            value,
            json!([
                { "driveId": "data", "mountPath": "/mnt/data", "config": {"Env": ["DATA=1"]} },
                { "mountPath": "/", "config": {"Cmd": ["/bin/sh"]} },
            ])
        );
        assert_eq!(configs.rootfs_config(), Some(&json!({"Cmd": ["/bin/sh"]})));
    }

    #[test]
    fn empty_serializes_as_empty_array() {
        let configs = ImageConfigs::new();
        let value = serde_json::to_value(&configs).expect("serialize");
        assert_eq!(value, json!([]));
    }

    #[test]
    fn round_trips() {
        let mut original = ImageConfigs::new();
        original.add(None::<String>, "/", json!({"WorkingDir": "/workspace"}));
        let json = serde_json::to_string(&original).expect("serialize");
        let restored: ImageConfigs = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    #[test]
    fn deserializes_empty_array() {
        let configs: ImageConfigs = serde_json::from_str("[]").expect("deserialize");
        assert!(configs.is_empty());
    }
}
