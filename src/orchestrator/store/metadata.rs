use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

use crate::orchestrator::SandboxState;
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::{PausedSandboxState, SandboxNetworkPolicy};
use crate::snapshot::{CommandContext, SnapshotRuntimeVersions, StartupCommand};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};
use crate::virtualization::VirtualizationMode;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SandboxTimeoutAction {
    Pause,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NewTimeout {
    UseExisting,
    Set(Duration),
    EnsureMinimum(Duration),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxMetadata {
    pub id: SandboxId,
    pub snapshot_id: String,
    pub snapshot_alias: Option<String>,
    pub state: SandboxState,
    pub created_at: SystemTime,
    pub timeout: Option<Duration>,
    pub timeout_action: SandboxTimeoutAction,
    pub expires_at: Option<SystemTime>,
    pub auto_resume: bool,
    /// Virtualization mode used by this sandbox for its entire lifecycle.
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    pub runtime_versions: SnapshotRuntimeVersions,
    pub resources: SandboxResources,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    pub user_metadata: Option<HashMap<String, String>>,
    pub network_policy: SandboxNetworkPolicy,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Persisted into committed snapshots so template launches inherit it
    /// unless overridden at create time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Whether envd requires the access token derived from this sandbox's ID.
    /// Older records deserialize as non-secure sandboxes.
    #[serde(default)]
    pub secure: bool,
    /// Paused state produced by the sandbox backend during `pause`.
    /// Passed back to the backend factory when `resume_sandbox` is called.
    #[serde(skip)]
    pub paused_state: Option<Arc<dyn PausedSandboxState>>,
}

impl Default for SandboxMetadata {
    fn default() -> Self {
        Self {
            id: SandboxId::new(),
            snapshot_id: "unknown".to_string(),
            snapshot_alias: None,
            state: SandboxState::Creating,
            created_at: SystemTime::now(),
            timeout: None,
            timeout_action: SandboxTimeoutAction::Pause,
            expires_at: None,
            auto_resume: false,
            virtualization_mode: VirtualizationMode::default(),
            runtime_versions: SnapshotRuntimeVersions::new(
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
            ),
            resources: SandboxResources::default(),
            context: CommandContext::default(),
            startup: None,
            image_configs: ImageConfigs::new(),
            user_metadata: None,
            network_policy: SandboxNetworkPolicy::default(),
            custom_extension_params: None,
            secure: false,
            paused_state: None,
        }
    }
}

impl SandboxMetadata {
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self._set_timeout(timeout, SystemTime::now());
    }

    fn _set_timeout(&mut self, timeout: Option<Duration>, from: SystemTime) {
        self.timeout = timeout;
        self.expires_at = timeout.and_then(|ttl| from.checked_add(ttl));
    }

    pub fn update_timeout(&mut self, new_timeout: NewTimeout) {
        self._update_timeout(new_timeout, SystemTime::now());
    }

    fn _update_timeout(&mut self, new_timeout: NewTimeout, from: SystemTime) {
        let timeout = match new_timeout {
            NewTimeout::UseExisting => self.timeout,
            NewTimeout::Set(timeout) => Some(timeout),
            NewTimeout::EnsureMinimum(minimum) => match self.timeout {
                Some(existing) => Some(existing.max(minimum)),
                None => Some(minimum),
            },
            NewTimeout::None => None,
        };
        self._set_timeout(timeout, from);
    }

    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn metadata_timeout_work() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata {
            created_at: base,
            ..Default::default()
        };
        metadata._set_timeout(Some(Duration::from_secs(10)), base);

        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(10)));
        assert!(!metadata.is_expired(base + Duration::from_secs(9)));
        assert!(metadata.is_expired(base + Duration::from_secs(10)));

        metadata.set_timeout(None);
        assert_eq!(metadata.timeout, None);
        assert_eq!(metadata.expires_at, None);
    }

    #[test]
    fn update_timeout_ensure_minimum_respects_longer_existing_timeout() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata::default();
        metadata.set_timeout(Some(Duration::from_secs(900)));

        metadata._update_timeout(NewTimeout::EnsureMinimum(Duration::from_secs(300)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(900)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(900)));
    }

    #[test]
    fn update_timeout_supports_set_use_existing_and_clear() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata::default();
        metadata.set_timeout(Some(Duration::from_secs(120)));

        metadata._update_timeout(NewTimeout::EnsureMinimum(Duration::from_secs(300)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(300)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));

        metadata._update_timeout(NewTimeout::UseExisting, base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(300)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));

        metadata._update_timeout(NewTimeout::Set(Duration::from_secs(45)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(45)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(45)));

        metadata.update_timeout(NewTimeout::None);
        assert_eq!(metadata.timeout, None);
        assert_eq!(metadata.expires_at, None);
    }
}
