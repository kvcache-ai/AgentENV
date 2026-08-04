use std::sync::Arc;

use super::store::{NewTimeout, SandboxMetadata};
use super::types::SandboxState;
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxLaunchConfig,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::{SandboxId, SandboxResources};

pub(super) struct CreateLaunchPlan {
    pub sandbox_id: SandboxId,
    pub source: CreateLaunchSource,
    pub launch_config: SandboxLaunchConfig,
    pub metadata: SandboxMetadata,
    pub timeout: NewTimeout,
}

pub(super) enum CreateLaunchSource {
    Snapshot {
        snapshot: Box<RunnableSnapshot>,
    },
    Fresh {
        build_spec: Box<FreshSandboxBuildSpec>,
    },
}

pub(super) struct ResumeLaunchPlan {
    pub sandbox_id: SandboxId,
    pub paused_state: Arc<dyn PausedSandboxState>,
    pub timeout: NewTimeout,
    pub resources: SandboxResources,
    pub envd_access_token: Option<EnvdAccessToken>,
}

pub(super) enum LaunchPlan {
    Create(Box<CreateLaunchPlan>),
    Resume(Box<ResumeLaunchPlan>),
}

impl LaunchPlan {
    pub(super) fn for_create_from_snapshot(
        sandbox_id: SandboxId,
        snapshot: Box<RunnableSnapshot>,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
    ) -> Self {
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            source: CreateLaunchSource::Snapshot { snapshot },
            launch_config,
            metadata,
            timeout,
        }))
    }

    pub(super) fn for_create_fresh(
        sandbox_id: SandboxId,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
    ) -> Self {
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            source: CreateLaunchSource::Fresh {
                build_spec: Box::new(build_spec),
            },
            launch_config,
            metadata,
            timeout,
        }))
    }

    pub(super) fn for_resume(
        sandbox_id: SandboxId,
        paused_state: Arc<dyn PausedSandboxState>,
        timeout: NewTimeout,
        resources: SandboxResources,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Self {
        Self::Resume(Box::new(ResumeLaunchPlan {
            sandbox_id,
            paused_state,
            timeout,
            resources,
            envd_access_token,
        }))
    }

    pub(super) fn sandbox_id(&self) -> SandboxId {
        match self {
            Self::Create(plan) => plan.sandbox_id,
            Self::Resume(plan) => plan.sandbox_id,
        }
    }

    pub(super) fn transitional_state(&self) -> SandboxState {
        match self {
            Self::Create(_) => SandboxState::Creating,
            Self::Resume(_) => SandboxState::Resuming,
        }
    }

    pub(super) fn transitional_metadata(&self) -> Option<&SandboxMetadata> {
        match self {
            Self::Create(plan) => Some(&plan.metadata),
            Self::Resume(_) => None,
        }
    }

    pub(super) fn timeout(&self) -> NewTimeout {
        match self {
            Self::Create(plan) => plan.timeout,
            Self::Resume(plan) => plan.timeout,
        }
    }

    pub(super) fn resources(&self) -> SandboxResources {
        match self {
            Self::Create(plan) => plan.metadata.resources,
            Self::Resume(plan) => plan.resources,
        }
    }
}
