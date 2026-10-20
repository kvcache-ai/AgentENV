use std::result::Result as StdResult;
use std::sync::Arc;

use super::claim::{Claim, Create, Resume};
use super::store::{NewTimeout, SandboxMetadata, StoreError};
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
    pub claim: Claim<Resume>,
    pub paused_state: Arc<dyn PausedSandboxState>,
    pub timeout: NewTimeout,
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
        claim: Claim<Resume>,
        paused_state: Arc<dyn PausedSandboxState>,
        timeout: NewTimeout,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Self {
        Self::Resume(Box::new(ResumeLaunchPlan {
            claim,
            paused_state,
            timeout,
            envd_access_token,
        }))
    }

    pub(super) fn sandbox_id(&self) -> SandboxId {
        match self {
            Self::Create(plan) => plan.sandbox_id,
            Self::Resume(plan) => plan.claim.sandbox_id(),
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
            Self::Resume(plan) => plan.claim.metadata().resources,
        }
    }
}

/// The claim a launch resolves: a resume arrives holding it, while a create
/// acquires it once the runtime has started.
pub(super) enum LaunchClaim {
    Create(Claim<Create>),
    Resume(Claim<Resume>),
}

impl LaunchClaim {
    pub(super) async fn commit(
        self,
        update: impl FnOnce(&mut SandboxMetadata) + Send + 'static,
    ) -> StdResult<SandboxMetadata, (Self, StoreError)> {
        match self {
            Self::Create(claim) => claim
                .commit(update)
                .await
                .map_err(|(claim, err)| (Self::Create(claim), err)),
            Self::Resume(claim) => claim
                .commit(update)
                .await
                .map_err(|(claim, err)| (Self::Resume(claim), err)),
        }
    }

    pub(super) async fn rollback(self) {
        match self {
            Self::Create(claim) => claim.rollback().await,
            Self::Resume(claim) => claim.rollback().await,
        }
    }
}
