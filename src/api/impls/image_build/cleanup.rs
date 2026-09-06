use super::{ApiImpl, BuildJournal, BuildSession, SessionState};
use crate::{snapshot::TemplateBuildErrorReason, snapshot::TemplateBuildStatus, types::SandboxId};
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use tracing::warn;

impl ApiImpl {
    /// Recover unfinished builds without allowing one failed entry to block other builds.
    pub async fn recover_image_builds(&self) -> Result<()> {
        let journal = self.build_journal().await?;
        for (key, _) in journal.scan_prefix(b"build/".to_vec()).await? {
            let result = async {
                let id = std::str::from_utf8(&key[6..]).context("invalid build journal key")?;
                self.retry_image_build_cleanup(id).await
            }
            .await;
            if let Err(error) = result {
                warn!(key = %String::from_utf8_lossy(&key), error = %format_args!("{error:#}"), "build recovery failed; journal retained for retry");
            }
        }
        self.collect_retired_build_caches().await?;
        Ok(())
    }

    pub fn start_image_build_cleanup(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let api = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let Some(api) = api.upgrade() else {
                    break;
                };
                if let Err(error) = api.recover_image_builds().await {
                    warn!(error = %format_args!("{error:#}"), "build recovery scan failed; will retry");
                }
            }
        })
    }

    pub(super) async fn retry_image_build_cleanup(&self, id: &str) -> Result<()> {
        let sandbox_id = SandboxId::parse_str(id).context("invalid build journal ID")?;
        let journal = self.build_journal().await?;
        let key = format!("build/{id}");
        if journal.get(key.clone()).await?.is_none() {
            return Ok(());
        }
        let session = self
            .build_sessions
            .active
            .entry(id.to_owned())
            .or_insert_with(|| {
                let session = BuildSession::new();
                session.state.send_replace(SessionState::Finished(Some(
                    TemplateBuildErrorReason::new("build interrupted by server restart"),
                )));
                session
            })
            .clone();
        let _cleanup = session.cleanup.lock().await;
        let reason = match &*session.state.borrow() {
            SessionState::Finished(reason) => reason.clone(),
            _ => return Ok(()),
        };
        // Another finalizer may have completed while this caller waited for the lock.
        let Some(value) = journal.get(key.clone()).await? else {
            self.build_sessions.active.remove_if(id, |_, current| {
                Arc::ptr_eq(&current.cleanup, &session.cleanup)
            });
            return Ok(());
        };
        let entry: BuildJournal = serde_json::from_slice(&value)?;
        let persisted: Result<()> = async {
            if let Some(reason) = reason {
                if let Some(record) = self.snapshot_manager.get(id).await? {
                    if matches!(
                        super::super::template::template_build_status(&record),
                        TemplateBuildStatus::Waiting | TemplateBuildStatus::Building
                    ) {
                        self.snapshot_manager
                            .mark_build_error(&record.id, reason)
                            .await?;
                    }
                }
            }
            Ok(())
        }
        .await;
        self.release_builder(id, &entry.cache).await?;
        self.cleanup_build_cache(id, &entry).await?;
        persisted?;
        self.orchestrator
            .unregister_template_build(sandbox_id)
            .await;
        journal.delete(key).await?;
        self.build_sessions.active.remove(id);
        Ok(())
    }
}
