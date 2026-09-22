use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream, StreamExt};
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};

use crate::cfg::ConfigManager;
use crate::sandbox::{SandboxBackendFactory, SandboxMetric};
use crate::types::SandboxId;

use super::super::persistence::SandboxPersister;
use super::super::store::MetadataStore;
use super::super::types::{SandboxLifecycleEventType, SandboxState};
use super::super::{OrchestratorError, Result};
use super::Orchestrator;

const METRICS_PARALLELISM_FACTOR: usize = 5;
const METRICS_REQUEST_TIMEOUT: Duration = Duration::from_millis(200);

pub(super) fn metrics_concurrency(running_sandboxes: usize) -> usize {
    running_sandboxes.div_ceil(METRICS_PARALLELISM_FACTOR)
}

#[derive(Clone)]
struct Sample {
    generation: u64,
    collected_at: Instant,
    metric: SandboxMetric,
}

#[derive(Default)]
pub(super) struct SandboxMetrics {
    samples: HashMap<SandboxId, VecDeque<Sample>>,
}

impl SandboxMetrics {
    pub(super) fn prune(&mut self, now: Instant, retention: Duration) {
        self.samples.retain(|_, samples| {
            while samples
                .front()
                .is_some_and(|sample| now.duration_since(sample.collected_at) >= retention)
            {
                samples.pop_front();
            }
            !samples.is_empty()
        });
    }

    pub(super) fn insert(
        &mut self,
        id: SandboxId,
        generation: u64,
        collected_at: Instant,
        metric: SandboxMetric,
    ) {
        self.samples.entry(id).or_default().push_back(Sample {
            generation,
            collected_at,
            metric,
        });
    }

    pub(super) fn has_generation(&self, id: &SandboxId, generation: u64) -> bool {
        self.samples
            .get(id)
            .and_then(|samples| samples.back())
            .is_some_and(|sample| sample.generation == generation)
    }

    pub(super) fn history(
        &self,
        id: &SandboxId,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Vec<SandboxMetric> {
        let mut samples: Vec<_> = self
            .samples
            .get(id)
            .into_iter()
            .flatten()
            .filter(|sample| {
                start.is_none_or(|timestamp| sample.metric.timestamp.timestamp() >= timestamp)
                    && end.is_none_or(|timestamp| sample.metric.timestamp.timestamp() <= timestamp)
            })
            .map(|sample| sample.metric.clone())
            .collect();
        samples.sort_by_key(|sample| sample.timestamp);
        samples
    }

    pub(super) fn latest(&self, id: &SandboxId, generation: u64) -> Option<SandboxMetric> {
        self.samples
            .get(id)
            .and_then(|samples| samples.back())
            .filter(|sample| sample.generation == generation)
            .map(|sample| sample.metric.clone())
    }
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    pub(super) fn start_metrics_task(this: &Arc<Self>) {
        let config = &ConfigManager::global_config().orchestrator;
        if config.metrics_interval_secs == 0 {
            return;
        }
        let mut shutdown = this.shutdown_tx.subscribe();
        let mut events = this.sandbox_event_tx.subscribe();
        let weak = Arc::downgrade(this);
        let interval = Duration::from_secs(config.metrics_interval_secs);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let target = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    event = events.recv() => match event {
                        Ok(event) if matches!(event.event_type, SandboxLifecycleEventType::Create | SandboxLifecycleEventType::Resume | SandboxLifecycleEventType::Fork) => Some(event.sandbox_id),
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = timer.tick() => None,
                };
                let Some(this) = weak.upgrade() else {
                    break;
                };
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = this.collect_sandbox_metrics(target) => {},
                }
            }
        });
    }

    pub(super) async fn collect_sandbox_metrics(&self, target: Option<SandboxId>) {
        let config = &ConfigManager::global_config().orchestrator;
        self.sandbox_metrics.lock().await.prune(
            Instant::now(),
            Duration::from_secs(config.metrics_retention_secs),
        );
        let metadata = match self.store.list().await {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(%error, "cannot enumerate sandboxes for metrics");
                return;
            }
        };
        let running_ids: Vec<_> = metadata
            .into_iter()
            .filter(|metadata| metadata.state == SandboxState::Running)
            .map(|metadata| metadata.id)
            .collect();
        let concurrency = metrics_concurrency(running_ids.len());
        let ids = running_ids
            .into_iter()
            .filter(|id| target.is_none_or(|target_id| target_id == *id));
        stream::iter(ids)
            .for_each_concurrent(concurrency, |id| async move {
                let generation = match self.proxy_routes.read().await.route(&id) {
                    Some(route) => route.version(),
                    None => return,
                };
                if target.is_some()
                    && self
                        .sandbox_metrics
                        .lock()
                        .await
                        .has_generation(&id, generation)
                {
                    return;
                }
                let handle = self.sandboxes.read().await.get(&id).cloned();
                let Some(handle) = handle else { return };
                let future = match handle.try_lock() {
                    Ok(backend) => backend.metrics_sample(),
                    Err(_) => return,
                };
                let Some(future) = future else { return };
                match tokio::time::timeout(METRICS_REQUEST_TIMEOUT, future).await {
                    Ok(Ok(metric)) => {
                        if !matches!(self.store.get(&id).await, Ok(Some(metadata)) if metadata.state == SandboxState::Running) {
                            return;
                        }
                        let generation_is_current = self
                            .proxy_routes
                            .read()
                            .await
                            .route(&id)
                            .is_some_and(|route| route.version() == generation);
                        if !generation_is_current {
                            return;
                        }
                        self.sandbox_metrics.lock().await.insert(
                            id,
                            generation,
                            Instant::now(),
                            metric,
                        );
                    }
                    result => debug!(sandbox_id = %id, error = ?result, "guest metrics sample unavailable"),
                }
            })
            .await;
    }

    pub async fn sandbox_metrics_history(
        &self,
        id: SandboxId,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<SandboxMetric>> {
        if self.store.get(&id).await?.is_none() {
            return Err(OrchestratorError::SandboxNotFound(id));
        }
        let config = &ConfigManager::global_config().orchestrator;
        let mut cache = self.sandbox_metrics.lock().await;
        cache.prune(
            Instant::now(),
            Duration::from_secs(config.metrics_retention_secs),
        );
        Ok(cache.history(&id, start, end))
    }

    pub async fn latest_sandbox_metrics(
        &self,
        ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, SandboxMetric>> {
        let config = &ConfigManager::global_config().orchestrator;
        let running_ids: HashSet<_> = self
            .store
            .list()
            .await?
            .into_iter()
            .filter(|metadata| metadata.state == SandboxState::Running)
            .map(|metadata| metadata.id)
            .collect();
        let generations: Vec<_> = {
            let routes = self.proxy_routes.read().await;
            ids.iter()
                .filter(|id| running_ids.contains(id))
                .filter_map(|id| routes.route(id).map(|route| (*id, route.version())))
                .collect()
        };
        let mut cache = self.sandbox_metrics.lock().await;
        cache.prune(
            Instant::now(),
            Duration::from_secs(config.metrics_retention_secs),
        );
        let mut result = HashMap::new();
        for (id, generation) in generations {
            if let Some(metric) = cache.latest(&id, generation) {
                result.insert(id, metric);
            }
        }
        Ok(result)
    }
}
