use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use base64::Engine;
use chrono::{DateTime, DurationRound, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::orchestrator::{SandboxLifecycleEvent, SandboxLifecycleEventType};
use crate::snapshot::repository::{RepositoryError, SnapshotRepository};

/// AgentENV is single-tenant; every webhook and event belongs to this team.
pub const TEAM_ID: Uuid = Uuid::nil();
/// Days delivery attempts are kept, also advertised as `events_ttl_days`.
const RETENTION_DAYS: i64 = 7;
const MAX_ATTEMPTS: usize = 3;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONCURRENT_DELIVERIES: usize = 64;
const MAX_RESPONSE_BODY_BYTES: usize = 4096;
/// Upper bound on how long a node reuses its cached registration list.
/// Changes normally apply on the next event through the registry generation;
/// this only bounds staleness if a generation update was lost.
const REGISTRY_CACHE_MAX_AGE: Duration = Duration::from_secs(600);
const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);
/// Upper bound on a stats range, keeping the hourly bucket count bounded.
const MAX_STATS_RANGE_DAYS: i64 = 31;

/// Lifecycle event names accepted in webhook `events` filters. The first four
/// match E2B; `forked` is an AgentENV extension (E2B has no fork event).
pub const EVENT_TYPES: [&str; 5] = [
    "sandbox.lifecycle.created",
    "sandbox.lifecycle.killed",
    "sandbox.lifecycle.paused",
    "sandbox.lifecycle.resumed",
    "sandbox.lifecycle.forked",
];

/// Maps a lifecycle event to E2B's `(type, legacy event_label)`.
fn event_names(event_type: SandboxLifecycleEventType) -> (&'static str, &'static str) {
    match event_type {
        SandboxLifecycleEventType::Create => ("sandbox.lifecycle.created", "create"),
        SandboxLifecycleEventType::Delete => ("sandbox.lifecycle.killed", "kill"),
        SandboxLifecycleEventType::Pause => ("sandbox.lifecycle.paused", "pause"),
        SandboxLifecycleEventType::Resume => ("sandbox.lifecycle.resumed", "resume"),
        SandboxLifecycleEventType::Fork => ("sandbox.lifecycle.forked", "fork"),
    }
}

/// Durable webhook registration. The signature secret is stored as given,
/// because every delivery must be signed with it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookRecord {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub events: Vec<String>,
    pub enabled: bool,
    pub signature_secret: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct NewWebhook {
    pub name: String,
    pub url: String,
    pub events: Vec<String>,
    pub enabled: Option<bool>,
    pub signature_secret: String,
}

#[derive(Clone, Debug, Default)]
pub struct WebhookPatch {
    pub name: Option<String>,
    pub url: Option<String>,
    pub events: Option<Vec<String>>,
    pub enabled: Option<bool>,
    pub signature_secret: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("webhook not found")]
    NotFound,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type WebhookResult<T> = Result<T, WebhookError>;

/// One recorded delivery attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub id: Uuid,
    pub webhook_id: Uuid,
    pub event_id: Uuid,
    pub sandbox_id: String,
    pub event_type: String,
    pub success: bool,
    pub duration_ms: u32,
    pub request_body: String,
    pub request_headers: String,
    pub request_url: String,
    pub response_body: Option<String>,
    pub response_headers: Option<String>,
    pub response_status: Option<u16>,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct DeliveryQuery {
    pub cursor: Option<String>,
    pub limit: usize,
    pub order_asc: bool,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    /// `success` / `failed`; empty matches both.
    pub statuses: Vec<String>,
    pub event_types: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryGroup {
    pub event_id: Uuid,
    pub event_type: String,
    pub sandbox_id: String,
    pub attempts: Vec<DeliveryRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryPage {
    pub groups: Vec<DeliveryGroup>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DurationStats {
    pub minimum: f64,
    pub average: f64,
    pub maximum: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatsBucket {
    pub timestamp: DateTime<Utc>,
    pub total: i64,
    pub failed: i64,
    pub duration_ms: DurationStats,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryStats {
    pub buckets: Vec<StatsBucket>,
    pub total: i64,
    pub failed: i64,
    pub duration_ms: DurationStats,
}

struct RegistryCache {
    loaded_at: Instant,
    /// Registry generation read before `records` was listed.
    generation: Option<String>,
    records: Vec<WebhookRecord>,
}

pub struct WebhookService {
    repository: Arc<dyn SnapshotRepository>,
    deliveries: LocalKvStore,
    client: reqwest::Client,
    cache: RwLock<Option<RegistryCache>>,
    delivery_slots: Arc<Semaphore>,
    /// Waits before the 2nd and 3rd attempt.
    retry_delays: [Duration; MAX_ATTEMPTS - 1],
}

impl WebhookService {
    /// Opens the node-local delivery store under `deliveries_path`.
    pub async fn open(
        repository: Arc<dyn SnapshotRepository>,
        deliveries_path: impl Into<PathBuf>,
    ) -> anyhow::Result<Arc<Self>> {
        let deliveries = LocalKvStore::open(deliveries_path, LocalStoreDurability::Wal)
            .await
            .context("open webhook delivery store")?;
        let client = reqwest::Client::builder()
            .timeout(DELIVERY_TIMEOUT)
            // Deliveries go to the registered URL only.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build webhook HTTP client")?;
        Ok(Arc::new(Self {
            repository,
            deliveries,
            client,
            cache: RwLock::new(None),
            delivery_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES)),
            retry_delays: [Duration::from_secs(1), Duration::from_secs(5)],
        }))
    }

    pub async fn list(&self) -> WebhookResult<Vec<WebhookRecord>> {
        let mut records = self.repository.list_webhooks().await?;
        records.sort_by_key(|record| (record.created_at, record.id));
        Ok(records)
    }

    pub async fn get(&self, id: Uuid) -> WebhookResult<WebhookRecord> {
        self.repository
            .get_webhook(&id)
            .await?
            .ok_or(WebhookError::NotFound)
    }

    pub async fn create(&self, input: NewWebhook) -> WebhookResult<WebhookRecord> {
        validate_name(&input.name)?;
        validate_url(&input.url)?;
        validate_events(&input.events)?;
        validate_secret(&input.signature_secret)?;
        let record = WebhookRecord {
            id: Uuid::now_v7(),
            name: input.name,
            url: input.url,
            events: dedup(input.events),
            enabled: input.enabled.unwrap_or(true),
            signature_secret: input.signature_secret,
            created_at: Utc::now(),
        };
        self.repository.put_webhook(record.clone()).await?;
        self.registry_changed().await;
        info!(webhook_id = %record.id, url = %record.url, "webhook registered");
        Ok(record)
    }

    pub async fn update(&self, id: Uuid, patch: WebhookPatch) -> WebhookResult<WebhookRecord> {
        let mut record = self.get(id).await?;
        if let Some(name) = patch.name {
            validate_name(&name)?;
            record.name = name;
        }
        if let Some(url) = patch.url {
            validate_url(&url)?;
            record.url = url;
        }
        if let Some(events) = patch.events {
            validate_events(&events)?;
            record.events = dedup(events);
        }
        if let Some(secret) = patch.signature_secret {
            validate_secret(&secret)?;
            record.signature_secret = secret;
        }
        if let Some(enabled) = patch.enabled {
            record.enabled = enabled;
        }
        self.repository.put_webhook(record.clone()).await?;
        self.registry_changed().await;
        Ok(record)
    }

    pub async fn delete(&self, id: Uuid) -> WebhookResult<()> {
        self.get(id).await?;
        self.repository.delete_webhook(&id).await?;
        self.registry_changed().await;
        // Other nodes' history ages out through retention pruning.
        let keys: Vec<_> = self
            .deliveries
            .scan_prefix(format!("{id}/"))
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        if !keys.is_empty() {
            self.deliveries
                .write_batch(
                    keys.into_iter()
                        .map(crate::local_store::LocalKvBatchOp::delete),
                )
                .await?;
        }
        info!(webhook_id = %id, "webhook deleted");
        Ok(())
    }

    /// Publishes a registration change: replaces the shared generation so
    /// every node reloads on its next event, and drops this node's cache.
    async fn registry_changed(&self) {
        *self.cache.write().await = None;
        if let Err(error) = self
            .repository
            .put_webhook_generation(&Uuid::now_v7().to_string())
            .await
        {
            // The registration itself is committed; other nodes pick it up
            // once their cache reaches REGISTRY_CACHE_MAX_AGE.
            warn!(error = %error, "failed to publish webhook registry generation");
        }
    }

    /// Returns the registrations to dispatch against. Costs one small
    /// generation read per event; the full list is reloaded only when the
    /// generation changed or the cache reached its maximum age.
    async fn cached_registrations(&self) -> anyhow::Result<Vec<WebhookRecord>> {
        let generation = match self.repository.get_webhook_generation().await {
            Ok(generation) => generation,
            Err(error) => {
                // Keep delivering from a cache that is still within its age bound.
                if let Some(cache) = self.cache.read().await.as_ref() {
                    if cache.loaded_at.elapsed() < REGISTRY_CACHE_MAX_AGE {
                        warn!(error = %error, "failed to read webhook registry generation; using cached registrations");
                        return Ok(cache.records.clone());
                    }
                }
                return Err(error).context("read webhook registry generation");
            }
        };
        if let Some(cache) = self.cache.read().await.as_ref() {
            if cache.generation == generation && cache.loaded_at.elapsed() < REGISTRY_CACHE_MAX_AGE
            {
                return Ok(cache.records.clone());
            }
        }
        let records = self
            .repository
            .list_webhooks()
            .await
            .context("load webhook registrations")?;
        *self.cache.write().await = Some(RegistryCache {
            loaded_at: Instant::now(),
            generation,
            records: records.clone(),
        });
        Ok(records)
    }

    // ---- Dispatch -------------------------------------------------------

    /// Delivers every lifecycle event from `events` until the channel closes,
    /// pruning expired delivery history once per hour.
    pub fn start(
        self: &Arc<Self>,
        mut events: broadcast::Receiver<SandboxLifecycleEvent>,
    ) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut prune = tokio::time::interval(PRUNE_INTERVAL);
            loop {
                tokio::select! {
                    event = events.recv() => match event {
                        Ok(event) => this.dispatch(event).await,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "webhook dispatcher lagged; sandbox events were not delivered");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = prune.tick() => {
                        if let Err(error) = this.prune_expired(Utc::now()).await {
                            warn!(error = %format_args!("{error:#}"), "failed to prune webhook delivery history");
                        }
                    }
                }
            }
            debug!("webhook dispatcher stopped");
        })
    }

    async fn dispatch(self: &Arc<Self>, event: SandboxLifecycleEvent) {
        if event.template_builder {
            return;
        }
        let (event_type, _) = event_names(event.event_type);
        let registrations = match self.cached_registrations().await {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), sandbox_id = %event.sandbox_id, event_type, "dropping sandbox event for webhooks");
                return;
            }
        };
        let targets: Vec<_> = registrations
            .into_iter()
            .filter(|hook| hook.enabled && hook.events.iter().any(|name| name == event_type))
            .collect();
        if targets.is_empty() {
            return;
        }
        let event_id = Uuid::now_v7();
        let body = event_payload(&event, event_id).to_string();
        for hook in targets {
            let this = Arc::clone(self);
            let body = body.clone();
            let sandbox_id = event.sandbox_id.to_string();
            let Ok(permit) = Arc::clone(&self.delivery_slots).acquire_owned().await else {
                return;
            };
            tokio::spawn(async move {
                let _permit = permit;
                this.deliver(hook, event_id, event_type, &sandbox_id, &body)
                    .await;
            });
        }
    }

    /// Sends one event to one webhook, retrying failed attempts. The
    /// registration is re-read before each retry, so a deleted or disabled
    /// webhook stops retrying and a changed URL or secret is used right away.
    async fn deliver(
        &self,
        mut hook: WebhookRecord,
        event_id: Uuid,
        event_type: &str,
        sandbox_id: &str,
        body: &str,
    ) {
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(self.retry_delays[attempt - 1]).await;
                match self.repository.get_webhook(&hook.id).await {
                    Ok(Some(current))
                        if current.enabled
                            && current.events.iter().any(|name| name == event_type) =>
                    {
                        hook = current;
                    }
                    Ok(_) => {
                        debug!(webhook_id = %hook.id, %event_id, "webhook removed, disabled, or unsubscribed; stopping retries");
                        return;
                    }
                    Err(error) => {
                        warn!(error = %error, webhook_id = %hook.id, "failed to re-read webhook before retry; using previous registration");
                    }
                }
            }
            let record = self
                .attempt(&hook, event_id, event_type, sandbox_id, body)
                .await;
            let success = record.success;
            if let Err(error) = self.store_delivery(&record).await {
                warn!(error = %format_args!("{error:#}"), webhook_id = %hook.id, "failed to record webhook delivery");
            }
            if success {
                return;
            }
        }
        warn!(webhook_id = %hook.id, %event_id, event_type, "webhook delivery failed after all attempts");
    }

    async fn attempt(
        &self,
        hook: &WebhookRecord,
        event_id: Uuid,
        event_type: &str,
        sandbox_id: &str,
        body: &str,
    ) -> DeliveryRecord {
        let delivery_id = Uuid::now_v7();
        let signature = sign(&hook.signature_secret, body);
        let headers = [
            ("content-type", "application/json".to_string()),
            ("e2b-webhook-id", hook.id.to_string()),
            ("e2b-delivery-id", delivery_id.to_string()),
            ("e2b-signature-version", "v1".to_string()),
            ("e2b-signature", signature),
        ];
        let recorded_headers: BTreeMap<_, _> = headers
            .iter()
            .map(|(name, value)| {
                let value = if *name == "e2b-signature" {
                    "[REDACTED]".to_string()
                } else {
                    value.clone()
                };
                (*name, value)
            })
            .collect();
        let mut record = DeliveryRecord {
            id: delivery_id,
            webhook_id: hook.id,
            event_id,
            sandbox_id: sandbox_id.to_string(),
            event_type: event_type.to_string(),
            success: false,
            duration_ms: 0,
            request_body: body.to_string(),
            request_headers: serde_json::to_string(&recorded_headers).unwrap_or_default(),
            request_url: hook.url.clone(),
            response_body: None,
            response_headers: None,
            response_status: None,
            error_class: None,
            error_message: None,
            timestamp: Utc::now(),
        };
        let mut request = self.client.post(&hook.url).body(body.to_string());
        for (name, value) in &headers {
            request = request.header(*name, value);
        }
        let started = Instant::now();
        let result = request.send().await;
        match result {
            Ok(response) => {
                let status = response.status();
                record.response_status = Some(status.as_u16());
                let response_headers: BTreeMap<_, _> = response
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect();
                record.response_headers = serde_json::to_string(&response_headers).ok();
                match response.bytes().await {
                    Ok(bytes) => {
                        let end = bytes.len().min(MAX_RESPONSE_BODY_BYTES);
                        record.response_body =
                            Some(String::from_utf8_lossy(&bytes[..end]).into_owned());
                    }
                    Err(error) => record.error_message = Some(error.to_string()),
                }
                record.success = status.is_success();
                if !record.success {
                    record.error_class = Some("http_error".to_string());
                }
            }
            Err(error) => {
                record.error_class = Some(classify_error(&error).to_string());
                record.error_message = Some(error.to_string());
            }
        }
        record.duration_ms = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
        record
    }

    // ---- Delivery history ----------------------------------------------

    fn delivery_key(record: &DeliveryRecord) -> String {
        format!(
            "{}/{:020}/{}",
            record.webhook_id,
            record.timestamp.timestamp_millis().max(0),
            record.id
        )
    }

    async fn store_delivery(&self, record: &DeliveryRecord) -> anyhow::Result<()> {
        self.deliveries
            .put(Self::delivery_key(record), serde_json::to_vec(record)?)
            .await
    }

    async fn load_deliveries(&self, webhook_id: Uuid) -> anyhow::Result<Vec<DeliveryRecord>> {
        self.deliveries
            .scan_prefix(format!("{webhook_id}/"))
            .await?
            .into_iter()
            .map(|(_, value)| serde_json::from_slice(&value).context("parse webhook delivery"))
            .collect()
    }

    async fn prune_expired(&self, now: DateTime<Utc>) -> anyhow::Result<()> {
        let cutoff = (now - TimeDelta::days(RETENTION_DAYS)).timestamp_millis();
        let expired: Vec<_> = self
            .deliveries
            .scan_prefix(Vec::new())
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| {
                std::str::from_utf8(key)
                    .ok()
                    .and_then(|key| key.split('/').nth(1))
                    .and_then(|millis| millis.parse::<i64>().ok())
                    .is_some_and(|millis| millis < cutoff)
            })
            .collect();
        if !expired.is_empty() {
            debug!(count = expired.len(), "pruning expired webhook deliveries");
            self.deliveries
                .write_batch(
                    expired
                        .into_iter()
                        .map(crate::local_store::LocalKvBatchOp::delete),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn deliveries(&self, id: Uuid, query: DeliveryQuery) -> WebhookResult<DeliveryPage> {
        self.get(id).await?;
        let cursor = query.cursor.as_deref().map(parse_cursor).transpose()?;
        let mut groups: BTreeMap<Uuid, DeliveryGroup> = BTreeMap::new();
        for record in self.load_deliveries(id).await? {
            if query.start.is_some_and(|start| record.timestamp < start)
                || query.end.is_some_and(|end| record.timestamp >= end)
                || !matches_status(&query.statuses, record.success)
                || (!query.event_types.is_empty()
                    && !query.event_types.contains(&record.event_type))
            {
                continue;
            }
            groups
                .entry(record.event_id)
                .or_insert_with(|| DeliveryGroup {
                    event_id: record.event_id,
                    event_type: record.event_type.clone(),
                    sandbox_id: record.sandbox_id.clone(),
                    attempts: Vec::new(),
                })
                .attempts
                .push(record);
        }
        // Order groups by their first attempt; ties break on event id.
        let mut groups: Vec<_> = groups
            .into_values()
            .map(|mut group| {
                group
                    .attempts
                    .sort_by_key(|attempt| (attempt.timestamp, attempt.id));
                let key = (
                    group.attempts[0].timestamp.timestamp_millis(),
                    group.event_id,
                );
                (key, group)
            })
            .collect();
        groups.sort_by_key(|(key, _)| *key);
        if !query.order_asc {
            groups.reverse();
        }
        let start_index = match cursor {
            None => 0,
            Some(cursor) => groups
                .iter()
                .position(|(key, _)| {
                    if query.order_asc {
                        *key > cursor
                    } else {
                        *key < cursor
                    }
                })
                .unwrap_or(groups.len()),
        };
        let limit = query.limit.max(1);
        let page: Vec<_> = groups
            .into_iter()
            .skip(start_index)
            .take(limit + 1)
            .collect();
        let next_cursor = (page.len() > limit).then(|| {
            let (millis, event_id) = page[limit - 1].0;
            format!("{millis}_{event_id}")
        });
        Ok(DeliveryPage {
            groups: page
                .into_iter()
                .take(limit)
                .map(|(_, group)| group)
                .collect(),
            next_cursor,
        })
    }

    pub async fn stats(
        &self,
        id: Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> WebhookResult<DeliveryStats> {
        self.get(id).await?;
        let end = end.unwrap_or_else(Utc::now);
        let start = start.unwrap_or(end - TimeDelta::hours(24));
        if start >= end {
            return Err(WebhookError::InvalidRequest(
                "start must be before end".to_string(),
            ));
        }
        if end - start > TimeDelta::days(MAX_STATS_RANGE_DAYS) {
            return Err(WebhookError::InvalidRequest(format!(
                "stats range must not exceed {MAX_STATS_RANGE_DAYS} days"
            )));
        }
        let hour = TimeDelta::hours(1);
        let first_bucket = start.duration_trunc(hour).map_err(anyhow::Error::from)?;
        let mut buckets: BTreeMap<DateTime<Utc>, Vec<&DeliveryRecord>> = BTreeMap::new();
        let mut bucket = first_bucket;
        while bucket < end {
            buckets.insert(bucket, Vec::new());
            bucket += hour;
        }
        let records = self.load_deliveries(id).await?;
        let in_range: Vec<_> = records
            .iter()
            .filter(|record| record.timestamp >= start && record.timestamp < end)
            .collect();
        for record in &in_range {
            if let Ok(bucket) = record.timestamp.duration_trunc(hour) {
                buckets.entry(bucket).or_default().push(record);
            }
        }
        Ok(DeliveryStats {
            buckets: buckets
                .into_iter()
                .map(|(timestamp, records)| StatsBucket {
                    timestamp,
                    total: records.len() as i64,
                    failed: records.iter().filter(|record| !record.success).count() as i64,
                    duration_ms: duration_stats(&records),
                })
                .collect(),
            total: in_range.len() as i64,
            failed: in_range.iter().filter(|record| !record.success).count() as i64,
            duration_ms: duration_stats(&in_range),
        })
    }
}

/// E2B webhook signature: unpadded base64 of `sha256(secret + body)`.
pub fn sign(secret: &str, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(body.as_bytes());
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(hasher.finalize())
}

/// Builds the E2B v2 `SandboxEvent` body for one lifecycle event.
fn event_payload(event: &SandboxLifecycleEvent, event_id: Uuid) -> serde_json::Value {
    let (event_type, label) = event_names(event.event_type);
    let mut event_data = serde_json::json!({
        "sandbox_metadata": event.user_metadata.clone().unwrap_or_default(),
    });
    if let Some(source) = event.source_sandbox_id {
        event_data["source_sandbox_id"] = serde_json::Value::String(source.to_string());
    }
    serde_json::json!({
        "id": event_id,
        "version": "v2",
        "type": event_type,
        "timestamp": DateTime::<Utc>::from(event.timestamp).to_rfc3339_opts(SecondsFormat::Micros, true),
        "event_category": "lifecycle",
        "event_label": label,
        "event_data": event_data,
        "sandbox_id": event.sandbox_id.to_string(),
        "sandbox_execution_id": "",
        "sandbox_template_id": event.template_id,
        "sandbox_build_id": "",
        "sandbox_team_id": TEAM_ID,
        "events_ttl_days": RETENTION_DAYS,
    })
}

fn classify_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "transport_error"
    } else if error.is_builder() || error.is_request() {
        "request_error"
    } else {
        "transport_error"
    }
}

fn matches_status(statuses: &[String], success: bool) -> bool {
    statuses.is_empty()
        || statuses
            .iter()
            .any(|status| (status == "success") == success)
}

fn duration_stats(records: &[&DeliveryRecord]) -> DurationStats {
    if records.is_empty() {
        return DurationStats::default();
    }
    let durations = records.iter().map(|record| f64::from(record.duration_ms));
    DurationStats {
        minimum: durations.clone().fold(f64::INFINITY, f64::min),
        average: durations.clone().sum::<f64>() / records.len() as f64,
        maximum: durations.fold(0.0, f64::max),
    }
}

fn parse_cursor(cursor: &str) -> WebhookResult<(i64, Uuid)> {
    cursor
        .split_once('_')
        .and_then(|(millis, id)| Some((millis.parse().ok()?, Uuid::parse_str(id).ok()?)))
        .ok_or_else(|| WebhookError::InvalidRequest("invalid cursor".to_string()))
}

fn dedup(events: Vec<String>) -> Vec<String> {
    let mut unique = Vec::with_capacity(events.len());
    for event in events {
        if !unique.contains(&event) {
            unique.push(event);
        }
    }
    unique
}

fn validate_name(name: &str) -> WebhookResult<()> {
    if name.trim().is_empty() {
        return Err(WebhookError::InvalidRequest(
            "name must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_url(url: &str) -> WebhookResult<()> {
    let parsed = url::Url::parse(url)
        .map_err(|error| WebhookError::InvalidRequest(format!("invalid url: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
        return Err(WebhookError::InvalidRequest(
            "url must be an absolute http or https URL".to_string(),
        ));
    }
    Ok(())
}

fn validate_events(events: &[String]) -> WebhookResult<()> {
    if events.is_empty() {
        return Err(WebhookError::InvalidRequest(
            "events must not be empty".to_string(),
        ));
    }
    if let Some(unknown) = events
        .iter()
        .find(|event| !EVENT_TYPES.contains(&event.as_str()))
    {
        return Err(WebhookError::InvalidRequest(format!(
            "unknown event type '{unknown}'; expected one of: {}",
            EVENT_TYPES.join(", ")
        )));
    }
    Ok(())
}

fn validate_secret(secret: &str) -> WebhookResult<()> {
    if secret.is_empty() {
        return Err(WebhookError::InvalidRequest(
            "signatureSecret must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::types::{SandboxId, SandboxResources};
    use axum::{extract::State, http::HeaderMap, routing::post, Router};
    use std::sync::Mutex as StdMutex;

    struct Fixture {
        _root: tempfile::TempDir,
        service: Arc<WebhookService>,
    }

    async fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })
        .unwrap();
        let mut service =
            WebhookService::open(backend.repository(), root.path().join("deliveries"))
                .await
                .unwrap();
        Arc::get_mut(&mut service).unwrap().retry_delays =
            [Duration::from_millis(10), Duration::from_millis(10)];
        Fixture {
            _root: root,
            service,
        }
    }

    #[derive(Clone, Default)]
    struct Receiver {
        /// Status codes to answer with, in order; 200 once exhausted.
        statuses: Arc<StdMutex<Vec<u16>>>,
        requests: Arc<StdMutex<Vec<(HeaderMap, String)>>>,
    }

    async fn spawn_receiver(statuses: Vec<u16>) -> (String, Receiver) {
        async fn handle(
            State(receiver): State<Receiver>,
            headers: HeaderMap,
            body: String,
        ) -> http::StatusCode {
            receiver.requests.lock().unwrap().push((headers, body));
            let mut statuses = receiver.statuses.lock().unwrap();
            let status = if statuses.is_empty() {
                200
            } else {
                statuses.remove(0)
            };
            http::StatusCode::from_u16(status).unwrap()
        }
        let receiver = Receiver {
            statuses: Arc::new(StdMutex::new(statuses)),
            ..Receiver::default()
        };
        let app = Router::new()
            .route("/hook", post(handle))
            .with_state(receiver.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{addr}/hook"), receiver)
    }

    fn new_webhook(url: &str, events: &[&str]) -> NewWebhook {
        NewWebhook {
            name: "hook".into(),
            url: url.into(),
            events: events.iter().map(|event| event.to_string()).collect(),
            enabled: None,
            signature_secret: "secret".into(),
        }
    }

    fn event(event_type: SandboxLifecycleEventType) -> SandboxLifecycleEvent {
        SandboxLifecycleEvent {
            event_type,
            sandbox_id: SandboxId::new(),
            resources: SandboxResources::default(),
            template_id: "template-1".into(),
            template_builder: false,
            user_metadata: Some([("owner".to_string(), "ci".to_string())].into()),
            timestamp: std::time::SystemTime::now(),
            source_sandbox_id: None,
        }
    }

    async fn wait_for_attempts(
        service: &WebhookService,
        id: Uuid,
        count: usize,
    ) -> Vec<DeliveryRecord> {
        for _ in 0..200 {
            let records = service.load_deliveries(id).await.unwrap();
            if records.len() >= count {
                return records;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("expected {count} delivery attempts");
    }

    #[test]
    fn signature_matches_e2b_reference() {
        // base64_nopad(sha256("secret" + body)), computed independently.
        assert_eq!(
            sign("secret", r#"{"id":1}"#),
            "0EycWlTIfit6vE1cbhFBN5CphTIXOfYwfQlzrh6AeNM"
        );
    }

    #[tokio::test]
    async fn registry_crud_round_trip() {
        let fixture = fixture().await;
        let service = &fixture.service;
        let created = service
            .create(new_webhook(
                "https://example.com/hook",
                &["sandbox.lifecycle.created"],
            ))
            .await
            .unwrap();
        assert!(created.enabled);
        assert_eq!(service.list().await.unwrap(), vec![created.clone()]);

        let updated = service
            .update(
                created.id,
                WebhookPatch {
                    enabled: Some(false),
                    events: Some(vec![
                        "sandbox.lifecycle.killed".into(),
                        "sandbox.lifecycle.killed".into(),
                    ]),
                    ..WebhookPatch::default()
                },
            )
            .await
            .unwrap();
        assert!(!updated.enabled);
        assert_eq!(updated.events, vec!["sandbox.lifecycle.killed"]);
        assert_eq!(service.get(created.id).await.unwrap(), updated);

        service.delete(created.id).await.unwrap();
        assert!(matches!(
            service.get(created.id).await,
            Err(WebhookError::NotFound)
        ));
        assert!(matches!(
            service.delete(created.id).await,
            Err(WebhookError::NotFound)
        ));
    }

    #[tokio::test]
    async fn create_rejects_invalid_input() {
        let fixture = fixture().await;
        for input in [
            new_webhook("ftp://example.com", &["sandbox.lifecycle.created"]),
            new_webhook("not a url", &["sandbox.lifecycle.created"]),
            new_webhook("https://example.com", &[]),
            new_webhook("https://example.com", &["sandbox.lifecycle.exploded"]),
            NewWebhook {
                signature_secret: String::new(),
                ..new_webhook("https://example.com", &["sandbox.lifecycle.created"])
            },
            NewWebhook {
                name: " ".into(),
                ..new_webhook("https://example.com", &["sandbox.lifecycle.created"])
            },
        ] {
            assert!(
                matches!(
                    fixture.service.create(input.clone()).await,
                    Err(WebhookError::InvalidRequest(_))
                ),
                "{input:?}"
            );
        }
    }

    #[tokio::test]
    async fn delivers_signed_event_in_e2b_format() {
        let fixture = fixture().await;
        let (url, receiver) = spawn_receiver(Vec::new()).await;
        let hook = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.paused"]))
            .await
            .unwrap();
        let event = event(SandboxLifecycleEventType::Pause);
        fixture.service.dispatch(event.clone()).await;

        let records = wait_for_attempts(&fixture.service, hook.id, 1).await;
        assert!(records[0].success);
        assert_eq!(records[0].response_status, Some(200));
        assert!(records[0].request_headers.contains("[REDACTED]"));

        let (headers, body) = receiver.requests.lock().unwrap()[0].clone();
        assert_eq!(headers["e2b-webhook-id"], hook.id.to_string());
        assert_eq!(headers["e2b-signature-version"], "v1");
        assert_eq!(headers["e2b-signature"], sign("secret", &body).as_str());
        assert_eq!(headers["e2b-delivery-id"], records[0].id.to_string());
        let payload: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(payload["type"], "sandbox.lifecycle.paused");
        assert_eq!(payload["version"], "v2");
        assert_eq!(payload["event_label"], "pause");
        assert_eq!(payload["sandbox_id"], event.sandbox_id.to_string());
        assert_eq!(payload["sandbox_template_id"], "template-1");
        assert_eq!(payload["event_data"]["sandbox_metadata"]["owner"], "ci");
        assert_eq!(payload["id"], records[0].event_id.to_string());
    }

    #[tokio::test]
    async fn fork_events_use_forked_type_with_source() {
        let fixture = fixture().await;
        let (url, receiver) = spawn_receiver(Vec::new()).await;
        let hook = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.forked"]))
            .await
            .unwrap();
        let source = SandboxId::new();
        let mut fork = event(SandboxLifecycleEventType::Fork);
        fork.source_sandbox_id = Some(source);
        fixture.service.dispatch(fork.clone()).await;
        // Created events do not match a forked-only subscription.
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;

        wait_for_attempts(&fixture.service, hook.id, 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests = receiver.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&requests[0].1).unwrap();
        assert_eq!(payload["type"], "sandbox.lifecycle.forked");
        assert_eq!(payload["event_label"], "fork");
        assert_eq!(payload["sandbox_id"], fork.sandbox_id.to_string());
        assert_eq!(
            payload["event_data"]["source_sandbox_id"],
            source.to_string()
        );
    }

    #[tokio::test]
    async fn filters_by_event_type_enabled_and_template_builder() {
        let fixture = fixture().await;
        let (url, receiver) = spawn_receiver(Vec::new()).await;
        fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.killed"]))
            .await
            .unwrap();
        fixture
            .service
            .create(NewWebhook {
                enabled: Some(false),
                ..new_webhook(&url, &["sandbox.lifecycle.created"])
            })
            .await
            .unwrap();

        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        let mut builder = event(SandboxLifecycleEventType::Delete);
        builder.template_builder = true;
        fixture.service.dispatch(builder).await;
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Delete))
            .await;

        tokio::time::sleep(Duration::from_millis(300)).await;
        let requests = receiver.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].1.contains("sandbox.lifecycle.killed"));
    }

    #[tokio::test]
    async fn retries_failures_up_to_three_attempts() {
        let fixture = fixture().await;
        let (url, receiver) = spawn_receiver(vec![500, 503, 500, 500]).await;
        let hook = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.created"]))
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;

        let records = wait_for_attempts(&fixture.service, hook.id, 3).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(receiver.requests.lock().unwrap().len(), 3);
        assert!(records.iter().all(|record| !record.success));
        assert!(records
            .iter()
            .all(|record| record.error_class.as_deref() == Some("http_error")));
        // Every retry carries a fresh delivery id for the same event.
        let delivery_ids: std::collections::HashSet<_> =
            records.iter().map(|record| record.id).collect();
        assert_eq!(delivery_ids.len(), 3);
        assert!(records
            .iter()
            .all(|record| record.event_id == records[0].event_id));

        let page = fixture
            .service
            .deliveries(
                hook.id,
                DeliveryQuery {
                    limit: 25,
                    ..DeliveryQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.groups[0].attempts.len(), 3);
        let stats = fixture.service.stats(hook.id, None, None).await.unwrap();
        assert_eq!((stats.total, stats.failed), (3, 3));
        assert_eq!(
            stats.buckets.iter().map(|bucket| bucket.total).sum::<i64>(),
            3
        );
    }

    #[tokio::test]
    async fn stops_retrying_after_success_and_classifies_transport_errors() {
        let fixture = fixture().await;
        let (url, _receiver) = spawn_receiver(vec![500]).await;
        let hook = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.created"]))
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        let records = wait_for_attempts(&fixture.service, hook.id, 2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let records_after = fixture.service.load_deliveries(hook.id).await.unwrap();
        assert_eq!(records_after.len(), 2);
        assert_eq!(records.iter().filter(|record| record.success).count(), 1);

        // Nothing listens on port 9 of localhost.
        let dead = fixture
            .service
            .create(new_webhook(
                "http://127.0.0.1:9/hook",
                &["sandbox.lifecycle.created"],
            ))
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        let records = wait_for_attempts(&fixture.service, dead.id, 3).await;
        assert!(records
            .iter()
            .all(|record| record.error_class.as_deref() == Some("transport_error")));
        assert!(records
            .iter()
            .all(|record| record.response_status.is_none()));
    }

    fn record(
        webhook_id: Uuid,
        event_id: Uuid,
        timestamp: DateTime<Utc>,
        success: bool,
    ) -> DeliveryRecord {
        DeliveryRecord {
            id: Uuid::now_v7(),
            webhook_id,
            event_id,
            sandbox_id: "sbx".into(),
            event_type: if success {
                "sandbox.lifecycle.created"
            } else {
                "sandbox.lifecycle.killed"
            }
            .into(),
            success,
            duration_ms: if success { 10 } else { 30 },
            request_body: "{}".into(),
            request_headers: "{}".into(),
            request_url: "https://example.com".into(),
            response_body: None,
            response_headers: None,
            response_status: None,
            error_class: None,
            error_message: None,
            timestamp,
        }
    }

    #[tokio::test]
    async fn deliveries_paginate_and_filter() {
        let fixture = fixture().await;
        let hook = fixture
            .service
            .create(new_webhook(
                "https://example.com",
                &["sandbox.lifecycle.created"],
            ))
            .await
            .unwrap();
        let base = Utc::now() - TimeDelta::hours(1);
        let events: Vec<_> = (0..5).map(|_| Uuid::now_v7()).collect();
        for (index, event_id) in events.iter().enumerate() {
            let timestamp = base + TimeDelta::minutes(index as i64);
            fixture
                .service
                .store_delivery(&record(hook.id, *event_id, timestamp, index % 2 == 0))
                .await
                .unwrap();
        }

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = fixture
                .service
                .deliveries(
                    hook.id,
                    DeliveryQuery {
                        cursor: cursor.clone(),
                        limit: 2,
                        ..DeliveryQuery::default()
                    },
                )
                .await
                .unwrap();
            seen.extend(page.groups.iter().map(|group| group.event_id));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let newest_first: Vec<_> = events.iter().rev().copied().collect();
        assert_eq!(seen, newest_first);

        let failed = fixture
            .service
            .deliveries(
                hook.id,
                DeliveryQuery {
                    limit: 25,
                    order_asc: true,
                    statuses: vec!["failed".into()],
                    ..DeliveryQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            failed
                .groups
                .iter()
                .map(|group| group.event_id)
                .collect::<Vec<_>>(),
            vec![events[1], events[3]]
        );

        let windowed = fixture
            .service
            .deliveries(
                hook.id,
                DeliveryQuery {
                    limit: 25,
                    start: Some(base + TimeDelta::minutes(1)),
                    end: Some(base + TimeDelta::minutes(3)),
                    event_types: vec!["sandbox.lifecycle.killed".into()],
                    ..DeliveryQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(windowed.groups.len(), 1);
        assert_eq!(windowed.groups[0].event_id, events[1]);

        assert!(matches!(
            fixture
                .service
                .deliveries(
                    hook.id,
                    DeliveryQuery {
                        cursor: Some("bad".into()),
                        limit: 2,
                        ..DeliveryQuery::default()
                    }
                )
                .await,
            Err(WebhookError::InvalidRequest(_))
        ));
        assert!(matches!(
            fixture
                .service
                .deliveries(Uuid::now_v7(), DeliveryQuery::default())
                .await,
            Err(WebhookError::NotFound)
        ));
    }

    #[tokio::test]
    async fn stats_aggregate_and_validate_range() {
        let fixture = fixture().await;
        let hook = fixture
            .service
            .create(new_webhook(
                "https://example.com",
                &["sandbox.lifecycle.created"],
            ))
            .await
            .unwrap();
        let start = Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap() - TimeDelta::hours(3);
        let end = start + TimeDelta::hours(3);
        for (offset_minutes, success) in [(5, true), (70, false), (75, true), (300, true)] {
            let timestamp = start + TimeDelta::minutes(offset_minutes);
            fixture
                .service
                .store_delivery(&record(hook.id, Uuid::now_v7(), timestamp, success))
                .await
                .unwrap();
        }
        let stats = fixture
            .service
            .stats(hook.id, Some(start), Some(end))
            .await
            .unwrap();
        assert_eq!((stats.total, stats.failed), (3, 1));
        assert_eq!(stats.buckets.len(), 3);
        assert_eq!(
            stats
                .buckets
                .iter()
                .map(|bucket| bucket.total)
                .collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
        assert_eq!(
            stats.duration_ms,
            DurationStats {
                minimum: 10.0,
                average: 50.0 / 3.0,
                maximum: 30.0
            }
        );

        assert!(matches!(
            fixture.service.stats(hook.id, Some(end), Some(start)).await,
            Err(WebhookError::InvalidRequest(_))
        ));
        assert!(matches!(
            fixture
                .service
                .stats(hook.id, Some(start - TimeDelta::days(40)), Some(end))
                .await,
            Err(WebhookError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn prune_removes_expired_and_delete_clears_history() {
        let fixture = fixture().await;
        let hook = fixture
            .service
            .create(new_webhook(
                "https://example.com",
                &["sandbox.lifecycle.created"],
            ))
            .await
            .unwrap();
        let now = Utc::now();
        fixture
            .service
            .store_delivery(&record(
                hook.id,
                Uuid::now_v7(),
                now - TimeDelta::days(8),
                true,
            ))
            .await
            .unwrap();
        fixture
            .service
            .store_delivery(&record(hook.id, Uuid::now_v7(), now, true))
            .await
            .unwrap();
        fixture.service.prune_expired(now).await.unwrap();
        assert_eq!(
            fixture
                .service
                .load_deliveries(hook.id)
                .await
                .unwrap()
                .len(),
            1
        );

        fixture.service.delete(hook.id).await.unwrap();
        assert!(fixture
            .service
            .load_deliveries(hook.id)
            .await
            .unwrap()
            .is_empty());
    }

    fn remote_record(url: String) -> WebhookRecord {
        WebhookRecord {
            id: Uuid::now_v7(),
            name: "remote".into(),
            url,
            events: vec!["sandbox.lifecycle.created".into()],
            enabled: true,
            signature_secret: "secret".into(),
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn registrations_from_another_node_apply_on_next_event() {
        let fixture = fixture().await;
        let (url, receiver) = spawn_receiver(Vec::new()).await;
        // Prime the cache with an empty registry.
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;

        // Another node registers through the shared repository and bumps the
        // generation, exactly as `create` does.
        let record = remote_record(url);
        let repository = &fixture.service.repository;
        repository.put_webhook(record.clone()).await.unwrap();
        repository.put_webhook_generation("remote-1").await.unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        wait_for_attempts(&fixture.service, record.id, 1).await;

        // A remote delete stops delivery on the very next event.
        repository.delete_webhook(&record.id).await.unwrap();
        repository.put_webhook_generation("remote-2").await.unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(receiver.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lost_generation_update_is_bounded_by_max_age() {
        let fixture = fixture().await;
        let (url, _receiver) = spawn_receiver(Vec::new()).await;
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        // Registration written without a generation bump.
        let record = remote_record(url);
        fixture
            .service
            .repository
            .put_webhook(record.clone())
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(fixture
            .service
            .load_deliveries(record.id)
            .await
            .unwrap()
            .is_empty());

        fixture
            .service
            .cache
            .write()
            .await
            .as_mut()
            .unwrap()
            .loaded_at -= REGISTRY_CACHE_MAX_AGE;
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        wait_for_attempts(&fixture.service, record.id, 1).await;
    }

    #[tokio::test]
    async fn retries_stop_after_remote_delete_and_use_rotated_secret() {
        let mut fixture = fixture().await;
        Arc::get_mut(&mut fixture.service).unwrap().retry_delays =
            [Duration::from_millis(300), Duration::from_millis(300)];
        let (url, receiver) = spawn_receiver(vec![500, 200]).await;
        let hook = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.created"]))
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        wait_for_attempts(&fixture.service, hook.id, 1).await;
        // Rotate the secret before the retry fires.
        let rotated = WebhookRecord {
            signature_secret: "rotated".into(),
            ..hook.clone()
        };
        fixture
            .service
            .repository
            .put_webhook(rotated)
            .await
            .unwrap();
        wait_for_attempts(&fixture.service, hook.id, 2).await;
        {
            let requests = receiver.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].0["e2b-signature"],
                sign("secret", &requests[0].1).as_str()
            );
            assert_eq!(
                requests[1].0["e2b-signature"],
                sign("rotated", &requests[1].1).as_str()
            );
        }

        // A webhook deleted between attempts gets no further retries.
        let (url, receiver) = spawn_receiver(vec![500, 500, 500]).await;
        let doomed = fixture
            .service
            .create(new_webhook(&url, &["sandbox.lifecycle.killed"]))
            .await
            .unwrap();
        fixture
            .service
            .dispatch(event(SandboxLifecycleEventType::Delete))
            .await;
        wait_for_attempts(&fixture.service, doomed.id, 1).await;
        fixture
            .service
            .repository
            .delete_webhook(&doomed.id)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert_eq!(receiver.requests.lock().unwrap().len(), 1);
    }
}
