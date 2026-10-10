//! E2B-compatible sandbox lifecycle event webhook, backed by the custom
//! extension.
//!
//! AgentENV exposes at most one webhook: the custom extension's
//! `POST {url}/sandbox-hook/event`. It exists only while
//! `[custom_extension].url` is configured, and its URL cannot be changed.
//! Its other settings (name, subscribed events, enabled, signature secret)
//! are stored in the snapshot repository as `webhooks/extension.json`, so
//! every node sharing a repository uses the same configuration.
//!
//! Each node forwards the events of its own sandboxes. Delivery is
//! best-effort: one attempt per event, never blocking or failing the sandbox
//! operation, and failures are only logged.

use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::orchestrator::{SandboxLifecycleEvent, SandboxLifecycleEventType};
use crate::sandbox::CustomExtensionClient;
use crate::snapshot::repository::{RepositoryError, SnapshotRepository};

/// AgentENV is single-tenant; the webhook and every event belong to this team.
pub const TEAM_ID: Uuid = Uuid::nil();
/// Fixed identifier of the extension-backed webhook.
pub const EXTENSION_WEBHOOK_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_6578_7465_6e64);
const DEFAULT_NAME: &str = "custom-extension";
const MAX_CONCURRENT_DELIVERIES: usize = 64;

/// Lifecycle event names accepted in the webhook `events` filter. The first
/// four match E2B; `forked` is an AgentENV extension (E2B has no fork event).
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

/// Webhook settings stored in the repository. The URL is not stored: it is
/// always derived from `[custom_extension].url`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionWebhookConfig {
    pub name: String,
    pub events: Vec<String>,
    pub enabled: bool,
    /// Signs deliveries when set; stored as given because each delivery needs it.
    pub signature_secret: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Default for ExtensionWebhookConfig {
    /// Disabled until first updated, so extensions that do not implement the
    /// event hook are not called.
    fn default() -> Self {
        Self {
            name: DEFAULT_NAME.to_string(),
            events: EVENT_TYPES.iter().map(|event| event.to_string()).collect(),
            enabled: false,
            signature_secret: None,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        }
    }
}

/// The webhook as reported by the API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookView {
    pub id: Uuid,
    pub url: String,
    pub config: ExtensionWebhookConfig,
}

#[derive(Clone, Debug, Default)]
pub struct WebhookPatch {
    pub name: Option<String>,
    pub url: Option<String>,
    pub events: Option<Vec<String>>,
    pub enabled: Option<bool>,
    /// An empty string removes the secret and stops signing.
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
}

pub type WebhookResult<T> = Result<T, WebhookError>;

pub struct WebhookService {
    repository: Arc<dyn SnapshotRepository>,
    extension: Option<Arc<CustomExtensionClient>>,
    delivery_slots: Arc<Semaphore>,
}

impl WebhookService {
    /// Backs the webhook with the process-wide custom extension client.
    pub fn new(repository: Arc<dyn SnapshotRepository>) -> Arc<Self> {
        Self::with_client(repository, CustomExtensionClient::global())
    }

    pub(crate) fn with_client(
        repository: Arc<dyn SnapshotRepository>,
        extension: Option<Arc<CustomExtensionClient>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            repository,
            extension,
            delivery_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES)),
        })
    }

    async fn config(&self) -> WebhookResult<ExtensionWebhookConfig> {
        Ok(self
            .repository
            .get_extension_webhook()
            .await?
            .unwrap_or_default())
    }

    fn view(&self, config: ExtensionWebhookConfig) -> Option<WebhookView> {
        let extension = self.extension.as_ref()?;
        Some(WebhookView {
            id: EXTENSION_WEBHOOK_ID,
            url: extension.event_hook_url(),
            config,
        })
    }

    /// Returns the webhook, or nothing when no custom extension is configured.
    pub async fn list(&self) -> WebhookResult<Vec<WebhookView>> {
        if self.extension.is_none() {
            return Ok(Vec::new());
        }
        Ok(self.view(self.config().await?).into_iter().collect())
    }

    pub async fn get(&self, id: Uuid) -> WebhookResult<WebhookView> {
        if id != EXTENSION_WEBHOOK_ID || self.extension.is_none() {
            return Err(WebhookError::NotFound);
        }
        let config = self.config().await?;
        self.view(config).ok_or(WebhookError::NotFound)
    }

    pub async fn update(&self, id: Uuid, patch: WebhookPatch) -> WebhookResult<WebhookView> {
        let mut view = self.get(id).await?;
        if patch.url.is_some() {
            return Err(WebhookError::InvalidRequest(
                "url cannot be changed: it is the custom extension's sandbox-hook/event endpoint"
                    .to_string(),
            ));
        }
        let config = &mut view.config;
        if let Some(name) = patch.name {
            if name.trim().is_empty() {
                return Err(WebhookError::InvalidRequest(
                    "name must not be empty".to_string(),
                ));
            }
            config.name = name;
        }
        if let Some(events) = patch.events {
            validate_events(&events)?;
            config.events = dedup(events);
        }
        if let Some(enabled) = patch.enabled {
            config.enabled = enabled;
        }
        if let Some(secret) = patch.signature_secret {
            config.signature_secret = (!secret.is_empty()).then_some(secret);
        }
        if config.created_at == DateTime::<Utc>::UNIX_EPOCH {
            config.created_at = Utc::now();
        }
        self.repository
            .put_extension_webhook(view.config.clone())
            .await?;
        info!(enabled = view.config.enabled, events = ?view.config.events, "event webhook updated");
        Ok(view)
    }

    /// Forwards every lifecycle event from `events` until the channel closes.
    /// Does nothing when no custom extension is configured.
    pub fn start(
        self: &Arc<Self>,
        mut events: broadcast::Receiver<SandboxLifecycleEvent>,
    ) -> Option<JoinHandle<()>> {
        self.extension.as_ref()?;
        let this = Arc::clone(self);
        Some(tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => this.dispatch(event).await,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            "event webhook lagged; sandbox events were not forwarded"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            debug!("event webhook dispatcher stopped");
        }))
    }

    async fn dispatch(&self, event: SandboxLifecycleEvent) {
        let Some(extension) = self.extension.clone() else {
            return;
        };
        if event.template_builder {
            return;
        }
        let (event_type, _) = event_names(event.event_type);
        let config = match self.config().await {
            Ok(config) => config,
            Err(error) => {
                warn!(error = %error, sandbox_id = %event.sandbox_id, event_type, "dropping sandbox event for webhook");
                return;
            }
        };
        if !config.enabled || !config.events.iter().any(|name| name == event_type) {
            return;
        }
        let body = match serde_json::to_vec(&event_payload(&event, Uuid::now_v7())) {
            Ok(body) => body,
            Err(error) => {
                warn!(error = %error, "failed to serialize sandbox event");
                return;
            }
        };
        let Ok(permit) = Arc::clone(&self.delivery_slots).acquire_owned().await else {
            return;
        };
        let sandbox_id = event.sandbox_id;
        tokio::spawn(async move {
            let _permit = permit;
            let signature = config
                .signature_secret
                .as_deref()
                .map(|secret| sign(secret, &body));
            if let Err(error) = extension
                .hook_event(EXTENSION_WEBHOOK_ID, body, signature)
                .await
            {
                warn!(error = %format_args!("{error:#}"), %sandbox_id, event_type, "custom extension event hook failed");
            }
        });
    }
}

/// E2B webhook signature: unpadded base64 of `sha256(secret + body)`.
pub fn sign(secret: &str, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(body);
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
        // AgentENV does not expose per-runtime execution or build ids.
        "sandbox_execution_id": "",
        "sandbox_template_id": event.template_id,
        "sandbox_build_id": "",
        "sandbox_team_id": TEAM_ID,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::custom_extension::client::tests::test_client;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::types::{SandboxId, SandboxResources};
    use axum::{body::Bytes, extract::State, http::HeaderMap, routing::post, Router};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    type Requests = Arc<StdMutex<Vec<(String, HeaderMap, Bytes)>>>;

    async fn spawn_extension() -> (String, Requests) {
        async fn handle(
            State(requests): State<Requests>,
            uri: axum::http::Uri,
            headers: HeaderMap,
            body: Bytes,
        ) {
            requests
                .lock()
                .unwrap()
                .push((uri.path().to_string(), headers, body));
        }
        let requests = Requests::default();
        let app = Router::new()
            .route("/sandbox-hook/event", post(handle))
            .with_state(Arc::clone(&requests));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{addr}"), requests)
    }

    fn repository(root: &tempfile::TempDir) -> Arc<dyn SnapshotRepository> {
        PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })
        .unwrap()
        .repository()
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

    async fn wait_for(requests: &Requests, count: usize) {
        for _ in 0..100 {
            if requests.lock().unwrap().len() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("expected {count} event hook requests");
    }

    fn enable(events: &[&str], secret: Option<&str>) -> WebhookPatch {
        WebhookPatch {
            enabled: Some(true),
            events: Some(events.iter().map(|event| event.to_string()).collect()),
            signature_secret: secret.map(str::to_string),
            ..WebhookPatch::default()
        }
    }

    #[test]
    fn signature_matches_e2b_reference() {
        // base64_nopad(sha256("secret" + body)), computed independently.
        assert_eq!(
            sign("secret", br#"{"id":1}"#),
            "0EycWlTIfit6vE1cbhFBN5CphTIXOfYwfQlzrh6AeNM"
        );
    }

    #[tokio::test]
    async fn no_extension_means_no_webhook() {
        let root = tempfile::tempdir().unwrap();
        let service = WebhookService::with_client(repository(&root), None);
        assert!(service.list().await.unwrap().is_empty());
        assert!(matches!(
            service.get(EXTENSION_WEBHOOK_ID).await,
            Err(WebhookError::NotFound)
        ));
        assert!(matches!(
            service
                .update(EXTENSION_WEBHOOK_ID, enable(&[], None))
                .await,
            Err(WebhookError::NotFound)
        ));
        assert!(service.start(broadcast::channel(1).1).is_none());
    }

    #[tokio::test]
    async fn defaults_then_update_persists_shared_config() {
        let root = tempfile::tempdir().unwrap();
        let repository = repository(&root);
        let client = Arc::new(test_client("http://extension.invalid"));
        let service = WebhookService::with_client(Arc::clone(&repository), Some(client.clone()));

        let listed = service.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        let view = &listed[0];
        assert_eq!(view.id, EXTENSION_WEBHOOK_ID);
        assert_eq!(view.url, "http://extension.invalid/sandbox-hook/event");
        assert!(!view.config.enabled, "disabled until first updated");
        assert_eq!(view.config.events.len(), EVENT_TYPES.len());
        assert!(repository.get_extension_webhook().await.unwrap().is_none());

        let updated = service
            .update(
                EXTENSION_WEBHOOK_ID,
                WebhookPatch {
                    name: Some("audit".into()),
                    ..enable(
                        &["sandbox.lifecycle.killed", "sandbox.lifecycle.killed"],
                        Some("s3cret"),
                    )
                },
            )
            .await
            .unwrap();
        assert!(updated.config.enabled);
        assert_eq!(updated.config.name, "audit");
        assert_eq!(updated.config.events, vec!["sandbox.lifecycle.killed"]);
        assert_ne!(updated.config.created_at, DateTime::<Utc>::UNIX_EPOCH);

        // Another node sharing the repository sees the same config.
        let other = WebhookService::with_client(repository, Some(client));
        assert_eq!(other.get(EXTENSION_WEBHOOK_ID).await.unwrap(), updated);

        // An empty secret removes it.
        let cleared = other
            .update(
                EXTENSION_WEBHOOK_ID,
                WebhookPatch {
                    signature_secret: Some(String::new()),
                    ..WebhookPatch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(cleared.config.signature_secret, None);
        assert_eq!(cleared.config.created_at, updated.config.created_at);
    }

    #[tokio::test]
    async fn update_rejects_url_and_invalid_fields() {
        let root = tempfile::tempdir().unwrap();
        let service = WebhookService::with_client(
            repository(&root),
            Some(Arc::new(test_client("http://extension.invalid"))),
        );
        for patch in [
            WebhookPatch {
                url: Some("http://elsewhere.invalid".into()),
                ..WebhookPatch::default()
            },
            enable(&[], None),
            enable(&["sandbox.lifecycle.updated"], None),
            WebhookPatch {
                name: Some(" ".into()),
                ..WebhookPatch::default()
            },
        ] {
            assert!(
                matches!(
                    service.update(EXTENSION_WEBHOOK_ID, patch.clone()).await,
                    Err(WebhookError::InvalidRequest(_))
                ),
                "{patch:?}"
            );
        }
        assert!(matches!(
            service.get(Uuid::now_v7()).await,
            Err(WebhookError::NotFound)
        ));
        // Nothing was persisted by the rejected updates.
        assert!(
            !service
                .get(EXTENSION_WEBHOOK_ID)
                .await
                .unwrap()
                .config
                .enabled
        );
    }

    #[tokio::test]
    async fn forwards_signed_event_with_exact_body() {
        let root = tempfile::tempdir().unwrap();
        let (base, requests) = spawn_extension().await;
        let service =
            WebhookService::with_client(repository(&root), Some(Arc::new(test_client(&base))));
        service
            .update(
                EXTENSION_WEBHOOK_ID,
                enable(&["sandbox.lifecycle.forked"], Some("s3cret")),
            )
            .await
            .unwrap();
        let source = SandboxId::new();
        let mut fork = event(SandboxLifecycleEventType::Fork);
        fork.source_sandbox_id = Some(source);
        service.dispatch(fork.clone()).await;
        wait_for(&requests, 1).await;

        let (path, headers, body) = requests.lock().unwrap()[0].clone();
        assert_eq!(path, "/sandbox-hook/event");
        assert_eq!(headers["e2b-webhook-id"], EXTENSION_WEBHOOK_ID.to_string());
        assert_eq!(headers["e2b-signature-version"], "v1");
        assert_eq!(headers["e2b-signature"], sign("s3cret", &body).as_str());
        assert!(headers.contains_key("e2b-delivery-id"));
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["type"], "sandbox.lifecycle.forked");
        assert_eq!(payload["version"], "v2");
        assert_eq!(payload["event_label"], "fork");
        assert_eq!(payload["sandbox_id"], fork.sandbox_id.to_string());
        assert_eq!(payload["sandbox_template_id"], "template-1");
        assert_eq!(payload["event_data"]["sandbox_metadata"]["owner"], "ci");
        assert_eq!(
            payload["event_data"]["source_sandbox_id"],
            source.to_string()
        );
    }

    #[tokio::test]
    async fn filters_disabled_unsubscribed_and_template_builder_events() {
        let root = tempfile::tempdir().unwrap();
        let (base, requests) = spawn_extension().await;
        let service =
            WebhookService::with_client(repository(&root), Some(Arc::new(test_client(&base))));
        // Disabled by default: nothing is sent.
        service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;

        service
            .update(
                EXTENSION_WEBHOOK_ID,
                enable(&["sandbox.lifecycle.killed"], None),
            )
            .await
            .unwrap();
        service
            .dispatch(event(SandboxLifecycleEventType::Create))
            .await;
        let mut builder = event(SandboxLifecycleEventType::Delete);
        builder.template_builder = true;
        service.dispatch(builder).await;
        service
            .dispatch(event(SandboxLifecycleEventType::Delete))
            .await;

        wait_for(&requests, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (_, headers, body) = &requests[0];
        assert!(
            !headers.contains_key("e2b-signature"),
            "unsigned without a secret"
        );
        assert!(String::from_utf8_lossy(body).contains("sandbox.lifecycle.killed"));
    }

    #[tokio::test]
    async fn dispatcher_forwards_broadcast_events() {
        let root = tempfile::tempdir().unwrap();
        let (base, requests) = spawn_extension().await;
        let service =
            WebhookService::with_client(repository(&root), Some(Arc::new(test_client(&base))));
        service
            .update(
                EXTENSION_WEBHOOK_ID,
                enable(&["sandbox.lifecycle.created"], None),
            )
            .await
            .unwrap();
        let (tx, rx) = broadcast::channel(16);
        let dispatcher = service.start(rx).expect("extension is configured");
        tx.send(event(SandboxLifecycleEventType::Create)).unwrap();
        wait_for(&requests, 1).await;
        drop(tx);
        tokio::time::timeout(Duration::from_secs(2), dispatcher)
            .await
            .expect("dispatcher stops when the channel closes")
            .unwrap();
    }
}
