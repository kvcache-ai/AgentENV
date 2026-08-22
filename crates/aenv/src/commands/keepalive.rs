use super::DEFAULT_TIMEOUT_SECS;
use crate::client::Client;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::task::JoinHandle;

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
const KEEPALIVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Owns a background lease-refresh task and stops it when the command or
/// terminal session ends.
pub(crate) struct KeepAliveTask(JoinHandle<()>);

impl Drop for KeepAliveTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Refreshes a sandbox lease once, independently from the command stream.
pub(crate) fn spawn_once(client: Client, sandbox_id: String) -> KeepAliveTask {
    KeepAliveTask(tokio::spawn(async move {
        refresh(client, sandbox_id).await;
    }))
}

/// Runs sandbox lease refreshes independently from a terminal stream for the
/// duration of the interactive session. The first tick is immediate so a
/// newly-connected sandbox starts checking for activity without delaying the
/// terminal session. A refresh is sent only when input or visible output has
/// occurred since the previous check.
pub(crate) fn spawn_periodic(
    client: Client,
    sandbox_id: String,
    activity_generation: Arc<AtomicU64>,
) -> KeepAliveTask {
    KeepAliveTask(tokio::spawn(async move {
        let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
        let mut last_activity = activity_generation.load(Ordering::Relaxed);

        loop {
            interval.tick().await;
            if !should_refresh(&activity_generation, &mut last_activity) {
                continue;
            }
            refresh(client.clone(), sandbox_id.clone()).await;
        }
    }))
}

fn should_refresh(activity_generation: &AtomicU64, last_activity: &mut u64) -> bool {
    let current_activity = activity_generation.load(Ordering::Relaxed);
    if current_activity == *last_activity {
        return false;
    }
    *last_activity = current_activity;
    true
}

async fn refresh(client: Client, sandbox_id: String) {
    let refresh = tokio::task::spawn_blocking(move || {
        client.refresh_sandbox(&sandbox_id, Some(DEFAULT_TIMEOUT_SECS))
    });

    let _ = tokio::time::timeout(KEEPALIVE_REQUEST_TIMEOUT, refresh).await;
}

#[cfg(test)]
mod tests {
    use super::should_refresh;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn refresh_requires_new_activity() {
        let activity = AtomicU64::new(0);
        let mut last_activity = activity.load(Ordering::Relaxed);

        assert!(!should_refresh(&activity, &mut last_activity));

        activity.fetch_add(1, Ordering::Relaxed);
        assert!(should_refresh(&activity, &mut last_activity));
        assert!(!should_refresh(&activity, &mut last_activity));
    }
}
