use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::watch;

use agentenv_observability::{init_prometheus_recorder, serve_metrics_with_extra};

pub(crate) async fn spawn(
    listen_addr: &str,
    mut shutdown: watch::Receiver<bool>,
    extra: Arc<dyn Fn() -> String + Send + Sync>,
) -> Result<()> {
    let listen_addr = listen_addr.trim();
    if listen_addr.is_empty() {
        tracing::info!("ublk daemon metrics server disabled");
        return Ok(());
    }

    init_prometheus_recorder()?;

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind ublk daemon metrics listener on {listen_addr}"))?;
    let local_addr = listener.local_addr().ok();
    tracing::info!(
        listen_addr,
        local_addr = ?local_addr,
        "ublk daemon metrics server listening"
    );

    tokio::spawn(async move {
        let result = serve_metrics_with_extra(
            listener,
            async move {
                if !*shutdown.borrow() {
                    let _ = shutdown.changed().await;
                }
            },
            Some(extra),
        )
        .await;
        if let Err(err) = result {
            tracing::error!(?err, "ublk daemon metrics server failed");
        }
    });
    Ok(())
}
