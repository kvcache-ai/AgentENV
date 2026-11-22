use std::future::Future;
use std::time::Duration;

use agentenv::p2p::{P2pError, P2pResult};

pub async fn await_p2p_transport_shutdown(
    shutdown: impl Future<Output = P2pResult<()>>,
    timeout: Duration,
) -> P2pResult<()> {
    match tokio::time::timeout(timeout, shutdown).await {
        Ok(result) => result,
        Err(_) => Err(P2pError::Timeout {
            operation: "shutdown P2P transport",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pending_shutdown_obeys_the_deadline() {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            await_p2p_transport_shutdown(
                std::future::pending::<P2pResult<()>>(),
                Duration::from_millis(10),
            ),
        )
        .await;
        assert!(matches!(
            result,
            Ok(Err(P2pError::Timeout {
                operation: "shutdown P2P transport"
            }))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn successful_shutdown_returns_immediately() {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            await_p2p_transport_shutdown(
                std::future::ready(Ok(())),
                Duration::from_millis(10),
            ),
        )
        .await;
        assert!(matches!(result, Ok(Ok(()))));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_error_is_not_rewritten_as_timeout() {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            await_p2p_transport_shutdown(
                std::future::ready(Err(P2pError::TransportDisabled)),
                Duration::from_millis(10),
            ),
        )
        .await;
        assert!(matches!(result, Ok(Err(P2pError::TransportDisabled))));
    }
}
