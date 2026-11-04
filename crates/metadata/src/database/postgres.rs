use anyhow::{anyhow, Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;

/// Open a connection pool without changing the database schema. The owner closes the pool
/// after its database consumers finish.
pub async fn open_pool(url: &str) -> Result<PgPool> {
    // The parser logs unknown URL parameters before returning; their values may be secrets.
    let options = tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        url.parse::<PgConnectOptions>()
    })
    .map_err(|_| anyhow!("invalid database.postgres_url"))?;
    PgPool::connect_with(options)
        .await
        .context("connect PostgreSQL database")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_parameters_do_not_expose_credentials() {
        let output = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(output.reopen().unwrap()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let error = futures::executor::block_on(open_pool(
                "postgresql://localhost/db?passwrod=private-password&sslmode=private-password",
            ))
            .unwrap_err();
            assert_eq!(error.to_string(), "invalid database.postgres_url");
            tracing::warn!("parser returned");
        });
        let log = std::fs::read_to_string(output.path()).unwrap();
        assert!(log.contains("parser returned"));
        assert!(!log.contains("private-password"));
    }
}
