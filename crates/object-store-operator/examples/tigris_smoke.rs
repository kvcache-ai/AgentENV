//! End-to-end smoke test for the S3-compatible snapshot backend against a real
//! object store (e.g. Tigris). It exercises the SAME `build_object_store_operator`
//! path the OSS snapshot backend uses, so a successful round-trip proves that the
//! configured `addressing_style` actually reaches the wire.
//!
//! Usage:
//!   SMOKE_BUCKET=my-bucket \
//!   SMOKE_ACCESS_KEY_ID=tid_xxx SMOKE_SECRET_ACCESS_KEY=tsec_xxx \
//!   cargo run -p object-store-operator --example tigris_smoke
//!
//! Env (defaults in brackets):
//!   SMOKE_ENDPOINT  [https://t3.storage.dev]
//!   SMOKE_REGION    [auto]
//!   SMOKE_STYLE     [virtual]   -- "virtual" or "path"
//!   SMOKE_BUCKET    (required)
//!   SMOKE_ACCESS_KEY_ID / SMOKE_SECRET_ACCESS_KEY (required)
//!
//! Tip: run once with SMOKE_STYLE=path against a modern Tigris bucket to see the
//! failure this fix addresses, then again with SMOKE_STYLE=virtual to see it pass.

use object_store_operator::{
    build_object_store_operator, AddressingStyle, ObjectStoreOperatorConfig, ResolvedCredential,
};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_req(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("missing required env var {key}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env_or("SMOKE_ENDPOINT", "https://t3.storage.dev");
    let region = env_or("SMOKE_REGION", "auto");
    let style = match env_or("SMOKE_STYLE", "virtual").as_str() {
        "virtual" => AddressingStyle::Virtual,
        "path" => AddressingStyle::Path,
        other => anyhow::bail!("SMOKE_STYLE must be 'virtual' or 'path', got '{other}'"),
    };
    let bucket = env_req("SMOKE_BUCKET");
    let cred = ResolvedCredential::new(
        env_req("SMOKE_ACCESS_KEY_ID"),
        env_req("SMOKE_SECRET_ACCESS_KEY"),
        None,
        None,
    )?;

    let config = ObjectStoreOperatorConfig {
        bucket: bucket.clone(),
        endpoint: endpoint.clone(),
        region,
        addressing_style: style.clone(),
        timeout: None,
        max_retries: None,
    };

    println!("endpoint={endpoint} bucket={bucket} addressing_style={style:?}");
    let op = build_object_store_operator(&config, Some(&cred))?;

    let key = "agentenv-smoke/roundtrip.txt";
    let payload = b"agentenv addressing-style smoke test".to_vec();

    println!("-> write  {key}");
    op.write(key, payload.clone()).await?;

    println!("-> read   {key}");
    let got = op.read(key).await?.to_vec();
    anyhow::ensure!(got == payload, "read-back mismatch: object content differs");

    println!("-> delete {key}");
    op.delete(key).await?;

    println!("OK: {style:?}-host round-trip succeeded against {endpoint}");
    Ok(())
}
