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

fn env_req(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|err| anyhow::anyhow!("required env var {key}: {err}"))
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
    let bucket = env_req("SMOKE_BUCKET")?;
    let cred = ResolvedCredential::new(
        env_req("SMOKE_ACCESS_KEY_ID")?,
        env_req("SMOKE_SECRET_ACCESS_KEY")?,
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

    // Random per-run key so concurrent runs (on any host) cannot interfere and
    // the test never overwrites or deletes a pre-existing object.
    let key = format!("agentenv-smoke/{}/roundtrip.txt", uuid::Uuid::new_v4());
    let payload = b"agentenv addressing-style smoke test".to_vec();

    // A failed write can still have committed the object remotely (timeout or
    // retry ambiguity), so attempt cleanup even then; the unique key makes the
    // extra delete safe.
    println!("-> write  {key}");
    if let Err(write_err) = op.write(&key, payload.clone()).await {
        if let Err(cleanup_err) = op.delete(&key).await {
            eprintln!("warning: failed to delete {key} after write error: {cleanup_err}");
        }
        return Err(write_err.into());
    }

    // From here on the object exists remotely, so always attempt cleanup even
    // when the read or content check fails, then surface the primary error.
    let round_trip = async {
        println!("-> read   {key}");
        let got = op.read(&key).await?.to_vec();
        anyhow::ensure!(got == payload, "read-back mismatch: object content differs");
        Ok(())
    }
    .await;

    println!("-> delete {key}");
    let cleanup = op.delete(&key).await;
    if let Err(err) = &cleanup {
        eprintln!("warning: failed to delete {key}: {err}");
    }

    round_trip?;
    cleanup?;

    println!("OK: {style:?}-host round-trip succeeded against {endpoint}");
    Ok(())
}
