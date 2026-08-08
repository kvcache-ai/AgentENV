//! Hand-written upload endpoint for template build-context archives.
//!
//! `GET /templates/{templateID}/files/{hash}` (generated API) hands the E2B
//! SDK a bearer URL pointing here; the SDK then `PUT`s a tar archive with no
//! authentication headers. The durable random token embedded in the URL is
//! therefore the credential, and this route stays outside the generated
//! router so the archive can stream to disk instead of buffering in memory.

use std::time::Duration;

use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

use agentenv_http_server::models;

use super::ApiImpl;
use crate::cfg::ConfigManager;
use crate::snapshot::repository::build_files::is_valid_build_files_hash;

pub(crate) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/templates/{template_id}/files/{hash}/content",
            put(upload_build_archive::<I>),
        )
        .with_state(api_impl)
}

struct UploadQuery {
    expires: i64,
    token: String,
}

fn parse_upload_query(query: Option<&str>) -> Option<UploadQuery> {
    let query = query?;
    let mut expires: Option<i64> = None;
    let mut token: Option<String> = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "expires" => expires = value.parse().ok(),
            "token" => token = Some(value.into_owned()),
            _ => {}
        }
    }
    Some(UploadQuery {
        expires: expires?,
        token: token?,
    })
}

fn error_response(code: StatusCode, message: impl Into<String>) -> Response {
    (
        code,
        Json(models::Error::new(code.as_u16() as i32, message.into())),
    )
        .into_response()
}

async fn upload_build_archive<I>(
    State(api_impl): State<I>,
    Path((template_id, hash)): Path<(String, String)>,
    request: Request,
) -> Response
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let api: &ApiImpl = api_impl.as_ref();

    if !is_valid_build_files_hash(&hash) {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("invalid build files hash '{hash}'"),
        );
    }
    let Some(store) = api.snapshot_manager().template_build_files() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "the configured snapshot backend does not support build-context uploads",
        );
    };
    let Some(query) = parse_upload_query(request.uri().query()) else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "upload URL is missing the expires/token query parameters",
        );
    };

    // Verification does not consume the grant: consumption happens only after
    // the archive has been durably published, so an upload that fails while
    // streaming, staging, or storing the body stays retryable with this URL.
    let now_unix = chrono::Utc::now().timestamp();
    let authorized = match store
        .verify_upload_grant(&query.token, &template_id, &hash, query.expires, now_unix)
        .await
    {
        Ok(authorized) => authorized,
        Err(error) => {
            warn!(error = %error, "failed to verify build-file upload grant");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to validate upload grant",
            );
        }
    };
    if !authorized {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "upload grant is invalid, expired, or already used; request a fresh upload link",
        );
    }

    let max_bytes = ConfigManager::global_config()
        .template_build
        .files_max_upload_mib
        .saturating_mul(1024 * 1024);
    let upload_timeout = Duration::from_secs(
        ConfigManager::global_config()
            .template_build
            .files_upload_timeout_secs,
    );

    // `staged` is the drop guard that removes the staging file on every early
    // return below, so it must stay bound for the rest of the handler.
    let staged = match tokio::task::spawn_blocking(tempfile::NamedTempFile::new).await {
        Ok(Ok(staged)) => staged,
        Ok(Err(error)) => {
            warn!(error = %error, "failed to create staging file for build archive");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to stage build archive",
            );
        }
        Err(error) => {
            warn!(error = %error, "failed to join staging file creation for build archive");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to stage build archive",
            );
        }
    };
    let staged_path = staged.path().to_path_buf();

    let mut file = match tokio::fs::File::create(&staged_path).await {
        Ok(file) => file,
        Err(error) => {
            warn!(error = %error, "failed to open staging file for build archive");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to stage build archive",
            );
        }
    };

    let consume_body = async {
        let mut total: u64 = 0;
        let mut stream = request.into_body().into_data_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    debug!(error = %error, "build archive upload stream aborted");
                    return Err(error_response(
                        StatusCode::BAD_REQUEST,
                        "failed to read the uploaded archive body",
                    ));
                }
            };
            total += chunk.len() as u64;
            if total > max_bytes {
                return Err(error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("build archive exceeds the configured limit of {max_bytes} bytes"),
                ));
            }
            if let Err(error) = file.write_all(&chunk).await {
                warn!(error = %error, "failed to write staged build archive");
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to stage build archive",
                ));
            }
        }
        if let Err(error) = file.flush().await {
            warn!(error = %error, "failed to flush staged build archive");
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to stage build archive",
            ));
        }
        Ok(total)
    };

    let total = match tokio::time::timeout(upload_timeout, consume_body).await {
        Ok(Ok(total)) => total,
        Ok(Err(response)) => return response,
        Err(_) => {
            debug!(template_id, hash, "build archive upload timed out");
            return error_response(
                StatusCode::REQUEST_TIMEOUT,
                format!(
                    "build archive upload did not complete within {} seconds",
                    upload_timeout.as_secs()
                ),
            );
        }
    };
    drop(file);

    // Publishing before the grant is consumed keeps a failed store retryable
    // with the same URL. An unclaimed replay reaching this point is harmless:
    // the token authorizes exactly this template_id/hash and `import` is
    // first-write-wins, so it can neither publish a different key nor change
    // what is already stored.
    //
    // `hash` is the cache key the SDK computed for this build context, not a
    // digest of the received bytes that the server verified.
    if let Err(error) = store.import(&hash, &staged_path).await {
        warn!(error = %error, hash, "failed to import build archive");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to store build archive; the upload can be retried with the same link",
        );
    }

    // The archive is published, so the claim only enforces single-use: the
    // atomic remove/delete picks a single winner among concurrent replays, and
    // a replay that loses the race is rejected even though the archive it
    // uploaded is stored. `now_unix` is the timestamp taken before the body was
    // read, so a slow but authorized upload is not rejected for aging past the
    // TTL.
    let claimed = match store
        .claim_upload_grant(&query.token, &template_id, &hash, query.expires, now_unix)
        .await
    {
        Ok(claimed) => claimed,
        Err(error) => {
            warn!(error = %error, "failed to claim build-file upload grant");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to validate upload grant",
            );
        }
    };
    if !claimed {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "upload grant is invalid, expired, or already used; request a fresh upload link",
        );
    }

    debug!(
        template_id,
        hash,
        bytes = total,
        "stored build-context archive"
    );
    StatusCode::OK.into_response()
}

#[cfg(test)]
mod tests {
    use super::parse_upload_query;

    #[test]
    fn upload_query_parses_bearer_token_and_expiry() {
        let query = parse_upload_query(Some("expires=1234&token=upload-token"))
            .expect("query should parse");
        assert_eq!(query.expires, 1234);
        assert_eq!(query.token, "upload-token");
    }

    #[test]
    fn upload_query_requires_both_fields() {
        assert!(parse_upload_query(Some("expires=1234")).is_none());
        assert!(parse_upload_query(Some("token=upload-token")).is_none());
        assert!(parse_upload_query(None).is_none());
    }
}
