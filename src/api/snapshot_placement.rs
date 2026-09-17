use agentenv_http_server::apis::ApiKeyAuthHeader;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::ApiImpl;
use crate::repository::RepositoryError;
use crate::snapshot::SnapshotPlacement;

const PLACEMENT_ROUTE: &str = "/internal/snapshots/placement";

#[derive(Debug, Deserialize)]
struct PlacementQuery {
    #[serde(rename = "snapshotID")]
    snapshot_ref: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlacementResponse {
    snapshot_type: SnapshotPlacement,
    #[serde(rename = "ownerNodeID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_node_id: Option<String>,
}

fn placement_error_status(error: &anyhow::Error) -> StatusCode {
    match ApiImpl::find_repository_error(error) {
        Some(RepositoryError::InvalidRequest { .. }) => StatusCode::BAD_REQUEST,
        Some(RepositoryError::Unavailable { .. } | RepositoryError::Backend { .. }) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(PLACEMENT_ROUTE, get(get_placement::<I>))
        .with_state(api_impl)
}

async fn get_placement<I>(
    State(api_impl): State<I>,
    Query(query): Query<PlacementQuery>,
    headers: HeaderMap,
) -> Result<Json<PlacementResponse>, StatusCode>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    if api_impl
        .as_ref()
        .extract_claims_from_header(&headers, "X-API-Key")
        .await
        .is_none()
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let record = api_impl
        .as_ref()
        .snapshot_manager()
        .get(&query.snapshot_ref)
        .await
        .map_err(|error| placement_error_status(&error))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let owner_node_id = match record.snapshot_type {
        SnapshotPlacement::Distributed => None,
        SnapshotPlacement::Local => Some(
            record
                .owner_node_id
                .filter(|owner| !owner.is_empty())
                .ok_or(StatusCode::SERVICE_UNAVAILABLE)?,
        ),
    };
    Ok(Json(PlacementResponse {
        snapshot_type: record.snapshot_type,
        owner_node_id,
    }))
}
