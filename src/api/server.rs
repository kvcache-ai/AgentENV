use axum::{middleware, routing::get, Router};

use super::{impls::auth, proxy, ApiImpl};
use crate::observability::prometheus;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

pub fn new<I, A, E, C>(api_impl: I) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
        + apis::snapshots::Snapshots<E, Claims = C>
        + apis::templates::Templates<E, Claims = C>
        + apis::ApiKeyAuthHeader<Claims = C>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    // Keep the generated control-plane API as the primary router, then merge in
    // the hand-written `/proxy/*` entrypoints needed for the temporary reverse
    // proxy contract.
    agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
        .merge(proxy::router(api_impl.clone()))
        .route("/metrics", get(metrics_handler))
        .layer(middleware::from_fn_with_state(
            api_impl.clone(),
            proxy::sandbox_proxy_classifier::<I>,
        ))
        .layer(middleware::from_fn_with_state(
            api_impl,
            auth::require_auth::<I>,
        ))
        .layer(middleware::from_fn(prometheus::http_metrics_middleware))
}
