use std::{error::Error as StdError, future::Future, time::Duration};

use axum::{
    body::Body,
    extract::{
        ws::{
            rejection::WebSocketUpgradeRejection, CloseFrame, Message as WebSocketMessage,
            WebSocket, WebSocketUpgrade,
        },
        FromRequestParts, MatchedPath, Request, State,
    },
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri},
    middleware::Next,
    response::IntoResponse,
    routing::any,
    Router,
};
use futures::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::{sync::watch, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        self,
        client::IntoClientRequest,
        protocol::{frame::coding::CloseCode, Message as TungsteniteMessage},
    },
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, info, trace, warn};

use crate::{
    api::{impls::auth::TRAFFIC_ACCESS_TOKEN_HEADER, ApiImpl},
    cfg::ConfigManager,
    observability::prometheus::HttpRouteSource,
    orchestrator::{
        NewTimeout, OrchestratorError, ProxyLookupResult, ProxyTarget, SandboxMetadata,
        SandboxState,
    },
    types::SandboxId,
};

#[cfg(test)]
use crate::api::impls::auth::{API_KEY_HEADER, ENVD_ACCESS_TOKEN_HEADER};

/// Shared outbound HTTP client for the client-facing reverse proxy.
pub(crate) type ProxyClient = Client<HttpConnector, Body>;
type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct ResolvedProxyRequest {
    sandbox_id: SandboxId,
    upstream_uri: Uri,
    original_host: Option<HeaderValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostProxyRoute {
    sandbox_id: SandboxId,
    target_port: u16,
}

#[derive(Debug)]
enum ProxyRequestError {
    MissingSandboxId,
    InvalidSandboxId,
    MissingTargetPort,
    InvalidTargetPort,
    InvalidHostRoute(&'static str),
    SandboxNotFound(SandboxId),
    SandboxUnavailable(SandboxId, SandboxState),
    AutoResumeFailed(SandboxId),
    AutoResumeTimedOut(SandboxId),
    MissingRuntimeRoute(SandboxId),
    InvalidUpstreamUri,
    InternalServerError,
}

const PROXY_ROUTE: &str = "/proxy";
const ENVD_STREAM_INPUT_PATH: &str = "/process.Process/StreamInput";
/// Header carrying the target sandbox chosen by the client.
const SANDBOX_ID_HEADER: &str = "x-agentenv-sandbox-id";
/// E2B-compatible alias for the sandbox routing header.
const E2B_SANDBOX_ID_HEADER: &str = "e2b-sandbox-id";
/// Header carrying the destination port inside the sandbox network.
const TARGET_PORT_HEADER: &str = "x-agentenv-target-port";
/// E2B-compatible alias for the target port header.
const E2B_TARGET_PORT_HEADER: &str = "e2b-sandbox-port";
#[cfg(test)]
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
const PROXY_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const PROXY_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
const PROXY_REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const PROXY_REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
const PROXY_AUTO_RESUME_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const PROXY_AUTO_RESUME_TIMEOUT: Duration = Duration::from_secs(60);

fn auto_resume_min_sandbox_timeout() -> Duration {
    static AUTO_RESUME_MIN_SANDBOX_TIMEOUT: std::sync::OnceLock<Duration> =
        std::sync::OnceLock::new();

    *AUTO_RESUME_MIN_SANDBOX_TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            ConfigManager::global_config()
                .orchestrator
                .auto_resume_min_sandbox_timeout_secs,
        )
    })
}

pub(crate) fn build_proxy_client() -> ProxyClient {
    let mut connector = HttpConnector::new();
    // Proxied requests and responses are small, so Nagle only ever adds a
    // delayed-ACK wait to them.
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(PROXY_CONNECT_TIMEOUT));
    // Interaction IPs are reused across sandbox runtime generations. Hyper keys
    // its idle pool by authority, so a pooled connection can retain a stale VM flow.
    Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(0)
        .build(connector)
}

pub(crate) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(PROXY_ROUTE, any(proxy_via_prefix::<I>))
        .route("/proxy/{*proxy_path}", any(proxy_via_prefix::<I>))
        .fallback(proxy_via_fallback::<I>)
        .with_state(api_impl)
}

pub(crate) fn route_for_auth(request: &Request, domains: &[String]) -> Option<(SandboxId, u16)> {
    if !has_proxy_prefix(request.uri().path()) {
        match parse_host_proxy_route(request_host(request), domains) {
            Ok(Some(route)) => return Some((route.sandbox_id, route.target_port)),
            Ok(None) => {}
            Err(_) => return None,
        }
    }

    Some((
        parse_sandbox_id_header(request.headers()).ok()?,
        parse_target_port_header(request.headers()).ok()?,
    ))
}

pub(crate) fn is_sandbox_proxy_request(request: &Request, domains: &[String]) -> bool {
    let path = request.uri().path();
    if has_proxy_prefix(path) {
        return true;
    }

    match parse_host_proxy_route(request_host(request), domains) {
        Ok(Some(_)) | Err(_) => true,
        Ok(None) => {
            request.extensions().get::<MatchedPath>().is_none()
                && has_routing_header(request.headers())
        }
    }
}

fn request_host(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
        })
}

pub(crate) async fn sandbox_proxy_classifier<I>(
    State(api_impl): State<I>,
    request: Request,
    next: Next,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let path = request.uri().path();
    if has_proxy_prefix(path) {
        return next.run(request).await;
    }

    let host_route = match parse_host_proxy_route(
        request_host(&request),
        api_impl.as_ref().sandbox_proxy_domains(),
    ) {
        Ok(Some(route)) => route,
        Ok(None) => {
            return next.run(request).await;
        }
        Err(err) => {
            return with_route_source(proxy_error_response(&err), HttpRouteSource::ProxyHost);
        }
    };

    let (mut parts, body) = request.into_parts();
    parts.headers.insert(
        HeaderName::from_static(SANDBOX_ID_HEADER),
        HeaderValue::from_str(&host_route.sandbox_id.to_string())
            .expect("sandbox ids are valid header values"),
    );
    parts.headers.insert(
        HeaderName::from_static(TARGET_PORT_HEADER),
        HeaderValue::from_str(&host_route.target_port.to_string())
            .expect("ports are valid header values"),
    );
    // Middleware cannot receive extractor params like Axum handlers do, so pull
    // the WebSocket upgrade out of request parts before rebuilding the request.
    let websocket_upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &api_impl).await;
    let request = Request::from_parts(parts, body);
    let forward_path = request.uri().path().to_owned();

    with_route_source(
        proxy_request(api_impl.as_ref(), websocket_upgrade, request, forward_path).await,
        HttpRouteSource::ProxyHost,
    )
}

/// Handler for the explicit `/proxy` and `/proxy/*` routes. The `/proxy`
/// prefix is stripped before the request is forwarded to the upstream
/// sandbox.
async fn proxy_via_prefix<I>(
    State(api_impl): State<I>,
    websocket_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    request: Request,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Send + Sync,
{
    let forward_path = strip_proxy_prefix(request.uri().path()).to_owned();
    with_route_source(
        proxy_request(api_impl.as_ref(), websocket_upgrade, request, forward_path).await,
        HttpRouteSource::ProxyPrefix,
    )
}

/// Fallback handler that triggers when no other route matches. If the request
/// carries a sandbox-id routing header, it is dispatched to the proxy handler
/// using the original path unmodified. This mirrors the distributed gateway's
/// header-based dispatch so clients can use `E2B_SANDBOX_URL=${E2B_API_URL}`
/// in both standalone and multi-node deployments without appending `/proxy`.
async fn proxy_via_fallback<I>(
    State(api_impl): State<I>,
    websocket_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    request: Request,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Send + Sync,
{
    if !has_routing_header(request.headers()) {
        // Unmatched control-plane route: return the API error envelope so
        // JSON clients surface "route not found" instead of failing to parse
        // an empty 404 body.
        let error = agentenv_http_server::models::Error::new(
            404,
            format!(
                "route not found: {} {}",
                request.method(),
                request.uri().path()
            ),
        );
        return (StatusCode::NOT_FOUND, axum::Json(error)).into_response();
    }
    let forward_path = request.uri().path().to_owned();
    with_route_source(
        proxy_request(api_impl.as_ref(), websocket_upgrade, request, forward_path).await,
        HttpRouteSource::ProxyHeader,
    )
}

fn with_route_source(mut response: Response<Body>, source: HttpRouteSource) -> Response<Body> {
    response.extensions_mut().insert(source);
    response
}

async fn proxy_request(
    api_impl: &ApiImpl,
    websocket_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    request: Request,
    forward_path: String,
) -> Response<Body> {
    let is_websocket_request = is_websocket_upgrade_request(request.headers());
    let (parts, body) = request.into_parts();
    let resolved =
        match resolve_proxy_request(api_impl, &forward_path, &parts, is_websocket_request).await {
            Ok(resolved) => resolved,
            Err(error) => return proxy_error_response(&error),
        };

    if is_websocket_request {
        return proxy_websocket_request(websocket_upgrade, parts, resolved).await;
    }

    proxy_http_request(api_impl, parts, body, resolved).await
}

fn strip_proxy_prefix(path: &str) -> &str {
    path.strip_prefix(PROXY_ROUTE).unwrap_or("")
}

pub(crate) fn has_proxy_prefix(path: &str) -> bool {
    path == PROXY_ROUTE || path.starts_with("/proxy/")
}

fn parse_host_proxy_route(
    raw_host: Option<&str>,
    domains: &[String],
) -> Result<Option<HostProxyRoute>, ProxyRequestError> {
    if domains.is_empty() {
        return Ok(None);
    }

    let host = raw_host
        .map(|host| {
            strip_host_port(host.trim())
                .trim_end_matches('.')
                .to_ascii_lowercase()
        })
        .unwrap_or_default();
    if host.is_empty() {
        return Ok(None);
    }

    for domain in domains {
        let domain = domain.as_str();
        if host == domain {
            return Ok(None);
        }

        let Some(prefix) = host.strip_suffix(domain) else {
            continue;
        };
        let Some(label) = prefix.strip_suffix('.') else {
            continue;
        };
        if label.is_empty() || label.contains('.') {
            continue;
        }

        let Some((port, sandbox_id)) = label.split_once('-') else {
            continue;
        };
        if sandbox_id.is_empty() {
            return Err(ProxyRequestError::InvalidHostRoute(
                "invalid sandbox data-plane host: sandbox id is empty",
            ));
        }

        let target_port = port.parse::<u16>().ok().filter(|port| *port > 0).ok_or(
            ProxyRequestError::InvalidHostRoute("invalid sandbox data-plane host: port is invalid"),
        )?;
        let sandbox_id = SandboxId::parse_str(sandbox_id).map_err(|_| {
            ProxyRequestError::InvalidHostRoute(
                "invalid sandbox data-plane host: sandbox id is invalid",
            )
        })?;

        return Ok(Some(HostProxyRoute {
            sandbox_id,
            target_port,
        }));
    }

    Ok(None)
}

fn strip_host_port(host: &str) -> &str {
    let Some((without_port, port)) = host.rsplit_once(':') else {
        return host;
    };
    if !without_port.is_empty()
        && !without_port.contains(':')
        && port.chars().all(|c| c.is_ascii_digit())
    {
        without_port
    } else {
        host
    }
}

fn has_routing_header(headers: &HeaderMap) -> bool {
    headers.get(SANDBOX_ID_HEADER).is_some() || headers.get(E2B_SANDBOX_ID_HEADER).is_some()
}

pub(crate) fn effective_envd_port(metadata: &SandboxMetadata) -> u16 {
    metadata
        .paused_state
        .as_ref()
        .and_then(|state| state.control_plane_port())
        .unwrap_or_else(|| ConfigManager::global_config().tools.control_plane_port)
}

/// Proxies a standard HTTP request to the resolved upstream URI and returns the response.
async fn proxy_http_request(
    api_impl: &ApiImpl,
    mut parts: http::request::Parts,
    body: Body,
    resolved: ResolvedProxyRequest,
) -> Response<Body> {
    let ResolvedProxyRequest {
        sandbox_id,
        upstream_uri,
        original_host,
    } = resolved;

    sanitize_request_headers(&mut parts.headers);
    inject_forwarded_headers(
        &mut parts.headers,
        original_host.as_ref(),
        &parts.method,
        &upstream_uri,
        "http",
    );
    if let Some(authority) = upstream_uri.authority() {
        if let Ok(host) = HeaderValue::from_str(authority.as_str()) {
            parts.headers.insert(header::HOST, host);
        }
    }
    parts.uri = upstream_uri.clone();

    trace!(
        sandbox_id = %sandbox_id,
        method = %parts.method,
        upstream = %upstream_uri,
        "proxying client request"
    );

    let upstream_method = parts.method.clone();
    let upstream_uri_for_log = upstream_uri.clone();
    let is_stream_input =
        is_envd_stream_input_request(&upstream_method, upstream_uri_for_log.path());
    if is_stream_input {
        info!(
            sandbox_id = %sandbox_id,
            method = %upstream_method,
            upstream = %upstream_uri_for_log,
            "client attached to sandbox"
        );
    }

    let (body, activity_rx) = track_request_body_activity(body);
    let upstream_request = Request::from_parts(parts, body);
    let upstream_response_result = match wait_for_upstream_response_headers_with_activity_timeout(
        api_impl.proxy_client().request(upstream_request),
        activity_rx,
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            warn!(
                sandbox_id = %sandbox_id,
                method = %upstream_method,
                upstream = %upstream_uri_for_log,
                request_body_idle_timeout_ms = PROXY_REQUEST_BODY_IDLE_TIMEOUT.as_millis(),
                response_header_timeout_ms = PROXY_RESPONSE_HEADER_TIMEOUT.as_millis(),
                "timed out waiting for upstream response headers or request body progress"
            );
            return StatusCode::GATEWAY_TIMEOUT.into_response();
        }
    };

    let upstream_response = match upstream_response_result {
        Ok(response) => {
            if is_stream_input {
                info!(
                    sandbox_id = %sandbox_id,
                    status = %response.status(),
                    "client detached from sandbox"
                );
            }
            response
        }
        Err(err) => {
            if is_benign_stream_input_disconnect(
                &upstream_method,
                upstream_uri_for_log.path(),
                &err,
            ) {
                info!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "client detached from sandbox"
                );
                return StatusCode::BAD_GATEWAY.into_response();
            }
            warn!(sandbox_id = %sandbox_id, error = %err, "upstream proxy request failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    map_upstream_response(upstream_response)
}

fn track_request_body_activity(body: Body) -> (Body, watch::Receiver<()>) {
    let (activity_tx, activity_rx) = watch::channel(());
    let body = body.map_frame(move |frame| {
        // Each data frame means upload progress; trailers do not reset the
        // upload idle timer.
        if frame.data_ref().is_some() {
            let _ = activity_tx.send(());
        }
        frame
    });

    (Body::new(body), activity_rx)
}

async fn wait_for_upstream_response_headers_with_activity_timeout<F>(
    request: F,
    mut activity_rx: watch::Receiver<()>,
) -> Result<Result<Response<Incoming>, hyper_util::client::legacy::Error>, ()>
where
    F: Future<Output = Result<Response<Incoming>, hyper_util::client::legacy::Error>>,
{
    let mut request = std::pin::pin!(request);
    let mut timer = std::pin::pin!(tokio::time::sleep(PROXY_REQUEST_BODY_IDLE_TIMEOUT));
    // The sender lives in the wrapped request body. While it is open, the
    // timer measures upload idle time. Once it closes, the upload phase is
    // over, so the timer switches to waiting for upstream response headers.
    let mut activity_open = true;

    loop {
        tokio::select! {
            result = &mut request => return Ok(result),
            _ = &mut timer => return Err(()),
            changed = activity_rx.changed(), if activity_open => {
                if changed.is_ok() {
                    timer.as_mut().reset(tokio::time::Instant::now() + PROXY_REQUEST_BODY_IDLE_TIMEOUT);
                } else {
                    activity_open = false;
                    timer.as_mut().reset(tokio::time::Instant::now() + PROXY_RESPONSE_HEADER_TIMEOUT);
                }
            }
        }
    }
}

fn is_envd_stream_input_request(method: &Method, path: &str) -> bool {
    *method == Method::POST && path == ENVD_STREAM_INPUT_PATH
}

fn is_benign_stream_input_disconnect(
    method: &Method,
    path: &str,
    err: &hyper_util::client::legacy::Error,
) -> bool {
    if !is_envd_stream_input_request(method, path) {
        return false;
    }

    is_hyper_stream_closed(err) || is_send_request_failure_text(err)
}

fn is_hyper_stream_closed(err: &hyper_util::client::legacy::Error) -> bool {
    err.source()
        .and_then(|source| source.downcast_ref::<hyper::Error>())
        .is_some_and(|source| source.is_canceled() || source.is_closed())
}

fn is_send_request_failure_text(err: &impl std::fmt::Display) -> bool {
    // hyper-util keeps the ErrorKind private. StreamInput is a long-lived
    // client-streaming attach request, so SendRequest here means the upstream
    // request channel was torn down while the client detached or envd paused.
    err.to_string() == "client error (SendRequest)"
}

/// Proxies a WebSocket upgrade request by performing the handshake with the upstream and then
/// bridging the client and upstream WebSocket streams.
async fn proxy_websocket_request(
    websocket_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    mut parts: http::request::Parts,
    resolved: ResolvedProxyRequest,
) -> Response<Body> {
    let websocket_upgrade = match websocket_upgrade {
        Ok(websocket_upgrade) => websocket_upgrade,
        Err(err) => {
            warn!(error = ?err, "received malformed websocket upgrade request");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    let ResolvedProxyRequest {
        sandbox_id,
        upstream_uri,
        original_host,
    } = resolved;

    sanitize_websocket_request_headers(&mut parts.headers);
    inject_forwarded_headers(
        &mut parts.headers,
        original_host.as_ref(),
        &parts.method,
        &upstream_uri,
        "ws",
    );

    let upstream_request = match build_websocket_upstream_request(&upstream_uri, &parts.headers) {
        Ok(request) => request,
        Err(status) => return status.into_response(),
    };

    debug!(
        sandbox_id = %sandbox_id,
        method = %parts.method,
        upstream = %upstream_uri,
        "proxying client websocket request"
    );

    let (upstream_websocket, upstream_response) = match timeout(
        PROXY_RESPONSE_HEADER_TIMEOUT,
        connect_async(upstream_request),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => match err {
            tungstenite::Error::Http(response) => {
                info!(sandbox_id = %sandbox_id, status = %response.status(), "upstream websocket handshake rejected");
                return map_websocket_handshake_rejection_response(*response);
            }
            err => {
                warn!(sandbox_id = %sandbox_id, error = %err, "upstream websocket handshake failed");
                return StatusCode::BAD_GATEWAY.into_response();
            }
        },
        Err(_) => {
            warn!(
                sandbox_id = %sandbox_id,
                timeout_ms = PROXY_RESPONSE_HEADER_TIMEOUT.as_millis(),
                "timed out waiting for upstream websocket handshake"
            );
            return StatusCode::GATEWAY_TIMEOUT.into_response();
        }
    };

    let sandbox_id_for_upgrade = sandbox_id.to_string();
    let sandbox_id_for_bridge = sandbox_id.to_string();
    let mut websocket_upgrade = websocket_upgrade.on_failed_upgrade(move |error| {
        warn!(
            sandbox_id = %sandbox_id_for_upgrade,
            error = %error,
            "client websocket upgrade failed"
        );
    });

    if let Some(protocol) = upstream_response
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
    {
        websocket_upgrade = websocket_upgrade.protocols([protocol.to_owned()]);
    }

    let mut upstream_headers = upstream_response.headers().clone();
    // The sec-websocket-* headers (Accept, Protocol, Extensions, etc.) are
    // part of the WebSocket handshake between the proxy and the upstream.
    // Axum generates its own set for the client-facing 101 response, so
    // forwarding the upstream's copies would duplicate or conflict with them.
    remove_websocket_handshake_headers(&mut upstream_headers);
    remove_hop_by_hop_headers(&mut upstream_headers);

    let mut response = websocket_upgrade.on_upgrade(move |socket| async move {
        bridge_websocket_streams(socket, upstream_websocket, sandbox_id_for_bridge).await;
    });

    // Axum's on_upgrade() produces a minimal 101 response with only the
    // mandatory WebSocket headers. Extend it with the remaining upstream
    // headers so application-level headers (e.g. X-Request-Id, Set-Cookie)
    // are visible to the client, matching the HTTP proxy path's behavior.
    response.headers_mut().extend(upstream_headers);

    response
}

fn map_websocket_handshake_rejection_response(
    response: http::Response<Option<Vec<u8>>>,
) -> Response<Body> {
    let (mut parts, body) = response.into_parts();
    remove_hop_by_hop_headers(&mut parts.headers);
    let body = body.map_or_else(Body::empty, Body::from);
    Response::from_parts(parts, body)
}

/// Resolves the proxy request by:
/// - Determining the target sandbox from the request headers.
/// - Looking up the sandbox's proxy target from the orchestrator.
/// - Constructing the upstream URI based on the target and the incoming request path and query.
async fn resolve_proxy_request(
    api_impl: &ApiImpl,
    proxy_path: &str,
    parts: &http::request::Parts,
    is_websocket_request: bool,
) -> Result<ResolvedProxyRequest, ProxyRequestError> {
    let sandbox_id = parse_sandbox_id_header(&parts.headers)?;
    let target_port = parse_target_port_header(&parts.headers)?;

    let mut auto_resume_attempted = false;
    let target = loop {
        match api_impl.orchestrator().proxy_lookup_for(&sandbox_id).await {
            Ok(ProxyLookupResult::Ready(target)) => break target,
            Ok(ProxyLookupResult::NotFound) => {
                return Err(ProxyRequestError::SandboxNotFound(sandbox_id))
            }
            Ok(ProxyLookupResult::Paused { auto_resume: true }) => {
                if auto_resume_attempted {
                    return Err(ProxyRequestError::AutoResumeFailed(sandbox_id));
                }
                try_auto_resume(api_impl, sandbox_id).await?;
                auto_resume_attempted = true;
                continue;
            }
            Ok(ProxyLookupResult::Paused { auto_resume: false }) => {
                return Err(ProxyRequestError::SandboxUnavailable(
                    sandbox_id,
                    SandboxState::Paused,
                ))
            }
            Ok(ProxyLookupResult::Unavailable(_)) | Ok(ProxyLookupResult::RouteMissing)
                if auto_resume_attempted =>
            {
                return Err(ProxyRequestError::AutoResumeFailed(sandbox_id))
            }
            Ok(ProxyLookupResult::Unavailable(state)) => {
                return Err(ProxyRequestError::SandboxUnavailable(sandbox_id, state))
            }
            Ok(ProxyLookupResult::RouteMissing) => {
                return Err(ProxyRequestError::MissingRuntimeRoute(sandbox_id))
            }
            Err(OrchestratorError::SandboxNotFound(_)) => {
                return Err(ProxyRequestError::SandboxNotFound(sandbox_id))
            }
            Err(err) if auto_resume_attempted => {
                warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to resolve proxy target after auto-resume"
                );
                return Err(ProxyRequestError::AutoResumeFailed(sandbox_id));
            }
            Err(err) => {
                warn!(sandbox_id = %sandbox_id, error = %err, "failed to resolve proxy target");
                return Err(ProxyRequestError::InternalServerError);
            }
        }
    };

    let upstream_uri = if is_websocket_request {
        build_upstream_uri_with_scheme("ws", &target, target_port, proxy_path, parts.uri.query())
    } else {
        build_upstream_uri_with_scheme("http", &target, target_port, proxy_path, parts.uri.query())
    }
    .map_err(|_| ProxyRequestError::InvalidUpstreamUri)?;

    Ok(ResolvedProxyRequest {
        sandbox_id,
        upstream_uri,
        original_host: parts.headers.get(header::HOST).cloned(),
    })
}

async fn try_auto_resume(
    api_impl: &ApiImpl,
    sandbox_id: SandboxId,
) -> Result<(), ProxyRequestError> {
    match timeout(
        PROXY_AUTO_RESUME_TIMEOUT,
        api_impl.orchestrator().resume_sandbox(
            sandbox_id,
            NewTimeout::EnsureMinimum(auto_resume_min_sandbox_timeout()),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            debug!(sandbox_id = %sandbox_id, "sandbox auto-resume completed");
            Ok(())
        }
        Ok(Err(err)) => {
            warn!(sandbox_id = %sandbox_id, error = %err, "sandbox auto-resume failed");
            Err(ProxyRequestError::AutoResumeFailed(sandbox_id))
        }
        Err(_) => {
            warn!(
                sandbox_id = %sandbox_id,
                timeout_ms = PROXY_AUTO_RESUME_TIMEOUT.as_millis(),
                "sandbox auto-resume timed out"
            );
            Err(ProxyRequestError::AutoResumeTimedOut(sandbox_id))
        }
    }
}

fn parse_sandbox_id_header(headers: &HeaderMap) -> Result<SandboxId, ProxyRequestError> {
    let raw_id = first_header_value(headers, &[SANDBOX_ID_HEADER, E2B_SANDBOX_ID_HEADER])
        .ok_or(ProxyRequestError::MissingSandboxId)?;

    SandboxId::parse_str(raw_id).map_err(|_| ProxyRequestError::InvalidSandboxId)
}

fn parse_target_port_header(headers: &HeaderMap) -> Result<u16, ProxyRequestError> {
    let raw_port = first_header_value(headers, &[TARGET_PORT_HEADER, E2B_TARGET_PORT_HEADER])
        .ok_or(ProxyRequestError::MissingTargetPort)?;

    raw_port
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or(ProxyRequestError::InvalidTargetPort)
}

pub(crate) fn sandbox_not_found_response(sandbox_id: SandboxId) -> Response<Body> {
    proxy_error_response(&ProxyRequestError::SandboxNotFound(sandbox_id))
}

fn proxy_error_response(error: &ProxyRequestError) -> Response<Body> {
    let (status, message) = match error {
        ProxyRequestError::MissingSandboxId => {
            (StatusCode::BAD_REQUEST, "missing sandbox routing header")
        }
        ProxyRequestError::InvalidSandboxId => {
            (StatusCode::BAD_REQUEST, "invalid sandbox routing header")
        }
        ProxyRequestError::MissingTargetPort => (
            StatusCode::BAD_REQUEST,
            "missing target port routing header",
        ),
        ProxyRequestError::InvalidTargetPort => (
            StatusCode::BAD_REQUEST,
            "invalid target port routing header",
        ),
        ProxyRequestError::InvalidHostRoute(message) => (StatusCode::BAD_REQUEST, *message),
        ProxyRequestError::SandboxNotFound(_) => (StatusCode::NOT_FOUND, "sandbox not found"),
        ProxyRequestError::SandboxUnavailable(_, _) => (
            StatusCode::GONE,
            "sandbox is not proxyable in its current state",
        ),
        ProxyRequestError::AutoResumeFailed(_) => {
            (StatusCode::BAD_GATEWAY, "sandbox auto-resume failed")
        }
        ProxyRequestError::AutoResumeTimedOut(_) => {
            (StatusCode::GATEWAY_TIMEOUT, "sandbox auto-resume timed out")
        }
        ProxyRequestError::MissingRuntimeRoute(_) => (
            StatusCode::BAD_GATEWAY,
            "sandbox route is temporarily unavailable",
        ),
        ProxyRequestError::InvalidUpstreamUri => {
            (StatusCode::BAD_REQUEST, "failed to construct upstream URI")
        }
        ProxyRequestError::InternalServerError => (StatusCode::INTERNAL_SERVER_ERROR, ""),
    };

    if matches!(error, ProxyRequestError::InternalServerError) {
        return status.into_response();
    }

    match error {
        ProxyRequestError::MissingSandboxId
        | ProxyRequestError::InvalidSandboxId
        | ProxyRequestError::MissingTargetPort
        | ProxyRequestError::InvalidTargetPort
        | ProxyRequestError::InvalidHostRoute(_)
        | ProxyRequestError::InvalidUpstreamUri => {
            debug!(status = %status, message, "rejecting bad proxy request")
        }
        ProxyRequestError::SandboxNotFound(sandbox_id) => {
            debug!(sandbox_id = %sandbox_id, status = %status, message, "proxy request rejected")
        }
        ProxyRequestError::SandboxUnavailable(sandbox_id, state) => {
            debug!(sandbox_id = %sandbox_id, state = ?state, status = %status, message, "proxy request rejected")
        }
        ProxyRequestError::AutoResumeFailed(sandbox_id) => {
            warn!(sandbox_id = %sandbox_id, status = %status, message, "proxy auto-resume failed")
        }
        ProxyRequestError::AutoResumeTimedOut(sandbox_id) => {
            warn!(sandbox_id = %sandbox_id, status = %status, message, "proxy auto-resume timed out")
        }
        ProxyRequestError::MissingRuntimeRoute(sandbox_id) => {
            warn!(sandbox_id = %sandbox_id, status = %status, message, "sandbox route missing for running sandbox")
        }
        ProxyRequestError::InternalServerError => unreachable!("handled above"),
    }

    Response::builder()
        .status(status)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(Body::from(message))
        .unwrap_or_else(|_| status.into_response())
}

fn first_header_value<'a>(headers: &'a HeaderMap, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
    })
}

fn build_upstream_uri_with_scheme(
    scheme: &str,
    target: &ProxyTarget,
    target_port: u16,
    proxy_path: &str,
    query: Option<&str>,
) -> Result<Uri, StatusCode> {
    let normalized_path = if proxy_path.is_empty() {
        "/".to_string()
    } else if proxy_path.starts_with('/') {
        proxy_path.to_string()
    } else {
        format!("/{proxy_path}")
    };

    if proxy_path.is_empty() {
        let path_and_query = match query {
            Some(query) => format!("{normalized_path}?{query}"),
            None => normalized_path,
        };

        return Uri::builder()
            .scheme(scheme)
            .authority(format!("{}:{}", target.ip, target_port).as_str())
            .path_and_query(path_and_query)
            .build()
            .map_err(|_| StatusCode::BAD_REQUEST);
    }

    let path_and_query = match query {
        Some(query) => format!("{normalized_path}?{query}"),
        None => normalized_path,
    };

    Uri::builder()
        .scheme(scheme)
        .authority(format!("{}:{}", target.ip, target_port).as_str())
        .path_and_query(path_and_query)
        .build()
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn sanitize_request_headers(headers: &mut HeaderMap) {
    // These headers are only for the control-plane hop between the client and
    // AgentENV. Upstream sandbox services should not see them.
    headers.remove(SANDBOX_ID_HEADER);
    headers.remove(E2B_SANDBOX_ID_HEADER);
    headers.remove(TARGET_PORT_HEADER);
    headers.remove(E2B_TARGET_PORT_HEADER);
    headers.remove(TRAFFIC_ACCESS_TOKEN_HEADER);
    headers.remove(header::HOST);
    remove_hop_by_hop_headers(headers);
}

fn sanitize_websocket_request_headers(headers: &mut HeaderMap) {
    sanitize_request_headers(headers);
    headers.remove(header::SEC_WEBSOCKET_ACCEPT);
    headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
    headers.remove(header::SEC_WEBSOCKET_KEY);
    headers.remove(header::SEC_WEBSOCKET_VERSION);
}

fn inject_forwarded_headers(
    headers: &mut HeaderMap,
    original_host: Option<&HeaderValue>,
    method: &Method,
    upstream_uri: &Uri,
    forwarded_proto: &str,
) {
    if let Some(host) = original_host {
        headers.insert(HeaderName::from_static("x-forwarded-host"), host.clone());
    }

    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_str(forwarded_proto).unwrap_or_else(|_| HeaderValue::from_static("http")),
    );
    headers.insert(
        HeaderName::from_static("x-forwarded-method"),
        HeaderValue::from_str(method.as_str()).unwrap_or_else(|_| HeaderValue::from_static("GET")),
    );
    if let Some(path_and_query) = upstream_uri.path_and_query() {
        if let Ok(value) = HeaderValue::from_str(path_and_query.as_str()) {
            headers.insert(HeaderName::from_static("x-forwarded-uri"), value);
        }
    }
}

fn build_websocket_upstream_request(
    upstream_uri: &Uri,
    headers: &HeaderMap,
) -> Result<http::Request<()>, StatusCode> {
    let mut request = upstream_uri
        .to_string()
        .into_client_request()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    for (name, value) in headers {
        request.headers_mut().insert(name, value.clone());
    }

    Ok(request)
}

fn is_websocket_upgrade_request(headers: &HeaderMap) -> bool {
    header_contains_token(headers, header::CONNECTION, "upgrade")
        && header_contains_token(headers, header::UPGRADE, "websocket")
}

fn header_contains_token(headers: &HeaderMap, header_name: HeaderName, token: &str) -> bool {
    headers
        .get_all(header_name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

/// Removes `sec-websocket-*` headers that are part of the WebSocket handshake
/// protocol and should not be forwarded across proxy hops.
fn remove_websocket_handshake_headers(headers: &mut HeaderMap) {
    let keys: Vec<_> = headers
        .keys()
        .filter(|name| name.as_str().starts_with("sec-websocket"))
        .cloned()
        .collect();
    for key in keys {
        headers.remove(&key);
    }
}

fn remove_hop_by_hop_headers(headers: &mut HeaderMap) {
    let connection_nominated_headers = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| HeaderName::from_bytes(value.as_bytes()).ok())
        .collect::<Vec<_>>();

    for header_name in [
        header::CONNECTION,
        header::UPGRADE,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
    ] {
        headers.remove(header_name);
    }

    for header_name in connection_nominated_headers {
        headers.remove(header_name);
    }

    headers.remove(HeaderName::from_static("keep-alive"));
}

fn map_upstream_response(response: Response<Incoming>) -> Response<Body> {
    let (mut parts, body) = response.into_parts();
    // Mirror the request-side filtering on the way back so connection-scoped
    // headers from the upstream do not leak through this proxy hop.
    remove_hop_by_hop_headers(&mut parts.headers);
    Response::from_parts(parts, Body::new(body.map_err(axum::Error::new)))
}

async fn bridge_websocket_streams(
    client_socket: WebSocket,
    upstream_socket: UpstreamWebSocket,
    sandbox_id: String,
) {
    let (client_sender, client_receiver) = client_socket.split();
    let (upstream_sender, upstream_receiver) = upstream_socket.split();

    let client_to_upstream = forward_client_messages(client_receiver, upstream_sender, &sandbox_id);
    let upstream_to_client =
        forward_upstream_messages(upstream_receiver, client_sender, &sandbox_id);

    tokio::join!(client_to_upstream, upstream_to_client);
}

async fn forward_client_messages(
    mut client_receiver: futures::stream::SplitStream<WebSocket>,
    mut upstream_sender: futures::stream::SplitSink<UpstreamWebSocket, TungsteniteMessage>,
    sandbox_id: &str,
) {
    while let Some(message) = client_receiver.next().await {
        let message = match message {
            Ok(message) => message,
            Err(err) => {
                warn!(sandbox_id = sandbox_id, error = %err, "failed to read websocket frame from client");
                break;
            }
        };

        if upstream_sender
            .send(axum_message_to_tungstenite(message))
            .await
            .is_err()
        {
            warn!(
                sandbox_id = sandbox_id,
                "failed to forward websocket frame to upstream"
            );
            break;
        }
    }

    let _ = upstream_sender.close().await;
}

async fn forward_upstream_messages(
    mut upstream_receiver: futures::stream::SplitStream<UpstreamWebSocket>,
    mut client_sender: futures::stream::SplitSink<WebSocket, WebSocketMessage>,
    sandbox_id: &str,
) {
    while let Some(message) = upstream_receiver.next().await {
        let message = match message {
            Ok(message) => message,
            Err(err) => {
                warn!(sandbox_id = sandbox_id, error = %err, "failed to read websocket frame from upstream");
                break;
            }
        };

        let Some(message) = tungstenite_message_to_axum(message) else {
            continue;
        };

        if client_sender.send(message).await.is_err() {
            warn!(
                sandbox_id = sandbox_id,
                "failed to forward websocket frame to client"
            );
            break;
        }
    }

    let _ = client_sender.close().await;
}

fn axum_message_to_tungstenite(message: WebSocketMessage) -> TungsteniteMessage {
    match message {
        WebSocketMessage::Text(text) => TungsteniteMessage::Text(text.to_string().into()),
        WebSocketMessage::Binary(binary) => TungsteniteMessage::Binary(binary),
        WebSocketMessage::Ping(ping) => TungsteniteMessage::Ping(ping),
        WebSocketMessage::Pong(pong) => TungsteniteMessage::Pong(pong),
        WebSocketMessage::Close(Some(close)) => {
            TungsteniteMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: CloseCode::from(close.code),
                reason: close.reason.to_string().into(),
            }))
        }
        WebSocketMessage::Close(None) => TungsteniteMessage::Close(None),
    }
}

fn tungstenite_message_to_axum(message: TungsteniteMessage) -> Option<WebSocketMessage> {
    match message {
        TungsteniteMessage::Text(text) => Some(WebSocketMessage::Text(text.to_string().into())),
        TungsteniteMessage::Binary(binary) => Some(WebSocketMessage::Binary(binary)),
        TungsteniteMessage::Ping(ping) => Some(WebSocketMessage::Ping(ping)),
        TungsteniteMessage::Pong(pong) => Some(WebSocketMessage::Pong(pong)),
        TungsteniteMessage::Close(Some(close)) => Some(WebSocketMessage::Close(Some(CloseFrame {
            code: close.code.into(),
            reason: close.reason.to_string().into(),
        }))),
        TungsteniteMessage::Close(None) => Some(WebSocketMessage::Close(None)),
        TungsteniteMessage::Frame(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::Infallible,
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    use axum::body::Bytes;
    use axum::http::header::HOST;
    use axum::{
        extract::ws::{Message as AxumWebSocketMessage, WebSocketUpgrade},
        routing::{get, post},
        Json,
    };
    use futures::{stream, SinkExt, StreamExt};
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;

    use crate::{
        api::server,
        api_key::ApiKey,
        cfg::AppConfig,
        image::ImageResolver,
        orchestrator::{FileBackedSandboxPersister, Orchestrator},
        snapshot::mock::mock_snapshot_manager,
        template::TemplateBuilder,
    };

    const TEST_API_KEY: &str =
        "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn strip_host_port_handles_dns_and_ipv6_hosts() {
        for (host, expected) in [
            ("sandbox.example.invalid", "sandbox.example.invalid"),
            ("sandbox.example.invalid:443", "sandbox.example.invalid"),
            ("[::1]:8080", "[::1]:8080"),
            ("[::1]", "[::1]"),
            ("::1", "::1"),
        ] {
            assert_eq!(strip_host_port(host), expected);
        }
    }

    #[test]
    fn parse_host_proxy_route_matches_configured_domain() {
        let sandbox_id = SandboxId::new();
        let domains = vec!["sandbox.example.invalid".to_string()];
        let host = format!("8080-{sandbox_id}.sandbox.example.invalid");

        let route = parse_host_proxy_route(Some(&host), &domains)
            .unwrap()
            .expect("host route should match");
        assert_eq!(
            route,
            HostProxyRoute {
                sandbox_id,
                target_port: 8080,
            }
        );

        let bare_domain = "sandbox.example.invalid";
        assert_eq!(
            parse_host_proxy_route(Some(bare_domain), &domains).unwrap(),
            None
        );

        let bad_port = format!("0-{sandbox_id}.sandbox.example.invalid");
        assert!(parse_host_proxy_route(Some(&bad_port), &domains).is_err());

        let bad_sandbox = "8080-not-a-sandbox.sandbox.example.invalid";
        assert!(parse_host_proxy_route(Some(bad_sandbox), &domains).is_err());
    }

    async fn read_http_request_head(stream: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0, "connection closed before request headers");
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return;
            }
        }
    }

    async fn respond_empty(stream: &mut tokio::net::TcpStream) {
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
    }

    // `previous_runtime` models the idle flow to the stopped VM. Bytes arriving on
    // it mean the client reused that stale connection; a new `accept()` models a
    // connection to the next VM that inherited the same address. The return value
    // is true only when the previous runtime's connection was reused.

    async fn simulate_runtime_generation_change(listener: tokio::net::TcpListener) -> bool {
        let (mut previous_runtime, _) = listener.accept().await.unwrap();
        read_http_request_head(&mut previous_runtime).await;
        respond_empty(&mut previous_runtime).await;

        let mut stale_request = [0_u8; 1024];
        tokio::select! {
            read = previous_runtime.read(&mut stale_request) => {
                match read.unwrap() {
                    0 => {
                        let (mut next_runtime, _) = timeout(
                            Duration::from_secs(1),
                            listener.accept(),
                        )
                        .await
                        .expect("client did not connect to the next runtime generation")
                        .unwrap();
                        read_http_request_head(&mut next_runtime).await;
                        respond_empty(&mut next_runtime).await;
                        false
                    }
                    _ => {
                        // The same address now belongs to another sandbox runtime. A packet on
                        // the previous generation's flow receives the RST observed in production.
                        previous_runtime.set_zero_linger().unwrap();
                        true
                    }
                }
            }
            accepted = listener.accept() => {
                let (mut next_runtime, _) = accepted.unwrap();
                read_http_request_head(&mut next_runtime).await;
                respond_empty(&mut next_runtime).await;
                false
            }
        }
    }

    fn empty_proxy_request(address: SocketAddr) -> Request<Body> {
        Request::builder()
            .uri(format!("http://{address}/health"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn proxy_client_does_not_reuse_connections_across_runtime_generations() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = tokio::spawn(simulate_runtime_generation_change(listener));
        let client = build_proxy_client();

        let first_response = client.request(empty_proxy_request(address)).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        first_response.into_body().collect().await.unwrap();

        let (second_response, reused_previous_runtime) = tokio::join!(
            timeout(
                Duration::from_secs(5),
                client.request(empty_proxy_request(address)),
            ),
            timeout(Duration::from_secs(5), runtime),
        );
        let second_response = second_response.expect("second proxy request timed out");
        let reused_previous_runtime = reused_previous_runtime
            .expect("runtime generation simulation timed out")
            .unwrap();

        assert!(
            !reused_previous_runtime,
            "proxy client reused an idle TCP connection after the runtime generation changed"
        );
        let second_response = second_response.expect("request to the next runtime should succeed");
        assert_eq!(second_response.status(), StatusCode::OK);
        second_response.into_body().collect().await.unwrap();
    }

    async fn spawn_upstream(router: axum::Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        addr
    }

    async fn start_upstream_server() -> SocketAddr {
        async fn upstream_handler(headers: HeaderMap, uri: Uri) -> Json<Value> {
            Json(json!({
                "path": uri.path(),
                "query": uri.query(),
                "sandbox_header_seen": headers.get(SANDBOX_ID_HEADER).is_some(),
                "e2b_sandbox_header_seen": headers.get(E2B_SANDBOX_ID_HEADER).is_some(),
                "target_port_header_seen": headers.get(TARGET_PORT_HEADER).is_some(),
                "e2b_target_port_header_seen": headers.get(E2B_TARGET_PORT_HEADER).is_some(),
                "api_key_header_seen": headers.get(API_KEY_HEADER).is_some(),
                "traffic_token_header_seen": headers.get(TRAFFIC_ACCESS_TOKEN_HEADER).is_some(),
                "access_token": headers
                    .get(ENVD_ACCESS_TOKEN_HEADER)
                    .and_then(|value| value.to_str().ok()),
                "authorization": headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
                "forwarded_host": headers
                    .get("x-forwarded-host")
                    .and_then(|value| value.to_str().ok()),
            }))
        }

        spawn_upstream(
            axum::Router::new()
                .route("/", get(upstream_handler))
                .route("/{*path}", get(upstream_handler)),
        )
        .await
    }

    async fn start_connection_header_server() -> SocketAddr {
        async fn connection_handler(headers: HeaderMap) -> Response<Body> {
            let request_foo_seen = headers
                .get("foo")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONNECTION, "bar")
                .header("bar", "response-hop-by-hop")
                .header("x-request-foo-seen", request_foo_seen.unwrap_or_default())
                .body(Body::empty())
                .unwrap()
        }

        spawn_upstream(axum::Router::new().route("/{*path}", get(connection_handler))).await
    }

    async fn start_http_rejection_server() -> SocketAddr {
        async fn rejection_handler() -> Response<Body> {
            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .header("x-upstream-error", "denied")
                .body(Body::from("upstream denied http"))
                .unwrap()
        }

        spawn_upstream(axum::Router::new().route("/{*path}", get(rejection_handler))).await
    }

    async fn start_websocket_upstream_server() -> SocketAddr {
        async fn websocket_handler(
            ws: WebSocketUpgrade,
            headers: HeaderMap,
            uri: Uri,
        ) -> Response<Body> {
            let forwarded_host = headers
                .get("x-forwarded-host")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let requested_protocol = headers
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);

            let mut ws = ws;
            if requested_protocol
                .as_deref()
                .is_some_and(|value| value.split(',').any(|item| item.trim() == "chat"))
            {
                ws = ws.protocols(["chat"]);
            }

            let initial_message = json!({
                "path": uri.path(),
                "query": uri.query(),
                "sandbox_header_seen": headers.get(SANDBOX_ID_HEADER).is_some(),
                "e2b_sandbox_header_seen": headers.get(E2B_SANDBOX_ID_HEADER).is_some(),
                "target_port_header_seen": headers.get(TARGET_PORT_HEADER).is_some(),
                "e2b_target_port_header_seen": headers.get(E2B_TARGET_PORT_HEADER).is_some(),
                "forwarded_host": forwarded_host,
                "requested_protocol": requested_protocol,
            })
            .to_string();

            let mut response = ws.on_upgrade(move |mut socket| async move {
                socket
                    .send(AxumWebSocketMessage::Text(initial_message.into()))
                    .await
                    .unwrap();

                while let Some(message) = socket.next().await {
                    let message = match message {
                        Ok(message) => message,
                        Err(_) => break,
                    };

                    match message {
                        AxumWebSocketMessage::Text(text) => {
                            if socket.send(AxumWebSocketMessage::Text(text)).await.is_err() {
                                break;
                            }
                        }
                        AxumWebSocketMessage::Binary(binary) => {
                            if socket
                                .send(AxumWebSocketMessage::Binary(binary))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        AxumWebSocketMessage::Close(frame) => {
                            let _ = socket.send(AxumWebSocketMessage::Close(frame)).await;
                            break;
                        }
                        AxumWebSocketMessage::Ping(_) | AxumWebSocketMessage::Pong(_) => {}
                    }
                }
            });
            response.headers_mut().insert(
                HeaderName::from_static("x-upstream-ws-custom"),
                HeaderValue::from_static("ws-header-value"),
            );
            response.headers_mut().insert(
                header::SEC_WEBSOCKET_EXTENSIONS,
                HeaderValue::from_static("permessage-deflate"),
            );
            response
        }

        spawn_upstream(axum::Router::new().route("/ws/{*path}", get(websocket_handler))).await
    }

    async fn start_rejecting_websocket_upstream_server() -> SocketAddr {
        async fn rejecting_handler() -> Response<Body> {
            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from("upstream denied websocket"))
                .unwrap()
        }

        spawn_upstream(axum::Router::new().route("/ws/{*path}", get(rejecting_handler))).await
    }

    async fn start_streaming_sse_server() -> SocketAddr {
        async fn sse_handler() -> Response<Body> {
            let stream = stream::unfold(0, |state| async move {
                match state {
                    0 => Some((
                        Ok::<Bytes, Infallible>(Bytes::from_static(b"data: first\n\n")),
                        1,
                    )),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        Some((
                            Ok::<Bytes, Infallible>(Bytes::from_static(b"data: second\n\n")),
                            2,
                        ))
                    }
                    _ => None,
                }
            });

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap()
        }

        spawn_upstream(axum::Router::new().route("/events", get(sse_handler))).await
    }

    async fn start_large_upload_server() -> SocketAddr {
        async fn upload_handler(request: Request) -> Json<Value> {
            let body = request.into_body().collect().await.unwrap().to_bytes();
            Json(json!({ "size": body.len() }))
        }

        spawn_upstream(axum::Router::new().route("/upload", post(upload_handler))).await
    }

    async fn start_slow_headers_server() -> SocketAddr {
        async fn slow_handler() -> Response<Body> {
            tokio::time::sleep(Duration::from_millis(150)).await;
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("slow"))
                .unwrap()
        }

        spawn_upstream(axum::Router::new().route("/slow", get(slow_handler))).await
    }

    async fn start_client_stream_response_server() -> SocketAddr {
        async fn client_stream_handler(request: Request) -> Response<Body> {
            let body = request.into_body().collect().await.unwrap().to_bytes();
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(body.len().to_string()))
                .unwrap()
        }

        spawn_upstream(
            axum::Router::new().route(ENVD_STREAM_INPUT_PATH, post(client_stream_handler)),
        )
        .await
    }

    async fn build_api() -> Arc<ApiImpl> {
        build_api_with_sandbox_proxy_domains(Vec::new()).await
    }

    async fn build_api_with_sandbox_proxy_domains(domains: Vec<String>) -> Arc<ApiImpl> {
        build_api_with_auth(domains, TEST_API_KEY).await
    }

    async fn build_api_with_auth(domains: Vec<String>, api_key: &str) -> Arc<ApiImpl> {
        let root = tempfile::tempdir().unwrap();
        let orchestrator = Orchestrator::new(
            crate::orchestrator::InMemoryMetadataStore::new(),
            crate::sandbox::FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().to_path_buf()),
        )
        .await
        .unwrap();
        let snapshot_manager = Arc::new(mock_snapshot_manager());
        let template_builder = Arc::new(TemplateBuilder::new());
        let image_resolver = Arc::new(ImageResolver::new(&AppConfig::default()));
        Arc::new(ApiImpl::new(
            orchestrator,
            snapshot_manager,
            template_builder,
            image_resolver,
            None,
            domains,
            ApiKey::new(api_key).unwrap(),
        ))
    }

    async fn proxy_app_for_sandbox_with_state_and_auto_resume(
        sandbox_id: &SandboxId,
        state: crate::orchestrator::SandboxState,
        auto_resume: bool,
    ) -> axum::Router {
        let api = build_api().await;
        api.orchestrator()
            .set_proxy_target_for_test(*sandbox_id, ProxyTarget::new(Ipv4Addr::LOCALHOST), state)
            .await;
        api.orchestrator()
            .set_auto_resume_for_test(sandbox_id, auto_resume)
            .await
            .unwrap();
        server::new(api)
    }

    async fn proxy_app_for_sandbox(sandbox_id: &SandboxId) -> axum::Router {
        proxy_app_for_sandbox_with_state_and_auto_resume(
            sandbox_id,
            crate::orchestrator::SandboxState::Running,
            false,
        )
        .await
    }

    async fn proxy_app_with_access_token_for_sandbox(
        sandbox_id: &SandboxId,
    ) -> (axum::Router, String) {
        let api = build_api().await;
        let access_token = api.traffic_access_token(*sandbox_id);
        api.orchestrator()
            .set_proxy_target_for_test(
                *sandbox_id,
                ProxyTarget::new(Ipv4Addr::LOCALHOST),
                crate::orchestrator::SandboxState::Running,
            )
            .await;
        api.orchestrator()
            .set_allow_public_traffic_for_test(sandbox_id, false)
            .await
            .unwrap();
        (server::new(api), access_token)
    }

    async fn proxy_app_for_sandbox_with_domains(
        sandbox_id: &SandboxId,
        domains: Vec<String>,
    ) -> (axum::Router, String) {
        let api = build_api_with_sandbox_proxy_domains(domains).await;
        let access_token = api.traffic_access_token(*sandbox_id);
        api.orchestrator()
            .set_proxy_target_for_test(
                *sandbox_id,
                ProxyTarget::new(Ipv4Addr::LOCALHOST),
                crate::orchestrator::SandboxState::Running,
            )
            .await;
        api.orchestrator()
            .set_allow_public_traffic_for_test(sandbox_id, false)
            .await
            .unwrap();
        (server::new(api), access_token)
    }

    async fn proxy_app_for_running_sandbox_without_route(sandbox_id: &SandboxId) -> axum::Router {
        let api = build_api().await;
        api.orchestrator()
            .set_metadata_state_for_test(*sandbox_id, crate::orchestrator::SandboxState::Running)
            .await
            .unwrap();
        api.orchestrator()
            .remove_proxy_route_for_test(sandbox_id)
            .await;
        server::new(api)
    }

    async fn start_proxy_server(sandbox_id: &SandboxId) -> SocketAddr {
        spawn_upstream(proxy_app_for_sandbox(sandbox_id).await).await
    }

    async fn get_status(app: &axum::Router, uri: &str, headers: &[(&str, &str)]) -> StatusCode {
        let mut request = Request::builder()
            .uri(uri)
            .header(header::HOST, "localhost");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        app.clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[test]
    fn parses_agentenv_headers() {
        let sandbox_id = SandboxId::new().to_string();
        let mut headers = HeaderMap::new();
        headers.insert(
            SANDBOX_ID_HEADER,
            HeaderValue::from_str(&sandbox_id).unwrap(),
        );
        headers.insert(TARGET_PORT_HEADER, HeaderValue::from_static("8080"));

        assert_eq!(
            parse_sandbox_id_header(&headers).unwrap().to_string(),
            sandbox_id
        );
        assert_eq!(parse_target_port_header(&headers).unwrap(), 8080);
    }

    #[test]
    fn parses_e2b_headers() {
        let sandbox_id = SandboxId::new().to_string();
        let mut headers = HeaderMap::new();
        headers.insert(
            E2B_SANDBOX_ID_HEADER,
            HeaderValue::from_str(&sandbox_id).unwrap(),
        );
        headers.insert(E2B_TARGET_PORT_HEADER, HeaderValue::from_static("8080"));

        assert_eq!(
            parse_sandbox_id_header(&headers).unwrap().to_string(),
            sandbox_id
        );
        assert_eq!(parse_target_port_header(&headers).unwrap(), 8080);
    }

    #[test]
    fn sanitize_request_headers_removes_internal_routing_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(SANDBOX_ID_HEADER, HeaderValue::from_static("sandbox-id"));
        headers.insert(
            E2B_SANDBOX_ID_HEADER,
            HeaderValue::from_static("sandbox-id"),
        );
        headers.insert(TARGET_PORT_HEADER, HeaderValue::from_static("8080"));
        headers.insert(E2B_TARGET_PORT_HEADER, HeaderValue::from_static("8080"));
        headers.insert(API_KEY_HEADER, HeaderValue::from_static("application-key"));
        headers.insert(
            TRAFFIC_ACCESS_TOKEN_HEADER,
            HeaderValue::from_static("traffic-token"),
        );
        headers.insert(HOST, HeaderValue::from_static("client.example"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            HeaderName::from_static("x-extra"),
            HeaderValue::from_static("keep"),
        );

        sanitize_request_headers(&mut headers);

        assert!(headers.get(SANDBOX_ID_HEADER).is_none());
        assert!(headers.get(E2B_SANDBOX_ID_HEADER).is_none());
        assert!(headers.get(TARGET_PORT_HEADER).is_none());
        assert!(headers.get(E2B_TARGET_PORT_HEADER).is_none());
        assert_eq!(headers.get(API_KEY_HEADER).unwrap(), "application-key");
        assert!(headers.get(TRAFFIC_ACCESS_TOKEN_HEADER).is_none());
        assert!(headers.get(HOST).is_none());
        assert!(headers.get(header::CONNECTION).is_none());
        assert_eq!(headers.get("x-extra").unwrap(), "keep");
    }

    #[test]
    fn build_upstream_uri_preserves_path_and_query() {
        let target = ProxyTarget {
            ip: std::net::Ipv4Addr::LOCALHOST,
        };

        let uri =
            build_upstream_uri_with_scheme("http", &target, 8080, "echo/test", Some("foo=bar"))
                .unwrap();

        assert_eq!(uri.to_string(), "http://127.0.0.1:8080/echo/test?foo=bar");
    }

    #[test]
    fn classifies_only_envd_stream_input_requests() {
        assert!(is_envd_stream_input_request(
            &Method::POST,
            ENVD_STREAM_INPUT_PATH
        ));
        assert!(!is_envd_stream_input_request(
            &Method::GET,
            ENVD_STREAM_INPUT_PATH
        ));
        assert!(!is_envd_stream_input_request(
            &Method::POST,
            "/process.Process/Connect"
        ));
    }

    #[test]
    fn classifies_send_request_error_text_as_stream_input_detach_only() {
        struct SendRequestError;

        impl std::fmt::Display for SendRequestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("client error (SendRequest)")
            }
        }

        assert!(is_send_request_failure_text(&SendRequestError));
        assert!(!is_send_request_failure_text(&"client error (Connect)"));
    }

    #[tokio::test]
    async fn control_plane_auth_is_separate_from_sandbox_auth() {
        let app = server::new(build_api().await);

        for headers in [
            vec![],
            vec![(header::AUTHORIZATION.as_str(), "Bearer test-key")],
            vec![(API_KEY_HEADER, "wrong-key")],
            vec![
                (API_KEY_HEADER, TEST_API_KEY),
                (API_KEY_HEADER, TEST_API_KEY),
            ],
        ] {
            assert_eq!(
                get_status(&app, "/nonexistent/path", &headers).await,
                StatusCode::UNAUTHORIZED
            );
        }

        for (path, headers, expected) in [
            (
                "/nonexistent/path",
                vec![(API_KEY_HEADER, TEST_API_KEY)],
                StatusCode::NOT_FOUND,
            ),
            ("/health", vec![], StatusCode::NO_CONTENT),
        ] {
            assert_eq!(get_status(&app, path, &headers).await, expected);
        }
        assert_ne!(
            get_status(&app, "/metrics", &[]).await,
            StatusCode::UNAUTHORIZED
        );

        let sandbox_id = SandboxId::new().to_string();
        let route = [
            (SANDBOX_ID_HEADER, sandbox_id.as_str()),
            (TARGET_PORT_HEADER, "8080"),
        ];
        assert_eq!(
            get_status(&app, "/sandboxes", &route).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get_status(&app, "/proxy/health", &route).await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn application_proxy_auth_respects_public_and_private_ingress() {
        let upstream_addr = start_upstream_server().await;
        let api = build_api().await;
        let sandbox_id = SandboxId::new();
        api.orchestrator()
            .set_proxy_target_for_test(
                sandbox_id,
                ProxyTarget::new(Ipv4Addr::LOCALHOST),
                crate::orchestrator::SandboxState::Running,
            )
            .await;

        let app = server::new(Arc::clone(&api));
        let sandbox_id_text = sandbox_id.to_string();
        let port = upstream_addr.port().to_string();
        let route = [
            (SANDBOX_ID_HEADER, sandbox_id_text.as_str()),
            (TARGET_PORT_HEADER, port.as_str()),
        ];
        assert_eq!(
            get_status(&app, "/proxy/public", &route).await,
            StatusCode::OK
        );

        api.orchestrator()
            .set_allow_public_traffic_for_test(&sandbox_id, false)
            .await
            .unwrap();
        let traffic_token = api.traffic_access_token(sandbox_id);

        for credential in [
            None,
            Some((API_KEY_HEADER, TEST_API_KEY)),
            Some((TRAFFIC_ACCESS_TOKEN_HEADER, "incorrect")),
            Some((ENVD_ACCESS_TOKEN_HEADER, "envd-token")),
        ] {
            let mut headers = route.to_vec();
            if let Some((header_name, value)) = credential {
                headers.push((header_name, value));
            }
            assert_eq!(
                get_status(&app, "/proxy/private", &headers).await,
                StatusCode::UNAUTHORIZED
            );
        }

        let mut headers = route.to_vec();
        headers.push((TRAFFIC_ACCESS_TOKEN_HEADER, traffic_token.as_str()));
        headers.push((API_KEY_HEADER, "application-api-key"));
        assert_eq!(
            get_status(&app, "/proxy/private", &headers).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn envd_proxy_auth_depends_only_on_secure_mode_and_envd_token() {
        let api = build_api().await;
        let sandbox_id = SandboxId::new();
        api.orchestrator()
            .set_proxy_target_for_test(
                sandbox_id,
                ProxyTarget::new(Ipv4Addr::LOCALHOST),
                crate::orchestrator::SandboxState::Running,
            )
            .await;
        let target_port = ConfigManager::global_config()
            .tools
            .control_plane_port
            .to_string();
        let app = server::new(Arc::clone(&api));
        let sandbox_id_text = sandbox_id.to_string();
        let route = [
            (SANDBOX_ID_HEADER, sandbox_id_text.as_str()),
            (TARGET_PORT_HEADER, target_port.as_str()),
        ];
        let envd_paths = ["/proxy/health", "/proxy/metrics"];
        for path in envd_paths {
            assert_ne!(
                get_status(&app, path, &route).await,
                StatusCode::UNAUTHORIZED
            );
        }

        api.orchestrator()
            .set_secure_for_test(&sandbox_id, true)
            .await
            .unwrap();
        let metadata = api
            .orchestrator()
            .get_sandbox(&sandbox_id)
            .await
            .unwrap()
            .unwrap();
        let envd_token = api.orchestrator().get_envd_access_token(&metadata).unwrap();
        let traffic_token = api.traffic_access_token(sandbox_id);

        for credential in [
            None,
            Some((API_KEY_HEADER, TEST_API_KEY)),
            Some((TRAFFIC_ACCESS_TOKEN_HEADER, traffic_token.as_str())),
            Some((ENVD_ACCESS_TOKEN_HEADER, "incorrect")),
        ] {
            for path in envd_paths {
                let mut headers = route.to_vec();
                if let Some((header_name, value)) = credential {
                    headers.push((header_name, value));
                }
                assert_eq!(
                    get_status(&app, path, &headers).await,
                    StatusCode::UNAUTHORIZED
                );
            }
        }

        let mut headers = route.to_vec();
        headers.push((ENVD_ACCESS_TOKEN_HEADER, envd_token.expose()));
        for path in envd_paths {
            assert_ne!(
                get_status(&app, path, &headers).await,
                StatusCode::UNAUTHORIZED
            );
        }
    }

    #[tokio::test]
    async fn proxy_requires_routing_headers() {
        let app = server::new(build_api().await);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/proxy/hello")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"missing sandbox routing header"));
    }

    #[tokio::test]
    async fn proxy_returns_gone_for_non_running_sandbox() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox_with_state_and_auto_resume(
            &sandbox_id,
            crate::orchestrator::SandboxState::Paused,
            false,
        )
        .await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header("x-api-key", "application-key")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::GONE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body,
            Bytes::from_static(b"sandbox is not proxyable in its current state")
        );
    }

    #[tokio::test]
    async fn proxy_attempts_auto_resume_for_paused_sandbox_when_enabled() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox_with_state_and_auto_resume(
            &sandbox_id,
            crate::orchestrator::SandboxState::Paused,
            true,
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"sandbox auto-resume failed"));
    }

    #[tokio::test]
    async fn paused_secure_envd_auto_resume_requires_valid_access_token() {
        let sandbox_id = SandboxId::new();
        let api = build_api().await;
        api.orchestrator()
            .set_proxy_target_for_test(
                sandbox_id,
                ProxyTarget::new(Ipv4Addr::LOCALHOST),
                crate::orchestrator::SandboxState::Paused,
            )
            .await;
        api.orchestrator()
            .set_auto_resume_for_test(&sandbox_id, true)
            .await
            .unwrap();
        api.orchestrator()
            .set_secure_for_test(&sandbox_id, true)
            .await
            .unwrap();
        let metadata = api
            .orchestrator()
            .get_sandbox(&sandbox_id)
            .await
            .unwrap()
            .expect("paused sandbox metadata");
        let valid_token = api
            .orchestrator()
            .get_envd_access_token(&metadata)
            .expect("secure paused sandbox token");
        let app = server::new(api);

        for token in [None, Some("incorrect")] {
            let mut request = Request::builder()
                .method(Method::GET)
                .uri("/proxy/health")
                .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                .header(
                    TARGET_PORT_HEADER,
                    ConfigManager::global_config()
                        .tools
                        .control_plane_port
                        .to_string(),
                );
            if let Some(token) = token {
                request = request.header(ENVD_ACCESS_TOKEN_HEADER, token);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(
                        TARGET_PORT_HEADER,
                        ConfigManager::global_config()
                            .tools
                            .control_plane_port
                            .to_string(),
                    )
                    .header(ENVD_ACCESS_TOKEN_HEADER, valid_token.expose())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn proxy_returns_bad_gateway_for_running_sandbox_without_runtime_route() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_running_sandbox_without_route(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body,
            Bytes::from_static(b"sandbox route is temporarily unavailable")
        );
    }

    #[tokio::test]
    async fn proxy_rejects_invalid_target_port_with_stable_error_body() {
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, "0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body,
            Bytes::from_static(b"invalid target port routing header")
        );
    }

    #[tokio::test]
    async fn proxy_root_forwards_to_upstream_root_path() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy?foo=bar")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/");
        assert_eq!(payload["query"], "foo=bar");
    }

    #[tokio::test]
    async fn proxy_forwards_application_headers_and_strips_internal_headers() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/echo/test?foo=bar".to_string())
                    .header("host", "client.example")
                    .header(API_KEY_HEADER, "application-key")
                    .header(header::AUTHORIZATION, "Bearer application-token")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .header(ENVD_ACCESS_TOKEN_HEADER, "envd-token")
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, "traffic-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/echo/test");
        assert_eq!(payload["query"], "foo=bar");
        assert_eq!(payload["sandbox_header_seen"], false);
        assert_eq!(payload["e2b_sandbox_header_seen"], false);
        assert_eq!(payload["target_port_header_seen"], false);
        assert_eq!(payload["e2b_target_port_header_seen"], false);
        assert!(payload["access_token"].is_null());
        assert_eq!(payload["traffic_token_header_seen"], false);
        assert_eq!(payload["forwarded_host"], "client.example");
        assert_eq!(payload["api_key_header_seen"], true);
        assert_eq!(payload["authorization"], "Bearer application-token");
    }

    #[tokio::test]
    async fn proxy_preserves_raw_percent_encoded_wildcard_path() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/a%2Fb/%2525")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/a%2Fb/%2525");
    }

    #[tokio::test]
    async fn proxy_preserves_repeated_leading_slashes_in_wildcard_path() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy//api")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "//api");
    }

    #[tokio::test]
    async fn proxy_strips_connection_nominated_headers_on_requests_and_responses() {
        let upstream_addr = start_connection_header_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/check")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .header(header::CONNECTION, "foo")
                    .header("foo", "request-hop-by-hop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-request-foo-seen")
                .unwrap()
                .to_str()
                .unwrap(),
            ""
        );
        assert!(response.headers().get(header::CONNECTION).is_none());
        assert!(response.headers().get("bar").is_none());
    }

    #[tokio::test]
    async fn proxy_preserves_upstream_http_error_status_headers_and_body() {
        let upstream_addr = start_http_rejection_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/reject")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response.headers().get("x-upstream-error").unwrap(),
            "denied"
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"upstream denied http"));
    }

    #[tokio::test]
    async fn proxy_fallback_dispatches_when_routing_header_is_present() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/files?path=/")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/api/files");
        assert_eq!(payload["query"], "path=/");
        assert_eq!(payload["sandbox_header_seen"], false);
        assert_eq!(payload["target_port_header_seen"], false);
    }

    #[tokio::test]
    async fn proxy_fallback_dispatches_with_e2b_header_alias() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/envd/health")
                    .header(E2B_SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(E2B_TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/envd/health");
    }

    #[tokio::test]
    async fn proxy_fallback_returns_not_found_without_routing_header() {
        let app = server::new(build_api().await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent/path")
                    .header(API_KEY_HEADER, TEST_API_KEY)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(
            content_type.starts_with("application/json"),
            "unexpected content-type: {content_type}"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["code"], 404);
        let message = payload["message"].as_str().unwrap();
        assert!(
            message.contains("route not found: GET /nonexistent/path"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn sandbox_proxy_host_routes_control_paths_and_skips_explicit_proxy() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let (app, access_token) = proxy_app_for_sandbox_with_domains(
            &sandbox_id,
            vec!["sandbox.example.invalid".to_string()],
        )
        .await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health")
                    .header(
                        "host",
                        format!(
                            "{}-{}.sandbox.example.invalid",
                            upstream_addr.port(),
                            sandbox_id
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health?foo=bar")
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, &access_token)
                    .header(
                        "host",
                        format!(
                            "{}-{}.sandbox.example.invalid",
                            upstream_addr.port(),
                            sandbox_id
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/health");
        assert_eq!(payload["query"], "foo=bar");
        assert_eq!(payload["sandbox_header_seen"], false);
        assert_eq!(payload["target_port_header_seen"], false);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "http://{}-{}.sandbox.example.invalid/authority",
                        upstream_addr.port(),
                        sandbox_id
                    ))
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, &access_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/authority");

        let (app, access_token) = proxy_app_for_sandbox_with_domains(
            &sandbox_id,
            vec!["sandbox.example.invalid".to_string()],
        )
        .await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, &access_token)
                    .header(
                        "host",
                        format!(
                            "{}-{}.sandbox.example.invalid",
                            upstream_addr.port(),
                            sandbox_id
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/health")
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, access_token)
                    .header(
                        "host",
                        format!(
                            "{}-{}.sandbox.example.invalid",
                            upstream_addr.port(),
                            sandbox_id
                        ),
                    )
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/health");
    }

    #[tokio::test]
    async fn proxy_accepts_e2b_compatible_headers() {
        let upstream_addr = start_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let (app, access_token) = proxy_app_with_access_token_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/e2b/health")
                    .header(TRAFFIC_ACCESS_TOKEN_HEADER, access_token)
                    .header(E2B_SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(E2B_TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["path"], "/e2b/health");
        assert_eq!(payload["sandbox_header_seen"], false);
        assert_eq!(payload["e2b_sandbox_header_seen"], false);
        assert_eq!(payload["target_port_header_seen"], false);
        assert_eq!(payload["e2b_target_port_header_seen"], false);
        assert!(
            payload["access_token"].is_null(),
            "envd credential leaked to application port"
        );
    }

    #[tokio::test]
    async fn proxy_streams_sse_response_without_buffering_entire_body() {
        let upstream_addr = start_streaming_sse_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/events")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );

        let mut stream = response.into_body().into_data_stream();
        let first_chunk = tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .expect("first chunk should not wait for the full body")
            .unwrap()
            .unwrap();
        assert_eq!(first_chunk, Bytes::from_static(b"data: first\n\n"));

        let second_chunk = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("second chunk should still be streamed through")
            .unwrap()
            .unwrap();
        assert_eq!(second_chunk, Bytes::from_static(b"data: second\n\n"));
    }

    #[tokio::test]
    async fn proxy_forwards_large_request_bodies() {
        let upstream_addr = start_large_upload_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;
        let chunk = vec![b'x'; 1024 * 1024];
        let expected_size = chunk.len() * 3;
        let body_stream = stream::iter(vec![
            Ok::<Bytes, Infallible>(Bytes::from(chunk.clone())),
            Ok::<Bytes, Infallible>(Bytes::from(chunk.clone())),
            Ok::<Bytes, Infallible>(Bytes::from(chunk)),
        ]);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/proxy/upload")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::from_stream(body_stream))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["size"], expected_size);
    }

    #[tokio::test]
    async fn proxy_allows_slow_uploads_while_body_makes_progress() {
        let upstream_addr = start_large_upload_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;
        let body_stream = stream::unfold(0, |state| async move {
            match state {
                0 => Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"first")), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"second")), 2))
                }
                2 => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"third")), 3))
                }
                _ => None,
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/proxy/upload")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::from_stream(body_stream))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["size"], 16);
    }

    #[tokio::test]
    async fn proxy_returns_gateway_timeout_when_upstream_headers_are_too_slow() {
        let upstream_addr = start_slow_headers_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/proxy/slow")
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn proxy_keeps_envd_stream_input_alive_with_body_activity() {
        let upstream_addr = start_client_stream_response_server().await;
        let sandbox_id = SandboxId::new();
        let app = proxy_app_for_sandbox(&sandbox_id).await;
        let body_stream = stream::unfold(0, |state| async move {
            match state {
                0 => Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"first")), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"second")), 2))
                }
                2 => {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Some((Ok::<Bytes, Infallible>(Bytes::from_static(b"third")), 3))
                }
                _ => None,
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(ENVD_STREAM_INPUT_PATH)
                    .header(SANDBOX_ID_HEADER, sandbox_id.to_string())
                    .header(TARGET_PORT_HEADER, upstream_addr.port().to_string())
                    .body(Body::from_stream(body_stream))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"16"));
    }

    #[tokio::test]
    async fn proxy_bridges_websocket_upgrade_and_bidirectional_messages() {
        let upstream_addr = start_websocket_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let proxy_addr = start_proxy_server(&sandbox_id).await;

        let mut request = format!("ws://{proxy_addr}/proxy/ws/echo?foo=bar")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static(TEST_API_KEY));
        request.headers_mut().insert(
            SANDBOX_ID_HEADER,
            HeaderValue::from_str(&sandbox_id.to_string()).unwrap(),
        );
        request.headers_mut().insert(
            TARGET_PORT_HEADER,
            HeaderValue::from_str(&upstream_addr.port().to_string()).unwrap(),
        );
        request
            .headers_mut()
            .insert(HOST, HeaderValue::from_static("client.example"));
        request.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("chat"),
        );

        let (mut websocket, response) = connect_async(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .unwrap(),
            "chat"
        );
        assert_eq!(
            response.headers().get("x-upstream-ws-custom").unwrap(),
            "ws-header-value",
            "upstream WebSocket handshake response headers should be forwarded to the client"
        );
        assert!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_EXTENSIONS)
                .is_none(),
            "upstream sec-websocket-* headers should not leak through the proxy"
        );

        let initial_message = websocket.next().await.unwrap().unwrap();
        let initial_payload: Value =
            serde_json::from_str(initial_message.to_text().unwrap()).unwrap();
        assert_eq!(initial_payload["path"], "/ws/echo");
        assert_eq!(initial_payload["query"], "foo=bar");
        assert_eq!(initial_payload["sandbox_header_seen"], false);
        assert_eq!(initial_payload["e2b_sandbox_header_seen"], false);
        assert_eq!(initial_payload["target_port_header_seen"], false);
        assert_eq!(initial_payload["e2b_target_port_header_seen"], false);
        assert_eq!(initial_payload["forwarded_host"], "client.example");
        assert_eq!(initial_payload["requested_protocol"], "chat");

        websocket
            .send(TungsteniteMessage::Text("hello over ws".into()))
            .await
            .unwrap();
        let echoed_message = websocket.next().await.unwrap().unwrap();
        assert_eq!(echoed_message.to_text().unwrap(), "hello over ws");

        websocket
            .send(TungsteniteMessage::Close(None))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn proxy_preserves_upstream_websocket_handshake_rejection_status() {
        let upstream_addr = start_rejecting_websocket_upstream_server().await;
        let sandbox_id = SandboxId::new();
        let proxy_addr = start_proxy_server(&sandbox_id).await;

        let mut request = format!("ws://{proxy_addr}/proxy/ws/reject")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static(TEST_API_KEY));
        request.headers_mut().insert(
            SANDBOX_ID_HEADER,
            HeaderValue::from_str(&sandbox_id.to_string()).unwrap(),
        );
        request.headers_mut().insert(
            TARGET_PORT_HEADER,
            HeaderValue::from_str(&upstream_addr.port().to_string()).unwrap(),
        );

        let err = connect_async(request).await.unwrap_err();
        let response = match err {
            tokio_tungstenite::tungstenite::Error::Http(response) => response,
            other => panic!("expected http rejection, got {other:?}"),
        };

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.body().as_deref(),
            Some(b"upstream denied websocket".as_slice())
        );
    }
}
