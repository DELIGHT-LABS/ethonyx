use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::sync::OwnedSemaphorePermit;
use tracing::{Instrument, debug, info, trace, warn};

use crate::BoxError;
use crate::headers::{sanitize_response_headers, strip_hop_by_hop};
use crate::health;
use crate::metrics::{UpstreamLayer, classify_method};
use crate::state::{AppState, HttpTarget};
use crate::websocket::WsMode;

pub(crate) type ResBody = UnsyncBoxBody<Bytes, BoxError>;

pub(crate) type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub(crate) type ProxyClient = Client<Https, ResBody>;

const MAX_BUFFERED_REQUEST_BYTES: usize = 64 * 1024 * 1024;

fn build_http_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_keepalive(Some(Duration::from_secs(60)));
    http
}

fn configure_client_builder() -> hyper_util::client::legacy::Builder {
    let mut builder = Client::builder(TokioExecutor::new());
    builder
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(Duration::from_secs(60))
        .timer(TokioTimer::new())
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(false);
    builder
}

/// Build the shared WebPKI TLS configuration once so `https://` and `wss://`
/// upstreams share root certificates while keeping `wss://` ALPN empty (strictly
/// HTTP/1.1 `Upgrade`) and using [`tokio_tungstenite::Connector::Plain`] on `ws://`.
pub(crate) fn build_connectors(ws_is_tls: bool) -> (Https, tokio_tungstenite::Connector) {
    use hyper_rustls::ConfigBuilderExt;

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let base_tls = rustls::ClientConfig::builder()
        .with_webpki_roots()
        .with_no_client_auth();
    let ws_tls = if ws_is_tls {
        tokio_tungstenite::Connector::Rustls(Arc::new(base_tls.clone()))
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(base_tls)
        .https_or_http()
        // ALPN advertises ["h2","http/1.1"] for https upstreams -> auto-negotiate
        // h2 when the upstream offers it; cleartext upstreams stay h1.
        .enable_all_versions()
        .wrap_connector(build_http_connector());
    (https, ws_tls)
}

pub(crate) fn build_client(https: Https) -> ProxyClient {
    configure_client_builder().build(https)
}

/// Cleartext-only HTTP/2 prior-knowledge (`h2c`) client for probed `http://` EL upstreams,
/// sharing the same `Https` connector type so both clients unify as [`ProxyClient`].
pub(crate) fn build_h2c_client(https: Https) -> ProxyClient {
    let mut builder = configure_client_builder();
    builder.http2_only(true);
    builder.build(https)
}

/// Permit bounding concurrent WebSocket relays across both HTTP/1.1 `Upgrade`
/// and HTTP/2 Extended `CONNECT` streams under `--max-connections`.
pub(crate) enum WsPermit {
    Upgraded { _permit: Arc<OwnedSemaphorePermit> },
    IncludedH2(Arc<Connection>),
    DedicatedH2 { _permit: OwnedSemaphorePermit },
}

impl Drop for WsPermit {
    fn drop(&mut self) {
        if let Self::IncludedH2(conn) = self {
            conn.h2_ws_slot_in_use.store(false, Ordering::Release);
        }
    }
}

/// Connection-owned state. Requests clone one Arc; detached WebSocket relays
/// and HTTP/2 request tasks also retain the shared lifecycle and connection permit.
pub(crate) struct Connection {
    pub(crate) state: Arc<AppState>,
    pub(crate) lifecycle: crate::lifecycle::Lifecycle,
    peer: SocketAddr,
    permit: Arc<OwnedSemaphorePermit>,
    h2_ws_slot_in_use: AtomicBool,
}

impl Connection {
    pub(crate) const fn new(
        state: Arc<AppState>,
        lifecycle: crate::lifecycle::Lifecycle,
        peer: SocketAddr,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            state,
            lifecycle,
            peer,
            permit,
            h2_ws_slot_in_use: AtomicBool::new(false),
        }
    }

    /// Claim a WebSocket concurrency slot:
    /// - HTTP/1.1 `Upgrade` shares the TCP connection's existing permit.
    /// - HTTP/2 Extended `CONNECT` reuses the H2 connection's own permit for its
    ///   first active WS stream, and acquires dedicated permits from `--max-connections`
    ///   for additional concurrent WS streams so multiplex amplification is bounded.
    pub(crate) fn acquire_ws_permit(self: &Arc<Self>, mode: WsMode) -> Option<WsPermit> {
        match mode {
            WsMode::Upgrade => Some(WsPermit::Upgraded {
                _permit: Arc::clone(&self.permit),
            }),
            WsMode::ExtendedConnect => {
                if self
                    .h2_ws_slot_in_use
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    Some(WsPermit::IncludedH2(Arc::clone(self)))
                } else if let Ok(_permit) = self.state.conn_limit.clone().try_acquire_owned() {
                    Some(WsPermit::DedicatedH2 { _permit })
                } else {
                    self.state.metrics.inc_saturated();
                    None
                }
            }
        }
    }

    pub(crate) async fn dispatch(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<ResBody>, Infallible> {
        // The same decision drives routing, metrics and access logs. GET/HEAD
        // probes and invalid targets return locally without upstream metrics/spans.
        let target = match classify_request(&req) {
            Route::Probe(probe) => return Ok(probe.response(req.method(), &self.state)),
            Route::Reject => {
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    Bytes::from_static(b"invalid request target"),
                ));
            }
            Route::Proxy(target) => target,
        };
        let (layer, upstream_proto) = target.layer_and_proto();
        let upstream_type = layer.as_str();
        let (method_idx, method_label) = classify_method(req.method());
        let version = req.version();
        let access_ctx = tracing::enabled!(target: "access_log", tracing::Level::INFO)
            .then(|| (req.method().clone(), req.uri().clone()));
        let otel_enabled = self.state.otel_enabled;
        let span = if otel_enabled || tracing::enabled!(tracing::Level::DEBUG) {
            let span = tracing::info_span!(
                "request",
                method = %req.method(),
                path = req.uri().path(),
                upstream = upstream_type,
            );
            #[cfg(feature = "otel")]
            if otel_enabled {
                crate::otel::attach_parent_context(&span, req.headers());
            }
            span
        } else {
            tracing::Span::none()
        };
        let start = std::time::Instant::now();
        let res = route(req, &self, target).instrument(span).await;
        let response = res.unwrap_or_else(|e| {
            warn!(
                upstream = upstream_type,
                proto = upstream_proto,
                error = %e,
                "upstream proxy error"
            );
            text_response(StatusCode::BAD_GATEWAY, Bytes::from_static(b"bad gateway"))
        });
        let elapsed = start.elapsed();
        let status = response.status().as_u16();
        if let Some((method, uri)) = access_ctx {
            info!(
                target: "access_log",
                peer = %self.peer,
                ?version,
                %method,
                path = uri.path(),
                upstream = upstream_type,
                proto = upstream_proto,
                status,
                latency_ms = elapsed.as_secs_f64() * 1000.0,
                "request"
            );
        }
        self.state
            .metrics
            .record_request(layer, method_idx, method_label, status, elapsed);
        Ok(response)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Probe(Probe),
    Reject,
    Proxy(Target),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Cl,
    El,
    WebSocket(WsMode),
}

impl Target {
    const fn layer_and_proto(self) -> (UpstreamLayer, &'static str) {
        match self {
            Self::Cl => (UpstreamLayer::Cl, "HTTP"),
            Self::El => (UpstreamLayer::El, "HTTP"),
            Self::WebSocket(_) => (UpstreamLayer::El, "WS"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    Health,
    Ready,
    Live,
    Metrics,
}

impl Probe {
    fn response(self, method: &Method, state: &AppState) -> Response<ResBody> {
        let head = method == Method::HEAD;
        match self {
            Self::Health => health::report(state, head),
            Self::Ready => health::ready(state, head),
            Self::Live => text_response(
                StatusCode::OK,
                if head {
                    Bytes::new()
                } else {
                    Bytes::from_static(b"ok")
                },
            ),
            Self::Metrics => {
                let body = if head {
                    Bytes::new()
                } else {
                    Bytes::from(
                        crate::PROMETHEUS_HANDLE
                            .get()
                            .map(|h| h.render())
                            .unwrap_or_default(),
                    )
                };
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(box_full(Full::new(body)))
                    .unwrap()
            }
        }
    }
}

fn classify_request<B>(req: &Request<B>) -> Route {
    let path = req.uri().path();
    if !path.starts_with('/') {
        return Route::Reject;
    }
    if req.method() == Method::GET || req.method() == Method::HEAD {
        let probe = match path {
            "/healthz" => Some(Probe::Health),
            "/readyz" => Some(Probe::Ready),
            "/livez" => Some(Probe::Live),
            "/metrics" => Some(Probe::Metrics),
            _ => None,
        };
        if let Some(probe) = probe {
            return Route::Probe(probe);
        }
    }
    // CL routes take precedence over upgrade headers, as in the forwarding path.
    let is_cl = is_cl_path(path);
    if !is_cl && let Some(mode) = WsMode::classify(req) {
        Route::Proxy(Target::WebSocket(mode))
    } else if req.method() == Method::CONNECT {
        Route::Reject
    } else if is_cl {
        Route::Proxy(Target::Cl)
    } else {
        Route::Proxy(Target::El)
    }
}

pub(crate) fn is_cl_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'/') {
        return false;
    }
    let remainder = &bytes[1..];
    let len = remainder
        .iter()
        .position(|&b| b == b'/')
        .unwrap_or(remainder.len());
    let segment = &remainder[..len];
    match segment.len() {
        3 => segment == b"eth",
        4 => segment == b"teku",
        5 => segment == b"prysm",
        6 => segment == b"nimbus",
        8 => segment == b"lodestar",
        10 => segment == b"lighthouse",
        _ => false,
    }
}

async fn route(
    req: Request<Incoming>,
    connection: &Arc<Connection>,
    target: Target,
) -> Result<Response<ResBody>, BoxError> {
    let state = &connection.state;
    trace!(method = %req.method(), path = req.uri().path(), "routing request");
    match target {
        Target::Cl => proxy_cl(req, state).await,
        Target::WebSocket(mode) => crate::websocket::upgrade(req, mode, connection).await,
        Target::El => proxy_el(req, state).await,
    }
}

async fn proxy_cl(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
    if (req.method() == Method::GET || req.method() == Method::HEAD)
        && req.uri().path() == "/eth/v1/node/health"
        && state.cfg.mock_cl_node_health
    {
        return Ok(health::node_health(state));
    }
    if req.method() == Method::POST
        && state.cfg.mock_cl_subscriptions
        && matches!(
            req.uri().path(),
            "/eth/v1/validator/prepare_beacon_proposer"
                | "/eth/v1/validator/beacon_committee_subscriptions"
                | "/eth/v1/validator/sync_committee_subscriptions"
        )
    {
        if let Err(err) = drain_limited(req.into_body(), state.cfg.proxy_timeout).await {
            return Ok(err.into_response());
        }
        return Ok(json_response(
            StatusCode::OK,
            Bytes::from_static(b"{\"code\":200,\"message\":\"success\"}"),
        ));
    }
    let (mut parts, body) = req.into_parts();
    let target = if parts.method == Method::POST
        && parts.uri.path() == "/eth/v1/validator/register_validator"
        && let Some(ref mev) = state.cl_mev_relay
    {
        let Ok(replaced) =
            crate::upstream::replace_uri_path(parts.uri, "/eth/v1/builder/validators")
        else {
            return Ok(uri_too_long_response());
        };
        parts.uri = replaced;
        mev
    } else {
        &state.cl
    };
    execute_upstream(parts, box_incoming(body), state, target).await
}

async fn proxy_el(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
    let (parts, body) = if req.method() == Method::POST && state.cfg.mock_eth_syncing {
        let (parts, body) = req.into_parts();
        let bytes = match collect_limited(body, state.cfg.proxy_timeout).await {
            Ok(bytes) => bytes,
            Err(err) => return Ok(err.into_response()),
        };
        if let Some(mock_res) = try_mock_eth_syncing(&bytes) {
            return Ok(mock_res);
        }
        (parts, box_full(Full::new(bytes)))
    } else {
        let (parts, body) = req.into_parts();
        (parts, box_incoming(body))
    };
    // Select at dispatch, after any mock body inspection: local replies need
    // no transport lookup, and forwarded requests follow the latest verdict.
    execute_upstream(parts, body, state, &state.el).await
}

#[derive(Clone, Copy)]
struct BodyReadError {
    status: StatusCode,
    message: &'static [u8],
}

impl BodyReadError {
    fn into_response(self) -> Response<ResBody> {
        text_response(self.status, Bytes::from_static(self.message))
    }
}

fn map_limited_body_error(err: BoxError) -> BodyReadError {
    if err.is::<http_body_util::LengthLimitError>() {
        BodyReadError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: b"request body exceeds limit",
        }
    } else {
        debug!(error = %err, "failed to read request body");
        BodyReadError {
            status: StatusCode::BAD_REQUEST,
            message: b"failed to read request body",
        }
    }
}

async fn run_limited_body<T, F>(timeout: Duration, fut: F) -> Result<T, BodyReadError>
where
    F: Future<Output = Result<T, BoxError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(err)) => Err(map_limited_body_error(err)),
        Err(_) => Err(BodyReadError {
            status: StatusCode::REQUEST_TIMEOUT,
            message: b"request body timed out",
        }),
    }
}

async fn drain_limited(body: Incoming, timeout: Duration) -> Result<(), BodyReadError> {
    run_limited_body(timeout, async move {
        let mut limited = http_body_util::Limited::new(body, MAX_BUFFERED_REQUEST_BYTES);
        while let Some(frame) = limited.frame().await {
            let _ = frame?;
        }
        Ok(())
    })
    .await
}

async fn collect_limited(body: Incoming, timeout: Duration) -> Result<Bytes, BodyReadError> {
    run_limited_body(timeout, async move {
        let collected = http_body_util::Limited::new(body, MAX_BUFFERED_REQUEST_BYTES)
            .collect()
            .await?;
        Ok(collected.to_bytes())
    })
    .await
}

fn try_mock_eth_syncing(bytes: &[u8]) -> Option<Response<ResBody>> {
    match crate::rpc::mock_syncing(bytes) {
        crate::rpc::MockResult::Forward => None,
        crate::rpc::MockResult::Silent => Some(text_response(StatusCode::NO_CONTENT, Bytes::new())),
        crate::rpc::MockResult::Reply(body) => {
            Some(json_response(StatusCode::OK, Bytes::from(body)))
        }
    }
}

async fn execute_upstream(
    mut parts: http::request::Parts,
    body: ResBody,
    state: &AppState,
    target: &HttpTarget,
) -> Result<Response<ResBody>, BoxError> {
    let Ok(rewritten) = target.upstream.rewrite_uri(parts.uri) else {
        return Ok(uri_too_long_response());
    };
    parts.uri = rewritten;
    // Strip hop-by-hop and Connection-nominated headers BEFORE injecting OTel
    // trace context so `Connection: traceparent` cannot strip outgoing spans.
    strip_hop_by_hop(&mut parts.headers);
    #[cfg(feature = "otel")]
    if state.otel_enabled {
        crate::otel::propagate_context(&mut parts.headers);
    }
    parts.extensions.clear();
    // The inbound version (e.g. HTTP/2) must not be forced onto the upstream
    // connection — normalize so the client uses whatever the upstream negotiates
    // (h1 for cleartext, h2 via ALPN for an https upstream).
    parts.version = Version::HTTP_11;
    let upstream_req = Request::from_parts(parts, body);
    let client = target.transport.select_client(&state.client);

    let resp = tokio::time::timeout(state.cfg.proxy_timeout, client.request(upstream_req))
        .await
        .map_err(|_| -> BoxError { "upstream timeout".into() })??;

    target.transport.record_version(resp.version());

    let (mut resp_parts, resp_body) = resp.into_parts();
    sanitize_response_headers(resp_parts.status, &mut resp_parts.headers);
    let idle_timeout = response_idle_timeout(
        &resp_parts.headers,
        state.cfg.proxy_timeout,
        state.cfg.sse_idle_timeout,
    );
    let body = IdleTimeoutBody::new(resp_body, idle_timeout).boxed_unsync();
    Ok(Response::from_parts(resp_parts, body))
}

fn is_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|media| media.trim_ascii().eq_ignore_ascii_case("text/event-stream"))
}

fn response_idle_timeout(
    headers: &HeaderMap,
    proxy_timeout: Duration,
    sse_idle_timeout: Duration,
) -> Option<Duration> {
    if is_event_stream(headers) {
        (!sse_idle_timeout.is_zero()).then_some(sse_idle_timeout)
    } else {
        Some(proxy_timeout)
    }
}

/// Streaming response body wrapper that enforces a per-frame idle timeout while
/// preserving `Body::size_hint` and `Body::is_end_stream` (keeping `Content-Length`
/// intact on HTTP/1.1 responses) and allocating `Sleep` lazily only if `Pending`.
struct IdleTimeoutBody<B> {
    inner: B,
    timeout: Option<Duration>,
    deadline: crate::lifecycle::LazyDeadline,
    done: bool,
}

impl<B> IdleTimeoutBody<B> {
    const fn new(inner: B, timeout: Option<Duration>) -> Self {
        Self {
            inner,
            timeout,
            deadline: crate::lifecycle::LazyDeadline::new(),
            done: false,
        }
    }
}

impl<B> Body for IdleTimeoutBody<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                this.deadline.disarm();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.deadline.disarm();
                this.done = true;
                Poll::Ready(Some(Err(err.into())))
            }
            Poll::Ready(None) => {
                this.deadline.disarm();
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                let Some(duration) = this.timeout else {
                    return Poll::Pending;
                };
                if this.deadline.poll_expired(cx, duration) {
                    this.done = true;
                    Poll::Ready(Some(Err("response body idle timeout".into())))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn box_incoming<B>(body: B) -> ResBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

pub(crate) fn box_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

fn json_response(code: StatusCode, body: Bytes) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(body)))
        .unwrap()
}

pub(crate) fn text_response(code: StatusCode, msg: impl Into<Bytes>) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .body(box_full(Full::new(msg.into())))
        .unwrap()
}

pub(crate) fn uri_too_long_response() -> Response<ResBody> {
    text_response(
        StatusCode::URI_TOO_LONG,
        Bytes::from_static(b"request URI too long"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cl_path() {
        // Positive cases: Standard and client-specific CL prefixes
        assert!(is_cl_path("/eth"));
        assert!(is_cl_path("/eth/"));
        assert!(is_cl_path("/eth/v1/node/syncing"));

        assert!(is_cl_path("/teku"));
        assert!(is_cl_path("/teku/"));
        assert!(is_cl_path("/teku/v1/node/syncing"));

        assert!(is_cl_path("/prysm"));
        assert!(is_cl_path("/prysm/"));
        assert!(is_cl_path("/prysm/v1/node/syncing"));

        assert!(is_cl_path("/nimbus"));
        assert!(is_cl_path("/nimbus/"));
        assert!(is_cl_path("/nimbus/v1/node/syncing"));

        assert!(is_cl_path("/lodestar"));
        assert!(is_cl_path("/lodestar/"));
        assert!(is_cl_path("/lodestar/v1/node/syncing"));

        assert!(is_cl_path("/lighthouse"));
        assert!(is_cl_path("/lighthouse/"));
        assert!(is_cl_path("/lighthouse/v1/node/syncing"));

        // Negative cases
        assert!(!is_cl_path("/"));
        assert!(!is_cl_path(""));
        assert!(!is_cl_path("/healthz"));
        assert!(!is_cl_path("/readyz"));
        assert!(!is_cl_path("/livez"));
        assert!(!is_cl_path("/lighthousestuff"));
        assert!(!is_cl_path("/prys"));
        assert!(!is_cl_path("eth"));
    }

    #[test]
    fn test_classify_request() {
        // 1. CL Path HTTP request
        let req = Request::builder()
            .uri("/eth/v1/node/syncing")
            .body(())
            .unwrap();
        assert_eq!(classify_request(&req), Route::Proxy(Target::Cl));

        // 2. EL Path HTTP request
        let req = Request::builder().uri("/").body(()).unwrap();
        assert_eq!(classify_request(&req), Route::Proxy(Target::El));

        // 3. WS HTTP/1.1 Upgrade request
        let req = Request::builder()
            .uri("/")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .body(())
            .unwrap();
        assert_eq!(
            classify_request(&req),
            Route::Proxy(Target::WebSocket(WsMode::Upgrade))
        );

        // 4. WS HTTP/2 Extended CONNECT request
        let mut req = Request::builder()
            .version(Version::HTTP_2)
            .method(Method::CONNECT)
            .uri("/")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        assert_eq!(
            classify_request(&req),
            Route::Proxy(Target::WebSocket(WsMode::ExtendedConnect))
        );
    }

    #[test]
    fn routing_preserves_probe_and_cl_precedence() {
        for (path, probe) in [
            ("/healthz", Probe::Health),
            ("/readyz", Probe::Ready),
            ("/livez", Probe::Live),
            ("/metrics", Probe::Metrics),
        ] {
            for method in [Method::GET, Method::HEAD, Method::POST, Method::CONNECT] {
                let mut req = Request::builder()
                    .method(method.clone())
                    .uri(format!("{path}?check=1"))
                    .body(())
                    .unwrap();
                let expected = if method == Method::GET || method == Method::HEAD {
                    Route::Probe(probe)
                } else if method == Method::CONNECT {
                    Route::Reject
                } else {
                    Route::Proxy(Target::El)
                };
                assert_eq!(classify_request(&req), expected);
                if method == Method::CONNECT {
                    *req.version_mut() = Version::HTTP_2;
                    req.extensions_mut()
                        .insert(hyper::ext::Protocol::from_static("websocket"));
                    assert_eq!(
                        classify_request(&req),
                        Route::Proxy(Target::WebSocket(WsMode::ExtendedConnect))
                    );
                } else {
                    req.headers_mut()
                        .insert("connection", "keep-alive, Upgrade".parse().unwrap());
                    req.headers_mut()
                        .insert("upgrade", "WebSocket".parse().unwrap());
                    assert_eq!(
                        classify_request(&req),
                        if method == Method::POST {
                            // Malformed H1 upgrades still reach Tungstenite's validator.
                            Route::Proxy(Target::WebSocket(WsMode::Upgrade))
                        } else {
                            expected
                        }
                    );
                }
            }
        }
        for path in ["/eth", "/eth/v1/node/health", "/lighthouse/peers"] {
            let req = Request::builder()
                .method(Method::GET)
                .uri(path)
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .body(())
                .unwrap();
            assert_eq!(classify_request(&req), Route::Proxy(Target::Cl));

            let mut connect_req = Request::builder()
                .version(Version::HTTP_2)
                .method(Method::CONNECT)
                .uri(path)
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .body(())
                .unwrap();
            connect_req
                .extensions_mut()
                .insert(hyper::ext::Protocol::from_static("websocket"));
            assert_eq!(classify_request(&connect_req), Route::Reject);
        }
        let mut req = Request::builder()
            .version(Version::HTTP_2)
            .method(Method::CONNECT)
            .uri("/")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("other"));
        assert_eq!(classify_request(&req), Route::Reject);

        let asterisk = Request::builder()
            .method(Method::OPTIONS)
            .uri("*")
            .body(())
            .unwrap();
        assert_eq!(classify_request(&asterisk), Route::Reject);
    }

    #[tokio::test]
    async fn test_try_mock_eth_syncing() {
        // Single eth_syncing request
        let req = r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":100}"#;
        let res = try_mock_eth_syncing(req.as_bytes()).unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, r#"{"jsonrpc":"2.0","id":100,"result":false}"#);

        // Non-eth_syncing request should return None
        let req = r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":101}"#;
        assert!(try_mock_eth_syncing(req.as_bytes()).is_none());

        // Batch eth_syncing request
        let batch = r#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}]"#;
        let res = try_mock_eth_syncing(batch.as_bytes()).unwrap();
        let body = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, r#"[{"jsonrpc":"2.0","id":1,"result":false}]"#);
    }

    #[test]
    fn response_idle_timeout_distinguishes_event_streams_and_zero_disable() {
        let proxy_timeout = Duration::from_secs(60);
        let sse_timeout = Duration::from_secs(900);

        let mut plain = HeaderMap::new();
        assert_eq!(
            response_idle_timeout(&plain, proxy_timeout, sse_timeout),
            Some(proxy_timeout)
        );
        plain.insert("content-type", "application/json".parse().unwrap());
        assert_eq!(
            response_idle_timeout(&plain, proxy_timeout, sse_timeout),
            Some(proxy_timeout)
        );

        for ct in [
            "text/event-stream",
            "text/event-stream; charset=utf-8",
            "Text/Event-Stream ;charset=UTF-8",
        ] {
            let mut sse = HeaderMap::new();
            sse.insert("content-type", ct.parse().unwrap());
            assert_eq!(
                response_idle_timeout(&sse, proxy_timeout, sse_timeout),
                Some(sse_timeout),
                "ct={ct}"
            );
            assert_eq!(
                response_idle_timeout(&sse, proxy_timeout, Duration::ZERO),
                None,
                "ct={ct} disabled"
            );
        }
    }

    #[tokio::test]
    async fn idle_timeout_body_preserves_size_hint_and_avoids_timer_when_immediately_ready() {
        let payload = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}");
        let mut body =
            IdleTimeoutBody::new(Full::new(payload.clone()), Some(Duration::from_secs(5)));
        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(payload.len() as u64));

        let collected = (&mut body).collect().await.unwrap().to_bytes();
        assert_eq!(collected, payload);
        assert!(
            !body.deadline.is_allocated(),
            "immediate bodies must not allocate Sleep"
        );
        assert!(body.is_end_stream());

        let empty = IdleTimeoutBody::new(Full::new(Bytes::new()), Some(Duration::from_secs(5)));
        assert!(empty.is_end_stream());
        assert_eq!(empty.size_hint().exact(), Some(0));
    }

    struct ChannelBody(tokio::sync::mpsc::Receiver<Result<Frame<Bytes>, Infallible>>);

    impl Body for ChannelBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.0.poll_recv(cx)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_body_resets_between_chunks_and_aborts_when_stalled() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut body = IdleTimeoutBody::new(ChannelBody(rx), Some(Duration::from_secs(10)));

        let reader = tokio::spawn(async move {
            let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(first, Bytes::from_static(b"chunk-1"));
            let second = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(second, Bytes::from_static(b"chunk-2"));
            let err = body.frame().await.unwrap().unwrap_err();
            assert_eq!(err.to_string(), "response body idle timeout");
            assert!(body.frame().await.is_none(), "body must fuse after timeout");
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-1"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Another 7s (14s total > 10s, but < 10s since chunk-1) must succeed.
        tokio::time::advance(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-2"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Stalling > 10s with tx still open triggers the idle timeout.
        tokio::time::advance(Duration::from_secs(11)).await;
        reader.await.unwrap();
    }

    #[test]
    fn sanitize_before_idle_timeout_prevents_connection_nominated_sse_spoofing() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "content-type".parse().unwrap());
        headers.insert("content-type", "text/event-stream".parse().unwrap());
        sanitize_response_headers(StatusCode::OK, &mut headers);
        assert_eq!(
            response_idle_timeout(&headers, Duration::from_secs(60), Duration::from_secs(900)),
            Some(Duration::from_secs(60))
        );
    }

    #[tokio::test]
    async fn h2_extended_connect_permit_reuses_first_slot_and_caps_concurrent_amplification() {
        use clap::Parser;
        use tokio::sync::{mpsc, watch};

        let cfg =
            crate::Config::parse_from(["ethonyx", "--trust-upstream", "--max-connections", "2"]);
        let validated = cfg.parse_validated().unwrap();
        let state =
            Arc::new(AppState::new(&cfg, validated, false, health::HealthSource::Trusted).unwrap());
        let sem = Arc::clone(&state.conn_limit);
        let tcp_permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        let (_shutdown_tx, shutdown_rx) = watch::channel(None);
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let lifecycle = crate::lifecycle::Lifecycle::new(shutdown_rx, conn_tx);
        let conn = Arc::new(Connection::new(
            state,
            lifecycle,
            "127.0.0.1:12345".parse().unwrap(),
            tcp_permit,
        ));

        // 1st H2 Extended CONNECT stream reuses the connection's own permit (1 left in sem).
        let p1 = conn.acquire_ws_permit(WsMode::ExtendedConnect);
        assert!(p1.is_some());
        assert_eq!(sem.available_permits(), 1);

        // 2nd concurrent H2 Extended CONNECT stream claims the remaining semaphore permit.
        let p2 = conn.acquire_ws_permit(WsMode::ExtendedConnect);
        assert!(p2.is_some());
        assert_eq!(sem.available_permits(), 0);

        // 3rd concurrent stream is rejected instead of amplifying beyond max_connections=2.
        assert!(conn.acquire_ws_permit(WsMode::ExtendedConnect).is_none());

        // Dropping p1 releases the connection's included H2 WS slot for reuse.
        drop(p1);
        let p1_reused = conn.acquire_ws_permit(WsMode::ExtendedConnect);
        assert!(p1_reused.is_some());
        drop((p2, p1_reused));
        assert_eq!(sem.available_permits(), 1);
    }
}
