use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Version};
use hyper::body::{Body as _, Incoming};
use tokio::sync::OwnedSemaphorePermit;
use tracing::{Instrument, debug, info, trace, warn};

use crate::headers::{
    has_non_100_expect, has_unsupported_transfer_encoding, sanitize_response_headers,
    strip_hop_by_hop,
};
use crate::health;
use crate::metrics::{MethodBucket, UpstreamLayer};
use crate::response::{
    BodyReadError, PrefixBody, ResBody, collect_prefix_or_stream, drain_body, empty_body,
    full_body, prometheus_response, raw_json_response, status_response, text_response,
    uri_too_long_response,
};
use crate::state::{AppState, ClState};
use crate::transport::{HttpTarget, WsTarget};
use crate::websocket::WsMode;

const MAX_MOCK_DRAIN_BYTES: usize = 64 * 1024 * 1024;
const MAX_MOCK_INSPECT_BYTES: usize = 64 * 1024;

/// Permit bounding concurrent WebSocket relays across both HTTP/1.1 `Upgrade`
/// and HTTP/2 Extended `CONNECT` streams under `--max-connections`.
enum WsPermit {
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
    state: Arc<AppState>,
    lifecycle: crate::lifecycle::Lifecycle,
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
    fn acquire_ws_permit(self: &Arc<Self>, mode: WsMode) -> Option<WsPermit> {
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
                } else if let Ok(_permit) = Arc::clone(&self.state.conn_limit).try_acquire_owned() {
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
        let is_head = req.method() == Method::HEAD;
        // Hyper's HTTP/2 server does not suppress bodies for `Method::HEAD` (unlike
        // HTTP/1.1, which preserves local `text_response` `Content-Length` while
        // omitting body bytes on the wire), so strip non-empty bodies on HTTP/2
        // `HEAD` here while keeping all response headers intact.
        let strip_head_body = is_head && req.version() == Version::HTTP_2;
        let finish = |mut resp: Response<ResBody>| {
            if strip_head_body && !resp.body().is_end_stream() {
                *resp.body_mut() = empty_body();
            }
            Ok(resp)
        };
        // The same decision drives routing, metrics and access logs. GET/HEAD
        // probes, unconfigured CL/WS routes, and invalid targets return locally
        // without upstream metrics/spans.
        let forward = match classify_request(&req) {
            Route::Probe(probe) => return Ok(probe.response(is_head, &self.state)),
            Route::Reject => {
                return finish(text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid request target",
                ));
            }
            Route::Proxy(target) => {
                let Some(forward) = Forward::resolve(target, &self.state) else {
                    return finish(text_response(StatusCode::NOT_FOUND, "not found"));
                };
                forward
            }
        };
        let (layer, upstream_proto) = forward.layer_and_proto();
        let method_bucket = MethodBucket::classify(req.method());
        let access_ctx = tracing::enabled!(target: "access_log", tracing::Level::INFO).then(|| {
            (
                req.version(),
                req.method().clone(),
                req.uri().path_and_query().cloned(),
            )
        });
        let otel_enabled = self.state.opts.otel_enabled;
        let span = if otel_enabled || tracing::enabled!(tracing::Level::DEBUG) {
            let span = tracing::info_span!(
                parent: None,
                "request",
                method = %req.method(),
                path = req.uri().path(),
                upstream = layer.as_str(),
                http.response.status_code = tracing::field::Empty,
            );
            if otel_enabled {
                crate::otel::attach_parent_context(&span, req.headers());
            }
            span
        } else {
            tracing::Span::none()
        };
        let start = std::time::Instant::now();
        let response = route(req, &self, forward).instrument(span.clone()).await;
        let elapsed = start.elapsed();
        let status = response.status().as_u16();
        if !span.is_disabled() {
            // Pass `i64` so `tracing-opentelemetry` records an integer attribute (`Value::I64`)
            // per OTel HTTP semantic conventions without allocating a formatted `String`.
            span.record("http.response.status_code", i64::from(status));
            // On `otel` builds, keep this ungated on `otel_enabled` so library callers
            // with an external `tracing-opentelemetry` layer also receive `Status::Error`
            // on 5xx (no-op on default builds without the `otel` feature).
            if status >= 500 {
                crate::otel::mark_span_error(&span);
            }
        }
        drop(span);
        if let Some((version, method, pq)) = access_ctx {
            info!(
                target: "access_log",
                peer = %self.peer,
                ?version,
                %method,
                path = pq.as_ref().map_or("/", PathAndQuery::path),
                upstream = layer.as_str(),
                proto = upstream_proto.as_str(),
                status,
                latency_ms = elapsed.as_secs_f64() * 1000.0,
                "request"
            );
        }
        self.state
            .metrics
            .record_request(layer, method_bucket, status, elapsed);
        finish(response)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    Health,
    Ready,
    Live,
    Metrics,
}

impl Probe {
    fn response(self, head: bool, state: &AppState) -> Response<ResBody> {
        match self {
            Self::Health => health::report(state, head),
            Self::Ready => health::ready(state, head),
            Self::Live => text_response(StatusCode::OK, if head { "" } else { "ok" }),
            Self::Metrics => prometheus_response(if head {
                Bytes::new()
            } else {
                Bytes::from(state.metrics.render_prometheus())
            }),
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
    if req.method() == Method::CONNECT {
        return if !is_cl_path(path) && WsMode::is_extended_connect(req) {
            Route::Proxy(Target::WebSocket(WsMode::ExtendedConnect))
        } else {
            Route::Reject
        };
    }
    // CL routes take precedence over upgrade headers.
    if is_cl_path(path) {
        Route::Proxy(Target::Cl)
    } else if WsMode::is_upgrade(req) {
        Route::Proxy(Target::WebSocket(WsMode::Upgrade))
    } else {
        Route::Proxy(Target::El)
    }
}

fn is_cl_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix('/') else {
        return false;
    };
    let segment = rest.split_once('/').map_or(rest, |(first, _)| first);
    matches!(
        segment,
        "eth" | "teku" | "prysm" | "nimbus" | "lodestar" | "lighthouse"
    )
}

#[derive(Clone, Copy)]
enum UpstreamProto {
    Http,
    Ws,
}

impl UpstreamProto {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "HTTP",
            Self::Ws => "WS",
        }
    }
}

/// A [`Target`] bound to the configured upstream it requires.
#[derive(Clone, Copy)]
enum Forward<'a> {
    Cl(&'a ClState),
    El,
    WebSocket(WsMode, &'a WsTarget),
}

impl<'a> Forward<'a> {
    fn resolve(target: Target, state: &'a AppState) -> Option<Self> {
        Some(match target {
            Target::Cl => Self::Cl(state.cl.as_ref()?),
            Target::El => Self::El,
            Target::WebSocket(mode) => Self::WebSocket(mode, state.el_ws.as_ref()?),
        })
    }

    const fn layer_and_proto(self) -> (UpstreamLayer, UpstreamProto) {
        match self {
            Self::Cl(_) => (UpstreamLayer::Cl, UpstreamProto::Http),
            Self::El => (UpstreamLayer::El, UpstreamProto::Http),
            Self::WebSocket(..) => (UpstreamLayer::El, UpstreamProto::Ws),
        }
    }
}

async fn route(
    req: Request<Incoming>,
    connection: &Arc<Connection>,
    forward: Forward<'_>,
) -> Response<ResBody> {
    let state = &connection.state;
    trace!(method = %req.method(), path = req.uri().path(), "routing request");
    match forward {
        Forward::Cl(cl) => proxy_http(req, state, Some(cl)).await,
        Forward::El => proxy_http(req, state, None).await,
        Forward::WebSocket(mode, ws) => {
            Box::pin(crate::websocket::upgrade(
                req,
                mode,
                ws,
                state,
                &connection.lifecycle,
                || connection.acquire_ws_permit(mode),
            ))
            .await
        }
    }
}

#[derive(Clone, Copy)]
enum LocalMock {
    NodeHealth,
    ClSubscription,
    ElSyncing,
}

async fn proxy_http(
    req: Request<Incoming>,
    state: &AppState,
    cl: Option<&ClState>,
) -> Response<ResBody> {
    let (parts, body) = req.into_parts();
    if has_unsupported_transfer_encoding(&parts.headers) {
        return text_response(StatusCode::NOT_IMPLEMENTED, "unsupported transfer encoding");
    }

    let mock = match cl {
        Some(cl)
            if cl.mock_node_health
                && matches!(parts.method, Method::GET | Method::HEAD)
                && parts.uri.path() == "/eth/v1/node/health" =>
        {
            Some(LocalMock::NodeHealth)
        }
        Some(cl)
            if cl.mock_subscriptions
                && parts.method == Method::POST
                && matches!(
                    parts.uri.path(),
                    "/eth/v1/validator/prepare_beacon_proposer"
                        | "/eth/v1/validator/beacon_committee_subscriptions"
                        | "/eth/v1/validator/sync_committee_subscriptions"
                ) =>
        {
            Some(LocalMock::ClSubscription)
        }
        None if state.opts.mock_eth_syncing && parts.method == Method::POST => {
            Some(LocalMock::ElSyncing)
        }
        _ => None,
    };

    let (body, deadline) = match mock {
        Some(mock) if !has_non_100_expect(&parts.headers) => match mock {
            LocalMock::NodeHealth => return health::node_health(state, parts.uri.query()),
            LocalMock::ClSubscription => {
                let deadline = tokio::time::Instant::now() + state.opts.proxy_timeout;
                if let Err(err) = drain_body(body, MAX_MOCK_DRAIN_BYTES, deadline).await {
                    return body_read_error_response(err);
                }
                return raw_json_response(
                    StatusCode::OK,
                    Bytes::from_static(b"{\"code\":200,\"message\":\"success\"}"),
                );
            }
            LocalMock::ElSyncing => {
                let deadline = tokio::time::Instant::now() + state.opts.proxy_timeout;
                match collect_prefix_or_stream(body, MAX_MOCK_INSPECT_BYTES, deadline).await {
                    Ok(PrefixBody::Complete(bytes)) => {
                        if let Some(mock_res) = try_mock_eth_syncing(&bytes) {
                            return mock_res;
                        }
                        (full_body(bytes), Some(deadline))
                    }
                    Ok(PrefixBody::Overflow { prefix, rest }) => {
                        (ResBody::prefixed(prefix, rest), Some(deadline))
                    }
                    Err(err) => return body_read_error_response(err),
                }
            }
        },
        _ => (ResBody::incoming(body), None),
    };

    let (target, layer, path_override) = match cl {
        Some(cl) => {
            let (target, path_override) = if let Some(mev) = &cl.mev_relay
                && parts.method == Method::POST
                && parts.uri.path() == "/eth/v1/validator/register_validator"
            {
                (mev, Some("/eth/v1/builder/validators"))
            } else {
                (&cl.beacon, None)
            };
            (target, UpstreamLayer::Cl, path_override)
        }
        None => (&state.el, UpstreamLayer::El, None),
    };
    // Select EL transport at dispatch, after any mock body inspection: local replies need
    // no transport lookup, and forwarded requests follow the latest verdict.
    execute_upstream(parts, body, state, target, layer, path_override, deadline).await
}

fn body_read_error_response(err: BodyReadError<hyper::Error>) -> Response<ResBody> {
    match err {
        BodyReadError::TooLarge => {
            text_response(StatusCode::PAYLOAD_TOO_LARGE, "request body exceeds limit")
        }
        BodyReadError::TimedOut => {
            text_response(StatusCode::REQUEST_TIMEOUT, "request body timed out")
        }
        BodyReadError::Body(err) => {
            debug!(error = %err, "failed to read request body");
            text_response(StatusCode::BAD_REQUEST, "failed to read request body")
        }
    }
}

fn try_mock_eth_syncing(bytes: &[u8]) -> Option<Response<ResBody>> {
    match crate::rpc::mock_syncing(bytes) {
        crate::rpc::MockResult::Forward => None,
        crate::rpc::MockResult::Silent => Some(status_response(StatusCode::NO_CONTENT)),
        crate::rpc::MockResult::Reply(body) => {
            Some(raw_json_response(StatusCode::OK, Bytes::from(body)))
        }
    }
}

/// Upstream response framing disposition classified once from `(status, is_head)`
/// after rejecting `1xx` responses.
#[derive(Clone, Copy)]
enum RespFraming {
    /// `204 No Content`: never carries a body; strips `Content-Length` and rejects
    /// unsupported `Transfer-Encoding` codings.
    NoContent,
    /// `205 Reset Content`: MUST NOT carry message content; rejects unsupported
    /// `Transfer-Encoding` and non-zero `Content-Length`, then drains with a 0-byte cap.
    ResetContent,
    /// `304 Not Modified` or `HEAD` (except `204`/`205`): carries no wire body and
    /// may advertise representation `Transfer-Encoding` as metadata (stripped on hop).
    BodylessMetadata,
    /// Normal response carrying a streamed message body.
    Stream,
}

impl RespFraming {
    const fn classify(status: StatusCode, is_head: bool) -> Self {
        match status {
            StatusCode::NO_CONTENT => Self::NoContent,
            StatusCode::RESET_CONTENT => Self::ResetContent,
            StatusCode::NOT_MODIFIED => Self::BodylessMetadata,
            _ if is_head => Self::BodylessMetadata,
            _ => Self::Stream,
        }
    }
}

async fn execute_upstream(
    mut parts: http::request::Parts,
    body: ResBody,
    state: &AppState,
    target: &HttpTarget,
    layer: UpstreamLayer,
    path_override: Option<&'static str>,
    deadline: Option<tokio::time::Instant>,
) -> Response<ResBody> {
    let Ok(rewritten) = target
        .upstream
        .rewrite_uri_with_path(parts.uri, path_override)
    else {
        return uri_too_long_response();
    };
    parts.uri = rewritten;
    // Strip hop-by-hop and Connection-nominated headers BEFORE injecting OTel
    // trace context so `Connection: traceparent` cannot strip outgoing spans.
    strip_hop_by_hop(&mut parts.headers);
    // `strip_hop_by_hop` drops `Content-Length` so Hyper recalculates framing,
    // but Hyper's HTTP/1.1 client omits `Content-Length` when `is_end_stream()`.
    // Restore `Content-Length: 0` on bodyless payload methods (matching Hyper's
    // HTTP/2 client and RFC 9110 §8.6) so strict upstreams do not return `411`.
    if body.is_end_stream() && matches!(parts.method, Method::POST | Method::PUT | Method::PATCH) {
        parts
            .headers
            .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    }
    if state.opts.otel_enabled {
        crate::otel::propagate_context(&mut parts.headers);
    }
    let is_head = parts.method == Method::HEAD;
    parts.extensions.clear();
    // The inbound version (e.g. HTTP/2) must not be forced onto the upstream
    // connection — normalize so the client uses whatever the upstream negotiates
    // (h1 for cleartext, h2 via ALPN for an https upstream).
    parts.version = Version::HTTP_11;
    let upstream_req = Request::from_parts(parts, body);

    let deadline =
        deadline.unwrap_or_else(|| tokio::time::Instant::now() + state.opts.proxy_timeout);
    let resp = match tokio::time::timeout_at(
        deadline,
        target.transport.request(&state.client, upstream_req),
    )
    .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => return bad_gateway(layer, err),
        Err(_) => return bad_gateway(layer, "upstream timeout"),
    };

    let (mut resp_parts, resp_body) = resp.into_parts();
    if resp_parts.status.is_informational() {
        return bad_gateway(layer, "unexpected 1xx upstream response");
    }
    let framing = RespFraming::classify(resp_parts.status, is_head);
    // RFC 9112 §6.1 / §6.3: `304` and non-204/205 `HEAD` responses carry no wire body
    // and may advertise representation transfer codings as metadata (stripped below on
    // the hop boundary); all other responses reject unsupported transfer codings.
    if !matches!(framing, RespFraming::BodylessMetadata)
        && has_unsupported_transfer_encoding(&resp_parts.headers)
    {
        return bad_gateway(layer, "unsupported upstream transfer-encoding");
    }
    // Normalize the upstream hop's version and clear client-side extensions
    // (`ReasonPhrase`, `OnUpgrade`) so HTTP/2 upstreams keep Hyper's HTTP/1.1
    // fast path and HTTP/1.0 upstreams cannot downgrade downstream keep-alive.
    resp_parts.version = Version::HTTP_11;
    resp_parts.extensions.clear();
    sanitize_response_headers(resp_parts.status, &mut resp_parts.headers);
    let body = match framing {
        RespFraming::ResetContent => {
            // RFC 9110 §15.3.6: `205 Reset Content` MUST NOT carry message content,
            // but valid upstreams may frame an empty body as chunked (`0\r\n\r\n`),
            // EOF, or an open H2 stream before `END_STREAM`. Reject non-zero
            // `Content-Length` upfront, then drain with a 0-byte cap (zero-poll on
            // `is_end_stream()`, preserves H1 keep-alive on empty chunked, and
            // rejects any non-empty payload as `502`).
            if resp_parts
                .headers
                .get(http::header::CONTENT_LENGTH)
                .is_some_and(|v| v != "0")
            {
                return bad_gateway(layer, "non-empty 205 upstream response");
            }
            if let Err(err) = drain_body(resp_body, 0, deadline).await {
                return match err {
                    BodyReadError::TooLarge => {
                        bad_gateway(layer, "non-empty 205 upstream response")
                    }
                    BodyReadError::TimedOut => bad_gateway(layer, "upstream 205 body timeout"),
                    BodyReadError::Body(err) => {
                        bad_gateway(layer, format_args!("upstream 205 body error: {err}"))
                    }
                };
            }
            empty_body()
        }
        RespFraming::NoContent | RespFraming::BodylessMetadata => {
            // Collapse `HEAD` and bodyless statuses unconditionally so downstream
            // never wraps `Incoming` in `IdleTimeoutBody`, re-inserts `content-length`
            // from an unclosed H2 `204`/`304`, or pipes forbidden `DATA` frames.
            empty_body()
        }
        RespFraming::Stream => ResBody::streaming(
            resp_body,
            response_idle_timeout(
                &resp_parts.headers,
                state.opts.proxy_timeout,
                state.opts.sse_idle_timeout,
            ),
        ),
    };
    Response::from_parts(resp_parts, body)
}

fn bad_gateway(layer: UpstreamLayer, error: impl std::fmt::Display) -> Response<ResBody> {
    warn!(
        upstream = layer.as_str(),
        proto = UpstreamProto::Http.as_str(),
        %error,
        "upstream proxy error"
    );
    text_response(StatusCode::BAD_GATEWAY, "bad gateway")
}

fn is_event_stream(headers: &HeaderMap) -> bool {
    headers.get(http::header::CONTENT_TYPE).is_some_and(|v| {
        let bytes = v.as_bytes();
        let media = bytes
            .iter()
            .position(|&b| b == b';')
            .map_or(bytes, |idx| &bytes[..idx]);
        media
            .trim_ascii()
            .eq_ignore_ascii_case(b"text/event-stream")
    })
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
                            // Non-GET H1 upgrades are rejected early in `WsMode::response`.
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
        // Origin-form `CONNECT /` with HTTP/1.1 Upgrade headers (and non-websocket
        // HTTP/2 CONNECT + Upgrade headers) must still be rejected as `Route::Reject`.
        for version in [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2] {
            let mut bad_connect = Request::builder()
                .version(version)
                .method(Method::CONNECT)
                .uri("/")
                .header("connection", "Upgrade")
                .header("upgrade", "websocket")
                .body(())
                .unwrap();
            if version == Version::HTTP_2 {
                bad_connect
                    .extensions_mut()
                    .insert(hyper::ext::Protocol::from_static("other"));
            }
            assert_eq!(classify_request(&bad_connect), Route::Reject);
        }

        let root_get = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();
        assert_eq!(classify_request(&root_get), Route::Proxy(Target::El));

        let root_ws = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header("connection", "keep-alive, Upgrade")
            .header("upgrade", "WebSocket")
            .body(())
            .unwrap();
        assert_eq!(
            classify_request(&root_ws),
            Route::Proxy(Target::WebSocket(WsMode::Upgrade))
        );

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

        // Notification eth_syncing returns 204 No Content with an empty body and no headers
        for notif in [
            r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#,
            r#"[{"jsonrpc":"2.0","method":"eth_syncing"}]"#,
        ] {
            let res = try_mock_eth_syncing(notif.as_bytes()).unwrap();
            assert_eq!(res.status(), StatusCode::NO_CONTENT);
            assert!(res.body().is_end_stream());
            assert!(res.headers().is_empty());
        }

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
        use tokio::sync::mpsc;

        let cfg = crate::Config::parse_from([
            "ethonyx",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
            "--max-connections",
            "2",
        ]);
        let parsed = cfg.parse_validated().unwrap();
        let (state, _) = AppState::start(
            parsed.opts,
            parsed.upstreams,
            parsed.health,
            crate::metrics::ServerMetrics::default(),
        )
        .await;
        let sem = Arc::clone(&state.conn_limit);
        let tcp_permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        let (_shutdown_tx, shutdown_rx) = crate::lifecycle::channel();
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

    #[tokio::test]
    async fn probes_match_head_headers_and_skip_bodies() {
        use clap::Parser;

        let cfg = crate::Config::parse_from([
            "ethonyx",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
        ]);
        let parsed = cfg.parse_validated().unwrap();
        let (state, _) = AppState::start(
            parsed.opts,
            parsed.upstreams,
            parsed.health,
            crate::metrics::ServerMetrics::default(),
        )
        .await;
        state.metrics.inc_saturated();

        for (head, has_body) in [(false, true), (true, false)] {
            let res = Probe::Metrics.response(head, &state);
            assert_eq!(res.status(), StatusCode::OK);
            assert_eq!(
                res.headers()[http::header::CONTENT_TYPE],
                "text/plain; version=0.0.4; charset=utf-8"
            );
            assert_eq!(res.body().is_end_stream(), !has_body, "head={head}");
        }

        // Every probe answers `HEAD` with the status and headers of its `GET` and no body.
        for probe in [Probe::Health, Probe::Ready, Probe::Live, Probe::Metrics] {
            let get = probe.response(false, &state);
            let head = probe.response(true, &state);
            assert_eq!(head.status(), get.status(), "{probe:?}");
            assert_eq!(head.headers(), get.headers(), "{probe:?}");
            assert!(!get.body().is_end_stream(), "{probe:?}");
            assert!(head.body().is_end_stream(), "{probe:?}");
        }
    }
}
