use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Version};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use tracing::{Instrument, debug, info, trace};

use crate::headers::strip_hop_by_hop;
use crate::health;
use crate::state::AppState;
use crate::upstream::HttpUpstream;
use crate::websocket::WsMode;

pub use crate::BoxError;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type ProxyClient = Client<Https, ResBody>;

pub fn build_client(force_h2: bool) -> ProxyClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_keepalive(Some(Duration::from_secs(60)));
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        // ALPN advertises ["h2","http/1.1"] for https upstreams → auto-negotiate
        // h2 when the upstream offers it; cleartext upstreams stay h1.
        .enable_all_versions()
        .wrap_connector(http);
    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_idle_timeout(Duration::from_secs(60));
    // Cleartext h2 can't be auto-negotiated, so an opt-in h2c upstream forces
    // HTTP/2 (prior-knowledge); an https upstream then uses h2 directly.
    if force_h2 {
        builder.http2_only(true);
    }
    builder.build(https)
}

/// Connection-owned state. Requests clone one Arc; detached WebSocket relays
/// and HTTP/2 request tasks also retain the shared lifecycle.
pub(crate) struct Connection {
    state: Arc<AppState>,
    lifecycle: crate::lifecycle::Lifecycle,
    peer: SocketAddr,
}

impl Connection {
    pub(crate) fn new(
        state: Arc<AppState>,
        lifecycle: crate::lifecycle::Lifecycle,
        peer: SocketAddr,
    ) -> Self {
        Self {
            state,
            lifecycle,
            peer,
        }
    }

    pub(crate) async fn dispatch(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<ResBody>, Infallible> {
        // The same decision drives routing, metrics and access logs. GET/HEAD
        // probes return locally; Extended CONNECT on those paths is still EL WS.
        let target = match classify_request(&req) {
            Route::Probe(probe) => return Ok(probe.response(req.method(), &self.state)),
            Route::Proxy(target) => target,
        };
        let (upstream_type, upstream_proto) = target.labels();
        let method_label = crate::method_to_static_str(req.method());
        let version = req.version();
        let access_log_enabled = tracing::enabled!(target: "access_log", tracing::Level::INFO);
        let otel_enabled = self.state.otel_enabled;
        let span_enabled = otel_enabled || tracing::enabled!(tracing::Level::DEBUG);
        let (method, path) = if access_log_enabled || span_enabled {
            (Some(req.method().clone()), req.uri().path().to_owned())
        } else {
            (None, String::new())
        };
        let start = std::time::Instant::now();
        let fut = route(req, &self, target);
        let res = if span_enabled {
            let method = method.as_ref().expect("method captured when span enabled");
            fut.instrument(tracing::info_span!(
                "request",
                %method,
                path = %path,
                upstream = upstream_type,
            ))
            .await
        } else {
            fut.await
        };
        let response = res.unwrap_or_else(|e| {
            debug!(error = %e, "proxy error");
            text_response(StatusCode::BAD_GATEWAY, e.to_string())
        });
        let elapsed = start.elapsed();
        let status = response.status().as_u16();
        if access_log_enabled {
            let method = method
                .as_ref()
                .expect("method captured when access_log enabled");
            info!(
                target: "access_log",
                peer = %self.peer,
                ?version, %method, %path,
                upstream = upstream_type, proto = upstream_proto,
                status, latency_ms = elapsed.as_secs_f64() * 1000.0,
                "request"
            );
        }
        ::metrics::counter!(
            "ethonyx_proxy_requests_total",
            "upstream" => upstream_type,
            "method" => method_label,
            "status" => crate::status_to_static_str(status)
        )
        .increment(1);
        ::metrics::histogram!(
            "ethonyx_proxy_request_duration_seconds", "upstream" => upstream_type
        )
        .record(elapsed.as_secs_f64());
        Ok(response)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Probe(Probe),
    Proxy(Target),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Cl,
    El,
    WebSocket(WsMode),
}

impl Target {
    fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::Cl => ("CL", "HTTP"),
            Self::El => ("EL", "HTTP"),
            Self::WebSocket(_) => ("EL", "WS"),
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
    let target = if is_cl_path(path) {
        Target::Cl
    } else if let Some(mode) = WsMode::classify(req) {
        Target::WebSocket(mode)
    } else {
        Target::El
    };
    Route::Proxy(target)
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
    connection: &Connection,
    target: Target,
) -> Result<Response<ResBody>, BoxError> {
    let state = &connection.state;
    trace!(method = %req.method(), path = req.uri().path(), "routing request");
    match target {
        Target::Cl => cl_proxy(req, state).await,
        Target::WebSocket(mode) => {
            crate::websocket::upgrade(
                req,
                mode,
                &state.el_ws,
                state.cfg.proxy_timeout,
                state.cfg.mock_eth_syncing,
                connection.lifecycle.clone(),
            )
            .await
        }
        Target::El => el_proxy(req, state).await,
    }
}

async fn cl_proxy(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
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
        let res = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(box_full(Full::new(Bytes::from_static(
                b"{\"code\":200,\"message\":\"success\"}",
            ))))
            .unwrap();
        return Ok(res);
    }
    let (upstream, override_path) = if req.method() == Method::POST
        && req.uri().path() == "/eth/v1/validator/register_validator"
        && let Some(ref mev) = state.cl_mev_relay
    {
        (mev, Some("/eth/v1/builder/validators"))
    } else {
        (&state.cl_beacon, None)
    };
    send_http(req, state, upstream, override_path, &state.client).await
}

async fn el_proxy(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
    if req.method() == Method::POST && state.cfg.mock_eth_syncing {
        let (parts, body) = req.into_parts();
        let collected = tokio::time::timeout(
            state.cfg.proxy_timeout,
            http_body_util::Limited::new(body, 64 * 1024 * 1024).collect(),
        )
        .await;
        match collected {
            Ok(Ok(collected)) => {
                let bytes = collected.to_bytes();
                if let Some(mock_res) = try_mock_eth_syncing(&bytes) {
                    return Ok(mock_res);
                }
                let req = Request::from_parts(parts, Full::new(bytes));
                return el_http_proxy(req, state).await;
            }
            Ok(Err(e)) => {
                return Ok(text_response(
                    if e.is::<http_body_util::LengthLimitError>() {
                        StatusCode::PAYLOAD_TOO_LARGE
                    } else {
                        StatusCode::BAD_REQUEST
                    },
                    format!("failed to read request body: {e}"),
                ));
            }
            Err(_) => {
                return Ok(text_response(
                    StatusCode::REQUEST_TIMEOUT,
                    "request body timed out",
                ));
            }
        }
    }

    el_http_proxy(req, state).await
}

fn try_mock_eth_syncing(bytes: &[u8]) -> Option<Response<ResBody>> {
    match crate::rpc::mock_syncing(bytes) {
        crate::rpc::MockResult::Forward => None,
        crate::rpc::MockResult::Silent => Some(text_response(StatusCode::NO_CONTENT, Bytes::new())),
        crate::rpc::MockResult::Reply(body) => Some(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(box_full(Full::new(Bytes::from(body))))
                .unwrap(),
        ),
    }
}

async fn el_http_proxy<B>(req: Request<B>, state: &AppState) -> Result<Response<ResBody>, BoxError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    // Select at dispatch, after any mock body inspection. Local replies need
    // no transport lookup, and forwarded requests follow the latest verdict.
    send_http(req, state, &state.el_http, None, state.el_client()).await
}

async fn send_http<B>(
    req: Request<B>,
    state: &AppState,
    upstream: &HttpUpstream,
    override_path: Option<&'static str>,
    client: &ProxyClient,
) -> Result<Response<ResBody>, BoxError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let (mut parts, body) = req.into_parts();
    #[cfg(feature = "otel")]
    if state.otel_enabled {
        crate::otel::propagate_context(&mut parts.headers);
    }
    parts.uri = upstream.rewrite_uri(parts.uri, override_path)?;
    strip_hop_by_hop(&mut parts.headers);
    parts.extensions.clear();
    // The inbound version (e.g. HTTP/2) must not be forced onto the upstream
    // connection — normalize so the client uses whatever the upstream negotiates
    // (h1 for cleartext, h2 via ALPN for an https upstream).
    parts.version = Version::HTTP_11;
    let upstream_req = Request::from_parts(parts, box_incoming(body));

    let resp = tokio::time::timeout(state.cfg.proxy_timeout, client.request(upstream_req))
        .await
        .map_err(|_| -> BoxError { "upstream timeout".into() })??;

    let (mut resp_parts, resp_body) = resp.into_parts();
    strip_hop_by_hop(&mut resp_parts.headers);
    Ok(Response::from_parts(resp_parts, box_incoming(resp_body)))
}

pub fn box_incoming<B>(body: B) -> ResBody
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

pub fn box_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

pub fn text_response(code: StatusCode, msg: impl Into<Bytes>) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .body(box_full(Full::new(msg.into())))
        .unwrap()
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
            let mut req = Request::builder()
                .method(Method::CONNECT)
                .uri(path)
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .body(())
                .unwrap();
            req.extensions_mut()
                .insert(hyper::ext::Protocol::from_static("websocket"));
            assert_eq!(classify_request(&req), Route::Proxy(Target::Cl));
        }
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri("/")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("other"));
        assert_eq!(classify_request(&req), Route::Proxy(Target::El));
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
}
