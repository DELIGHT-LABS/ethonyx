//! WebSocket HTTP/1.1 `Upgrade` and HTTP/2 Extended `CONNECT` handshake, subprotocol
//! validation, and upstream dialing.

mod relay;

use crate::lifecycle::Lifecycle;
use crate::metrics::UpstreamLayer;
use crate::response::{ResBody, empty_body, text_response, uri_too_long_response};
use crate::state::{ConnPermitGuard, ProxyState};
use crate::transport::{WsTarget, configure_tcp_stream};
use http::{Method, Request, Response, StatusCode, Version};
use hyper::body::{Body as _, Incoming};
use hyper_util::rt::TokioIo;
use relay::{WriteProgressIo, bridge_ws};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::OwnedSemaphorePermit;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use tracing::{debug, warn};

/// Shared TCP permit + single reusable WebSocket slot, allocated only for
/// WS-enabled servers with a bounded `--max-connections` after protocol preface
/// succeeds so detached WS relays never retain `Arc<Connection>` / `Arc<ProxyState>`.
struct SharedPermit {
    _permit: OwnedSemaphorePermit,
    ws_slot_in_use: AtomicBool,
}

/// Connection-scoped WebSocket state for WS-enabled servers.
/// `permit` is declared before `lifecycle` so the semaphore permit drops before
/// releasing the drain token if `Connection` drops standalone.
pub(crate) struct WsConn {
    permit: Option<Arc<SharedPermit>>,
    lifecycle: Lifecycle,
}

/// Permit bounding concurrent WebSocket relays across both HTTP/1.1 `Upgrade`
/// and HTTP/2 Extended `CONNECT` streams under `--max-connections`.
#[must_use = "WebSocket permit is released immediately if dropped"]
enum WsPermit {
    Shared(Arc<SharedPermit>),
    Direct { _guard: ConnPermitGuard },
}

impl Drop for WsPermit {
    fn drop(&mut self) {
        if let Self::Shared(permit) = self {
            permit.ws_slot_in_use.store(false, Ordering::Release);
        }
    }
}

impl WsConn {
    pub(crate) fn new(permit: ConnPermitGuard, lifecycle: Lifecycle) -> Self {
        Self {
            permit: permit.into_inner().map(|permit| {
                Arc::new(SharedPermit {
                    _permit: permit,
                    ws_slot_in_use: AtomicBool::new(false),
                })
            }),
            lifecycle,
        }
    }

    /// Claim a WebSocket concurrency slot:
    /// - Unlimited `--max-connections 0`: zero heap allocation and zero atomic CAS.
    /// - HTTP/1.1 `Upgrade` and the first active HTTP/2 Extended `CONNECT` stream
    ///   share the TCP connection's existing permit via `ws_slot_in_use`.
    /// - Additional concurrent HTTP/2 Extended `CONNECT` streams acquire dedicated
    ///   permits from `--max-connections` so multiplex amplification is bounded.
    fn acquire_permit(&self, state: &ProxyState) -> Option<WsPermit> {
        let Some(shared) = &self.permit else {
            return Some(WsPermit::Direct {
                _guard: ConnPermitGuard::unlimited(),
            });
        };
        if shared
            .ws_slot_in_use
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(WsPermit::Shared(Arc::clone(shared)))
        } else {
            state
                .try_acquire_permit()
                .map(|_guard| WsPermit::Direct { _guard })
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandshakeError {
    UnsupportedVersion,
    Invalid(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WsMode {
    Upgrade,
    ExtendedConnect,
}

impl WsMode {
    /// True when a `CONNECT` request carries RFC 8441 HTTP/2 `:protocol = websocket`.
    pub(crate) fn is_extended_connect<B>(req: &Request<B>) -> bool {
        req.method() == Method::CONNECT
            && req.version() == Version::HTTP_2
            && req
                .extensions()
                .get::<hyper::ext::Protocol>()
                .is_some_and(|p| p.as_str() == "websocket")
    }

    /// True when a non-`CONNECT` request carries RFC 6455 `Upgrade: websocket` + `Connection: upgrade`.
    pub(crate) fn is_upgrade<B>(req: &Request<B>) -> bool {
        // Check rare `Upgrade` before ubiquitous HTTP/1.1 `Connection` to avoid scanning
        // `Connection` tokens on every ordinary JSON-RPC request.
        crate::headers::header_has_token(req.headers(), http::header::UPGRADE, "websocket")
            && crate::headers::header_has_token(req.headers(), http::header::CONNECTION, "upgrade")
    }

    fn response(self, req: &mut Request<()>) -> Result<Response<()>, HandshakeError> {
        use http::header::{
            CONNECTION, SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
            SEC_WEBSOCKET_VERSION, UPGRADE,
        };
        if matches!(self, Self::Upgrade) {
            if req.method() != Method::GET {
                return Err(HandshakeError::Invalid("invalid websocket HTTP method"));
            }
            if req.version() != Version::HTTP_11 {
                return Err(HandshakeError::Invalid("invalid websocket HTTP version"));
            }
        }
        let mut versions = req.headers().get_all(SEC_WEBSOCKET_VERSION).iter();
        if versions.next().is_none_or(|v| v != "13") || versions.next().is_some() {
            return Err(HandshakeError::UnsupportedVersion);
        }
        let key = match self {
            Self::Upgrade => {
                let mut keys = req.headers().get_all(SEC_WEBSOCKET_KEY).iter();
                if keys.next().is_none() || keys.next().is_some() {
                    return Err(HandshakeError::Invalid("invalid websocket key count"));
                }
                req.headers_mut().remove(SEC_WEBSOCKET_KEY)
            }
            Self::ExtendedConnect => None,
        };
        let protocol =
            take_normalized_subprotocols(req.headers_mut()).map_err(HandshakeError::Invalid)?;
        // Strip hop-by-hop and Connection-nominated headers BEFORE restoring canonical
        // WebSocket handshake fields (and before upstream OTel context injection).
        crate::headers::strip_hop_by_hop(req.headers_mut());
        // Each hop terminates frames; Tungstenite cannot decode negotiated compression.
        req.headers_mut().remove(SEC_WEBSOCKET_EXTENSIONS);
        req.headers_mut()
            .insert(CONNECTION, http::HeaderValue::from_static("Upgrade"));
        req.headers_mut()
            .insert(UPGRADE, http::HeaderValue::from_static("websocket"));
        req.headers_mut()
            .insert(SEC_WEBSOCKET_VERSION, http::HeaderValue::from_static("13"));
        if let Some(protocol) = protocol {
            req.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        match key {
            Some(key) => {
                req.headers_mut().insert(SEC_WEBSOCKET_KEY, key);
                tokio_tungstenite::tungstenite::handshake::server::create_response(req)
                    .map_err(|_| HandshakeError::Invalid("invalid websocket handshake"))
            }
            None => Ok(Response::new(())),
        }
    }
}

fn upstream_request(
    mut req: Request<()>,
    uri: http::Uri,
    upstream: &crate::upstream::WsUpstream,
    otel_enabled: bool,
) -> Result<Request<()>, http::header::InvalidHeaderValue> {
    // RFC 6455: each WebSocket hop conducts an independent key/accept exchange,
    // so always generate a fresh Sec-WebSocket-Key for the upstream HTTP/1 handshake.
    let key = http::HeaderValue::try_from(
        tokio_tungstenite::tungstenite::handshake::client::generate_key(),
    )?;
    if otel_enabled {
        crate::otel::propagate_context(req.headers_mut());
    }
    req.headers_mut()
        .insert(http::header::HOST, upstream.host_header().clone());
    req.headers_mut()
        .insert(http::header::SEC_WEBSOCKET_KEY, key);
    *req.uri_mut() = uri;
    *req.method_mut() = Method::GET;
    *req.version_mut() = Version::HTTP_11;
    // OnUpgrade was taken before consuming the request. Move its headers rather
    // than cloning the map, and reuse the parsed URI for the upstream request.
    req.extensions_mut().clear();
    Ok(req)
}

fn is_valid_subprotocol_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

// HeaderMap::get sees only one field line, as does Tungstenite's offer parser.
// Validate and collapse all offer lines in place, returning the canonical value
// while leaving `headers` untouched if any token is invalid or duplicated.
fn take_normalized_subprotocols(
    headers: &mut http::HeaderMap,
) -> Result<Option<http::HeaderValue>, &'static str> {
    use http::header::SEC_WEBSOCKET_PROTOCOL;
    const INVALID: &str = "invalid websocket subprotocol offer";
    let mut iter = headers.get_all(SEC_WEBSOCKET_PROTOCOL).iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    if iter.next().is_none() && first.to_str().is_ok_and(is_valid_subprotocol_token) {
        return Ok(headers.remove(SEC_WEBSOCKET_PROTOCOL));
    }

    let mut protocols = String::new();
    // Borrow tokens from the original headers. The set only checks uniqueness;
    // appending below retains offer order and case without a quadratic scan.
    let mut seen = HashSet::new();
    for value in headers.get_all(SEC_WEBSOCKET_PROTOCOL) {
        for token in value.to_str().map_err(|_| INVALID)?.split(',') {
            let token = token.trim_matches([' ', '\t']);
            if !is_valid_subprotocol_token(token) || !seen.insert(token) {
                return Err(INVALID);
            }
            if !protocols.is_empty() {
                protocols.push_str(", ");
            }
            protocols.push_str(token);
        }
    }
    // Reuse the assembled buffer instead of copying it into a header value.
    let val = http::HeaderValue::try_from(protocols).map_err(|_| INVALID)?;
    headers.remove(SEC_WEBSOCKET_PROTOCOL);
    Ok(Some(val))
}

fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(2 * 1024 * 1024)
        .max_message_size(Some(64 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
}

pub(crate) async fn upgrade(
    req: Request<Incoming>,
    mode: WsMode,
    ws: &WsTarget,
    state: &ProxyState,
    ws_conn: &WsConn,
) -> Response<ResBody> {
    if matches!(mode, WsMode::Upgrade) && !req.body().is_end_stream() {
        return text_response(StatusCode::BAD_REQUEST, "invalid websocket upgrade");
    }
    let mut req = req.map(|_| ());
    let mut response = match mode.response(&mut req) {
        Ok(response) => response,
        Err(HandshakeError::UnsupportedVersion) => {
            debug!("rejected websocket upgrade: unsupported version");
            let mut resp = text_response(StatusCode::BAD_REQUEST, "invalid websocket upgrade");
            resp.headers_mut().insert(
                http::header::SEC_WEBSOCKET_VERSION,
                http::HeaderValue::from_static("13"),
            );
            return resp;
        }
        Err(HandshakeError::Invalid(reason)) => {
            debug!(reason, "rejected websocket upgrade");
            return text_response(StatusCode::BAD_REQUEST, "invalid websocket upgrade");
        }
    };
    let inbound_uri = std::mem::take(req.uri_mut());
    let Ok(upstream_uri) = ws.upstream.rewrite_uri(inbound_uri) else {
        return uri_too_long_response();
    };
    let Some(permit) = ws_conn.acquire_permit(state) else {
        return text_response(StatusCode::SERVICE_UNAVAILABLE, "connection limit reached");
    };
    let on_upgrade = hyper::upgrade::on(&mut req);
    let Ok(ws_req) = upstream_request(req, upstream_uri, &ws.upstream, state.opts.otel_enabled)
    else {
        return text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
    };
    let io_timeout = state.opts.proxy_timeout;
    let dial = async {
        let tcp = tokio::net::TcpStream::connect(ws.upstream.dial_target())
            .await
            .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
        configure_tcp_stream(&tcp);
        tokio_tungstenite::client_async_tls_with_config(
            ws_req,
            WriteProgressIo::new(tcp),
            Some(ws_config()),
            Some(ws.tls.clone()),
        )
        .await
    };
    let (upstream_ws, mut upstream_response) = match tokio::time::timeout(io_timeout, dial).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            warn!(
                upstream = UpstreamLayer::El.as_str(),
                proto = "WS",
                error = %e,
                "upstream ws connect failed"
            );
            return text_response(StatusCode::BAD_GATEWAY, "upstream ws connect failed");
        }
        Err(_) => {
            warn!(
                upstream = UpstreamLayer::El.as_str(),
                proto = "WS",
                "upstream ws connect timed out"
            );
            return text_response(StatusCode::BAD_GATEWAY, "upstream ws connect timed out");
        }
    };
    if upstream_response
        .headers()
        .contains_key(http::header::SEC_WEBSOCKET_EXTENSIONS)
        || upstream_response
            .headers()
            .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
            .iter()
            .nth(1)
            .is_some()
    {
        warn!(
            upstream = UpstreamLayer::El.as_str(),
            proto = "WS",
            "invalid upstream websocket negotiation"
        );
        return text_response(
            StatusCode::BAD_GATEWAY,
            "invalid upstream websocket negotiation",
        );
    }
    // Tungstenite checks that the selected protocol was offered by the client.
    if let Some(protocol) = upstream_response
        .headers_mut()
        .remove(http::header::SEC_WEBSOCKET_PROTOCOL)
    {
        response
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, protocol);
    }
    let lifecycle = ws_conn.lifecycle.clone();
    let mock_enabled = state.opts.mock_eth_syncing;
    let ws_slot = state.core.metrics.ws_slot();
    tokio::spawn(lifecycle.run_with(move |shutdown| async move {
        let _permit = permit;
        match shutdown.bound(on_upgrade).await {
            Some(Ok(upgraded)) => {
                let _ws_guard = ws_slot.activate();
                let client_ws = WebSocketStream::from_raw_socket(
                    WriteProgressIo::new(TokioIo::new(upgraded)),
                    Role::Server,
                    Some(ws_config()),
                )
                .await;
                bridge_ws(client_ws, upstream_ws, mock_enabled, io_timeout, &shutdown).await;
            }
            Some(Err(e)) => debug!(error = %e, "client ws upgrade failed"),
            None => {}
        }
    }));
    response.map(|()| empty_body())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_offers_preserve_all_lines_and_reject_invalid_tokens() {
        for (values, expected) in [
            (vec![], Some("")),
            (vec![" first\t"], Some("first")),
            (vec!["first", "second, Third"], Some("first, second, Third")),
            (
                vec![" first\t,second ", "THIRD"],
                Some("first, second, THIRD"),
            ),
            (vec!["first", "First"], Some("first, First")),
            (vec!["rpc.v2, rpc", "RPC"], Some("rpc.v2, rpc, RPC")),
            (vec!["!#$%&'*+-.^_`|~"], Some("!#$%&'*+-.^_`|~")),
            (vec!["first", "first"], None),
            (vec!["first, first"], None),
            (vec!["first, second", "Third, first"], None),
            (vec!["first, second", "\tsecond "], None),
            (vec!["first", "", "second"], None),
            (vec!["first", "\t ", "second"], None),
            (vec!["first", ",second"], None),
            (vec![""], None),
            (vec!["first,"], None),
            (vec!["first,,second"], None),
            (vec!["not a token"], None),
            (vec!["first/second"], None),
            (vec!["\"first\""], None),
        ] {
            let mut headers = http::HeaderMap::new();
            headers.insert("x-preserved", "original".parse().unwrap());
            for value in values {
                headers.append("sec-websocket-protocol", value.parse().unwrap());
            }
            let original = headers.clone();
            let result = take_normalized_subprotocols(&mut headers);
            assert_eq!(result.is_ok(), expected.is_some());
            assert_eq!(headers["x-preserved"], "original");
            if let Some(expected) = expected {
                let normalized = result.unwrap();
                assert_eq!(
                    normalized.as_ref().map_or("", |v| v.to_str().unwrap()),
                    expected
                );
                assert!(!headers.contains_key("sec-websocket-protocol"));
            } else {
                assert_eq!(headers, original, "invalid offers must remain unchanged");
            }
        }
    }

    #[test]
    fn large_subprotocol_offers_preserve_order_and_reject_late_duplicates() {
        use http::header::SEC_WEBSOCKET_PROTOCOL;
        // Reverse order catches sorting; the final case variant is distinct.
        // Both representations fit within the HTTP/1 header buffer limit.
        let mut tokens: Vec<_> = (0..32_768).rev().map(|i| format!("p{i:05}")).collect();
        tokens.push("P00000".into());
        let expected = tokens.join(", ");
        for per_line in [tokens.len(), 4096] {
            let mut headers = http::HeaderMap::new();
            for chunk in tokens.chunks(per_line) {
                headers.append(SEC_WEBSOCKET_PROTOCOL, chunk.join(",").parse().unwrap());
            }
            let original = headers.clone();
            let normalized = take_normalized_subprotocols(&mut headers).unwrap().unwrap();
            assert_eq!(normalized, expected);
            assert!(!headers.contains_key(SEC_WEBSOCKET_PROTOCOL));

            let mut invalid = original;
            invalid.append(SEC_WEBSOCKET_PROTOCOL, " \tp32767\t ".parse().unwrap());
            let before = invalid.clone();
            assert!(take_normalized_subprotocols(&mut invalid).is_err());
            assert_eq!(invalid, before);
        }
    }

    #[test]
    fn subprotocol_offers_reject_non_ascii_without_replacing_headers() {
        let mut headers = http::HeaderMap::new();
        headers.append("sec-websocket-protocol", "valid".parse().unwrap());
        headers.append(
            "sec-websocket-protocol",
            http::HeaderValue::from_bytes(b"other, \xff").unwrap(),
        );
        let before = headers.clone();
        assert!(take_normalized_subprotocols(&mut headers).is_err());
        assert_eq!(headers, before);
    }

    #[test]
    fn extended_connect_requires_http2_and_websocket_protocol() {
        for version in [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2] {
            for protocol in [None, Some("websocket"), Some("other")] {
                let mut req = Request::builder()
                    .version(version)
                    .method(Method::CONNECT)
                    .uri("/")
                    .body(())
                    .unwrap();
                if let Some(protocol) = protocol {
                    req.extensions_mut()
                        .insert(hyper::ext::Protocol::from_static(protocol));
                }
                let extended = version == Version::HTTP_2 && protocol == Some("websocket");
                assert_eq!(WsMode::is_extended_connect(&req), extended);
                assert!(!WsMode::is_upgrade(&req));
                req.headers_mut()
                    .insert("connection", "Upgrade".parse().unwrap());
                req.headers_mut()
                    .insert("upgrade", "websocket".parse().unwrap());
                assert_eq!(WsMode::is_extended_connect(&req), extended);
                assert!(WsMode::is_upgrade(&req));
            }
        }
        // Non-GET HTTP/1.1 Upgrade fails fast inside `WsMode::response` before subprotocol work.
        let mut post_upgrade = Request::builder()
            .version(Version::HTTP_11)
            .method(Method::POST)
            .uri("/")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap();
        assert_eq!(
            WsMode::Upgrade.response(&mut post_upgrade).unwrap_err(),
            HandshakeError::Invalid("invalid websocket HTTP method")
        );
        let mut h10_upgrade = Request::builder()
            .version(Version::HTTP_10)
            .method(Method::GET)
            .uri("/")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap();
        assert_eq!(
            WsMode::Upgrade.response(&mut h10_upgrade).unwrap_err(),
            HandshakeError::Invalid("invalid websocket HTTP version")
        );
        // Non-CONNECT HTTP/2 request carrying Protocol("websocket") must not match ExtendedConnect.
        let mut get_h2 = Request::builder()
            .version(Version::HTTP_2)
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();
        get_h2
            .extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        assert!(!WsMode::is_extended_connect(&get_h2));
    }

    #[test]
    fn handshake_strips_hop_by_hop_and_restores_canonical_ws_headers() {
        let upstream = crate::upstream::WsUpstream::parse(
            "--el-ws-url",
            "ws://127.0.0.1:8546/ws?api_key=secret",
        )
        .unwrap();
        for mode in [WsMode::Upgrade, WsMode::ExtendedConnect] {
            let mut req = Request::builder()
                .version(if mode == WsMode::Upgrade {
                    Version::HTTP_11
                } else {
                    Version::HTTP_2
                })
                .method(if mode == WsMode::Upgrade {
                    Method::GET
                } else {
                    Method::CONNECT
                })
                .uri("/sub?tag=1")
                .header(
                    "connection",
                    "Upgrade, keep-alive, X-Internal, sec-websocket-key, sec-websocket-version, sec-websocket-protocol",
                )
                .header("upgrade", "websocket")
                .header("sec-websocket-version", "13")
                .header("sec-websocket-protocol", "first, second")
                .header("sec-websocket-extensions", "permessage-deflate")
                .header("x-internal", "secret")
                .header("proxy-authorization", "Basic Zm9vOmJhcg==")
                .header("proxy-connection", "keep-alive")
                .header("te", "trailers")
                .header("authorization", "Bearer token")
                .body(())
                .unwrap();
            let client_key = "dGhlIHNhbXBsZSBub25jZQ==";
            req.headers_mut()
                .insert("sec-websocket-key", client_key.parse().unwrap());
            if mode == WsMode::ExtendedConnect {
                // RFC 8441 has no key exchange: client-supplied (even duplicate) keys must
                // be replaced with a single fresh upstream key by `upstream_request`.
                req.headers_mut()
                    .append("sec-websocket-key", "Y2xpZW50LWR1cC1rZXk=".parse().unwrap());
            }
            let resp = mode.response(&mut req).unwrap();
            assert_eq!(
                resp.status(),
                if mode == WsMode::Upgrade {
                    StatusCode::SWITCHING_PROTOCOLS
                } else {
                    StatusCode::OK
                }
            );
            let inbound_uri = std::mem::take(req.uri_mut());
            let uri = upstream.rewrite_uri(inbound_uri).unwrap();
            let ws_req = upstream_request(req, uri, &upstream, false).unwrap();
            let h = ws_req.headers();
            assert!(!h.contains_key("x-internal"));
            assert!(!h.contains_key("proxy-authorization"));
            assert!(!h.contains_key("proxy-connection"));
            assert!(!h.contains_key("te"));
            assert!(!h.contains_key("keep-alive"));
            assert!(!h.contains_key("sec-websocket-extensions"));
            assert_eq!(h["connection"], "Upgrade");
            assert_eq!(h["upgrade"], "websocket");
            assert_eq!(h["sec-websocket-version"], "13");
            assert_eq!(h["sec-websocket-protocol"], "first, second");
            let mut keys = h.get_all("sec-websocket-key").iter();
            let upstream_key = keys.next().expect("upstream sec-websocket-key");
            assert!(keys.next().is_none(), "single upstream sec-websocket-key");
            assert_ne!(upstream_key, client_key);
            assert_ne!(upstream_key, "Y2xpZW50LWR1cC1rZXk=");
            assert_eq!(h["host"], "127.0.0.1:8546");
            assert_eq!(h["authorization"], "Bearer token");
            assert_eq!(
                ws_req.uri().path_and_query().unwrap().as_str(),
                "/ws/sub?api_key=secret&tag=1"
            );
        }
    }

    #[tokio::test]
    async fn ws_conn_reuses_first_slot_caps_amplification_and_skips_alloc_when_unlimited() {
        use clap::Parser;

        let parsed = crate::Config::parse_from([
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--el-ws-url",
            "ws://127.0.0.1:8546",
            "--trust-upstream",
            "--max-connections",
            "2",
        ])
        .parse_validated()
        .unwrap();
        let (state, _) = ProxyState::start(
            parsed.core,
            parsed.mode.into_proxy().unwrap(),
            crate::metrics::ServerMetrics::disabled(),
        )
        .await;
        let tcp_permit = state.try_acquire_permit().unwrap();
        let (_shutdown_tx, lifecycle, _drain) = crate::lifecycle::channel();
        let ws_conn = WsConn::new(tcp_permit, lifecycle.clone());
        let state_refs_before = Arc::strong_count(&state);

        // 1st WebSocket stream reuses the connection's own permit (1 left in sem)
        // without retaining `Arc<ProxyState>`.
        let p1 = ws_conn.acquire_permit(&state);
        assert!(matches!(p1, Some(WsPermit::Shared(_))));
        assert_eq!(state.conn_limit.available_permits(), Some(1));
        assert_eq!(Arc::strong_count(&state), state_refs_before);

        // 2nd concurrent H2 Extended CONNECT stream claims the remaining semaphore permit.
        let p2 = ws_conn.acquire_permit(&state);
        assert!(matches!(p2, Some(WsPermit::Direct { _guard: ref guard }) if guard.is_bounded()));
        assert_eq!(state.conn_limit.available_permits(), Some(0));

        // 3rd concurrent stream is rejected instead of amplifying beyond max_connections=2.
        assert!(ws_conn.acquire_permit(&state).is_none());

        // Dropping p1 releases the connection's included WS slot for reuse.
        drop(p1);
        let p1_reused = ws_conn.acquire_permit(&state);
        assert!(matches!(p1_reused, Some(WsPermit::Shared(_))));
        drop((p2, p1_reused));
        assert_eq!(state.conn_limit.available_permits(), Some(1));

        // A detached relay holding the shared slot keeps the TCP permit alive after
        // `WsConn` drops, then returns it on relay drop.
        let detached = ws_conn.acquire_permit(&state).unwrap();
        assert!(matches!(detached, WsPermit::Shared(_)));
        drop(ws_conn);
        assert_eq!(state.conn_limit.available_permits(), Some(1));
        drop(detached);
        assert_eq!(state.conn_limit.available_permits(), Some(2));

        // Unlimited `--max-connections 0` skips `Arc<SharedPermit>` allocation and CAS.
        let unlimited = crate::Config::parse_from([
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--el-ws-url",
            "ws://127.0.0.1:8546",
            "--trust-upstream",
            "--max-connections",
            "0",
        ])
        .parse_validated()
        .unwrap();
        let (unlimited_state, _) = ProxyState::start(
            unlimited.core,
            unlimited.mode.into_proxy().unwrap(),
            crate::metrics::ServerMetrics::disabled(),
        )
        .await;
        let unlimited_conn = WsConn::new(
            unlimited_state.try_acquire_permit().unwrap(),
            lifecycle.clone(),
        );
        assert!(unlimited_conn.permit.is_none());
        assert!(matches!(
            unlimited_conn.acquire_permit(&unlimited_state),
            Some(WsPermit::Direct { _guard: ref guard }) if !guard.is_bounded()
        ));
    }
}
