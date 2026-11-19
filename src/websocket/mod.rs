//! WebSocket HTTP/1.1 `Upgrade` and HTTP/2 Extended `CONNECT` handshake, subprotocol
//! validation, and upstream dialing.

mod relay;

use crate::BoxError;
use crate::lifecycle::Lifecycle;
use crate::response::{ResBody, empty_body, text_response, uri_too_long_response};
use crate::state::AppState;
use crate::transport::{WsTarget, configure_tcp_stream};
use http::{Method, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use relay::{WriteProgressIo, bridge_ws};
use std::collections::HashSet;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use tracing::{debug, warn};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WsMode {
    Upgrade,
    ExtendedConnect,
}

impl WsMode {
    pub(crate) fn classify<B>(req: &Request<B>) -> Option<Self> {
        if req.version() == Version::HTTP_2
            && req.method() == Method::CONNECT
            && req
                .extensions()
                .get::<hyper::ext::Protocol>()
                .is_some_and(|p| p.as_str() == "websocket")
        {
            Some(Self::ExtendedConnect)
        } else if is_upgrade_request(req) {
            Some(Self::Upgrade)
        } else {
            None
        }
    }

    fn response(self, req: &mut Request<()>, otel_enabled: bool) -> Result<Response<()>, BoxError> {
        use http::header::{
            CONNECTION, SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
            SEC_WEBSOCKET_VERSION, UPGRADE,
        };
        let mut versions = req.headers().get_all(SEC_WEBSOCKET_VERSION).iter();
        if versions.next().is_none_or(|v| v != "13") || versions.next().is_some() {
            return Err("invalid websocket version".into());
        }
        let key = if self == Self::Upgrade {
            if req.version() != Version::HTTP_11 {
                return Err("invalid websocket HTTP version".into());
            }
            if req.headers().get_all(SEC_WEBSOCKET_KEY).iter().count() != 1 {
                return Err("invalid websocket key count".into());
            }
            req.headers_mut().remove(SEC_WEBSOCKET_KEY)
        } else {
            None
        };
        normalize_subprotocols(req.headers_mut())?;
        let protocol = req.headers_mut().remove(SEC_WEBSOCKET_PROTOCOL);
        // Strip hop-by-hop and Connection-nominated headers BEFORE injecting OTel
        // trace context or restoring canonical WebSocket handshake fields.
        crate::headers::strip_hop_by_hop(req.headers_mut());
        if otel_enabled {
            crate::otel::propagate_context(req.headers_mut());
        }
        // Each hop terminates frames; Tungstenite cannot decode negotiated compression.
        req.headers_mut().remove(SEC_WEBSOCKET_EXTENSIONS);
        req.headers_mut().remove(SEC_WEBSOCKET_KEY);
        req.headers_mut()
            .insert(CONNECTION, http::HeaderValue::from_static("Upgrade"));
        req.headers_mut()
            .insert(UPGRADE, http::HeaderValue::from_static("websocket"));
        req.headers_mut()
            .insert(SEC_WEBSOCKET_VERSION, http::HeaderValue::from_static("13"));
        if let Some(key) = key {
            req.headers_mut().insert(SEC_WEBSOCKET_KEY, key);
        }
        if let Some(protocol) = protocol {
            req.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        match self {
            Self::Upgrade => {
                Ok(tokio_tungstenite::tungstenite::handshake::server::create_response(req)?)
            }
            Self::ExtendedConnect => Ok(Response::builder().status(StatusCode::OK).body(())?),
        }
    }
}

fn is_upgrade_request<B>(req: &Request<B>) -> bool {
    crate::headers::header_has_token(req.headers(), http::header::CONNECTION, "upgrade")
        && crate::headers::header_has_token(req.headers(), http::header::UPGRADE, "websocket")
}

fn is_valid_subprotocol_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

// HeaderMap::get sees only one field line, as does Tungstenite's offer parser.
// Preserve the complete ordered offer and validate each case-sensitive token.
fn normalize_subprotocols(headers: &mut http::HeaderMap) -> Result<(), BoxError> {
    use http::header::SEC_WEBSOCKET_PROTOCOL;
    let mut iter = headers.get_all(SEC_WEBSOCKET_PROTOCOL).iter();
    let Some(first) = iter.next() else {
        return Ok(());
    };
    if iter.next().is_none() && !first.as_bytes().contains(&b',') {
        let raw = first.to_str()?;
        let trimmed = raw.trim_matches([' ', '\t']);
        if !is_valid_subprotocol_token(trimmed) {
            return Err("invalid websocket subprotocol offer".into());
        }
        if trimmed.len() != raw.len() {
            let val = http::HeaderValue::from_str(trimmed)?;
            headers.insert(SEC_WEBSOCKET_PROTOCOL, val);
        }
        return Ok(());
    }

    let mut protocols = String::new();
    // Borrow tokens from the original headers. The set only checks uniqueness;
    // appending below retains offer order and case without a quadratic scan.
    let mut seen = HashSet::new();
    for value in headers.get_all(SEC_WEBSOCKET_PROTOCOL) {
        for token in value.to_str()?.split(',') {
            let token = token.trim_matches([' ', '\t']);
            if !is_valid_subprotocol_token(token) || !seen.insert(token) {
                return Err("invalid websocket subprotocol offer".into());
            }
            if !protocols.is_empty() {
                protocols.push_str(", ");
            }
            protocols.push_str(token);
        }
    }
    if !protocols.is_empty() {
        // Reuse the assembled buffer instead of copying it into a header value.
        headers.insert(
            SEC_WEBSOCKET_PROTOCOL,
            http::HeaderValue::try_from(protocols)?,
        );
    }
    Ok(())
}

fn build_ws_request(
    mut req: Request<()>,
    uri: http::Uri,
    upstream: &crate::upstream::WsUpstream,
    mode: WsMode,
) -> Request<()> {
    req.headers_mut()
        .insert(http::header::HOST, upstream.host_header().clone());
    // RFC 8441 replaces the client's key/accept exchange with :protocol.
    // Generate a fresh key only for the separate HTTP/1 upstream handshake.
    if mode == WsMode::ExtendedConnect {
        let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
        req.headers_mut().insert(
            http::header::SEC_WEBSOCKET_KEY,
            http::HeaderValue::try_from(key).expect("base64 key is valid header value"),
        );
    }
    *req.uri_mut() = uri;
    *req.method_mut() = Method::GET;
    *req.version_mut() = Version::HTTP_11;
    // OnUpgrade was taken before consuming the request. Move its headers rather
    // than cloning the map, and reuse the parsed URI for the upstream request.
    req.extensions_mut().clear();
    req
}

fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(64 * 1024 * 1024 + 64 * 1024)
        .max_message_size(Some(64 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
}

pub(crate) async fn upgrade<P: Send + 'static>(
    req: Request<Incoming>,
    mode: WsMode,
    ws: &WsTarget,
    state: &AppState,
    lifecycle: &Lifecycle,
    acquire_permit: impl FnOnce() -> Option<P>,
) -> Result<Response<ResBody>, BoxError> {
    let (parts, _body) = req.into_parts();
    let mut req = Request::from_parts(parts, ());
    let mut response = match mode.response(&mut req, state.opts.otel_enabled) {
        Ok(response) => response,
        Err(_) => {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "invalid websocket upgrade",
            ));
        }
    };
    let inbound_uri = std::mem::take(req.uri_mut());
    let Ok(upstream_uri) = ws.upstream.rewrite_uri(inbound_uri) else {
        return Ok(uri_too_long_response());
    };
    let Some(permit) = acquire_permit() else {
        return Ok(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "connection limit reached",
        ));
    };
    let on_upgrade = hyper::upgrade::on(&mut req);
    let ws_req = build_ws_request(req, upstream_uri, &ws.upstream, mode);
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
            warn!(error = %e, "upstream ws connect failed");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect failed",
            ));
        }
        Err(_) => {
            warn!("upstream ws connect timed out");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect timed out",
            ));
        }
    };
    if upstream_response
        .headers()
        .contains_key(http::header::SEC_WEBSOCKET_EXTENSIONS)
        || upstream_response
            .headers()
            .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
            .iter()
            .count()
            > 1
    {
        warn!("invalid upstream websocket negotiation");
        return Ok(text_response(
            StatusCode::BAD_GATEWAY,
            "invalid upstream websocket negotiation",
        ));
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
    let lifecycle = lifecycle.clone();
    let mock_enabled = state.opts.mock_eth_syncing;
    let ws_slot = state.metrics.ws_slot();
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
    Ok(response.map(|()| empty_body()))
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
            let result = normalize_subprotocols(&mut headers);
            assert_eq!(result.is_ok(), expected.is_some());
            assert_eq!(headers["x-preserved"], "original");
            if let Some(expected) = expected {
                assert_eq!(
                    headers
                        .get("sec-websocket-protocol")
                        .map_or("", |v| v.to_str().unwrap()),
                    expected
                );
                assert!(headers.get_all("sec-websocket-protocol").iter().count() <= 1);
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
            normalize_subprotocols(&mut headers).unwrap();
            assert_eq!(headers[SEC_WEBSOCKET_PROTOCOL], expected);
            assert_eq!(headers.get_all(SEC_WEBSOCKET_PROTOCOL).iter().count(), 1);

            let mut invalid = original;
            invalid.append(SEC_WEBSOCKET_PROTOCOL, " \tp32767\t ".parse().unwrap());
            let before = invalid.clone();
            assert!(normalize_subprotocols(&mut invalid).is_err());
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
        assert!(normalize_subprotocols(&mut headers).is_err());
        assert_eq!(headers, before);
    }

    #[test]
    fn extended_connect_requires_http2_connect_and_websocket_protocol() {
        for version in [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2] {
            for method in [Method::GET, Method::POST, Method::CONNECT] {
                for protocol in [None, Some("websocket"), Some("other")] {
                    let mut req = Request::builder()
                        .version(version)
                        .method(method.clone())
                        .uri("/")
                        .body(())
                        .unwrap();
                    if let Some(protocol) = protocol {
                        req.extensions_mut()
                            .insert(hyper::ext::Protocol::from_static(protocol));
                    }
                    let extended = version == Version::HTTP_2
                        && method == Method::CONNECT
                        && protocol == Some("websocket");
                    assert_eq!(
                        WsMode::classify(&req),
                        extended.then_some(WsMode::ExtendedConnect)
                    );
                    req.headers_mut()
                        .insert("connection", "Upgrade".parse().unwrap());
                    req.headers_mut()
                        .insert("upgrade", "websocket".parse().unwrap());
                    assert_eq!(
                        WsMode::classify(&req),
                        Some(if extended {
                            WsMode::ExtendedConnect
                        } else {
                            WsMode::Upgrade
                        })
                    );
                }
            }
        }
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
            if mode == WsMode::Upgrade {
                req.headers_mut().insert(
                    "sec-websocket-key",
                    "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
                );
            }
            let resp = mode.response(&mut req, false).unwrap();
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
            let ws_req = build_ws_request(req, uri, &upstream, mode);
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
            assert!(h.contains_key("sec-websocket-key"));
            assert_eq!(h["host"], "127.0.0.1:8546");
            assert_eq!(h["authorization"], "Bearer token");
            assert_eq!(
                ws_req.uri().path_and_query().unwrap().as_str(),
                "/ws/sub?api_key=secret&tag=1"
            );
        }
    }
}
