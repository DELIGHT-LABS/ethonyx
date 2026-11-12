//! WebSocket handshake and bounded relay. HTTP/1 and HTTP/2 share the same dial path.
use crate::lifecycle::Lifecycle;
use crate::proxy::{BoxError, ResBody, box_full, text_response};
use crate::rpc::{MockResult, mock_syncing};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use http::{Method, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::collections::HashSet;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Error, Message, protocol::Role};
use tracing::debug;

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

    fn response(self, req: &mut Request<()>) -> Result<Response<()>, BoxError> {
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
            let mut keys = req.headers().get_all(SEC_WEBSOCKET_KEY).iter();
            let Some(key) = keys.next().cloned() else {
                return Err("invalid websocket key count".into());
            };
            if keys.next().is_some() {
                return Err("invalid websocket key count".into());
            }
            Some(key)
        } else {
            None
        };
        normalize_subprotocols(req.headers_mut())?;
        let protocol = req.headers().get(SEC_WEBSOCKET_PROTOCOL).cloned();
        // Strip hop-by-hop and Connection-nominated headers before overwriting
        // Connection/Upgrade, then restore canonical WebSocket handshake fields.
        crate::headers::strip_hop_by_hop(req.headers_mut());
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
    fn contains(headers: &http::HeaderMap, name: http::HeaderName, token: &str) -> bool {
        headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case(token))
    }
    contains(req.headers(), http::header::CONNECTION, "upgrade")
        && contains(req.headers(), http::header::UPGRADE, "websocket")
}

// HeaderMap::get sees only one field line, as does Tungstenite's offer parser.
// Preserve the complete ordered offer and validate each case-sensitive token.
fn normalize_subprotocols(headers: &mut http::HeaderMap) -> Result<(), BoxError> {
    use http::header::SEC_WEBSOCKET_PROTOCOL;
    let mut protocols = String::new();
    // Borrow tokens from the original headers. The set only checks uniqueness;
    // appending below retains offer order and case without a quadratic scan.
    let mut seen = HashSet::new();
    for value in headers.get_all(SEC_WEBSOCKET_PROTOCOL) {
        for token in value.to_str()?.split(',') {
            let token = token.trim_matches([' ', '\t']);
            if token.is_empty()
                || !token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
                || !seen.insert(token)
            {
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
    upstream: &crate::upstream::WsUpstream,
    mode: WsMode,
) -> Result<Request<()>, BoxError> {
    let inbound_uri = std::mem::take(req.uri_mut());
    let uri = upstream.rewrite_uri(inbound_uri)?;
    req.headers_mut()
        .insert(http::header::HOST, upstream.host_header().clone());
    // RFC 8441 replaces the client's key/accept exchange with :protocol.
    // Generate a fresh key only for the separate HTTP/1 upstream handshake.
    if mode == WsMode::ExtendedConnect {
        let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
        req.headers_mut().insert(
            http::header::SEC_WEBSOCKET_KEY,
            http::HeaderValue::from_str(&key)?,
        );
    }
    *req.uri_mut() = uri;
    *req.method_mut() = Method::GET;
    *req.version_mut() = Version::HTTP_11;
    // OnUpgrade was taken before consuming the request. Move its headers rather
    // than cloning the map, and reuse the parsed URI for the upstream request.
    req.extensions_mut().clear();
    Ok(req)
}

fn get_ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(16 * 1024)
        .max_message_size(Some(64 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
}

pub(crate) async fn upgrade(
    req: Request<Incoming>,
    mode: WsMode,
    upstream: &crate::upstream::WsUpstream,
    connect_timeout: Duration,
    mock_enabled: bool,
    lifecycle: Lifecycle,
    permit: Option<std::sync::Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> Result<Response<ResBody>, BoxError> {
    let (parts, _body) = req.into_parts();
    let mut req = Request::from_parts(parts, ());
    let mut response = match mode.response(&mut req) {
        Ok(response) => response,
        Err(_) => {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "invalid websocket upgrade",
            ));
        }
    };
    let on_upgrade = hyper::upgrade::on(&mut req);
    let ws_req = build_ws_request(req, upstream, mode)?;
    let (upstream_ws, upstream_response) = match tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async_with_config(ws_req, Some(get_ws_config()), true),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            debug!(error = %e, "upstream ws connect failed");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect failed",
            ));
        }
        Err(_) => {
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
        return Ok(text_response(
            StatusCode::BAD_GATEWAY,
            "invalid upstream websocket negotiation",
        ));
    }
    configure_ws_keepalive(&upstream_ws);
    // Tungstenite checks that the selected protocol was offered by the client.
    if let Some(protocol) = upstream_response
        .headers()
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
    {
        response
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, protocol.clone());
    }
    tokio::spawn(lifecycle.run(async move {
        let _permit = permit;
        match on_upgrade.await {
            Ok(upgraded) => {
                let client_ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(get_ws_config()),
                )
                .await;
                bridge_ws(client_ws, upstream_ws, mock_enabled).await;
            }
            Err(e) => debug!(error = %e, "client ws upgrade failed"),
        }
    }));
    Ok(response.map(|()| box_full(http_body_util::Full::new(bytes::Bytes::new()))))
}

async fn forward<R, W>(mut reader: R, mut writer: W)
where
    R: Stream<Item = Result<Message, Error>> + Unpin,
    W: Sink<Message, Error = Error> + Unpin,
{
    while let Some(Ok(message)) = reader.next().await {
        let closing = message.is_close();
        let sent = writer.send(message).await;
        if closing {
            // Reading once more flushes Tungstenite's queued close acknowledgement
            // on this reader's socket without adding a shared sink mutex.
            let _ = tokio::time::timeout(Duration::from_secs(1), reader.next()).await;
            break;
        }
        if sent.is_err() {
            break;
        }
    }
}

async fn bridge_ws<C, U>(
    client_ws: tokio_tungstenite::WebSocketStream<C>,
    upstream_ws: tokio_tungstenite::WebSocketStream<U>,
    mock_enabled: bool,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _guard = crate::metrics::ActiveConnectionGuard::ws();
    let (c_tx, mut c_rx) = client_ws.split();
    let (mut u_tx, mut u_rx) = upstream_ws.split();
    if !mock_enabled {
        drain_pair(forward(c_rx, u_tx), forward(u_rx, c_tx)).await;
        return;
    }
    let c_tx = tokio::sync::Mutex::new(c_tx);

    let c2u = async {
        while let Some(Ok(m)) = c_rx.next().await {
            if let Message::Text(text) = &m {
                match mock_syncing(text.as_bytes()) {
                    MockResult::Silent => continue,
                    MockResult::Reply(reply) => {
                        if c_tx
                            .lock()
                            .await
                            .send(Message::Text(reply.into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    MockResult::Forward => {}
                }
            }
            let closing = m.is_close();
            let sent = u_tx.send(m).await;
            if closing {
                let _ = tokio::time::timeout(Duration::from_secs(1), c_rx.next()).await;
                break;
            }
            if sent.is_err() {
                break;
            }
        }
    };
    let u2c = async {
        while let Some(Ok(m)) = u_rx.next().await {
            let closing = m.is_close();
            let sent = c_tx.lock().await.send(m).await;
            if closing {
                let _ = tokio::time::timeout(Duration::from_secs(1), u_rx.next()).await;
                break;
            }
            if sent.is_err() {
                break;
            }
        }
    };
    drain_pair(c2u, u2c).await;
}

async fn drain_pair(
    a: impl std::future::Future<Output = ()>,
    b: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(a, b);
    // Give the opposite direction time to flush its close reply; a broken peer
    // must not keep the other half of a tunnel alive indefinitely.
    let a_finished = tokio::select! { _ = &mut a => true, _ = &mut b => false };
    let grace = Duration::from_secs(1);
    if a_finished {
        let _ = tokio::time::timeout(grace, b).await;
    } else {
        let _ = tokio::time::timeout(grace, a).await;
    }
}

fn configure_ws_keepalive(
    ws: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let inner = ws.get_ref();
    let tcp_stream = match inner {
        tokio_tungstenite::MaybeTlsStream::Plain(s) => Some(s),
        tokio_tungstenite::MaybeTlsStream::Rustls(s) => Some(s.get_ref().0),
        _ => None,
    };
    if let Some(stream) = tcp_stream {
        let sock_ref = socket2::SockRef::from(stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10));
        if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
            debug!(error = %e, "failed to set upstream WS TCP keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_offers_preserve_all_lines_and_reject_invalid_tokens() {
        for (values, expected) in [
            (vec![], Some("")),
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
            let resp = mode.response(&mut req).unwrap();
            assert_eq!(
                resp.status(),
                if mode == WsMode::Upgrade {
                    StatusCode::SWITCHING_PROTOCOLS
                } else {
                    StatusCode::OK
                }
            );
            let ws_req = build_ws_request(req, &upstream, mode).unwrap();
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
