//! WebSocket handshake and bounded full-duplex single-task relay. HTTP/1 and HTTP/2 share the same dial path.
use crate::proxy::{BoxError, Connection, ResBody, box_full, text_response};
use crate::rpc::{MockResult, mock_syncing};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use http::{Method, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, Role},
};
use tracing::{debug, warn};

const WS_CLOSE_GRACE: Duration = Duration::from_secs(1);
const WS_CONTROL_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BATCH_FRAMES: usize = 32;

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
        #[cfg(not(feature = "otel"))]
        let _ = otel_enabled;
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
        #[cfg(feature = "otel")]
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
            http::HeaderValue::try_from(key)?,
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
    connection: &Arc<Connection>,
) -> Result<Response<ResBody>, BoxError> {
    let state = &connection.state;
    let (parts, _body) = req.into_parts();
    let mut req = Request::from_parts(parts, ());
    let mut response = match mode.response(&mut req, state.otel_enabled) {
        Ok(response) => response,
        Err(_) => {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "invalid websocket upgrade",
            ));
        }
    };
    let Some(permit) = connection.acquire_ws_permit(mode) else {
        return Ok(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "connection limit reached",
        ));
    };
    let on_upgrade = hyper::upgrade::on(&mut req);
    let ws_req = build_ws_request(req, &state.el_ws, mode)?;
    let io_timeout = state.cfg.proxy_timeout;
    let (upstream_ws, mut upstream_response) = match tokio::time::timeout(
        io_timeout,
        tokio_tungstenite::connect_async_with_config(ws_req, Some(get_ws_config()), true),
    )
    .await
    {
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
    configure_ws_keepalive(&upstream_ws);
    // Tungstenite checks that the selected protocol was offered by the client.
    if let Some(protocol) = upstream_response
        .headers_mut()
        .remove(http::header::SEC_WEBSOCKET_PROTOCOL)
    {
        response
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, protocol);
    }
    let lifecycle = connection.lifecycle.clone();
    let mock_enabled = state.cfg.mock_eth_syncing;
    tokio::spawn(lifecycle.run(async move {
        let _permit = permit;
        match on_upgrade.await {
            Ok(upgraded) => {
                let client_ws = WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(get_ws_config()),
                )
                .await;
                bridge_ws(client_ws, upstream_ws, mock_enabled, io_timeout).await;
            }
            Err(e) => debug!(error = %e, "client ws upgrade failed"),
        }
    }));
    Ok(response.map(|()| box_full(http_body_util::Full::new(bytes::Bytes::new()))))
}

async fn send_bounded<S>(ws: &mut WebSocketStream<S>, msg: Message, timeout: Duration) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, ws.send(msg))
        .await
        .is_ok_and(|res| res.is_ok())
}

async fn flush_bounded<S>(ws: &mut WebSocketStream<S>, timeout: Duration) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, ws.flush())
        .await
        .is_ok_and(|res| res.is_ok())
}

/// Non-blocking full-duplex WebSocket endpoint state with a 1-slot outbound
/// backpressure queue and lazy stall timer.
struct WsEndpoint<S> {
    ws: WebSocketStream<S>,
    queued: Option<Message>,
    needs_flush: bool,
    write_deadline: Option<Pin<Box<Sleep>>>,
    timer_armed: bool,
    pending_timeout: Duration,
}

impl<S> WsEndpoint<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    const fn new(ws: WebSocketStream<S>, default_timeout: Duration) -> Self {
        Self {
            ws,
            queued: None,
            needs_flush: false,
            write_deadline: None,
            timer_armed: false,
            pending_timeout: default_timeout,
        }
    }

    const fn can_accept(&self) -> bool {
        self.queued.is_none()
    }

    fn enqueue(&mut self, msg: Message, timeout: Duration) {
        debug_assert!(self.queued.is_none());
        if !self.needs_flush {
            self.pending_timeout = timeout;
        }
        self.queued = Some(msg);
    }

    fn request_control_flush(&mut self) {
        if self.needs_flush || self.queued.is_some() {
            self.pending_timeout = self.pending_timeout.min(WS_CONTROL_FLUSH_TIMEOUT);
        } else {
            self.pending_timeout = WS_CONTROL_FLUSH_TIMEOUT;
        }
        self.needs_flush = true;
    }

    fn poll_send_queued(&mut self, cx: &mut Context<'_>) -> Result<(), ()> {
        if self.queued.is_none() {
            return Ok(());
        }
        match Pin::new(&mut self.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = self.queued.take().expect("queued checked");
                Pin::new(&mut self.ws).start_send(msg).map_err(|_| ())?;
                self.needs_flush = true;
                Ok(())
            }
            Poll::Ready(Err(_)) => Err(()),
            Poll::Pending => self.poll_write_timer(cx),
        }
    }

    fn poll_flush_and_timer(&mut self, cx: &mut Context<'_>) -> Result<(), ()> {
        if self.queued.is_some() {
            return self.poll_write_timer(cx);
        }
        if !self.needs_flush {
            self.timer_armed = false;
            return Ok(());
        }
        match Pin::new(&mut self.ws).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.needs_flush = false;
                self.timer_armed = false;
                Ok(())
            }
            Poll::Ready(Err(_)) => Err(()),
            Poll::Pending => self.poll_write_timer(cx),
        }
    }

    fn poll_write_timer(&mut self, cx: &mut Context<'_>) -> Result<(), ()> {
        let desired = tokio::time::Instant::now() + self.pending_timeout;
        if !self.timer_armed {
            match self.write_deadline.as_mut() {
                Some(sleep) => sleep.as_mut().reset(desired),
                None => self.write_deadline = Some(Box::pin(tokio::time::sleep_until(desired))),
            }
            self.timer_armed = true;
        } else if let Some(sleep) = self.write_deadline.as_mut()
            && desired < sleep.deadline()
        {
            sleep.as_mut().reset(desired);
        }
        if self
            .write_deadline
            .as_mut()
            .expect("write_deadline initialized when armed")
            .as_mut()
            .poll(cx)
            .is_ready()
        {
            self.timer_armed = false;
            Err(())
        } else {
            Ok(())
        }
    }

    async fn drain_pending(&mut self, extra: Option<Message>, timeout: Duration) -> bool {
        if let Some(msg) = self.queued.take() {
            if !send_bounded(&mut self.ws, msg, timeout).await {
                return false;
            }
            self.needs_flush = false;
        }
        if let Some(msg) = extra {
            let ok = send_bounded(&mut self.ws, msg, timeout).await;
            if ok {
                self.needs_flush = false;
            }
            return ok;
        }
        if self.needs_flush {
            self.needs_flush = false;
            return flush_bounded(&mut self.ws, timeout).await;
        }
        true
    }
}

enum BridgeExit {
    ClientClosed(Option<CloseFrame>),
    UpstreamClosed(Option<CloseFrame>),
    ClientEof,
    UpstreamEof,
    ClientError,
    UpstreamError,
    Abort,
}

async fn finish_close<A, B>(
    closer: &mut WsEndpoint<A>,
    other: &mut WsEndpoint<B>,
    extra_for_other: Option<Message>,
    frame: Option<CloseFrame>,
) where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Flush Tungstenite's auto-queued Close reply (and any queued frame) to the
    // initiator first, then drain queued frames + Close to the opposite peer.
    closer.needs_flush = true;
    let _ = closer.drain_pending(None, WS_CLOSE_GRACE).await;
    if other.drain_pending(extra_for_other, WS_CLOSE_GRACE).await
        && send_bounded(&mut other.ws, Message::Close(frame), WS_CLOSE_GRACE).await
    {
        let _ = tokio::time::timeout(WS_CLOSE_GRACE, async {
            while let Some(Ok(msg)) = other.ws.next().await {
                if msg.is_close() {
                    break;
                }
            }
        })
        .await;
    }
}

async fn bridge_ws<C, U>(
    client_ws: WebSocketStream<C>,
    upstream_ws: WebSocketStream<U>,
    mock_enabled: bool,
    io_timeout: Duration,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _guard = crate::metrics::ActiveConnectionGuard::ws();
    let mut client = WsEndpoint::new(client_ws, io_timeout);
    let mut upstream = WsEndpoint::new(upstream_ws, io_timeout);
    // Dedicated 1-slot buffer for local `eth_syncing` replies so `client -> upstream`
    // forwarding never stalls behind `upstream -> client` write backpressure.
    let mut pending_mock_reply: Option<Message> = None;

    let exit = std::future::poll_fn(|cx| {
        let mut hit_budget = false;

        // 1. Advance any already-queued outbound frames first so freed slots are
        // immediately visible to both directional read loops below.
        if upstream.poll_send_queued(cx).is_err() || client.poll_send_queued(cx).is_err() {
            return Poll::Ready(BridgeExit::Abort);
        }
        if client.can_accept()
            && let Some(reply) = pending_mock_reply.take()
        {
            client.enqueue(reply, io_timeout);
            if client.poll_send_queued(cx).is_err() {
                return Poll::Ready(BridgeExit::Abort);
            }
        }

        // 2. Client -> Upstream (bounded batch per poll turn for scheduler fairness).
        let mut c2u_count = 0;
        while c2u_count < MAX_BATCH_FRAMES {
            if !upstream.can_accept() || pending_mock_reply.is_some() {
                break;
            }
            match Pin::new(&mut client.ws).poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(BridgeExit::ClientEof),
                Poll::Ready(Some(Err(_))) => return Poll::Ready(BridgeExit::ClientError),
                Poll::Ready(Some(Ok(msg))) => {
                    c2u_count += 1;
                    match msg {
                        Message::Text(text) => {
                            if mock_enabled {
                                match mock_syncing(text.as_bytes()) {
                                    MockResult::Silent => continue,
                                    MockResult::Reply(reply) => {
                                        let reply_msg = Message::Text(reply.into());
                                        if client.can_accept() {
                                            client.enqueue(reply_msg, io_timeout);
                                            if client.poll_send_queued(cx).is_err() {
                                                return Poll::Ready(BridgeExit::Abort);
                                            }
                                        } else {
                                            pending_mock_reply = Some(reply_msg);
                                        }
                                        continue;
                                    }
                                    MockResult::Forward => {}
                                }
                            }
                            upstream.enqueue(Message::Text(text), io_timeout);
                            if upstream.poll_send_queued(cx).is_err() {
                                return Poll::Ready(BridgeExit::Abort);
                            }
                        }
                        Message::Binary(bin) => {
                            upstream.enqueue(Message::Binary(bin), io_timeout);
                            if upstream.poll_send_queued(cx).is_err() {
                                return Poll::Ready(BridgeExit::Abort);
                            }
                        }
                        Message::Ping(_) => {
                            // Ping/Pong are hop-by-hop control frames: Tungstenite queued
                            // the matching Pong on `client.ws` during `poll_next`.
                            client.request_control_flush();
                        }
                        Message::Pong(_) | Message::Frame(_) => {}
                        Message::Close(frame) => {
                            return Poll::Ready(BridgeExit::ClientClosed(frame));
                        }
                    }
                }
            }
        }
        if c2u_count == MAX_BATCH_FRAMES {
            hit_budget = true;
        }

        // 3. Upstream -> Client (independent full-duplex progress even when `upstream` sink is full).
        let mut u2c_count = 0;
        while u2c_count < MAX_BATCH_FRAMES {
            if !client.can_accept() {
                break;
            }
            match Pin::new(&mut upstream.ws).poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(BridgeExit::UpstreamEof),
                Poll::Ready(Some(Err(_))) => return Poll::Ready(BridgeExit::UpstreamError),
                Poll::Ready(Some(Ok(msg))) => {
                    u2c_count += 1;
                    match msg {
                        Message::Text(text) => {
                            client.enqueue(Message::Text(text), io_timeout);
                            if client.poll_send_queued(cx).is_err() {
                                return Poll::Ready(BridgeExit::Abort);
                            }
                        }
                        Message::Binary(bin) => {
                            client.enqueue(Message::Binary(bin), io_timeout);
                            if client.poll_send_queued(cx).is_err() {
                                return Poll::Ready(BridgeExit::Abort);
                            }
                        }
                        Message::Ping(_) => {
                            upstream.request_control_flush();
                        }
                        Message::Pong(_) | Message::Frame(_) => {}
                        Message::Close(frame) => {
                            return Poll::Ready(BridgeExit::UpstreamClosed(frame));
                        }
                    }
                }
            }
        }
        if u2c_count == MAX_BATCH_FRAMES {
            hit_budget = true;
        }

        if client.can_accept()
            && let Some(reply) = pending_mock_reply.take()
        {
            client.enqueue(reply, io_timeout);
            if client.poll_send_queued(cx).is_err() {
                return Poll::Ready(BridgeExit::Abort);
            }
        }

        // 4. Flush batched outbound frames once per direction & enforce stall deadlines.
        if upstream.poll_flush_and_timer(cx).is_err() || client.poll_flush_and_timer(cx).is_err() {
            return Poll::Ready(BridgeExit::Abort);
        }

        if hit_budget {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    })
    .await;

    match exit {
        BridgeExit::ClientClosed(frame) => {
            finish_close(&mut client, &mut upstream, None, frame).await;
        }
        BridgeExit::UpstreamClosed(frame) => {
            finish_close(&mut upstream, &mut client, pending_mock_reply, frame).await;
        }
        BridgeExit::ClientEof => {
            let _ = upstream.drain_pending(None, WS_CLOSE_GRACE).await;
        }
        BridgeExit::UpstreamEof => {
            let _ = client
                .drain_pending(pending_mock_reply, WS_CLOSE_GRACE)
                .await;
        }
        BridgeExit::ClientError => {
            let _ = upstream.drain_pending(None, WS_CLOSE_GRACE).await;
            let _ = flush_bounded(&mut client.ws, WS_CLOSE_GRACE).await;
        }
        BridgeExit::UpstreamError => {
            let _ = client
                .drain_pending(pending_mock_reply, WS_CLOSE_GRACE)
                .await;
            let _ = flush_bounded(&mut upstream.ws, WS_CLOSE_GRACE).await;
        }
        BridgeExit::Abort => {}
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
            let resp = mode.response(&mut req, false).unwrap();
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

    #[tokio::test]
    async fn single_loop_bridge_flushes_ping_locally_without_forwarding_or_duplicating_pong() {
        let (client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, upstream_io) = tokio::io::duplex(16 * 1024);

        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client =
            WebSocketStream::from_raw_socket(proxy_client_io, Role::Server, Some(get_ws_config()))
                .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            proxy_upstream_io,
            Role::Client,
            Some(get_ws_config()),
        )
        .await;
        let mut upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;

        let relay = tokio::spawn(bridge_ws(
            proxy_client,
            proxy_upstream,
            true,
            Duration::from_secs(5),
        ));

        // 1. Client Ping while upstream is completely idle (not even reading!) must
        // immediately yield 1 Pong to the client and never forward Ping to upstream.
        client
            .send(Message::Ping(bytes::Bytes::from_static(b"hb-1")))
            .await
            .unwrap();
        let pong = tokio::time::timeout(Duration::from_secs(1), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(pong, Message::Pong(bytes::Bytes::from_static(b"hb-1")));

        // 2. Unsolicited Pong from client is absorbed locally.
        client
            .send(Message::Pong(bytes::Bytes::from_static(b"unsolicited")))
            .await
            .unwrap();

        // 3. Mocked eth_syncing replies locally without touching upstream.
        client
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","method":"eth_syncing","id":9}"#.into(),
            ))
            .await
            .unwrap();
        let mock_reply = client.next().await.unwrap().unwrap();
        assert_eq!(
            mock_reply,
            Message::Text(r#"{"jsonrpc":"2.0","id":9,"result":false}"#.into())
        );

        // 4. Non-eth_syncing message is the VERY FIRST frame upstream sees!
        client
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#.into(),
            ))
            .await
            .unwrap();
        let seen_by_upstream = upstream.next().await.unwrap().unwrap();
        assert_eq!(
            seen_by_upstream,
            Message::Text(r#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#.into())
        );
        upstream
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#.into(),
            ))
            .await
            .unwrap();
        let forwarded_reply = client.next().await.unwrap().unwrap();
        assert_eq!(
            forwarded_reply,
            Message::Text(r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#.into())
        );

        // 5. Orderly close completes with zero extra Pong frames.
        client.close(None).await.unwrap();
        let close_reply = client.next().await.unwrap().unwrap();
        assert!(matches!(close_reply, Message::Close(_)), "{close_reply:?}");
        assert!(matches!(
            upstream.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
        // Poll once more (as a server read loop does) so Tungstenite flushes its Close-ack.
        assert!(upstream.next().await.is_none());
        relay.await.unwrap();
    }

    #[tokio::test]
    async fn pipelined_and_bidirectional_bursts_complete_without_deadlock() {
        for mock_enabled in [false, true] {
            // Scenario A: Client pipelines 64 x 16 KiB (1 MiB) requests before reading a
            // single reply over 64 KiB duplexes while upstream replies 2 KiB per request
            // (128 KiB total > 64 KiB c2p duplex, saturating the outbound client sink).
            let (client_io, proxy_client_io) = tokio::io::duplex(64 * 1024);
            let (proxy_upstream_io, upstream_io) = tokio::io::duplex(64 * 1024);

            let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            let proxy_client = WebSocketStream::from_raw_socket(
                proxy_client_io,
                Role::Server,
                Some(get_ws_config()),
            )
            .await;
            let proxy_upstream = WebSocketStream::from_raw_socket(
                proxy_upstream_io,
                Role::Client,
                Some(get_ws_config()),
            )
            .await;
            let mut upstream =
                WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;

            let relay = tokio::spawn(bridge_ws(
                proxy_client,
                proxy_upstream,
                mock_enabled,
                Duration::from_secs(5),
            ));
            let upstream_task = tokio::spawn(async move {
                let reply_pad = "r".repeat(2048);
                while let Some(Ok(msg)) = upstream.next().await {
                    if msg.is_close() {
                        break;
                    }
                    upstream
                        .send(Message::Text(reply_pad.clone().into()))
                        .await
                        .unwrap();
                }
            });

            let req_payload = format!(
                r#"{{"jsonrpc":"2.0","method":"eth_call","params":["{}"],"id":1}}"#,
                "a".repeat(16 * 1024)
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                for i in 0..64 {
                    if mock_enabled && i == 40 {
                        // Interleave a local eth_syncing reply while client sink is already full!
                        client
                            .send(Message::Text(
                                r#"{"jsonrpc":"2.0","method":"eth_syncing","id":999}"#.into(),
                            ))
                            .await
                            .unwrap();
                    }
                    client
                        .send(Message::Text(req_payload.clone().into()))
                        .await
                        .unwrap();
                }
                let expected_replies = 64 + usize::from(mock_enabled);
                for _ in 0..expected_replies {
                    let msg = client.next().await.unwrap().unwrap();
                    assert!(matches!(msg, Message::Text(_)));
                }
                client.close(None).await.unwrap();
                while let Some(Ok(msg)) = client.next().await {
                    if msg.is_close() {
                        break;
                    }
                }
            })
            .await
            .expect("pipelined burst must not deadlock");

            upstream_task.await.unwrap();
            relay.await.unwrap();

            // Scenario B: Simultaneous 96 KiB bursts (> 64 KiB duplex) from BOTH client
            // and upstream before either side begins reading.
            let (client_io, proxy_client_io) = tokio::io::duplex(64 * 1024);
            let (proxy_upstream_io, upstream_io) = tokio::io::duplex(64 * 1024);
            let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            let proxy_client = WebSocketStream::from_raw_socket(
                proxy_client_io,
                Role::Server,
                Some(get_ws_config()),
            )
            .await;
            let proxy_upstream = WebSocketStream::from_raw_socket(
                proxy_upstream_io,
                Role::Client,
                Some(get_ws_config()),
            )
            .await;
            let mut upstream =
                WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;

            let relay = tokio::spawn(bridge_ws(
                proxy_client,
                proxy_upstream,
                mock_enabled,
                Duration::from_secs(5),
            ));
            let chunk = bytes::Bytes::from(vec![b'x'; 16 * 1024]);
            let up_chunk = chunk.clone();
            let upstream_burst = tokio::spawn(async move {
                for _ in 0..6 {
                    upstream
                        .send(Message::Binary(up_chunk.clone()))
                        .await
                        .unwrap();
                }
                for _ in 0..6 {
                    let msg = upstream.next().await.unwrap().unwrap();
                    assert!(matches!(msg, Message::Binary(b) if b.len() == 16 * 1024));
                }
            });

            tokio::time::timeout(Duration::from_secs(5), async {
                for _ in 0..6 {
                    client.send(Message::Binary(chunk.clone())).await.unwrap();
                }
                for _ in 0..6 {
                    let msg = client.next().await.unwrap().unwrap();
                    assert!(matches!(msg, Message::Binary(b) if b.len() == 16 * 1024));
                }
                upstream_burst.await.unwrap();
            })
            .await
            .expect("simultaneous bidirectional burst must not deadlock");
            drop(client);
            relay.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_sink_deadline_is_not_extended_by_trickle_frames() {
        // Tiny 256-byte duplex so a single 1 KiB frame blocks inside `poll_flush`.
        let (client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, _stalled_upstream_io) = tokio::io::duplex(256);

        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client =
            WebSocketStream::from_raw_socket(proxy_client_io, Role::Server, Some(get_ws_config()))
                .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            proxy_upstream_io,
            Role::Client,
            Some(get_ws_config()),
        )
        .await;

        let relay = tokio::spawn(bridge_ws(
            proxy_client,
            proxy_upstream,
            false,
            Duration::from_secs(10),
        ));

        // Frame 1 (1 KiB) fits in Tungstenite's 16 KiB write_buffer, blocks in poll_flush,
        // and arms the 10s deadline at t = 0.
        client
            .send(Message::Binary(bytes::Bytes::from(vec![1u8; 1024])))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Trickle tiny frames at t = 4s and t = 8s; deadline must stay at t = 10s!
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(4)).await;
            client
                .send(Message::Binary(bytes::Bytes::from_static(b"trickle")))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            assert!(!relay.is_finished());
        }

        // Advancing 3s more (t = 11s > 10s, even though only 3s since last trickle) aborts!
        tokio::time::advance(Duration::from_secs(3)).await;
        relay.await.unwrap();
    }
}
