//! WebSocket handshake and bounded full-duplex single-task relay. HTTP/1 and HTTP/2 share the same dial path.
use crate::lifecycle::LazyDeadline;
use crate::proxy::{BoxError, Connection, ResBody, box_full, text_response, uri_too_long_response};
use crate::rpc::{MockResult, mock_syncing};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use http::{Method, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, Role},
};
use tracing::{debug, warn};

const WS_CLOSE_GRACE: Duration = Duration::from_secs(1);
const WS_CONTROL_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BATCH_FRAMES: usize = 32;
const MAX_WRITE_CHUNK: usize = 64 * 1024;
const MIN_WRITE_PROGRESS_BYTES: usize = 4 * 1024;

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
    upstream: &crate::upstream::WsUpstream,
    mode: WsMode,
) -> Result<Request<()>, http::Error> {
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
            http::HeaderValue::try_from(key).expect("base64 key is valid header value"),
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
        .max_write_buffer_size(64 * 1024 * 1024 + 64 * 1024)
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
    let on_upgrade = hyper::upgrade::on(&mut req);
    let Ok(ws_req) = build_ws_request(req, &state.el_ws, mode) else {
        return Ok(uri_too_long_response());
    };
    let Some(permit) = connection.acquire_ws_permit(mode) else {
        return Ok(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "connection limit reached",
        ));
    };
    let io_timeout = state.cfg.proxy_timeout;
    let dial = async {
        let tcp =
            tokio::net::TcpStream::connect((state.el_ws.dial_host(), state.el_ws.dial_port()))
                .await
                .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
        crate::configure_tcp_stream(&tcp);
        tokio_tungstenite::client_async_tls_with_config(
            ws_req,
            WriteProgressIo::new(tcp),
            Some(get_ws_config()),
            Some(state.ws_tls.clone()),
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
    let lifecycle = connection.lifecycle.clone();
    let mock_enabled = state.cfg.mock_eth_syncing;
    let state = Arc::clone(state);
    tokio::spawn(lifecycle.run(async move {
        let _permit = permit;
        match on_upgrade.await {
            Ok(upgraded) => {
                let _ws_guard = state.metrics.active_ws();
                let client_ws = WebSocketStream::from_raw_socket(
                    WriteProgressIo::new(TokioIo::new(upgraded)),
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

/// Caps single writes to [`MAX_WRITE_CHUNK`] (so HTTP/2 Extended `CONNECT`'s
/// `Upgrade` bridge cannot swallow a multi-MiB frame in one `Box<[u8]>` and
/// starve progress tracking) and accumulates written bytes so slow-progressing
/// peers (`>= MIN_WRITE_PROGRESS_BYTES` per window) survive while `0`-byte or
/// sub-threshold Slowloris drains abort on deadline.
struct WriteProgressIo<I> {
    inner: I,
    written: usize,
}

impl<I> WriteProgressIo<I> {
    const fn new(inner: I) -> Self {
        Self { inner, written: 0 }
    }
}

impl<I: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for WriteProgressIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<I: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for WriteProgressIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let capped = &buf[..buf.len().min(MAX_WRITE_CHUNK)];
        let poll = Pin::new(&mut self.inner).poll_write(cx, capped);
        if let Poll::Ready(Ok(n)) = poll {
            self.written = self.written.saturating_add(n);
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

trait HasWriteProgress {
    fn take_write_progress(&mut self, min_bytes: usize) -> bool;
    fn reset_write_progress(&mut self);
}

impl<I> HasWriteProgress for WriteProgressIo<I> {
    fn take_write_progress(&mut self, min_bytes: usize) -> bool {
        if self.written >= min_bytes {
            self.written = 0;
            true
        } else {
            false
        }
    }

    fn reset_write_progress(&mut self) {
        self.written = 0;
    }
}

impl<S: HasWriteProgress> HasWriteProgress for tokio_tungstenite::MaybeTlsStream<S> {
    fn take_write_progress(&mut self, min_bytes: usize) -> bool {
        match self {
            Self::Plain(s) => s.take_write_progress(min_bytes),
            Self::Rustls(tls) => tls.get_mut().0.take_write_progress(min_bytes),
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }

    fn reset_write_progress(&mut self) {
        match self {
            Self::Plain(s) => s.reset_write_progress(),
            Self::Rustls(tls) => tls.get_mut().0.reset_write_progress(),
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }
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

enum OutboundState {
    Idle,
    ControlFlush,
    DataActive { queued: Option<Message> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Client,
    Upstream,
}

enum Stop {
    Closed(Option<CloseFrame>),
    Eof,
    Failed,
    Abort,
}

/// Non-blocking full-duplex WebSocket endpoint state with a 1-slot outbound
/// backpressure queue, 1-slot local mock-reply buffer, and progress-based stall timer.
struct WsEndpoint<S> {
    ws: WebSocketStream<S>,
    side: Side,
    outbound: OutboundState,
    local_reply: Option<Message>,
    deadline: LazyDeadline,
    io_timeout: Duration,
}

impl<S> WsEndpoint<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress,
{
    const fn new(ws: WebSocketStream<S>, side: Side, io_timeout: Duration) -> Self {
        Self {
            ws,
            side,
            outbound: OutboundState::Idle,
            local_reply: None,
            deadline: LazyDeadline::new(),
            io_timeout,
        }
    }

    const fn can_accept(&self) -> bool {
        !matches!(self.outbound, OutboundState::DataActive { queued: Some(_) })
    }

    fn enqueue_data(&mut self, msg: Message) {
        debug_assert!(self.can_accept());
        if matches!(self.outbound, OutboundState::ControlFlush) {
            // Upgrade from 5s control flush to full data `io_timeout`.
            self.deadline.disarm();
        }
        self.outbound = OutboundState::DataActive { queued: Some(msg) };
    }

    fn request_control_flush(&mut self) {
        // Never downgrade an active data transfer's `io_timeout` to 5s on `Ping`.
        if matches!(self.outbound, OutboundState::Idle) {
            self.outbound = OutboundState::ControlFlush;
        }
    }

    fn try_queue_idle_ping(
        &mut self,
        cx: &mut Context<'_>,
        payload: bytes::Bytes,
    ) -> Result<(), Stop> {
        if !matches!(self.outbound, OutboundState::Idle) || self.local_reply.is_some() {
            return Ok(());
        }
        if let Poll::Ready(res) = Pin::new(&mut self.ws).poll_ready(cx) {
            res.map_err(|_| Stop::Abort)?;
            Pin::new(&mut self.ws)
                .start_send(Message::Ping(payload))
                .map_err(|_| Stop::Abort)?;
            self.request_control_flush();
        }
        Ok(())
    }

    fn check_stall_deadline(
        &mut self,
        cx: &mut Context<'_>,
        timeout: Duration,
    ) -> Result<(), Stop> {
        if !self.deadline.is_armed() {
            // Discard initial buffer-fill bytes written before backpressure began.
            self.ws.get_mut().reset_write_progress();
        } else if self
            .ws
            .get_mut()
            .take_write_progress(MIN_WRITE_PROGRESS_BYTES)
        {
            self.deadline.disarm();
        }
        if self.deadline.poll_expired(cx, timeout) {
            debug!(
                side = ?self.side,
                timeout_secs = timeout.as_secs(),
                "ws write stalled; aborting relay"
            );
            Err(Stop::Abort)
        } else {
            Ok(())
        }
    }

    fn poll_send_queued(&mut self, cx: &mut Context<'_>) -> Result<(), Stop> {
        let OutboundState::DataActive { queued } = &mut self.outbound else {
            return Ok(());
        };
        if queued.is_none() {
            return Ok(());
        }
        match Pin::new(&mut self.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = queued.take().expect("queued checked");
                Pin::new(&mut self.ws)
                    .start_send(msg)
                    .map_err(|_| Stop::Abort)
            }
            Poll::Ready(Err(_)) => Err(Stop::Abort),
            // Defer stall-timer evaluation to Step 4 (`poll_flush_and_timer`),
            // which runs once per turn after both directional pumps finish.
            Poll::Pending => Ok(()),
        }
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Result<(), Stop> {
        self.poll_send_queued(cx)?;
        if self.can_accept()
            && let Some(reply) = self.local_reply.take()
        {
            self.enqueue_data(reply);
            self.poll_send_queued(cx)?;
        }
        Ok(())
    }

    fn poll_flush_and_timer(&mut self, cx: &mut Context<'_>) -> Result<(), Stop> {
        let timeout = match self.outbound {
            OutboundState::Idle => {
                self.deadline.disarm();
                return Ok(());
            }
            OutboundState::DataActive { queued: Some(_) } => {
                return self.check_stall_deadline(cx, self.io_timeout);
            }
            OutboundState::DataActive { queued: None } => self.io_timeout,
            OutboundState::ControlFlush => WS_CONTROL_FLUSH_TIMEOUT,
        };
        match Pin::new(&mut self.ws).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.outbound = OutboundState::Idle;
                self.ws.get_mut().reset_write_progress();
                self.deadline.disarm();
                Ok(())
            }
            Poll::Ready(Err(_)) => Err(Stop::Abort),
            Poll::Pending => self.check_stall_deadline(cx, timeout),
        }
    }

    async fn drain_pending(&mut self, timeout: Duration) -> bool {
        let was_active = !matches!(self.outbound, OutboundState::Idle);
        let queued = match std::mem::replace(&mut self.outbound, OutboundState::Idle) {
            OutboundState::DataActive { queued } => queued,
            OutboundState::Idle | OutboundState::ControlFlush => None,
        };
        let reply = self.local_reply.take();
        if !was_active && queued.is_none() && reply.is_none() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            for msg in queued.into_iter().chain(reply) {
                self.ws.feed(msg).await?;
            }
            self.ws.flush().await
        })
        .await
        .is_ok_and(|res| res.is_ok())
    }
}

fn pump<A, B>(
    cx: &mut Context<'_>,
    src: &mut WsEndpoint<A>,
    dst: &mut WsEndpoint<B>,
    intercept_mock: bool,
) -> Result<bool, Stop>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress,
{
    let mut count = 0;
    while count < MAX_BATCH_FRAMES {
        if !dst.can_accept() || src.local_reply.is_some() {
            break;
        }
        match Pin::new(&mut src.ws).poll_next(cx) {
            Poll::Pending => break,
            Poll::Ready(None) => return Err(Stop::Eof),
            Poll::Ready(Some(Err(_))) => return Err(Stop::Failed),
            Poll::Ready(Some(Ok(msg))) => {
                count += 1;
                match msg {
                    Message::Text(text) => {
                        if intercept_mock {
                            match mock_syncing(text.as_bytes()) {
                                MockResult::Silent => continue,
                                MockResult::Reply(reply) => {
                                    let reply_msg = Message::Text(reply.into());
                                    if src.can_accept() {
                                        src.enqueue_data(reply_msg);
                                        src.poll_send_queued(cx)?;
                                    } else {
                                        src.local_reply = Some(reply_msg);
                                    }
                                    continue;
                                }
                                MockResult::Forward => {}
                            }
                        }
                        dst.enqueue_data(Message::Text(text));
                        dst.poll_send_queued(cx)?;
                    }
                    Message::Binary(bin) => {
                        dst.enqueue_data(Message::Binary(bin));
                        dst.poll_send_queued(cx)?;
                    }
                    Message::Ping(payload) => {
                        // Ping/Pong are hop-by-hop control frames: Tungstenite queued
                        // the matching Pong on `src.ws` during `poll_next`.
                        src.request_control_flush();
                        // Forward upstream keepalive Pings to a completely idle client so
                        // L7 load balancers / NATs in front of ethonyx observe traffic
                        // without adding per-connection proxy timers. Client's Pong is
                        // absorbed locally when it arrives.
                        if src.side == Side::Upstream {
                            dst.try_queue_idle_ping(cx, payload)?;
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => return Err(Stop::Closed(frame)),
                }
            }
        }
    }
    Ok(count == MAX_BATCH_FRAMES && dst.can_accept() && src.local_reply.is_none())
}

async fn finish<A, B>(src: &mut WsEndpoint<A>, other: &mut WsEndpoint<B>, stop: Stop)
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress,
{
    match stop {
        Stop::Closed(frame) => {
            // `src` initiated Close, so Tungstenite is now in closing state and
            // rejects new `ws.send(data)` calls with `SendAfterClosing`. Drop
            // unsent frames destined for `src` and flush its Close reply directly.
            src.outbound = OutboundState::Idle;
            src.local_reply = None;
            let _ = flush_bounded(&mut src.ws, WS_CLOSE_GRACE).await;
            if other.drain_pending(WS_CLOSE_GRACE).await
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
        Stop::Eof => {
            let _ = other.drain_pending(WS_CLOSE_GRACE).await;
        }
        Stop::Failed => {
            let _ = other.drain_pending(WS_CLOSE_GRACE).await;
            let _ = flush_bounded(&mut src.ws, WS_CLOSE_GRACE).await;
        }
        Stop::Abort => {}
    }
}

async fn bridge_ws<C, U>(
    client_ws: WebSocketStream<C>,
    upstream_ws: WebSocketStream<U>,
    mock_enabled: bool,
    io_timeout: Duration,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress + Send + 'static,
{
    let mut client = WsEndpoint::new(client_ws, Side::Client, io_timeout);
    let mut upstream = WsEndpoint::new(upstream_ws, Side::Upstream, io_timeout);

    let (side, stop) = std::future::poll_fn(|cx| {
        // 1. Advance any already-queued outbound frames first so freed slots are
        // immediately visible to both directional read loops below.
        if upstream.poll_pending(cx).is_err() || client.poll_pending(cx).is_err() {
            return Poll::Ready((Side::Client, Stop::Abort));
        }

        // 2. Client -> Upstream (bounded batch per poll turn for scheduler fairness).
        let c2u_full = match pump(cx, &mut client, &mut upstream, mock_enabled) {
            Ok(full) => full,
            Err(stop) => return Poll::Ready((Side::Client, stop)),
        };

        // 3. Upstream -> Client (independent full-duplex progress even when `upstream` sink is full).
        let u2c_full = match pump(cx, &mut upstream, &mut client, false) {
            Ok(full) => full,
            Err(stop) => return Poll::Ready((Side::Upstream, stop)),
        };

        // 4. Flush batched outbound frames once per direction & enforce stall deadlines.
        if upstream.poll_flush_and_timer(cx).is_err() || client.poll_flush_and_timer(cx).is_err() {
            return Poll::Ready((Side::Client, Stop::Abort));
        }

        if c2u_full || u2c_full {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    })
    .await;

    match side {
        Side::Client => finish(&mut client, &mut upstream, stop).await,
        Side::Upstream => finish(&mut upstream, &mut client, stop).await,
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

    async fn duplex_bridge(
        c2p_cap: usize,
        p2u_cap: usize,
        mock_enabled: bool,
        io_timeout: Duration,
    ) -> (
        WebSocketStream<tokio::io::DuplexStream>,
        WebSocketStream<tokio::io::DuplexStream>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client_io, proxy_client_io) = tokio::io::duplex(c2p_cap);
        let (proxy_upstream_io, upstream_io) = tokio::io::duplex(p2u_cap);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(get_ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(get_ws_config()),
        )
        .await;
        let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
        let relay = tokio::spawn(bridge_ws(
            proxy_client,
            proxy_upstream,
            mock_enabled,
            io_timeout,
        ));
        (client, upstream, relay)
    }

    #[tokio::test]
    async fn single_loop_bridge_flushes_ping_locally_without_forwarding_or_duplicating_pong() {
        let (mut client, mut upstream, relay) =
            duplex_bridge(16 * 1024, 16 * 1024, true, Duration::from_secs(5)).await;

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
            let (mut client, mut upstream, relay) =
                duplex_bridge(64 * 1024, 64 * 1024, mock_enabled, Duration::from_secs(5)).await;
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
            let (mut client, mut upstream, relay) =
                duplex_bridge(64 * 1024, 64 * 1024, mock_enabled, Duration::from_secs(5)).await;
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
        let (mut client, _stalled_upstream, relay) =
            duplex_bridge(16 * 1024, 256, false, Duration::from_secs(10)).await;

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

    #[tokio::test(start_paused = true)]
    async fn slow_progressing_reader_survives_past_io_timeout_and_concurrent_ping() {
        // 4 KiB client duplex with io_timeout = 10s (> 5s WS_CONTROL_FLUSH_TIMEOUT).
        let (mut client, mut upstream, relay) =
            duplex_bridge(4096, 64 * 1024, false, Duration::from_secs(10)).await;

        let sender = tokio::spawn(async move {
            for i in 0u8..8 {
                upstream
                    .send(Message::Binary(bytes::Bytes::from(vec![i; 8 * 1024])))
                    .await
                    .unwrap();
            }
            upstream
        });
        tokio::task::yield_now().await;

        // At t = 1s, send a client Ping while `proxy_client` is already blocked in
        // `DataActive` (writing 64 KiB through a 4 KiB socket). The 5s control-flush
        // timeout must NOT overwrite the active 10s data deadline!
        tokio::time::advance(Duration::from_secs(1)).await;
        client
            .send(Message::Ping(bytes::Bytes::from_static(b"hb-under-load")))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Wait 7s after Ping (t = 8s: > 5s control timeout, < 10s data timeout).
        tokio::time::advance(Duration::from_secs(7)).await;
        tokio::task::yield_now().await;
        assert!(
            !relay.is_finished(),
            "Ping must not shrink active data io_timeout to 5s"
        );

        // Drain slowly: 1 data frame every 6s starting at t = 8s -> t = 50s (5x io_timeout!).
        // Because each 8 KiB read frees >= MIN_WRITE_PROGRESS_BYTES (4 KiB) in the socket,
        // `WriteProgressIo` observes real write progress and resets the stall deadline.
        let mut data_received = 0usize;
        let mut saw_pong = false;
        while data_received < 8 || !saw_pong {
            match client.next().await.unwrap().unwrap() {
                Message::Binary(b) => {
                    assert_eq!(b, vec![data_received as u8; 8 * 1024]);
                    data_received += 1;
                    if data_received < 8 {
                        tokio::time::advance(Duration::from_secs(6)).await;
                    }
                }
                Message::Pong(p) => {
                    assert_eq!(p, bytes::Bytes::from_static(b"hb-under-load"));
                    saw_pong = true;
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        let mut upstream = sender.await.unwrap();
        client.close(None).await.unwrap();
        while let Some(Ok(msg)) = client.next().await {
            if msg.is_close() {
                break;
            }
        }
        let _ = upstream.next().await;
        relay.await.unwrap();
    }

    #[tokio::test]
    async fn upstream_ping_forwards_to_idle_client_without_duplicate_pong_and_skips_busy_client() {
        // 1. Idle client: upstream Ping is answered locally to upstream AND forwarded
        // to the idle client; the client's Pong is absorbed without duplicating to upstream.
        let (mut client, mut upstream, relay) =
            duplex_bridge(16 * 1024, 16 * 1024, false, Duration::from_secs(5)).await;

        upstream
            .send(Message::Ping(bytes::Bytes::from_static(b"el-idle-hb")))
            .await
            .unwrap();
        let up_pong = tokio::time::timeout(Duration::from_secs(1), upstream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            up_pong,
            Message::Pong(bytes::Bytes::from_static(b"el-idle-hb"))
        );

        let forwarded_ping = tokio::time::timeout(Duration::from_secs(1), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            forwarded_ping,
            Message::Ping(bytes::Bytes::from_static(b"el-idle-hb"))
        );

        // Client's tungstenite auto-queued Pong during `client.next()`; flush it and
        // follow immediately with a data frame to prove upstream never sees a 2nd Pong.
        client.flush().await.unwrap();
        client
            .send(Message::Text("after-pong".into()))
            .await
            .unwrap();
        let next_at_upstream = upstream.next().await.unwrap().unwrap();
        assert_eq!(next_at_upstream, Message::Text("after-pong".into()));

        client.close(None).await.unwrap();
        while let Some(Ok(msg)) = client.next().await {
            if msg.is_close() {
                break;
            }
        }
        let _ = upstream.next().await;
        relay.await.unwrap();

        // 2. Busy client (`DataActive`): upstream Ping is still answered to upstream,
        // but NOT injected onto the already-active client stream.
        let (mut client, mut upstream, relay) =
            duplex_bridge(512, 64 * 1024, false, Duration::from_secs(5)).await;
        for i in 0u8..2 {
            upstream
                .send(Message::Binary(bytes::Bytes::from(vec![i; 4096])))
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;

        upstream
            .send(Message::Ping(bytes::Bytes::from_static(b"busy-hb")))
            .await
            .unwrap();
        let up_pong = tokio::time::timeout(Duration::from_secs(1), upstream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            up_pong,
            Message::Pong(bytes::Bytes::from_static(b"busy-hb"))
        );

        for i in 0u8..2 {
            let msg = client.next().await.unwrap().unwrap();
            assert_eq!(msg, Message::Binary(bytes::Bytes::from(vec![i; 4096])));
        }
        client.close(None).await.unwrap();
        while let Some(Ok(msg)) = client.next().await {
            if msg.is_close() {
                break;
            }
        }
        let _ = upstream.next().await;
        relay.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn sub_threshold_slowloris_drain_and_stalled_ping_flood_still_abort() {
        use tokio::io::AsyncReadExt;

        // 1. Sub-4 KiB Slowloris drain (512 B every 4s on 10s io_timeout = 1024 B < 4 KiB)
        // must NOT disarm the stall deadline and must abort at t = 10s.
        let (mut raw_client, proxy_client_io) = tokio::io::duplex(4096);
        let (proxy_upstream_io, upstream_io) = tokio::io::duplex(64 * 1024);
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(get_ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(get_ws_config()),
        )
        .await;
        let mut upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
        let relay = tokio::spawn(bridge_ws(
            proxy_client,
            proxy_upstream,
            false,
            Duration::from_secs(10),
        ));

        upstream
            .send(Message::Binary(bytes::Bytes::from(vec![0xAB; 32 * 1024])))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let mut trickle_buf = [0u8; 512];
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(4)).await;
            let n = raw_client.read(&mut trickle_buf).await.unwrap();
            assert_eq!(n, 512);
            tokio::task::yield_now().await;
            assert!(!relay.is_finished());
        }

        // At t = 11s (> 10s, only 3s after last 512 B read), relay aborts!
        tokio::time::advance(Duration::from_secs(3)).await;
        relay.await.unwrap();

        // 2. Zero-window client flooding Ping frames: bounded by max_write_buffer_size
        // and evicted at WS_CONTROL_FLUSH_TIMEOUT (5s).
        let (attacker_to_proxy, proxy_from_attacker) = tokio::io::duplex(64 * 1024);
        let (_, attacker_tx) = tokio::io::split(attacker_to_proxy);
        let (proxy_rx, _) = tokio::io::split(proxy_from_attacker);

        let (proxy_to_attacker, _stalled_attacker) = tokio::io::duplex(64);
        let (_, proxy_tx) = tokio::io::split(proxy_to_attacker);

        let (_keep_open_tx, attacker_idle) = tokio::io::duplex(64);
        let (attacker_rx, _) = tokio::io::split(attacker_idle);

        let mut attacker_ws = WebSocketStream::from_raw_socket(
            tokio::io::join(attacker_rx, attacker_tx),
            Role::Client,
            None,
        )
        .await;
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(tokio::io::join(proxy_rx, proxy_tx)),
            Role::Server,
            Some(get_ws_config()),
        )
        .await;
        let (proxy_upstream_io, _upstream_io) = tokio::io::duplex(16 * 1024);
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(get_ws_config()),
        )
        .await;
        let relay = tokio::spawn(bridge_ws(
            proxy_client,
            proxy_upstream,
            false,
            Duration::from_secs(60),
        ));

        for _ in 0..32 {
            attacker_ws
                .feed(Message::Ping(bytes::Bytes::from(vec![b'p'; 125])))
                .await
                .unwrap();
        }
        attacker_ws.flush().await.unwrap();
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(4)).await;
        tokio::task::yield_now().await;
        assert!(!relay.is_finished());

        tokio::time::advance(Duration::from_secs(2)).await;
        relay.await.unwrap();
    }

    #[tokio::test]
    async fn peer_close_while_sink_is_backpressured_still_flushes_close_reply_and_drains_other() {
        // Tiny 512-byte client duplex so upstream frames immediately backpressure `client`.
        let (mut client, mut upstream, relay) =
            duplex_bridge(512, 64 * 1024, false, Duration::from_secs(5)).await;

        // Fill `proxy_client`'s 16 KiB write buffer + 1-slot `queued` (`DataActive { queued: Some(_) }`).
        for _ in 0..3 {
            upstream
                .send(Message::Binary(bytes::Bytes::from(vec![b'p'; 12 * 1024])))
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;

        // Also pipeline a client->upstream frame right before Close to verify `other` drains cleanly.
        client
            .send(Message::Text("final-before-close".into()))
            .await
            .unwrap();
        client
            .send(Message::Close(Some(CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "done".into(),
            })))
            .await
            .unwrap();

        // Drain inbound frames on client until the RFC 6455 Close echo arrives cleanly
        // (must not fail with `ResetWithoutClosingHandshake` from `SendAfterClosing`).
        let mut saw_close_echo = false;
        while let Some(res) = client.next().await {
            if res.expect("clean close handshake").is_close() {
                saw_close_echo = true;
                break;
            }
        }
        assert!(saw_close_echo, "client must receive Close echo");

        let forwarded = upstream.next().await.unwrap().unwrap();
        assert_eq!(forwarded, Message::Text("final-before-close".into()));
        let close_to_upstream = upstream.next().await.unwrap().unwrap();
        assert!(close_to_upstream.is_close());
        let _ = upstream.next().await;
        relay.await.unwrap();
    }
}
