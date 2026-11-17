//! Bounded full-duplex single-task WebSocket frame relay and socket write-progress tracking.

use crate::lifecycle::{LazyDeadline, Shutdown};
use crate::rpc::{MockResult, mock_syncing};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message, Utf8Bytes,
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tracing::debug;

const WS_CLOSE_GRACE: Duration = Duration::from_secs(1);
const WS_CONTROL_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BATCH_FRAMES: usize = 32;
const MAX_WRITE_CHUNK: usize = 64 * 1024;
const MIN_WRITE_PROGRESS_BYTES: usize = 4 * 1024;
const GOING_AWAY_FRAME: CloseFrame = CloseFrame {
    code: CloseCode::Away,
    reason: Utf8Bytes::from_static("server shutting down"),
};

/// Caps single writes to [`MAX_WRITE_CHUNK`] (so HTTP/2 Extended `CONNECT`'s
/// `Upgrade` bridge cannot swallow a multi-MiB frame in one `Box<[u8]>` and
/// starve progress tracking) and accumulates written bytes so slow-progressing
/// peers (`>= MIN_WRITE_PROGRESS_BYTES` per window) survive while `0`-byte or
/// sub-threshold Slowloris drains abort on deadline.
pub(super) struct WriteProgressIo<I> {
    inner: I,
    written: usize,
}

impl<I> WriteProgressIo<I> {
    pub(super) const fn new(inner: I) -> Self {
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

pub(super) trait HasWriteProgress {
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
            _ => false,
        }
    }

    fn reset_write_progress(&mut self) {
        match self {
            Self::Plain(s) => s.reset_write_progress(),
            Self::Rustls(tls) => tls.get_mut().0.reset_write_progress(),
            _ => {}
        }
    }
}

/// Relay transport: async IO whose socket-level write progress is observable.
pub(super) trait RelayIo:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress
{
}

impl<T> RelayIo for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + HasWriteProgress
{
}

enum OutboundState {
    Idle,
    ControlFlush,
    DataActive { queued: Option<Message> },
}

#[derive(Clone, Copy, Debug)]
enum Side {
    Client,
    Upstream,
}

struct Abort;

enum PeerStop {
    Closed(Option<CloseFrame>),
    Eof,
    Failed,
}

enum PumpStop {
    Abort,
    Peer(PeerStop),
}

impl From<Abort> for PumpStop {
    fn from(_: Abort) -> Self {
        Self::Abort
    }
}

enum CloseReason {
    Peer(Side, PeerStop),
    Shutdown,
}

/// Non-blocking full-duplex WebSocket endpoint state with a 1-slot outbound
/// backpressure queue, 1-slot local mock-reply buffer, and progress-based stall timer.
struct WsEndpoint<S> {
    ws: WebSocketStream<S>,
    side: Side,
    intercept_mock: bool,
    forward_ping: bool,
    outbound: OutboundState,
    local_reply: Option<Message>,
    deadline: LazyDeadline,
    io_timeout: Duration,
}

impl<S: RelayIo> WsEndpoint<S> {
    const fn new(ws: WebSocketStream<S>, side: Side, io_timeout: Duration) -> Self {
        Self {
            ws,
            side,
            intercept_mock: false,
            forward_ping: false,
            outbound: OutboundState::Idle,
            local_reply: None,
            deadline: LazyDeadline::new(),
            io_timeout,
        }
    }

    const fn with_mock_syncing(mut self, enabled: bool) -> Self {
        self.intercept_mock = enabled;
        self
    }

    const fn with_ping_forwarding(mut self) -> Self {
        self.forward_ping = true;
        self
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

    const fn has_pending(&self) -> bool {
        !matches!(self.outbound, OutboundState::Idle) || self.local_reply.is_some()
    }

    fn try_queue_idle_ping(
        &mut self,
        cx: &mut Context<'_>,
        payload: bytes::Bytes,
    ) -> Result<(), Abort> {
        if self.has_pending() {
            return Ok(());
        }
        if let Poll::Ready(res) = Pin::new(&mut self.ws).poll_ready(cx) {
            res.map_err(|_| Abort)?;
            Pin::new(&mut self.ws)
                .start_send(Message::Ping(payload))
                .map_err(|_| Abort)?;
            self.request_control_flush();
        }
        Ok(())
    }

    fn check_stall_deadline(
        &mut self,
        cx: &mut Context<'_>,
        timeout: Duration,
    ) -> Result<(), Abort> {
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
            Err(Abort)
        } else {
            Ok(())
        }
    }

    fn poll_send_queued(&mut self, cx: &mut Context<'_>) -> Result<(), Abort> {
        let OutboundState::DataActive { queued } = &mut self.outbound else {
            return Ok(());
        };
        if queued.is_none() {
            return Ok(());
        }
        match Pin::new(&mut self.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = queued.take().expect("queued checked");
                Pin::new(&mut self.ws).start_send(msg).map_err(|_| Abort)
            }
            Poll::Ready(Err(_)) => Err(Abort),
            // Defer stall-timer evaluation to Step 4 (`poll_flush_and_timer`),
            // which runs once per turn after both directional pumps finish.
            Poll::Pending => Ok(()),
        }
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Result<(), Abort> {
        self.poll_send_queued(cx)?;
        if self.can_accept()
            && let Some(reply) = self.local_reply.take()
        {
            self.enqueue_data(reply);
            self.poll_send_queued(cx)?;
        }
        Ok(())
    }

    fn poll_flush_and_timer(&mut self, cx: &mut Context<'_>) -> Result<(), Abort> {
        let timeout = match self.outbound {
            OutboundState::Idle => return Ok(()),
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
            Poll::Ready(Err(_)) => Err(Abort),
            Poll::Pending => self.check_stall_deadline(cx, timeout),
        }
    }

    async fn flush_pending(&mut self) -> bool {
        if !self.has_pending() {
            return true;
        }
        let queued = match std::mem::replace(&mut self.outbound, OutboundState::Idle) {
            OutboundState::DataActive { queued } => queued,
            OutboundState::Idle | OutboundState::ControlFlush => None,
        };
        let reply = self.local_reply.take();
        for msg in queued.into_iter().chain(reply) {
            if self.ws.feed(msg).await.is_err() {
                return false;
            }
        }
        self.ws.flush().await.is_ok()
    }

    /// Drain queued outbound data, transmit `Close(frame)`, await the peer's echoed
    /// `Close` so kernel `SO_RCVBUF` empties without `TCP RST`, and shut down the
    /// transport write half cleanly (`FIN` / H2 `END_STREAM` / TLS `close_notify`)
    /// before dropping `self`. Callers bound the overall close phase with a single
    /// timeout ([`WS_CLOSE_GRACE`] or the shutdown drain deadline).
    async fn close_handshake(mut self, frame: Option<CloseFrame>) {
        if self.flush_pending().await && self.ws.send(Message::Close(frame)).await.is_ok() {
            while let Some(Ok(msg)) = self.ws.next().await {
                if msg.is_close() {
                    break;
                }
            }
            let _ = tokio::io::AsyncWriteExt::shutdown(self.ws.get_mut()).await;
        }
    }
}

fn pump<A, B>(
    cx: &mut Context<'_>,
    src: &mut WsEndpoint<A>,
    dst: &mut WsEndpoint<B>,
) -> Result<bool, PumpStop>
where
    A: RelayIo,
    B: RelayIo,
{
    let mut count = 0;
    while count < MAX_BATCH_FRAMES {
        if !dst.can_accept() || src.local_reply.is_some() {
            break;
        }
        match Pin::new(&mut src.ws).poll_next(cx) {
            Poll::Pending => break,
            Poll::Ready(None) => return Err(PumpStop::Peer(PeerStop::Eof)),
            Poll::Ready(Some(Err(_))) => return Err(PumpStop::Peer(PeerStop::Failed)),
            Poll::Ready(Some(Ok(msg))) => {
                count += 1;
                match msg {
                    Message::Text(text) => {
                        if src.intercept_mock {
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
                        if src.forward_ping {
                            dst.try_queue_idle_ping(cx, payload)?;
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => return Err(PumpStop::Peer(PeerStop::Closed(frame))),
                }
            }
        }
    }
    Ok(count == MAX_BATCH_FRAMES && dst.can_accept() && src.local_reply.is_none())
}

async fn finish<A, B>(mut src: WsEndpoint<A>, mut other: WsEndpoint<B>, stop: PeerStop)
where
    A: RelayIo,
    B: RelayIo,
{
    let _ = tokio::time::timeout(WS_CLOSE_GRACE, async {
        match stop {
            PeerStop::Closed(frame) => {
                // `src` initiated Close, so Tungstenite is now in closing state and
                // rejects new `ws.send(data)` calls with `SendAfterClosing`. Drop
                // unsent frames destined for `src`, flush its Close reply, and close
                // `src` concurrently with `other`'s handshake.
                let mut src_ws = src.ws;
                drop((src.outbound, src.local_reply));
                let close_src = async move {
                    if src_ws.flush().await.is_ok() {
                        let _ = tokio::io::AsyncWriteExt::shutdown(src_ws.get_mut()).await;
                    }
                };
                tokio::join!(close_src, other.close_handshake(frame));
            }
            PeerStop::Eof => {
                let _ = other.flush_pending().await;
            }
            PeerStop::Failed => {
                let (_, _) = tokio::join!(other.flush_pending(), src.ws.flush());
            }
        }
    })
    .await;
}

/// Keep relaying across `--shutdown-grace`, then resolve up to [`WS_CLOSE_GRACE`]
/// before the hard drain deadline so the relay can initiate a `Close(1001)` handshake.
async fn shutdown_close_signal(shutdown: &Shutdown) {
    let end = shutdown.deadline().await;
    let now = tokio::time::Instant::now();
    let remaining = end.saturating_duration_since(now);
    let delay = remaining.saturating_sub((remaining / 8).min(WS_CLOSE_GRACE));
    if !delay.is_zero() {
        tokio::time::sleep_until(now + delay).await;
    }
}

pub(super) async fn bridge_ws<C, U>(
    client_ws: WebSocketStream<C>,
    upstream_ws: WebSocketStream<U>,
    mock_enabled: bool,
    io_timeout: Duration,
    shutdown: &Shutdown,
) where
    C: RelayIo,
    U: RelayIo,
{
    let mut client =
        WsEndpoint::new(client_ws, Side::Client, io_timeout).with_mock_syncing(mock_enabled);
    let mut upstream =
        WsEndpoint::new(upstream_ws, Side::Upstream, io_timeout).with_ping_forwarding();

    // Phase 1: full-duplex relay with a single steady-state `Notify` registration.
    let Some(reason) = ({
        let close_signal = shutdown_close_signal(shutdown);
        tokio::pin!(close_signal);

        std::future::poll_fn(|cx| {
            if close_signal.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Some(CloseReason::Shutdown));
            }

            // 1. Advance any already-queued outbound frames first so freed slots are
            // immediately visible to both directional read loops below.
            if upstream.poll_pending(cx).is_err() || client.poll_pending(cx).is_err() {
                return Poll::Ready(None);
            }

            // 2. Client -> Upstream (bounded batch per poll turn for scheduler fairness).
            let c2u_full = match pump(cx, &mut client, &mut upstream) {
                Ok(full) => full,
                Err(PumpStop::Abort) => return Poll::Ready(None),
                Err(PumpStop::Peer(stop)) => {
                    return Poll::Ready(Some(CloseReason::Peer(Side::Client, stop)));
                }
            };

            // 3. Upstream -> Client (independent full-duplex progress even when `upstream` sink is full).
            let u2c_full = match pump(cx, &mut upstream, &mut client) {
                Ok(full) => full,
                Err(PumpStop::Abort) => return Poll::Ready(None),
                Err(PumpStop::Peer(stop)) => {
                    return Poll::Ready(Some(CloseReason::Peer(Side::Upstream, stop)));
                }
            };

            // 4. Flush batched outbound frames once per direction & enforce stall deadlines.
            if upstream.poll_flush_and_timer(cx).is_err()
                || client.poll_flush_and_timer(cx).is_err()
            {
                return Poll::Ready(None);
            }

            if c2u_full || u2c_full {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await
    }) else {
        return;
    };

    match &reason {
        CloseReason::Peer(Side::Client, PeerStop::Eof) if !upstream.has_pending() => return,
        CloseReason::Peer(Side::Upstream, PeerStop::Eof) if !client.has_pending() => return,
        _ => {}
    }

    // Phase 2: single hard-deadline bound governing all close/drain paths, including
    // a shutdown signal that arrives while a peer-initiated close is already running.
    let _ = shutdown
        .bound(async {
            match reason {
                CloseReason::Peer(Side::Client, stop) => finish(client, upstream, stop).await,
                CloseReason::Peer(Side::Upstream, stop) => finish(upstream, client, stop).await,
                CloseReason::Shutdown => {
                    tokio::join!(
                        client.close_handshake(Some(GOING_AWAY_FRAME)),
                        upstream.close_handshake(Some(GOING_AWAY_FRAME)),
                    );
                }
            }
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::super::ws_config;
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Role;

    fn spawn_bridge<C, U>(
        proxy_client: WebSocketStream<C>,
        proxy_upstream: WebSocketStream<U>,
        mock_enabled: bool,
        io_timeout: Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        C: RelayIo + Send + 'static,
        U: RelayIo + Send + 'static,
    {
        tokio::spawn(async move {
            let (_tx, shutdown) = crate::lifecycle::channel();
            bridge_ws(
                proxy_client,
                proxy_upstream,
                mock_enabled,
                io_timeout,
                &shutdown,
            )
            .await;
        })
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
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;
        let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
        let relay = spawn_bridge(proxy_client, proxy_upstream, mock_enabled, io_timeout);
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
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;
        let mut upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
        let relay = spawn_bridge(proxy_client, proxy_upstream, false, Duration::from_secs(10));

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
            Some(ws_config()),
        )
        .await;
        let (proxy_upstream_io, _upstream_io) = tokio::io::duplex(16 * 1024);
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;
        let relay = spawn_bridge(proxy_client, proxy_upstream, false, Duration::from_secs(60));

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

    #[tokio::test(start_paused = true)]
    async fn shutdown_signal_sends_going_away_and_completes_clean_close_handshake() {
        async fn expect_going_away(peer: &mut WebSocketStream<tokio::io::DuplexStream>) {
            let close_msg = peer
                .next()
                .await
                .expect("Close frame")
                .expect("clean frame");
            let Message::Close(Some(frame)) = close_msg else {
                panic!("expected Close(Some(..)), got {close_msg:?}");
            };
            assert_eq!(frame.code, CloseCode::Away);
            assert_eq!(frame.reason.as_str(), "server shutting down");
            assert!(
                peer.next().await.is_none(),
                "peer must observe clean EOF without reset after Close handshake"
            );
        }

        // 1. Both peers active (`10s` grace -> `9s` live relay + `1s` WS_CLOSE_GRACE margin):
        // frames relay across the drain window, then Close(1001) completes cleanly on both sides.
        let (client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, upstream_io) = tokio::io::duplex(16 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;
        let mut upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;

        let (tx, rx) = crate::lifecycle::channel();
        let relay = tokio::spawn(async move {
            bridge_ws(
                proxy_client,
                proxy_upstream,
                false,
                Duration::from_secs(60),
                &rx,
            )
            .await;
        });
        tokio::task::yield_now().await;

        let started = tokio::time::Instant::now();
        tx.send(started + Duration::from_secs(10));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(8500)).await;
        client
            .send(Message::Text("mid-drain".into()))
            .await
            .unwrap();
        assert_eq!(
            upstream.next().await.unwrap().unwrap(),
            Message::Text("mid-drain".into())
        );
        assert!(!relay.is_finished());

        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::join!(
            expect_going_away(&mut client),
            expect_going_away(&mut upstream)
        );
        relay.await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(9));

        // 2. Stalled upstream never echoes Close: active client still observes immediate
        // Close(1001) + clean EOF at `end - WS_CLOSE_GRACE` (`9s`) without waiting for
        // upstream's `1s` timeout, and `relay` terminates strictly at `end` (`10s`).
        let (client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, _stalled_upstream_io) = tokio::io::duplex(16 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;

        let (tx, rx) = crate::lifecycle::channel();
        let relay = tokio::spawn(async move {
            bridge_ws(
                proxy_client,
                proxy_upstream,
                false,
                Duration::from_secs(60),
                &rx,
            )
            .await;
        });
        tokio::task::yield_now().await;

        let started = tokio::time::Instant::now();
        tx.send(started + Duration::from_secs(10));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::time::timeout(Duration::from_millis(50), expect_going_away(&mut client))
            .await
            .expect("client Close handshake must not block on stalled upstream");
        assert!(!relay.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        relay.await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(10));

        // 3. Zero-grace (`end == now`) with stalled peers exits immediately without
        // blocking on `WS_CLOSE_GRACE`.
        let (_stalled_client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, _stalled_upstream_io) = tokio::io::duplex(16 * 1024);
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;

        let (tx, rx) = crate::lifecycle::channel();
        tx.send(tokio::time::Instant::now());
        let started = tokio::time::Instant::now();
        bridge_ws(
            proxy_client,
            proxy_upstream,
            false,
            Duration::from_secs(60),
            &rx,
        )
        .await;
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_deadline_bounds_an_in_progress_peer_close() {
        let (client_io, proxy_client_io) = tokio::io::duplex(16 * 1024);
        let (proxy_upstream_io, _stalled_upstream_io) = tokio::io::duplex(16 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let proxy_client = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_client_io),
            Role::Server,
            Some(ws_config()),
        )
        .await;
        let proxy_upstream = WebSocketStream::from_raw_socket(
            WriteProgressIo::new(proxy_upstream_io),
            Role::Client,
            Some(ws_config()),
        )
        .await;

        let (tx, rx) = crate::lifecycle::channel();
        let relay = tokio::spawn(async move {
            bridge_ws(
                proxy_client,
                proxy_upstream,
                false,
                Duration::from_secs(60),
                &rx,
            )
            .await;
        });
        tokio::task::yield_now().await;

        // Client initiates a clean Close while upstream never echoes Close, parking
        // `bridge_ws` inside Phase 2 (`finish` -> `WS_CLOSE_GRACE` = 1s).
        client.close(None).await.unwrap();
        let echo = client.next().await.expect("Close echo").expect("clean");
        assert!(echo.is_close(), "expected Close echo, got {echo:?}");

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(
            !relay.is_finished(),
            "peer close must still be waiting on stalled upstream before shutdown"
        );

        // Zero-grace shutdown arriving mid-`finish` must preempt the remaining 900ms immediately.
        let shutdown_at = tokio::time::Instant::now();
        tx.send(shutdown_at);
        relay.await.unwrap();
        assert_eq!(
            shutdown_at.elapsed(),
            Duration::ZERO,
            "shutdown deadline must preempt an in-progress peer close immediately"
        );
    }
}
