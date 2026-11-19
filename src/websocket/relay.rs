//! Bounded full-duplex single-task WebSocket frame relay and socket write-progress tracking.

use crate::lifecycle::{LazyDeadline, Shutdown};
use crate::rpc::{MockResult, mock_syncing};
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message, Utf8Bytes,
    protocol::frame::{
        CloseFrame, Frame,
        coding::{CloseCode, Data as OpData, OpCode},
    },
};
use tracing::debug;

const WS_CLOSE_GRACE: Duration = Duration::from_secs(1);
const WS_CONTROL_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BATCH_FRAMES: usize = 32;
const MAX_WRITE_CHUNK: usize = 64 * 1024;
const MAX_OUTBOUND_FRAME_BYTES: usize = 1024 * 1024;
const _: () = assert!(MAX_OUTBOUND_FRAME_BYTES >= 4);
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
    fn reset_write_progress(&mut self) {
        let _ = self.take_write_progress(0);
    }
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
}

impl<S: HasWriteProgress> HasWriteProgress for tokio_tungstenite::MaybeTlsStream<S> {
    fn take_write_progress(&mut self, min_bytes: usize) -> bool {
        match self {
            Self::Plain(s) => s.take_write_progress(min_bytes),
            Self::Rustls(tls) => tls.get_mut().0.take_write_progress(min_bytes),
            _ => false,
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

#[derive(Clone, Copy)]
enum DataKind {
    Text,
    Binary,
}

/// Zero-copy outbound data cursor that emits wire frames capped at
/// [`MAX_OUTBOUND_FRAME_BYTES`] (`1 MiB`, well within peer `16 MiB` `max_frame_size`
/// and our `2 MiB` write buffer).
struct QueuedMsg {
    kind: DataKind,
    remaining: Bytes,
    first: bool,
}

impl QueuedMsg {
    fn text(text: Utf8Bytes) -> Self {
        Self {
            kind: DataKind::Text,
            remaining: text.into(),
            first: true,
        }
    }

    const fn binary(bin: Bytes) -> Self {
        Self {
            kind: DataKind::Binary,
            remaining: bin,
            first: true,
        }
    }

    /// Advance the cursor in place, returning the next wire frame and whether
    /// this was the final frame of the message.
    fn pop_frame(&mut self) -> (Message, bool) {
        let len = self.remaining.len();
        let mut end = MAX_OUTBOUND_FRAME_BYTES.min(len);
        if matches!(self.kind, DataKind::Text) && end < len {
            // Align text fragments to a UTF-8 scalar boundary (at most 3 bytes back).
            while end > 0 && (self.remaining[end] & 0xC0) == 0x80 {
                end -= 1;
            }
        }
        let opcode = if self.first {
            match self.kind {
                DataKind::Text => OpCode::Data(OpData::Text),
                DataKind::Binary => OpCode::Data(OpData::Binary),
            }
        } else {
            OpCode::Data(OpData::Continue)
        };
        let is_final = end == len;
        let chunk = if is_final {
            // Move the final slice without bumping/dropping `Bytes`' refcount.
            std::mem::take(&mut self.remaining)
        } else {
            self.first = false;
            self.remaining.split_to(end)
        };
        (
            Message::Frame(Frame::message(chunk, opcode, is_final)),
            is_final,
        )
    }
}

enum OutboundState {
    Idle,
    ControlFlush,
    DataFlush,
    DataQueued {
        current: QueuedMsg,
        /// Client-only local `eth_syncing` reply queued while `current` (from upstream)
        /// waits for sink capacity; structurally unrepresentable in other outbound states.
        deferred_reply: Option<String>,
    },
}

impl OutboundState {
    const fn has_queued(&self) -> bool {
        matches!(self, Self::DataQueued { .. })
    }

    fn pop_frame(&mut self) -> Option<Message> {
        let Self::DataQueued {
            current,
            deferred_reply,
        } = self
        else {
            return None;
        };
        let (frame, is_final) = current.pop_frame();
        if is_final {
            if let Some(reply) = deferred_reply.take() {
                *current = QueuedMsg::text(reply.into());
            } else {
                *self = Self::DataFlush;
            }
        }
        Some(frame)
    }
}

#[derive(Clone, Copy)]
enum EndpointRole {
    Client,
    ClientMock,
    Upstream,
}

impl EndpointRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Client | Self::ClientMock => "client",
            Self::Upstream => "upstream",
        }
    }
}

enum PeerStop {
    Closed(Option<CloseFrame>),
    Failed,
}

enum RelayStop {
    Abort,
    Client(PeerStop),
    Upstream(PeerStop),
    Shutdown,
}

/// Non-blocking full-duplex WebSocket endpoint state with a 1-slot outbound
/// backpressure queue, client-only local mock-reply buffer, and progress-based stall timer.
struct WsEndpoint<S> {
    ws: WebSocketStream<S>,
    role: EndpointRole,
    outbound: OutboundState,
    deadline: LazyDeadline,
    io_timeout: Duration,
}

impl<S: RelayIo> WsEndpoint<S> {
    const fn new(ws: WebSocketStream<S>, role: EndpointRole, io_timeout: Duration) -> Self {
        Self {
            ws,
            role,
            outbound: OutboundState::Idle,
            deadline: LazyDeadline::new(),
            io_timeout,
        }
    }

    const fn can_accept(&self) -> bool {
        !self.outbound.has_queued()
    }

    const fn can_read(&self) -> bool {
        !matches!(
            self.outbound,
            OutboundState::ControlFlush
                | OutboundState::DataQueued {
                    deferred_reply: Some(_),
                    ..
                }
        )
    }

    fn enqueue_data(&mut self, msg: QueuedMsg) {
        debug_assert!(self.can_accept());
        if matches!(self.outbound, OutboundState::ControlFlush) {
            // Upgrade from the shorter control flush timeout to full data `io_timeout`
            // while preserving elapsed stall time and write progress.
            self.deadline
                .extend(self.io_timeout.saturating_sub(WS_CONTROL_FLUSH_TIMEOUT));
        }
        self.outbound = OutboundState::DataQueued {
            current: msg,
            deferred_reply: None,
        };
    }

    fn enqueue_local_reply(&mut self, reply: String) {
        match &mut self.outbound {
            OutboundState::DataQueued { deferred_reply, .. } => {
                debug_assert!(deferred_reply.is_none());
                *deferred_reply = Some(reply);
            }
            _ => self.enqueue_data(QueuedMsg::text(reply.into())),
        }
    }

    fn request_control_flush(&mut self) {
        // Never downgrade an active data transfer's `io_timeout` to control timeout on `Ping`.
        if matches!(self.outbound, OutboundState::Idle) {
            self.outbound = OutboundState::ControlFlush;
        }
    }

    const fn has_pending(&self) -> bool {
        !matches!(self.outbound, OutboundState::Idle)
    }

    fn try_queue_idle_ping(
        &mut self,
        cx: &mut Context<'_>,
        payload: Bytes,
    ) -> Result<(), RelayStop> {
        if self.has_pending() {
            return Ok(());
        }
        if let Poll::Ready(res) = Pin::new(&mut self.ws).poll_ready(cx) {
            res.map_err(|_| RelayStop::Abort)?;
            Pin::new(&mut self.ws)
                .start_send(Message::Ping(payload))
                .map_err(|_| RelayStop::Abort)?;
            self.request_control_flush();
        }
        Ok(())
    }

    fn check_stall_deadline(
        &mut self,
        cx: &mut Context<'_>,
        timeout: Duration,
    ) -> Result<(), RelayStop> {
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
                side = self.role.as_str(),
                ?timeout,
                "ws write stalled; aborting relay"
            );
            Err(RelayStop::Abort)
        } else {
            Ok(())
        }
    }

    fn poll_send_queued(&mut self, cx: &mut Context<'_>) -> Result<(), RelayStop> {
        while self.outbound.has_queued() {
            match Pin::new(&mut self.ws).poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    if let Some(frame) = self.outbound.pop_frame() {
                        Pin::new(&mut self.ws)
                            .start_send(frame)
                            .map_err(|_| RelayStop::Abort)?;
                    }
                }
                Poll::Ready(Err(_)) => return Err(RelayStop::Abort),
                // Defer stall-timer evaluation to Step 4 (`poll_flush_and_timer`),
                // which runs once per turn after both directional pumps finish.
                Poll::Pending => return Ok(()),
            }
        }
        Ok(())
    }

    fn poll_flush_and_timer(&mut self, cx: &mut Context<'_>) -> Result<(), RelayStop> {
        let timeout = match self.outbound {
            OutboundState::Idle => return Ok(()),
            OutboundState::DataQueued { .. } => {
                return self.check_stall_deadline(cx, self.io_timeout);
            }
            OutboundState::DataFlush => self.io_timeout,
            OutboundState::ControlFlush => self.io_timeout.min(WS_CONTROL_FLUSH_TIMEOUT),
        };
        match Pin::new(&mut self.ws).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.outbound = OutboundState::Idle;
                self.ws.get_mut().reset_write_progress();
                self.deadline.disarm();
                Ok(())
            }
            Poll::Ready(Err(_)) => Err(RelayStop::Abort),
            Poll::Pending => self.check_stall_deadline(cx, timeout),
        }
    }

    async fn flush_pending(&mut self) -> bool {
        if !self.has_pending() {
            return true;
        }
        while let Some(frame) = self.outbound.pop_frame() {
            if self.ws.feed(frame).await.is_err() {
                self.outbound = OutboundState::Idle;
                return false;
            }
        }
        self.outbound = OutboundState::Idle;
        self.ws.flush().await.is_ok()
    }

    async fn flush_and_shutdown(mut self) {
        if self.flush_pending().await {
            let _ = tokio::io::AsyncWriteExt::shutdown(self.ws.get_mut()).await;
        }
    }

    /// Drop any unsent outbound payloads queued for this endpoint, flush any
    /// protocol-level Close reply already buffered by Tungstenite, and shut down
    /// the transport write half cleanly.
    async fn flush_close_reply(mut self) {
        self.outbound = OutboundState::Idle;
        if self.ws.flush().await.is_ok() {
            let _ = tokio::io::AsyncWriteExt::shutdown(self.ws.get_mut()).await;
        }
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
    on_peer: fn(PeerStop) -> RelayStop,
) -> Result<bool, RelayStop>
where
    A: RelayIo,
    B: RelayIo,
{
    let mut count = 0;
    while count < MAX_BATCH_FRAMES {
        if !dst.can_accept() || !src.can_read() {
            return Ok(true);
        }
        match Pin::new(&mut src.ws).poll_next(cx) {
            Poll::Pending => return Ok(false),
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => return Err(on_peer(PeerStop::Failed)),
            Poll::Ready(Some(Ok(msg))) => {
                count += 1;
                match msg {
                    Message::Text(text) => {
                        if matches!(src.role, EndpointRole::ClientMock) {
                            match mock_syncing(text.as_bytes()) {
                                MockResult::Silent => continue,
                                MockResult::Reply(reply) => {
                                    src.enqueue_local_reply(reply);
                                    src.poll_send_queued(cx)?;
                                    continue;
                                }
                                MockResult::Forward => {}
                            }
                        }
                        dst.enqueue_data(QueuedMsg::text(text));
                        dst.poll_send_queued(cx)?;
                    }
                    Message::Binary(bin) => {
                        dst.enqueue_data(QueuedMsg::binary(bin));
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
                        if matches!(src.role, EndpointRole::Upstream) {
                            dst.try_queue_idle_ping(cx, payload)?;
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => return Err(on_peer(PeerStop::Closed(frame))),
                }
            }
        }
    }
    Ok(true)
}

async fn finish<A, B>(src: WsEndpoint<A>, other: WsEndpoint<B>, stop: PeerStop)
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
                tokio::join!(src.flush_close_reply(), other.close_handshake(frame));
            }
            PeerStop::Failed => {
                tokio::join!(other.flush_and_shutdown(), src.flush_close_reply());
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
    let client_role = if mock_enabled {
        EndpointRole::ClientMock
    } else {
        EndpointRole::Client
    };
    let mut client = WsEndpoint::new(client_ws, client_role, io_timeout);
    let mut upstream = WsEndpoint::new(upstream_ws, EndpointRole::Upstream, io_timeout);

    // Phase 1: full-duplex relay with a single steady-state `Notify` registration.
    let reason = {
        let close_signal = shutdown_close_signal(shutdown);
        tokio::pin!(close_signal);

        std::future::poll_fn(|cx| {
            if close_signal.as_mut().poll(cx).is_ready() {
                return Poll::Ready(RelayStop::Shutdown);
            }
            let step = (|| -> Result<bool, RelayStop> {
                // 1. Advance any already-queued outbound frames first so freed slots are
                // immediately visible to both directional read loops below.
                upstream.poll_send_queued(cx)?;
                client.poll_send_queued(cx)?;

                // 2. Client -> Upstream (bounded batch per poll turn for scheduler fairness).
                let c2u_unread = pump(cx, &mut client, &mut upstream, RelayStop::Client)?;

                // 3. Upstream -> Client (independent full-duplex progress even when `upstream` sink is full).
                let u2c_unread = pump(cx, &mut upstream, &mut client, RelayStop::Upstream)?;

                // 4. Flush batched outbound frames once per direction & enforce stall deadlines.
                upstream.poll_flush_and_timer(cx)?;
                client.poll_flush_and_timer(cx)?;

                Ok((c2u_unread && upstream.can_accept() && client.can_read())
                    || (u2c_unread && client.can_accept() && upstream.can_read()))
            })();
            match step {
                Ok(true) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Ok(false) => Poll::Pending,
                Err(stop) => Poll::Ready(stop),
            }
        })
        .await
    };

    // Phase 2: single hard-deadline bound governing all close/drain paths, including
    // a shutdown signal that arrives while a peer-initiated close is already running.
    match reason {
        RelayStop::Abort => {}
        RelayStop::Client(stop) => {
            let _ = shutdown.bound(finish(client, upstream, stop)).await;
        }
        RelayStop::Upstream(stop) => {
            let _ = shutdown.bound(finish(upstream, client, stop)).await;
        }
        RelayStop::Shutdown => {
            let _ = shutdown
                .bound(async {
                    tokio::join!(
                        client.close_handshake(Some(GOING_AWAY_FRAME)),
                        upstream.close_handshake(Some(GOING_AWAY_FRAME)),
                    );
                })
                .await;
        }
    }
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
