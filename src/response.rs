//! Shared concrete HTTP body, bounded body reader, streaming idle-timeout wrapper,
//! and downstream response constructors.

use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{Response, StatusCode, Version};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::time::Instant;

use crate::lifecycle::LazyDeadline;

const PROMETHEUS_CONTENT_TYPE: HeaderValue =
    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");
const JSON_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("application/json");
const TEXT_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");

/// Zero-allocation error type for streaming HTTP bodies.
#[derive(Debug)]
pub(crate) enum IdleTimeoutError<E> {
    Inner(E),
    IdleTimeout,
}

impl<E: fmt::Display> fmt::Display for IdleTimeoutError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(err) => err.fmt(f),
            Self::IdleTimeout => f.write_str("response body idle timeout"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for IdleTimeoutError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Inner(err) => Some(err),
            Self::IdleTimeout => None,
        }
    }
}

/// Streaming response body wrapper that enforces a per-frame idle timeout while
/// preserving `Body::size_hint` and `Body::is_end_stream` (keeping `Content-Length`
/// intact on HTTP/1.1 responses) and allocating `Sleep` lazily only if `Pending`.
struct IdleTimeoutBody<B> {
    /// Cleared on EOF, error, or timeout so `Option<Incoming>` uses `Incoming`'s
    /// niche (no separate `done: bool` padding) and releases inner resources immediately.
    inner: Option<B>,
    timeout: Duration,
    deadline: LazyDeadline,
}

impl<B> IdleTimeoutBody<B> {
    const fn new(inner: B, timeout: Duration) -> Self {
        Self {
            inner: Some(inner),
            timeout,
            deadline: LazyDeadline::new(),
        }
    }

    fn finish(&mut self) {
        self.inner = None;
        self.deadline = LazyDeadline::new();
    }
}

impl<B> Body for IdleTimeoutBody<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = IdleTimeoutError<B::Error>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                this.deadline.disarm();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.finish();
                Poll::Ready(Some(Err(IdleTimeoutError::Inner(err))))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Pending => {
                if this.deadline.poll_expired(cx, this.timeout) {
                    this.finish();
                    Poll::Ready(Some(Err(IdleTimeoutError::IdleTimeout)))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(Body::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), Body::size_hint)
    }
}

enum ResBodyInner {
    Bytes(Bytes),
    Incoming(Incoming),
    /// Buffered prefix bytes replayed ahead of the remaining live [`Incoming`] stream
    /// (used when `--mock-eth-syncing` exceeds its inspection cap and falls back to streaming).
    Prefix {
        prefix: Bytes,
        rest: Incoming,
    },
    Timeout(IdleTimeoutBody<Incoming>),
}

/// Unboxed concrete HTTP body shared across local responses, health probes,
/// passthrough request streams, and idle-bounded upstream response streams.
pub(crate) struct ResBody(ResBodyInner);

impl ResBody {
    pub(crate) fn incoming(body: Incoming) -> Self {
        if body.is_end_stream() {
            empty_body()
        } else {
            Self(ResBodyInner::Incoming(body))
        }
    }

    /// Replay `prefix` ahead of the remaining `rest` stream without buffering the rest.
    pub(crate) fn prefixed(prefix: Bytes, rest: Incoming) -> Self {
        if prefix.is_empty() {
            Self::incoming(rest)
        } else if rest.is_end_stream() {
            full_body(prefix)
        } else {
            Self(ResBodyInner::Prefix { prefix, rest })
        }
    }

    /// Wrap an upstream [`Incoming`] stream with an optional per-frame idle timeout,
    /// collapsing already-empty bodies to [`empty_body()`].
    pub(crate) fn streaming(body: Incoming, idle_timeout: Option<Duration>) -> Self {
        if body.is_end_stream() {
            return empty_body();
        }
        match idle_timeout {
            Some(timeout) => Self(ResBodyInner::Timeout(IdleTimeoutBody::new(body, timeout))),
            None => Self(ResBodyInner::Incoming(body)),
        }
    }
}

impl Body for ResBody {
    type Data = Bytes;
    type Error = IdleTimeoutError<hyper::Error>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.0 {
            ResBodyInner::Bytes(bytes) => {
                if bytes.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Frame::data(std::mem::take(bytes)))))
                }
            }
            ResBodyInner::Incoming(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|opt| opt.map(|res| res.map_err(IdleTimeoutError::Inner))),
            ResBodyInner::Prefix { prefix, rest } => {
                if !prefix.is_empty() {
                    Poll::Ready(Some(Ok(Frame::data(std::mem::take(prefix)))))
                } else {
                    Pin::new(rest)
                        .poll_frame(cx)
                        .map(|opt| opt.map(|res| res.map_err(IdleTimeoutError::Inner)))
                }
            }
            ResBodyInner::Timeout(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            ResBodyInner::Bytes(bytes) => bytes.is_empty(),
            ResBodyInner::Incoming(body) => body.is_end_stream(),
            ResBodyInner::Prefix { prefix, rest } => prefix.is_empty() && rest.is_end_stream(),
            ResBodyInner::Timeout(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            ResBodyInner::Bytes(bytes) => SizeHint::with_exact(bytes.len() as u64),
            ResBodyInner::Incoming(body) => body.size_hint(),
            ResBodyInner::Prefix { prefix, rest } => {
                prefix_size_hint(prefix.len(), rest.size_hint())
            }
            ResBodyInner::Timeout(body) => body.size_hint(),
        }
    }
}

/// Widen `upper` before raising `lower` so `SizeHint::set_lower`'s `lower <= upper`
/// assertion never panics when `rest` carries a bounded upper hint (e.g. `with_exact`).
fn prefix_size_hint(prefix_len: usize, mut hint: SizeHint) -> SizeHint {
    let p = prefix_len as u64;
    if let Some(upper) = hint.upper() {
        hint.set_upper(upper.saturating_add(p));
    }
    hint.set_lower(hint.lower().saturating_add(p));
    hint
}

/// Typed failure returned by [`drain_body`], [`collect_body`], and [`collect_prefix_or_stream`].
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum BodyReadError<E> {
    TooLarge,
    TimedOut,
    Body(E),
}

enum BodyAcc {
    Empty,
    One(Bytes),
    Many(BytesMut),
}

impl BodyAcc {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(bytes) => bytes.len(),
            Self::Many(buf) => buf.len(),
        }
    }

    fn push(&mut self, chunk: Bytes, hint: usize) {
        match self {
            Self::Empty => *self = Self::One(chunk),
            Self::One(first) => {
                let cap = first.len().saturating_add(chunk.len()).max(hint);
                let mut buf = BytesMut::with_capacity(cap);
                buf.extend_from_slice(first);
                buf.extend_from_slice(&chunk);
                *self = Self::Many(buf);
            }
            Self::Many(buf) => buf.extend_from_slice(&chunk),
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Empty => Bytes::new(),
            Self::One(bytes) => bytes,
            Self::Many(buf) => buf.freeze(),
        }
    }
}

/// Poll non-empty data frames from `body` (optionally bounded by `deadline`),
/// invoking `on_chunk` until the stream ends (`Ok(false)`) or `on_chunk` breaks
/// early (`Ok(true)`).
async fn drive_frames<B>(
    body: &mut B,
    deadline: Option<Instant>,
    mut on_chunk: impl FnMut(Bytes) -> ControlFlow<()>,
) -> Result<bool, BodyReadError<B::Error>>
where
    B: Body<Data = Bytes> + Unpin,
{
    if body.is_end_stream() {
        return Ok(false);
    }
    let fut = async {
        while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
        {
            let frame = frame.map_err(BodyReadError::Body)?;
            if let Ok(chunk) = frame.into_data()
                && !chunk.is_empty()
                && on_chunk(chunk).is_break()
            {
                return Ok(true);
            }
        }
        Ok(false)
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, fut)
            .await
            .unwrap_or(Err(BodyReadError::TimedOut)),
        None => fut.await,
    }
}

async fn read_limited<B>(
    mut body: B,
    limit: usize,
    deadline: Option<Instant>,
    mut on_chunk: impl FnMut(Bytes),
) -> Result<(), BodyReadError<B::Error>>
where
    B: Body<Data = Bytes> + Unpin,
{
    if body.size_hint().lower() > limit as u64 {
        return Err(BodyReadError::TooLarge);
    }
    let mut remaining = limit;
    if drive_frames(&mut body, deadline, |chunk| {
        let Some(next) = remaining.checked_sub(chunk.len()) else {
            return ControlFlow::Break(());
        };
        remaining = next;
        on_chunk(chunk);
        ControlFlow::Continue(())
    })
    .await?
    {
        Err(BodyReadError::TooLarge)
    } else {
        Ok(())
    }
}

/// Drain an [`Incoming`] body frame-by-frame up to `limit` bytes before `deadline`
/// without buffering its payload (keeping HTTP/1.1 keep-alive connections synchronized).
pub(crate) async fn drain_body(
    body: Incoming,
    limit: usize,
    deadline: Instant,
) -> Result<(), BodyReadError<hyper::Error>> {
    read_limited(body, limit, Some(deadline), |_| {}).await
}

const NON_2XX_DRAIN_BYTES: usize = 8 * 1024;
const NON_2XX_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

async fn drain_http1_error_body_with<B>(version: Version, body: B, deadline: Instant)
where
    B: Body<Data = Bytes> + Unpin,
{
    if version == Version::HTTP_11 && !body.is_end_stream() {
        let drain_deadline = deadline.min(Instant::now() + NON_2XX_DRAIN_TIMEOUT);
        let _ = read_limited(body, NON_2XX_DRAIN_BYTES, Some(drain_deadline), |_| {}).await;
    }
}

/// Opportunistically drain small HTTP/1.1 non-2xx error bodies so Hyper keeps the
/// TCP+TLS connection pooled, without buffering untrusted error bodies or burning
/// the full request timeout on stalled peers.
pub(crate) async fn drain_http1_error_body(version: Version, body: Incoming, deadline: Instant) {
    drain_http1_error_body_with(version, body, deadline).await;
}

const MAX_PREALLOC_HINT: usize = 16 * 1024;

/// Cap upfront `BytesMut` preallocation so a large declared `Content-Length` from an
/// untrusted peer cannot force a multi-MiB allocation before actual frames arrive.
fn prealloc_hint(lower: u64, limit: usize) -> usize {
    usize::try_from(lower)
        .unwrap_or(limit)
        .min(limit)
        .min(MAX_PREALLOC_HINT)
}

/// Collect an [`Incoming`] body up to `limit` bytes, optionally bounded by `deadline`
/// (zero-copy on single-frame bodies; callers enforcing an outer deadline pass `None`).
pub(crate) async fn collect_body(
    body: Incoming,
    limit: usize,
    deadline: Option<Instant>,
) -> Result<Bytes, BodyReadError<hyper::Error>> {
    let hint = prealloc_hint(body.size_hint().lower(), limit);
    let mut acc = BodyAcc::Empty;
    read_limited(body, limit, deadline, |chunk| acc.push(chunk, hint)).await?;
    Ok(acc.into_bytes())
}

/// Result of inspecting a request body up to a prefix cap before deciding whether
/// to mock locally or stream upstream.
pub(crate) enum PrefixBody<B = Incoming> {
    /// Complete request body finished within the inspection cap.
    Complete(Bytes),
    /// Request body exceeded the inspection cap; replay `prefix` ahead of the live `rest`
    /// stream without buffering the full payload.
    Overflow { prefix: Bytes, rest: B },
}

/// Read up to `limit` bytes of `body` before `deadline`. If the stream ends within
/// `limit`, returns [`PrefixBody::Complete`]; as soon as the known lower bound or
/// accumulated frames exceed `limit`, stops buffering immediately and returns
/// [`PrefixBody::Overflow`] preserving every byte read so far.
pub(crate) async fn collect_prefix_or_stream<B>(
    mut body: B,
    limit: usize,
    deadline: Instant,
) -> Result<PrefixBody<B>, BodyReadError<B::Error>>
where
    B: Body<Data = Bytes> + Unpin,
{
    let lower = body.size_hint().lower();
    if lower > limit as u64 {
        return Ok(PrefixBody::Overflow {
            prefix: Bytes::new(),
            rest: body,
        });
    }
    let hint = prealloc_hint(lower, limit);
    let mut acc = BodyAcc::Empty;
    let overflowed = drive_frames(&mut body, Some(deadline), |chunk| {
        acc.push(chunk, hint);
        if acc.len() > limit {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .await?;

    let bytes = acc.into_bytes();
    if overflowed {
        Ok(PrefixBody::Overflow {
            prefix: bytes,
            rest: body,
        })
    } else {
        Ok(PrefixBody::Complete(bytes))
    }
}

pub(crate) const fn full_body(bytes: Bytes) -> ResBody {
    ResBody(ResBodyInner::Bytes(bytes))
}

pub(crate) const fn empty_body() -> ResBody {
    ResBody(ResBodyInner::Bytes(Bytes::new()))
}

pub(crate) fn status_response(code: StatusCode) -> Response<ResBody> {
    let mut resp = Response::new(empty_body());
    *resp.status_mut() = code;
    resp
}

fn typed_response(code: StatusCode, content_type: HeaderValue, body: Bytes) -> Response<ResBody> {
    let mut resp = Response::new(full_body(body));
    *resp.status_mut() = code;
    resp.headers_mut().insert(CONTENT_TYPE, content_type);
    resp
}

pub(crate) fn prometheus_response(body: Bytes) -> Response<ResBody> {
    typed_response(StatusCode::OK, PROMETHEUS_CONTENT_TYPE, body)
}

pub(crate) fn raw_json_response(code: StatusCode, body: Bytes) -> Response<ResBody> {
    typed_response(code, JSON_CONTENT_TYPE, body)
}

pub(crate) fn empty_json_response(code: StatusCode) -> Response<ResBody> {
    raw_json_response(code, Bytes::new())
}

pub(crate) fn text_response(code: StatusCode, msg: &'static str) -> Response<ResBody> {
    typed_response(code, TEXT_CONTENT_TYPE, Bytes::from_static(msg.as_bytes()))
}

pub(crate) fn uri_too_long_response() -> Response<ResBody> {
    text_response(StatusCode::URI_TOO_LONG, "request uri too long")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;

    #[tokio::test]
    async fn res_body_bytes_preserves_size_hint_and_end_stream() {
        let empty = empty_body();
        assert!(empty.is_end_stream());
        assert_eq!(empty.size_hint().exact(), Some(0));
        assert!(empty.collect().await.unwrap().to_bytes().is_empty());

        let payload = Bytes::from_static(b"hello");
        let mut body = full_body(payload.clone());
        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(5));
        assert_eq!((&mut body).collect().await.unwrap().to_bytes(), payload);
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));

        // `prefix_size_hint` widens `upper` before `lower` so exact/bounded hints never panic.
        assert_eq!(
            prefix_size_hint(7, SizeHint::with_exact(10)).exact(),
            Some(17)
        );
        assert_eq!(
            prefix_size_hint(7, SizeHint::with_exact(u64::MAX)).exact(),
            Some(u64::MAX)
        );
        let mut bounded = SizeHint::new();
        bounded.set_upper(20);
        bounded.set_lower(10);
        let widened = prefix_size_hint(5, bounded);
        assert_eq!(widened.lower(), 15);
        assert_eq!(widened.upper(), Some(25));
        let unbounded = prefix_size_hint(5, SizeHint::new());
        assert_eq!(unbounded.lower(), 5);
        assert_eq!(unbounded.upper(), None);
    }

    #[tokio::test]
    async fn idle_timeout_body_preserves_size_hint_and_avoids_timer_when_immediately_ready() {
        let payload = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}");
        let mut body = IdleTimeoutBody::new(Full::new(payload.clone()), Duration::from_secs(5));
        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(payload.len() as u64));

        let collected = (&mut body).collect().await.unwrap().to_bytes();
        assert_eq!(collected, payload);
        assert!(
            !body.deadline.is_allocated(),
            "immediate bodies must not allocate Sleep"
        );
        assert!(body.is_end_stream());

        let empty = IdleTimeoutBody::new(Full::new(Bytes::new()), Duration::from_secs(5));
        assert!(empty.is_end_stream());
        assert_eq!(empty.size_hint().exact(), Some(0));
    }

    struct ChannelBody<E = Infallible>(tokio::sync::mpsc::Receiver<Result<Frame<Bytes>, E>>);

    impl<E> Body for ChannelBody<E> {
        type Data = Bytes;
        type Error = E;

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
        let mut body = IdleTimeoutBody::new(ChannelBody::<Infallible>(rx), Duration::from_secs(10));

        let reader = tokio::spawn(async move {
            let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(first, Bytes::from_static(b"chunk-1"));
            let second = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(second, Bytes::from_static(b"chunk-2"));
            let err = body.frame().await.unwrap().unwrap_err();
            assert_eq!(err.to_string(), "response body idle timeout");
            assert!(
                !body.deadline.is_allocated(),
                "finish() must release Sleep immediately on timeout"
            );
            assert!(body.is_end_stream());
            assert_eq!(body.size_hint().exact(), Some(0));
            assert!(body.frame().await.is_none(), "body must fuse after timeout");
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-1"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Another 7s (14s total > 10s, but < 10s since chunk-1) must succeed.
        // Using `sleep` (not `advance`) steps through intermediate timer expirations so
        // an un-disarmed deadline at t=10s would wake `reader` and fail the test.
        tokio::time::sleep(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-2"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Stalling > 10s with tx still open triggers the idle timeout and drops inner.
        tokio::time::sleep(Duration::from_secs(11)).await;
        reader.await.unwrap();
        assert!(tx.is_closed(), "finish() must drop inner body on timeout");
    }

    #[test]
    fn prealloc_hint_caps_at_limit_and_16kib_ceiling() {
        assert_eq!(prealloc_hint(0, 64 * 1024), 0);
        assert_eq!(prealloc_hint(1024, 64 * 1024), 1024);
        assert_eq!(
            prealloc_hint(MAX_PREALLOC_HINT as u64, 64 * 1024),
            MAX_PREALLOC_HINT
        );
        assert_eq!(prealloc_hint(64 * 1024, 64 * 1024), MAX_PREALLOC_HINT);
        assert_eq!(prealloc_hint(u64::MAX, 64 * 1024), MAX_PREALLOC_HINT);
        assert_eq!(prealloc_hint(64 * 1024, 4096), 4096);
    }

    #[tokio::test(start_paused = true)]
    async fn read_limited_handles_zero_copy_multi_frame_limits_errors_and_deadlines() {
        let deadline = || Instant::now() + Duration::from_secs(1);

        // Empty and single-frame bodies return zero-copy (both timer-free and deadline-bounded).
        let mut empty = BodyAcc::Empty;
        read_limited(empty_body(), 16, None, |c| empty.push(c, 0))
            .await
            .unwrap();
        assert!(empty.into_bytes().is_empty());

        let single_payload = Bytes::from_static(b"hello");
        let mut single = BodyAcc::Empty;
        read_limited(
            full_body(single_payload.clone()),
            5,
            Some(deadline()),
            |c| single.push(c, 5),
        )
        .await
        .unwrap();
        assert_eq!(single.into_bytes(), single_payload);

        // Upfront size_hint().lower() > limit rejects immediately as TooLarge.
        assert!(matches!(
            read_limited(
                full_body(Bytes::from_static(b"12345")),
                4,
                Some(deadline()),
                |_| {},
            )
            .await,
            Err(BodyReadError::TooLarge)
        ));

        // Multi-frame streaming: concatenates chunks up to limit, and drains without buffering.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"ab"))))
            .await
            .unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"cde"))))
            .await
            .unwrap();
        drop(tx);
        let mut multi = BodyAcc::Empty;
        read_limited(ChannelBody::<&'static str>(rx), 5, Some(deadline()), |c| {
            multi.push(c, 5)
        })
        .await
        .unwrap();
        assert_eq!(multi.into_bytes(), Bytes::from_static(b"abcde"));

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"12345"))))
            .await
            .unwrap();
        drop(tx);
        assert!(
            read_limited(ChannelBody::<&'static str>(rx), 5, Some(deadline()), |_| {})
                .await
                .is_ok()
        );

        // Streaming overflow and inner read errors map cleanly without BoxError downcasting.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
            .await
            .unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"de"))))
            .await
            .unwrap();
        drop(tx);
        assert!(matches!(
            read_limited(ChannelBody::<&'static str>(rx), 4, Some(deadline()), |_| {}).await,
            Err(BodyReadError::TooLarge)
        ));

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Err("broken chunk")).await.unwrap();
        drop(tx);
        assert!(matches!(
            read_limited(ChannelBody(rx), 16, Some(deadline()), |_| {}).await,
            Err(BodyReadError::Body("broken chunk"))
        ));

        // Total deadline bounds stalled streams without resetting between partial chunks.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let fixed_deadline = Instant::now() + Duration::from_secs(10);
        let reader = tokio::spawn(async move {
            let mut acc = BodyAcc::Empty;
            read_limited(
                ChannelBody::<&'static str>(rx),
                16,
                Some(fixed_deadline),
                |c| acc.push(c, 0),
            )
            .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"part"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        // Another 5s (11s total > 10s deadline, < 10s since last chunk) must time out.
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(
            reader.await.unwrap(),
            Err(BodyReadError::TimedOut)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn collect_prefix_or_stream_handles_size_hint_complete_overflow_and_timeout() {
        let deadline = || Instant::now() + Duration::from_secs(1);

        // Known size_hint().lower() > limit transitions immediately to Overflow without buffering.
        match collect_prefix_or_stream(full_body(Bytes::from_static(b"12345")), 4, deadline())
            .await
            .unwrap()
        {
            PrefixBody::Overflow { prefix, rest } => {
                assert!(prefix.is_empty());
                assert_eq!(rest.collect().await.unwrap().to_bytes(), b"12345"[..]);
            }
            PrefixBody::Complete(_) => panic!("expected Overflow for size_hint > limit"),
        }

        // Empty and multi-frame bodies within limit return Complete.
        assert!(matches!(
            collect_prefix_or_stream(empty_body(), 4, deadline()).await,
            Ok(PrefixBody::Complete(b)) if b.is_empty()
        ));
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"ab"))))
            .await
            .unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"cd"))))
            .await
            .unwrap();
        drop(tx);
        assert!(matches!(
            collect_prefix_or_stream(ChannelBody::<&'static str>(rx), 4, deadline()).await,
            Ok(PrefixBody::Complete(b)) if b == b"abcd"[..]
        ));

        // Streaming overflow stops buffering on the crossing frame and preserves unread remainder.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"ab"))))
            .await
            .unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"cde"))))
            .await
            .unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"fg"))))
            .await
            .unwrap();
        drop(tx);
        match collect_prefix_or_stream(ChannelBody::<&'static str>(rx), 4, deadline())
            .await
            .unwrap()
        {
            PrefixBody::Overflow { prefix, rest } => {
                assert_eq!(prefix, Bytes::from_static(b"abcde"));
                assert_eq!(
                    rest.collect().await.unwrap().to_bytes(),
                    Bytes::from_static(b"fg")
                );
            }
            PrefixBody::Complete(_) => panic!("expected streaming Overflow"),
        }

        // Stalled prefix read respects deadline.
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, &'static str>>(4);
        let d = Instant::now() + Duration::from_secs(5);
        let task = tokio::spawn(collect_prefix_or_stream(ChannelBody(rx), 16, d));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(matches!(task.await.unwrap(), Err(BodyReadError::TimedOut)));
    }

    #[tokio::test(start_paused = true)]
    async fn drain_http1_error_body_respects_version_byte_cap_and_timeout_budget() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingBody<B> {
            inner: B,
            polls: Arc<AtomicUsize>,
            bytes: Arc<AtomicUsize>,
        }

        impl<B: Body<Data = Bytes> + Unpin> Body for CountingBody<B> {
            type Data = Bytes;
            type Error = B::Error;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                self.polls.fetch_add(1, Ordering::Relaxed);
                let res = Pin::new(&mut self.inner).poll_frame(cx);
                if let Poll::Ready(Some(Ok(frame))) = &res
                    && let Some(data) = frame.data_ref()
                {
                    self.bytes.fetch_add(data.len(), Ordering::Relaxed);
                }
                res
            }
        }

        let make_counting = |cap: usize| {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(cap);
            let polls = Arc::new(AtomicUsize::new(0));
            let bytes = Arc::new(AtomicUsize::new(0));
            let body = CountingBody {
                inner: ChannelBody(rx),
                polls: Arc::clone(&polls),
                bytes: Arc::clone(&bytes),
            };
            (tx, body, polls, bytes)
        };

        // 1. HTTP/2 bodies are skipped immediately without polling any frames.
        let (tx, body, polls, bytes) = make_counting(2);
        tx.send(Ok(Frame::data(Bytes::from_static(b"ignored"))))
            .await
            .unwrap();
        drop(tx);
        drain_http1_error_body_with(
            Version::HTTP_2,
            body,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert_eq!(polls.load(Ordering::Relaxed), 0);
        assert_eq!(bytes.load(Ordering::Relaxed), 0);

        // 2. HTTP/1.1 body up to `NON_2XX_DRAIN_BYTES` (`8 KiB`) is drained completely.
        let (tx, body, polls, bytes) = make_counting(4);
        let chunk = Bytes::from(vec![b'a'; 4096]);
        tx.send(Ok(Frame::data(chunk.clone()))).await.unwrap();
        tx.send(Ok(Frame::data(chunk.clone()))).await.unwrap();
        drop(tx);
        drain_http1_error_body_with(
            Version::HTTP_11,
            body,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert_eq!(bytes.load(Ordering::Relaxed), NON_2XX_DRAIN_BYTES);
        assert_eq!(polls.load(Ordering::Relaxed), 3); // 2 data frames + EOF

        // 3. HTTP/1.1 body exceeding `NON_2XX_DRAIN_BYTES` aborts on the crossing frame
        // without polling subsequent queued frames.
        let (tx, body, polls, _bytes) = make_counting(4);
        for _ in 0..4 {
            tx.send(Ok(Frame::data(chunk.clone()))).await.unwrap();
        }
        drop(tx);
        drain_http1_error_body_with(
            Version::HTTP_11,
            body,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert_eq!(polls.load(Ordering::Relaxed), 3); // 4th frame must never be polled

        // 4. Stalled HTTP/1.1 error body caps at `NON_2XX_DRAIN_TIMEOUT` (`250ms`) when the
        // outer deadline is longer, and honors a shorter outer deadline (`50ms`) otherwise.
        let (_tx, body, _polls, _bytes) = make_counting(2);
        let start = Instant::now();
        drain_http1_error_body_with(Version::HTTP_11, body, start + Duration::from_secs(10)).await;
        assert_eq!(start.elapsed(), NON_2XX_DRAIN_TIMEOUT);

        let (_tx2, body2, _polls2, _bytes2) = make_counting(2);
        let start2 = Instant::now();
        let short = Duration::from_millis(50);
        drain_http1_error_body_with(Version::HTTP_11, body2, start2 + short).await;
        assert_eq!(start2.elapsed(), short);
    }
}
