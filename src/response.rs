//! Shared concrete HTTP body, bounded body reader, streaming idle-timeout wrapper,
//! and downstream response constructors.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{Response, StatusCode};
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
    inner: B,
    timeout: Duration,
    deadline: LazyDeadline,
    done: bool,
}

impl<B> IdleTimeoutBody<B> {
    const fn new(inner: B, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            deadline: LazyDeadline::new(),
            done: false,
        }
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
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                this.deadline.disarm();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.deadline.disarm();
                this.done = true;
                Poll::Ready(Some(Err(IdleTimeoutError::Inner(err))))
            }
            Poll::Ready(None) => {
                this.deadline.disarm();
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                if this.deadline.poll_expired(cx, this.timeout) {
                    this.done = true;
                    Poll::Ready(Some(Err(IdleTimeoutError::IdleTimeout)))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

enum ResBodyInner {
    Bytes(Bytes),
    Incoming(Incoming),
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

    /// Wrap a non-empty upstream [`Incoming`] stream with an optional per-frame idle timeout.
    pub(crate) const fn streaming(body: Incoming, idle_timeout: Option<Duration>) -> Self {
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
            ResBodyInner::Timeout(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            ResBodyInner::Bytes(bytes) => bytes.is_empty(),
            ResBodyInner::Incoming(body) => body.is_end_stream(),
            ResBodyInner::Timeout(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            ResBodyInner::Bytes(bytes) => SizeHint::with_exact(bytes.len() as u64),
            ResBodyInner::Incoming(body) => body.size_hint(),
            ResBodyInner::Timeout(body) => body.size_hint(),
        }
    }
}

/// Typed failure returned by [`drain_limited_body`], [`collect_limited_body`],
/// and [`collect_limited_body_until`].
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum BodyReadError<E> {
    TooLarge,
    TimedOut,
    Body(E),
}

enum BodyAcc {
    Drain,
    Empty,
    One(Bytes),
    Many(BytesMut),
}

impl BodyAcc {
    fn push(&mut self, chunk: Bytes) {
        match self {
            Self::Drain => {}
            Self::Empty => *self = Self::One(chunk),
            Self::One(first) => {
                let mut buf = BytesMut::with_capacity(first.len() + chunk.len());
                buf.extend_from_slice(first);
                buf.extend_from_slice(&chunk);
                *self = Self::Many(buf);
            }
            Self::Many(buf) => buf.extend_from_slice(&chunk),
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Drain | Self::Empty => Bytes::new(),
            Self::One(bytes) => bytes,
            Self::Many(buf) => buf.freeze(),
        }
    }
}

async fn read_body_limited<B>(
    mut body: B,
    limit: usize,
    mut acc: BodyAcc,
    mut deadline_at: impl FnMut() -> Instant,
) -> Result<BodyAcc, BodyReadError<B::Error>>
where
    B: Body<Data = Bytes> + Unpin,
{
    if body.size_hint().lower() > limit as u64 {
        return Err(BodyReadError::TooLarge);
    }
    if body.is_end_stream() {
        return Ok(acc);
    }
    let mut remaining = limit;
    let mut deadline = LazyDeadline::new();
    std::future::poll_fn(|cx| {
        loop {
            match Pin::new(&mut body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(chunk) = frame.into_data()
                        && !chunk.is_empty()
                    {
                        let Some(next) = remaining.checked_sub(chunk.len()) else {
                            return Poll::Ready(Err(BodyReadError::TooLarge));
                        };
                        remaining = next;
                        acc.push(chunk);
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(BodyReadError::Body(err))),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => {
                    if deadline.poll_expired_with(cx, &mut deadline_at) {
                        return Poll::Ready(Err(BodyReadError::TimedOut));
                    }
                    return Poll::Pending;
                }
            }
        }
    })
    .await?;
    Ok(acc)
}

/// Drain an [`Incoming`] body frame-by-frame up to `limit` bytes within `timeout`
/// without buffering its payload (keeping HTTP/1.1 keep-alive connections synchronized).
pub(crate) async fn drain_limited_body(
    body: Incoming,
    limit: usize,
    timeout: Duration,
) -> Result<(), BodyReadError<hyper::Error>> {
    read_body_limited(body, limit, BodyAcc::Drain, move || {
        Instant::now() + timeout
    })
    .await
    .map(|_| ())
}

/// Collect an [`Incoming`] body up to `limit` bytes within `timeout` (zero-copy on
/// single-frame bodies and allocating `Sleep` lazily only if the stream pends).
pub(crate) async fn collect_limited_body(
    body: Incoming,
    limit: usize,
    timeout: Duration,
) -> Result<Bytes, BodyReadError<hyper::Error>> {
    read_body_limited(body, limit, BodyAcc::Empty, move || {
        Instant::now() + timeout
    })
    .await
    .map(BodyAcc::into_bytes)
}

/// Collect an [`Incoming`] body up to `limit` bytes before an already-computed `deadline`.
pub(crate) async fn collect_limited_body_until(
    body: Incoming,
    limit: usize,
    deadline: Instant,
) -> Result<Bytes, BodyReadError<hyper::Error>> {
    read_body_limited(body, limit, BodyAcc::Empty, move || deadline)
        .await
        .map(BodyAcc::into_bytes)
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
            assert!(body.frame().await.is_none(), "body must fuse after timeout");
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-1"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Another 7s (14s total > 10s, but < 10s since chunk-1) must succeed.
        tokio::time::advance(Duration::from_secs(7)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-2"))))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Stalling > 10s with tx still open triggers the idle timeout.
        tokio::time::advance(Duration::from_secs(11)).await;
        reader.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn read_body_limited_handles_zero_copy_multi_frame_limits_errors_and_deadlines() {
        // Empty and single-frame bodies return zero-copy without allocating a Sleep timer.
        let empty = read_body_limited(empty_body(), 16, BodyAcc::Empty, || {
            Instant::now() + Duration::from_secs(1)
        })
        .await
        .unwrap()
        .into_bytes();
        assert!(empty.is_empty());

        let single_payload = Bytes::from_static(b"hello");
        let single =
            read_body_limited(full_body(single_payload.clone()), 5, BodyAcc::Empty, || {
                Instant::now() + Duration::from_secs(1)
            })
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(single, single_payload);

        // Upfront size_hint().lower() > limit rejects immediately as TooLarge.
        assert!(matches!(
            read_body_limited(
                full_body(Bytes::from_static(b"12345")),
                4,
                BodyAcc::Empty,
                || Instant::now() + Duration::from_secs(1),
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
        let multi = read_body_limited(ChannelBody::<&'static str>(rx), 5, BodyAcc::Empty, || {
            Instant::now() + Duration::from_secs(1)
        })
        .await
        .unwrap()
        .into_bytes();
        assert_eq!(multi, Bytes::from_static(b"abcde"));

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Frame::data(Bytes::from_static(b"12345"))))
            .await
            .unwrap();
        drop(tx);
        assert!(
            read_body_limited(ChannelBody::<&'static str>(rx), 5, BodyAcc::Drain, || {
                Instant::now() + Duration::from_secs(1)
            })
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
            read_body_limited(ChannelBody::<&'static str>(rx), 4, BodyAcc::Empty, || {
                Instant::now() + Duration::from_secs(1)
            })
            .await,
            Err(BodyReadError::TooLarge)
        ));

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Err("broken chunk")).await.unwrap();
        drop(tx);
        assert!(matches!(
            read_body_limited(ChannelBody(rx), 16, BodyAcc::Empty, || {
                Instant::now() + Duration::from_secs(1)
            })
            .await,
            Err(BodyReadError::Body("broken chunk"))
        ));

        // Both relative lazy Duration and fixed Instant deadlines time out stalled streams
        // without resetting between partial chunks (total-deadline semantics).
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let fixed_deadline = Instant::now() + Duration::from_secs(10);
        let reader = tokio::spawn(async move {
            read_body_limited(
                ChannelBody::<&'static str>(rx),
                16,
                BodyAcc::Empty,
                move || fixed_deadline,
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
}
