//! Inbound TCP listener round-robin acceptance, protocol-preface framing, and
//! graceful HTTP/1.1 + HTTP/2 connection drivers.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::TryAcquireError;
use tracing::{debug, trace, warn};

use crate::state::AppState;
use crate::{BoxError, lifecycle, proxy, transport};

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP1_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(65);

async fn accept_round_robin(
    listeners: &[TcpListener],
    next_idx: &mut usize,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    std::future::poll_fn(|cx| {
        let len = listeners.len();
        for offset in 0..len {
            let idx = (*next_idx + offset) % len;
            if let Poll::Ready(res) = listeners[idx].poll_accept(cx) {
                *next_idx = (idx + 1) % len;
                return Poll::Ready(res);
            }
        }
        Poll::Pending
    })
    .await
}

/// Replays stack-buffered initial bytes consumed during pre-preface detection
/// before delegating directly to the underlying socket with zero heap allocation.
struct PrefixedIo<I> {
    inner: I,
    buf: [u8; 64],
    pos: u8,
    len: u8,
}

impl<I: AsyncRead + Unpin> AsyncRead for PrefixedIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        read_buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if this.pos < this.len {
            let remaining = &this.buf[usize::from(this.pos)..usize::from(this.len)];
            let to_copy = remaining.len().min(read_buf.remaining());
            read_buf.put_slice(&remaining[..to_copy]);
            this.pos += to_copy as u8;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, read_buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetectedProto {
    Http1,
    Http2,
}

/// Bounds silent (`0`-byte) and partial HTTP/2-preface (`"PRI ..."`) sockets by
/// `timeout` and classifies HTTP/1 vs HTTP/2 in a single stack-only pass.
async fn read_initial_preface<I: AsyncRead + Unpin>(
    mut stream: I,
    timeout: Duration,
    lifecycle: &lifecycle::Lifecycle,
) -> Option<(DetectedProto, PrefixedIo<I>)> {
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    let read_preface = async {
        loop {
            let n = stream.read(&mut buf[len..]).await.ok()?;
            if n == 0 {
                return None;
            }
            len += n;
            // Keep reading within the initial deadline only while the buffered
            // bytes are still an incomplete prefix of the 24-byte HTTP/2 preface.
            if len >= H2_PREFACE.len() || !H2_PREFACE.starts_with(&buf[..len]) {
                return Some(());
            }
        }
    };
    tokio::select! {
        biased;
        _ = lifecycle.deadline() => None,
        res = tokio::time::timeout(timeout, read_preface) => match res {
            Ok(Some(())) => {
                let proto = if buf[..len].starts_with(H2_PREFACE) {
                    DetectedProto::Http2
                } else {
                    DetectedProto::Http1
                };
                Some((
                    proto,
                    PrefixedIo {
                        inner: stream,
                        buf,
                        pos: 0,
                        len: len as u8,
                    },
                ))
            }
            _ => None,
        },
    }
}

/// Pre-configured HTTP/1.1 and HTTP/2 server builders constructed once so
/// accepted connections share `TokioTimer` handles and the drain-tracked
/// [`lifecycle::Lifecycle`] without per-connection setup.
struct ServerBuilders {
    http1: http1::Builder,
    http2: http2::Builder<lifecycle::Lifecycle>,
    lifecycle: lifecycle::Lifecycle,
}

impl ServerBuilders {
    fn new(lifecycle: lifecycle::Lifecycle) -> Self {
        let mut http1 = http1::Builder::new();
        http1
            .timer(TokioTimer::new())
            .header_read_timeout(Some(HTTP1_HEADER_READ_TIMEOUT));
        let mut http2 = http2::Builder::new(lifecycle.clone());
        http2
            .timer(TokioTimer::new())
            .enable_connect_protocol()
            .max_concurrent_streams(256)
            .keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10));
        Self {
            http1,
            http2,
            lifecycle,
        }
    }
}

async fn drive_graceful<C>(
    conn: C,
    graceful_shutdown: impl FnOnce(Pin<&mut C>),
    conn_lifecycle: &lifecycle::Lifecycle,
    peer: std::net::SocketAddr,
) where
    C: Future<Output = hyper::Result<()>>,
{
    tokio::pin!(conn);
    tokio::select! {
        biased;
        deadline = conn_lifecycle.deadline() => {
            graceful_shutdown(conn.as_mut());
            match tokio::time::timeout_at(deadline, conn).await {
                Ok(Ok(())) => trace!(%peer, "connection closed after shutdown"),
                Ok(Err(e)) => debug!(error = %e, %peer, "connection closed after shutdown"),
                Err(_) => trace!(%peer, "connection drain deadline reached"),
            }
        }
        res = conn.as_mut() => match res {
            Ok(()) => trace!(%peer, "connection closed"),
            Err(e) => debug!(error = %e, %peer, "connection closed"),
        },
    }
}

async fn serve_detected<I, B, S>(
    builders: &ServerBuilders,
    proto: DetectedProto,
    io: TokioIo<PrefixedIo<I>>,
    svc: S,
    peer: std::net::SocketAddr,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: hyper::service::Service<
            http::Request<hyper::body::Incoming>,
            Response = http::Response<B>,
            Error = std::convert::Infallible,
        >,
    S::Future: Send + 'static,
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    match proto {
        DetectedProto::Http1 => {
            let conn = builders.http1.serve_connection(io, svc).with_upgrades();
            drive_graceful(
                conn,
                http1::UpgradeableConnection::graceful_shutdown,
                &builders.lifecycle,
                peer,
            )
            .await;
        }
        DetectedProto::Http2 => {
            Box::pin(drive_graceful(
                builders.http2.serve_connection(io, svc),
                http2::Connection::graceful_shutdown,
                &builders.lifecycle,
                peer,
            ))
            .await;
        }
    }
}

/// Serve one accepted TCP stream. Once polled, `builders` is dropped after `stream`,
/// `_guard`, `permit`, and `state`, keeping the server's drain barrier alive until
/// connection cleanup finishes (including on early preface timeouts).
async fn serve_connection(
    builders: Arc<ServerBuilders>,
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _guard = state.metrics.active_tcp();
    let Some((proto, prefixed)) =
        read_initial_preface(stream, INITIAL_READ_TIMEOUT, &builders.lifecycle).await
    else {
        trace!(%peer, "connection closed before protocol preface");
        return;
    };
    let io = TokioIo::new(prefixed);
    let connection = Arc::new(proxy::Connection::new(
        state,
        &builders.lifecycle,
        peer,
        permit,
    ));
    let svc = service_fn(move |req| Arc::clone(&connection).dispatch(req));
    serve_detected(&builders, proto, io, svc, peer).await;
}

pub(crate) async fn accept_loop(
    listeners: Vec<TcpListener>,
    state: Arc<AppState>,
    lifecycle: lifecycle::Lifecycle,
) {
    if listeners.is_empty() {
        return;
    }
    let builders = Arc::new(ServerBuilders::new(lifecycle));
    let shutdown = builders.lifecycle.deadline();
    tokio::pin!(shutdown);
    let mut next_idx = 0usize;
    let mut saturated = false;
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => return,
            accept = accept_round_robin(&listeners, &mut next_idx) => match accept {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "accept failed");
                    // Back off so a persistent error (e.g. fd exhaustion)
                    // doesn't busy-spin a core retrying immediately.
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => return,
                        () = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    }
                }
            },
        };
        // Acquire a permit AFTER accepting an actual TCP connection so idle listeners
        // never pre-hoard permits needed by HTTP/2 Extended CONNECT streams or inflate
        // `ethonyx_connection_limit_saturated_total` before real contention occurs.
        let permit = match state.try_acquire_permit() {
            Ok(permit) => {
                saturated = false;
                permit
            }
            Err(TryAcquireError::Closed) => return,
            Err(TryAcquireError::NoPermits) => {
                if !saturated {
                    saturated = true;
                    warn!(
                        max_connections = state.opts.max_connections.map(NonZeroUsize::get),
                        "connection limit reached; queueing accepts"
                    );
                }
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    res = Arc::clone(&state.conn_limit).acquire_owned() => match res {
                        Ok(permit) => permit,
                        Err(_) => return,
                    },
                }
            }
        };
        transport::configure_tcp_stream(&stream);
        // Connection lifecycle is fine-grained, high-frequency detail, so
        // it lives at trace (accept/close) and on a dedicated `access_log`
        // target (the negotiated protocol + first request line) rather
        // than the main info log — which stays reserved for lifecycle and
        // state changes. See the `access_log` config flag.
        trace!(%peer, "connection accepted");
        tokio::spawn(serve_connection(
            Arc::clone(&builders),
            stream,
            peer,
            Arc::clone(&state),
            permit,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Response;
    use http_body_util::Full;
    use std::convert::Infallible;
    use tokio::io::AsyncWriteExt;

    #[tokio::test(start_paused = true)]
    async fn preface_timeout_evicts_silent_and_partial_pri_while_http1_keepalive_lasts_65s() {
        let (_shutdown_tx, lifecycle, _drain) = lifecycle::channel();

        // 1. Completely silent 0-byte socket times out at INITIAL_READ_TIMEOUT (15s).
        let (_silent_client, silent_server) = tokio::io::duplex(256);
        let lc1 = lifecycle.clone();
        let silent_task = tokio::spawn(async move {
            read_initial_preface(silent_server, INITIAL_READ_TIMEOUT, &lc1)
                .await
                .is_none()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(14)).await;
        tokio::task::yield_now().await;
        assert!(!silent_task.is_finished());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(silent_task.await.unwrap());

        // 2. Partial "PRI " H2-preface prefix socket ALSO times out at 15s instead of
        // hanging forever waiting for the rest of the 24-byte preface.
        let (mut pri_client, pri_server) = tokio::io::duplex(256);
        pri_client.write_all(b"PRI ").await.unwrap();
        let lc2 = lifecycle.clone();
        let pri_task = tokio::spawn(async move {
            read_initial_preface(pri_server, INITIAL_READ_TIMEOUT, &lc2)
                .await
                .is_none()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(14)).await;
        tokio::task::yield_now().await;
        assert!(!pri_task.is_finished());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(pri_task.await.unwrap());

        // 2b. Complete 24-byte HTTP/2 preface is classified as DetectedProto::Http2.
        let (mut h2_client, h2_server) = tokio::io::duplex(256);
        h2_client.write_all(H2_PREFACE).await.unwrap();
        let (h2_proto, _) = read_initial_preface(h2_server, INITIAL_READ_TIMEOUT, &lifecycle)
            .await
            .expect("HTTP/2 preface");
        assert_eq!(h2_proto, DetectedProto::Http2);

        // 3. HTTP/1.1 connection completes preface immediately, survives 30s idle (> 15s),
        // and closes after 65s of keep-alive inactivity via `serve_detected`.
        let (mut h1_client, h1_server) = tokio::io::duplex(4096);
        let builders = ServerBuilders::new(lifecycle);
        let peer: std::net::SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let server_task = tokio::spawn(async move {
            let (proto, prefixed) =
                read_initial_preface(h1_server, INITIAL_READ_TIMEOUT, &builders.lifecycle)
                    .await
                    .expect("HTTP/1.1 preface");
            assert_eq!(proto, DetectedProto::Http1);
            let svc = service_fn(|_req| async {
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
            });
            serve_detected(&builders, proto, TokioIo::new(prefixed), svc, peer).await;
        });

        h1_client
            .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut resp_buf = [0u8; 256];
        let n = h1_client.read(&mut resp_buf).await.unwrap();
        assert!(resp_buf[..n].starts_with(b"HTTP/1.1 200 OK"));

        // Idle for 30s (> old 15s timeout, < new 65s keep-alive idle timeout): still alive!
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert!(!server_task.is_finished());

        h1_client
            .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let n = h1_client.read(&mut resp_buf).await.unwrap();
        assert!(resp_buf[..n].starts_with(b"HTTP/1.1 200 OK"));

        // Idle for 66s (> 65s): server closes the idle keep-alive socket!
        tokio::time::advance(Duration::from_secs(66)).await;
        server_task.await.unwrap();

        // 4. Triggering shutdown cancels a silent socket during `read_initial_preface`
        // immediately instead of waiting for `INITIAL_READ_TIMEOUT` or the drain deadline.
        let (early_tx, early_lc, early_drain) = lifecycle::channel();
        let (_early_client, early_server) = tokio::io::duplex(256);
        let early_task = tokio::spawn(async move {
            read_initial_preface(early_server, INITIAL_READ_TIMEOUT, &early_lc)
                .await
                .is_none()
        });
        tokio::task::yield_now().await;
        assert!(!early_task.is_finished());
        early_tx.send(tokio::time::Instant::now() + Duration::from_secs(30));
        assert!(early_task.await.unwrap());
        early_drain.wait().await;
    }
}
