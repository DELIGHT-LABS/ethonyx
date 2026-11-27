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
use tracing::{debug, trace, warn};

use crate::state::{AppState, ConnLimit, ConnPermitGuard};
use crate::{BoxError, lifecycle, proxy, transport};

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const PREFACE_BUF_LEN: usize = 64;
const _: () = assert!(H2_PREFACE.len() <= PREFACE_BUF_LEN && PREFACE_BUF_LEN <= u8::MAX as usize);
const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP1_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(65);
const ADMIN_MAX_CONNECTIONS: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(n) => n,
    None => unreachable!(),
};
const ADMIN_INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(5);
const ADMIN_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);
const ADMIN_MAX_CONN_LIFETIME: Duration = Duration::from_secs(10);
const ADMIN_CLOSE_GRACE: Duration = Duration::from_secs(1);
const _: () = assert!(ADMIN_CLOSE_GRACE.as_nanos() < ADMIN_MAX_CONN_LIFETIME.as_nanos());

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
    buf: [u8; PREFACE_BUF_LEN],
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
async fn read_initial_preface<I>(
    mut stream: I,
    timeout: Duration,
    lifecycle: &lifecycle::Lifecycle,
) -> Option<(DetectedProto, PrefixedIo<I>)>
where
    I: AsyncRead + Unpin,
{
    if lifecycle.current_deadline().is_some() {
        return None;
    }
    let mut buf = [0u8; PREFACE_BUF_LEN];
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
    // Poll `read_preface` before `lifecycle.deadline()` so sockets whose initial
    // bytes are already buffered in `SO_RCVBUF` complete with zero `Notify` locks.
    let res = tokio::select! {
        biased;
        res = tokio::time::timeout(timeout, read_preface) => res,
        _ = lifecycle.deadline() => return None,
    };
    match res {
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
    fn new_data(lifecycle: lifecycle::Lifecycle, enable_ws: bool) -> Self {
        let mut http1 = http1::Builder::new();
        http1
            .timer(TokioTimer::new())
            .header_read_timeout(Some(HTTP1_HEADER_READ_TIMEOUT));
        let mut http2 = http2::Builder::new(lifecycle.clone());
        http2
            .timer(TokioTimer::new())
            .max_concurrent_streams(256)
            .keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10));
        if enable_ws {
            // Advertise RFC 8441 SETTINGS_ENABLE_CONNECT_PROTOCOL only when
            // WebSocket forwarding is configured.
            http2.enable_connect_protocol();
        }
        Self {
            http1,
            http2,
            lifecycle,
        }
    }

    /// Dedicated builders for `--admin-listen`: short header timeout, immediate
    /// HTTP/1.1 connection close (`keep_alive(false)`), and a conservative HTTP/2
    /// stream cap without background PING keepalive.
    fn new_admin(lifecycle: lifecycle::Lifecycle) -> Self {
        let mut http1 = http1::Builder::new();
        http1
            .timer(TokioTimer::new())
            .keep_alive(false)
            .header_read_timeout(Some(ADMIN_HEADER_READ_TIMEOUT));
        let mut http2 = http2::Builder::new(lifecycle.clone());
        http2.max_concurrent_streams(32);
        Self {
            http1,
            http2,
            lifecycle,
        }
    }
}

/// Bounded connection lifetime for `--admin-listen`: sends a graceful shutdown
/// signal (`GOAWAY` on HTTP/2) at `graceful` and hard-evicts at `hard` so idle
/// clients cannot wedge the 64-slot admin pool.
#[derive(Clone, Copy, Debug)]
struct AdminLifetime {
    graceful: tokio::time::Instant,
    hard: tokio::time::Instant,
}

impl AdminLifetime {
    fn from_start(started: tokio::time::Instant) -> Self {
        Self {
            graceful: started + (ADMIN_MAX_CONN_LIFETIME - ADMIN_CLOSE_GRACE),
            hard: started + ADMIN_MAX_CONN_LIFETIME,
        }
    }

    fn shutdown_deadline(self, sd: tokio::time::Instant) -> tokio::time::Instant {
        sd.min(tokio::time::Instant::now() + ADMIN_CLOSE_GRACE)
            .min(self.hard)
    }
}

fn log_conn_closed(res: hyper::Result<()>, peer: std::net::SocketAddr, msg: &'static str) {
    match res {
        Ok(()) => trace!(%peer, "{msg}"),
        Err(error) => debug!(%error, %peer, "{msg}"),
    }
}

async fn drive_graceful<C>(
    conn: C,
    graceful_shutdown: impl FnOnce(Pin<&mut C>),
    lifecycle: &lifecycle::Lifecycle,
    peer: std::net::SocketAddr,
) where
    C: Future<Output = hyper::Result<()>>,
{
    tokio::pin!(conn);
    let deadline = if let Some(deadline) = lifecycle.current_deadline() {
        deadline
    } else {
        // Scope `lifecycle.deadline()` inside `select!` so its `Notified + Option<Waker>`
        // drops before the `timeout_at` drain await, and poll `conn` first so already-closed
        // connections skip `Notify` waiter registration.
        tokio::select! {
            biased;
            res = conn.as_mut() => {
                log_conn_closed(res, peer, "connection closed");
                return;
            }
            deadline = lifecycle.deadline() => deadline,
        }
    };
    graceful_shutdown(conn.as_mut());
    match tokio::time::timeout_at(deadline, conn).await {
        Ok(res) => log_conn_closed(res, peer, "connection closed after shutdown"),
        Err(_) => trace!(%peer, "connection drain deadline reached"),
    }
}

/// Admin-plane connection driver: enforces [`AdminLifetime`] (`9s` graceful
/// shutdown -> `10s` hard eviction) and bounds global-shutdown drain to
/// [`ADMIN_CLOSE_GRACE`] (`1s`) using a single stack-pinned `Sleep` (zero heap
/// allocations; skips timer-wheel registration when an HTTP/1.1 reply completes
/// on its first poll).
async fn drive_admin_graceful<C>(
    conn: C,
    graceful_shutdown: impl FnOnce(Pin<&mut C>),
    lifecycle: &lifecycle::Lifecycle,
    peer: std::net::SocketAddr,
    lifetime: AdminLifetime,
) where
    C: Future<Output = hyper::Result<()>>,
{
    tokio::pin!(conn);
    let sleep = tokio::time::sleep_until(lifetime.graceful);
    tokio::pin!(sleep);

    let (drain_deadline, from_lifetime) = if let Some(sd) = lifecycle.current_deadline() {
        (lifetime.shutdown_deadline(sd), false)
    } else {
        tokio::select! {
            biased;
            res = conn.as_mut() => {
                log_conn_closed(res, peer, "admin connection closed");
                return;
            }
            sd = lifecycle.deadline() => (lifetime.shutdown_deadline(sd), false),
            () = sleep.as_mut() => (lifetime.hard, true),
        }
    };

    graceful_shutdown(conn.as_mut());
    sleep.as_mut().reset(drain_deadline);

    if from_lifetime {
        let sd = tokio::select! {
            biased;
            res = conn.as_mut() => {
                log_conn_closed(res, peer, "admin connection closed after lifetime GOAWAY");
                return;
            }
            () = sleep.as_mut() => {
                trace!(%peer, "admin connection max lifetime reached");
                return;
            }
            sd = lifecycle.deadline() => sd,
        };
        if sd < drain_deadline {
            sleep.as_mut().reset(sd);
        }
    }

    tokio::select! {
        biased;
        res = conn.as_mut() => log_conn_closed(res, peer, "admin connection closed after shutdown"),
        () = sleep.as_mut() => {
            trace!(%peer, "admin connection drain deadline reached");
        }
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
    permit: ConnPermitGuard,
    probes: proxy::ListenProbes,
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
        probes,
    ));
    let svc = service_fn(move |req| Arc::clone(&connection).dispatch(req));
    serve_detected(&builders, proto, io, svc, peer).await;
}

async fn serve_admin_io<I>(
    builders: &ServerBuilders,
    stream: I,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let lifetime = AdminLifetime::from_start(tokio::time::Instant::now());
    let Some((proto, prefixed)) =
        read_initial_preface(stream, ADMIN_INITIAL_READ_TIMEOUT, &builders.lifecycle).await
    else {
        trace!(%peer, "admin connection closed before protocol preface");
        return;
    };
    let io = TokioIo::new(prefixed);
    let svc = service_fn(move |req| {
        std::future::ready(Ok::<_, std::convert::Infallible>(proxy::dispatch_admin(
            &state, &req,
        )))
    });
    match proto {
        DetectedProto::Http1 => {
            drive_admin_graceful(
                builders.http1.serve_connection(io, svc),
                http1::Connection::graceful_shutdown,
                &builders.lifecycle,
                peer,
                lifetime,
            )
            .await;
        }
        DetectedProto::Http2 => {
            Box::pin(drive_admin_graceful(
                builders.http2.serve_connection(io, svc),
                http2::Connection::graceful_shutdown,
                &builders.lifecycle,
                peer,
                lifetime,
            ))
            .await;
        }
    }
}

/// Serve one accepted TCP stream on `--admin-listen`. `builders` is declared first
/// so the drain barrier stays armed until `stream`, `state`, and `_permit` drop.
async fn serve_admin_connection(
    builders: Arc<ServerBuilders>,
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
    _permit: ConnPermitGuard,
) {
    serve_admin_io(&builders, stream, peer, state).await;
}

enum AcceptTarget {
    Data(proxy::ListenProbes),
    Admin(ConnLimit),
}

impl AcceptTarget {
    fn try_acquire(&self, state: &AppState) -> Option<ConnPermitGuard> {
        match self {
            Self::Data(_) => state.try_acquire_permit(),
            Self::Admin(limit) => limit.try_acquire(),
        }
    }

    async fn acquire(&self, state: &AppState) -> Option<ConnPermitGuard> {
        match self {
            Self::Data(_) => state.conn_limit.acquire().await,
            Self::Admin(limit) => limit.acquire().await,
        }
    }

    fn log_accept_error(&self, error: &std::io::Error) {
        match self {
            Self::Data(_) => warn!(%error, "accept failed"),
            Self::Admin(_) => warn!(%error, "admin accept failed"),
        }
    }

    fn log_transient_accept_error(&self, error: &std::io::Error) {
        match self {
            Self::Data(_) => debug!(%error, "transient accept error; retrying"),
            Self::Admin(_) => debug!(%error, "transient admin accept error; retrying"),
        }
    }

    fn log_saturated(&self, state: &AppState) {
        match self {
            Self::Data(_) => warn!(
                max_connections = state.conn_limit.max().map(NonZeroUsize::get),
                "connection limit reached; queueing accepts"
            ),
            Self::Admin(limit) => warn!(
                max_connections = limit.max().map(NonZeroUsize::get),
                "admin connection limit reached; queueing accepts"
            ),
        }
    }

    fn spawn(
        &self,
        builders: &Arc<ServerBuilders>,
        stream: tokio::net::TcpStream,
        peer: std::net::SocketAddr,
        state: &Arc<AppState>,
        permit: ConnPermitGuard,
    ) {
        match self {
            Self::Data(probes) => {
                // Connection lifecycle is fine-grained, high-frequency detail, so
                // it lives at trace (accept/close) and on a dedicated `access_log`
                // target (per-request peer, version, method, path, upstream, status,
                // and latency) rather than the main info log — which stays reserved
                // for lifecycle and state changes. See the `access_log` config flag.
                trace!(%peer, "connection accepted");
                tokio::spawn(serve_connection(
                    Arc::clone(builders),
                    stream,
                    peer,
                    Arc::clone(state),
                    permit,
                    *probes,
                ));
            }
            Self::Admin(_) => {
                trace!(%peer, "admin connection accepted");
                tokio::spawn(serve_admin_connection(
                    Arc::clone(builders),
                    stream,
                    peer,
                    Arc::clone(state),
                    permit,
                ));
            }
        }
    }
}

/// Classify per-connection or signal-interrupted `accept()` errors that are safe
/// to retry immediately without the 100ms backoff reserved for persistent OS /
/// file-descriptor exhaustion (`EMFILE`/`ENFILE` are never one of the four kinds
/// below, whatever `ErrorKind` the toolchain reports for them, so they always fail
/// closed into backoff).
fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::Interrupted
    )
}

async fn run_accept_loop(
    listeners: &[TcpListener],
    state: Arc<AppState>,
    builders: Arc<ServerBuilders>,
    target: AcceptTarget,
) {
    if listeners.is_empty() {
        return;
    }
    let shutdown = builders.lifecycle.deadline();
    tokio::pin!(shutdown);
    let mut next_idx = 0usize;
    let mut saturated = false;
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => return,
            accept = accept_round_robin(listeners, &mut next_idx) => match accept {
                Ok(pair) => pair,
                Err(e) if is_transient_accept_error(&e) => {
                    target.log_transient_accept_error(&e);
                    continue;
                }
                Err(e) => {
                    target.log_accept_error(&e);
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
        let permit = match target.try_acquire(&state) {
            Some(permit) => {
                saturated = false;
                permit
            }
            None => {
                if !saturated {
                    saturated = true;
                    target.log_saturated(&state);
                }
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    permit = target.acquire(&state) => match permit {
                        Some(permit) => permit,
                        None => return,
                    },
                }
            }
        };
        transport::configure_tcp_stream(&stream);
        target.spawn(&builders, stream, peer, &state, permit);
    }
}

pub(crate) async fn accept_loop(
    listeners: Vec<TcpListener>,
    state: Arc<AppState>,
    lifecycle: lifecycle::Lifecycle,
    probes: proxy::ListenProbes,
) {
    let builders = Arc::new(ServerBuilders::new_data(lifecycle, state.el_ws.is_some()));
    run_accept_loop(&listeners, state, builders, AcceptTarget::Data(probes)).await;
}

/// Dedicated accept loop for `--admin-listen` with its own bounded concurrency pool,
/// isolated from `--max-connections` saturation and data-plane TCP gauges.
pub(crate) async fn accept_admin_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    lifecycle: lifecycle::Lifecycle,
) {
    let builders = Arc::new(ServerBuilders::new_admin(lifecycle));
    let limit = ConnLimit::new(Some(ADMIN_MAX_CONNECTIONS));
    run_accept_loop(
        std::slice::from_ref(&listener),
        state,
        builders,
        AcceptTarget::Admin(limit),
    )
    .await;
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
        let builders = ServerBuilders::new_data(lifecycle, false);
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
        // immediately instead of waiting for `INITIAL_READ_TIMEOUT` or the drain deadline,
        // and an already-active shutdown rejects even a socket with buffered bytes upfront.
        let (early_tx, early_lc, early_drain) = lifecycle::channel();
        let post_lc = early_lc.clone();
        let (_early_client, early_server) = tokio::io::duplex(256);
        let early_task = tokio::spawn(async move {
            read_initial_preface(early_server, INITIAL_READ_TIMEOUT, &early_lc)
                .await
                .is_none()
        });
        tokio::task::yield_now().await;
        assert!(!early_task.is_finished());
        let before = tokio::time::Instant::now();
        early_tx.send(before + Duration::from_secs(30));
        assert!(early_task.await.unwrap());
        // The paused clock auto-advances to `INITIAL_READ_TIMEOUT` when nothing else can run,
        // so `is_none()` alone also holds if shutdown never reaches the read.
        assert_eq!(tokio::time::Instant::now(), before);

        let (mut buffered_client, buffered_server) = tokio::io::duplex(64);
        buffered_client
            .write_all(b"GET / HTTP/1.1\r\n")
            .await
            .unwrap();
        assert!(
            read_initial_preface(buffered_server, INITIAL_READ_TIMEOUT, &post_lc)
                .await
                .is_none()
        );
        drop(post_lc);
        early_drain.wait().await;
    }

    async fn admin_test_state() -> Arc<AppState> {
        use clap::Parser;
        let cfg = crate::Config::parse_from([
            "ethonyx",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
        ]);
        let validated = cfg.parse_validated().unwrap();
        let (state, _poller) = AppState::start(
            validated.opts,
            validated.max_connections,
            validated.upstreams,
            validated.health,
            crate::metrics::ServerMetrics::disabled(),
        )
        .await;
        state
    }

    fn contains_h2_frame_type(mut data: &[u8], expected_type: u8) -> bool {
        while data.len() >= 9 {
            let payload_len =
                usize::from(data[0]) << 16 | usize::from(data[1]) << 8 | usize::from(data[2]);
            let frame_type = data[3];
            let total = 9 + payload_len;
            if data.len() < total {
                break;
            }
            if frame_type == expected_type {
                return true;
            }
            data = &data[total..];
        }
        false
    }

    #[tokio::test(start_paused = true)]
    async fn admin_connections_close_h1_immediately_evict_slowloris_at_5s_and_bound_h2c_at_10s() {
        use http_body_util::BodyExt;

        let state = admin_test_state().await;
        let peer: std::net::SocketAddr = "127.0.0.1:41000".parse().unwrap();
        let (_shutdown_tx, lifecycle, _drain) = lifecycle::channel();
        let builders = Arc::new(ServerBuilders::new_admin(lifecycle));

        // 1. HTTP/1.1 disables keep-alive (`Connection: close`) and closes immediately after one reply.
        let (mut h1_client, h1_server) = tokio::io::duplex(4096);
        let b1 = Arc::clone(&builders);
        let s1 = Arc::clone(&state);
        let h1_task = tokio::spawn(async move { serve_admin_io(&b1, h1_server, peer, s1).await });
        h1_client
            .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        h1_client.read_to_end(&mut raw).await.unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(
            text.to_ascii_lowercase().contains("connection: close"),
            "admin HTTP/1.1 must disable keep-alive, got: {text}"
        );
        assert!(text.ends_with("\r\n\r\nok"), "got: {text}");
        h1_task.await.unwrap();

        // 2. Silent 0-byte socket and 1-byte HTTP/1.1 Slowloris (`b"G"`) both evict at exact 5s.
        for payload in [&b""[..], &b"G"[..]] {
            let (mut slow_client, slow_server) = tokio::io::duplex(256);
            if !payload.is_empty() {
                slow_client.write_all(payload).await.unwrap();
            }
            let b = Arc::clone(&builders);
            let s = Arc::clone(&state);
            let start = tokio::time::Instant::now();
            let task = tokio::spawn(async move { serve_admin_io(&b, slow_server, peer, s).await });
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(4)).await;
            tokio::task::yield_now().await;
            assert!(
                !task.is_finished(),
                "payload {payload:?} must not evict before 5s"
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            task.await.unwrap();
            assert_eq!(
                start.elapsed(),
                ADMIN_HEADER_READ_TIMEOUT,
                "payload {payload:?} must evict at exact 5s without auto-advance masking"
            );
            drop(slow_client);
        }

        // 3. Well-behaved idle `h2c` client receives graceful GOAWAY at 9s and closes before 10s.
        let (h2_client, h2_server) = tokio::io::duplex(8192);
        let b3 = Arc::clone(&builders);
        let s3 = Arc::clone(&state);
        let h2_start = tokio::time::Instant::now();
        let h2_server_task =
            tokio::spawn(async move { serve_admin_io(&b3, h2_server, peer, s3).await });
        let (mut sender, h2_conn) =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .handshake::<_, Full<Bytes>>(TokioIo::new(h2_client))
                .await
                .unwrap();
        let h2_client_task = tokio::spawn(async move {
            let _ = h2_conn.await;
        });
        let req = http::Request::builder()
            .version(http::Version::HTTP_2)
            .uri("http://localhost/livez")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            b"ok"[..]
        );

        tokio::time::advance(Duration::from_millis(8_900)).await;
        tokio::task::yield_now().await;
        assert!(!h2_server_task.is_finished());

        tokio::time::advance(Duration::from_millis(100)).await;
        h2_server_task.await.unwrap();
        h2_client_task.await.unwrap();
        assert_eq!(
            h2_start.elapsed(),
            ADMIN_MAX_CONN_LIFETIME - ADMIN_CLOSE_GRACE,
            "compliant h2c peer must close on the GOAWAY sent at exactly 9s"
        );

        // 4. Stubborn raw `h2c` peer ignoring GOAWAY receives GOAWAY at 9s and is hard-evicted at exact 10s.
        let (mut raw_client, raw_server) = tokio::io::duplex(8192);
        let b4 = Arc::clone(&builders);
        let s4 = Arc::clone(&state);
        let raw_start = tokio::time::Instant::now();
        let raw_task = tokio::spawn(async move { serve_admin_io(&b4, raw_server, peer, s4).await });
        // Client preface + empty SETTINGS frame (type 0x04, len 0).
        raw_client.write_all(H2_PREFACE).await.unwrap();
        raw_client
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut h2_wire = Vec::new();
        let mut chunk = [0u8; 256];
        // Drain server's initial SETTINGS / SETTINGS ACK.
        tokio::task::yield_now().await;
        let n = raw_client.read(&mut chunk).await.unwrap();
        h2_wire.extend_from_slice(&chunk[..n]);

        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;
        assert!(
            !raw_task.is_finished(),
            "stubborn h2c peer should stay inside 1s post-GOAWAY window until 10s"
        );
        // Server must have emitted a GOAWAY frame (type 0x07) at 9s, then close at exact 10s.
        tokio::time::advance(Duration::from_secs(1)).await;
        raw_task.await.unwrap();
        assert_eq!(
            raw_start.elapsed(),
            ADMIN_MAX_CONN_LIFETIME,
            "stubborn h2c peer must evict at exact 10s without auto-advance masking"
        );
        raw_client.read_to_end(&mut h2_wire).await.unwrap();
        assert!(
            contains_h2_frame_type(&h2_wire, 0x07),
            "expected RFC 9113 GOAWAY (type 0x07) before hard eviction, got {} bytes",
            h2_wire.len()
        );

        // 5. Server shutdown with zero grace during the `[9s, 10s]` post-GOAWAY window
        // preempts immediately without waiting for the remaining `ADMIN_CLOSE_GRACE`.
        let (sd_tx, sd_lc, sd_drain) = lifecycle::channel();
        let sd_builders = Arc::new(ServerBuilders::new_admin(sd_lc));
        let (mut sd_client, sd_server) = tokio::io::duplex(8192);
        let b5 = Arc::clone(&sd_builders);
        let s5 = Arc::clone(&state);
        let sd_task = tokio::spawn(async move { serve_admin_io(&b5, sd_server, peer, s5).await });
        sd_client.write_all(H2_PREFACE).await.unwrap();
        sd_client
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let _ = sd_client.read(&mut chunk).await.unwrap();

        tokio::time::advance(Duration::from_millis(9_200)).await;
        tokio::task::yield_now().await;
        assert!(!sd_task.is_finished());

        let before_shutdown = tokio::time::Instant::now();
        sd_tx.send(before_shutdown);
        sd_task.await.unwrap();
        assert_eq!(
            tokio::time::Instant::now(),
            before_shutdown,
            "zero-grace shutdown during [9s, 10s] window must preempt immediately"
        );
        drop(sd_builders);
        sd_drain.wait().await;

        // 5b. Server shutdown with a long grace (`30s`) arriving during the `[9s, 10s]`
        // post-GOAWAY window must never extend hard eviction past `ADMIN_MAX_CONN_LIFETIME` (`10s`).
        let (late_tx, late_lc, late_drain) = lifecycle::channel();
        let late_builders = Arc::new(ServerBuilders::new_admin(late_lc));
        let (mut late_client, late_server) = tokio::io::duplex(8192);
        let b5b = Arc::clone(&late_builders);
        let s5b = Arc::clone(&state);
        let late_task =
            tokio::spawn(async move { serve_admin_io(&b5b, late_server, peer, s5b).await });
        late_client.write_all(H2_PREFACE).await.unwrap();
        late_client
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let _ = late_client.read(&mut chunk).await.unwrap();

        tokio::time::advance(Duration::from_millis(9_200)).await;
        tokio::task::yield_now().await;
        assert!(!late_task.is_finished());

        late_tx.send(tokio::time::Instant::now() + Duration::from_secs(30));
        tokio::task::yield_now().await;
        assert!(!late_task.is_finished());

        tokio::time::advance(Duration::from_millis(799)).await;
        tokio::task::yield_now().await;
        assert!(
            !late_task.is_finished(),
            "hard eviction must not fire before 10s"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(
            late_task.is_finished(),
            "long-grace shutdown during [9s, 10s] window must not extend eviction past 10s"
        );
        late_task.await.unwrap();
        drop(late_builders);
        late_drain.wait().await;

        // 6. Global shutdown with a long `--shutdown-grace 30s` closes compliant `h2c`
        // immediately (`0ms`) and caps stubborn `h2c` peers at `ADMIN_CLOSE_GRACE` (`1s`).
        let (g_tx, g_lc, g_drain) = lifecycle::channel();
        let g_builders = Arc::new(ServerBuilders::new_admin(g_lc));

        let (ok_client, ok_server) = tokio::io::duplex(8192);
        let b6a = Arc::clone(&g_builders);
        let s6a = Arc::clone(&state);
        let ok_task = tokio::spawn(async move { serve_admin_io(&b6a, ok_server, peer, s6a).await });
        let (mut ok_sender, ok_conn) =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .handshake::<_, Full<Bytes>>(TokioIo::new(ok_client))
                .await
                .unwrap();
        let ok_driver = tokio::spawn(async move {
            let _ = ok_conn.await;
        });
        let ok_resp = ok_sender
            .send_request(
                http::Request::builder()
                    .version(http::Version::HTTP_2)
                    .uri("http://localhost/livez")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), http::StatusCode::OK);
        let _ = ok_resp.into_body().collect().await.unwrap();

        let (mut stuck_client, stuck_server) = tokio::io::duplex(8192);
        let b6b = Arc::clone(&g_builders);
        let s6b = Arc::clone(&state);
        let stuck_task =
            tokio::spawn(async move { serve_admin_io(&b6b, stuck_server, peer, s6b).await });
        stuck_client.write_all(H2_PREFACE).await.unwrap();
        stuck_client
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut stuck_wire = Vec::new();
        let n = stuck_client.read(&mut chunk).await.unwrap();
        stuck_wire.extend_from_slice(&chunk[..n]);

        tokio::time::advance(Duration::from_millis(200)).await;
        let shutdown_at = tokio::time::Instant::now();
        g_tx.send(shutdown_at + Duration::from_secs(30));

        ok_task.await.unwrap();
        ok_driver.await.unwrap();
        assert_eq!(
            shutdown_at.elapsed(),
            Duration::ZERO,
            "compliant h2c admin peer must close immediately on shutdown GOAWAY"
        );

        assert!(!stuck_task.is_finished());
        tokio::time::advance(ADMIN_CLOSE_GRACE).await;
        stuck_task.await.unwrap();
        assert_eq!(
            shutdown_at.elapsed(),
            ADMIN_CLOSE_GRACE,
            "stubborn h2c admin peer must evict at 1s ADMIN_CLOSE_GRACE, not 10s or 30s"
        );
        stuck_client.read_to_end(&mut stuck_wire).await.unwrap();
        assert!(contains_h2_frame_type(&stuck_wire, 0x07));

        drop(g_builders);
        g_drain.wait().await;

        // 7. Zero grace (`--shutdown-grace 0`) evicts a stubborn `h2c` peer immediately
        // instead of waiting out `ADMIN_CLOSE_GRACE`.
        let (z_tx, z_lc, z_drain) = lifecycle::channel();
        let z_builders = Arc::new(ServerBuilders::new_admin(z_lc));
        let (mut z_client, z_server) = tokio::io::duplex(8192);
        let b7 = Arc::clone(&z_builders);
        let s7 = Arc::clone(&state);
        let z_task = tokio::spawn(async move { serve_admin_io(&b7, z_server, peer, s7).await });
        z_client.write_all(H2_PREFACE).await.unwrap();
        z_client
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let _ = z_client.read(&mut chunk).await.unwrap();
        tokio::time::advance(Duration::from_millis(200)).await;
        let zero_at = tokio::time::Instant::now();
        z_tx.send(zero_at);
        z_task.await.unwrap();
        assert_eq!(
            zero_at.elapsed(),
            Duration::ZERO,
            "zero-grace shutdown must evict a stubborn h2c admin peer immediately"
        );
        drop(z_builders);
        z_drain.wait().await;
    }

    #[test]
    fn transient_accept_errors_retry_while_resource_and_unknown_errors_back_off() {
        use std::io::{Error, ErrorKind};

        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionRefused,
            ErrorKind::Interrupted,
        ] {
            assert!(
                is_transient_accept_error(&Error::from(kind)),
                "{kind:?} must retry immediately"
            );
        }
        for kind in [
            ErrorKind::Other,
            ErrorKind::OutOfMemory,
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
            ErrorKind::InvalidInput,
            ErrorKind::WouldBlock,
            ErrorKind::BrokenPipe,
        ] {
            assert!(
                !is_transient_accept_error(&Error::from(kind)),
                "{kind:?} must back off instead of tight-looping"
            );
        }
        // What `accept()` really returns when descriptors run out (`EMFILE` / `ENFILE`,
        // the same numbers on Linux and macOS); their `ErrorKind` is toolchain-dependent.
        #[cfg(unix)]
        for code in [24, 23] {
            assert!(
                !is_transient_accept_error(&Error::from_raw_os_error(code)),
                "os error {code} must back off instead of tight-looping"
            );
        }
    }
}
