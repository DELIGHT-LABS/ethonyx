//! Ethonyx — Ethereum EL/CL sidecar library entry.
//!
//! The binary in `src/main.rs` is a thin wrapper around [`run`]. Tests can call
//! [`run`] directly with a custom shutdown future to drive the full stack
//! in-process without spawning a subprocess.

#![forbid(unsafe_code)]

pub mod config;
mod headers;
mod health;
mod lifecycle;
mod metrics;
#[cfg(feature = "otel")]
pub mod otel;
mod proxy;
mod rpc;
mod state;
mod upstream;
mod websocket;

pub use config::Config;
use upstream::RedactedUrl;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{TryAcquireError, watch};
use tracing::{debug, info, trace, warn};

use crate::state::AppState;

use std::sync::OnceLock;
pub static PROMETHEUS_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    OnceLock::new();

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP1_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(65);

/// Bind every `cfg.listen` address, serve all routes, and wait for `shutdown`
/// before draining for `cfg.shutdown_grace` seconds.
pub async fn run<F>(cfg: Config, shutdown: F) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    let validated = cfg.parse_validated()?;
    #[cfg(feature = "otel")]
    {
        let telemetry = crate::otel::start(cfg.otel_endpoint.as_deref()).await?;
        let has_telemetry = telemetry.is_some();
        let res = run_inner(cfg, validated, shutdown, has_telemetry).await;
        if let Some(tp) = telemetry {
            tp.shutdown().await;
        }
        res
    }
    #[cfg(not(feature = "otel"))]
    {
        run_inner(cfg, validated, shutdown).await
    }
}

/// Run with providers initialized before the caller constructs its tracing layer.
#[cfg(feature = "otel")]
pub async fn run_with_telemetry<F>(
    cfg: Config,
    shutdown: F,
    telemetry: Option<otel::Telemetry>,
) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    let validated = match cfg.parse_validated() {
        Ok(v) => v,
        Err(e) => {
            if let Some(tp) = telemetry {
                tp.shutdown().await;
            }
            return Err(e);
        }
    };
    let has_telemetry = telemetry.is_some();
    let res = run_inner(cfg, validated, shutdown, has_telemetry).await;
    if let Some(tp) = telemetry {
        tp.shutdown().await;
    }
    res
}

async fn run_inner<F>(
    cfg: Config,
    mut validated: config::ValidatedConfig,
    shutdown: F,
    #[cfg(feature = "otel")] has_telemetry: bool,
) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    let listen_addrs = std::mem::take(&mut validated.listen);

    let prometheus_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    #[cfg(feature = "otel")]
    let otel_enabled = has_telemetry;
    #[cfg(not(feature = "otel"))]
    let otel_enabled = false;

    #[cfg(feature = "otel")]
    let recorder_installed = if has_telemetry {
        let fanout = metrics_util::layers::FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .add_recorder(crate::otel::OtelRecorder::new())
            .build();
        ::metrics::set_global_recorder(fanout).is_ok()
    } else {
        ::metrics::set_global_recorder(prometheus_recorder).is_ok()
    };
    #[cfg(not(feature = "otel"))]
    let recorder_installed = ::metrics::set_global_recorder(prometheus_recorder).is_ok();

    if recorder_installed {
        let _ = PROMETHEUS_HANDLE.set(prometheus_handle.clone());
        // The recorder is process-global: only its successful installer starts
        // upkeep, and it remains available across individual run() calls.
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                prometheus_handle.run_upkeep();
            }
        });
    }

    info!(
        version = concat!("v", env!("CARGO_PKG_VERSION")),
        git = env!("ETHONYX_GIT_DESCRIBE"),
        ?cfg,
        "starting ethonyx"
    );

    let poll_interval = cfg.health_poll_interval;
    let shutdown_grace = cfg.shutdown_grace;

    let (probe_tx, health) = if cfg.trust_upstream {
        (None, health::HealthSource::Trusted)
    } else {
        let (tx, rx) = watch::channel(Arc::new(health::Probe::pending()));
        (Some(tx), health::HealthSource::Polled(rx))
    };
    let (shutdown_tx, _) = watch::channel(None);

    let state = Arc::new(AppState::new(cfg, validated, otel_enabled, health)?);

    let mut initial_degraded = false;
    if let Some(ref tx) = probe_tx {
        // Warm the health cache with one poll before serving, then refresh it in
        // the background so /healthz and /readyz read a snapshot instead of
        // hitting upstream on every probe.
        let first_probe = Arc::new(health::probe_once(&state).await);
        let eval = health::evaluate_readiness(&state, &first_probe);
        health::update_metrics(&first_probe, eval);
        initial_degraded = first_probe.has_errors();
        let _ = tx.send(first_probe);
    }
    let startup_msg = if state.cfg.trust_upstream {
        "upstreams configured (trusted)"
    } else if initial_degraded {
        "upstreams configured (initial probe degraded)"
    } else {
        "upstreams connected"
    };
    info!(
        el_http = %RedactedUrl(&state.cfg.el_http_url),
        el_http_proto = %state.el_transport.startup_proto_label(),
        el_ws = %RedactedUrl(&state.cfg.el_ws_url),
        cl_beacon = %RedactedUrl(&state.cfg.cl_beacon_url),
        cl_mev_relay = %upstream::RedactedOptUrl(state.cfg.cl_mev_relay_url.as_deref()),
        cl_proto = %state.cl_transport.startup_proto_label(),
        "{startup_msg}"
    );

    let mut listeners = Vec::with_capacity(listen_addrs.len());
    for addr in &listen_addrs {
        let listener = TcpListener::bind(&**addr).await?;
        info!(listen = %addr, "listening");
        listeners.push(listener);
    }

    // Spawn the background poller only after all TCP listeners bind successfully
    // so a bind error never leaves a detached background poller running.
    let poller = probe_tx.map(|tx| {
        tokio::spawn(health::poll_loop(
            state.clone(),
            tx,
            shutdown_tx.subscribe(),
            poll_interval,
        ))
    });

    let (conn_tx, mut conn_rx) = tokio::sync::mpsc::channel::<()>(1);
    let lifecycle = lifecycle::Lifecycle::new(shutdown_tx.subscribe(), conn_tx);
    let accept_handle = tokio::spawn(accept_loop(listeners, state, lifecycle));

    shutdown.await;
    info!("shutdown signal received");

    let deadline = tokio::time::Instant::now() + shutdown_grace;
    shutdown_tx.send_replace(Some(deadline));

    let _ = accept_handle.await;
    if let Some(poller) = poller {
        let _ = poller.await;
    }

    info!(
        grace_secs = shutdown_grace.as_secs(),
        "draining connections"
    );
    // Each connection, HTTP/2 request and relay uses the same deadline. Wait for
    // their futures to be dropped even with zero grace; timing out this barrier
    // would let run() return before sockets and request tasks are released.
    let _ = conn_rx.recv().await;
    Ok(())
}

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
    lifecycle: &mut lifecycle::Lifecycle,
) -> Option<(DetectedProto, PrefixedIo<I>)> {
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    let deadline = tokio::time::Instant::now() + timeout;
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
        res = tokio::time::timeout_at(deadline, read_preface) => match res {
            Ok(Some(())) => {
                let proto = if len >= H2_PREFACE.len() && buf[..H2_PREFACE.len()] == *H2_PREFACE {
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
/// accepted connections share `TokioTimer` handles without per-connection setup.
#[derive(Clone)]
struct ServerBuilders {
    http1: http1::Builder,
    http2: http2::Builder<lifecycle::Lifecycle>,
}

impl ServerBuilders {
    fn new(lifecycle: lifecycle::Lifecycle) -> Self {
        let mut http1 = http1::Builder::new();
        http1
            .timer(TokioTimer::new())
            .header_read_timeout(Some(HTTP1_HEADER_READ_TIMEOUT));
        let mut http2 = http2::Builder::new(lifecycle);
        http2
            .timer(TokioTimer::new())
            .enable_connect_protocol()
            .max_concurrent_streams(256)
            .keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10));
        Self { http1, http2 }
    }
}

trait GracefulConn: Future<Output = hyper::Result<()>> {
    fn initiate_graceful_shutdown(self: Pin<&mut Self>);
}

impl<I, B, S> GracefulConn for http1::UpgradeableConnection<I, S>
where
    S: hyper::service::HttpService<hyper::body::Incoming, ResBody = B>,
    S::Error: Into<BoxError>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    B: hyper::body::Body + 'static,
    B::Error: Into<BoxError>,
{
    fn initiate_graceful_shutdown(self: Pin<&mut Self>) {
        self.graceful_shutdown();
    }
}

impl<I, B, S, E> GracefulConn for http2::Connection<I, S, E>
where
    S: hyper::service::HttpService<hyper::body::Incoming, ResBody = B>,
    S::Error: Into<BoxError>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin,
    B: hyper::body::Body + 'static,
    B::Error: Into<BoxError>,
    E: hyper::rt::bounds::Http2ServerConnExec<S::Future, B>,
{
    fn initiate_graceful_shutdown(self: Pin<&mut Self>) {
        self.graceful_shutdown();
    }
}

async fn drive_graceful<C: GracefulConn>(
    conn: C,
    conn_lifecycle: &mut lifecycle::Lifecycle,
    peer: std::net::SocketAddr,
) {
    tokio::pin!(conn);
    tokio::select! {
        res = conn.as_mut() => match res {
            Ok(()) => trace!(%peer, "connection closed"),
            Err(e) => debug!(error = %e, %peer, "connection closed"),
        },
        deadline = conn_lifecycle.deadline() => {
            conn.as_mut().initiate_graceful_shutdown();
            match tokio::time::timeout_at(deadline, conn).await {
                Ok(Ok(())) => trace!(%peer, "connection closed after shutdown"),
                Ok(Err(e)) => debug!(error = %e, %peer, "connection closed after shutdown"),
                Err(_) => trace!(%peer, "connection drain deadline reached"),
            }
        }
    }
}

async fn serve_detected<I, B, S>(
    builders: &ServerBuilders,
    proto: DetectedProto,
    io: TokioIo<PrefixedIo<I>>,
    svc: S,
    conn_lifecycle: &mut lifecycle::Lifecycle,
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
            drive_graceful(conn, conn_lifecycle, peer).await;
        }
        DetectedProto::Http2 => {
            let conn = builders.http2.serve_connection(io, svc);
            drive_graceful(conn, conn_lifecycle, peer).await;
        }
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
    mut conn_lifecycle: lifecycle::Lifecycle,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    builders: ServerBuilders,
) {
    let _guard = state.metrics.active_tcp();
    let Some((proto, prefixed)) =
        read_initial_preface(stream, INITIAL_READ_TIMEOUT, &mut conn_lifecycle).await
    else {
        trace!(%peer, "connection closed before protocol preface");
        return;
    };
    let io = TokioIo::new(prefixed);
    let connection = Arc::new(proxy::Connection::new(
        state,
        conn_lifecycle.clone(),
        peer,
        permit,
    ));
    let svc = service_fn(move |req| Arc::clone(&connection).dispatch(req));
    serve_detected(&builders, proto, io, svc, &mut conn_lifecycle, peer).await;
}

async fn accept_loop(
    listeners: Vec<TcpListener>,
    state: Arc<AppState>,
    mut lifecycle: lifecycle::Lifecycle,
) {
    if listeners.is_empty() {
        return;
    }
    let builders = ServerBuilders::new(lifecycle.clone());
    let mut next_idx = 0usize;
    let mut saturated = false;
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = lifecycle.deadline() => return,
            accept = accept_round_robin(&listeners, &mut next_idx) => match accept {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "accept failed");
                    // Back off so a persistent error (e.g. fd exhaustion)
                    // doesn't busy-spin a core retrying immediately.
                    tokio::select! {
                        biased;
                        _ = lifecycle.deadline() => return,
                        () = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    }
                }
            },
        };
        // Acquire a permit AFTER accepting an actual TCP connection so idle listeners
        // never pre-hoard permits needed by HTTP/2 Extended CONNECT streams or inflate
        // `ethonyx_connection_limit_saturated_total` before real contention occurs.
        let permit = match state.conn_limit.clone().try_acquire_owned() {
            Ok(permit) => {
                saturated = false;
                Arc::new(permit)
            }
            Err(TryAcquireError::Closed) => return,
            Err(TryAcquireError::NoPermits) => {
                state.metrics.inc_saturated();
                if !saturated {
                    saturated = true;
                    warn!(
                        max_connections = state.cfg.max_connections,
                        "connection limit reached; queueing accepts"
                    );
                }
                tokio::select! {
                    biased;
                    _ = lifecycle.deadline() => return,
                    res = state.conn_limit.clone().acquire_owned() => match res {
                        Ok(permit) => Arc::new(permit),
                        Err(_) => return,
                    },
                }
            }
        };
        if let Err(e) = stream.set_nodelay(true) {
            debug!(error = %e, "set_nodelay failed");
        }
        let sock_ref = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10));
        if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
            debug!(error = %e, "failed to set TCP keepalive");
        }
        // Connection lifecycle is fine-grained, high-frequency detail, so
        // it lives at trace (accept/close) and on a dedicated `access_log`
        // target (the negotiated protocol + first request line) rather
        // than the main info log — which stays reserved for lifecycle and
        // state changes. See the `access_log` config flag.
        trace!(%peer, "connection accepted");
        tokio::spawn(serve_connection(
            stream,
            peer,
            Arc::clone(&state),
            lifecycle.clone(),
            permit,
            builders.clone(),
        ));
    }
}

fn method_to_static_str(m: &hyper::http::Method) -> &'static str {
    match m.as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "OPTIONS" => "OPTIONS",
        "HEAD" => "HEAD",
        "PATCH" => "PATCH",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "OTHER",
    }
}

fn status_to_static_str(status: u16) -> &'static str {
    match status {
        100 => "100",
        101 => "101",
        200 => "200",
        201 => "201",
        202 => "202",
        204 => "204",
        206 => "206",
        301 => "301",
        302 => "302",
        304 => "304",
        400 => "400",
        401 => "401",
        403 => "403",
        404 => "404",
        405 => "405",
        408 => "408",
        409 => "409",
        413 => "413",
        414 => "414",
        415 => "415",
        429 => "429",
        500 => "500",
        502 => "502",
        503 => "503",
        504 => "504",
        _ => "OTHER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::http::Method;

    #[test]
    fn test_method_to_static_str() {
        assert_eq!(method_to_static_str(&Method::GET), "GET");
        assert_eq!(method_to_static_str(&Method::POST), "POST");
        assert_eq!(method_to_static_str(&Method::CONNECT), "CONNECT");
        assert_eq!(
            method_to_static_str(&Method::from_bytes(b"OPTIONS").unwrap()),
            "OPTIONS"
        );
        // Edge case: Custom method falls back to OTHER
        assert_eq!(
            method_to_static_str(&Method::from_bytes(b"PURGE").unwrap()),
            "OTHER"
        );
    }

    #[test]
    fn test_status_to_static_str() {
        assert_eq!(status_to_static_str(200), "200");
        assert_eq!(status_to_static_str(404), "404");
        assert_eq!(status_to_static_str(408), "408");
        assert_eq!(status_to_static_str(413), "413");
        assert_eq!(status_to_static_str(414), "414");
        assert_eq!(status_to_static_str(502), "502");
        // Edge case: Custom/unmapped status falls back to OTHER
        assert_eq!(status_to_static_str(418), "OTHER");
        assert_eq!(status_to_static_str(307), "OTHER");
    }

    #[tokio::test(start_paused = true)]
    async fn preface_timeout_evicts_silent_and_partial_pri_while_http1_keepalive_lasts_65s() {
        use bytes::Bytes;
        use http::Response;
        use http_body_util::Full;
        use std::convert::Infallible;
        use tokio::io::AsyncWriteExt;

        let (_shutdown_tx, shutdown_rx) = watch::channel(None);
        let (conn_tx, _conn_rx) = tokio::sync::mpsc::channel(1);
        let lifecycle = lifecycle::Lifecycle::new(shutdown_rx, conn_tx);

        // 1. Completely silent 0-byte socket times out at INITIAL_READ_TIMEOUT (15s).
        let (_silent_client, silent_server) = tokio::io::duplex(256);
        let mut lc1 = lifecycle.clone();
        let silent_task = tokio::spawn(async move {
            read_initial_preface(silent_server, INITIAL_READ_TIMEOUT, &mut lc1)
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
        let mut lc2 = lifecycle.clone();
        let pri_task = tokio::spawn(async move {
            read_initial_preface(pri_server, INITIAL_READ_TIMEOUT, &mut lc2)
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
        let mut lc_h2 = lifecycle.clone();
        let (h2_proto, _) = read_initial_preface(h2_server, INITIAL_READ_TIMEOUT, &mut lc_h2)
            .await
            .expect("HTTP/2 preface");
        assert_eq!(h2_proto, DetectedProto::Http2);

        // 3. HTTP/1.1 connection completes preface immediately, survives 30s idle (> 15s),
        // and closes after 65s of keep-alive inactivity via `serve_detected`.
        let (mut h1_client, h1_server) = tokio::io::duplex(4096);
        let mut lc3 = lifecycle.clone();
        let builders = ServerBuilders::new(lc3.clone());
        let peer: std::net::SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let server_task = tokio::spawn(async move {
            let (proto, prefixed) = read_initial_preface(h1_server, INITIAL_READ_TIMEOUT, &mut lc3)
                .await
                .expect("HTTP/1.1 preface");
            assert_eq!(proto, DetectedProto::Http1);
            let svc = service_fn(|_req| async {
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
            });
            serve_detected(
                &builders,
                proto,
                TokioIo::new(prefixed),
                svc,
                &mut lc3,
                peer,
            )
            .await;
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
    }
}
