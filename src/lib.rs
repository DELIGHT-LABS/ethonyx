//! Ethonyx — Ethereum EL/CL sidecar library entry.
//!
//! The binary in `src/main.rs` is a thin wrapper around [`run`]. Tests can call
//! [`run`] directly with a custom shutdown future to drive the full stack
//! in-process without spawning a subprocess.

#![forbid(unsafe_code)]
#![warn(unreachable_pub)]

pub mod config;
mod headers;
mod health;
mod lifecycle;
mod metrics;
#[cfg(feature = "otel")]
mod otel;
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
pub(crate) static PROMETHEUS_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    OnceLock::new();

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP1_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(65);

/// A validated configuration plus the telemetry providers started for it.
///
/// Preparation is separate from [`Prepared::run`] so a binary can build its
/// tracing subscriber from [`Prepared::tracer`] before the server starts.
#[must_use = "Prepared does nothing unless `.run(shutdown).await` is called"]
pub struct Prepared {
    cfg: Config,
    validated: config::ValidatedConfig,
    #[cfg(feature = "otel")]
    telemetry: Option<otel::Telemetry>,
}

impl Prepared {
    /// Validate `cfg` and start the OTLP providers when an endpoint is configured.
    pub async fn new(cfg: Config) -> Result<Self, BoxError> {
        #[cfg(not(feature = "otel"))]
        let validated = cfg.parse_validated()?;
        #[cfg(feature = "otel")]
        let (validated, telemetry) = {
            let mut validated = cfg.parse_validated()?;
            let telemetry = otel::start(validated.otel_endpoint.take()).await?;
            (validated, telemetry)
        };
        Ok(Self {
            cfg,
            validated,
            #[cfg(feature = "otel")]
            telemetry,
        })
    }

    /// Borrow the validated [`Config`] (e.g. to configure logging before [`Prepared::run`]).
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Tracer for the caller's `tracing-opentelemetry` layer, if telemetry started.
    #[cfg(feature = "otel")]
    pub fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        self.telemetry.as_ref().map(otel::Telemetry::tracer)
    }

    /// Bind every `cfg.listen` address, serve all routes, and wait for `shutdown`
    /// before draining for `cfg.shutdown_grace` seconds. Telemetry providers are
    /// shut down whether or not serving succeeded.
    pub async fn run<F>(self, shutdown: F) -> Result<(), BoxError>
    where
        F: Future<Output = ()> + Send,
    {
        #[cfg(not(feature = "otel"))]
        {
            run_inner(self.cfg, self.validated, shutdown, false).await
        }
        #[cfg(feature = "otel")]
        {
            let otel_enabled = self.telemetry.is_some();
            let res = run_inner(self.cfg, self.validated, shutdown, otel_enabled).await;
            if let Some(telemetry) = self.telemetry {
                telemetry.shutdown().await;
            }
            res
        }
    }
}

/// Validate `cfg`, then serve until `shutdown` resolves. See [`Prepared`].
pub async fn run<F>(cfg: Config, shutdown: F) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    Prepared::new(cfg).await?.run(shutdown).await
}

async fn run_inner<F>(
    cfg: Config,
    mut validated: config::ValidatedConfig,
    shutdown: F,
    otel_enabled: bool,
) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    let listen_addrs = std::mem::take(&mut validated.listen);

    let prometheus_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    #[cfg(feature = "otel")]
    let recorder_installed = if otel_enabled {
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

    let state = Arc::new(AppState::new(&cfg, validated, otel_enabled, health)?);

    tokio::pin!(shutdown);

    let mut initial_degraded = false;
    if let Some(ref tx) = probe_tx {
        // Warm the health cache with one poll before serving, then refresh it in
        // the background so /healthz and /readyz read a snapshot instead of
        // hitting upstream on every probe. Race `shutdown` so SIGTERM during a
        // slow or unreachable startup probe exits immediately.
        let first_probe = tokio::select! {
            biased;
            () = &mut shutdown => {
                info!("shutdown signal received during initial health probe");
                return Ok(());
            }
            probe = health::probe_once(&state) => Arc::new(probe),
        };
        let eval = health::evaluate_readiness(&state, &first_probe);
        health::update_metrics(&state, &first_probe, eval);
        initial_degraded = first_probe.has_errors();
        let _ = tx.send(first_probe);
    }
    let startup_msg = if cfg.trust_upstream {
        "upstreams configured (trusted)"
    } else if initial_degraded {
        "upstreams configured (initial probe degraded)"
    } else {
        "upstreams connected"
    };
    info!(
        el_http = %RedactedUrl(&cfg.el_http_url),
        el_http_proto = %state.el.transport.startup_proto_label(),
        el_ws = %RedactedUrl(&cfg.el_ws_url),
        cl_beacon = %RedactedUrl(&cfg.cl_beacon_url),
        cl_mev_relay = %upstream::RedactedOptUrl(cfg.cl_mev_relay_url.as_deref()),
        cl_proto = %state.cl.transport.startup_proto_label(),
        "{startup_msg}"
    );
    drop(cfg);

    let mut listeners = Vec::with_capacity(listen_addrs.len());
    for addr in listen_addrs {
        let listener = TcpListener::bind((&*addr.host, addr.port))
            .await
            .map_err(|e| format!("failed to bind --listen address '{addr}': {e}"))?;
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

async fn drive_graceful<C>(
    conn: C,
    graceful_shutdown: impl FnOnce(Pin<&mut C>),
    conn_lifecycle: &mut lifecycle::Lifecycle,
    peer: std::net::SocketAddr,
) where
    C: Future<Output = hyper::Result<()>>,
{
    tokio::pin!(conn);
    tokio::select! {
        res = conn.as_mut() => match res {
            Ok(()) => trace!(%peer, "connection closed"),
            Err(e) => debug!(error = %e, %peer, "connection closed"),
        },
        deadline = conn_lifecycle.deadline() => {
            graceful_shutdown(conn.as_mut());
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
            drive_graceful(
                conn,
                http1::UpgradeableConnection::graceful_shutdown,
                conn_lifecycle,
                peer,
            )
            .await;
        }
        DetectedProto::Http2 => {
            let conn = builders.http2.serve_connection(io, svc);
            drive_graceful(
                conn,
                http2::Connection::graceful_shutdown,
                conn_lifecycle,
                peer,
            )
            .await;
        }
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
    mut conn_lifecycle: lifecycle::Lifecycle,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    builders: Arc<ServerBuilders>,
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

pub(crate) fn configure_tcp_stream(stream: &tokio::net::TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        debug!(error = %e, "failed to set TCP_NODELAY");
    }
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10));
    if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
        debug!(error = %e, "failed to set TCP keepalive");
    }
}

async fn accept_loop(
    listeners: Vec<TcpListener>,
    state: Arc<AppState>,
    mut lifecycle: lifecycle::Lifecycle,
) {
    if listeners.is_empty() {
        return;
    }
    let builders = Arc::new(ServerBuilders::new(lifecycle.clone()));
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
        configure_tcp_stream(&stream);
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
            Arc::clone(&builders),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::http::Method;

    #[test]
    fn test_classify_method() {
        assert_eq!(metrics::classify_method(&Method::POST), (Some(0), "POST"));
        assert_eq!(metrics::classify_method(&Method::GET), (Some(1), "GET"));
        assert_eq!(
            metrics::classify_method(&Method::CONNECT),
            (Some(2), "CONNECT")
        );
        assert_eq!(metrics::classify_method(&Method::HEAD), (Some(3), "HEAD"));
        assert_eq!(
            metrics::classify_method(&Method::from_bytes(b"OPTIONS").unwrap()),
            (None, "OPTIONS")
        );
        // Edge case: Custom method falls back to OTHER
        assert_eq!(
            metrics::classify_method(&Method::from_bytes(b"PURGE").unwrap()),
            (None, "OTHER")
        );
    }

    #[tokio::test]
    async fn listen_addr_resolves_numeric_and_named_ipv6_zone_ids() {
        use clap::Parser;
        use std::net::SocketAddr;

        #[cfg(target_os = "macos")]
        let named = "[fe80::1%lo0]:0";
        #[cfg(target_os = "linux")]
        let named = "[fe80::1%lo]:0";

        let cfg = Config::parse_from(["ethonyx", "--listen", "[fe80::1%1]:0"]);
        let validated = cfg.parse_validated().unwrap();
        let addr = &validated.listen[0];
        let resolved: Vec<_> = tokio::net::lookup_host((&*addr.host, addr.port))
            .await
            .expect("numeric IPv6 zone ID must resolve")
            .collect();
        assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() == 1));

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let cfg = Config::parse_from(["ethonyx", "--listen", named]);
            let validated = cfg.parse_validated().unwrap();
            let addr = &validated.listen[0];
            let resolved: Vec<_> = tokio::net::lookup_host((&*addr.host, addr.port))
                .await
                .expect("named IPv6 zone ID must resolve via OS getaddrinfo")
                .collect();
            assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() != 0));

            #[cfg(target_os = "macos")]
            {
                let listener = TcpListener::bind((&*addr.host, addr.port))
                    .await
                    .expect("macOS lo0 must bind [fe80::1%lo0]:0");
                let local = listener.local_addr().unwrap();
                assert!(matches!(local, SocketAddr::V6(v6) if v6.scope_id() != 0));
            }
        }
    }

    #[test]
    fn test_classify_status() {
        assert_eq!(metrics::classify_status(101), (Some(0), "101"));
        assert_eq!(metrics::classify_status(200), (Some(1), "200"));
        assert_eq!(metrics::classify_status(204), (Some(2), "204"));
        assert_eq!(metrics::classify_status(400), (Some(3), "400"));
        assert_eq!(metrics::classify_status(404), (Some(4), "404"));
        assert_eq!(metrics::classify_status(408), (None, "408"));
        assert_eq!(metrics::classify_status(413), (Some(5), "413"));
        assert_eq!(metrics::classify_status(414), (None, "414"));
        assert_eq!(metrics::classify_status(502), (Some(6), "502"));
        assert_eq!(metrics::classify_status(503), (Some(7), "503"));
        // Edge case: Custom/unmapped status falls back to OTHER
        assert_eq!(metrics::classify_status(418), (None, "OTHER"));
        assert_eq!(metrics::classify_status(307), (None, "OTHER"));
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
