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
pub use upstream::RedactedUrl;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::state::AppState;

use std::sync::OnceLock;
pub static PROMETHEUS_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    OnceLock::new();

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bind every `cfg.listen` address, serve all routes, and wait for `shutdown`
/// before draining for `cfg.shutdown_grace` seconds.
pub async fn run<F>(cfg: Config, shutdown: F) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    cfg.validate()?;
    #[cfg(feature = "otel")]
    let telemetry = crate::otel::start(cfg.otel_endpoint.as_deref()).await?;
    run_inner(
        cfg,
        shutdown,
        #[cfg(feature = "otel")]
        telemetry,
    )
    .await
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
    run_inner(cfg, shutdown, telemetry).await
}

async fn run_inner<F>(
    cfg: Config,
    shutdown: F,
    #[cfg(feature = "otel")] telemetry: Option<otel::Telemetry>,
) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    let config::ValidatedConfig {
        el_http,
        el_ws,
        cl_beacon,
        cl_mev_relay,
        cl_genesis_time,
        cl_seconds_per_slot,
    } = cfg.parse_validated()?;

    // Install default rustls cryptoprovider to prevent panics during parallel tests
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let prometheus_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    #[cfg(feature = "otel")]
    let otel_enabled = telemetry.is_some() || cfg.otel_endpoint.is_some();
    #[cfg(not(feature = "otel"))]
    let otel_enabled = false;

    #[cfg(feature = "otel")]
    let recorder_installed = if telemetry.is_some() {
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

    let client = proxy::build_client(false);
    // A second, HTTP/2-only client for the EL hop; the health poller decides at
    // runtime (`el_use_h2`) whether to use it — preferring h2c and falling back
    // to HTTP/1.1 when the upstream doesn't speak it.
    let el_h2_client = proxy::build_client(true);
    let cl_syncing_uri =
        cl_beacon.rewrite_uri(http::Uri::from_static("/"), Some("/eth/v1/node/syncing"))?;
    let cl_peer_count_uri =
        cl_beacon.rewrite_uri(http::Uri::from_static("/"), Some("/eth/v1/node/peer_count"))?;
    let poll_interval = cfg.health_poll_interval;
    let shutdown_grace = cfg.shutdown_grace;

    let (probe_tx, probe_rx) = watch::channel(Arc::new(health::Probe::pending()));
    let (shutdown_tx, _) = watch::channel(None);

    let el_transport = health::ElTransport::new(cfg.trust_upstream, el_http.is_tls());
    let cl_tls_version = cl_beacon.is_tls().then(health::ObservedHttpVersion::new);
    let state = Arc::new(AppState {
        cfg,
        client,
        el_h2_client,
        el_transport,
        cl_tls_version,
        el_http,
        el_ws,
        cl_beacon,
        cl_mev_relay,
        cl_syncing_uri,
        cl_peer_count_uri,
        cl_genesis_time,
        cl_seconds_per_slot,
        otel_enabled,
        probe: probe_rx,
    });

    if !state.cfg.trust_upstream {
        // Warm the health cache with one poll before serving, then refresh it in
        // the background so /healthz and /readyz read a snapshot instead of
        // hitting upstream on every probe.
        let first_probe = Arc::new(health::probe_once(&state).await);
        let first_report = health::evaluate_ready(&state, &first_probe);
        health::update_metrics(&first_probe, &first_report);
        let _ = probe_tx.send(first_probe);
    }
    info!(
        el_http = %RedactedUrl(&state.cfg.el_http_url),
        el_http_proto = %state.el_transport.startup_proto_label(),
        el_ws = %RedactedUrl(&state.cfg.el_ws_url),
        cl_beacon = %RedactedUrl(&state.cfg.cl_beacon_url),
        cl_mev_relay = %upstream::RedactedOptUrl(state.cfg.cl_mev_relay_url.as_deref()),
        cl_proto = %state.cl_startup_proto_label(),
        "upstreams connected"
    );
    let poller = (!state.cfg.trust_upstream).then(|| {
        tokio::spawn(health::poll_loop(
            state.clone(),
            probe_tx,
            shutdown_tx.subscribe(),
            poll_interval,
        ))
    });

    let mut listeners = Vec::with_capacity(state.cfg.listen.len());
    for addr in &state.cfg.listen {
        let listener = TcpListener::bind(addr).await?;
        info!(listen = %addr, "listening");
        listeners.push(listener);
    }

    let (conn_tx, mut conn_rx) = tokio::sync::mpsc::channel::<()>(1);
    let lifecycle = lifecycle::Lifecycle::new(shutdown_tx.subscribe(), conn_tx);

    let mut accept_handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        accept_handles.push(tokio::spawn(accept_loop(
            listener,
            state.clone(),
            lifecycle.clone(),
        )));
    }

    shutdown.await;
    info!("shutdown signal received");

    let deadline = tokio::time::Instant::now() + shutdown_grace;
    shutdown_tx.send_replace(Some(deadline));

    for h in accept_handles {
        let _ = h.await;
    }
    if let Some(poller) = poller {
        let _ = poller.await;
    }

    // Drop primary sender so receiver returns None when all client connection senders drop
    drop(lifecycle);

    info!(
        grace_secs = shutdown_grace.as_secs(),
        "draining connections"
    );
    // Each connection, HTTP/2 request and relay uses the same deadline. Wait for
    // their futures to be dropped even with zero grace; timing out this barrier
    // would let run() return before sockets and request tasks are released.
    let _ = conn_rx.recv().await;
    #[cfg(feature = "otel")]
    if let Some(tp) = telemetry {
        tp.shutdown().await;
    }
    Ok(())
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    mut lifecycle: lifecycle::Lifecycle,
) {
    loop {
        tokio::select! {
            biased;
            _ = lifecycle.deadline() => return,
            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        // Back off so a persistent error (e.g. fd exhaustion)
                        // doesn't busy-spin a core retrying immediately.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
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
                let state = state.clone();
                let mut conn_lifecycle = lifecycle.clone();
                tokio::spawn(async move {
                    let _guard = crate::metrics::ActiveConnectionGuard::tcp();
                    let io = TokioIo::new(stream);
                    let connection = Arc::new(proxy::Connection::new(
                        state,
                        conn_lifecycle.clone(),
                        peer,
                    ));
                    let svc = service_fn(move |req| connection.clone().dispatch(req));
                    // Auto-detect HTTP/1 vs HTTP/2 (incl. cleartext h2c preface);
                    // `_with_upgrades` keeps HTTP/1.1 WebSocket Upgrade working;
                    // `enable_connect_protocol` advertises RFC 8441 Extended CONNECT
                    // (HTTP/2 WebSocket).
                    let mut builder = auto::Builder::new(conn_lifecycle.clone());
                    builder.http2().enable_connect_protocol();
                    let conn = builder.serve_connection_with_upgrades(io, svc);
                    tokio::pin!(conn);
                    tokio::select! {
                        res = conn.as_mut() => {
                            match res {
                                Ok(()) => trace!(%peer, "connection closed"),
                                Err(e) => debug!(error = %e, %peer, "connection closed"),
                            }
                        }
                        deadline = conn_lifecycle.deadline() => {
                            conn.as_mut().graceful_shutdown();
                            match tokio::time::timeout_at(deadline, conn).await {
                                Ok(Ok(())) => trace!(%peer, "connection closed after shutdown"),
                                Ok(Err(e)) => debug!(error = %e, %peer, "connection closed after shutdown"),
                                Err(_) => trace!(%peer, "connection drain deadline reached"),
                            }
                        }
                    }
                });
            }
        }
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
        409 => "409",
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
        assert_eq!(status_to_static_str(502), "502");
        // Edge case: Custom/unmapped status falls back to OTHER
        assert_eq!(status_to_static_str(418), "OTHER");
        assert_eq!(status_to_static_str(307), "OTHER");
    }
}
