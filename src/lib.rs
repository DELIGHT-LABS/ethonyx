//! Ethonyx — Ethereum EL/CL sidecar library entry.
//!
//! The binary in `src/main.rs` is a thin wrapper around [`Prepared`]. Callers can
//! drive the full stack in-process via [`run`] / [`Prepared::run`], or split port
//! binding from serving via [`Prepared::bind`] and [`Bound::serve`] to inspect
//! ephemeral [`Bound::local_addrs`] first.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod config;
mod headers;
mod health;
mod lifecycle;
mod metrics;
#[cfg_attr(not(feature = "otel"), path = "otel_noop.rs")]
mod otel;
mod proxy;
mod response;
mod rpc;
mod server;
mod state;
mod transport;
mod upstream;
mod websocket;

pub use config::Config;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::OptionFuture;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::state::AppState;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A validated configuration plus the telemetry providers started for it.
///
/// Preparation is separate from [`Prepared::bind`] and [`Prepared::run`] so a
/// binary can build its tracing subscriber from `Prepared::tracer` before the
/// server starts, and callers using ephemeral `:0` ports can inspect
/// [`Bound::local_addrs`] before calling [`Bound::serve`].
#[must_use = "Prepared does nothing unless `.bind().await` or `.run(shutdown).await` is called"]
pub struct Prepared {
    cfg: Config,
    parsed: config::ParsedConfig,
    telemetry: otel::Telemetry,
}

impl Prepared {
    /// Validate `cfg` and start the OTLP providers when an endpoint is configured.
    pub async fn new(cfg: Config) -> Result<Self, BoxError> {
        let (parsed, otel_endpoint) = cfg.parse_with_otel()?;
        let telemetry = otel::start(otel_endpoint)?;
        Ok(Self {
            cfg,
            parsed,
            telemetry,
        })
    }

    /// Borrow the validated [`Config`] (e.g. to configure logging before [`Prepared::run`]).
    pub const fn config(&self) -> &Config {
        &self.cfg
    }

    /// Tracer for the caller's `tracing-opentelemetry` layer, if telemetry started.
    #[cfg(feature = "otel")]
    pub fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        self.telemetry.tracer()
    }

    /// Warm the initial health snapshot (unless `--trust-upstream` is set) and bind
    /// every `--listen` TCP socket, returning a [`Bound`] server ready to [`Bound::serve`].
    ///
    /// Background health polling and connection acceptance start when [`Bound::serve`]
    /// is polled. If binding fails, any started telemetry providers trigger best-effort
    /// background cleanup on drop without stalling error reporting.
    pub async fn bind(self) -> Result<Bound, BoxError> {
        let Self {
            cfg,
            parsed:
                config::ParsedConfig {
                    listen,
                    opts,
                    max_connections,
                    upstreams,
                    health,
                    shutdown_grace,
                },
            telemetry,
        } = self;
        let el_references = health.reference_count();

        info!(
            version = concat!("v", env!("CARGO_PKG_VERSION")),
            git = env!("ETHONYX_GIT_DESCRIBE"),
            ?cfg,
            "starting ethonyx"
        );
        drop(cfg);

        // `Prepared::run` cancels this warm-up poll by dropping the `bind` future
        // when shutdown arrives before listeners are bound.
        let (state, poller) = AppState::start(
            opts,
            max_connections,
            upstreams,
            health,
            metrics::ServerMetrics::new(telemetry.metrics()),
        )
        .await;

        let startup_msg = state.health.startup_message();
        let el_ws = state.el_ws.as_ref().map(|ws| ws.upstream.redacted());
        let cl_beacon = state.cl.as_ref().map(|cl| cl.beacon.upstream.redacted());
        let cl_mev_relay = state
            .cl
            .as_ref()
            .and_then(|cl| cl.mev_relay.as_ref())
            .map(|mev| mev.upstream.redacted());
        let cl_proto = state
            .cl
            .as_ref()
            .map_or("none", |cl| cl.beacon.transport.startup_proto_label());
        info!(
            el_http = %state.el.upstream.redacted(),
            el_http_proto = %state.el.transport.startup_proto_label(),
            el_ws = %upstream::RedactedOptUri(el_ws),
            el_references,
            cl_beacon = %upstream::RedactedOptUri(cl_beacon),
            cl_mev_relay = %upstream::RedactedOptUri(cl_mev_relay),
            cl_proto = %cl_proto,
            "{startup_msg}"
        );

        // No `.await` follows a failed bind: `Prepared::run` races `self.bind()`
        // against `shutdown`, so returning immediately via `?` ensures a concurrent
        // shutdown signal can never overtake and swallow a bind error.
        let mut listeners = Vec::with_capacity(listen.len());
        let mut local_addrs = Vec::with_capacity(listen.len());
        for addr in listen {
            let listener = TcpListener::bind((&*addr.host, addr.port))
                .await
                .map_err(|e| format!("failed to bind --listen address '{addr}': {e}"))?;
            let local_addr = listener
                .local_addr()
                .map_err(|e| format!("failed to query local address for '{addr}': {e}"))?;
            info!(listen = %local_addr, "listening");
            listeners.push(listener);
            local_addrs.push(local_addr);
        }

        Ok(Bound {
            listeners,
            local_addrs,
            state,
            poller,
            shutdown_grace,
            telemetry,
        })
    }

    /// Bind every `cfg.listen` address, serve all routes, and wait for `shutdown`
    /// before draining for `cfg.shutdown_grace` seconds and flushing telemetry.
    ///
    /// If `shutdown` resolves before [`Prepared::bind`] completes, returns `Ok(())`
    /// immediately without binding any listener or awaiting telemetry flush (any
    /// started providers trigger best-effort background cleanup on drop).
    pub async fn run<F>(self, shutdown: F) -> Result<(), BoxError>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let bound = tokio::select! {
            biased;
            () = &mut shutdown => {
                // Dropping the `bind` future cancels startup (including a slow or
                // unreachable initial health probe) before any listener is bound;
                // `Telemetry::drop` triggers best-effort background cleanup.
                info!("shutdown signal received during startup");
                return Ok(());
            }
            bound = self.bind() => bound?,
        };
        bound.serve(shutdown).await
    }
}

/// A [`Prepared`] server whose initial health snapshot is warm and whose TCP
/// listeners are bound and ready to accept connections.
#[must_use = "Bound does nothing unless `.serve(shutdown).await` is called"]
pub struct Bound {
    listeners: Vec<TcpListener>,
    local_addrs: Vec<std::net::SocketAddr>,
    state: Arc<AppState>,
    poller: Option<health::HealthPoller>,
    shutdown_grace: Duration,
    telemetry: otel::Telemetry,
}

impl Bound {
    /// Actual bound socket addresses in the same order as [`Config::listen`].
    pub fn local_addrs(&self) -> &[std::net::SocketAddr] {
        &self.local_addrs
    }

    /// Serve all bound listeners until `shutdown` resolves, then drain active
    /// connections for `cfg.shutdown_grace` seconds and shut down telemetry.
    pub async fn serve<F>(self, shutdown: F) -> Result<(), BoxError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            listeners,
            local_addrs: _,
            state,
            poller,
            shutdown_grace,
            telemetry,
        } = self;

        let (shutdown_tx, lifecycle, drain) = lifecycle::channel();
        let mut poller = poller.map(|p| p.spawn(Arc::clone(&state), lifecycle.shutdown()));
        let mut accept_handle = tokio::spawn(server::accept_loop(listeners, state, lifecycle));

        enum ServeExit {
            Shutdown,
            Accept(Result<(), tokio::task::JoinError>),
            Poller(Result<(), tokio::task::JoinError>),
        }

        tokio::pin!(shutdown);
        let exit = tokio::select! {
            biased;
            () = &mut shutdown => {
                info!("shutdown signal received");
                ServeExit::Shutdown
            }
            res = &mut accept_handle => {
                warn!("accept loop exited unexpectedly; shutting down");
                ServeExit::Accept(res)
            }
            Some(res) = OptionFuture::from(poller.as_mut()), if poller.is_some() => {
                warn!("health poller exited unexpectedly; shutting down");
                ServeExit::Poller(res)
            }
        };

        let deadline = tokio::time::Instant::now() + shutdown_grace;
        shutdown_tx.send(deadline);

        let (accept_res, poller_res, unexpected) = match exit {
            ServeExit::Shutdown => {
                let (a, p) = tokio::join!(accept_handle, OptionFuture::from(poller));
                (a, p, None)
            }
            ServeExit::Accept(a) => (
                a,
                OptionFuture::from(poller).await,
                Some("accept loop exited unexpectedly"),
            ),
            ServeExit::Poller(p) => (
                accept_handle.await,
                Some(p),
                Some("health poller exited unexpectedly"),
            ),
        };
        let outcome: Result<(), BoxError> = accept_res
            .and_then(|()| poller_res.unwrap_or(Ok(())))
            .map_err(BoxError::from)
            .and_then(|()| match unexpected {
                Some(msg) => Err(msg.into()),
                None => Ok(()),
            });

        info!(grace = ?shutdown_grace, "draining connections");
        // Each connection, HTTP/2 request and relay uses the same deadline. Wait for
        // their futures to be dropped even with zero grace; timing out this barrier
        // would let serve() return before sockets and request tasks are released.
        drain.wait().await;

        telemetry.shutdown().await;
        outcome
    }
}

/// Validate `cfg`, then serve until `shutdown` resolves. See [`Prepared`].
pub async fn run<F>(cfg: Config, shutdown: F) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    Prepared::new(cfg).await?.run(shutdown).await
}

#[cfg(test)]
pub(crate) mod test_util {
    /// Abort a background mock server task on drop so unit tests never leak listeners.
    pub(crate) struct AbortOnDrop(pub(crate) tokio::task::JoinHandle<()>);

    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    /// Loop-peek up to 3 bytes to detect an HTTP/2 client preface (`"PRI"`) safely
    /// even if TCP delivers the preface split across 1–2 byte segments.
    pub(crate) async fn peek_is_h2_preface(stream: &tokio::net::TcpStream) -> bool {
        let mut buf = [0u8; 3];
        tokio::time::timeout(std::time::Duration::from_millis(500), async {
            loop {
                match stream.peek(&mut buf).await {
                    Ok(0) | Err(_) => return false,
                    Ok(n) if n >= 3 => return &buf == b"PRI",
                    Ok(n) if !b"PRI".starts_with(&buf[..n]) => return false,
                    Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Spawn an HTTP/1.1 + `h2c` mock server on an ephemeral loopback port, aborting on drop.
    pub(crate) async fn spawn_http_mock<F, Fut>(handler: F) -> (std::net::SocketAddr, AbortOnDrop)
    where
        F: Fn(http::Request<hyper::body::Incoming>) -> Fut + Clone + Send + Sync + 'static,
        Fut:
            std::future::Future<Output = http::Response<crate::response::ResBody>> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = AbortOnDrop(tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let handler = handler.clone();
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req| {
                        let fut = handler(req);
                        async move { Ok::<_, std::convert::Infallible>(fut.await) }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
                });
            }
        }));
        (addr, task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prepared_bind_assigns_ephemeral_ports_and_serves_until_shutdown() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cfg = Config::parse_from([
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--listen",
            "127.0.0.1:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
            "--shutdown-grace",
            "0",
        ]);
        let bound = Prepared::new(cfg).await.unwrap().bind().await.unwrap();
        let [a0, a1] = bound.local_addrs() else {
            panic!("expected two bound addresses");
        };
        assert_ne!(a0.port(), 0);
        assert_ne!(a1.port(), 0);
        assert_ne!(a0.port(), a1.port());
        let addr0 = *a0;

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(bound.serve(async move {
            let _ = rx.await;
        }));

        let mut stream = tokio::net::TcpStream::connect(addr0).await.unwrap();
        stream
            .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = [0u8; 128];
        let n = stream.read(&mut resp).await.unwrap();
        assert!(resp[..n].starts_with(b"HTTP/1.1 200 OK"));
        drop(stream);

        let _ = tx.send(());
        server.await.unwrap().unwrap();
    }
}
