//! Ethonyx — Ethereum EL/CL sidecar library entry.
//!
//! The binary in `src/main.rs` is a thin wrapper around [`Prepared`]. Callers can
//! drive the full stack in-process via [`run`] / [`Prepared::run`], or split port
//! binding from serving via [`Prepared::bind`] and [`Bound::serve`] to inspect
//! ephemeral [`Bound::local_addrs`] and [`Bound::admin_addr`] first.

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
/// [`Bound::local_addrs`] and [`Bound::admin_addr`] before calling [`Bound::serve`].
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
    /// every `--listen` TCP socket (plus optional `--admin-listen`), returning a
    /// [`Bound`] server ready to [`Bound::serve`].
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
        let (listen, admin_listen) = listen.into_parts();
        let mut resolved_listen = Vec::with_capacity(listen.len());
        for addr in listen {
            resolved_listen.push(resolve_listener("--listen", addr).await?);
        }
        let mut resolved_admin = match admin_listen {
            Some(addr) => Some(resolve_listener("--admin-listen", addr).await?),
            None => None,
        };
        config::validate_resolved_addrs(&resolved_listen, resolved_admin.as_ref())?;

        let mut listeners = Vec::with_capacity(resolved_listen.len());
        let mut local_addrs = Vec::with_capacity(resolved_listen.len());
        for resolved in &mut resolved_listen {
            let (listener, local_addr) = bind_listener("--listen", resolved).await?;
            resolved.set_bound_addr(local_addr);
            listeners.push(listener);
            local_addrs.push(local_addr);
        }
        let admin = match &mut resolved_admin {
            Some(resolved) => {
                let (listener, local_addr) = bind_listener("--admin-listen", resolved).await?;
                resolved.set_bound_addr(local_addr);
                Some((listener, local_addr))
            }
            None => None,
        };
        config::validate_resolved_addrs(&resolved_listen, resolved_admin.as_ref())?;
        // Emit `"listening"` only after every listener bind and post-bind overlap check succeeds
        // so a failed `--admin-listen` bind never leaves misleading `"listening"` logs for dropped sockets.
        for local_addr in &local_addrs {
            info!(listen = %local_addr, "listening");
        }
        if let Some((_, admin_addr)) = &admin {
            info!(admin_listen = %admin_addr, "listening");
        }

        Ok(Bound {
            listeners,
            local_addrs,
            admin,
            state,
            poller,
            shutdown_grace,
            telemetry,
        })
    }

    /// Bind every `cfg.listen` address (and optional `cfg.admin_listen`), serve all
    /// routes, and wait for `shutdown` before draining for `cfg.shutdown_grace`
    /// seconds and flushing telemetry.
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
    admin: Option<(TcpListener, std::net::SocketAddr)>,
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

    /// Actual bound socket address for [`Config::admin_listen`], if configured.
    pub const fn admin_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.admin {
            Some((_, addr)) => Some(*addr),
            None => None,
        }
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
            admin,
            state,
            poller,
            shutdown_grace,
            telemetry,
        } = self;

        let (shutdown_tx, lifecycle, drain) = lifecycle::channel();
        let mut poller = poller.map(|p| p.spawn(Arc::clone(&state), lifecycle.shutdown()));
        let probes = if admin.is_some() {
            proxy::ListenProbes::Hidden
        } else {
            proxy::ListenProbes::Serve
        };
        let mut admin_handle = admin.map(|(listener, _)| {
            tokio::spawn(server::accept_admin_loop(
                listener,
                Arc::clone(&state),
                lifecycle.clone(),
            ))
        });
        let mut accept_handle = Some(tokio::spawn(server::accept_loop(
            listeners, state, lifecycle, probes,
        )));

        tokio::pin!(shutdown);
        let (first_res, unexpected) = tokio::select! {
            biased;
            () = &mut shutdown => {
                info!("shutdown signal received");
                (Ok(()), None)
            }
            Some(res) = OptionFuture::from(accept_handle.as_mut()) => {
                accept_handle = None;
                warn!("accept loop exited unexpectedly; shutting down");
                (res, Some("accept loop exited unexpectedly"))
            }
            Some(res) = OptionFuture::from(admin_handle.as_mut()), if admin_handle.is_some() => {
                admin_handle = None;
                warn!("admin accept loop exited unexpectedly; shutting down");
                (res, Some("admin accept loop exited unexpectedly"))
            }
            Some(res) = OptionFuture::from(poller.as_mut()), if poller.is_some() => {
                poller = None;
                warn!("health poller exited unexpectedly; shutting down");
                (res, Some("health poller exited unexpectedly"))
            }
        };

        let deadline = tokio::time::Instant::now() + shutdown_grace;
        shutdown_tx.send(deadline);

        let (accept_res, admin_res, poller_res) = tokio::join!(
            OptionFuture::from(accept_handle),
            OptionFuture::from(admin_handle),
            OptionFuture::from(poller),
        );
        let outcome: Result<(), BoxError> = first_res
            .and_then(|()| accept_res.unwrap_or(Ok(())))
            .and_then(|()| admin_res.unwrap_or(Ok(())))
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

async fn resolve_listener(
    flag: &'static str,
    addr: config::ListenAddr,
) -> Result<config::ResolvedListen, BoxError> {
    match addr.resolve_target() {
        config::ResolveTarget::Literal(sa) => Ok(config::ResolvedListen::single(addr, sa)),
        config::ResolveTarget::Lookup(host, port) => {
            let addrs: Vec<_> = tokio::net::lookup_host((host, port))
                .await
                .map_err(|e| format!("failed to resolve {flag} address '{addr}': {e}"))?
                .collect();
            if addrs.is_empty() {
                return Err(format!(
                    "failed to resolve {flag} address '{addr}': no addresses returned"
                )
                .into());
            }
            Ok(config::ResolvedListen::many(addr, addrs))
        }
    }
}

fn format_bind_error(
    flag: &'static str,
    resolved: &config::ResolvedListen,
    sa: std::net::SocketAddr,
    e: &std::io::Error,
) -> BoxError {
    if matches!(
        resolved.configured().resolve_target(),
        config::ResolveTarget::Literal(_)
    ) {
        format!("failed to bind {flag} address '{resolved}': {e}").into()
    } else {
        format!("failed to bind {flag} address '{resolved}' ({sa}): {e}").into()
    }
}

fn should_fail_fast_bind(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
    )
}

async fn bind_listener(
    flag: &'static str,
    resolved: &config::ResolvedListen,
) -> Result<(TcpListener, std::net::SocketAddr), BoxError> {
    let mut last_err = None;
    for &sa in resolved.addrs() {
        match TcpListener::bind(sa).await {
            Ok(listener) => {
                let local_addr = listener.local_addr().map_err(|e| {
                    format!("failed to query local {flag} address for '{resolved}': {e}")
                })?;
                return Ok((listener, local_addr));
            }
            // Fail fast on address/port occupancy or policy denial rather than falling
            // back to a sibling A/AAAA record (which would cause dual-stack split-brain).
            Err(e) if should_fail_fast_bind(&e) => {
                return Err(format_bind_error(flag, resolved, sa, &e));
            }
            Err(e) => last_err = Some((sa, e)),
        }
    }
    match last_err {
        Some((sa, e)) => Err(format_bind_error(flag, resolved, sa, &e)),
        None => Err(
            format!("failed to bind {flag} address '{resolved}': no candidate addresses").into(),
        ),
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
        assert!(bound.admin_addr().is_none());
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

    #[tokio::test]
    async fn prepared_bind_binds_admin_addr_and_rolls_back_on_admin_bind_failure() {
        use clap::Parser;

        // Ephemeral `--admin-listen 127.0.0.1:0` gets its own distinct non-zero port.
        let cfg = Config::parse_from([
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--admin-listen",
            "127.0.0.1:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
            "--shutdown-grace",
            "0",
        ]);
        let bound = Prepared::new(cfg).await.unwrap().bind().await.unwrap();
        let admin_addr = bound.admin_addr().expect("admin_addr must be bound");
        assert_ne!(admin_addr.port(), 0);
        assert_ne!(admin_addr.port(), bound.local_addrs()[0].port());
        drop(bound);

        // If `--admin-listen` fails to bind after `--listen` succeeds, the error cites
        // `--admin-listen` and the already-bound `--listen` port is released immediately.
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let occupied_addr = occupied.local_addr().unwrap();
        let listen_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let free_listen_addr = listen_probe.local_addr().unwrap();
        drop(listen_probe);

        let fail_cfg = Config::parse_from([
            "ethonyx",
            "--listen",
            &free_listen_addr.to_string(),
            "--admin-listen",
            &occupied_addr.to_string(),
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
        ]);
        let err = Prepared::new(fail_cfg)
            .await
            .unwrap()
            .bind()
            .await
            .err()
            .expect("occupied admin port must fail bind")
            .to_string();
        assert!(err.contains("--admin-listen"), "unexpected error: {err}");
        // The `--listen` socket was dropped on rollback and can be bound again immediately.
        let rebound = TcpListener::bind(free_listen_addr).await;
        assert!(rebound.is_ok(), "rollback should release --listen socket");
        drop(occupied);
    }

    #[tokio::test]
    async fn prepared_bind_rejects_resolved_hostname_conflicts_and_allows_ephemeral_zero() {
        use clap::Parser;

        let resolved: Vec<_> = tokio::net::lookup_host(("localhost", 0))
            .await
            .expect("localhost must resolve")
            .collect();
        let has_v4 = resolved.iter().any(|a| a.is_ipv4());
        let has_v6 = resolved.iter().any(|a| a.is_ipv6());
        assert!(has_v4 || has_v6, "localhost resolved to no addresses");

        // Pre-bind resolved candidate validation rejects hostname overlaps across both
        // planes and within `--listen` before any socket is opened (zero-TOCTOU).
        let mut cross_cases = Vec::new();
        let mut same_cases = Vec::new();
        if has_v4 {
            cross_cases.extend(["0.0.0.0:8547", "127.0.0.1:8547"]);
            same_cases.push(("0.0.0.0:8547,localhost:8547", "conflicts with"));
        }
        if has_v6 {
            cross_cases.push("[::1]:8547");
            same_cases.push(("[::]:8547,localhost:8547", "conflicts with"));
        }
        for listen_spec in cross_cases {
            let cfg = Config::parse_from([
                "ethonyx",
                "--listen",
                listen_spec,
                "--admin-listen",
                "localhost:8547",
                "--el-http-url",
                "http://127.0.0.1:8545",
                "--trust-upstream",
            ]);
            let err = Prepared::new(cfg)
                .await
                .unwrap()
                .bind()
                .await
                .err()
                .expect("hostname overlapping --listen must fail pre-bind")
                .to_string();
            assert!(
                err.contains("--admin-listen") && err.contains("conflicts with --listen"),
                "unexpected error for {listen_spec}: {err}"
            );
        }
        for (listen_spec, expected) in same_cases {
            let cfg = Config::parse_from([
                "ethonyx",
                "--listen",
                listen_spec,
                "--el-http-url",
                "http://127.0.0.1:8545",
                "--trust-upstream",
            ]);
            let err = Prepared::new(cfg)
                .await
                .unwrap()
                .bind()
                .await
                .err()
                .expect("same-plane hostname overlap must fail pre-bind")
                .to_string();
            assert!(
                err.contains(expected),
                "unexpected error for {listen_spec}: {err}"
            );
        }

        // Ephemeral `:0` ports on `localhost` remain permitted across both planes and
        // receive distinct kernel-assigned ports.
        let cfg = Config::parse_from([
            "ethonyx",
            "--listen",
            "localhost:0",
            "--admin-listen",
            "localhost:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
        ]);
        let bound = Prepared::new(cfg).await.unwrap().bind().await.unwrap();
        let data_port = bound.local_addrs()[0].port();
        let admin_port = bound.admin_addr().expect("admin_addr must be bound").port();
        assert_ne!(data_port, 0);
        assert_ne!(admin_port, 0);
        assert_ne!(data_port, admin_port);
    }

    #[tokio::test]
    async fn bind_listener_fails_fast_on_addr_in_use_and_falls_back_on_unavailable_family() {
        use std::io::{Error, ErrorKind};

        // Pure OS-independent check for fail-fast vs family-fallback error classification.
        assert!(should_fail_fast_bind(&Error::from(ErrorKind::AddrInUse)));
        assert!(should_fail_fast_bind(&Error::from(
            ErrorKind::PermissionDenied
        )));
        assert!(!should_fail_fast_bind(&Error::from(
            ErrorKind::AddrNotAvailable
        )));
        assert!(!should_fail_fast_bind(&Error::from(ErrorKind::Unsupported)));
        assert!(!should_fail_fast_bind(&Error::other("family unavailable")));

        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let occupied_addr = occupied.local_addr().unwrap();
        let free_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let free_addr = free_probe.local_addr().unwrap();
        drop(free_probe);

        let (mut parsed, _) = config::ListenPlan::parse(&["localhost:8547".to_owned()], None)
            .unwrap()
            .into_parts();
        let resolved =
            config::ResolvedListen::many(parsed.pop().unwrap(), vec![occupied_addr, free_addr]);
        let err = bind_listener("--admin-listen", &resolved)
            .await
            .expect_err("AddrInUse must fail fast instead of falling back")
            .to_string();
        assert!(
            err.contains("--admin-listen") && err.contains(&occupied_addr.to_string()),
            "unexpected error: {err}"
        );
        drop(occupied);

        // On hosts without `net.ipv4.ip_nonlocal_bind = 1`, verify live fallback from an
        // unassignable address (`AddrNotAvailable`) to the subsequent loopback candidate.
        let unroutable: std::net::SocketAddr = "192.0.2.1:0".parse().unwrap();
        if TcpListener::bind(unroutable).await.is_err() {
            let (mut parsed, _) = config::ListenPlan::parse(&["localhost:0".to_owned()], None)
                .unwrap()
                .into_parts();
            let loopback: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let resolved =
                config::ResolvedListen::many(parsed.pop().unwrap(), vec![unroutable, loopback]);
            let (_listener, local) = bind_listener("--listen", &resolved)
                .await
                .expect("AddrNotAvailable should fall back to loopback candidate");
            assert!(local.ip().is_loopback());
            assert_ne!(local.port(), 0);
        }
    }
}
